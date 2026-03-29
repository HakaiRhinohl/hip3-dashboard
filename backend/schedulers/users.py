"""
schedulers/users.py — HIP-3 Users bootstrap + incremental updater.

Uses AWS CLI (subprocess) to download fills parquet files from Hydromancer S3,
processes them with DuckDB, and stores results in SQLite at USERS_DB_PATH.

Exposes:
  run_users_bootstrap()    — process all unprocessed dates (idempotent)
  run_users_incremental()  — process any new dates since last run
  UsersCollector           — class with collect() / get_data() / get_timeline() / get_top_venues()
"""

import json
import logging
import os
import sqlite3
import subprocess
import tempfile
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Lock

logger = logging.getLogger("kinetiq.users")

DB_PATH = os.environ.get("USERS_DB_PATH", "/data/users.db")

DEXES = ["km", "xyz", "flx", "cash", "hyna", "vntl"]

# First available date per DEX (inclusive)
DEX_START = {
    "km":   date(2026, 1, 12),
    "xyz":  date(2025, 10, 13),
    "flx":  date(2025, 11, 13),
    "cash": date(2026, 1, 20),
    "hyna": date(2025, 12, 3),
    "vntl": date(2025, 11, 13),
}

# Last available date for all DEXes
DATA_END = date(2026, 3, 23)

S3_BUCKET   = "hydromancer-reservoir"
S3_REGION   = "ap-northeast-1"
S3_BASE     = f"s3://{S3_BUCKET}/by_dex"


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _open_db() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS user_stats (
            address            TEXT PRIMARY KEY,
            first_hip3_date    TEXT,
            last_hip3_date     TEXT,
            hip3_volume_usd    REAL DEFAULT 0,
            trade_count        INTEGER DEFAULT 0,
            primary_dex        TEXT,
            dex_volumes        TEXT,
            first_seen_dex     TEXT
        );

        CREATE TABLE IF NOT EXISTS daily_new_users (
            date        TEXT,
            dex         TEXT,
            new_users   INTEGER,
            PRIMARY KEY (date, dex)
        );

        CREATE TABLE IF NOT EXISTS bootstrap_progress (
            dex          TEXT,
            date         TEXT,
            processed_at TEXT,
            PRIMARY KEY (dex, date)
        );
    """)
    conn.commit()
    return conn


def _processed_dates(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    rows = conn.execute("SELECT dex, date FROM bootstrap_progress").fetchall()
    return {(r["dex"], r["date"]) for r in rows}


def _total_dates() -> int:
    total = 0
    for dex, start in DEX_START.items():
        delta = (DATA_END - start).days + 1
        total += max(0, delta)
    return total


# ── S3 download ────────────────────────────────────────────────────────────────

def _download_parquet(dex: str, date_str: str, local_path: str) -> bool:
    """Download a single parquet file via aws s3 cp. Returns True on success."""
    s3_key = f"{S3_BASE}/{dex}/fills/perp/all/date={date_str}/fills.parquet"
    cmd = [
        "aws", "s3", "cp", s3_key, local_path,
        "--request-payer", "requester",
        "--region", S3_REGION,
        "--quiet",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if result.returncode == 0:
            return True
        # 404 / NoSuchKey is normal for dates with no data
        stderr = result.stderr.decode("utf-8", errors="replace")
        if "NoSuchKey" in stderr or "404" in stderr or "does not exist" in stderr:
            logger.debug(f"No data for {dex}/{date_str} (expected)")
        else:
            logger.warning(f"aws s3 cp failed for {dex}/{date_str}: {stderr[:200]}")
        return False
    except subprocess.TimeoutExpired:
        logger.warning(f"Download timed out for {dex}/{date_str}")
        return False
    except Exception as e:
        logger.warning(f"Download error for {dex}/{date_str}: {e}")
        return False


# ── DuckDB processing ──────────────────────────────────────────────────────────

def _process_parquet(local_path: str, dex: str, date_str: str) -> list[dict]:
    """
    Use DuckDB to aggregate fills from a local parquet file.
    Returns list of {address, first_ts, last_ts, volume, trades}.
    """
    try:
        import duckdb  # type: ignore
        conn = duckdb.connect()
        rows = conn.execute(f"""
            SELECT
                address,
                MIN(CAST(timestamp AS VARCHAR))::TEXT AS first_ts,
                MAX(CAST(timestamp AS VARCHAR))::TEXT AS last_ts,
                SUM(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)) AS volume,
                COUNT(*) AS trades
            FROM read_parquet('{local_path}')
            WHERE address IS NOT NULL AND address != ''
            GROUP BY address
        """).fetchall()
        conn.close()
        return [
            {
                "address": r[0],
                "first_ts": r[1],
                "last_ts":  r[2],
                "volume":   float(r[3] or 0),
                "trades":   int(r[4] or 0),
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"DuckDB processing failed for {local_path}: {e}", exc_info=True)
        return []


# ── Upsert ─────────────────────────────────────────────────────────────────────

def _upsert_rows(conn: sqlite3.Connection, rows: list[dict], dex: str, date_str: str):
    """Upsert aggregated rows into user_stats."""
    now_ts = datetime.now(timezone.utc).isoformat()
    for r in rows:
        address  = r["address"]
        vol      = r["volume"]
        trades   = r["trades"]
        row_date = date_str  # Use the file date as the trade date

        # Fetch existing
        existing = conn.execute(
            "SELECT first_hip3_date, last_hip3_date, hip3_volume_usd, "
            "trade_count, primary_dex, dex_volumes, first_seen_dex "
            "FROM user_stats WHERE address = ?",
            (address,),
        ).fetchone()

        if existing:
            dex_vols: dict = json.loads(existing["dex_volumes"] or "{}")
            dex_vols[dex] = dex_vols.get(dex, 0.0) + vol

            first_date = min(existing["first_hip3_date"], row_date) if existing["first_hip3_date"] else row_date
            last_date  = max(existing["last_hip3_date"],  row_date) if existing["last_hip3_date"]  else row_date
            total_vol  = (existing["hip3_volume_usd"] or 0.0) + vol
            total_trd  = (existing["trade_count"] or 0) + trades
            primary    = max(dex_vols, key=dex_vols.get)
            first_dex  = existing["first_seen_dex"] or dex
        else:
            dex_vols   = {dex: vol}
            first_date = row_date
            last_date  = row_date
            total_vol  = vol
            total_trd  = trades
            primary    = dex
            first_dex  = dex

        conn.execute("""
            INSERT OR REPLACE INTO user_stats
                (address, first_hip3_date, last_hip3_date, hip3_volume_usd,
                 trade_count, primary_dex, dex_volumes, first_seen_dex)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            address, first_date, last_date, total_vol,
            total_trd, primary, json.dumps(dex_vols), first_dex,
        ))


# ── Daily new users computation ────────────────────────────────────────────────

def _recompute_daily_new_users(conn: sqlite3.Connection):
    """
    For each address, find the date+dex of their first appearance.
    Aggregate into daily_new_users table.
    """
    logger.info("Recomputing daily_new_users...")
    conn.execute("DELETE FROM daily_new_users")

    rows = conn.execute(
        "SELECT address, first_hip3_date, first_seen_dex FROM user_stats "
        "WHERE first_hip3_date IS NOT NULL AND first_seen_dex IS NOT NULL"
    ).fetchall()

    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        key = (row["first_hip3_date"], row["first_seen_dex"])
        counts[key] = counts.get(key, 0) + 1

    for (d, dex), cnt in counts.items():
        conn.execute(
            "INSERT OR REPLACE INTO daily_new_users (date, dex, new_users) VALUES (?, ?, ?)",
            (d, dex, cnt),
        )
    conn.commit()
    logger.info(f"daily_new_users: {len(counts)} date+dex combos")


# ── Bootstrap / incremental ────────────────────────────────────────────────────

def run_users_bootstrap():
    """Process all unprocessed DEX+date combos. Idempotent."""
    logger.info("run_users_bootstrap: starting")
    conn = _open_db()
    processed = _processed_dates(conn)

    pending = []
    for dex, start in DEX_START.items():
        cur = start
        while cur <= DATA_END:
            date_str = cur.strftime("%Y-%m-%d")
            if (dex, date_str) not in processed:
                pending.append((dex, date_str))
            cur += timedelta(days=1)

    total   = _total_dates()
    done    = total - len(pending)
    logger.info(f"Bootstrap: {done}/{total} already done, {len(pending)} remaining")

    for i, (dex, date_str) in enumerate(pending):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False, prefix=f"hip3_fills_{dex}_{date_str}_") as tmp:
            local_path = tmp.name

        try:
            ok = _download_parquet(dex, date_str, local_path)
            if ok:
                rows = _process_parquet(local_path, dex, date_str)
                if rows:
                    _upsert_rows(conn, rows, dex, date_str)
                    conn.commit()
                    logger.info(f"  [{i+1}/{len(pending)}] {dex}/{date_str}: {len(rows)} addresses")
                else:
                    logger.debug(f"  [{i+1}/{len(pending)}] {dex}/{date_str}: no rows")
            else:
                logger.debug(f"  [{i+1}/{len(pending)}] {dex}/{date_str}: skipped (no file)")
        except Exception as e:
            logger.error(f"  [{i+1}/{len(pending)}] {dex}/{date_str} failed: {e}", exc_info=True)
        finally:
            try:
                os.unlink(local_path)
            except OSError:
                pass

        # Mark processed regardless (even if no data, so we don't retry)
        conn.execute(
            "INSERT OR REPLACE INTO bootstrap_progress (dex, date, processed_at) VALUES (?, ?, ?)",
            (dex, date_str, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

    _recompute_daily_new_users(conn)
    conn.close()
    logger.info("run_users_bootstrap: complete")


def run_users_incremental():
    """Check for any dates beyond last processed and run them."""
    logger.info("run_users_incremental: starting")
    conn = _open_db()
    processed = _processed_dates(conn)

    today = date.today()
    # Look at yesterday (data lags 1 day) and maybe today
    pending = []
    for dex, start in DEX_START.items():
        # Find the latest processed date for this dex
        dex_dates = sorted([d for (dx, d) in processed if dx == dex])
        last_processed = date.fromisoformat(dex_dates[-1]) if dex_dates else start - timedelta(days=1)
        # Process from day after last_processed up to yesterday
        cur = last_processed + timedelta(days=1)
        end = min(today - timedelta(days=1), DATA_END)
        while cur <= end:
            date_str = cur.strftime("%Y-%m-%d")
            if (dex, date_str) not in processed:
                pending.append((dex, date_str))
            cur += timedelta(days=1)

    if not pending:
        logger.info("run_users_incremental: nothing new")
        conn.close()
        return

    logger.info(f"run_users_incremental: {len(pending)} new date+dex combos")

    for dex, date_str in pending:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False, prefix=f"hip3_fills_{dex}_{date_str}_") as tmp:
            local_path = tmp.name

        try:
            ok = _download_parquet(dex, date_str, local_path)
            if ok:
                rows = _process_parquet(local_path, dex, date_str)
                if rows:
                    _upsert_rows(conn, rows, dex, date_str)
                    conn.commit()
                    logger.info(f"  incremental {dex}/{date_str}: {len(rows)} addresses")
        except Exception as e:
            logger.error(f"  incremental {dex}/{date_str} failed: {e}", exc_info=True)
        finally:
            try:
                os.unlink(local_path)
            except OSError:
                pass

        conn.execute(
            "INSERT OR REPLACE INTO bootstrap_progress (dex, date, processed_at) VALUES (?, ?, ?)",
            (dex, date_str, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

    _recompute_daily_new_users(conn)
    conn.close()
    logger.info("run_users_incremental: complete")


# ── Query helpers ──────────────────────────────────────────────────────────────

def _query_summary(conn: sqlite3.Connection) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM user_stats").fetchone()[0]

    by_dex: dict[str, int] = {}
    for dex in DEXES:
        count = conn.execute(
            "SELECT COUNT(*) FROM user_stats WHERE first_seen_dex = ?", (dex,)
        ).fetchone()[0]
        by_dex[dex] = count

    # New users in various periods (based on first_hip3_date)
    today = date.today()
    new_users: dict[str, int] = {}
    for period, days in [("1d", 1), ("7d", 7), ("30d", 30), ("90d", 90)]:
        cutoff = (today - timedelta(days=days)).isoformat()
        count = conn.execute(
            "SELECT COUNT(*) FROM user_stats WHERE first_hip3_date >= ?", (cutoff,)
        ).fetchone()[0]
        new_users[period] = count

    # Bootstrap status
    processed_count = conn.execute("SELECT COUNT(*) FROM bootstrap_progress").fetchone()[0]
    total_dates = _total_dates()
    bootstrap_complete = processed_count >= total_dates

    return {
        "total_unique_users": total,
        "by_dex": by_dex,
        "new_users": new_users,
        "bootstrap_status": {
            "complete": bootstrap_complete,
            "processed_dates": processed_count,
            "total_dates": total_dates,
        },
    }


def _query_timeline(conn: sqlite3.Connection, period: int = 90) -> list[dict]:
    today = date.today()
    cutoff = (today - timedelta(days=period)).isoformat()

    rows = conn.execute(
        "SELECT date, dex, new_users FROM daily_new_users WHERE date >= ? ORDER BY date",
        (cutoff,),
    ).fetchall()

    # Build dict keyed by date
    day_map: dict[str, dict] = {}
    for row in rows:
        d = row["date"]
        if d not in day_map:
            day_map[d] = {"date": d}
            for dex in DEXES:
                day_map[d][dex] = 0
        day_map[d][row["dex"]] = row["new_users"]

    # Fill all days in range
    result = []
    cur = today - timedelta(days=period - 1)
    while cur <= today:
        date_str = cur.strftime("%Y-%m-%d")
        entry = day_map.get(date_str, {"date": date_str})
        for dex in DEXES:
            entry.setdefault(dex, 0)
        result.append(entry)
        cur += timedelta(days=1)
    return result


def _query_top_venues(conn: sqlite3.Connection) -> list[dict]:
    total = conn.execute("SELECT COUNT(*) FROM user_stats").fetchone()[0]
    if total == 0:
        return []
    result = []
    for dex in DEXES:
        count = conn.execute(
            "SELECT COUNT(*) FROM user_stats WHERE first_seen_dex = ?", (dex,)
        ).fetchone()[0]
        result.append({
            "dex":           dex,
            "unique_users":  count,
            "pct":           round(count / total * 100, 1) if total > 0 else 0.0,
        })
    return sorted(result, key=lambda x: x["unique_users"], reverse=True)


# ── UsersCollector ─────────────────────────────────────────────────────────────

class UsersCollector:
    """
    Wraps run_users_bootstrap/incremental for the main.py scheduler.
    Also exposes API query methods reading directly from SQLite.
    """

    def __init__(self):
        self._lock = Lock()
        self.last_updated: str | None = None
        self._bootstrap_thread: threading.Thread | None = None

    # Called by the lifespan startup (runs bootstrap in background if needed)
    def maybe_start_bootstrap(self):
        try:
            conn = _open_db()
            processed = conn.execute("SELECT COUNT(*) FROM bootstrap_progress").fetchone()[0]
            total = _total_dates()
            conn.close()
            if processed < total:
                logger.info(f"Bootstrap incomplete ({processed}/{total}), starting background thread")
                t = threading.Thread(target=self._run_bootstrap_bg, daemon=True, name="users-bootstrap")
                t.start()
                self._bootstrap_thread = t
            else:
                logger.info("Bootstrap already complete")
        except Exception as e:
            logger.error(f"maybe_start_bootstrap error: {e}", exc_info=True)

    def _run_bootstrap_bg(self):
        try:
            run_users_bootstrap()
            with self._lock:
                self.last_updated = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            logger.error(f"Background bootstrap failed: {e}", exc_info=True)

    def collect(self):
        """Called by the hourly scheduler — runs incremental update."""
        try:
            run_users_incremental()
            with self._lock:
                self.last_updated = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            logger.error(f"UsersCollector.collect() failed: {e}", exc_info=True)

    # Legacy method — kept for /api/users backward compat
    def get_data(self) -> dict:
        try:
            conn = _open_db()
            data = _query_summary(conn)
            conn.close()
            return data
        except Exception as e:
            logger.warning(f"UsersCollector.get_data() failed: {e}")
            return {"status": "loading", "message": "Users data not yet available"}

    def get_summary(self) -> dict:
        return self.get_data()

    def get_timeline(self, period: int = 90) -> list[dict]:
        try:
            conn = _open_db()
            data = _query_timeline(conn, period=period)
            conn.close()
            return data
        except Exception as e:
            logger.warning(f"UsersCollector.get_timeline() failed: {e}")
            return []

    def get_top_venues(self) -> list[dict]:
        try:
            conn = _open_db()
            data = _query_top_venues(conn)
            conn.close()
            return data
        except Exception as e:
            logger.warning(f"UsersCollector.get_top_venues() failed: {e}")
            return []
