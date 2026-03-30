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

# Last available date for all DEXes — always 2 days ago
DATA_END = date.today() - timedelta(days=2)

TRADFI_ASSET_CLASSES = ["equity", "commodity", "currency", "index"]

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
            first_seen_dex     TEXT,
            tradfi_vol_usd     REAL DEFAULT 0,
            crypto_vol_usd     REAL DEFAULT 0
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
    # Add new columns to existing DBs that pre-date this schema change
    for col, typedef in [("tradfi_vol_usd", "REAL DEFAULT 0"), ("crypto_vol_usd", "REAL DEFAULT 0")]:
        try:
            conn.execute(f"ALTER TABLE user_stats ADD COLUMN {col} {typedef}")
            conn.commit()
        except Exception:
            pass  # Column already exists — that's fine
    conn.commit()
    return conn


def _processed_dates(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    rows = conn.execute("SELECT dex, date FROM bootstrap_progress").fetchall()
    return {(r["dex"], r["date"]) for r in rows}


def _total_dates() -> int:
    data_end = date.today() - timedelta(days=2)
    total = 0
    for dex, start in DEX_START.items():
        delta = (data_end - start).days + 1
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
    Groups by (address, asset_class) to compute tradfi vs crypto volume.
    Returns list of {address, first_ts, last_ts, volume, trades, tradfi_vol, crypto_vol}.
    """
    try:
        import duckdb  # type: ignore
        conn = duckdb.connect()

        # Check if asset_class column exists
        cols = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{local_path}') LIMIT 0").fetchall()
        col_names = [c[0].lower() for c in cols]
        has_asset_class = "asset_class" in col_names

        if has_asset_class:
            rows = conn.execute(f"""
                WITH base AS (
                    SELECT
                        address,
                        LOWER(COALESCE(asset_class, 'crypto')) AS asset_class,
                        CAST(price AS DOUBLE) * CAST(size AS DOUBLE) AS notional,
                        CAST(timestamp AS VARCHAR)::TEXT AS ts
                    FROM read_parquet('{local_path}')
                    WHERE address IS NOT NULL AND address != ''
                )
                SELECT
                    address,
                    MIN(ts) AS first_ts,
                    MAX(ts) AS last_ts,
                    SUM(notional) AS volume,
                    COUNT(*) AS trades,
                    SUM(CASE WHEN asset_class IN ('equity','commodity','currency','index') THEN notional ELSE 0 END) AS tradfi_vol,
                    SUM(CASE WHEN asset_class = 'crypto' THEN notional ELSE 0 END) AS crypto_vol
                FROM base
                GROUP BY address
            """).fetchall()
        else:
            rows = conn.execute(f"""
                SELECT
                    address,
                    MIN(CAST(timestamp AS VARCHAR))::TEXT AS first_ts,
                    MAX(CAST(timestamp AS VARCHAR))::TEXT AS last_ts,
                    SUM(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)) AS volume,
                    COUNT(*) AS trades,
                    0.0 AS tradfi_vol,
                    SUM(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)) AS crypto_vol
                FROM read_parquet('{local_path}')
                WHERE address IS NOT NULL AND address != ''
                GROUP BY address
            """).fetchall()

        conn.close()
        return [
            {
                "address":    r[0],
                "first_ts":   r[1],
                "last_ts":    r[2],
                "volume":     float(r[3] or 0),
                "trades":     int(r[4] or 0),
                "tradfi_vol": float(r[5] or 0),
                "crypto_vol": float(r[6] or 0),
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
        address    = r["address"]
        vol        = r["volume"]
        trades     = r["trades"]
        tradfi_vol = r.get("tradfi_vol", 0.0)
        crypto_vol = r.get("crypto_vol", 0.0)
        row_date   = date_str  # Use the file date as the trade date

        # Fetch existing
        existing = conn.execute(
            "SELECT first_hip3_date, last_hip3_date, hip3_volume_usd, "
            "trade_count, primary_dex, dex_volumes, first_seen_dex, "
            "COALESCE(tradfi_vol_usd, 0) AS tradfi_vol_usd, "
            "COALESCE(crypto_vol_usd, 0) AS crypto_vol_usd "
            "FROM user_stats WHERE address = ?",
            (address,),
        ).fetchone()

        if existing:
            dex_vols: dict = json.loads(existing["dex_volumes"] or "{}")
            dex_vols[dex] = dex_vols.get(dex, 0.0) + vol

            first_date  = min(existing["first_hip3_date"], row_date) if existing["first_hip3_date"] else row_date
            last_date   = max(existing["last_hip3_date"],  row_date) if existing["last_hip3_date"]  else row_date
            total_vol   = (existing["hip3_volume_usd"] or 0.0) + vol
            total_trd   = (existing["trade_count"] or 0) + trades
            total_tradfi = (existing["tradfi_vol_usd"] or 0.0) + tradfi_vol
            total_crypto = (existing["crypto_vol_usd"] or 0.0) + crypto_vol
            primary     = max(dex_vols, key=dex_vols.get)
            first_dex   = existing["first_seen_dex"] or dex
        else:
            dex_vols     = {dex: vol}
            first_date   = row_date
            last_date    = row_date
            total_vol    = vol
            total_trd    = trades
            total_tradfi = tradfi_vol
            total_crypto = crypto_vol
            primary      = dex
            first_dex    = dex

        conn.execute("""
            INSERT OR REPLACE INTO user_stats
                (address, first_hip3_date, last_hip3_date, hip3_volume_usd,
                 trade_count, primary_dex, dex_volumes, first_seen_dex,
                 tradfi_vol_usd, crypto_vol_usd)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            address, first_date, last_date, total_vol,
            total_trd, primary, json.dumps(dex_vols), first_dex,
            total_tradfi, total_crypto,
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

    data_end = date.today() - timedelta(days=2)
    pending = []
    for dex, start in DEX_START.items():
        cur = start
        while cur <= data_end:
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
    data_end = today - timedelta(days=2)
    # Look at yesterday (data lags 1 day) and maybe today
    pending = []
    for dex, start in DEX_START.items():
        # Find the latest processed date for this dex
        dex_dates = sorted([d for (dx, d) in processed if dx == dex])
        last_processed = date.fromisoformat(dex_dates[-1]) if dex_dates else start - timedelta(days=1)
        # Process from day after last_processed up to data_end
        cur = last_processed + timedelta(days=1)
        end = min(today - timedelta(days=1), data_end)
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


def _query_filter(
    conn: sqlite3.Connection,
    period_days: int,
    min_vol: float,
    tradfi_ratio_min: float,
    venues: list[str],
) -> dict:
    """
    Filter users by various criteria and return aggregate stats.

    period_days:      only users whose first_hip3_date is within last N days
    min_vol:          minimum hip3_volume_usd
    tradfi_ratio_min: minimum tradfi_vol_usd / hip3_volume_usd ratio (0.0-1.0)
    venues:           filter by first_seen_dex (empty = all)
    """
    today = date.today()
    cutoff = (today - timedelta(days=period_days)).isoformat()

    params: list = [cutoff, min_vol]
    where_clauses = [
        "first_hip3_date >= ?",
        "hip3_volume_usd >= ?",
    ]

    if tradfi_ratio_min > 0:
        where_clauses.append(
            "(hip3_volume_usd > 0 AND COALESCE(tradfi_vol_usd, 0) / hip3_volume_usd >= ?)"
        )
        params.append(tradfi_ratio_min)

    if venues:
        placeholders = ",".join(["?" for _ in venues])
        where_clauses.append(f"first_seen_dex IN ({placeholders})")
        params.extend(venues)

    where_sql = " AND ".join(where_clauses)

    total_row = conn.execute(
        f"SELECT COUNT(*) FROM user_stats WHERE {where_sql}", params
    ).fetchone()
    total_matching = total_row[0] if total_row else 0

    # Type A: tradfi ratio > 0.8
    type_a_params = list(params)
    type_a_where = where_sql + " AND hip3_volume_usd > 0 AND COALESCE(tradfi_vol_usd, 0) / hip3_volume_usd > 0.8"
    type_a_row = conn.execute(
        f"SELECT COUNT(*) FROM user_stats WHERE {type_a_where}", type_a_params
    ).fetchone()
    type_a_count = type_a_row[0] if type_a_row else 0

    # Type B: the rest
    type_b_count = total_matching - type_a_count

    # By-dex breakdown
    by_dex: dict[str, int] = {}
    for dex in DEXES:
        dex_params = list(params) + [dex]
        dex_row = conn.execute(
            f"SELECT COUNT(*) FROM user_stats WHERE {where_sql} AND first_seen_dex = ?",
            dex_params,
        ).fetchone()
        by_dex[dex] = dex_row[0] if dex_row else 0

    # Average volume
    avg_row = conn.execute(
        f"SELECT AVG(hip3_volume_usd) FROM user_stats WHERE {where_sql}", params
    ).fetchone()
    avg_volume = float(avg_row[0] or 0) if avg_row else 0.0

    # Sample users: top 20 by volume
    sample_rows = conn.execute(
        f"SELECT address, hip3_volume_usd, first_hip3_date, first_seen_dex, "
        f"COALESCE(tradfi_vol_usd, 0) AS tradfi_vol_usd "
        f"FROM user_stats WHERE {where_sql} ORDER BY hip3_volume_usd DESC LIMIT 20",
        params,
    ).fetchall()
    sample_users = [
        {
            "address":       r["address"],
            "volume":        round(float(r["hip3_volume_usd"] or 0), 2),
            "first_date":    r["first_hip3_date"],
            "first_dex":     r["first_seen_dex"],
            "tradfi_ratio":  round(
                float(r["tradfi_vol_usd"] or 0) / float(r["hip3_volume_usd"] or 1), 4
            ) if (r["hip3_volume_usd"] or 0) > 0 else 0.0,
        }
        for r in sample_rows
    ]

    return {
        "total_matching": total_matching,
        "type_a_count":   type_a_count,
        "type_b_count":   type_b_count,
        "by_dex":         by_dex,
        "avg_volume":     round(avg_volume, 2),
        "sample_users":   sample_users,
    }


def _query_type_breakdown(conn: sqlite3.Connection) -> dict:
    """
    Returns user type distribution:
      type_a = tradfi_vol_usd / hip3_volume_usd > 0.8 (and hip3_volume_usd > 0)
      type_b = the rest
    """
    total_row = conn.execute("SELECT COUNT(*) FROM user_stats").fetchone()
    total = total_row[0] if total_row else 0

    type_a_row = conn.execute(
        "SELECT COUNT(*) FROM user_stats "
        "WHERE hip3_volume_usd > 0 "
        "AND COALESCE(tradfi_vol_usd, 0) / hip3_volume_usd > 0.8"
    ).fetchone()
    type_a = type_a_row[0] if type_a_row else 0
    type_b = total - type_a

    avg_ratio_row = conn.execute(
        "SELECT AVG(COALESCE(tradfi_vol_usd, 0) / hip3_volume_usd) "
        "FROM user_stats WHERE hip3_volume_usd > 0"
    ).fetchone()
    avg_tradfi_ratio = float(avg_ratio_row[0] or 0) if avg_ratio_row else 0.0

    return {
        "type_a":          type_a,
        "type_b":          type_b,
        "type_a_pct":      round(type_a / total * 100, 1) if total > 0 else 0.0,
        "type_b_pct":      round(type_b / total * 100, 1) if total > 0 else 0.0,
        "avg_tradfi_ratio": round(avg_tradfi_ratio, 4),
    }


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

    def get_filter(
        self,
        period_days: int = 30,
        min_vol: float = 0.0,
        tradfi_ratio_min: float = 0.0,
        venues: list[str] | None = None,
    ) -> dict:
        try:
            conn = _open_db()
            data = _query_filter(conn, period_days, min_vol, tradfi_ratio_min, venues or [])
            conn.close()
            return data
        except Exception as e:
            logger.warning(f"UsersCollector.get_filter() failed: {e}")
            return {
                "total_matching": 0,
                "type_a_count": 0,
                "type_b_count": 0,
                "by_dex": {},
                "avg_volume": 0.0,
                "sample_users": [],
            }

    def get_type_breakdown(self) -> dict:
        try:
            conn = _open_db()
            data = _query_type_breakdown(conn)
            conn.close()
            return data
        except Exception as e:
            logger.warning(f"UsersCollector.get_type_breakdown() failed: {e}")
            return {
                "type_a": 0,
                "type_b": 0,
                "type_a_pct": 0.0,
                "type_b_pct": 0.0,
                "avg_tradfi_ratio": 0.0,
            }
