"""
schedulers/users.py — Incremental HIP-3 user tracking scheduler.

Runs every 1 hour, processes the latest S3 hourly file, and updates users.db.
Exposes UsersCollector with get_data() and collect() methods for the API layer.
"""

import io
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from threading import Lock

import boto3
import lz4.frame

from schedulers import hl_post

logger = logging.getLogger("kinetiq.users")

BUCKET = "hl-mainnet-node-data"
DEXES = ["km", "xyz", "flx", "cash"]
DB_PATH = os.environ.get("USERS_DB_PATH", "/data/users.db")

# node_fills_by_block is the current format (Jul 27, 2025 → present)
NODE_FILLS_BLOCK_PREFIX = "node_fills_by_block/hourly"

# Lookback window for "within 1 day" Type A classification
HIP3_THRESHOLD_MS = 86_400_000

# Period thresholds in days
PERIODS = {"1d": 1, "7d": 7, "30d": 30, "90d": 90}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_hip3_tickers() -> dict[str, set[str]]:
    """Fetch current HIP-3 tickers from the Hyperliquid API."""
    tickers_by_dex: dict[str, set[str]] = {}
    for dex in DEXES:
        data = hl_post({"type": "perpDexLimits", "dex": dex}, desc=f"perpDexLimits/{dex}")
        if data:
            coins = {pair[0] for pair in data.get("coinToOiCap", [])}
            tickers_by_dex[dex] = coins
        else:
            tickers_by_dex[dex] = set()
    return tickers_by_dex


def _build_coin_to_dex(tickers_by_dex: dict[str, set[str]]) -> dict[str, str]:
    coin_to_dex: dict[str, str] = {}
    for dex in DEXES:
        for coin in tickers_by_dex.get(dex, set()):
            if coin not in coin_to_dex:
                coin_to_dex[coin] = dex
    return coin_to_dex


def _open_db() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    # Ensure schema exists (bootstrap may not have run yet)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            address          TEXT PRIMARY KEY,
            first_ever_ts    INTEGER NOT NULL,
            first_hip3_ts    INTEGER,
            total_hl_volume  REAL DEFAULT 0,
            hip3_volume      REAL DEFAULT 0,
            type             TEXT,
            primary_dex      TEXT,
            km_volume        REAL DEFAULT 0,
            xyz_volume       REAL DEFAULT 0,
            flx_volume       REAL DEFAULT 0,
            cash_volume      REAL DEFAULT 0,
            km_first_ts      INTEGER,
            xyz_first_ts     INTEGER,
            flx_first_ts     INTEGER,
            cash_first_ts    INTEGER,
            tradfi_pct       REAL,
            updated_at       INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_users_type        ON users(type);
        CREATE INDEX IF NOT EXISTS idx_users_first_hip3  ON users(first_hip3_ts);
        CREATE INDEX IF NOT EXISTS idx_users_first_ever  ON users(first_ever_ts);
        CREATE INDEX IF NOT EXISTS idx_users_primary_dex ON users(primary_dex);
        CREATE TABLE IF NOT EXISTS bootstrap_meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    conn.commit()
    return conn


def _s3_key_for_hour(dt: datetime) -> str:
    return f"{NODE_FILLS_BLOCK_PREFIX}/{dt.strftime('%Y%m%d')}/{dt.strftime('%H').zfill(2)}.lz4"


def _download_s3_file(s3_client, key: str) -> bytes | None:
    try:
        obj = s3_client.get_object(Bucket=BUCKET, Key=key, RequestPayer="requester")
        return obj["Body"].read()
    except Exception as e:
        logger.warning(f"S3 download failed for {key}: {e}")
        return None


def _parse_block_format(raw_bytes: bytes) -> list[tuple[str, dict]]:
    fills = []
    text = lz4.frame.decompress(raw_bytes).decode("utf-8", errors="replace")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            for event in record.get("events", []):
                if isinstance(event, list) and len(event) >= 2:
                    address, fill_obj = event[0], event[1]
                    if isinstance(address, str) and isinstance(fill_obj, dict):
                        fills.append((address, fill_obj))
        except (json.JSONDecodeError, IndexError):
            continue
    return fills


def _upsert_user(
    conn: sqlite3.Connection,
    address: str,
    first_ever_ts: int,
    first_hip3_ts: int | None,
    total_hl_vol: float,
    hip3_vol: float,
    dex_volumes: dict[str, float],
    dex_first_ts: dict[str, int | None],
    coin_to_dex: dict[str, str],
    now_ms: int,
):
    """Insert or merge a user record in the DB."""
    # Fetch existing row
    row = conn.execute(
        "SELECT first_ever_ts, first_hip3_ts, total_hl_volume, hip3_volume, "
        "km_volume, xyz_volume, flx_volume, cash_volume, "
        "km_first_ts, xyz_first_ts, flx_first_ts, cash_first_ts "
        "FROM users WHERE address = ?",
        (address,),
    ).fetchone()

    if row:
        merged_first_ever = min(row["first_ever_ts"], first_ever_ts)
        merged_total_hl   = (row["total_hl_volume"] or 0) + total_hl_vol
        merged_hip3_vol   = (row["hip3_volume"]   or 0) + hip3_vol
        merged_hip3_first = (
            min(row["first_hip3_ts"], first_hip3_ts)
            if row["first_hip3_ts"] and first_hip3_ts
            else row["first_hip3_ts"] or first_hip3_ts
        )
        merged_dex_vols = {
            "km":   (row["km_volume"]   or 0) + dex_volumes.get("km",   0),
            "xyz":  (row["xyz_volume"]  or 0) + dex_volumes.get("xyz",  0),
            "flx":  (row["flx_volume"]  or 0) + dex_volumes.get("flx",  0),
            "cash": (row["cash_volume"] or 0) + dex_volumes.get("cash", 0),
        }
        merged_dex_first = {
            "km":   _min_ts(row["km_first_ts"],   dex_first_ts.get("km")),
            "xyz":  _min_ts(row["xyz_first_ts"],  dex_first_ts.get("xyz")),
            "flx":  _min_ts(row["flx_first_ts"],  dex_first_ts.get("flx")),
            "cash": _min_ts(row["cash_first_ts"], dex_first_ts.get("cash")),
        }
    else:
        merged_first_ever = first_ever_ts
        merged_total_hl   = total_hl_vol
        merged_hip3_vol   = hip3_vol
        merged_hip3_first = first_hip3_ts
        merged_dex_vols   = {d: dex_volumes.get(d, 0) for d in DEXES}
        merged_dex_first  = {d: dex_first_ts.get(d) for d in DEXES}

    # Recompute classification
    user_type = None
    primary_dex = None
    tradfi_pct = None
    if merged_hip3_first is not None:
        if merged_hip3_first <= merged_first_ever + HIP3_THRESHOLD_MS:
            user_type = "A"
        else:
            user_type = "B"
        dex_with_vol = {d: v for d, v in merged_dex_vols.items() if v > 0}
        primary_dex = max(dex_with_vol, key=dex_with_vol.get) if dex_with_vol else None
        tradfi_pct = (merged_hip3_vol / merged_total_hl * 100) if merged_total_hl > 0 else None

    conn.execute("""
        INSERT OR REPLACE INTO users (
            address, first_ever_ts, first_hip3_ts,
            total_hl_volume, hip3_volume,
            type, primary_dex,
            km_volume, xyz_volume, flx_volume, cash_volume,
            km_first_ts, xyz_first_ts, flx_first_ts, cash_first_ts,
            tradfi_pct, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        address, merged_first_ever, merged_hip3_first,
        merged_total_hl, merged_hip3_vol,
        user_type, primary_dex,
        merged_dex_vols["km"], merged_dex_vols["xyz"],
        merged_dex_vols["flx"], merged_dex_vols["cash"],
        merged_dex_first["km"], merged_dex_first["xyz"],
        merged_dex_first["flx"], merged_dex_first["cash"],
        tradfi_pct, now_ms,
    ))


def _min_ts(a: int | None, b: int | None) -> int | None:
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


# ── Query helpers ──────────────────────────────────────────────────────────────

def _query_summary(conn: sqlite3.Connection) -> dict:
    """Compute summary stats from the users table."""
    now_ms = int(time.time() * 1000)

    # Overall counts
    total = conn.execute("SELECT COUNT(*) FROM users WHERE type IS NOT NULL").fetchone()[0]
    type_a = conn.execute("SELECT COUNT(*) FROM users WHERE type='A'").fetchone()[0]
    type_b = conn.execute("SELECT COUNT(*) FROM users WHERE type='B'").fetchone()[0]

    # TradFi purity: users where hip3_volume >= 80% of total_hl_volume
    tradfi_pure = conn.execute(
        "SELECT COUNT(*) FROM users WHERE type IS NOT NULL AND tradfi_pct >= 80"
    ).fetchone()[0]
    tradfi_pct = round(tradfi_pure / total * 100, 1) if total > 0 else 0

    # By venue
    by_venue = {}
    for dex in DEXES:
        col = f"{dex}_first_ts"
        count = conn.execute(
            f"SELECT COUNT(*) FROM users WHERE {col} IS NOT NULL"
        ).fetchone()[0]
        by_venue[dex] = count

    # By period — new HIP-3 users (first_hip3_ts in window)
    by_period = {}
    for period, days in PERIODS.items():
        cutoff_ms = now_ms - days * 86_400_000
        row = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN type='A' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN type='B' THEN 1 ELSE 0 END) "
            "FROM users WHERE first_hip3_ts >= ?",
            (cutoff_ms,),
        ).fetchone()
        by_period[period] = {
            "total": row[0] or 0,
            "type_a": row[1] or 0,
            "type_b": row[2] or 0,
        }

    # Bootstrap date
    bootstrap_row = conn.execute(
        "SELECT value FROM bootstrap_meta WHERE key='bootstrap_completed_at'"
    ).fetchone()
    bootstrap_date = bootstrap_row[0][:10] if bootstrap_row else "unknown"

    return {
        "total_hip3_users": total,
        "type_a": type_a,
        "type_b": type_b,
        "tradfi_pure_users": tradfi_pure,
        "tradfi_pct": tradfi_pct,
        "by_venue": by_venue,
        "by_period": by_period,
        "bootstrap_date": bootstrap_date,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


def _query_timeline(conn: sqlite3.Connection, days: int = 90) -> list[dict]:
    """Return daily new HIP-3 user counts for the last N days."""
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - days * 86_400_000

    rows = conn.execute(
        "SELECT first_hip3_ts, type FROM users "
        "WHERE first_hip3_ts IS NOT NULL AND first_hip3_ts >= ?",
        (cutoff_ms,),
    ).fetchall()

    # Bucket by date
    day_counts: dict[str, dict] = {}
    for row in rows:
        ts_ms = row[0]
        user_type = row[1]
        date_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        if date_str not in day_counts:
            day_counts[date_str] = {"date": date_str, "total": 0, "type_a": 0, "type_b": 0}
        day_counts[date_str]["total"] += 1
        if user_type == "A":
            day_counts[date_str]["type_a"] += 1
        elif user_type == "B":
            day_counts[date_str]["type_b"] += 1

    # Fill in missing days with zeros
    result = []
    for i in range(days):
        dt = datetime.now(timezone.utc) - timedelta(days=days - 1 - i)
        date_str = dt.strftime("%Y-%m-%d")
        result.append(day_counts.get(date_str, {"date": date_str, "total": 0, "type_a": 0, "type_b": 0}))

    return result


# ── UsersCollector ─────────────────────────────────────────────────────────────

class UsersCollector:
    """
    Incremental collector for HIP-3 user data.
    Processes the most recent S3 hourly file every collect() call.
    """

    def __init__(self):
        self._lock = Lock()
        self._summary: dict = {}
        self._timeline: list = []
        self.last_updated: str | None = None
        self._last_processed_key: str | None = None

    def collect(self):
        """Process the latest S3 hourly file and update the DB."""
        logger.info("UsersCollector: starting incremental collection")
        try:
            # Refresh ticker list
            tickers_by_dex = _get_hip3_tickers()
            coin_to_dex = _build_coin_to_dex(tickers_by_dex)

            s3 = boto3.client("s3")
            conn = _open_db()
            now_ms = int(time.time() * 1000)

            # Try last 3 hours in case the most recent file isn't available yet
            processed = False
            now_utc = datetime.now(timezone.utc)
            for hours_back in range(1, 4):
                target_hour = now_utc - timedelta(hours=hours_back)
                key = _s3_key_for_hour(target_hour)

                if key == self._last_processed_key:
                    continue  # Already processed this file

                raw = _download_s3_file(s3, key)
                if raw is None:
                    continue

                fills = _parse_block_format(raw)
                logger.info(f"  Processing {key}: {len(fills):,} fills")

                # Aggregate fills in memory then upsert
                user_fills: dict[str, dict] = {}
                for address, fill in fills:
                    ts = fill.get("time")
                    coin = fill.get("coin", "")
                    if ts is None:
                        continue
                    try:
                        vol_usd = float(fill.get("px", 0)) * float(fill.get("sz", 0))
                    except (TypeError, ValueError):
                        vol_usd = 0.0

                    if address not in user_fills:
                        user_fills[address] = {
                            "first_ever_ts": ts,
                            "total_hl_vol": 0.0,
                            "first_hip3_ts": None,
                            "hip3_vol": 0.0,
                            "dex_volumes": {d: 0.0 for d in DEXES},
                            "dex_first_ts": {d: None for d in DEXES},
                        }

                    u = user_fills[address]
                    u["first_ever_ts"] = min(u["first_ever_ts"], ts)
                    u["total_hl_vol"] += vol_usd

                    dex = coin_to_dex.get(coin)
                    if dex:
                        if u["first_hip3_ts"] is None or ts < u["first_hip3_ts"]:
                            u["first_hip3_ts"] = ts
                        u["hip3_vol"] += vol_usd
                        u["dex_volumes"][dex] += vol_usd
                        if u["dex_first_ts"][dex] is None or ts < u["dex_first_ts"][dex]:
                            u["dex_first_ts"][dex] = ts

                # Upsert all users
                for address, data in user_fills.items():
                    _upsert_user(
                        conn, address,
                        data["first_ever_ts"],
                        data["first_hip3_ts"],
                        data["total_hl_vol"],
                        data["hip3_vol"],
                        data["dex_volumes"],
                        data["dex_first_ts"],
                        coin_to_dex,
                        now_ms,
                    )

                conn.commit()
                self._last_processed_key = key
                processed = True
                logger.info(f"  Upserted {len(user_fills):,} users from {key}")
                break

            if not processed:
                logger.info("  No new S3 files to process this cycle")

            # Refresh cached summary
            summary = _query_summary(conn)
            timeline = _query_timeline(conn, days=90)
            conn.close()

            with self._lock:
                self._summary = summary
                self._timeline = timeline
                self.last_updated = datetime.now(timezone.utc).isoformat()

            logger.info(
                f"UsersCollector: done — "
                f"{summary['total_hip3_users']:,} HIP-3 users "
                f"(A={summary['type_a']:,}, B={summary['type_b']:,})"
            )

        except Exception as e:
            logger.error(f"UsersCollector collect() failed: {e}", exc_info=True)

    def get_data(self) -> dict:
        """Return cached summary data for the API."""
        with self._lock:
            if not self._summary:
                # Try to read from DB directly if collect() hasn't run yet
                try:
                    conn = _open_db()
                    summary = _query_summary(conn)
                    conn.close()
                    return summary
                except Exception as e:
                    logger.warning(f"get_data(): DB read failed: {e}")
                    return {"status": "loading", "message": "Users data not yet available"}
            return dict(self._summary)

    def get_timeline(self, days: int = 90) -> list[dict]:
        """Return daily new user timeline."""
        with self._lock:
            if self._timeline:
                return list(self._timeline)
        # Fallback: query DB directly
        try:
            conn = _open_db()
            timeline = _query_timeline(conn, days=days)
            conn.close()
            return timeline
        except Exception as e:
            logger.warning(f"get_timeline(): DB read failed: {e}")
            return []
