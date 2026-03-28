"""
bootstrap_users.py — One-time script to process ALL historical S3 fills data
and populate users.db with HIP-3 user tracking data.

Usage:
    python bootstrap_users.py [--workers 4] [--reset]

Processes:
    - node_fills/hourly/YYYYMMDD/HH.lz4         (May 25 – Jul 27, 2025)
    - node_fills_by_block/hourly/YYYYMMDD/HH.lz4 (Jul 27, 2025 – now)

Each file is streamed directly from S3 without downloading to disk.
Progress is checkpointed in users_bootstrap_state.json so the script
can resume if interrupted.
"""

import argparse
import io
import json
import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from threading import Lock

import boto3
import lz4.frame
import requests

# ── Configuration ─────────────────────────────────────────────────────────────

BUCKET = "hl-mainnet-node-data"
# node_fills covers May 25 – Jul 27, 2025
NODE_FILLS_PREFIX = "node_fills/hourly"
NODE_FILLS_START = datetime(2025, 5, 25, tzinfo=timezone.utc)
NODE_FILLS_END   = datetime(2025, 7, 27, tzinfo=timezone.utc)
# node_fills_by_block covers Jul 27, 2025 – present
NODE_FILLS_BLOCK_PREFIX = "node_fills_by_block/hourly"
NODE_FILLS_BLOCK_START  = datetime(2025, 7, 27, tzinfo=timezone.utc)

DEXES = ["km", "xyz", "flx", "cash"]
HL_BASE = "https://api.hyperliquid.xyz/info"

DB_PATH = os.environ.get("USERS_DB_PATH", "/data/users.db")
STATE_FILE = "users_bootstrap_state.json"

HIP3_THRESHOLD_MS = 86_400_000   # 1 day in ms — window for Type A classification

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bootstrap_users.log"),
    ],
)
logger = logging.getLogger("bootstrap_users")


# ── HIP-3 ticker discovery ─────────────────────────────────────────────────────

def get_hip3_tickers() -> dict[str, set[str]]:
    """Return {dex: set_of_tickers} for all HIP-3 DEXes."""
    tickers_by_dex: dict[str, set[str]] = {}
    all_tickers: set[str] = set()

    for dex in DEXES:
        try:
            r = requests.post(
                HL_BASE,
                json={"type": "perpDexLimits", "dex": dex},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
            coins = {pair[0] for pair in data.get("coinToOiCap", [])}
            tickers_by_dex[dex] = coins
            all_tickers |= coins
            logger.info(f"  {dex}: {len(coins)} tickers")
        except Exception as e:
            logger.warning(f"  Failed to fetch tickers for {dex}: {e}")
            tickers_by_dex[dex] = set()

    logger.info(f"Total unique HIP-3 tickers: {len(all_tickers)}")
    return tickers_by_dex


def build_coin_to_dex(tickers_by_dex: dict[str, set[str]]) -> dict[str, str]:
    """Map coin → first matching DEX (km takes priority if overlap)."""
    coin_to_dex: dict[str, str] = {}
    for dex in DEXES:
        for coin in tickers_by_dex.get(dex, set()):
            if coin not in coin_to_dex:
                coin_to_dex[coin] = dex
    return coin_to_dex


# ── S3 file enumeration ────────────────────────────────────────────────────────

def enumerate_s3_files(s3_client) -> list[dict]:
    """
    Return all S3 keys to process, ordered chronologically.
    Each entry: {"key": str, "date": YYYYMMDD, "hour": HH, "format": "fills"|"block"}
    """
    files = []

    def iter_prefix(prefix: str, fmt: str):
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, RequestPayer="requester"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # key format: <prefix>/YYYYMMDD/HH.lz4
                parts = key.split("/")
                if len(parts) >= 3 and parts[-1].endswith(".lz4"):
                    date_part = parts[-2]   # YYYYMMDD
                    hour_part = parts[-1].replace(".lz4", "")  # HH
                    if len(date_part) == 8 and hour_part.isdigit():
                        files.append({
                            "key": key,
                            "date": date_part,
                            "hour": int(hour_part),
                            "format": fmt,
                        })

    logger.info("Enumerating node_fills files...")
    iter_prefix(NODE_FILLS_PREFIX, "fills")
    logger.info("Enumerating node_fills_by_block files...")
    iter_prefix(NODE_FILLS_BLOCK_PREFIX, "block")

    files.sort(key=lambda x: (x["date"], x["hour"]))
    logger.info(f"Total files to process: {len(files)}")
    return files


# ── State management ───────────────────────────────────────────────────────────

def load_state() -> dict:
    if Path(STATE_FILE).exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {
        "files_processed": [],
        "last_processed_key": None,
        "total_files_processed": 0,
        "total_users_seen": 0,
        "total_hip3_users": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": None,
    }


def save_state(state: dict):
    state["last_updated"] = datetime.now(timezone.utc).isoformat()
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── SQLite setup ───────────────────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            address          TEXT PRIMARY KEY,
            first_ever_ts    INTEGER NOT NULL,
            first_hip3_ts    INTEGER,
            total_hl_volume  REAL DEFAULT 0,
            hip3_volume      REAL DEFAULT 0,
            type             TEXT,        -- 'A', 'B', or NULL (not a HIP-3 user yet)
            primary_dex      TEXT,        -- dex with most volume
            km_volume        REAL DEFAULT 0,
            xyz_volume       REAL DEFAULT 0,
            flx_volume       REAL DEFAULT 0,
            cash_volume      REAL DEFAULT 0,
            km_first_ts      INTEGER,
            xyz_first_ts     INTEGER,
            flx_first_ts     INTEGER,
            cash_first_ts    INTEGER,
            tradfi_pct       REAL,        -- hip3_volume / total_hl_volume * 100
            updated_at       INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_users_type         ON users(type);
        CREATE INDEX IF NOT EXISTS idx_users_first_hip3   ON users(first_hip3_ts);
        CREATE INDEX IF NOT EXISTS idx_users_first_ever   ON users(first_ever_ts);
        CREATE INDEX IF NOT EXISTS idx_users_primary_dex  ON users(primary_dex);

        CREATE TABLE IF NOT EXISTS bootstrap_meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    conn.commit()
    return conn


# ── In-memory aggregation (thread-safe) ───────────────────────────────────────

class UserAggregator:
    """
    Thread-safe accumulator for user data.
    Holds everything in memory during processing, flushes to SQLite periodically.
    """

    def __init__(self, coin_to_dex: dict[str, str]):
        self.coin_to_dex = coin_to_dex
        self.lock = Lock()

        # address -> int (ms timestamp of first ever HL fill)
        self.all_first_ts: dict[str, int] = {}
        # address -> {dex -> {"first_ts": int, "volume": float}}
        self.hip3_data: dict[str, dict[str, dict]] = {}
        # address -> float (total HL volume across all fills)
        self.total_volume: dict[str, float] = {}

    def process_fills(self, fills: list[tuple[str, dict]]):
        """
        Process a batch of (address, fill_object) tuples.
        fill_object keys: coin, px, sz, side, time (ms), fee, ...
        """
        with self.lock:
            for address, fill in fills:
                ts = fill.get("time")
                coin = fill.get("coin", "")
                if ts is None:
                    continue

                # Track first ever HL fill
                if address not in self.all_first_ts or ts < self.all_first_ts[address]:
                    self.all_first_ts[address] = ts

                # Accumulate total volume (px * sz)
                try:
                    vol_usd = float(fill.get("px", 0)) * float(fill.get("sz", 0))
                except (TypeError, ValueError):
                    vol_usd = 0.0
                self.total_volume[address] = self.total_volume.get(address, 0.0) + vol_usd

                # If this coin is a HIP-3 asset, track per-dex stats
                dex = self.coin_to_dex.get(coin)
                if dex is None:
                    continue

                if address not in self.hip3_data:
                    self.hip3_data[address] = {}
                if dex not in self.hip3_data[address]:
                    self.hip3_data[address][dex] = {"first_ts": ts, "volume": 0.0}

                entry = self.hip3_data[address][dex]
                if ts < entry["first_ts"]:
                    entry["first_ts"] = ts
                entry["volume"] += vol_usd

    def user_count(self) -> int:
        return len(self.all_first_ts)

    def hip3_user_count(self) -> int:
        return len(self.hip3_data)

    def flush_to_db(self, conn: sqlite3.Connection):
        """Compute final classifications and write everything to SQLite."""
        logger.info(f"Flushing {len(self.all_first_ts):,} users to DB...")
        now_ms = int(time.time() * 1000)
        rows = []

        with self.lock:
            for address, first_ever_ts in self.all_first_ts.items():
                dex_data = self.hip3_data.get(address)
                total_hl_vol = self.total_volume.get(address, 0.0)

                if dex_data is None:
                    # Non-HIP-3 user — still record first_ever_ts for future reference
                    rows.append((
                        address, first_ever_ts, None,
                        total_hl_vol, 0.0,
                        None, None,
                        0.0, 0.0, 0.0, 0.0,
                        None, None, None, None,
                        None, now_ms,
                    ))
                    continue

                # Compute aggregate HIP-3 stats
                first_hip3_ts = min(v["first_ts"] for v in dex_data.values())
                hip3_vol = sum(v["volume"] for v in dex_data.values())

                # Classification
                if first_hip3_ts <= first_ever_ts + HIP3_THRESHOLD_MS:
                    user_type = "A"  # TradFi pure: first HIP-3 fill within 1d of first HL fill
                else:
                    user_type = "B"  # Existing HL user adopted HIP-3

                # Primary DEX (most volume)
                primary_dex = max(dex_data.keys(), key=lambda d: dex_data[d]["volume"])

                # Per-DEX columns
                km_vol   = dex_data.get("km",   {}).get("volume", 0.0)
                xyz_vol  = dex_data.get("xyz",  {}).get("volume", 0.0)
                flx_vol  = dex_data.get("flx",  {}).get("volume", 0.0)
                cash_vol = dex_data.get("cash", {}).get("volume", 0.0)

                km_first   = dex_data.get("km",   {}).get("first_ts")
                xyz_first  = dex_data.get("xyz",  {}).get("first_ts")
                flx_first  = dex_data.get("flx",  {}).get("first_ts")
                cash_first = dex_data.get("cash", {}).get("first_ts")

                tradfi_pct = (hip3_vol / total_hl_vol * 100) if total_hl_vol > 0 else None

                rows.append((
                    address, first_ever_ts, first_hip3_ts,
                    total_hl_vol, hip3_vol,
                    user_type, primary_dex,
                    km_vol, xyz_vol, flx_vol, cash_vol,
                    km_first, xyz_first, flx_first, cash_first,
                    tradfi_pct, now_ms,
                ))

        conn.executemany("""
            INSERT OR REPLACE INTO users (
                address, first_ever_ts, first_hip3_ts,
                total_hl_volume, hip3_volume,
                type, primary_dex,
                km_volume, xyz_volume, flx_volume, cash_volume,
                km_first_ts, xyz_first_ts, flx_first_ts, cash_first_ts,
                tradfi_pct, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        conn.commit()
        logger.info(f"  Flushed {len(rows):,} rows ({sum(1 for r in rows if r[6] is not None):,} HIP-3 users)")


# ── S3 file processing ─────────────────────────────────────────────────────────

def stream_s3_file(s3_client, key: str) -> bytes:
    """Download an S3 object with requester-pays and return raw bytes."""
    obj = s3_client.get_object(Bucket=BUCKET, Key=key, RequestPayer="requester")
    return obj["Body"].read()


def parse_fills_format(raw_bytes: bytes) -> list[tuple[str, dict]]:
    """
    Parse node_fills format: each line is [address, fill_object]
    Returns list of (address, fill_dict).
    """
    fills = []
    text = lz4.frame.decompress(raw_bytes).decode("utf-8", errors="replace")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            if isinstance(record, list) and len(record) >= 2:
                address, fill_obj = record[0], record[1]
                if isinstance(address, str) and isinstance(fill_obj, dict):
                    fills.append((address, fill_obj))
        except (json.JSONDecodeError, IndexError):
            continue
    return fills


def parse_block_format(raw_bytes: bytes) -> list[tuple[str, dict]]:
    """
    Parse node_fills_by_block format: each line is {"events": [[address, fill_object], ...], ...}
    Returns list of (address, fill_dict).
    """
    fills = []
    text = lz4.frame.decompress(raw_bytes).decode("utf-8", errors="replace")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            events = record.get("events", [])
            for event in events:
                if isinstance(event, list) and len(event) >= 2:
                    address, fill_obj = event[0], event[1]
                    if isinstance(address, str) and isinstance(fill_obj, dict):
                        fills.append((address, fill_obj))
        except (json.JSONDecodeError, IndexError):
            continue
    return fills


def process_file(s3_client, file_info: dict, aggregator: UserAggregator) -> int:
    """Download, decompress, parse, and aggregate one S3 file. Returns fill count."""
    key = file_info["key"]
    fmt = file_info["format"]
    try:
        raw = stream_s3_file(s3_client, key)
        if fmt == "fills":
            fills = parse_fills_format(raw)
        else:
            fills = parse_block_format(raw)
        aggregator.process_fills(fills)
        return len(fills)
    except Exception as e:
        logger.error(f"Failed to process {key}: {e}")
        return 0


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Bootstrap HIP-3 user tracking DB from S3")
    parser.add_argument("--workers", type=int, default=4, help="Parallel S3 download workers")
    parser.add_argument("--reset", action="store_true", help="Reset state and start fresh")
    parser.add_argument("--flush-every", type=int, default=500, help="Flush to DB every N files")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("HIP-3 User Bootstrap")
    logger.info(f"  DB:       {DB_PATH}")
    logger.info(f"  Workers:  {args.workers}")
    logger.info(f"  State:    {STATE_FILE}")
    logger.info("=" * 60)

    # Reset state if requested
    if args.reset and Path(STATE_FILE).exists():
        Path(STATE_FILE).unlink()
        logger.info("State file reset.")

    # Load or init state
    state = load_state()

    # Init DB
    conn = init_db(DB_PATH)

    # Fetch HIP-3 tickers
    logger.info("Fetching HIP-3 tickers from Hyperliquid API...")
    tickers_by_dex = get_hip3_tickers()
    coin_to_dex = build_coin_to_dex(tickers_by_dex)
    logger.info(f"Ticker map built: {len(coin_to_dex)} unique coins")

    # Save ticker list in DB meta
    conn.execute(
        "INSERT OR REPLACE INTO bootstrap_meta (key, value) VALUES (?, ?)",
        ("hip3_tickers", json.dumps({dex: list(tickers) for dex, tickers in tickers_by_dex.items()})),
    )
    conn.commit()

    # Init S3 client (boto3 uses env vars AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION)
    s3 = boto3.client("s3")

    # Enumerate all files
    logger.info("Enumerating S3 files...")
    all_files = enumerate_s3_files(s3)

    # Filter out already-processed files
    processed_keys = set(state.get("files_processed", []))
    remaining = [f for f in all_files if f["key"] not in processed_keys]
    logger.info(f"Already processed: {len(processed_keys):,} files")
    logger.info(f"Remaining:         {len(remaining):,} files")

    if not remaining:
        logger.info("Nothing to do — all files already processed.")
        return

    # Load existing user data from DB into memory aggregator
    aggregator = UserAggregator(coin_to_dex)
    logger.info("Loading existing user data from DB into memory...")
    cursor = conn.execute(
        "SELECT address, first_ever_ts, first_hip3_ts, total_hl_volume, hip3_volume, "
        "km_volume, xyz_volume, flx_volume, cash_volume, "
        "km_first_ts, xyz_first_ts, flx_first_ts, cash_first_ts FROM users"
    )
    for row in cursor:
        (address, first_ever_ts, first_hip3_ts, total_hl_vol, hip3_vol,
         km_vol, xyz_vol, flx_vol, cash_vol,
         km_first, xyz_first, flx_first, cash_first) = row

        aggregator.all_first_ts[address] = first_ever_ts
        aggregator.total_volume[address] = total_hl_vol or 0.0

        if first_hip3_ts is not None:
            aggregator.hip3_data[address] = {}
            for dex, vol, first_ts in [
                ("km",   km_vol,   km_first),
                ("xyz",  xyz_vol,  xyz_first),
                ("flx",  flx_vol,  flx_first),
                ("cash", cash_vol, cash_first),
            ]:
                if first_ts is not None:
                    aggregator.hip3_data[address][dex] = {"first_ts": first_ts, "volume": vol or 0.0}

    logger.info(f"Loaded {len(aggregator.all_first_ts):,} existing users")

    # Process files with thread pool
    t_start = time.time()
    files_done = 0
    total_fills = 0
    log_interval = 100
    flush_every = args.flush_every
    newly_processed = []

    logger.info(f"Starting processing with {args.workers} workers...")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_file, s3, f, aggregator): f
            for f in remaining
        }

        for future in as_completed(futures):
            file_info = futures[future]
            try:
                fill_count = future.result()
                total_fills += fill_count
                newly_processed.append(file_info["key"])
            except Exception as e:
                logger.error(f"Worker error for {file_info['key']}: {e}")

            files_done += 1

            if files_done % log_interval == 0:
                elapsed = time.time() - t_start
                rate = files_done / elapsed
                eta_s = (len(remaining) - files_done) / rate if rate > 0 else 0
                eta_str = str(timedelta(seconds=int(eta_s)))
                logger.info(
                    f"Progress: {files_done:,}/{len(remaining):,} files "
                    f"| {aggregator.user_count():,} users "
                    f"| {aggregator.hip3_user_count():,} HIP-3 users "
                    f"| {total_fills:,} fills "
                    f"| {rate:.1f} files/s | ETA {eta_str}"
                )

            # Periodic flush to DB and state save
            if files_done % flush_every == 0:
                aggregator.flush_to_db(conn)
                state["files_processed"] = list(processed_keys | set(newly_processed))
                state["total_files_processed"] = len(state["files_processed"])
                state["total_users_seen"] = aggregator.user_count()
                state["total_hip3_users"] = aggregator.hip3_user_count()
                state["last_processed_key"] = newly_processed[-1] if newly_processed else None
                save_state(state)
                logger.info(f"  Checkpoint saved at {files_done:,} files")

    # Final flush
    logger.info("Final flush to DB...")
    aggregator.flush_to_db(conn)

    # Update bootstrap metadata
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.executemany(
        "INSERT OR REPLACE INTO bootstrap_meta (key, value) VALUES (?, ?)",
        [
            ("bootstrap_completed_at", now_iso),
            ("total_files_processed", str(len(processed_keys) + files_done)),
            ("total_users", str(aggregator.user_count())),
            ("total_hip3_users", str(aggregator.hip3_user_count())),
        ],
    )
    conn.commit()

    # Final state save
    state["files_processed"] = list(processed_keys | set(newly_processed))
    state["total_files_processed"] = len(state["files_processed"])
    state["total_users_seen"] = aggregator.user_count()
    state["total_hip3_users"] = aggregator.hip3_user_count()
    state["bootstrap_completed_at"] = now_iso
    save_state(state)

    elapsed = time.time() - t_start
    logger.info("=" * 60)
    logger.info("Bootstrap complete!")
    logger.info(f"  Files processed : {files_done:,}")
    logger.info(f"  Total fills     : {total_fills:,}")
    logger.info(f"  Total users     : {aggregator.user_count():,}")
    logger.info(f"  HIP-3 users     : {aggregator.hip3_user_count():,}")
    logger.info(f"  Elapsed         : {timedelta(seconds=int(elapsed))}")
    logger.info(f"  DB              : {DB_PATH}")
    logger.info("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
