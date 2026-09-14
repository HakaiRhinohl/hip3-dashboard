"""
Exact per-DEX fee accounting, ingested from the Hydromancer reservoir.

Every other source we have is indirect. `clearinghouseState` is a balance that
moves with more than fee accrual, so the watermark built on it drifted to ~$9.1M
against ~$339K. The `referral` endpoint is a cumulative counter scoped to a
builder address with no DEX dimension at all, so it mixes Markets' own DEX with
whatever else those apps route. Neither can answer "what did Markets earn on
Markets".

The reservoir can: its fills carry `dex`, `builder`, `builder_fee` and
`deployer_fee` per fill, so both sides are a straight sum with an exact scope.

Two things about the data that the aggregation depends on:
  - `builder_fills/` is just the subset of `all/` that carries a builder. The
    builder_fee totals match, but deployer_fee does NOT -- roughly 2.4x lower on
    the subset -- so everything is read from `all/`.
  - Rows are per fill, so both sides of a trade appear. Fees are per fill and
    sum correctly; notional is therefore counted from both sides and is only
    used for rate context, never as venue volume.

Aggregates are stored per (date, dex, builder) without classifying anyone, so
who counts as Markets is decided at read time and the raw history survives a
change of mind. It also means we can list every builder touching km/mkts rather
than discovering them by accident.
"""

import logging
import os
import sqlite3
import subprocess
import tempfile
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger("kinetiq.reservoir")

DB_PATH = os.environ.get("RESERVOIR_DB_PATH", "/data/reservoir_fees.db")
S3_BUCKET = "hydromancer-reservoir"
S3_REGION = "ap-northeast-1"

# The two namespaces Markets has traded under, with the range each one covers.
# Outside these the reservoir has no partition and the day is marked empty.
DEX_RANGES = {
    "km":   (date(2026, 1, 12), date(2026, 6, 17)),
    "mkts": (date(2026, 7, 1), None),  # None = up to yesterday
}

# Days ingested per collection cycle. The backfill is ~230 days; this keeps any
# single cycle bounded instead of blocking startup on a full download.
DAYS_PER_CYCLE = int(os.environ.get("RESERVOIR_DAYS_PER_CYCLE", "12"))


def _db() -> sqlite3.Connection:
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dex_daily (
            date TEXT NOT NULL, dex TEXT NOT NULL,
            fills INTEGER, notional REAL, deployer_fee REAL, builder_fee REAL,
            PRIMARY KEY (date, dex)
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dex_builder_daily (
            date TEXT NOT NULL, dex TEXT NOT NULL, builder TEXT NOT NULL,
            fills INTEGER, notional REAL, builder_fee REAL,
            PRIMARY KEY (date, dex, builder)
        )""")
    # status: 'ok' when ingested, 'empty' when the partition has no file.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingested (
            date TEXT NOT NULL, dex TEXT NOT NULL, status TEXT, at TEXT,
            PRIMARY KEY (date, dex)
        )""")
    conn.commit()
    return conn


def _download(dex: str, day: str, dest: str) -> str:
    """'ok', 'empty' when the partition does not exist, or 'error'."""
    key = f"s3://{S3_BUCKET}/by_dex/{dex}/fills/perp/all/date={day}/fills.parquet"
    try:
        p = subprocess.run(
            ["aws", "s3", "cp", key, dest, "--region", S3_REGION,
             "--request-payer", "requester", "--only-show-errors"],
            capture_output=True, text=True, timeout=300,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"{dex}/{day}: download timed out")
        return "error"
    if p.returncode == 0:
        return "ok"
    err = (p.stderr or "").lower()
    if "not exist" in err or "nosuchkey" in err or "404" in err:
        return "empty"
    logger.warning(f"{dex}/{day}: download failed: {(p.stderr or '')[:160]}")
    return "error"


def _aggregate(conn: sqlite3.Connection, path: str, dex: str, day: str) -> bool:
    try:
        import duckdb
    except ImportError:
        logger.error("duckdb unavailable; cannot aggregate reservoir fills")
        return False
    try:
        d = duckdb.connect()
        totals = d.execute(f"""
            SELECT COUNT(*),
                   SUM(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)),
                   SUM(CAST(deployer_fee AS DOUBLE)),
                   SUM(CAST(builder_fee AS DOUBLE))
            FROM read_parquet('{path}')
        """).fetchone()
        per_builder = d.execute(f"""
            SELECT lower(builder), COUNT(*),
                   SUM(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)),
                   SUM(CAST(builder_fee AS DOUBLE))
            FROM read_parquet('{path}')
            WHERE builder IS NOT NULL AND builder <> ''
            GROUP BY 1
        """).fetchall()
    except Exception as exc:
        logger.warning(f"{dex}/{day}: aggregation failed: {exc}")
        return False

    conn.execute(
        "INSERT OR REPLACE INTO dex_daily VALUES (?,?,?,?,?,?)",
        (day, dex, totals[0] or 0, totals[1] or 0.0, totals[2] or 0.0, totals[3] or 0.0),
    )
    conn.executemany(
        "INSERT OR REPLACE INTO dex_builder_daily VALUES (?,?,?,?,?,?)",
        [(day, dex, b or "", n or 0, v or 0.0, f or 0.0) for b, n, v, f in per_builder],
    )
    return True


def _pending_days(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    done = {(r[0], r[1]) for r in conn.execute("SELECT date, dex FROM ingested WHERE status IN ('ok','empty')")}
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    out = []
    for dex, (start, end) in DEX_RANGES.items():
        last = min(end or yesterday, yesterday)
        day = start
        while day <= last:
            s = day.isoformat()
            if (s, dex) not in done:
                out.append((s, dex))
            day += timedelta(days=1)
    # Newest first: recent days matter most while the backfill catches up.
    out.sort(reverse=True)
    return out


def ingest(max_days: int | None = None) -> dict:
    """Ingest up to `max_days` unprocessed partitions. Safe to call repeatedly."""
    conn = _db()
    pending = _pending_days(conn)
    budget = max_days if max_days is not None else DAYS_PER_CYCLE
    todo = pending[:budget]
    ok = empty = failed = 0
    for day, dex in todo:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            dest = tmp.name
        try:
            status = _download(dex, day, dest)
            if status == "ok" and _aggregate(conn, dest, dex, day):
                ok += 1
            elif status == "empty":
                empty += 1
                status = "empty"
            else:
                failed += 1
                continue
            conn.execute(
                "INSERT OR REPLACE INTO ingested VALUES (?,?,?,?)",
                (day, dex, status, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
        finally:
            if os.path.exists(dest):
                os.unlink(dest)
    remaining = max(len(pending) - len(todo), 0)
    if todo:
        logger.info(f"reservoir ingest: {ok} ok, {empty} empty, {failed} failed, {remaining} days pending")
    conn.close()
    return {"ingested": ok, "empty": empty, "failed": failed, "pending": remaining}


def markets_fees(builders: list[str]) -> dict | None:
    """
    Markets' fees as summed from the reservoir. `builders` are the builder codes
    that count as Markets; everything else is reported separately rather than
    dropped, so the split stays visible.
    """
    conn = _db()
    row = conn.execute("SELECT COUNT(*), SUM(deployer_fee), SUM(notional) FROM dex_daily").fetchone()
    if not row or not row[0]:
        conn.close()
        return None
    days, deployer, notional = row[0], row[1] or 0.0, row[2] or 0.0

    ours = {b.lower() for b in builders if b}
    mine = other = 0.0
    per_builder = []
    for b, f, n in conn.execute(
        "SELECT builder, SUM(builder_fee), SUM(notional) FROM dex_builder_daily GROUP BY 1 ORDER BY 2 DESC"
    ):
        f = f or 0.0
        if b in ours:
            mine += f
        else:
            other += f
        per_builder.append({"builder": b, "builder_fee_usd": round(f, 2),
                            "notional_usd": round(n or 0.0, 2), "is_markets": b in ours})

    covered = conn.execute(
        "SELECT MIN(date), MAX(date) FROM ingested WHERE status='ok'"
    ).fetchone()
    pending = len(_pending_days(conn))
    conn.close()
    return {
        "deployer_fee_usd": round(deployer, 2),
        "builder_fee_markets_usd": round(mine, 2),
        "builder_fee_other_usd": round(other, 2),
        "total_usd": round(deployer + mine, 2),
        "days_ingested": days,
        "days_pending": pending,
        "covers": {"from": covered[0], "to": covered[1]} if covered else None,
        "fill_notional_usd": round(notional, 2),
        "builders": per_builder[:25],
        "complete": pending == 0,
    }
