"""
Liquidity collector — SQLite persistence, full auto-discovery, time-filtered queries.

Discovers ALL tickers available per DEX via perpDexLimits, stores snapshots to
SQLite so data survives restarts. Supports time-range filtering and timeseries
queries for charting.
"""

import logging
import os
import sqlite3
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone

from schedulers import hl_post

logger = logging.getLogger("kinetiq.liquidity")

DEXES = {
    "km":   {"name": "Markets",    "prefix": "km"},
    "xyz":  {"name": "Trade.xyz",  "prefix": "xyz"},
    "flx":  {"name": "Felix",      "prefix": "flx"},
    "cash": {"name": "Dreamcash",  "prefix": "cash"},
}

# SQLite path — override via env var for Docker volume mount
DB_PATH = os.environ.get(
    "LIQUIDITY_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "liquidity.db"),
)

KEEP_DAYS   = 7    # Retain 7 days of snapshots
DEFAULT_HRS = 4    # Hours used for the cached self.data


# ── Orderbook helpers ──────────────────────────────────────────────────────────

def get_book(coin: str, dex: str) -> dict | None:
    """Fetch L2 orderbook, trying multiple coin/dex format combos."""
    prefix = DEXES[dex]["prefix"]
    full   = f"{prefix}:{coin}"
    for c, p in [(full, dex), (coin, dex), (full, None)]:
        payload = {"type": "l2Book", "coin": c}
        if p:
            payload["perpDex"] = p
        book = hl_post(payload, f"book {c}")
        if book and "levels" in book:
            lvls = book["levels"]
            if len(lvls) >= 2 and lvls[0] and lvls[1]:
                return book
        time.sleep(0.05)
    return None


def analyze_book(book: dict) -> dict | None:
    """Return spread + depth metrics from an L2 book, or None if empty."""
    levels   = book["levels"]
    bids, asks = levels[0], levels[1]
    if not bids or not asks:
        return None

    best_bid = float(bids[0]["px"])
    best_ask = float(asks[0]["px"])
    mid      = (best_bid + best_ask) / 2
    if mid == 0:
        return None

    spread_bps = ((best_ask - best_bid) / mid) * 10_000

    def depth_within(side, max_bps):
        return sum(
            float(lvl["sz"]) * float(lvl["px"])
            for lvl in side
            if abs(float(lvl["px"]) - mid) / mid * 10_000 <= max_bps
        )

    return {
        "mid":          round(mid, 6),
        "spread_bps":   round(spread_bps, 3),
        "depth_10bps":  round(depth_within(bids, 10)  + depth_within(asks, 10)),
        "depth_50bps":  round(depth_within(bids, 50)  + depth_within(asks, 50)),
        "depth_100bps": round(depth_within(bids, 100) + depth_within(asks, 100)),
    }


# ── Statistics helpers ─────────────────────────────────────────────────────────

def _median(lst):
    return statistics.median(lst) if lst else 0


def _pct(lst, p):
    if not lst:
        return 0
    s   = sorted(lst)
    idx = int(len(s) * p / 100)
    return s[min(idx, len(s) - 1)]


# ── Collector ──────────────────────────────────────────────────────────────────

class LiquidityCollector:

    def __init__(self):
        self.data            = None
        self.last_updated    = None
        self.discovered_pairs: list[tuple[str, str]] = []   # [(dex, ticker)]
        self.tickers_by_dex:  dict[str, list[str]]   = {}   # dex -> [ticker]
        self._discovered     = False
        self.total_snapshots = 0
        self._db: sqlite3.Connection | None = None

    # ── DB helpers ─────────────────────────────────────────────────────────────

    def _get_db(self) -> sqlite3.Connection:
        if self._db is None:
            db_dir = os.path.dirname(DB_PATH)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
            self._db = sqlite3.connect(DB_PATH, check_same_thread=False)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=NORMAL")
            self._init_db()
        return self._db

    def _init_db(self):
        db = self._db
        db.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           INTEGER NOT NULL,
                dex          TEXT    NOT NULL,
                ticker       TEXT    NOT NULL,
                mid          REAL,
                spread_bps   REAL,
                depth_10bps  REAL,
                depth_50bps  REAL,
                depth_100bps REAL
            )
        """)
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_dex_ticker_ts "
            "ON snapshots(dex, ticker, ts)"
        )
        db.commit()

    def _cleanup_old_data(self):
        cutoff = int(time.time()) - KEEP_DAYS * 86_400
        db = self._get_db()
        db.execute("DELETE FROM snapshots WHERE ts < ?", (cutoff,))
        db.commit()
        logger.info("Old liquidity snapshots pruned")

    # ── Discovery ──────────────────────────────────────────────────────────────

    def _discover_pairs(self):
        """Query perpDexLimits for each DEX and collect ALL available tickers."""
        pairs: list[tuple[str, str]] = []
        tickers_by_dex: dict[str, list[str]] = {}

        for dex in DEXES:
            limits = hl_post({"type": "perpDexLimits", "dex": dex}, f"limits {dex}")
            if not limits:
                logger.warning(f"Could not fetch limits for {dex}")
                continue

            tickers = []
            for entry in limits.get("coinToOiCap", []):
                if not (isinstance(entry, list) and len(entry) == 2):
                    continue
                coin = entry[0]   # e.g. "km:SILVER"
                tkr  = coin.split(":", 1)[1] if ":" in coin else coin
                tickers.append(tkr)
                pairs.append((dex, tkr))

            tickers_by_dex[dex] = sorted(set(tickers))

        self.discovered_pairs = pairs
        self.tickers_by_dex   = tickers_by_dex
        self._discovered      = True
        n_unique = len({tkr for _, tkr in pairs})
        logger.info(
            f"Discovered {len(pairs)} pairs — "
            f"{n_unique} unique tickers across {len(tickers_by_dex)} DEXes"
        )

    # ── Snapshot collection ────────────────────────────────────────────────────

    def take_snapshot(self):
        """Collect one round of L2 snapshots for all discovered pairs."""
        if not self._discovered:
            self._discover_pairs()

        if not self.discovered_pairs:
            logger.warning("No pairs to snapshot")
            return

        db  = self._get_db()
        ts  = int(time.time())
        rows: list[tuple] = []

        for dex, ticker in self.discovered_pairs:
            book = get_book(ticker, dex)
            if not book:
                continue
            analysis = analyze_book(book)
            if not analysis:
                continue
            rows.append((
                ts, dex, ticker,
                analysis["mid"],
                analysis["spread_bps"],
                analysis["depth_10bps"],
                analysis["depth_50bps"],
                analysis["depth_100bps"],
            ))
            time.sleep(0.05)

        if rows:
            db.executemany(
                """INSERT INTO snapshots
                       (ts, dex, ticker, mid, spread_bps,
                        depth_10bps, depth_50bps, depth_100bps)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            db.commit()

        self.total_snapshots += 1

        # Prune every ~1 hour (120 rounds at 30s)
        if self.total_snapshots % 120 == 0:
            self._cleanup_old_data()

        # Refresh the default cached response
        self._compute_stats(hours=DEFAULT_HRS)

    # ── Query helpers ──────────────────────────────────────────────────────────

    def _query_raw(self, hours: int | None, ticker: str | None = None):
        """
        Query snapshots from DB.
        Returns {(dex, ticker): {spread:[], d10:[], d50:[], d100:[], mid:[]}}
        """
        db     = self._get_db()
        params = []
        where  = []

        if hours is not None:
            where.append("ts >= ?")
            params.append(int(time.time()) - hours * 3_600)
        if ticker is not None:
            where.append("ticker = ?")
            params.append(ticker)

        sql = (
            "SELECT dex, ticker, spread_bps, depth_10bps, depth_50bps, depth_100bps, mid "
            "FROM snapshots"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts ASC"

        result: dict[tuple, dict[str, list]] = defaultdict(
            lambda: {"spread": [], "d10": [], "d50": [], "d100": [], "mid": []}
        )
        for row in db.execute(sql, params).fetchall():
            dex, tkr, spread, d10, d50, d100, mid = row
            key = (dex, tkr)
            result[key]["spread"].append(spread)
            result[key]["d10"].append(d10)
            result[key]["d50"].append(d50)
            result[key]["d100"].append(d100)
            result[key]["mid"].append(mid)

        return result

    def _build_summary(self, raw):
        """Build summary list from _query_raw result."""
        summary = []
        for (dex_id, ticker), vals in raw.items():
            if not vals["spread"]:
                continue
            dex_info = DEXES.get(dex_id, {})
            summary.append({
                "ticker":    ticker,
                "dex":       dex_id,
                "name":      dex_info.get("name", dex_id),
                "n":         len(vals["spread"]),
                "spread":    round(_median(vals["spread"]), 3),
                "spreadP5":  round(_pct(vals["spread"], 5), 3),
                "spreadP95": round(_pct(vals["spread"], 95), 3),
                "d10":       round(_median(vals["d10"])),
                "d50":       round(_median(vals["d50"])),
                "d100":      round(_median(vals["d100"])),
                "mid":       round(_median(vals["mid"]), 2),
            })
        return summary

    def _build_response(self, summary: list, hours: int | None) -> dict:
        """Assemble the full API response from a summary list."""
        all_tickers = sorted(
            {tkr for tkrs in self.tickers_by_dex.values() for tkr in tkrs}
        )
        # Tickers present in ALL DEXes
        shared = [
            t for t in all_tickers
            if all(t in self.tickers_by_dex.get(dex, []) for dex in DEXES)
        ]

        def cross_ticker(field):
            rows = []
            for t in shared:
                row = {"ticker": t}
                for dex in DEXES:
                    item = next(
                        (s for s in summary if s["ticker"] == t and s["dex"] == dex),
                        None,
                    )
                    row[dex] = item[field] if item else 0
                rows.append(row)
            return rows

        return {
            "generated_at":    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "total_snapshots": self.total_snapshots,
            "pairs_monitored": len(self.discovered_pairs),
            "hours":           hours,
            "tickers":         all_tickers,
            "shared_tickers":  shared,
            "tickers_by_dex":  self.tickers_by_dex,
            "dex_names":       {k: v["name"] for k, v in DEXES.items()},
            "summary":         summary,
            "spread_all":      cross_ticker("spread"),
            "depth_10_all":    cross_ticker("d10"),
            "depth_50_all":    cross_ticker("d50"),
            "depth_100_all":   cross_ticker("d100"),
        }

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_data(self) -> dict:
        """Return the cached (default 4h window) response. Fast."""
        if self.data is None:
            return {"status": "loading", "message": "Collecting liquidity data..."}
        return self.data

    def _compute_stats(self, hours: int | None = DEFAULT_HRS):
        """Recompute and cache self.data for the given window."""
        raw     = self._query_raw(hours)
        summary = self._build_summary(raw)
        self.data         = self._build_response(summary, hours)
        self.last_updated = self.data["generated_at"]

    def get_stats(self, hours: int | None) -> dict:
        """Return time-filtered stats queried fresh from DB."""
        if not self._discovered:
            return {"status": "loading", "message": "Collecting liquidity data..."}
        raw     = self._query_raw(hours)
        summary = self._build_summary(raw)
        return self._build_response(summary, hours)

    def get_timeseries(self, ticker: str, hours: int = 4) -> dict:
        """Return bucketed timeseries for a specific ticker, all available DEXes."""
        db     = self._get_db()
        cutoff = int(time.time()) - hours * 3_600

        # Adaptive bucket: target ~80 data points per series
        bucket_min = max(1, int(hours * 60 / 80))
        bucket_sec = bucket_min * 60

        rows = db.execute(
            """SELECT dex, ts, spread_bps, depth_10bps, depth_50bps, depth_100bps
               FROM snapshots
               WHERE ticker = ? AND ts >= ?
               ORDER BY ts ASC""",
            (ticker, cutoff),
        ).fetchall()

        # Accumulate into time buckets per DEX
        buckets: dict[str, dict[int, dict[str, list]]] = defaultdict(
            lambda: defaultdict(lambda: {"spread": [], "d10": [], "d50": [], "d100": []})
        )
        for dex, ts, spread, d10, d50, d100 in rows:
            bts = (ts // bucket_sec) * bucket_sec
            b   = buckets[dex][bts]
            b["spread"].append(spread)
            b["d10"].append(d10)
            b["d50"].append(d50)
            b["d100"].append(d100)

        series: dict[str, list] = {}
        for dex, dex_buckets in buckets.items():
            series[dex] = [
                {
                    "t":      bts,
                    "spread": round(_median(b["spread"]), 3),
                    "d10":    round(_median(b["d10"])),
                    "d50":    round(_median(b["d50"])),
                    "d100":   round(_median(b["d100"])),
                }
                for bts, b in sorted(dex_buckets.items())
            ]

        return {
            "ticker":         ticker,
            "hours":          hours,
            "bucket_minutes": bucket_min,
            "series":         series,
            "dex_names":      {k: v["name"] for k, v in DEXES.items()},
        }

    def get_available_tickers(self) -> dict:
        """Return all discovered tickers, grouped by DEX."""
        all_tickers = sorted(
            {tkr for tkrs in self.tickers_by_dex.values() for tkr in tkrs}
        )
        return {
            "tickers_by_dex": self.tickers_by_dex,
            "all_tickers":    all_tickers,
        }
