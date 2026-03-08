"""
Liquidity collector — adapted from 11_silver_liquidity_monitor.py
Takes orderbook snapshots every 30s, keeps rolling window of stats.
"""

import logging
import time
import statistics
from collections import defaultdict
from datetime import datetime, timezone

from schedulers import hl_post

logger = logging.getLogger("kinetiq.liquidity")

DEXES = {
    "km": {"name": "Markets", "prefix": "km"},
    "xyz": {"name": "Trade.xyz", "prefix": "xyz"},
    "flx": {"name": "Felix", "prefix": "flx"},
    "cash": {"name": "Dreamcash", "prefix": "cash"},
}

TICKERS = ["SILVER", "GOLD", "NVDA", "TSLA"]

# Keep last N snapshots for rolling statistics
MAX_SNAPSHOTS = 600  # ~5 hours at 30s intervals


def get_book(coin: str, dex: str) -> dict | None:
    """Get L2 orderbook, try multiple formats."""
    prefix = DEXES[dex]["prefix"]
    full = f"{prefix}:{coin}"

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
    """Extract spread and depth from L2 book."""
    levels = book["levels"]
    bids = levels[0]
    asks = levels[1]

    if not bids or not asks:
        return None

    best_bid = float(bids[0]["px"])
    best_ask = float(asks[0]["px"])
    mid = (best_bid + best_ask) / 2
    if mid == 0:
        return None

    spread_bps = ((best_ask - best_bid) / mid) * 10000

    def depth_within(side, max_bps):
        total = 0
        for lvl in side:
            px = float(lvl["px"])
            sz = float(lvl["sz"])
            if abs(px - mid) / mid * 10000 <= max_bps:
                total += sz * px
        return total

    return {
        "mid": round(mid, 6),
        "spread_bps": round(spread_bps, 3),
        "depth_10bps": round(depth_within(bids, 10) + depth_within(asks, 10)),
        "depth_50bps": round(depth_within(bids, 50) + depth_within(asks, 50)),
        "depth_100bps": round(depth_within(bids, 100) + depth_within(asks, 100)),
        "depth_total": round(depth_within(bids, 99999) + depth_within(asks, 99999)),
        "bid_levels": len(bids),
        "ask_levels": len(asks),
    }


def safe_median(lst):
    return statistics.median(lst) if lst else 0


def safe_stdev(lst):
    return statistics.stdev(lst) if len(lst) >= 2 else 0


def percentile(lst, p):
    if not lst:
        return 0
    s = sorted(lst)
    idx = int(len(s) * p / 100)
    return s[min(idx, len(s) - 1)]


class LiquidityCollector:
    def __init__(self):
        self.data = None
        self.last_updated = None
        self.snapshots = defaultdict(list)  # (dex, ticker) -> [analysis_dicts]
        self.discovered_pairs = []  # [(dex, ticker)]
        self._discovered = False
        self.total_snapshots = 0

    def get_data(self) -> dict:
        if self.data is None:
            return {"status": "loading", "message": "Collecting liquidity data..."}
        return self.data

    def _discover_pairs(self):
        """Discover which tickers exist on which dex."""
        pairs = []
        for dex in DEXES:
            limits = hl_post({"type": "perpDexLimits", "dex": dex}, f"limits {dex}")
            if not limits:
                continue
            listed = set()
            for pair in limits.get("coinToOiCap", []):
                if isinstance(pair, list) and len(pair) == 2:
                    listed.add(pair[0])

            for ticker in TICKERS:
                full = f"{DEXES[dex]['prefix']}:{ticker}"
                if full in listed:
                    pairs.append((dex, ticker))

        self.discovered_pairs = pairs
        self._discovered = True
        logger.info(f"Discovered {len(pairs)} liquidity pairs")

    def take_snapshot(self):
        """Take one round of L2 snapshots across all pairs."""
        if not self._discovered:
            self._discover_pairs()

        if not self.discovered_pairs:
            logger.warning("No pairs discovered for liquidity monitoring")
            return

        ok = 0
        for dex, ticker in self.discovered_pairs:
            book = get_book(ticker, dex)
            if not book:
                continue

            analysis = analyze_book(book)
            if not analysis:
                continue

            key = (dex, ticker)
            self.snapshots[key].append(analysis)

            # Trim to max window
            if len(self.snapshots[key]) > MAX_SNAPSHOTS:
                self.snapshots[key] = self.snapshots[key][-MAX_SNAPSHOTS:]

            ok += 1
            time.sleep(0.05)

        self.total_snapshots += 1

        # Recompute aggregated stats
        self._compute_stats()

    def _compute_stats(self):
        """Compute aggregated statistics from snapshots."""
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        summary = []
        for ticker in TICKERS:
            for dex_id, dex_info in DEXES.items():
                key = (dex_id, ticker)
                snaps = self.snapshots.get(key, [])
                if not snaps:
                    continue

                spreads = [s["spread_bps"] for s in snaps]
                d10 = [s["depth_10bps"] for s in snaps]
                d50 = [s["depth_50bps"] for s in snaps]
                d100 = [s["depth_100bps"] for s in snaps]
                dtot = [s["depth_total"] for s in snaps]
                mids = [s["mid"] for s in snaps]

                summary.append({
                    "ticker": ticker,
                    "dex": dex_id,
                    "name": dex_info["name"],
                    "n": len(snaps),
                    "spread": round(safe_median(spreads), 3),
                    "spreadP5": round(percentile(spreads, 5), 3),
                    "spreadP95": round(percentile(spreads, 95), 3),
                    "d10": round(safe_median(d10)),
                    "d50": round(safe_median(d50)),
                    "d100": round(safe_median(d100)),
                    "total": round(safe_median(dtot)),
                    "mid": round(safe_median(mids), 2),
                })

        # Cross-ticker comparison data (for charts)
        spread_all = []
        for ticker in TICKERS:
            row = {"ticker": ticker}
            for dex_id in DEXES:
                item = next((s for s in summary if s["ticker"] == ticker and s["dex"] == dex_id), None)
                row[dex_id] = item["spread"] if item else 0
            spread_all.append(row)

        depth_all = {f"d{bp}": [] for bp in [10, 50, 100]}
        for bp in [10, 50, 100]:
            for ticker in TICKERS:
                row = {"ticker": ticker}
                for dex_id in DEXES:
                    item = next((s for s in summary if s["ticker"] == ticker and s["dex"] == dex_id), None)
                    row[dex_id] = item[f"d{bp}"] if item else 0
                depth_all[f"d{bp}"].append(row)

        self.data = {
            "generated_at": now_str,
            "total_snapshots": self.total_snapshots,
            "pairs_monitored": len(self.discovered_pairs),
            "tickers": TICKERS,
            "dex_names": {k: v["name"] for k, v in DEXES.items()},
            "summary": summary,
            "spread_all": spread_all,
            "depth_10_all": depth_all["d10"],
            "depth_50_all": depth_all["d50"],
            "depth_100_all": depth_all["d100"],
        }
        self.last_updated = now_str
