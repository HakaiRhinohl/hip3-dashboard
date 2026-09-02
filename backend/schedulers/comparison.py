"""Canonical comparison view built from the revenue collectors.

Comparison used to download candles and reconstruct fees independently. That
made it drift from the audited Revenue view, especially across Kinetiq's
``km`` (USDH) to ``mkts`` (USDC) migration. This collector is deliberately a
pure projection: one canonical dataset powers both dashboards.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

CACHE_DIR = os.environ.get("CACHE_DIR", "/data")

logger = logging.getLogger("kinetiq.comparison")

DEXES = ["km", "xyz", "flx", "cash"]
DEX_NAMES = {
    "km": "Markets (Kinetiq)",
    "xyz": "Trade.xyz",
    "flx": "Felix",
    "cash": "Dreamcash",
}


def _period_total(daily: dict[str, float], end_date, days: int, offset: int = 0) -> float:
    period_end = end_date - timedelta(days=offset)
    period_start = period_end - timedelta(days=days - 1)
    return sum(
        daily.get((period_start + timedelta(days=i)).isoformat(), 0)
        for i in range(days)
    )


def _change(current: float, previous: float) -> float | None:
    return round((current - previous) / previous * 100, 2) if previous > 0 else None


class ComparisonCollector:
    def __init__(self):
        self.data = None
        self.last_updated = None
        self._cache_path = os.path.join(CACHE_DIR, "comparison.json")
        self._load_cache()

    def _load_cache(self):
        try:
            if os.path.exists(self._cache_path):
                with open(self._cache_path) as f:
                    cached = json.load(f)
                data = cached.get("data")
                # Never revive output from the retired independent collector.
                if data and data.get("canonical_source") == "revenue_collectors":
                    self.data = data
                    self.last_updated = cached.get("last_updated")
                    logger.info("Loaded canonical comparison cache")
        except Exception as exc:
            logger.warning(f"Failed to load comparison cache: {exc}")

    def _save_cache(self):
        try:
            os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
            with open(self._cache_path, "w") as f:
                json.dump({"data": self.data, "last_updated": self.last_updated}, f)
        except Exception as exc:
            logger.warning(f"Failed to save comparison cache: {exc}")

    def get_data(self) -> dict:
        if self.data is None:
            return {"status": "loading", "message": "Initial collection in progress"}
        return self.data

    def collect(self, revenue_data: dict[str, dict]):
        """Build the comparison from completed RevenueCollector responses."""
        unavailable = [
            dex for dex in DEXES
            if not revenue_data.get(dex) or revenue_data[dex].get("status") == "loading"
        ]
        if unavailable:
            logger.warning(f"Comparison skipped; revenue unavailable for: {unavailable}")
            return

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        daily_by_dex: dict[str, dict[str, float]] = {}
        all_dates: set[str] = set()

        for dex in DEXES:
            daily = {
                row["date"]: float(row.get("daily_volume_usd", 0))
                for row in revenue_data[dex].get("daily_chart", [])
            }
            daily_by_dex[dex] = daily
            all_dates.update(daily)

        if all_dates:
            first = datetime.strptime(min(all_dates), "%Y-%m-%d").date()
            last = datetime.strptime(max(all_dates), "%Y-%m-%d").date()
            calendar_dates = [
                (first + timedelta(days=i)).isoformat()
                for i in range((last - first).days + 1)
            ]
        else:
            last = datetime.now(timezone.utc).date()
            calendar_dates = []

        cumulative = {dex: 0.0 for dex in DEXES}
        daily_chart = []
        for date_str in calendar_dates:
            row = {"date": date_str}
            for dex in DEXES:
                volume = daily_by_dex[dex].get(date_str, 0)
                cumulative[dex] += volume
                row[f"{dex}_vol"] = round(volume, 2)
                row[f"{dex}_cum"] = round(cumulative[dex], 2)
            daily_chart.append(row)

        trends = []
        dex_summaries = []
        for dex in DEXES:
            revenue = revenue_data[dex]
            fees = revenue.get("fees", {})
            rates = revenue.get("rates", {})
            averages = revenue.get("averages", {})
            daily = daily_by_dex[dex]
            # Anchor each dex's own trailing window to its own last observed date
            # (matching RevenueCollector's trailing_calendar_average), not the
            # global max date across dexes. Otherwise a dex whose candle fetch
            # lags behind the others gets its recent days zero-filled here,
            # silently undercounting volume_7d/volume_30d relative to that
            # same dex's own avg_7d/avg_30d.
            dex_last = datetime.strptime(max(daily), "%Y-%m-%d").date() if daily else last
            vol_7d = _period_total(daily, dex_last, 7)
            prev_7d = _period_total(daily, dex_last, 7, 7)
            vol_30d = _period_total(daily, dex_last, 30)
            prev_30d = _period_total(daily, dex_last, 30, 30)

            trends.append({
                "dex": dex,
                "volume_7d": round(vol_7d),
                "previous_7d": round(prev_7d),
                "change_7d_pct": _change(vol_7d, prev_7d),
                "volume_30d": round(vol_30d),
                "previous_30d": round(prev_30d),
                "change_30d_pct": _change(vol_30d, prev_30d),
            })

            source_counts = revenue.get("source_ticker_counts", {})
            default_active = (
                source_counts.get("mkts", revenue.get("num_tickers", 0))
                if dex == "km" else revenue.get("num_tickers", 0)
            )
            dex_summaries.append({
                "dex": dex,
                "name": DEX_NAMES[dex],
                "num_tickers": revenue.get("num_tickers", 0),
                "active_tickers": revenue.get("active_tickers", default_active),
                "historical_tickers": revenue.get("historical_tickers", 0),
                "num_days": revenue.get("days_since_launch", 0),
                "observed_days": len(daily),
                "cum_volume": revenue.get("total_volume", 0),
                "deployer_fees": fees.get("deployer", 0),
                "builder_fees": fees.get("builder", 0),
                "total_fees": fees.get("total", 0),
                "fee_coverage": revenue.get("fee_coverage", {}),
                "eff_deployer_bps": rates.get("eff_deployer_bps_growth", 0),
                "eff_deployer_bps_normal": rates.get("eff_deployer_bps_normal", 0),
                "run_rate_deployer_bps_30d": rates.get("run_rate_deployer_bps_30d", 0),
                "eff_total_bps": rates.get("eff_total_bps", 0),
                "run_rate_total_bps_30d": rates.get("run_rate_total_bps_30d", 0),
                "total_net_deposit": revenue.get("total_net_deposit", 0),
                "avg_7d": averages.get("avg_7d", 0),
                "avg_30d": averages.get("avg_30d", 0),
                "top_tickers": revenue.get("ticker_chart", []),
            })

        self.data = {
            "generated_at": now_str,
            "canonical_source": "revenue_collectors",
            "methodology": (
                "Volumes, fees, rates and ticker histories are the exact canonical "
                "values exposed by each Revenue collector; Comparison performs no "
                "independent fee or candle reconstruction."
            ),
            "migration": revenue_data["km"].get("migration"),
            "dex_summaries": dex_summaries,
            "trends": trends,
            "daily_chart": daily_chart,
        }
        self.last_updated = now_str
        self._save_cache()
        logger.info("Canonical comparison data updated")
