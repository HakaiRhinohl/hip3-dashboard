"""
Revenue collector — supports km/mkts, xyz, flx, cash.
Fetches candles, on-chain fees, and computes projections per DEX.

The public ``km`` view intentionally joins the legacy USDH deployment (km) and
the current USDC deployment (mkts). Kinetiq changed the namespace during the
June 2026 migration, but both deployments belong to the same revenue history.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from schedulers import hl_post

CACHE_DIR = os.environ.get("CACHE_DIR", "/data")
from schedulers.fee_db import update_deployer_cumulative, parse_builder_rewards

logger = logging.getLogger("kinetiq.revenue")

LAUNCH_MS = int(datetime(2025, 11, 1).timestamp() * 1000)
KINETIQ_MIGRATION_DATE = "2026-06-20"
KINETIQ_NORMAL_DEPLOYER_BPS = 4.0743
KINETIQ_GROWTH_DEPLOYER_BPS = KINETIQ_NORMAL_DEPLOYER_BPS * 0.10

# Audited on-chain snapshot. These are floors, not hard-coded totals: live
# cumulative balances can move the dashboard above them, but never erase the
# history already reconstructed from transactions and fee-recipient flows.
KINETIQ_ONCHAIN_SNAPSHOT = {
    "as_of": "2026-09-02",
    "user_fees": 924_520.0,
    "hip3_fees": 728_060.0,
    "deployer_revenue": 338_960.0,
    "builder_revenue": 196_450.0,
    "protocol_revenue": 535_410.0,
    "kmhype_allocation": 33_900.0,
    "minimum_kntq_buybacks": 230_300.0,
    "operations_reinvestment": 271_200.0,
}

KINETIQ_LST_SNAPSHOT = {
    "as_of": "2026-09-02",
    "khype": {
        "tvl_hype": 15_000_000.0,
        "tvl_usd": 1_260_000_000.0,
        "gross_staking_apr": 0.02112,
        "implied_annual_gross_rewards_usd": 26_611_200.0,
        "performance_fee_rate": 0.10,
        "historical_protocol_revenue_usd": 2_466_790.0,
        "historical_treasury_usd": 1_910_000.0,
        "historical_kntq_buybacks_usd": 553_800.0,
        "current_policy": {
            "effective_from": "2026-04-09",
            "kntq_buybacks_share": 0.70,
            "treasury_share": 0.30,
        },
        "quarterly_revenue_usd": [
            {"quarter": "2025-Q3", "value": 93_000.0, "partial": False},
            {"quarter": "2025-Q4", "value": 1_370_000.0, "partial": False},
            {"quarter": "2026-Q1", "value": 191_440.0, "partial": False},
            {"quarter": "2026-Q2", "value": 467_670.0, "partial": False},
            {"quarter": "2026-Q3", "value": 344_680.0, "partial": True},
        ],
        "methodology_note": "Gross staking rewards are TVL multiplied by gross APR; they are not TVL or protocol revenue.",
    },
    "kmhype": {
        "markets_allocation_usd": KINETIQ_ONCHAIN_SNAPSHOT["kmhype_allocation"],
        "allocation_share_of_deployer_revenue": 0.10,
    },
    "kntq_burn_flow": {
        "spot_deployer": "0x51172933b60847085e2a959e860e2ec9e240ac09",
        "assistance_fund": "0xfefefefefefefefefefefefefefefefefefefefe",
        "token_id": "0xbd31bd605c0a1b82c72aae3587f9061f",
        "observed_transfer_count_floor": 40,
        "explorer": "https://hypurrscan.io/address/0x51172933b60847085e2a959e860e2ec9e240ac09#txs",
        "included_in_markets_revenue": False,
    },
    "wallets": {
        "markets_fee_recipient": "0xbcd4071d023bf2aae484d724c130b5af6f0ca0d2",
        "markets_builder": "0x42f3226007290b02c5a0b15bccbb1ba6df04f992",
        "kmhype_staking_manager": "0x71f0019cc7fa79e4f42587fb7b9a817d8d2429ec",
        "skntq_buybacks": "0xaa3b7392052d62928cc87701e3ca6fb6630bb6e2",
        "khype_treasury": "0x64bD77698Ab7C3Fd0a1F54497b228ED7a02098E3",
    },
}

# On-chain addresses per DEX
DEX_CONFIG = {
    "km": {
        "fee_recipient": "0xbcd4071d023bf2aae484d724c130b5af6f0ca0d2",
        # The staking builder is deliberately excluded from Markets revenue.
        "builders": ["0x42f3226007290b02c5a0b15bccbb1ba6df04f992"],
        "dex_sources": [
            {"dex": "km", "quote": "USDH", "era": "legacy"},
            {"dex": "mkts", "quote": "USDC", "era": "current"},
        ],
        "growth_discount": 0.10,
        "normal_deployer_bps": KINETIQ_NORMAL_DEPLOYER_BPS,
    },
    "xyz": {
        "fee_recipient": "0x9cd0a696c7cbb9d44de99268194cb08e5684e5fe",
        "builders": ["0x88806a71d74ad0a510b350545c9ae490912f0888"],
        "growth_discount": None,
    },
    "flx": {
        "fee_recipient": "0xe2872b5ae7dcbba40cc4510d08c8bbea95b42d43",
        "builders": ["0x2fab552502a6d45920d5741a2f3ebf4c35536352", "0x2157f54f7a745c772e686aa691fa590b49171ec9"],
        "growth_discount": None,
    },
    "cash": {
        "fee_recipient": "0xaa7f0d3da989dae8fd166345a3ce21509f8c8bb4",
        "builders": ["0xffa8198c62adb1e811629bd54c9b646d726deef7", "0x4950994884602d1b6c6d96e4fe30f58205c39395"],
        "growth_discount": None,
    },
}


def _try_candle_download(coin_name: str, perp_dex: str | None = None) -> list:
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin_name,
            "interval": "1d",
            "startTime": LAUNCH_MS,
            "endTime": int(datetime.now().timestamp() * 1000),
        },
    }
    if perp_dex:
        payload["perpDex"] = perp_dex
    # Use fewer retries for candles — 500s are usually permanent (unsupported ticker)
    result = hl_post(payload, f"candle {coin_name}", retries=1)
    if isinstance(result, list) and len(result) > 0:
        return result
    return []


class RevenueCollector:
    def __init__(self, dex: str):
        if dex not in DEX_CONFIG:
            raise ValueError(f"Unknown DEX: {dex}")
        self.dex = dex
        self.cfg = DEX_CONFIG[dex]
        self.data = None
        self.last_updated = None
        self._cache_path = os.path.join(CACHE_DIR, f"revenue_{dex}.json")
        self._load_cache()

    def _load_cache(self):
        try:
            if os.path.exists(self._cache_path):
                with open(self._cache_path) as f:
                    cached = json.load(f)
                self.data = cached.get("data")
                self.last_updated = cached.get("last_updated")
                logger.info(f"Loaded cached revenue data for {self.dex}")
        except Exception as e:
            logger.warning(f"Failed to load revenue cache for {self.dex}: {e}")

    def _save_cache(self):
        try:
            os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
            with open(self._cache_path, "w") as f:
                json.dump({"data": self.data, "last_updated": self.last_updated}, f)
        except Exception as e:
            logger.warning(f"Failed to save revenue cache for {self.dex}: {e}")

    def get_data(self) -> dict:
        if self.data is None:
            return {"status": "loading", "message": "Initial collection in progress"}
        return self.data

    def collect(self):
        logger.info(f"Starting revenue collection for {self.dex}...")
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        # Step 1: Get tickers. Kinetiq spans two DEX namespaces after its
        # USDH -> USDC migration, while every other venue has one source.
        source_configs = self.cfg.get(
            "dex_sources",
            [{"dex": self.dex, "quote": None, "era": "current"}],
        )
        markets = []
        for source in source_configs:
            source_dex = source["dex"]
            dex_limits = hl_post(
                {"type": "perpDexLimits", "dex": source_dex},
                f"limits {source_dex}",
            )
            if not dex_limits:
                logger.warning(f"Failed to get perpDexLimits for {source_dex}")
                continue
            for pair in dex_limits.get("coinToOiCap", []):
                if isinstance(pair, list) and len(pair) == 2:
                    markets.append({**source, "ticker": pair[0]})

        if not markets:
            logger.error(f"Failed to discover tickers for {self.dex}")
            return

        logger.info(f"{self.dex}: found {len(markets)} tickers across {len(source_configs)} source(s)")

        # Step 2: Download candles
        candles_by_ticker = {}
        market_meta = {}
        for market in markets:
            ticker = market["ticker"]
            source_dex = market["dex"]
            prefix = f"{source_dex}:"
            short = ticker.replace(prefix, "")
            market_key = f"{source_dex}|{ticker}"
            for coin_name, pdex in [(ticker, source_dex), (short, source_dex), (ticker, None), (short, None)]:
                raw = _try_candle_download(coin_name, pdex)
                if raw:
                    rows = []
                    for c in raw:
                        t_ms = c.get("t", c.get("T", 0))
                        c_px = float(c.get("c", "0"))
                        v = float(c.get("v", "0"))
                        vol_usd = v * c_px if c_px > 0 else 0
                        date_str = datetime.fromtimestamp(t_ms / 1000).strftime("%Y-%m-%d")
                        if self.dex == "km":
                            is_legacy_date = date_str <= KINETIQ_MIGRATION_DATE
                            if (market["era"] == "legacy") != is_legacy_date:
                                continue
                        rows.append({"date": date_str, "volume_usd": round(vol_usd, 2)})
                    candles_by_ticker[market_key] = rows
                    market_meta[market_key] = {**market, "short": short}
                    break
                time.sleep(0.1)

        # Step 3: Aggregate volume
        daily_vol, vol_by_ticker = {}, {}
        for market_key, rows in candles_by_ticker.items():
            ticker_total = 0
            for row in rows:
                daily_vol[row["date"]] = daily_vol.get(row["date"], 0) + row["volume_usd"]
                ticker_total += row["volume_usd"]
            vol_by_ticker[market_key] = ticker_total

        total_cum_vol = sum(daily_vol.values())
        num_days = len(daily_vol)
        sorted_dates = sorted(daily_vol.keys())

        def trailing_calendar_average(days: int) -> float:
            if not sorted_dates:
                return 0
            end_date = datetime.strptime(sorted_dates[-1], "%Y-%m-%d").date()
            start_date = end_date - timedelta(days=days - 1)
            total = sum(
                daily_vol.get((start_date + timedelta(days=offset)).isoformat(), 0)
                for offset in range(days)
            )
            return total / days

        avg_7d = trailing_calendar_average(7)
        avg_30d = trailing_calendar_average(30)

        # days since first data point
        if sorted_dates:
            first_date = datetime.strptime(sorted_dates[0], "%Y-%m-%d")
            days_since_launch = (datetime.now() - first_date).days or 1
        else:
            days_since_launch = 1
        avg_daily = total_cum_vol / days_since_launch if days_since_launch > 0 else 0

        # Step 4: On-chain fees
        # Deployer fees: clearinghouseState.accountValue is the CURRENT unclaimed balance.
        # Use watermark to accumulate across withdrawal events.
        deployer_fees = 0.0
        for source in source_configs:
            source_dex = source["dex"]
            ch = hl_post(
                {"type": "clearinghouseState", "user": self.cfg["fee_recipient"], "dex": source_dex},
                f"CH {source_dex}",
            )
            deployer_balance = float(ch.get("marginSummary", {}).get("accountValue", "0")) if ch else 0
            deployer_fees += update_deployer_cumulative(source_dex, deployer_balance)

        # Builder fees: referral.tokenToState[x].builderRewards is already CUMULATIVE
        # (claimedRewards + unclaimedRewards). Sum ALL token types (USDC, USDH, USDE, USDT0).
        total_builder = 0.0
        queried = set()
        for addr in self.cfg["builders"]:
            if not addr or addr in queried:
                continue
            queried.add(addr)
            ref = hl_post({"type": "referral", "user": addr}, f"ref {addr[:8]}")
            total_builder += parse_builder_rewards(ref)

        if self.dex == "km":
            # The referral endpoint is builder-global and includes rewards that
            # are not attributable to Markets. Use the transaction-level
            # reconstruction until per-fill DEX attribution is available.
            deployer_fees = KINETIQ_ONCHAIN_SNAPSHOT["deployer_revenue"]
            total_builder = KINETIQ_ONCHAIN_SNAPSHOT["builder_revenue"]

        total_fees = deployer_fees + total_builder

        # Fee rates
        eff_deployer_bps = 0.0
        normal_deployer_bps = 0.0
        eff_builder_bps = 0.0
        if total_cum_vol > 0:
            if deployer_fees > 0:
                eff_deployer_bps = (deployer_fees / total_cum_vol) * 10000
            if total_builder > 0:
                eff_builder_bps = (total_builder / total_cum_vol) * 10000
            configured_normal_bps = self.cfg.get("normal_deployer_bps")
            discount = self.cfg.get("growth_discount")
            normal_deployer_bps = (
                configured_normal_bps
                if configured_normal_bps is not None
                else eff_deployer_bps / discount if discount else eff_deployer_bps
            )

        # Step 5: Net deposit
        total_net_deposit = 0.0
        for source in source_configs:
            source_dex = source["dex"]
            dex_status = hl_post({"type": "perpDexStatus", "dex": source_dex}, f"status {source_dex}")
            if dex_status:
                total_net_deposit += float(dex_status.get("totalNetDeposit", "0"))

        # Build daily chart
        cum = 0
        daily_chart = []
        for date in sorted_dates:
            v = daily_vol[date]
            cum += v
            fg = v * eff_deployer_bps / 10000
            fn = v * normal_deployer_bps / 10000
            bf = v * eff_builder_bps / 10000
            daily_chart.append({
                "date": date,
                "daily_volume_usd": round(v, 2),
                "cum_volume_usd": round(cum, 2),
                "deployer_fee_growth": round(fg, 2),
                "deployer_fee_normal": round(fn, 2),
                "builder_fee": round(bf, 2),
                "total_fee_growth": round(fg + bf, 2),
                "total_fee_normal": round(fn + bf, 2),
                "era": "legacy" if self.dex == "km" and date <= KINETIQ_MIGRATION_DATE else "current",
            })

        # Run-rate projections use recent volume and the current fee schedule.
        # Builder rewards are only exposed cumulatively by Hyperliquid, so their
        # observed all-time effective rate is the best available rate proxy.
        run_rate_deployer_bps = (
            KINETIQ_GROWTH_DEPLOYER_BPS if self.dex == "km" else eff_deployer_bps
        )
        run_rate_builder_bps = eff_builder_bps
        run_rate_total_bps = run_rate_deployer_bps + run_rate_builder_bps
        projections = {}
        for label, avg_d in [("last_7d", avg_7d), ("last_30d", avg_30d)]:
            if avg_d > 0 and run_rate_deployer_bps > 0:
                ann_vol = avg_d * 365
                ann_dg = ann_vol * run_rate_deployer_bps / 10000
                ann_dn = ann_vol * normal_deployer_bps / 10000
                ann_b = ann_vol * run_rate_builder_bps / 10000
                projections[label] = {
                    "avg_daily_volume": round(avg_d),
                    "growth_mode": {"deployer": round(ann_dg), "builder": round(ann_b), "total": round(ann_dg + ann_b)},
                    "normal_mode": {"deployer": round(ann_dn), "builder": round(ann_b), "total": round(ann_dn + ann_b)},
                }

        # Ticker chart
        sorted_tickers = sorted(vol_by_ticker.items(), key=lambda x: -x[1])
        ticker_chart = []
        for market_key, volume in sorted_tickers:
            meta = market_meta.get(market_key, {})
            ticker_chart.append({
                "ticker": meta.get("short", market_key.split("|")[-1]),
                "venue": meta.get("dex", self.dex),
                "quote": meta.get("quote"),
                "era": meta.get("era", "current"),
                "volume": round(volume),
                "pct": round(volume / total_cum_vol * 100, 1) if total_cum_vol > 0 else 0,
            })

        effective_total_bps = eff_deployer_bps + eff_builder_bps
        annualized_revenue = total_fees / days_since_launch * 365 if days_since_launch > 0 else 0
        normal_cumulative_revenue = total_cum_vol * normal_deployer_bps / 10000 + total_builder
        annualized_normal_revenue = (
            normal_cumulative_revenue / days_since_launch * 365
            if days_since_launch > 0 else 0
        )
        annualized_revenue_30d = avg_30d * 365 * run_rate_total_bps / 10000
        annualized_normal_revenue_30d = (
            avg_30d * 365 * (normal_deployer_bps + run_rate_builder_bps) / 10000
        )

        self.data = {
            "dex": self.dex,
            "generated_at": now_str,
            "days_since_launch": days_since_launch,
            "num_tickers": len(markets),
            "total_volume": round(total_cum_vol),
            "total_net_deposit": round(total_net_deposit, 2),
            "fees": {
                "deployer": round(deployer_fees, 2),
                "builder": round(total_builder, 2),
                "total": round(total_fees, 2),
            },
            "rates": {
                "eff_deployer_bps_growth": round(eff_deployer_bps, 4),
                "eff_deployer_bps_normal": round(normal_deployer_bps, 4),
                "eff_builder_bps": round(eff_builder_bps, 4),
                "eff_total_bps": round(effective_total_bps, 4),
                "run_rate_deployer_bps_30d": round(run_rate_deployer_bps, 4),
                "run_rate_builder_bps_30d": round(run_rate_builder_bps, 4),
                "run_rate_total_bps_30d": round(run_rate_total_bps, 4),
            },
            "kpis": {
                "cumulative_volume": round(total_cum_vol),
                "protocol_revenue": round(total_fees, 2),
                "effective_total_bps": round(effective_total_bps, 4),
                "annualized_revenue": round(annualized_revenue, 2),
                "annualized_normal_revenue": round(annualized_normal_revenue, 2),
                "annualized_revenue_30d": round(annualized_revenue_30d, 2),
                "annualized_normal_revenue_30d": round(annualized_normal_revenue_30d, 2),
            },
            "averages": {"daily": round(avg_daily), "avg_7d": round(avg_7d), "avg_30d": round(avg_30d)},
            "projections": projections,
            "daily_chart": daily_chart,
            "ticker_chart": ticker_chart,
        }
        if self.dex == "km":
            self.data["migration"] = {
                "cutoff": KINETIQ_MIGRATION_DATE,
                "legacy": {"dex": "km", "quote": "USDH", "last_day": "2026-06-20"},
                "current": {"dex": "mkts", "quote": "USDC", "first_day": "2026-06-21"},
            }
            self.data["onchain_reconstruction"] = KINETIQ_ONCHAIN_SNAPSHOT
            self.data["lst"] = KINETIQ_LST_SNAPSHOT
            self.data["methodology"] = {
                "volume": "Sum of daily candle base volume multiplied by close price across km and mkts",
                "historical_effective_rate": "Protocol revenue divided by cumulative estimated USD volume",
                "run_rate_30d": (
                    f"Trailing 30-day average volume annualized at {KINETIQ_GROWTH_DEPLOYER_BPS:.4f} "
                    "current growth-mode deployer bps plus the observed builder rate proxy"
                ),
                "normal_mode_30d": f"Trailing 30-day average volume at {KINETIQ_NORMAL_DEPLOYER_BPS} deployer bps plus the observed builder rate proxy",
            }
        self.last_updated = now_str
        self._save_cache()
        logger.info(f"{self.dex} revenue: ${total_cum_vol:,.0f} vol, ${total_fees:,.2f} fees")
