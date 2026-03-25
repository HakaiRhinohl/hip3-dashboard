"""
Revenue collector — adapted from 09_markets_revenue_report.py
Fetches km: candles, fees, and computes projections.
"""

import logging
import time
from datetime import datetime, timezone

from schedulers import hl_post

logger = logging.getLogger("kinetiq.revenue")

DEX = "km"
MARKETS_LAUNCH = datetime(2026, 1, 12)
MARKETS_LAUNCH_MS = int(MARKETS_LAUNCH.timestamp() * 1000)

FEE_RECIPIENT = "0xbcd4071d023bf2aae484d724c130b5af6f0ca0d2"
DEPLOYER = "0x75e05d6bc77ce5e288a9f0e935e5e75fa5c0a700"
TRADING_BUILDER = "0x42f3226007290b02c5a0b15bccbb1ba6df04f992"
STAKING_BUILDER = "0x4ec89c1c70ca1e2f224bb43e28d122f4d2b4e8bb"

GROWTH_DISCOUNT = 0.10


def try_candle_download(coin_name: str, perp_dex: str | None = None) -> list:
    """Try downloading candles with optional perpDex param."""
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin_name,
            "interval": "1d",
            "startTime": MARKETS_LAUNCH_MS,
            "endTime": int(datetime.now().timestamp() * 1000),
        },
    }
    if perp_dex:
        payload["perpDex"] = perp_dex

    result = hl_post(payload, f"candle {coin_name}")
    if isinstance(result, list) and len(result) > 0:
        return result
    return []


class RevenueCollector:
    def __init__(self):
        self.data = None
        self.last_updated = None

    def get_data(self) -> dict:
        if self.data is None:
            return {"status": "loading", "message": "Initial collection in progress"}
        return self.data

    def collect(self):
        """Run full revenue collection."""
        logger.info("Starting revenue collection...")
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        days_since_launch = (datetime.now() - MARKETS_LAUNCH).days

        # Step 1: Get km tickers
        dex_limits = hl_post({"type": "perpDexLimits", "dex": DEX}, "limits km")
        if not dex_limits:
            logger.error("Failed to get perpDexLimits")
            return

        km_tickers = []
        oi_caps = {}
        for pair in dex_limits.get("coinToOiCap", []):
            if isinstance(pair, list) and len(pair) == 2:
                km_tickers.append(pair[0])
                oi_caps[pair[0]] = float(pair[1])

        logger.info(f"Found {len(km_tickers)} km tickers")

        # Step 2: Download candles
        candles_by_ticker = {}
        for ticker in km_tickers:
            short = ticker.replace("km:", "")
            attempts = [
                (ticker, DEX),
                (short, DEX),
                (ticker, None),
                (short, None),
            ]

            for coin_name, pdex in attempts:
                raw = try_candle_download(coin_name, pdex)
                if raw:
                    rows = []
                    for c in raw:
                        t_ms = c.get("t", c.get("T", 0))
                        c_px = float(c.get("c", "0"))
                        v = float(c.get("v", "0"))
                        vol_usd = v * c_px if c_px > 0 else 0
                        date_str = datetime.fromtimestamp(t_ms / 1000).strftime("%Y-%m-%d")
                        rows.append({"date": date_str, "volume_usd": round(vol_usd, 2)})
                    candles_by_ticker[ticker] = rows
                    break
                time.sleep(0.1)

        # Step 3: Aggregate volume
        daily_vol = {}
        vol_by_ticker = {}

        for ticker, rows in candles_by_ticker.items():
            ticker_total = 0
            for row in rows:
                date = row["date"]
                vol = row["volume_usd"]
                daily_vol[date] = daily_vol.get(date, 0) + vol
                ticker_total += vol
            vol_by_ticker[ticker] = ticker_total

        total_cum_vol = sum(daily_vol.values())
        num_days = len(daily_vol)
        avg_daily = total_cum_vol / num_days if num_days > 0 else 0

        sorted_dates = sorted(daily_vol.keys())
        last_7 = sorted_dates[-7:] if len(sorted_dates) >= 7 else sorted_dates
        last_30 = sorted_dates[-30:] if len(sorted_dates) >= 30 else sorted_dates
        avg_7d = sum(daily_vol[d] for d in last_7) / len(last_7) if last_7 else 0
        avg_30d = sum(daily_vol[d] for d in last_30) / len(last_30) if last_30 else 0

        # Step 4: On-chain fees
        ch_km = hl_post(
            {"type": "clearinghouseState", "user": FEE_RECIPIENT, "dex": "km"},
            "CH fee_recipient",
        )
        deployer_fees = (
            float(ch_km.get("marginSummary", {}).get("accountValue", "0"))
            if ch_km
            else 0
        )

        ref_t = hl_post({"type": "referral", "user": TRADING_BUILDER}, "ref trading")
        trading_rewards = float(ref_t.get("builderRewards", "0")) if ref_t else 0

        ref_s = hl_post({"type": "referral", "user": STAKING_BUILDER}, "ref staking")
        staking_rewards = float(ref_s.get("builderRewards", "0")) if ref_s else 0

        total_builder = trading_rewards + staking_rewards
        total_fees = deployer_fees + total_builder

        # Fee rates
        eff_deployer_bps = 0
        normal_deployer_bps = 0
        eff_builder_bps = 0

        if total_cum_vol > 0 and deployer_fees > 0:
            eff_deployer_bps = (deployer_fees / total_cum_vol) * 10000
            eff_builder_bps = (total_builder / total_cum_vol) * 10000
            normal_deployer_bps = eff_deployer_bps / GROWTH_DISCOUNT

        # Step 5: Net deposit
        dex_status = hl_post({"type": "perpDexStatus", "dex": DEX}, "status km")
        total_net_deposit = (
            float(dex_status.get("totalNetDeposit", "0")) if dex_status else 0
        )

        # Build daily chart data
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
            })

        # Projections (based on 30d average)
        daily_b = total_builder / days_since_launch if days_since_launch > 0 else 0
        ann_b = daily_b * 365

        projections = {}
        for label, avg_d in [("last_7d", avg_7d), ("last_30d", avg_30d)]:
            if avg_d > 0 and eff_deployer_bps > 0:
                ann_vol = avg_d * 365
                ann_dg = ann_vol * eff_deployer_bps / 10000
                ann_dn = ann_vol * normal_deployer_bps / 10000
                projections[label] = {
                    "avg_daily_volume": round(avg_d),
                    "growth_mode": {
                        "deployer": round(ann_dg),
                        "builder": round(ann_b),
                        "total": round(ann_dg + ann_b),
                    },
                    "normal_mode": {
                        "deployer": round(ann_dn),
                        "builder": round(ann_b),
                        "total": round(ann_dn + ann_b),
                    },
                }

        # Volume by ticker (sorted)
        sorted_tickers = sorted(vol_by_ticker.items(), key=lambda x: -x[1])
        ticker_chart = [
            {"ticker": t.replace("km:", ""), "volume": round(v), "pct": round(v / total_cum_vol * 100, 1) if total_cum_vol > 0 else 0}
            for t, v in sorted_tickers
        ]

        # Assemble response
        self.data = {
            "generated_at": now_str,
            "days_since_launch": days_since_launch,
            "km_tickers": len(km_tickers),
            "total_volume": round(total_cum_vol),
            "total_net_deposit": round(total_net_deposit, 2),
            "fees": {
                "deployer": round(deployer_fees, 2),
                "trading_builder": round(trading_rewards, 2),
                "staking_builder": round(staking_rewards, 2),
                "total": round(total_fees, 2),
            },
            "rates": {
                "eff_deployer_bps_growth": round(eff_deployer_bps, 4),
                "eff_deployer_bps_normal": round(normal_deployer_bps, 4),
                "eff_builder_bps": round(eff_builder_bps, 4),
            },
            "averages": {
                "daily": round(avg_daily),
                "avg_7d": round(avg_7d),
                "avg_30d": round(avg_30d),
            },
            "projections": projections,
            "daily_chart": daily_chart,
            "ticker_chart": ticker_chart,
        }
        self.last_updated = now_str
        logger.info(f"Revenue data updated: ${total_cum_vol:,.0f} volume, ${total_fees:,.2f} fees")
