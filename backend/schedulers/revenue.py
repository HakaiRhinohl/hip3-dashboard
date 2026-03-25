"""
Revenue collector — supports km, xyz, flx, cash.
Fetches candles, on-chain fees, and computes projections per DEX.
"""

import logging
import time
from datetime import datetime, timezone

from schedulers import hl_post

logger = logging.getLogger("kinetiq.revenue")

LAUNCH_MS = int(datetime(2025, 11, 1).timestamp() * 1000)

# On-chain addresses per DEX
DEX_CONFIG = {
    "km": {
        "fee_recipient": "0xbcd4071d023bf2aae484d724c130b5af6f0ca0d2",
        "builders": [
            "0x42f3226007290b02c5a0b15bccbb1ba6df04f992",  # trading
            "0x4ec89c1c70ca1e2f224bb43e28d122f4d2b4e8bb",  # staking
        ],
        "growth_discount": 0.10,
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
    result = hl_post(payload, f"candle {coin_name}")
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

    def get_data(self) -> dict:
        if self.data is None:
            return {"status": "loading", "message": "Initial collection in progress"}
        return self.data

    def collect(self):
        logger.info(f"Starting revenue collection for {self.dex}...")
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        # Step 1: Get tickers
        dex_limits = hl_post({"type": "perpDexLimits", "dex": self.dex}, f"limits {self.dex}")
        if not dex_limits:
            logger.error(f"Failed to get perpDexLimits for {self.dex}")
            return

        tickers = []
        for pair in dex_limits.get("coinToOiCap", []):
            if isinstance(pair, list) and len(pair) == 2:
                tickers.append(pair[0])

        logger.info(f"{self.dex}: found {len(tickers)} tickers")

        # Step 2: Download candles
        prefix = f"{self.dex}:"
        candles_by_ticker = {}
        for ticker in tickers:
            short = ticker.replace(prefix, "")
            for coin_name, pdex in [(ticker, self.dex), (short, self.dex), (ticker, None), (short, None)]:
                raw = _try_candle_download(coin_name, pdex)
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
        daily_vol, vol_by_ticker = {}, {}
        for ticker, rows in candles_by_ticker.items():
            ticker_total = 0
            for row in rows:
                daily_vol[row["date"]] = daily_vol.get(row["date"], 0) + row["volume_usd"]
                ticker_total += row["volume_usd"]
            vol_by_ticker[ticker] = ticker_total

        total_cum_vol = sum(daily_vol.values())
        num_days = len(daily_vol)
        avg_daily = total_cum_vol / num_days if num_days > 0 else 0
        sorted_dates = sorted(daily_vol.keys())
        last_7 = sorted_dates[-7:] if len(sorted_dates) >= 7 else sorted_dates
        last_30 = sorted_dates[-30:] if len(sorted_dates) >= 30 else sorted_dates
        avg_7d = sum(daily_vol[d] for d in last_7) / len(last_7) if last_7 else 0
        avg_30d = sum(daily_vol[d] for d in last_30) / len(last_30) if last_30 else 0

        # days since first data point
        if sorted_dates:
            first_date = datetime.strptime(sorted_dates[0], "%Y-%m-%d")
            days_since_launch = (datetime.now() - first_date).days or 1
        else:
            days_since_launch = 1

        # Step 4: On-chain fees
        ch = hl_post(
            {"type": "clearinghouseState", "user": self.cfg["fee_recipient"], "dex": self.dex},
            f"CH {self.dex}",
        )
        deployer_fees = float(ch.get("marginSummary", {}).get("accountValue", "0")) if ch else 0

        total_builder = 0.0
        queried = set()
        addrs_to_check = list(self.cfg["builders"]) + [self.cfg["fee_recipient"]]
        for addr in addrs_to_check:
            if not addr or addr in queried:
                continue
            queried.add(addr)
            ref = hl_post({"type": "referral", "user": addr}, f"ref {addr[:8]}")
            total_builder += float(ref.get("builderRewards", "0")) if ref else 0

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
            discount = self.cfg.get("growth_discount")
            normal_deployer_bps = eff_deployer_bps / discount if discount else eff_deployer_bps

        # Step 5: Net deposit
        dex_status = hl_post({"type": "perpDexStatus", "dex": self.dex}, f"status {self.dex}")
        total_net_deposit = float(dex_status.get("totalNetDeposit", "0")) if dex_status else 0

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
            })

        # Projections
        daily_b = total_builder / days_since_launch
        ann_b = daily_b * 365
        projections = {}
        for label, avg_d in [("last_7d", avg_7d), ("last_30d", avg_30d)]:
            if avg_d > 0 and eff_deployer_bps > 0:
                ann_vol = avg_d * 365
                ann_dg = ann_vol * eff_deployer_bps / 10000
                ann_dn = ann_vol * normal_deployer_bps / 10000
                projections[label] = {
                    "avg_daily_volume": round(avg_d),
                    "growth_mode": {"deployer": round(ann_dg), "builder": round(ann_b), "total": round(ann_dg + ann_b)},
                    "normal_mode": {"deployer": round(ann_dn), "builder": round(ann_b), "total": round(ann_dn + ann_b)},
                }

        # Ticker chart
        sorted_tickers = sorted(vol_by_ticker.items(), key=lambda x: -x[1])
        ticker_chart = [
            {"ticker": t.replace(prefix, ""), "volume": round(v), "pct": round(v / total_cum_vol * 100, 1) if total_cum_vol > 0 else 0}
            for t, v in sorted_tickers
        ]

        self.data = {
            "dex": self.dex,
            "generated_at": now_str,
            "days_since_launch": days_since_launch,
            "num_tickers": len(tickers),
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
            },
            "averages": {"daily": round(avg_daily), "avg_7d": round(avg_7d), "avg_30d": round(avg_30d)},
            "projections": projections,
            "daily_chart": daily_chart,
            "ticker_chart": ticker_chart,
        }
        self.last_updated = now_str
        logger.info(f"{self.dex} revenue: ${total_cum_vol:,.0f} vol, ${total_fees:,.2f} fees")
