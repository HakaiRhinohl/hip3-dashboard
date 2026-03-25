"""
Comparison collector — adapted from 10_hip3_market_comparison.py
Compares km, xyz, flx, cash dexes: volume, fees, implied revenue.
"""

import logging
import time
from datetime import datetime, timezone

from schedulers import hl_post

logger = logging.getLogger("kinetiq.comparison")

DEXES = ["km", "xyz", "flx", "cash"]
DEX_NAMES = {
    "km": "Markets (Kinetiq)",
    "xyz": "Trade.xyz",
    "flx": "Felix",
    "cash": "Dreamcash",
}

KNOWN_ADDRESSES = {
    "km": {
        "fee_recipient": "0xbcd4071d023bf2aae484d724c130b5af6f0ca0d2",
        "deployer": "0x75e05d6bc77ce5e288a9f0e935e5e75fa5c0a700",
        "trading_builder": "0x42f3226007290b02c5a0b15bccbb1ba6df04f992",
        "staking_builder": "0x4ec89c1c70ca1e2f224bb43e28d122f4d2b4e8bb",
    },
    "xyz": {
        "fee_recipient": "0x9cd0a696c7cbb9d44de99268194cb08e5684e5fe",
        "deployer": "0x88806a71d74ad0a510b350545c9ae490912f0888",
    },
    "flx": {
        "fee_recipient": "0xe2872b5ae7dcbba40cc4510d08c8bbea95b42d43",
        "deployer": "0x2fab552502a6d45920d5741a2f3ebf4c35536352",
        "builder": "0x2157f54f7a745c772e686aa691fa590b49171ec9",
    },
    "cash": {
        "fee_recipient": "0xaa7f0d3da989dae8fd166345a3ce21509f8c8bb4",
        "deployer": "0xffa8198c62adb1e811629bd54c9b646d726deef7",
        "builder": "0x4950994884602d1b6c6d96e4fe30f58205c39395",
    },
}

KM_EFF_BPS_GROWTH = 0.4074
KM_EFF_BPS_NORMAL = 4.0743
GROWTH_DISCOUNT = 0.10
EARLIEST_MS = int(datetime(2025, 10, 1).timestamp() * 1000)


def try_candle(coin: str, pdex: str, start_ms: int, end_ms: int) -> list:
    """Try downloading candles with multiple name formats."""
    attempts = [
        (coin, pdex),
        (coin.split(":")[-1] if ":" in coin else coin, pdex),
        (coin, None),
    ]
    for c, p in attempts:
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": c, "interval": "1d", "startTime": start_ms, "endTime": end_ms},
        }
        if p:
            payload["perpDex"] = p
        result = hl_post(payload, f"candle {c}")
        if isinstance(result, list) and len(result) > 0:
            return result
        time.sleep(0.1)
    return []


class ComparisonCollector:
    def __init__(self):
        self.data = None
        self.last_updated = None

    def get_data(self) -> dict:
        if self.data is None:
            return {"status": "loading", "message": "Initial collection in progress"}
        return self.data

    def collect(self):
        """Run full comparison collection."""
        logger.info("Starting comparison collection...")
        now_ms = int(datetime.now().timestamp() * 1000)
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        # Get all perpDexs config
        all_dexes_raw = hl_post({"type": "perpDexs"}, "perpDexs")
        dex_config = {}
        if all_dexes_raw:
            for d in all_dexes_raw:
                if d and isinstance(d, dict):
                    name = d.get("name", "")
                    if name in DEXES:
                        dex_config[name] = d

        results = {}

        for dex in DEXES:
            logger.info(f"Collecting {DEX_NAMES[dex]}...")
            r = {"name": DEX_NAMES[dex], "dex": dex}

            # perpDexLimits -> tickers
            limits = hl_post({"type": "perpDexLimits", "dex": dex}, f"limits {dex}")
            tickers = []
            oi_caps = {}
            if limits:
                for pair in limits.get("coinToOiCap", []):
                    if isinstance(pair, list) and len(pair) == 2:
                        tickers.append(pair[0])
                        oi_caps[pair[0]] = float(pair[1])

            r["tickers"] = tickers
            r["num_tickers"] = len(tickers)

            # perpDexStatus -> net deposit
            status = hl_post({"type": "perpDexStatus", "dex": dex}, f"status {dex}")
            r["total_net_deposit"] = float(status.get("totalNetDeposit", "0")) if status else 0

            # perpDexs config -> deployer, feeRecipient
            cfg = dex_config.get(dex, {})
            r["deployer_addr"] = cfg.get("deployer", "")
            r["fee_recipient"] = cfg.get("feeRecipient", "")

            # Override with known addresses
            if dex in KNOWN_ADDRESSES:
                for k, v in KNOWN_ADDRESSES[dex].items():
                    if k not in r or not r[k]:
                        r[k] = v

            # Deployer fees
            if r.get("fee_recipient"):
                ch = hl_post(
                    {"type": "clearinghouseState", "user": r["fee_recipient"], "dex": dex},
                    f"CH {dex}",
                )
                r["deployer_fees"] = float(ch.get("marginSummary", {}).get("accountValue", "0")) if ch else 0
            else:
                r["deployer_fees"] = 0

            # Builder fees — query all known builder addresses + fee_recipient + deployer (deduped)
            builder_total = 0
            queried_addrs = set()

            def _add_builder_rewards(addr, label):
                nonlocal builder_total
                if not addr or addr in queried_addrs:
                    return
                queried_addrs.add(addr)
                ref = hl_post({"type": "referral", "user": addr}, f"ref {dex} {label}")
                br = float(ref.get("builderRewards", "0")) if ref else 0
                if br > 0:
                    builder_total += br

            if dex in KNOWN_ADDRESSES:
                for bkey in ["trading_builder", "staking_builder", "builder"]:
                    _add_builder_rewards(KNOWN_ADDRESSES[dex].get(bkey), bkey)

            _add_builder_rewards(r.get("fee_recipient"), "fee_recipient")
            _add_builder_rewards(r.get("deployer_addr"), "deployer")

            r["builder_fees"] = builder_total
            r["total_fees"] = r["deployer_fees"] + builder_total

            # Download candles for all tickers
            daily_vol = {}
            vol_by_ticker = {}

            for ticker in tickers:
                raw = try_candle(ticker, dex, EARLIEST_MS, now_ms)
                ticker_total = 0
                for c in raw:
                    t_ms = c.get("t", c.get("T", 0))
                    c_px = float(c.get("c", "0"))
                    v = float(c.get("v", "0"))
                    vol_usd = v * c_px if c_px > 0 else 0
                    date_str = datetime.fromtimestamp(t_ms / 1000).strftime("%Y-%m-%d")
                    daily_vol[date_str] = daily_vol.get(date_str, 0) + vol_usd
                    ticker_total += vol_usd
                vol_by_ticker[ticker] = ticker_total

            total_vol = sum(daily_vol.values())
            num_days = len(daily_vol)
            sorted_dates = sorted(daily_vol.keys())
            last_7 = sorted_dates[-7:] if len(sorted_dates) >= 7 else sorted_dates
            last_30 = sorted_dates[-30:] if len(sorted_dates) >= 30 else sorted_dates
            avg_7d = sum(daily_vol[d] for d in last_7) / len(last_7) if last_7 else 0
            avg_30d = sum(daily_vol[d] for d in last_30) / len(last_30) if last_30 else 0

            r["volume"] = {
                "cumulative": round(total_vol),
                "num_days": num_days,
                "avg_daily": round(total_vol / num_days) if num_days > 0 else 0,
                "avg_7d": round(avg_7d),
                "avg_30d": round(avg_30d),
                "first_date": sorted_dates[0] if sorted_dates else None,
                "last_date": sorted_dates[-1] if sorted_dates else None,
            }
            r["daily_vol"] = daily_vol

            # Effective rate
            if total_vol > 0 and r["deployer_fees"] > 0:
                r["eff_deployer_bps"] = round((r["deployer_fees"] / total_vol) * 10000, 4)
                r["eff_total_bps"] = round((r["total_fees"] / total_vol) * 10000, 4)
            else:
                r["eff_deployer_bps"] = 0
                r["eff_total_bps"] = 0

            # Implied Kinetiq revenue
            r["implied_km"] = {
                "growth_ann": round(avg_30d * 365 * KM_EFF_BPS_GROWTH / 10000),
                "normal_ann": round(avg_30d * 365 * KM_EFF_BPS_NORMAL / 10000),
            }

            # Top tickers
            sorted_tickers = sorted(vol_by_ticker.items(), key=lambda x: -x[1])[:10]
            r["top_tickers"] = [
                {"ticker": t, "volume": round(v), "pct": round(v / total_vol * 100, 1) if total_vol > 0 else 0}
                for t, v in sorted_tickers
            ]

            results[dex] = r
            logger.info(f"  {DEX_NAMES[dex]}: ${total_vol:,.0f} volume, {len(tickers)} tickers")

        # Build unified daily chart (all dexes on same date axis)
        all_dates = set()
        for dex in DEXES:
            all_dates.update(results[dex].get("daily_vol", {}).keys())
        all_dates = sorted(all_dates)

        daily_chart = []
        cum = {d: 0 for d in DEXES}
        for date in all_dates:
            row = {"date": date}
            for dex in DEXES:
                vol = results[dex].get("daily_vol", {}).get(date, 0)
                cum[dex] += vol
                row[f"{dex}_vol"] = round(vol, 2)
                row[f"{dex}_cum"] = round(cum[dex], 2)
            daily_chart.append(row)

        # Build summary cards
        dex_summaries = []
        for dex in DEXES:
            r = results[dex]
            v = r.get("volume", {})
            dex_summaries.append({
                "dex": dex,
                "name": DEX_NAMES[dex],
                "num_tickers": r["num_tickers"],
                "num_days": v.get("num_days", 0),
                "cum_volume": v.get("cumulative", 0),
                "deployer_fees": r.get("deployer_fees", 0),
                "builder_fees": r.get("builder_fees", 0),
                "total_fees": r.get("total_fees", 0),
                "eff_deployer_bps": r.get("eff_deployer_bps", 0),
                "total_net_deposit": r.get("total_net_deposit", 0),
                "avg_7d": v.get("avg_7d", 0),
                "avg_30d": v.get("avg_30d", 0),
                "implied_km_growth_ann": r.get("implied_km", {}).get("growth_ann", 0),
                "implied_km_normal_ann": r.get("implied_km", {}).get("normal_ann", 0),
                "top_tickers": r.get("top_tickers", []),
            })

        self.data = {
            "generated_at": now_str,
            "km_bps": {"growth": KM_EFF_BPS_GROWTH, "normal": KM_EFF_BPS_NORMAL},
            "dex_summaries": dex_summaries,
            "daily_chart": daily_chart,
        }
        self.last_updated = now_str
        logger.info("Comparison data updated")
