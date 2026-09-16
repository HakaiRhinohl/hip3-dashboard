"""
KNTQ supply, market cap and float -- the denominators for buyback yield.

Circulating supply has no single on-chain answer. Hyperliquid's own tokenDetails
reports ~1,000M as circulating because it only excludes the zero and dead
addresses, so treasury and unvested allocations count. The market-facing figure
is CoinGecko's (~280M), and it is also what the published memo's earlier "33%
staked" figure implies, so that is the basis used here -- and the source is
named in the payload rather than presented as a chain fact.

The float then removes two things that are verifiable on-chain:
  - KNTQ held by the sKNTQ contract on HyperEVM. Under KIP-5 (2026-09-15) sKNTQ
    stopped receiving buybacks and its role is undecided, so this is expected to
    fall; it is read live rather than assumed.
  - KNTQ held by the Hyperliquid Assistance Fund. Nobody controls that address,
    so anything sent there is out of circulation for good. Only part of it is
    traceable to Kinetiq -- the rest has senders this does not identify -- and
    the two are reported separately.
"""

import logging
from datetime import datetime, timezone

import requests

from schedulers import hl_post
from schedulers.lst import _eth_call

logger = logging.getLogger("kinetiq.kntq_supply")

KNTQ_TOKEN_ID = "0xbd31bd605c0a1b82c72aae3587f9061f"
KNTQ_EVM = "0x000000000000780555bd0bca3791f89f9542c2d6"
SKNTQ = "0x696238e0ca31c94e24ca4cbe7921754e172e4d0f"
ASSISTANCE_FUND = "0xfefefefefefefefefefefefefefefefefefefefe"
KNTQ_SPOT_DEPLOYER = "0x51172933b60847085e2a959e860e2ec9e240ac09"
KIP5_EFFECTIVE = "2026-09-15"

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/kinetiq"
    "?localization=false&tickers=false&community_data=false&developer_data=false"
)

_last: dict | None = None


def _coingecko() -> dict | None:
    try:
        r = requests.get(COINGECKO_URL, timeout=20, headers={"accept": "application/json"})
        if r.status_code != 200:
            logger.warning(f"coingecko returned {r.status_code}")
            return None
        m = r.json().get("market_data") or {}
        return {
            "price_usd": m.get("current_price", {}).get("usd"),
            "circulating_supply": m.get("circulating_supply"),
            "market_cap_usd": m.get("market_cap", {}).get("usd"),
            "as_of": r.json().get("last_updated"),
        }
    except Exception as exc:
        logger.warning(f"coingecko unavailable: {exc}")
        return None


def _af_balance() -> float | None:
    st = hl_post({"type": "spotClearinghouseState", "user": ASSISTANCE_FUND}, "AF balances")
    if not isinstance(st, dict):
        return None
    for b in st.get("balances") or []:
        if b.get("coin") == "KNTQ":
            return float(b.get("total") or 0)
    return 0.0


def _sent_to_af(address: str) -> float:
    """KNTQ an address has sent to the Assistance Fund, from its ledger."""
    from schedulers.buyback_origin import _ledger
    total = 0.0
    for rec in _ledger(address):
        d = rec.get("delta", {})
        if d.get("token") == "KNTQ" and (d.get("destination") or "").lower() == ASSISTANCE_FUND:
            try:
                total += float(d.get("amount") or 0)
            except (TypeError, ValueError):
                continue
    return total


def fetch_kntq_supply() -> dict | None:
    global _last
    market = _coingecko()
    burned = _af_balance()
    skntq_supply = _eth_call(SKNTQ, "0x18160ddd")
    staked_raw = _eth_call(KNTQ_EVM, "0x70a08231" + "0" * 24 + SKNTQ[2:])

    # CoinGecko rate-limits the free tier; keep the last good market read
    # rather than blanking mcap and float for a cycle.
    if not market and _last:
        market = _last.get("market")
    if not market or burned is None or staked_raw is None:
        logger.warning("KNTQ supply incomplete this cycle; serving last good read")
        return _last

    staked = staked_raw / 1e18
    sk_sup = (skntq_supply or 0) / 1e18
    circ = float(market["circulating_supply"] or 0)
    price = float(market["price_usd"] or 0)
    float_tokens = max(circ - staked - burned, 0.0)

    from_spot = _sent_to_af(KNTQ_SPOT_DEPLOYER)
    from_skntq = _sent_to_af(SKNTQ)
    traced = from_spot + from_skntq

    _last = {
        "market": market,
        "price_usd": price,
        "circulating_supply": circ,
        "market_cap_usd": market.get("market_cap_usd") or circ * price,
        "staked_kntq": round(staked, 2),
        "skntq_supply": round(sk_sup, 2),
        "skntq_ratio": round(staked / sk_sup, 4) if sk_sup else None,
        "staked_pct_of_circulating": round(staked / circ * 100, 2) if circ else None,
        "burned_kntq": round(burned, 2),
        "burned_pct_total_supply": round(burned / 1e9 * 100, 3),
        "burned_traced": {
            "from_spot_fees": round(from_spot, 2),
            "from_skntq_kip5": round(from_skntq, 2),
            "total": round(traced, 2),
            "unattributed": round(max(burned - traced, 0.0), 2),
        },
        "float_kntq": round(float_tokens, 2),
        "float_usd": round(float_tokens * price, 2),
        "kip5": {
            "effective": KIP5_EFFECTIVE,
            "summary": "Purchased KNTQ is sent to the Hyperliquid Assistance Fund instead of sKNTQ holders.",
        },
        "sources": {
            "price_circulating_mcap": "CoinGecko",
            "staked": "sKNTQ contract on HyperEVM",
            "burned": "Hyperliquid Assistance Fund spot balance",
        },
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }
    logger.info(
        f"KNTQ supply: mcap ${_last['market_cap_usd']:,.0f}, float ${_last['float_usd']:,.0f}, "
        f"staked {staked:,.0f}, burned {burned:,.0f}"
    )
    return _last
