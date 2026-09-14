"""
Live LST metrics, read from HyperEVM and Hyperliquid instead of a hand-updated
snapshot.

What is actually readable on-chain, and therefore live here:
  - Each Kinetiq LST's TVL. An LST's HYPE backing is its token supply times the
    protocol's own kHYPE->HYPE rate; the StakingAccountant exposes that rate.
    `totalStaked` on those contracts is NOT usable for this -- it is a gross
    cumulative counter (52M against ~12.9M actually staked, with `totalClaimed`
    tracking the other side), and the 2025 audit flagged its accounting.
  - The HYPE price (spot mid) and the staking APR (Kinetiq's validator).

What stays in the snapshot in revenue.py, because no RPC can produce it:
  historical revenue, treasury splits, quarterly figures, and the policy
  constants (10% performance fee, the 70/30 buyback/treasury split).

Addresses are from kinetiq.xyz/docs/contracts-and-audits. kmHYPE is deliberately
kept out of the kHYPE total: it is a separate product with its own card in the
dashboard, so folding it in would double-count it.
"""

import copy
import logging

import requests

from schedulers import hl_post

logger = logging.getLogger("kinetiq.lst")

RPC_URL = "https://rpc.hyperliquid.xyz/evm"
RPC_TIMEOUT = 15

# selector for totalSupply()
SEL_TOTAL_SUPPLY = "0x18160ddd"
# selector for kHYPEToHYPE(uint256), called with 1e18 to read the rate
SEL_RATE = "0x759bc2fc" + f"{10**18:064x}"

# name -> (token, stakingAccountant)
KHYPE_FAMILY = {
    "kHYPE":    ("0xfD739d4e423301CE9385c1fb8850539D657C296D", "0x9209648Ec9D448EF57116B73A2f081835643dc7A"),
    "flowHYPE": ("0x86d96fF0E78Dba9570b00f75807ce21213a19f3d", "0x968c6AB57CDe284eABA42D811b633BF27BdACC03"),
    "HiHYPE":   ("0x4f322145aBedb2b39f69e7d4531AB4B2e6483154", "0x62E6fa898761dE2345aB3d507b72c422a2829733"),
    "asxnHYPE": ("0x8599F2eFA5064C666B920E71381b5aaBc7Bb27F6", "0x2312Dd3349De01C2370b5BFFe3Ce5B4f69eAb797"),
    "hylqHYPE": ("0x498edC41Fa92530920a95483dea7a6CCe91F1C5c", "0x5D544F496FC2E7189375e749E4813dA82a91Cb9e"),
}
KMHYPE = ("0x360C140E5344A1A0593D44B4ea6Fc7C3DAf0C473", "0x5901e744759561C63309865Ef8822aBb041655E2")

# Kinetiq x Hyperion, the validator Kinetiq runs; its predicted APR is the
# gross staking rate before the protocol's performance fee.
KINETIQ_VALIDATOR = "0xeeee86f718f9da3e7250624a460f6ea710e9c006"

# Every HYPE LST on HyperEVM, for the ecosystem share view. Each address was
# verified by calling symbol() on it -- searching by ticker alone is unsafe,
# e.g. an unrelated 568M-supply token also answers to "HYPED".
#
# `provider` is the team the LST belongs to and is what the share chart groups
# by; the table breaks the individual tokens back out.
#
# Comparison here is on token supply, valued at the HYPE mid. Each LST accrues
# at its own rate and only Kinetiq's expose one we can read, so converting some
# to HYPE backing and not others would make the shares incomparable.
ECOSYSTEM_LSTS = [
    {"symbol": "kHYPE",    "provider": "Kinetiq",    "token": "0xfD739d4e423301CE9385c1fb8850539D657C296D"},
    {"symbol": "kmHYPE",   "provider": "Kinetiq",    "token": "0x360C140E5344A1A0593D44B4ea6Fc7C3DAf0C473", "note": "Markets-linked"},
    {"symbol": "HiHYPE",   "provider": "Kinetiq",    "token": "0x4f322145aBedb2b39f69e7d4531AB4B2e6483154", "note": "institutional"},
    {"symbol": "flowHYPE", "provider": "Kinetiq",    "token": "0x86d96fF0E78Dba9570b00f75807ce21213a19f3d", "note": "institutional"},
    {"symbol": "hylqHYPE", "provider": "Kinetiq",    "token": "0x498edC41Fa92530920a95483dea7a6CCe91F1C5c", "note": "institutional"},
    {"symbol": "asxnHYPE", "provider": "Kinetiq",    "token": "0x8599F2eFA5064C666B920E71381b5aaBc7Bb27F6", "note": "institutional"},
    {"symbol": "stHYPE",   "provider": "Valantis",   "token": "0xffaa4a3d97fe9107cef8a3f48c069f577ff76cc1", "note": "built by Thunderhead, acquired by Valantis in Aug 2025"},
    {"symbol": "vHYPE",    "provider": "Ventuals",   "token": "0x8888888fdaac0e7cf8c6523c8955bf7954c216fa", "note": "winding down since Jun 2026, redeeming for HYPE"},
    {"symbol": "beHYPE",   "provider": "Hyperbeat",  "token": "0xd8fc8f0b03eba61f64d08b0bef69d80916e5dda9", "note": "built with ether.fi"},
    {"symbol": "mHYPE",    "provider": "Hyperpie",   "token": "0xdabb040c428436d41cecd0fb06bcfdbaad3a9aa8", "note": "Magpie SubDAO, rebranded SpinUp"},
    # Addresses not resolved yet -- left here so the share view can say what it
    # is missing instead of silently under-reporting the ecosystem total.
    {"symbol": "iHYPE",    "provider": "Kinetiq",    "token": None, "note": "institutional pool"},
    {"symbol": "HYPED",    "provider": "Hyperdrive", "token": None, "note": "CoreWriter + precompiles"},
    {"symbol": "sHYPE",    "provider": "Kintsu",     "token": None},
    {"symbol": "aHYPE",    "provider": "AlphaTicks", "token": None, "note": "AlphaTicks absorbed by Hfun Labs"},
]


def _eth_call(to: str, data: str) -> int | None:
    """One eth_call returning a single uint256, or None if anything is off."""
    try:
        r = requests.post(
            RPC_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                  "params": [{"to": to, "data": data}, "latest"]},
            timeout=RPC_TIMEOUT,
        )
        r.raise_for_status()
        result = r.json().get("result")
    except Exception as exc:
        logger.warning(f"eth_call {to[:10]} {data[:10]} failed: {exc}")
        return None
    if not result or result == "0x":
        return None
    try:
        return int(result, 16)
    except ValueError:
        return None


def _lst_backing(name: str, token: str, accountant: str) -> dict | None:
    """HYPE backing one LST: supply x the protocol's own conversion rate."""
    supply = _eth_call(token, SEL_TOTAL_SUPPLY)
    rate = _eth_call(accountant, SEL_RATE)
    if supply is None or rate is None:
        logger.warning(f"{name}: missing supply or rate, skipping")
        return None
    supply_f = supply / 1e18
    rate_f = rate / 1e18
    return {
        "name": name,
        "supply": round(supply_f, 4),
        "rate": round(rate_f, 8),
        "hype": round(supply_f * rate_f, 4),
    }


def _hype_price() -> float | None:
    mids = hl_post({"type": "allMids"}, "allMids")
    if not isinstance(mids, dict):
        return None
    try:
        return float(mids["HYPE"])
    except (KeyError, TypeError, ValueError):
        return None


def _staking_apr() -> float | None:
    summaries = hl_post({"type": "validatorSummaries"}, "validatorSummaries")
    if not isinstance(summaries, list):
        return None
    for v in summaries:
        if not isinstance(v, dict) or v.get("validator") != KINETIQ_VALIDATOR:
            continue
        for entry in v.get("stats", []):
            if isinstance(entry, list) and len(entry) == 2 and entry[0] == "month":
                try:
                    return float(entry[1].get("predictedApr"))
                except (TypeError, ValueError, AttributeError):
                    return None
    return None


def fetch_live_lst() -> dict | None:
    """
    Live LST metrics, or None if the chain data can't be read. Callers fall
    back to the audited snapshot rather than showing a partial figure.
    """
    components = []
    for name, (token, accountant) in KHYPE_FAMILY.items():
        row = _lst_backing(name, token, accountant)
        if row:
            components.append(row)

    # kHYPE itself dominates the total; without it there is no usable number.
    if not any(c["name"] == "kHYPE" for c in components):
        logger.warning("kHYPE backing unavailable; not publishing live LST data")
        return None

    price = _hype_price()
    if price is None:
        logger.warning("HYPE price unavailable; not publishing live LST data")
        return None

    khype_hype = sum(c["hype"] for c in components)
    kmhype = _lst_backing("kmHYPE", *KMHYPE)
    apr = _staking_apr()

    live = {
        "source": "live",
        "hype_price_usd": round(price, 4),
        "khype": {
            "tvl_hype": round(khype_hype, 2),
            "tvl_usd": round(khype_hype * price, 2),
            "components": components,
        },
    }
    if apr is not None:
        live["khype"]["gross_staking_apr"] = round(apr, 6)
        live["khype"]["implied_annual_gross_rewards_usd"] = round(khype_hype * price * apr, 2)
    if kmhype:
        live["kmhype"] = {
            "tvl_hype": kmhype["hype"],
            "tvl_usd": round(kmhype["hype"] * price, 2),
            "supply": kmhype["supply"],
            "rate": kmhype["rate"],
        }

    ecosystem = fetch_ecosystem_share(price)
    if ecosystem:
        live["ecosystem"] = ecosystem

    logger.info(
        f"LST live: {khype_hype:,.0f} HYPE across {len(components)} LSTs "
        f"@ ${price:,.2f}" + (f", APR {apr:.4%}" if apr is not None else "")
    )
    return live


def fetch_ecosystem_share(price: float | None = None) -> dict | None:
    """
    Supply of every HYPE LST on HyperEVM, grouped by provider.

    Returns None only if nothing could be read at all; a single unreachable
    token is reported in `unresolved` rather than dropped silently, so the
    share percentages can be read knowing what is missing from them.
    """
    if price is None:
        price = _hype_price()
    if price is None:
        logger.warning("HYPE price unavailable; skipping ecosystem share")
        return None

    tokens, unresolved = [], []
    for entry in ECOSYSTEM_LSTS:
        if not entry.get("token"):
            unresolved.append({"symbol": entry["symbol"], "provider": entry["provider"],
                               "reason": "address not configured"})
            continue
        raw = _eth_call(entry["token"], SEL_TOTAL_SUPPLY)
        if raw is None:
            unresolved.append({"symbol": entry["symbol"], "provider": entry["provider"],
                               "reason": "supply unreadable"})
            continue
        supply = raw / 1e18
        tokens.append({
            "symbol": entry["symbol"],
            "provider": entry["provider"],
            "token": entry["token"],
            "note": entry.get("note"),
            "supply": round(supply, 4),
            "supply_usd": round(supply * price, 2),
        })

    if not tokens:
        logger.warning("no LST supplies readable; skipping ecosystem share")
        return None

    total = sum(t["supply"] for t in tokens)
    for t in tokens:
        t["share_pct"] = round(t["supply"] / total * 100, 4) if total > 0 else 0
    tokens.sort(key=lambda t: -t["supply"])

    by_provider: dict[str, dict] = {}
    for t in tokens:
        p = by_provider.setdefault(t["provider"], {"provider": t["provider"], "supply": 0.0, "tokens": []})
        p["supply"] += t["supply"]
        p["tokens"].append(t["symbol"])
    providers = []
    for p in by_provider.values():
        providers.append({
            "provider": p["provider"],
            "supply": round(p["supply"], 4),
            "supply_usd": round(p["supply"] * price, 2),
            "share_pct": round(p["supply"] / total * 100, 4) if total > 0 else 0,
            "tokens": p["tokens"],
        })
    providers.sort(key=lambda p: -p["supply"])

    logger.info(
        f"LST share: {total:,.0f} HYPE-denominated supply across {len(tokens)} LSTs / "
        f"{len(providers)} providers" + (f", {len(unresolved)} unresolved" if unresolved else "")
    )
    return {
        "hype_price_usd": round(price, 4),
        "total_supply": round(total, 4),
        "total_supply_usd": round(total * price, 2),
        "providers": providers,
        "tokens": tokens,
        "unresolved": unresolved,
        "basis": "token supply valued at the HYPE mid; each LST accrues at its own rate",
    }


def merge_with_snapshot(snapshot: dict, live: dict | None, now_str: str) -> dict:
    """
    Overlay live chain data on the audited snapshot.

    Only the fields an RPC can actually produce are replaced; historical and
    policy fields always come from the snapshot. When the chain read fails the
    snapshot is returned untouched and flagged, so the dashboard can say the
    numbers are a dated snapshot rather than quietly showing stale values as live.
    """
    merged = copy.deepcopy(snapshot)

    if not live:
        merged["source"] = "snapshot"
        merged["live_as_of"] = None
        return merged

    merged["source"] = "live"
    merged["live_as_of"] = now_str
    merged["hype_price_usd"] = live.get("hype_price_usd")

    khype_live = live.get("khype", {})
    khype = merged.setdefault("khype", {})
    for key in ("tvl_hype", "tvl_usd", "gross_staking_apr", "implied_annual_gross_rewards_usd"):
        if khype_live.get(key) is not None:
            khype[key] = khype_live[key]
    khype["components"] = khype_live.get("components", [])

    if live.get("kmhype"):
        merged.setdefault("kmhype", {}).update(live["kmhype"])
    if live.get("ecosystem"):
        merged["ecosystem"] = live["ecosystem"]

    return merged
