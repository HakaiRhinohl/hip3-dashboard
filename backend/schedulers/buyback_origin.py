"""
What funded the KNTQ buybacks, resolved to the business line the money came from.

The buyback wallet's own inbound list stops at whoever sent the transfer, which
leaves most of it unattributed: half a dozen of its funders are forwarding
wallets, and the largest single one -- the Markets fee recipient -- is a
treasury hub where deployer fees, builder transfers, staking revenue and the
kmHYPE and KNTQ-spot streams all arrive and leave together.

So funders are walked back one hop, and the fee recipient is resolved by
attributing each of its payments to the mix of money sitting in it at that
moment. That is the pooled (average-cost) convention: it accepts that dollars
are fungible, which they are, while still refusing to let a deposit fund a
payment that happened before it -- which attributing over the whole history
silently does, and which is not a convention but an error.

FIFO on the same data puts Markets at $62.9K against this method's $73.1K, so
the choice of causal convention moves the answer by about a sixth. Attribution
stops at one hop: past that the counterparties fan out into unrelated accounts
moving far larger sums, where any split would be invention.
"""

import logging
import time
from collections import defaultdict

from schedulers import hl_post
from schedulers.reservoir_fees import daily_fees

logger = logging.getLogger("kinetiq.buyback_origin")

BUYBACKS = "0xaa3b7392052d62928cc87701e3ca6fb6630bb6e2"
FEE_RECIPIENT = "0xbcd4071d023bf2aae484d724c130b5af6f0ca0d2"

# Wallets that only ever hold one kind of money, so a transfer out of them
# needs no further resolution.
DIRECT = {
    "0x42f3226007290b02c5a0b15bccbb1ba6df04f992": "Builder code (Markets)",
    "0x2af94a24e1f744a8e251b4996283ffb4657e915d": "Builder code (Markets)",
    "0x9cb4ac2598bf72d3f0b53efd817aac5cebe796fc": "Builder code (Markets)",
    "0x55758d720e0f32328f7f1e1b3de6b637e0bec4ba": "kHYPE / staking",
    "0xeeee86f718f9da3e7250624a460f6ea710e9c006": "kHYPE / staking",
    "0x2222222222222222222222222222222222222222": "kHYPE / staking",
    "0x132213efec1f9b999c063ff5aeb5c623934e7b4b": "kHYPE / staking",
    "0x51172933b60847085e2a959e860e2ec9e240ac09": "KNTQ spot",
    "0x032e0834c48f354c47415e2fa77b868db7d27c38": "kmHYPE",
}
# Inside the fee recipient, money coming back from a wallet it funded is not
# fresh income and is reported as such rather than double counted.
RECYCLED = {
    "0x537e3d1740d92a57add46508fa1766dea913f160",
    "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24",
    "0x111111a1a0667d36bd57c0a9f569b98057111111",
}
DOLLARS = ("USDC", "USDH", "USDE", "USDT0")
UNRESOLVED = "Recycled / unattributable"

# The revenue collector reports how much Markets has funded, and it must be the
# same figure the buybacks page shows. It used to be a pinned constant from an
# earlier one-hop pass ($160.6K), which drifted to more than double what this
# module computes once the fee recipient's mix was resolved. Both now read the
# last result of the one computation.
_last_result: dict | None = None


def last_funding() -> dict | None:
    """
    Latest attribution, falling back to the buybacks collector's cache on disk.

    The in-memory result resets on every restart, and the revenue collector runs
    before the buybacks collector at startup, so without the fallback the
    Markets-delivered figure shows as empty for the first cycle after each
    deploy.
    """
    if _last_result is not None:
        return _last_result
    try:
        import json
        import os
        path = os.path.join(os.environ.get("CACHE_DIR", "/data"), "buybacks.json")
        with open(path) as f:
            return (json.load(f).get("data") or {}).get("funding_composition")
    except Exception:
        return None


def _ledger(address: str) -> list:
    """Paginated non-funding ledger; the endpoint caps a page at 2000 records."""
    out, seen, start = [], set(), 0
    for _ in range(80):
        page = hl_post(
            {"type": "userNonFundingLedgerUpdates", "user": address, "startTime": start},
            f"ledger {address[:8]}",
        )
        if not isinstance(page, list) or not page:
            break
        for rec in page:
            key = (rec.get("time"), rec.get("hash"), str(rec.get("delta")))
            if key not in seen:
                seen.add(key)
                out.append(rec)
        if len(page) < 2000:
            break
        newest = max(r.get("time", 0) for r in page)
        if newest <= start:
            break
        start = newest
    out.sort(key=lambda r: r.get("time", 0))
    return out


def _transfers(recs: list, addr: str, dollars_only: bool = True):
    """Yield (time, direction, counterparty, usd) for transfer-type deltas."""
    a = addr.lower()
    for r in recs:
        d = r.get("delta", {})
        if d.get("type") not in ("send", "spotTransfer", "internalTransfer", "withdraw", "deposit"):
            continue
        if dollars_only and (d.get("token") or "USDC") not in DOLLARS:
            continue
        try:
            usd = float(d.get("usdcValue") or d.get("usdc") or d.get("amount") or 0)
        except (TypeError, ValueError):
            continue
        if usd <= 0:
            continue
        src, dst = (d.get("user") or "").lower(), (d.get("destination") or "").lower()
        if dst == a and src and src != a:
            yield r.get("time", 0), "in", src, usd
        elif src == a and dst and dst != a:
            yield r.get("time", 0), "out", dst, usd


def _fee_recipient_mix() -> dict:
    """
    Origin mix of every dollar the fee recipient paid out, by destination.

    Deployer fees accrue to this account without a ledger entry, so the
    reservoir's per-day deployer totals are injected as daily inflows -- the
    finest granularity that source has.
    """
    events = []
    for date, day in daily_fees().items():
        ts = int(time.mktime(time.strptime(date, "%Y-%m-%d")) - time.timezone) * 1000
        if day["deployer"] > 0:
            events.append((ts, "in", "HIP-3 deployer (Markets)", day["deployer"], ""))

    for t, direction, cp, usd in _transfers(_ledger(FEE_RECIPIENT), FEE_RECIPIENT):
        if direction == "in":
            origin = UNRESOLVED if cp in RECYCLED else DIRECT.get(cp, UNRESOLVED)
            events.append((t, "in", origin, usd, cp))
        else:
            events.append((t, "out", "", usd, cp))
    events.sort(key=lambda x: x[0])

    pool: dict[str, float] = defaultdict(float)
    paid: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for _, kind, origin, usd, cp in events:
        if kind == "in":
            pool[origin] += usd
            continue
        balance = sum(pool.values())
        take = min(usd, balance)
        if balance > 0:
            for o in list(pool):
                cut = take * pool[o] / balance
                paid[cp][o] += cut
                pool[o] -= cut
        if usd > take:
            paid[cp][UNRESOLVED] += usd - take
    return paid


def buyback_funding() -> dict | None:
    """Composition of everything the buyback wallet has received, by business line."""
    try:
        recip_mix = _fee_recipient_mix()
        inbound = defaultdict(float)
        senders = defaultdict(float)
        for _, direction, cp, usd in _transfers(_ledger(BUYBACKS), BUYBACKS, dollars_only=False):
            if direction == "in":
                senders[cp] += usd
        if not senders:
            return None

        for cp, usd in senders.items():
            if cp in DIRECT:
                inbound[DIRECT[cp]] += usd
                continue
            if cp == FEE_RECIPIENT:
                mix = recip_mix.get(cp) or recip_mix.get(BUYBACKS) or {}
                total = sum(mix.values())
                if total:
                    for o, v in mix.items():
                        inbound[o] += usd * v / total
                    continue
            # A forwarding wallet: resolve it by what funded it, one hop back.
            resolved = False
            funders = defaultdict(float)
            for _, d2, cp2, usd2 in _transfers(_ledger(cp), cp):
                if d2 == "in":
                    funders[cp2] += usd2
            known = {k: v for k, v in funders.items() if k in DIRECT or k == FEE_RECIPIENT}
            if known and sum(known.values()) > 0:
                base = sum(known.values())
                for k, v in known.items():
                    share = usd * v / base
                    if k == FEE_RECIPIENT:
                        mix = recip_mix.get(cp) or {}
                        tm = sum(mix.values())
                        if tm:
                            for o, mv in mix.items():
                                inbound[o] += share * mv / tm
                        else:
                            inbound[UNRESOLVED] += share
                    else:
                        inbound[DIRECT[k]] += share
                resolved = True
            if not resolved:
                inbound[UNRESOLVED] += usd

        total = sum(inbound.values())
        rows = [
            {"source": k, "usd": round(v, 2), "pct": round(v / total * 100, 1)}
            for k, v in sorted(inbound.items(), key=lambda x: -x[1])
        ]
        markets = sum(v for k, v in inbound.items() if "Markets" in k)
        logger.info(f"buyback funding: ${total:,.0f} total, ${markets:,.0f} from Markets")
        global _last_result
        _last_result = {
            "method": "pooled (average-cost) attribution on the live balance, one hop back",
            "total_usd": round(total, 2),
            "markets_usd": round(markets, 2),
            "markets_pct": round(markets / total * 100, 1),
            "sources": rows,
        }
        return _last_result
    except Exception as exc:
        logger.warning(f"buyback funding attribution failed: {exc}")
        return _last_result
