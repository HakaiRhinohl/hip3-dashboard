"""
Builder-code revenue, measured rather than estimated.

Hyperliquid exposes `builderRewards` only as a running cumulative (claimed +
unclaimed), which is why this used to be projected as a bps rate applied to
volume. That proxy is bad here: Markets' effective builder rate jumped ~14x at
the km -> mkts migration (0.20 -> 2.83 bps of notional) because app adoption
changed, so an all-time average understates the current pace several-fold.

There is a better source. Each claim lands in the builder's ledger as a dated
`rewardsClaim`, and a claim empties the unclaimed balance. So for any window
that starts at a claim:

    revenue(claim_date -> now) = sum(claims after claim_date) + unclaimed now

which is an exact measurement, not a rate estimate. Windows are therefore
anchored to claim dates rather than to round day counts.

Cumulative-to-date is simply `builderRewards`, summed across every token the
builder earns in (USDC, USDH, USDE, USDT0 -- all USD-pegged).
"""

import logging
from datetime import datetime, timezone

from schedulers import hl_post

logger = logging.getLogger("kinetiq.builder_fees")


def _cumulative_rewards(address: str) -> float | None:
    """builderRewards across all tokens, or None if the endpoint fails."""
    ref = hl_post({"type": "referral", "user": address}, f"referral {address[:8]}")
    if not isinstance(ref, dict):
        return None
    total = 0.0
    for entry in ref.get("tokenToState") or []:
        if isinstance(entry, list) and len(entry) == 2:
            try:
                total += float(entry[1].get("builderRewards", 0) or 0)
            except (TypeError, ValueError, AttributeError):
                continue
    return total


def _builder_share(address: str) -> float:
    """
    What fraction of this address's claimable rewards are builder rewards.

    `builderRewards` is builder-only, while `claimedRewards` + `unclaimedRewards`
    covers referral commissions too. Where the address refers users, the second
    is the larger of the two and the difference is not builder revenue.
    """
    ref = hl_post({"type": "referral", "user": address}, f"referral {address[:8]}")
    if not isinstance(ref, dict):
        return 1.0
    builder = claimable = 0.0
    for entry in ref.get("tokenToState") or []:
        if not (isinstance(entry, list) and len(entry) == 2):
            continue
        s = entry[1]
        try:
            builder += float(s.get("builderRewards", 0) or 0)
            claimable += float(s.get("claimedRewards", 0) or 0) + float(s.get("unclaimedRewards", 0) or 0)
        except (TypeError, ValueError, AttributeError):
            continue
    if claimable <= 0 or builder >= claimable:
        return 1.0
    share = builder / claimable
    logger.info(f"{address[:10]}: {(1 - share) * 100:.1f}% of claims are referral, not builder")
    return share


def _claims(address: str) -> list[tuple[int, float]]:
    """
    Dated (timestamp_ms, amount) for every rewardsClaim, oldest first.

    The ledger endpoint caps a response at 2000 records and returns the oldest
    ones, so it has to be paged. Some builder addresses are busy enough to hit
    that cap repeatedly -- the mobile builder logs 2000 records in under a
    month -- and a single unpaged call would miss nearly all of its claims.
    """
    out: list[tuple[int, float]] = []
    seen: set[tuple[int, str]] = set()
    start_ms = 0
    for _ in range(60):
        page = hl_post(
            {"type": "userNonFundingLedgerUpdates", "user": address, "startTime": start_ms},
            f"ledger {address[:8]}",
        )
        if not isinstance(page, list) or not page:
            break
        for rec in page:
            delta = rec.get("delta", {})
            if delta.get("type") != "rewardsClaim":
                continue
            key = (rec.get("time", 0), str(delta.get("amount")))
            if key in seen:
                continue
            seen.add(key)
            try:
                out.append((rec.get("time", 0), float(delta.get("amount", 0) or 0)))
            except (TypeError, ValueError):
                continue
        if len(page) < 2000:
            break
        newest = max(r.get("time", 0) for r in page)
        if newest <= start_ms:
            break  # no forward progress; stop rather than loop
        start_ms = newest
    out.sort()
    return out


def fetch_builder_revenue(addresses: list[str], target_days: int = 30) -> dict | None:
    """
    Measured builder revenue for one or more builder addresses.

    `target_days` is the window we would like; the window actually used snaps
    back to the nearest claim at or before that point, because only then is the
    starting unclaimed balance known to be zero. The returned `window_days` is
    the real elapsed period and is what the annualised figure divides by.
    """
    cumulative = 0.0
    claims: list[tuple[int, float]] = []
    seen = set()
    for addr in addresses:
        if not addr or addr in seen:
            continue
        seen.add(addr)
        c = _cumulative_rewards(addr)
        if c is None:
            logger.warning(f"builder rewards unreadable for {addr[:10]}")
            return None
        cumulative += c
        # A rewardsClaim is not necessarily a builder claim: the same ledger
        # entry covers referral commissions, and Markets' mobile address earns
        # both. Its claims exceed its builderRewards by ~$14.9K, so scaling each
        # address's claims by its own builder share keeps referral income out of
        # the builder run-rate. Addresses with no referral earnings scale by 1.
        addr_claims = _claims(addr)
        share = _builder_share(addr)
        claims.extend([(t, a * share) for t, a in addr_claims] if share < 1.0 else addr_claims)
    claims.sort()

    if not claims:
        logger.warning("no rewardsClaim history; cannot measure builder run-rate")
        return {"cumulative_usd": round(cumulative, 2), "measured": False}

    claimed = sum(a for _, a in claims)
    unclaimed = max(cumulative - claimed, 0.0)
    now = datetime.now(timezone.utc)
    target_ms = (now.timestamp() - target_days * 86400) * 1000

    # Snap to the newest claim at or before the target, so the window starts
    # from a point where nothing was left unclaimed.
    anchor = None
    for ts, _ in claims:
        if ts <= target_ms:
            anchor = ts
        else:
            break
    if anchor is None:
        anchor = claims[0][0]

    window_total = sum(a for ts, a in claims if ts > anchor) + unclaimed
    anchor_dt = datetime.fromtimestamp(anchor / 1000, tz=timezone.utc)
    window_days = max((now - anchor_dt).total_seconds() / 86400, 1.0)
    annualized = window_total / window_days * 365

    last_claim = datetime.fromtimestamp(claims[-1][0] / 1000, tz=timezone.utc)
    logger.info(
        f"builder revenue measured: ${cumulative:,.0f} cumulative, "
        f"${window_total:,.0f} over {window_days:.1f}d -> ${annualized:,.0f}/yr"
    )
    return {
        "measured": True,
        "cumulative_usd": round(cumulative, 2),
        "claimed_usd": round(claimed, 2),
        "unclaimed_usd": round(unclaimed, 2),
        "claim_count": len(claims),
        "last_claim": last_claim.strftime("%Y-%m-%d"),
        "window_days": round(window_days, 2),
        "window_start": anchor_dt.strftime("%Y-%m-%d"),
        "window_usd": round(window_total, 2),
        "annualized_usd": round(annualized, 2),
    }
