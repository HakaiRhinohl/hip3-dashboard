"""
Fee accumulator — persists cumulative deployer fee earnings to SQLite using a watermark pattern.

Problem: clearinghouseState.accountValue is the CURRENT unclaimed balance for the fee_recipient.
When a DEX claims/withdraws deployer fees, the balance resets to 0 and we lose history.

Note: referral.builderRewards (per tokenToState) is already CUMULATIVE (claimedRewards +
unclaimedRewards), so no watermark is needed for builder fees — just sum all tokens.

Solution for deployer fees: on every poll, compare current balance with last known balance.
If it increased → new fees earned, add delta to cumulative.
If it decreased → a claim happened, do NOT subtract (fees were already counted).

This way cumulative always grows and survives any number of withdrawals.

The km and mkts namespaces use this pattern independently so the USDH -> USDC
migration cannot erase or merge withdrawal history.
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger("kinetiq.fee_db")

DB_PATH = os.environ.get("FEE_DB_PATH", "/data/fees.db")

# Known historical deployer fee baselines — total earned BEFORE first withdrawal.
# Used to seed the watermark DB on first run so historical earnings are not lost.
# Only DEXes with a confirmed baseline use the watermark pattern. All others
# use the CURRENT clearinghouseState.accountValue directly (no accumulation).
DEPLOYER_BASELINES: dict[str, float] = {
    "km": 92700.0,  # accountValue before withdrawal on 2026-03-30
    "mkts": 0.0,    # current USDC deployment, launched after the migration
}

# DEXes that use the watermark accumulation pattern.
# All others: just return the current balance directly.
WATERMARK_DEXES: frozenset[str] = frozenset({"km", "mkts"})


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_fee_db():
    """Create tables if they don't exist. Call once at startup."""
    conn = _get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS deployer_fee_watermarks (
            dex          TEXT PRIMARY KEY,
            last_balance REAL NOT NULL DEFAULT 0,
            cumulative   REAL NOT NULL DEFAULT 0,
            updated_at   TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    logger.info("fee_db initialized")


def update_deployer_cumulative(dex: str, current_balance: float) -> float:
    """
    For DEXes in WATERMARK_DEXES: watermark pattern that accumulates earnings
    across withdrawal events (prevents balance resets from hiding historical fees).
    For all other DEXes: returns the current balance directly (no accumulation).

    Returns the total deployer fees to display for this DEX.
    """
    if dex not in WATERMARK_DEXES:
        # No known historical baseline — current balance IS the best estimate.
        return current_balance
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT last_balance, cumulative FROM deployer_fee_watermarks WHERE dex=?",
            (dex,),
        ).fetchone()

        if row is None:
            # First time — use known historical baseline if available (takes priority),
            # otherwise start from current balance.
            baseline = DEPLOYER_BASELINES.get(dex, 0.0)
            cumulative = max(baseline, current_balance)
            conn.execute(
                "INSERT INTO deployer_fee_watermarks (dex, last_balance, cumulative, updated_at) VALUES (?,?,?,?)",
                (dex, current_balance, cumulative, now),
            )
            logger.info(f"fee_db: bootstrapped {dex} deployer at ${cumulative:,.2f} (baseline=${baseline:,.0f}, current=${current_balance:,.2f})")
        else:
            last_balance = row["last_balance"]
            cumulative = row["cumulative"]

            if current_balance > last_balance:
                delta = current_balance - last_balance
                cumulative += delta
                logger.debug(f"fee_db: {dex} deployer +${delta:,.2f} → cumulative ${cumulative:,.2f}")
            elif current_balance < last_balance * 0.5:
                # Significant drop — likely a claim/withdrawal
                logger.info(
                    f"fee_db: {dex} deployer withdrawal detected "
                    f"(${last_balance:,.2f} → ${current_balance:,.2f}), "
                    f"cumulative preserved at ${cumulative:,.2f}"
                )

            conn.execute(
                "UPDATE deployer_fee_watermarks SET last_balance=?, cumulative=?, updated_at=? WHERE dex=?",
                (current_balance, cumulative, now, dex),
            )

        conn.commit()
        return cumulative
    finally:
        conn.close()


def parse_builder_rewards(ref_response: dict) -> float:
    """
    Sum builder rewards across ALL token types (USDC, USDH, USDE, USDT0, etc.).

    referral.tokenToState[i][1].builderRewards = claimedRewards + unclaimedRewards
    This is already CUMULATIVE and does NOT reset on claim. Just sum all tokens.

    All tokens are USD-pegged stablecoins so no price conversion needed.
    """
    if not ref_response:
        return 0.0
    total = 0.0
    for token_entry in ref_response.get("tokenToState", []):
        # Each entry is [token_index, state_dict]
        if isinstance(token_entry, (list, tuple)) and len(token_entry) == 2:
            state = token_entry[1]
            total += float(state.get("builderRewards", "0"))
    # Fallback to top-level if tokenToState is empty (shouldn't happen)
    if total == 0.0:
        total = float(ref_response.get("builderRewards", "0"))
    return total
