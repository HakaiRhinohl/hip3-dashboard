"""
Buybacks collector — tracks the sKNTQ buyback wallet's on-chain activity.

Kinetiq does not publish a machine-readable buyback feed, so this is
reconstructed directly from Hyperliquid data for the publicly listed buyback
wallet, from three distinct sources that must not be conflated:

  1. Funding (userNonFundingLedgerUpdates, transfers in) -- money arriving in
     the wallet from fee recipients, the kHYPE allocation wallet, etc.
  2. The actual buybacks (userFillsByTime, spot trade fills on the KNTQ/USDC
     and KNTQ/USDH pairs) -- when and how much KNTQ was actually purchased on
     the open market. This is what "buyback activity" means and is spread
     across most days in varying size, not batched into a handful of spikes.
  3. Forwarding (userNonFundingLedgerUpdates, transfers out) -- previously
     bought KNTQ being sent onward to other wallets. This happens in
     infrequent batches and is NOT when the buyback itself occurred, even
     though it also involves KNTQ leaving the buyback wallet.

An earlier version of this collector only looked at (1) and (3) and displayed
(3) as "daily buybacks" -- since forwarding is batched into ~10 transfers
total, that made real daily buying activity look like a few large spikes.

Counterparty roles for (1)/(3) are matched against addresses already tracked
elsewhere in this codebase (DEX fee recipients, builders, the KNTQ spot
deployer) or documented at kinetiq.xyz/docs/khype (10% fee on kHYPE staking
rewards, 70% buybacks / 30% treasury). Anything that doesn't match a known
address is reported as "unidentified" rather than guessed -- see
KNOWN_COUNTERPARTIES for which roles are doc-confirmed vs inferred from
on-chain flow patterns.

kmHYPE has a separate buyback wallet that is not tracked here yet (its
address was not available at the time this collector was written).
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

from schedulers import hl_post

CACHE_DIR = os.environ.get("CACHE_DIR", "/data")

logger = logging.getLogger("kinetiq.buybacks")

# Publicly listed sKNTQ buyback wallet.
BUYBACK_WALLET = "0xaa3b7392052d62928cc87701e3ca6fb6630bb6e2"

# `confirmed=True` roles come from other collectors in this codebase or from
# kinetiq.xyz/docs. `confirmed=False` roles are inferred purely from on-chain
# flow pattern (token, cadence, pass-through behavior) and have not been
# confirmed against documentation.
KNOWN_COUNTERPARTIES = {
    "0xbcd4071d023bf2aae484d724c130b5af6f0ca0d2": {
        "label": "Markets (km) fee recipient",
        "category": "markets_deployer_fees",
        "confirmed": True,
    },
    "0x51172933b60847085e2a959e860e2ec9e240ac09": {
        "label": "KNTQ spot deployer",
        "category": "kntq_spot_deployer",
        "confirmed": True,
    },
    "0x42f3226007290b02c5a0b15bccbb1ba6df04f992": {
        "label": "Markets (km) trading builder",
        "category": "markets_builder_fees",
        "confirmed": True,
    },
    "0x55758d720e0f32328f7f1e1b3de6b637e0bec4ba": {
        "label": "kHYPE buyback allocation",
        "category": "khype_staking_rewards",
        "confirmed": True,
    },
    "0x696238e0ca31c94e24ca4cbe7921754e172e4d0f": {
        # Holds ~1.24M KNTQ (from both the buyback wallet and the fee recipient)
        # and forwards it in bulk to 0x2000...007c, Hyperliquid's system bridge
        # address for token index 124 (KNTQ) -- i.e. it bridges bought-back KNTQ
        # from HyperCore spot to HyperEVM. Consistent with feeding the sKNTQ
        # staking contract, but not documented publicly.
        "label": "sKNTQ staking bridge (routes KNTQ to HyperEVM)",
        "category": "skntq_staking_bridge",
        "confirmed": False,
    },
}


def _describe(addr: str) -> dict:
    meta = KNOWN_COUNTERPARTIES.get(addr)
    if meta:
        return {"address": addr, **meta}
    return {"address": addr, "label": None, "category": "unidentified", "confirmed": False}


def _kntq_pair_names() -> set[str]:
    """Spot pair identifiers (e.g. "@334") whose two legs include KNTQ."""
    meta = hl_post({"type": "spotMeta"}, "spot meta")
    if not isinstance(meta, dict):
        return set()
    tokens = {t["index"]: t.get("name") for t in meta.get("tokens", [])}
    kntq_indices = {idx for idx, name in tokens.items() if name == "KNTQ"}
    if not kntq_indices:
        return set()
    return {
        pair["name"]
        for pair in meta.get("universe", [])
        if kntq_indices & set(pair.get("tokens", []))
    }


def _fetch_all_fills(wallet: str) -> list:
    """Paginate userFillsByTime (capped at 2000/page) to get the full history."""
    all_fills = []
    seen_tids = set()
    start_ms = 0
    for _ in range(100):
        batch = hl_post(
            {"type": "userFillsByTime", "user": wallet, "startTime": start_ms, "aggregateByTime": False},
            "buybacks fills",
        )
        if not isinstance(batch, list) or not batch:
            break
        new = [f for f in batch if f.get("tid") not in seen_tids]
        seen_tids.update(f["tid"] for f in new)
        all_fills.extend(new)
        if len(batch) < 2000:
            break
        start_ms = batch[-1]["time"]
    return all_fills


class BuybacksCollector:
    def __init__(self):
        self.data = None
        self.last_updated = None
        self._cache_path = os.path.join(CACHE_DIR, "buybacks.json")
        self._load_cache()

    def _load_cache(self):
        try:
            if os.path.exists(self._cache_path):
                with open(self._cache_path) as f:
                    cached = json.load(f)
                self.data = cached.get("data")
                self.last_updated = cached.get("last_updated")
                logger.info("Loaded cached buybacks data")
        except Exception as e:
            logger.warning(f"Failed to load buybacks cache: {e}")

    def _save_cache(self):
        try:
            os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
            with open(self._cache_path, "w") as f:
                json.dump({"data": self.data, "last_updated": self.last_updated}, f)
        except Exception as e:
            logger.warning(f"Failed to save buybacks cache: {e}")

    def get_data(self) -> dict:
        if self.data is None:
            return {"status": "loading", "message": "Initial collection in progress"}
        return self.data

    def collect(self):
        logger.info("Starting buybacks collection...")
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        ledger = hl_post(
            {"type": "userNonFundingLedgerUpdates", "user": BUYBACK_WALLET, "startTime": 0},
            "buybacks ledger",
        )
        if not isinstance(ledger, list):
            logger.error("Failed to fetch buyback wallet ledger")
            return

        inbound_by_source: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        outbound_by_dest: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        inbound_tx_count: dict[str, int] = defaultdict(int)
        outbound_tx_count: dict[str, int] = defaultdict(int)
        daily_inbound: dict[str, float] = defaultdict(float)
        daily_outbound: dict[str, float] = defaultdict(float)
        recent = []

        for rec in ledger:
            delta = rec.get("delta", {})
            if delta.get("type") not in ("send", "spotTransfer"):
                continue
            user = delta.get("user")
            dest = delta.get("destination")
            token = delta.get("token", "?")
            try:
                usd = float(delta.get("usdcValue", 0) or 0)
            except (TypeError, ValueError):
                usd = 0.0
            ts_ms = rec.get("time", 0)
            date_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

            if dest == BUYBACK_WALLET and user != BUYBACK_WALLET:
                inbound_by_source[user][token] += usd
                inbound_tx_count[user] += 1
                daily_inbound[date_str] += usd
                direction = "in"
                counterparty = user
            elif user == BUYBACK_WALLET and dest != BUYBACK_WALLET:
                outbound_by_dest[dest][token] += usd
                outbound_tx_count[dest] += 1
                daily_outbound[date_str] += usd
                direction = "out"
                counterparty = dest
            else:
                continue

            recent.append({
                "time": ts_ms,
                "hash": rec.get("hash"),
                "direction": direction,
                "counterparty": counterparty,
                "token": token,
                "amount": delta.get("amount"),
                "usd": round(usd, 2),
            })

        total_inbound = sum(sum(toks.values()) for toks in inbound_by_source.values())
        total_outbound = sum(sum(toks.values()) for toks in outbound_by_dest.values())

        def _build_breakdown(by_addr: dict, tx_counts: dict, total: float) -> list:
            rows = []
            for addr, toks in by_addr.items():
                usd = sum(toks.values())
                rows.append({
                    **_describe(addr),
                    "usd": round(usd, 2),
                    "pct": round(usd / total * 100, 2) if total > 0 else 0,
                    "tokens": {k: round(v, 2) for k, v in toks.items()},
                    "tx_count": tx_counts.get(addr, 0),
                })
            rows.sort(key=lambda r: -r["usd"])
            return rows

        sources = _build_breakdown(inbound_by_source, inbound_tx_count, total_inbound)
        destinations = _build_breakdown(outbound_by_dest, outbound_tx_count, total_outbound)

        recent.sort(key=lambda r: -r["time"])

        # The actual buybacks: spot trade fills on the KNTQ pairs, not the
        # batched outbound transfers above. Buys accumulate; any Sell (never
        # observed so far) nets against them rather than being ignored.
        kntq_pairs = _kntq_pair_names()
        fills = _fetch_all_fills(BUYBACK_WALLET) if kntq_pairs else []
        daily_buy_usd: dict[str, float] = defaultdict(float)
        daily_buy_kntq: dict[str, float] = defaultdict(float)
        buy_fill_count = 0
        total_bought_usd = 0.0
        total_bought_kntq = 0.0
        for fill in fills:
            if fill.get("coin") not in kntq_pairs:
                continue
            try:
                px = float(fill.get("px", 0) or 0)
                sz = float(fill.get("sz", 0) or 0)
            except (TypeError, ValueError):
                continue
            sign = 1 if fill.get("dir") == "Buy" else -1 if fill.get("dir") == "Sell" else 0
            if sign == 0:
                continue
            usd = px * sz * sign
            date_str = datetime.fromtimestamp(fill.get("time", 0) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            daily_buy_usd[date_str] += usd
            daily_buy_kntq[date_str] += sz * sign
            total_bought_usd += usd
            total_bought_kntq += sz * sign
            if sign > 0:
                buy_fill_count += 1

        all_dates = sorted(set(daily_inbound) | set(daily_outbound) | set(daily_buy_usd))
        cum_in = cum_out = cum_bought = 0.0
        daily_chart = []
        for date_str in all_dates:
            cum_in += daily_inbound.get(date_str, 0)
            cum_out += daily_outbound.get(date_str, 0)
            cum_bought += daily_buy_usd.get(date_str, 0)
            daily_chart.append({
                "date": date_str,
                "inbound_usd": round(daily_inbound.get(date_str, 0), 2),
                "cum_inbound_usd": round(cum_in, 2),
                "kntq_bought_usd": round(daily_buy_usd.get(date_str, 0), 2),
                "kntq_bought_amount": round(daily_buy_kntq.get(date_str, 0), 2),
                "cum_kntq_bought_usd": round(cum_bought, 2),
                "kntq_forwarded_usd": round(daily_outbound.get(date_str, 0), 2),
                "cum_kntq_forwarded_usd": round(cum_out, 2),
            })

        confirmed_inbound = sum(r["usd"] for r in sources if r["confirmed"])
        unidentified_inbound = total_inbound - confirmed_inbound

        # inbound - outbound is money that hasn't been forwarded OUT of the wallet
        # yet, but it isn't sitting idle: the wallet already converts it to KNTQ on
        # arrival. Confirm via the wallet's live spot balance rather than assuming.
        spot_state = hl_post(
            {"type": "spotClearinghouseState", "user": BUYBACK_WALLET},
            "buybacks spot balance",
        )
        current_holdings = []
        if isinstance(spot_state, dict):
            for bal in spot_state.get("balances", []):
                try:
                    amount = float(bal.get("total", 0) or 0)
                    cost_basis = float(bal.get("entryNtl", 0) or 0)
                except (TypeError, ValueError):
                    amount = cost_basis = 0.0
                if amount <= 0:
                    continue
                current_holdings.append({
                    "coin": bal.get("coin"),
                    "amount": round(amount, 6),
                    "cost_basis_usd": round(cost_basis, 2),
                })
        held_kntq = next((h for h in current_holdings if h["coin"] == "KNTQ"), None)

        self.data = {
            "generated_at": now_str,
            "wallet": BUYBACK_WALLET,
            "methodology": (
                "Three separate signals, don't confuse them: (1) funding -- money "
                "arriving in the buyback wallet, from userNonFundingLedgerUpdates; "
                "(2) actual buybacks -- spot trade fills on the KNTQ/USDC and "
                "KNTQ/USDH pairs, from userFillsByTime, which is what 'KNTQ Bought' "
                "and the daily chart reflect; (3) forwarding -- previously-bought "
                "KNTQ sent onward to other wallets in infrequent batches, which is "
                "NOT when the buyback happened even though it also moves KNTQ out of "
                "the wallet. Counterparty roles for (1)/(3) are matched against "
                "addresses already tracked elsewhere in this dashboard (fee "
                "recipients, builders, the KNTQ spot deployer) or documented at "
                "kinetiq.xyz/docs/khype; unmatched counterparties are reported as "
                "unidentified rather than guessed. kmHYPE's separate buyback wallet "
                "is not tracked here yet."
            ),
            "totals": {
                "inbound_usd": round(total_inbound, 2),
                "kntq_bought_usd": round(total_bought_usd, 2),
                "kntq_bought_amount": round(total_bought_kntq, 2),
                "kntq_bought_fill_count": buy_fill_count,
                "kntq_forwarded_usd": round(total_outbound, 2),
                "held_kntq_amount": held_kntq["amount"] if held_kntq else 0,
                "held_kntq_cost_basis_usd": held_kntq["cost_basis_usd"] if held_kntq else 0,
                "confirmed_inbound_usd": round(confirmed_inbound, 2),
                "confirmed_inbound_pct": round(confirmed_inbound / total_inbound * 100, 2) if total_inbound > 0 else 0,
                "unidentified_inbound_usd": round(unidentified_inbound, 2),
                "transaction_count": len(ledger),
            },
            "current_holdings": current_holdings,
            "sources": sources,
            "destinations": destinations,
            "daily_chart": daily_chart,
            "recent_transactions": recent[:25],
        }
        self.last_updated = now_str
        self._save_cache()
        logger.info(
            f"Buybacks: ${total_inbound:,.0f} in, ${total_bought_usd:,.0f} KNTQ bought "
            f"({buy_fill_count} fills), ${total_outbound:,.0f} forwarded onward "
            f"({len(sources)} sources, {len(destinations)} destinations)"
        )
