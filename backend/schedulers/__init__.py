"""Hyperliquid API client — shared across all schedulers."""

import logging
import requests

logger = logging.getLogger("kinetiq.hl")

HL_BASE = "https://api.hyperliquid.xyz/info"


def hl_post(payload: dict, desc: str = "") -> dict | None:
    """POST to Hyperliquid info API, return parsed JSON or None on error."""
    try:
        r = requests.post(HL_BASE, json=payload, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"hl_post [{desc}]: {e}")
        return None
