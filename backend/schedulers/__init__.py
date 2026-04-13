"""Hyperliquid API client — shared across all schedulers."""

import logging
import time
import threading
import requests

logger = logging.getLogger("kinetiq.hl")

HL_BASE = "https://api.hyperliquid.xyz/info"

# Global rate limiter: minimum gap between requests across all threads
_lock = threading.Lock()
_last_request = 0.0
_MIN_GAP = 0.12  # ~8 req/s max


def hl_post(payload: dict, desc: str = "", retries: int = 4) -> dict | None:
    """POST to Hyperliquid info API with rate limiting and exponential backoff."""
    global _last_request

    for attempt in range(retries):
        # Enforce minimum gap between requests
        with _lock:
            now = time.monotonic()
            wait = _MIN_GAP - (now - _last_request)
            if wait > 0:
                time.sleep(wait)
            _last_request = time.monotonic()

        try:
            r = requests.post(HL_BASE, json=payload, timeout=10)
            if r.status_code == 429:
                backoff = 2 ** attempt  # 1s, 2s, 4s, 8s
                logger.debug(f"hl_post [{desc}]: 429, backing off {backoff}s (attempt {attempt+1}/{retries})")
                time.sleep(backoff)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if r.status_code >= 500 and attempt < retries - 1:
                backoff = 2 ** attempt
                logger.debug(f"hl_post [{desc}]: {r.status_code}, retrying in {backoff}s")
                time.sleep(backoff)
                continue
            logger.warning(f"hl_post [{desc}]: {e}")
            return None
        except Exception as e:
            logger.warning(f"hl_post [{desc}]: {e}")
            return None

    logger.warning(f"hl_post [{desc}]: exhausted {retries} retries")
    return None
