"""
Kinetiq Dashboard — Backend API
FastAPI server with scheduled data collection from Hyperliquid L1.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from schedulers.revenue import RevenueCollector
from schedulers.comparison import ComparisonCollector
from schedulers.liquidity import LiquidityCollector
from schedulers.users import UsersCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kinetiq")

# ── Collectors (hold data in memory) ──────────────────────
REVENUE_DEXES = ["km", "xyz", "flx", "cash"]
revenue_collectors = {dex: RevenueCollector(dex) for dex in REVENUE_DEXES}
comparison_collector = ComparisonCollector()
liquidity_collector = LiquidityCollector()
users_collector = UsersCollector()

scheduler = AsyncIOScheduler()


async def run_revenue():
    for dex, collector in revenue_collectors.items():
        try:
            await asyncio.to_thread(collector.collect)
            logger.info(f"Revenue collection complete ({dex})")
        except Exception as e:
            logger.error(f"Revenue collection failed ({dex}): {e}")


async def run_comparison():
    try:
        await asyncio.to_thread(comparison_collector.collect)
        logger.info("Comparison collection complete")
    except Exception as e:
        logger.error(f"Comparison collection failed: {e}")


async def run_liquidity_snapshot():
    try:
        await asyncio.to_thread(liquidity_collector.take_snapshot)
        logger.info("Liquidity snapshot taken")
    except Exception as e:
        logger.error(f"Liquidity snapshot failed: {e}")


async def run_users():
    try:
        await asyncio.to_thread(users_collector.collect)
        logger.info("Users collection complete")
    except Exception as e:
        logger.error(f"Users collection failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run initial data collection, then start scheduler."""
    logger.info("Starting initial data collection...")

    # Run all collectors once at startup
    await run_revenue()
    await run_comparison()
    await run_liquidity_snapshot()

    # Users: start bootstrap in background thread if not complete; otherwise run incremental
    await asyncio.to_thread(users_collector.maybe_start_bootstrap)

    # Schedule periodic collection
    scheduler.add_job(run_revenue, "interval", minutes=5, id="revenue")
    scheduler.add_job(run_comparison, "interval", minutes=5, id="comparison")
    # Liquidity snapshots every 30 seconds
    scheduler.add_job(run_liquidity_snapshot, "interval", seconds=30, id="liquidity")
    # Users incremental update every 24 hours
    scheduler.add_job(run_users, "interval", hours=24, id="users")

    scheduler.start()
    logger.info("Scheduler started")

    yield

    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="Kinetiq Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Will restrict to your Vercel domain later
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Endpoints ─────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "collectors": {
            "revenue": {dex: revenue_collectors[dex].last_updated for dex in REVENUE_DEXES},
            "comparison": comparison_collector.last_updated,
            "liquidity": liquidity_collector.last_updated,
            "users": users_collector.last_updated,
        },
    }


@app.get("/api/revenue")
def get_revenue(dex: str = Query(default="km")):
    """Revenue data per DEX. ?dex=km|xyz|flx|cash"""
    collector = revenue_collectors.get(dex)
    if not collector:
        return {"error": f"Unknown dex: {dex}. Valid: {REVENUE_DEXES}"}
    return collector.get_data()


@app.get("/api/comparison")
def get_comparison():
    """HIP-3 market comparison data (km vs xyz vs flx vs cash)."""
    return comparison_collector.get_data()


@app.get("/api/liquidity")
def get_liquidity(hours: int | None = Query(default=None, ge=1, le=168)):
    """
    Orderbook liquidity stats.
    - No `hours` param → returns the fast cached 4h window (default).
    - `hours=N`       → queries DB fresh for the last N hours (1–168).
    """
    if hours is None:
        return liquidity_collector.get_data()
    return liquidity_collector.get_stats(hours=hours)


@app.get("/api/liquidity/timeseries")
def get_liquidity_timeseries(
    ticker: str,
    hours: int = Query(default=4, ge=1, le=168),
):
    """
    Bucketed spread + depth timeseries for a specific ticker, all DEXes.
    Used to power the 'over time' line charts in the frontend.
    """
    return liquidity_collector.get_timeseries(ticker=ticker, hours=hours)


@app.get("/api/liquidity/tickers")
def get_liquidity_tickers():
    """All discovered tickers, grouped by DEX."""
    return liquidity_collector.get_available_tickers()


@app.get("/api/users")
def get_users():
    """
    HIP-3 user tracking summary (legacy endpoint, kept for backward compat).
    Returns: total_hip3_users, by_dex, new_users, bootstrap_status.
    """
    return users_collector.get_data()


@app.get("/api/users/summary")
def get_users_summary():
    """
    HIP-3 user onboarding summary.
    Returns: total_unique_users, by_dex, new_users (1d/7d/30d/90d), bootstrap_status.
    """
    return users_collector.get_summary()


@app.get("/api/users/timeline")
def get_users_timeline(period: int = Query(default=90, ge=1, le=365)):
    """
    Daily new HIP-3 user counts per DEX for the last N days.
    Each entry: {date, km, xyz, flx, cash, hyna, vntl}
    """
    return users_collector.get_timeline(period=period)


@app.get("/api/users/top_venues")
def get_users_top_venues():
    """
    Unique users per venue, sorted descending.
    Returns: [{dex, unique_users, pct}, ...]
    """
    return users_collector.get_top_venues()
