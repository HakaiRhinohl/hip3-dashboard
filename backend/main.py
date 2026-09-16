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
from schedulers.buybacks import BuybacksCollector
from schedulers.fee_db import init_fee_db
from schedulers import reservoir_fees

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kinetiq")

# ── Collectors (hold data in memory) ──────────────────────
REVENUE_DEXES = ["km", "xyz", "flx", "cash", "para", "io"]
revenue_collectors = {dex: RevenueCollector(dex) for dex in REVENUE_DEXES}
comparison_collector = ComparisonCollector()
liquidity_collector = LiquidityCollector()
buybacks_collector = BuybacksCollector()

scheduler = AsyncIOScheduler()


async def run_revenue():
    for dex, collector in revenue_collectors.items():
        try:
            await asyncio.to_thread(collector.collect)
            logger.info(f"Revenue collection complete ({dex})")
        except Exception as e:
            logger.error(f"Revenue collection failed ({dex}): {e}")
    await run_comparison()


async def run_comparison():
    try:
        revenue_data = {
            dex: collector.get_data()
            for dex, collector in revenue_collectors.items()
        }
        await asyncio.to_thread(comparison_collector.collect, revenue_data)
        logger.info("Comparison collection complete")
    except Exception as e:
        logger.error(f"Comparison collection failed: {e}")


async def run_liquidity_snapshot():
    try:
        await asyncio.to_thread(liquidity_collector.take_snapshot)
        logger.info("Liquidity snapshot taken")
    except Exception as e:
        logger.error(f"Liquidity snapshot failed: {e}")


async def run_buybacks():
    try:
        await asyncio.to_thread(buybacks_collector.collect)
        logger.info("Buybacks collection complete")
    except Exception as e:
        logger.error(f"Buybacks collection failed: {e}")


async def run_reservoir():
    """Pull a bounded slice of unprocessed reservoir partitions.

    Each day is one Requester Pays download, so this runs on its own slow job
    rather than inside the 5-minute revenue cycle. A cold database backfills
    over several hours; afterwards there is at most one new day to fetch.
    """
    try:
        result = await asyncio.to_thread(reservoir_fees.ingest)
        if result["ingested"] or result["failed"]:
            logger.info(f"Reservoir ingest: {result}")
    except Exception as e:
        logger.error(f"Reservoir ingest failed: {e}")
    # Other venues: incremental, read in place from S3 rather than downloaded.
    try:
        await asyncio.to_thread(reservoir_fees.ingest_venues)
    except Exception as e:
        logger.error(f"Venue ingest failed: {e}")


async def run_initial_collection():
    """Refresh caches without blocking FastAPI from serving existing data."""
    # Project persisted revenue caches immediately, then refresh upstream data.
    await run_comparison()
    await run_revenue()
    await run_liquidity_snapshot()
    await run_buybacks()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Serve cached data immediately and refresh it in the background."""
    logger.info("Starting initial data collection...")

    # Initialize fee accumulator DB (watermark persistence for claim-resistant fee tracking)
    init_fee_db()

    # Do not block application startup while slow candle downloads run. Each
    # collector loads its persisted cache during construction, so endpoints can
    # serve the last good snapshot immediately.
    initial_task = asyncio.create_task(run_initial_collection())

    # Schedule periodic collection
    scheduler.add_job(run_revenue, "interval", minutes=5, id="revenue")
    # Liquidity snapshots every 30 seconds
    scheduler.add_job(run_liquidity_snapshot, "interval", seconds=30, id="liquidity")
    # Buybacks ledger every 5 minutes
    scheduler.add_job(run_buybacks, "interval", minutes=5, id="buybacks")
    # Reservoir partitions land once a day; the cadence here only governs how
    # fast a cold backfill catches up.
    scheduler.add_job(run_reservoir, "interval", minutes=20, id="reservoir")

    scheduler.start()
    logger.info("Scheduler started")

    yield

    if not initial_task.done():
        initial_task.cancel()
        await asyncio.gather(initial_task, return_exceptions=True)
    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="Kinetiq Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
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
            "buybacks": buybacks_collector.last_updated,
        },
    }


@app.get("/api/snapshot")
def get_snapshot(
    timeline_days: int = Query(default=90, ge=1, le=365),
    liquidity_hours: int = Query(default=4, ge=1, le=168),
):
    """Return every dashboard dataset in one document-friendly response."""
    generated_at = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": "1.0",
        "generated_at": generated_at,
        "parameters": {
            "timeline_days": timeline_days,
            "liquidity_hours": liquidity_hours,
        },
        "collector_updates": {
            "revenue": {dex: revenue_collectors[dex].last_updated for dex in REVENUE_DEXES},
            "comparison": comparison_collector.last_updated,
            "liquidity": liquidity_collector.last_updated,
            "buybacks": buybacks_collector.last_updated,
        },
        "revenue": {
            dex: revenue_collectors[dex].get_data()
            for dex in REVENUE_DEXES
        },
        "comparison": comparison_collector.get_data(),
        "liquidity": {
            "summary": liquidity_collector.get_stats(hours=liquidity_hours),
            "tickers": liquidity_collector.get_available_tickers(),
        },
        "buybacks": buybacks_collector.get_data(),
        "endpoints": {
            "snapshot": "/api/snapshot",
            "health": "/api/health",
            "revenue": "/api/revenue?dex=km|xyz|flx|cash|para|io",
            "comparison": "/api/comparison",
            "liquidity": "/api/liquidity?hours=1..168",
            "liquidity_timeseries": "/api/liquidity/timeseries?ticker=US500&hours=4",
            "liquidity_tickers": "/api/liquidity/tickers",
            "buybacks": "/api/buybacks",
        },
    }


@app.get("/api/revenue")
def get_revenue(dex: str = Query(default="km")):
    """Revenue data per DEX. ?dex=km|xyz|flx|cash|para|io"""
    collector = revenue_collectors.get(dex)
    if not collector:
        return {"error": f"Unknown dex: {dex}. Valid: {REVENUE_DEXES}"}
    return collector.get_data()


@app.get("/api/comparison")
def get_comparison():
    """HIP-3 market comparison data (km vs xyz vs flx vs cash)."""
    return comparison_collector.get_data()


@app.get("/api/buybacks")
def get_buybacks():
    """sKNTQ buyback wallet flows: inbound funding sources and outbound KNTQ destinations."""
    return buybacks_collector.get_data()


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
