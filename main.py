"""
Kinetiq Dashboard — Backend API
FastAPI server with scheduled data collection from Hyperliquid L1.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from schedulers.revenue import RevenueCollector
from schedulers.comparison import ComparisonCollector
from schedulers.liquidity import LiquidityCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kinetiq")

# ── Collectors (hold data in memory) ──────────────────────
revenue_collector = RevenueCollector()
comparison_collector = ComparisonCollector()
liquidity_collector = LiquidityCollector()

scheduler = AsyncIOScheduler()


async def run_revenue():
    try:
        await asyncio.to_thread(revenue_collector.collect)
        logger.info("Revenue collection complete")
    except Exception as e:
        logger.error(f"Revenue collection failed: {e}")


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run initial data collection, then start scheduler."""
    logger.info("Starting initial data collection...")

    # Run all collectors once at startup
    await run_revenue()
    await run_comparison()
    await run_liquidity_snapshot()

    # Schedule periodic collection
    scheduler.add_job(run_revenue, "interval", minutes=5, id="revenue")
    scheduler.add_job(run_comparison, "interval", minutes=5, id="comparison")
    # Liquidity snapshots every 30 seconds
    scheduler.add_job(run_liquidity_snapshot, "interval", seconds=30, id="liquidity")

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
            "revenue": revenue_collector.last_updated,
            "comparison": comparison_collector.last_updated,
            "liquidity": liquidity_collector.last_updated,
        },
    }


@app.get("/api/revenue")
def get_revenue():
    """Markets by Kinetiq revenue data."""
    return revenue_collector.get_data()


@app.get("/api/comparison")
def get_comparison():
    """HIP-3 market comparison data (km vs xyz vs flx vs cash)."""
    return comparison_collector.get_data()


@app.get("/api/liquidity")
def get_liquidity():
    """Orderbook liquidity data (spreads + depth)."""
    return liquidity_collector.get_data()
