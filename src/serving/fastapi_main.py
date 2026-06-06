"""
NexusFlow — Analytics Serving API (FastAPI)
============================================
RESTful API serving analytics from Redshift gold layer.
Provides:
  - Revenue KPIs (daily/weekly/monthly)
  - Customer 360 profiles
  - Product performance
  - Real-time inventory status
  - ML feature retrieval
  - Health/readiness probes for K8s
"""

from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from typing import Optional, List
from datetime import date, datetime, timedelta
from pydantic import BaseModel, Field
import asyncpg
import boto3
import json
import logging
import os
import time
from functools import lru_cache
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

# ── LOGGING ───────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("nexusflow.api")

# ── PROMETHEUS METRICS ────────────────────────────────────
REQUEST_COUNT = Counter(
    "nexusflow_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "nexusflow_api_request_duration_seconds",
    "API request latency",
    ["endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# ── CONFIG ────────────────────────────────────────────────
class Settings:
    redshift_host:     str = os.getenv("REDSHIFT_HOST", "localhost")
    redshift_port:     int = int(os.getenv("REDSHIFT_PORT", "5439"))
    redshift_db:       str = os.getenv("REDSHIFT_DB", "nexusflow")
    redshift_user:     str = os.getenv("REDSHIFT_USER", "nexusflow_admin")
    redshift_password: str = os.getenv("REDSHIFT_PASSWORD", "")
    secret_arn:        str = os.getenv("REDSHIFT_SECRET_ARN", "")
    env:               str = os.getenv("NEXUSFLOW_ENV", "dev")
    cache_ttl_seconds: int = int(os.getenv("CACHE_TTL_SECONDS", "300"))

settings = Settings()

# ── DATABASE POOL ─────────────────────────────────────────
db_pool: Optional[asyncpg.Pool] = None

async def get_db_password() -> str:
    """Fetch Redshift password from Secrets Manager."""
    if settings.secret_arn:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=settings.secret_arn)
        secret = json.loads(response["SecretString"])
        return secret["password"]
    return settings.redshift_password

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage DB pool lifecycle."""
    global db_pool
    password = await get_db_password()
    db_pool = await asyncpg.create_pool(
        host=settings.redshift_host,
        port=settings.redshift_port,
        database=settings.redshift_db,
        user=settings.redshift_user,
        password=password,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    logger.info("✅ DB pool initialized")
    yield
    await db_pool.close()
    logger.info("DB pool closed")

async def get_db() -> asyncpg.Connection:
    async with db_pool.acquire() as conn:
        yield conn

# ── APP ───────────────────────────────────────────────────
app = FastAPI(
    title="NexusFlow Analytics API",
    description="E-commerce analytics serving layer — gold layer queries",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── RESPONSE MODELS ───────────────────────────────────────
class RevenueKPI(BaseModel):
    date: date
    total_orders: int
    completed_orders: int
    total_revenue: float
    avg_order_value: float
    cancellation_rate: float
    refund_rate: float

class CustomerProfile(BaseModel):
    customer_id: str
    loyalty_tier: str
    segment: str
    age_bracket: str
    country: str
    total_orders: int
    total_revenue: float
    recency_days: int
    rfm_total_score: int
    churn_risk_tier: str
    customer_profile_text: str
    ltv_band: str

class ProductPerformance(BaseModel):
    product_sku: str
    product_name: str
    category: str
    subcategory: str
    total_units_sold: int
    total_revenue: float
    avg_unit_price: float
    return_rate: float
    order_count: int

class InventoryStatus(BaseModel):
    warehouse_id: str
    product_sku: str
    quantity_available: int
    is_low_stock: bool
    is_out_of_stock: bool
    reorder_point: int

class PipelineHealth(BaseModel):
    status: str
    last_pipeline_run: Optional[datetime]
    latest_order_date: Optional[date]
    total_orders_today: int
    data_freshness_hours: Optional[float]

# ── HEALTH ENDPOINTS ──────────────────────────────────────
@app.get("/health", tags=["ops"])
async def health():
    return {"status": "ok", "env": settings.env, "ts": datetime.utcnow().isoformat()}

@app.get("/ready", tags=["ops"])
async def readiness(conn=Depends(get_db)):
    try:
        await conn.fetchval("SELECT 1")
        return {"status": "ready"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB not ready: {e}")

@app.get("/metrics", tags=["ops"])
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ── REVENUE ENDPOINTS ─────────────────────────────────────
@app.get("/api/v1/revenue/daily", response_model=List[RevenueKPI], tags=["revenue"])
async def get_daily_revenue(
    start_date: date = Query(default=date.today() - timedelta(days=30)),
    end_date:   date = Query(default=date.today()),
    conn=Depends(get_db)
):
    """Daily revenue KPIs from fact_orders gold layer."""
    start = time.time()
    rows = await conn.fetch("""
        SELECT
            order_date_day                                          AS date,
            count(distinct order_id)                               AS total_orders,
            sum(is_completed)                                      AS completed_orders,
            round(sum(case when is_completed=1
                then total_amount else 0 end)::numeric, 2)        AS total_revenue,
            round(avg(case when is_completed=1
                then total_amount end)::numeric, 2)               AS avg_order_value,
            round(sum(is_cancelled)::numeric
                / nullif(count(*), 0), 4)                         AS cancellation_rate,
            round(sum(case when order_status='refunded'
                then 1 else 0 end)::numeric
                / nullif(count(*), 0), 4)                         AS refund_rate
        FROM gold.fact_orders
        WHERE order_date_day BETWEEN $1 AND $2
        GROUP BY 1
        ORDER BY 1 DESC
    """, start_date, end_date)

    REQUEST_LATENCY.labels("/api/v1/revenue/daily").observe(time.time() - start)
    REQUEST_COUNT.labels("GET", "/api/v1/revenue/daily", "200").inc()
    return [dict(r) for r in rows]

@app.get("/api/v1/revenue/summary", tags=["revenue"])
async def get_revenue_summary(conn=Depends(get_db)):
    """Revenue summary: today / 7d / 30d / MTD / YTD."""
    row = await conn.fetchrow("""
        SELECT
            round(sum(case when order_date_day = current_date
                and is_completed=1 then total_amount else 0 end)::numeric,2) AS today,
            round(sum(case when order_date_day >= current_date - 7
                and is_completed=1 then total_amount else 0 end)::numeric,2) AS last_7d,
            round(sum(case when order_date_day >= current_date - 30
                and is_completed=1 then total_amount else 0 end)::numeric,2) AS last_30d,
            round(sum(case when date_trunc('month', order_date_day) = date_trunc('month', current_date)
                and is_completed=1 then total_amount else 0 end)::numeric,2) AS mtd,
            round(sum(case when date_trunc('year', order_date_day) = date_trunc('year', current_date)
                and is_completed=1 then total_amount else 0 end)::numeric,2) AS ytd,
            count(distinct case when order_date_day = current_date
                then customer_id end)                                         AS unique_customers_today
        FROM gold.fact_orders
    """)
    REQUEST_COUNT.labels("GET", "/api/v1/revenue/summary", "200").inc()
    return dict(row)

# ── CUSTOMER ENDPOINTS ────────────────────────────────────
@app.get("/api/v1/customers/{customer_id}", response_model=CustomerProfile, tags=["customers"])
async def get_customer_profile(customer_id: str, conn=Depends(get_db)):
    """Full customer 360 profile from ML features gold layer."""
    row = await conn.fetchrow("""
        SELECT
            f.customer_id,
            d.loyalty_tier,
            d.segment,
            d.age_bracket,
            d.country,
            f.total_orders,
            f.total_revenue,
            f.recency_days,
            f.rfm_total_score,
            f.churn_risk_tier,
            f.customer_profile_text,
            d.ltv_band
        FROM ml_features.customer_ml_features f
        JOIN gold.dim_customers d
            ON f.customer_id = d.customer_id
           AND d._is_current = true
        WHERE f.customer_id = $1
          AND f.snapshot_date = current_date
    """, customer_id)

    if not row:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
    REQUEST_COUNT.labels("GET", "/api/v1/customers/{customer_id}", "200").inc()
    return dict(row)

@app.get("/api/v1/customers/segment/{segment}", tags=["customers"])
async def get_customers_by_segment(
    segment: str,
    limit: int = Query(default=100, le=1000),
    conn=Depends(get_db)
):
    """List customers in a given RFM/churn segment."""
    rows = await conn.fetch("""
        SELECT customer_id, loyalty_tier, total_orders, total_revenue,
               recency_days, rfm_total_score, churn_risk_tier
        FROM ml_features.customer_ml_features
        WHERE snapshot_date = current_date
          AND ($1 = 'all' OR churn_risk_tier = $1 OR segment = $1)
        ORDER BY total_revenue DESC
        LIMIT $2
    """, segment, limit)
    return [dict(r) for r in rows]

@app.get("/api/v1/customers/top", tags=["customers"])
async def get_top_customers(
    by:    str = Query(default="revenue", enum=["revenue", "orders", "rfm"]),
    limit: int = Query(default=20, le=500),
    conn=Depends(get_db)
):
    """Top customers by revenue, order count, or RFM score."""
    sort_col = {
        "revenue": "total_revenue",
        "orders":  "total_orders",
        "rfm":     "rfm_total_score",
    }[by]
    rows = await conn.fetch(f"""
        SELECT customer_id, loyalty_tier, segment, total_orders,
               total_revenue, recency_days, rfm_total_score, churn_risk_tier
        FROM ml_features.customer_ml_features
        WHERE snapshot_date = current_date
        ORDER BY {sort_col} DESC
        LIMIT $1
    """, limit)
    return [dict(r) for r in rows]

# ── PRODUCT ENDPOINTS ─────────────────────────────────────
@app.get("/api/v1/products/performance", response_model=List[ProductPerformance], tags=["products"])
async def get_product_performance(
    start_date: date = Query(default=date.today() - timedelta(days=30)),
    end_date:   date = Query(default=date.today()),
    category:   Optional[str] = None,
    limit:      int  = Query(default=50, le=500),
    conn=Depends(get_db)
):
    """Product performance: revenue, units, return rate."""
    rows = await conn.fetch("""
        SELECT
            i.product_sku,
            i.product_name,
            i.category,
            i.subcategory,
            sum(i.quantity)                                        AS total_units_sold,
            round(sum(i.total_price)::numeric, 2)                 AS total_revenue,
            round(avg(i.unit_price)::numeric, 2)                  AS avg_unit_price,
            round(sum(case when o.order_status='refunded'
                then 1 else 0 end)::numeric
                / nullif(count(distinct o.order_id), 0), 4)      AS return_rate,
            count(distinct o.order_id)                            AS order_count
        FROM gold.fact_orders o,
             o.items i
        WHERE o.order_date_day BETWEEN $1 AND $2
          AND ($3::text IS NULL OR i.category = $3)
        GROUP BY 1,2,3,4
        ORDER BY total_revenue DESC
        LIMIT $4
    """, start_date, end_date, category, limit)
    return [dict(r) for r in rows]

# ── INVENTORY ENDPOINTS ───────────────────────────────────
@app.get("/api/v1/inventory/low-stock", response_model=List[InventoryStatus], tags=["inventory"])
async def get_low_stock_items(conn=Depends(get_db)):
    """Items at or below reorder point."""
    rows = await conn.fetch("""
        SELECT warehouse_id, product_sku, quantity_available,
               is_low_stock, is_out_of_stock, reorder_point
        FROM silver.inventory
        WHERE snapshot_date = current_date
          AND is_low_stock = true
        ORDER BY quantity_available ASC
        LIMIT 500
    """)
    return [dict(r) for r in rows]

# ── PIPELINE HEALTH ───────────────────────────────────────
@app.get("/api/v1/pipeline/health", response_model=PipelineHealth, tags=["ops"])
async def get_pipeline_health(conn=Depends(get_db)):
    """Check data freshness and pipeline status."""
    row = await conn.fetchrow("""
        SELECT
            max(_processed_ts)                                   AS last_pipeline_run,
            max(order_date_day)                                  AS latest_order_date,
            count(case when order_date_day = current_date
                then 1 end)                                      AS total_orders_today,
            extract(epoch from (current_timestamp - max(_processed_ts)))
                / 3600.0                                         AS data_freshness_hours
        FROM gold.fact_orders
    """)
    freshness = row["data_freshness_hours"] or 999
    status = "healthy" if freshness < 6 else ("degraded" if freshness < 24 else "stale")
    return {**dict(row), "status": status}

# ── ATTRIBUTION ENDPOINT ──────────────────────────────────
@app.get("/api/v1/attribution/channels", tags=["marketing"])
async def get_channel_attribution(
    start_date: date = Query(default=date.today() - timedelta(days=30)),
    end_date:   date = Query(default=date.today()),
    conn=Depends(get_db)
):
    """Multi-channel attribution analysis."""
    rows = await conn.fetch("""
        SELECT
            utm_source,
            utm_medium,
            channel,
            count(distinct order_id)               AS total_orders,
            count(distinct customer_id)            AS unique_customers,
            round(sum(case when is_completed=1
                then total_amount else 0 end)::numeric, 2) AS revenue,
            round(avg(case when is_completed=1
                then total_amount end)::numeric, 2)       AS avg_order_value,
            round(100.0 * sum(case when is_completed=1
                then total_amount else 0 end)
                / sum(sum(case when is_completed=1
                    then total_amount else 0 end)) over ()
                ::numeric, 2)                             AS revenue_share_pct
        FROM gold.fact_orders
        WHERE order_date_day BETWEEN $1 AND $2
        GROUP BY 1,2,3
        ORDER BY revenue DESC
    """, start_date, end_date)
    return [dict(r) for r in rows]
