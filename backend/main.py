"""
Vàng Đà Nẵng API - FastAPI backend
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc, func

from database import get_db, engine, SessionLocal
from models import Base, Shop, Review, GoldPrice, PriceSource
from schemas import ShopOut, ReviewOut, GoldPriceOut, PriceCompareItem, StatsOut
from crawlers.gold_prices import crawl_all_prices
from crawlers.google_maps import crawl_via_places_api
from crawlers.price_pipeline import run_pipeline

# Init DB
Base.metadata.create_all(bind=engine)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Vàng Đà Nẵng API",
    description="API tìm kiếm và so sánh giá tiệm vàng tại Đà Nẵng",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ────────────────────────────────────────────
# Startup / Shutdown events
# ────────────────────────────────────────────

@app.on_event("startup")
def on_startup():
    from scheduler import start_scheduler
    start_scheduler()
    logger.info("Background price scheduler started")


@app.on_event("shutdown")
def on_shutdown():
    from scheduler import stop_scheduler
    stop_scheduler()
    logger.info("Background price scheduler stopped")

# Serve frontend static files
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")


# ────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────

def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate distance between two coordinates in km."""
    import math
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def shop_to_out(shop: Shop, lat: float = None, lng: float = None, db: Session = None) -> dict:
    data = {
        "id": shop.id,
        "name": shop.name,
        "address": shop.address,
        "district": shop.district,
        "lat": shop.lat,
        "lng": shop.lng,
        "phone": shop.phone,
        "hours": shop.hours,
        "rating": shop.rating,
        "review_count": shop.review_count,
        "description": shop.description,
        "website": shop.website,
        "facebook_url": shop.facebook_url,
        "google_maps_url": shop.google_maps_url,
        "is_chain": shop.is_chain,
        "is_verified": shop.is_verified,
        "source": shop.source,
        "created_at": shop.created_at,
        "distance_km": None,
        "latest_prices": [],
        "recent_reviews": [],
    }
    if lat and lng and shop.lat and shop.lng:
        data["distance_km"] = round(haversine_km(lat, lng, shop.lat, shop.lng), 2)

    if db:
        prices = db.query(GoldPrice).filter(
            GoldPrice.shop_id == shop.id
        ).order_by(desc(GoldPrice.crawled_at)).limit(5).all()
        data["latest_prices"] = [
            {"id": p.id, "source_name": p.source_name, "gold_type": p.gold_type,
             "buy_price": p.buy_price, "sell_price": p.sell_price, "unit": p.unit,
             "crawled_at": p.crawled_at}
            for p in prices
        ]
        reviews = db.query(Review).filter(
            Review.shop_id == shop.id
        ).order_by(desc(Review.id)).limit(3).all()
        data["recent_reviews"] = [
            {"id": r.id, "shop_id": r.shop_id, "text": r.text, "rating": r.rating,
             "author": r.author, "date": r.date, "source": r.source,
             "helpful_count": r.helpful_count, "created_at": r.created_at}
            for r in reviews
        ]
    return data


# ────────────────────────────────────────────
# Routes
# ────────────────────────────────────────────

@app.get("/")
def index():
    index_path = os.path.join(frontend_path, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Vàng Đà Nẵng API", "docs": "/docs"}


@app.get("/api/stats", response_model=StatsOut)
def get_stats(db: Session = Depends(get_db)):
    total_shops = db.query(Shop).count()
    total_reviews = db.query(Review).count()
    last_price = db.query(GoldPrice).order_by(desc(GoldPrice.crawled_at)).first()
    price_sources = db.query(PriceSource).filter(PriceSource.is_active == True).count()
    districts = [r[0] for r in db.query(Shop.district).distinct().all() if r[0]]

    return {
        "total_shops": total_shops,
        "total_reviews": total_reviews,
        "last_price_update": last_price.crawled_at if last_price else None,
        "price_sources_count": price_sources,
        "districts": sorted(districts),
    }


@app.get("/api/shops")
def list_shops(
    district: Optional[str] = None,
    min_rating: Optional[float] = None,
    search: Optional[str] = None,
    is_chain: Optional[bool] = None,
    limit: int = Query(50, le=200),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    q = db.query(Shop)

    if district:
        q = q.filter(Shop.district.ilike(f"%{district}%"))
    if min_rating is not None:
        q = q.filter(Shop.rating >= min_rating)
    if search:
        q = q.filter(Shop.name.ilike(f"%{search}%") | Shop.address.ilike(f"%{search}%"))
    if is_chain is not None:
        q = q.filter(Shop.is_chain == is_chain)

    shops = q.order_by(desc(Shop.rating)).offset(offset).limit(limit).all()
    return [shop_to_out(s) for s in shops]


@app.get("/api/shops/nearby")
def nearby_shops(
    lat: float = Query(..., description="Vĩ độ người dùng"),
    lng: float = Query(..., description="Kinh độ người dùng"),
    radius_km: float = Query(5.0, description="Bán kính tìm kiếm (km)"),
    limit: int = Query(20, le=50),
    db: Session = Depends(get_db),
):
    """Find shops within radius_km from given coordinates, sorted by distance."""
    shops = db.query(Shop).filter(Shop.lat.isnot(None), Shop.lng.isnot(None)).all()

    nearby = []
    for shop in shops:
        dist = haversine_km(lat, lng, shop.lat, shop.lng)
        if dist <= radius_km:
            data = shop_to_out(shop, lat=lat, lng=lng, db=db)
            nearby.append(data)

    nearby.sort(key=lambda x: x["distance_km"] or 999)
    return nearby[:limit]


@app.get("/api/shops/{shop_id}")
def get_shop(shop_id: int, db: Session = Depends(get_db)):
    shop = db.query(Shop).filter(Shop.id == shop_id).first()
    if not shop:
        raise HTTPException(status_code=404, detail="Không tìm thấy tiệm vàng")
    return shop_to_out(shop, db=db)


@app.get("/api/prices/latest")
def get_latest_prices(
    source: Optional[str] = None,
    gold_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Get latest gold prices, optionally filtered by source or type."""
    # Get latest per source+gold_type combo
    subq = (
        db.query(
            GoldPrice.source_name,
            GoldPrice.gold_type,
            func.max(GoldPrice.crawled_at).label("max_ts"),
        )
        .group_by(GoldPrice.source_name, GoldPrice.gold_type)
        .subquery()
    )

    q = db.query(GoldPrice).join(
        subq,
        (GoldPrice.source_name == subq.c.source_name)
        & (GoldPrice.gold_type == subq.c.gold_type)
        & (GoldPrice.crawled_at == subq.c.max_ts),
    )

    if source:
        q = q.filter(GoldPrice.source_name.ilike(f"%{source}%"))
    if gold_type:
        q = q.filter(GoldPrice.gold_type.ilike(f"%{gold_type}%"))

    prices = q.order_by(GoldPrice.source_name, GoldPrice.gold_type).all()
    return [
        {
            "id": p.id,
            "source_name": p.source_name,
            "gold_type": p.gold_type,
            "buy_price": p.buy_price,
            "sell_price": p.sell_price,
            "unit": p.unit,
            "crawled_at": p.crawled_at,
        }
        for p in prices
    ]


@app.get("/api/prices/compare")
def compare_prices(
    gold_type: str = Query("SJC", description="Loại vàng cần so sánh"),
    db: Session = Depends(get_db),
):
    """Compare prices for a specific gold type across all sources."""
    subq = (
        db.query(
            GoldPrice.source_name,
            func.max(GoldPrice.crawled_at).label("max_ts"),
        )
        .filter(GoldPrice.gold_type.ilike(f"%{gold_type}%"))
        .group_by(GoldPrice.source_name)
        .subquery()
    )

    prices = (
        db.query(GoldPrice)
        .join(
            subq,
            (GoldPrice.source_name == subq.c.source_name)
            & (GoldPrice.crawled_at == subq.c.max_ts),
        )
        .filter(GoldPrice.gold_type.ilike(f"%{gold_type}%"))
        .all()
    )

    results = []
    for p in prices:
        shop_name = None
        if p.shop_id:
            shop = db.query(Shop).filter(Shop.id == p.shop_id).first()
            shop_name = shop.name if shop else None
        results.append({
            "source_name": p.source_name,
            "shop_id": p.shop_id,
            "shop_name": shop_name,
            "gold_type": p.gold_type,
            "buy_price": p.buy_price,
            "sell_price": p.sell_price,
            "crawled_at": p.crawled_at,
        })

    # Sort by sell_price ascending (best deal for buyer)
    results.sort(key=lambda x: x["sell_price"] or float("inf"))
    return results


@app.post("/api/crawl/prices")
async def trigger_price_crawl(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Trigger a gold price crawl in background."""
    def do_crawl():
        try:
            prices = crawl_all_prices()
            _db = next(get_db())
            for p in prices:
                price = GoldPrice(**p)
                _db.add(price)
            # Update price sources last_crawled
            for ps in _db.query(PriceSource).filter(PriceSource.is_active == True).all():
                ps.last_crawled = datetime.utcnow()
            _db.commit()
            logger.info(f"Price crawl complete: {len(prices)} prices saved")
        except Exception as e:
            logger.error(f"Price crawl background task failed: {e}")

    background_tasks.add_task(do_crawl)
    return {"message": "Đang crawl giá vàng...", "status": "started"}


@app.post("/api/crawl/shops")
async def trigger_shop_crawl(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Trigger a shop crawl from Google Maps (requires GOOGLE_MAPS_API_KEY)."""
    def do_crawl():
        try:
            shops = crawl_via_places_api()
            if not shops:
                logger.info("No shops from Places API (no API key?)")
                return
            _db = next(get_db())
            new_count = 0
            for s in shops:
                exists = _db.query(Shop).filter(Shop.name == s["name"]).first()
                if not exists:
                    shop = Shop(**s)
                    _db.add(shop)
                    new_count += 1
            _db.commit()
            logger.info(f"Shop crawl complete: {new_count} new shops added")
        except Exception as e:
            logger.error(f"Shop crawl failed: {e}")

    background_tasks.add_task(do_crawl)
    return {"message": "Đang crawl danh sách tiệm vàng...", "status": "started"}


@app.get("/api/districts")
def list_districts(db: Session = Depends(get_db)):
    districts = [r[0] for r in db.query(Shop.district).distinct().all() if r[0]]
    return sorted(districts)


# ────────────────────────────────────────────
# Realtime price pipeline endpoints
# ────────────────────────────────────────────

@app.get("/prices/live")
def get_live_prices():
    """
    Immediately crawl all sources and return summary.
    Prices are also saved to the database.
    """
    db = SessionLocal()
    try:
        summary = run_pipeline(db)
        return summary
    finally:
        db.close()


@app.get("/prices/history")
def get_price_history(
    source: Optional[str] = Query(None, description="Filter by source_name (e.g. SJC, PNJ)"),
    hours: int = Query(24, description="Look back N hours", ge=1, le=720),
    db: Session = Depends(get_db),
):
    """
    Return gold price history from the database for the last N hours.
    Optionally filter by source_name.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    q = db.query(GoldPrice).filter(GoldPrice.crawled_at >= cutoff)
    if source:
        q = q.filter(GoldPrice.source_name.ilike(f"%{source}%"))
    prices = q.order_by(desc(GoldPrice.crawled_at)).limit(500).all()
    return [
        {
            "id": p.id,
            "shop_id": p.shop_id,
            "source_name": p.source_name,
            "gold_type": p.gold_type,
            "buy_price": p.buy_price,
            "sell_price": p.sell_price,
            "unit": p.unit,
            "crawled_at": p.crawled_at,
        }
        for p in prices
    ]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
