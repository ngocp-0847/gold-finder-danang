from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class ReviewBase(BaseModel):
    text: Optional[str]
    rating: Optional[float]
    author: Optional[str]
    date: Optional[str]
    source: str = "google"


class ReviewOut(ReviewBase):
    id: int
    shop_id: int
    created_at: Optional[datetime]

    class Config:
        from_attributes = True


class GoldPriceOut(BaseModel):
    id: int
    source_name: str
    gold_type: str
    buy_price: Optional[float]
    sell_price: Optional[float]
    unit: str
    crawled_at: Optional[datetime]

    class Config:
        from_attributes = True


class ShopBase(BaseModel):
    name: str
    address: Optional[str]
    district: Optional[str]
    lat: Optional[float]
    lng: Optional[float]
    phone: Optional[str]
    hours: Optional[str]
    rating: float = 0.0
    review_count: int = 0
    description: Optional[str]
    website: Optional[str]
    facebook_url: Optional[str]
    google_maps_url: Optional[str]
    is_chain: bool = False


class ShopOut(ShopBase):
    id: int
    is_verified: bool
    source: str
    created_at: Optional[datetime]
    distance_km: Optional[float] = None  # populated when nearby search
    latest_prices: Optional[List[GoldPriceOut]] = []
    recent_reviews: Optional[List[ReviewOut]] = []

    class Config:
        from_attributes = True


class PriceCompareItem(BaseModel):
    source_name: str
    shop_id: Optional[int]
    shop_name: Optional[str]
    gold_type: str
    buy_price: Optional[float]
    sell_price: Optional[float]
    crawled_at: Optional[datetime]


class StatsOut(BaseModel):
    total_shops: int
    total_reviews: int
    last_price_update: Optional[datetime]
    price_sources_count: int
    districts: List[str]
