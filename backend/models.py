from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


class Shop(Base):
    __tablename__ = "shops"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    address = Column(String(500))
    district = Column(String(100))  # Hải Châu, Thanh Khê, Sơn Trà, etc.
    lat = Column(Float)
    lng = Column(Float)
    phone = Column(String(50))
    hours = Column(String(255))
    rating = Column(Float, default=0.0)
    review_count = Column(Integer, default=0)
    description = Column(Text)
    website = Column(String(255))
    facebook_url = Column(String(255))
    google_maps_url = Column(String(500))
    source = Column(String(100), default="manual")
    is_verified = Column(Boolean, default=False)
    is_chain = Column(Boolean, default=False)  # PNJ, SJC, DOJI chains
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    reviews = relationship("Review", back_populates="shop", cascade="all, delete-orphan")
    prices = relationship("GoldPrice", back_populates="shop", cascade="all, delete-orphan")


class Review(Base):
    __tablename__ = "reviews"

    id = Column(Integer, primary_key=True, index=True)
    shop_id = Column(Integer, ForeignKey("shops.id"), nullable=False)
    text = Column(Text)
    rating = Column(Float)
    author = Column(String(255))
    date = Column(String(50))
    source = Column(String(100), default="google")
    helpful_count = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    shop = relationship("Shop", back_populates="reviews")


class GoldPrice(Base):
    __tablename__ = "gold_prices"

    id = Column(Integer, primary_key=True, index=True)
    shop_id = Column(Integer, ForeignKey("shops.id"), nullable=True)  # null = national price
    source_name = Column(String(100), nullable=False)  # SJC, PNJ, DOJI, BTMC, shop name
    gold_type = Column(String(100), nullable=False)  # "SJC 1 lượng", "Nhẫn 9999", "Vàng tây 18K"
    buy_price = Column(Float)   # Giá mua vào (VND)
    sell_price = Column(Float)  # Giá bán ra (VND)
    unit = Column(String(50), default="lượng")
    currency = Column(String(10), default="VND")
    crawled_at = Column(DateTime(timezone=True), server_default=func.now())

    shop = relationship("Shop", back_populates="prices")


class PriceSource(Base):
    __tablename__ = "price_sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    url = Column(String(500))
    last_crawled = Column(DateTime(timezone=True))
    is_active = Column(Boolean, default=True)
    crawl_interval_minutes = Column(Integer, default=60)
