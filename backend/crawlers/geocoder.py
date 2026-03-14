"""
Geocode shops missing lat/lng using Nominatim (OpenStreetMap).
Free, no API key needed. Rate limit: 1 req/sec.

Run: python crawlers/geocoder.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import logging
import requests
from database import SessionLocal
from models import Shop

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "GoldFinderDaNang/1.0 (gold-finder-danang; educational project)",
    "Accept-Language": "vi,en",
}

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Da Nang bounding box — filter results to Da Nang only
DA_NANG_BOUNDS = {
    "viewbox": "107.9,15.9,108.4,16.2",  # lon_min,lat_min,lon_max,lat_max
    "bounded": 1,
    "countrycodes": "vn",
}

# Fallback coordinates by district if Nominatim fails
DISTRICT_FALLBACKS = {
    "Hải Châu":     (16.0544, 108.2022),
    "Thanh Khê":    (16.0700, 108.1800),
    "Sơn Trà":      (16.0945, 108.2350),
    "Ngũ Hành Sơn": (15.9930, 108.2530),
    "Liên Chiểu":   (16.1030, 108.1500),
    "Cẩm Lệ":       (16.0140, 108.2170),
    "Hòa Vang":     (16.0050, 108.1200),
    "Đà Nẵng":      (16.0544, 108.2022),
}


def nominatim_geocode(query: str) -> tuple[float, float] | None:
    """Query Nominatim for coordinates. Returns (lat, lng) or None."""
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
        **DA_NANG_BOUNDS,
    }
    try:
        resp = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=10)
        data = resp.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        logger.debug(f"Nominatim error for '{query}': {e}")
    return None


def build_queries(shop) -> list[str]:
    """Build multiple query candidates from most to least specific."""
    queries = []

    # 1. Full address + city
    if shop.address:
        addr = shop.address.strip()
        if "Đà Nẵng" not in addr:
            addr += ", Đà Nẵng"
        queries.append(addr)

        # 2. Address without shop name prefix noise
        # e.g. "215 Hùng Vương, Hải Châu, Đà Nẵng"
        import re
        street_match = re.search(r'\d+[^\,]+', addr)
        if street_match:
            queries.append(street_match.group(0) + ", Đà Nẵng, Vietnam")

    # 3. Name + district
    if shop.district and shop.district != "Đà Nẵng":
        queries.append(f"{shop.name}, {shop.district}, Đà Nẵng")

    # 4. Name + city
    queries.append(f"{shop.name}, Đà Nẵng, Vietnam")

    return queries


def geocode_shops():
    db = SessionLocal()
    shops_missing = db.query(Shop).filter(
        (Shop.lat == None) | (Shop.lng == None)
    ).all()

    total = len(shops_missing)
    logger.info(f"Geocoding {total} shops missing coordinates...\n")

    success = 0
    fallback = 0
    failed = 0

    for i, shop in enumerate(shops_missing, 1):
        logger.info(f"[{i}/{total}] {shop.name}")

        coords = None
        queries = build_queries(shop)

        for q in queries:
            coords = nominatim_geocode(q)
            if coords:
                logger.info(f"  ✅ {coords[0]:.5f}, {coords[1]:.5f}  ← '{q}'")
                break
            time.sleep(1.1)  # Nominatim rate limit: 1 req/sec

        if coords:
            shop.lat, shop.lng = coords
            success += 1
        else:
            # Use district centroid as fallback
            district_coords = DISTRICT_FALLBACKS.get(shop.district) or DISTRICT_FALLBACKS["Đà Nẵng"]
            # Add small jitter so pins don't stack exactly
            import random
            jitter = lambda: random.uniform(-0.003, 0.003)
            shop.lat = district_coords[0] + jitter()
            shop.lng = district_coords[1] + jitter()
            logger.warning(f"  ⚠️  Fallback to district centroid: {shop.district}")
            fallback += 1

        db.commit()

        # Extra delay every 10 requests to be polite
        if i % 10 == 0:
            logger.info(f"  ... pausing 3s ...")
            time.sleep(3)

    db.close()
    logger.info(f"\n🎉 Done!")
    logger.info(f"  ✅ Geocoded: {success}/{total}")
    logger.info(f"  ⚠️  Fallback: {fallback}/{total}")
    logger.info(f"  ❌ Failed:   {failed}/{total}")


if __name__ == "__main__":
    geocode_shops()
