"""
Crawl gold shop info from Google Maps.
- Uses Google Maps Places API if GOOGLE_MAPS_API_KEY is set
- Falls back to basic scraping otherwise
"""

import os
import requests
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def crawl_via_places_api(query: str = "tiệm vàng Đà Nẵng") -> List[Dict]:
    """Use Google Places API to find gold shops."""
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        logger.info("No GOOGLE_MAPS_API_KEY, skipping Places API crawl")
        return []

    results = []
    try:
        # Text search
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            "query": query,
            "key": api_key,
            "language": "vi",
            "region": "vn",
        }
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()

        for place in data.get("results", []):
            shop = {
                "name": place.get("name"),
                "address": place.get("formatted_address"),
                "lat": place.get("geometry", {}).get("location", {}).get("lat"),
                "lng": place.get("geometry", {}).get("location", {}).get("lng"),
                "rating": place.get("rating", 0.0),
                "review_count": place.get("user_ratings_total", 0),
                "google_maps_url": f"https://maps.google.com/?place_id={place.get('place_id')}",
                "source": "google_places",
                "district": _detect_district(place.get("formatted_address", "")),
            }
            results.append(shop)

        logger.info(f"Google Places: found {len(results)} shops")
    except Exception as e:
        logger.error(f"Google Places API failed: {e}")

    return results


def _detect_district(address: str) -> str:
    """Detect Da Nang district from address string."""
    district_keywords = {
        "Hải Châu": ["Hải Châu", "Hai Chau"],
        "Thanh Khê": ["Thanh Khê", "Thanh Khe", "Thanh khê"],
        "Sơn Trà": ["Sơn Trà", "Son Tra"],
        "Ngũ Hành Sơn": ["Ngũ Hành Sơn", "Ngu Hanh Son"],
        "Liên Chiểu": ["Liên Chiểu", "Lien Chieu"],
        "Cẩm Lệ": ["Cẩm Lệ", "Cam Le"],
        "Hòa Vang": ["Hòa Vang", "Hoa Vang"],
    }
    for district, keywords in district_keywords.items():
        for kw in keywords:
            if kw.lower() in address.lower():
                return district
    return "Đà Nẵng"


def get_place_details(place_id: str, api_key: str) -> Optional[Dict]:
    """Get detailed place info including phone, hours."""
    try:
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "place_id": place_id,
            "key": api_key,
            "language": "vi",
            "fields": "name,formatted_address,formatted_phone_number,opening_hours,website,reviews,rating,user_ratings_total",
        }
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json().get("result", {})

        hours = None
        if "opening_hours" in data:
            weekday_text = data["opening_hours"].get("weekday_text", [])
            hours = " | ".join(weekday_text) if weekday_text else None

        reviews = []
        for r in data.get("reviews", [])[:5]:
            reviews.append({
                "text": r.get("text"),
                "rating": r.get("rating"),
                "author": r.get("author_name"),
                "date": r.get("relative_time_description"),
                "source": "google",
            })

        return {
            "phone": data.get("formatted_phone_number"),
            "hours": hours,
            "website": data.get("website"),
            "reviews": reviews,
        }
    except Exception as e:
        logger.error(f"Place details failed for {place_id}: {e}")
        return None
