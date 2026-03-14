"""
Enrich shops in DB with Google Places data:
- place_id
- real rating & review count
- phone, hours, website
- top 5 reviews (Vietnamese)

Run: python -m crawlers.enrich_shops
Or:  python crawlers/enrich_shops.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import logging
import requests
from dotenv import load_dotenv
from database import SessionLocal
from models import Shop, Review

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PLACES_API_BASE = "https://maps.googleapis.com/maps/api/place"


def search_place_id(name: str, address: str, api_key: str) -> str | None:
    """Find Google place_id by shop name + address."""
    query = f"{name} {address}"
    url = f"{PLACES_API_BASE}/findplacefromtext/json"
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id,name,geometry",
        "key": api_key,
        "language": "vi",
        "locationbias": "circle:30000@16.0544,108.2022",  # Da Nang center
    }
    resp = requests.get(url, params=params, timeout=10)
    data = resp.json()
    candidates = data.get("candidates", [])
    if candidates:
        place_id = candidates[0].get("place_id")
        logger.info(f"  Found: {candidates[0].get('name')} → {place_id}")
        return place_id
    logger.warning(f"  No place_id found for: {name}")
    return None


def get_place_details(place_id: str, api_key: str) -> dict:
    """Fetch details + reviews for a place_id."""
    url = f"{PLACES_API_BASE}/details/json"
    params = {
        "place_id": place_id,
        "key": api_key,
        "language": "vi",
        "fields": (
            "name,rating,user_ratings_total,"
            "formatted_phone_number,opening_hours,"
            "website,url,reviews"
        ),
    }
    resp = requests.get(url, params=params, timeout=10)
    result = resp.json().get("result", {})
    return result


def parse_hours(opening_hours: dict) -> str | None:
    weekday_text = opening_hours.get("weekday_text", [])
    if weekday_text:
        return " | ".join(weekday_text)
    return None


def enrich_all_shops():
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        logger.error("GOOGLE_MAPS_API_KEY not set in .env")
        return

    db = SessionLocal()
    shops = db.query(Shop).all()
    logger.info(f"Enriching {len(shops)} shops...")

    updated = 0
    reviews_added = 0

    for shop in shops:
        logger.info(f"\n→ {shop.name}")

        # Step 1: find place_id
        place_id = search_place_id(shop.name, shop.address or "", api_key)
        time.sleep(0.3)  # polite delay

        if not place_id:
            continue

        # Update google_maps_url with place_id
        shop.google_maps_url = f"https://maps.google.com/?cid=0&place_id={place_id}"

        # Step 2: get details
        details = get_place_details(place_id, api_key)
        time.sleep(0.3)

        # Update shop fields
        if details.get("rating"):
            shop.rating = details["rating"]
        if details.get("user_ratings_total"):
            shop.review_count = details["user_ratings_total"]
        if details.get("formatted_phone_number") and not shop.phone:
            shop.phone = details["formatted_phone_number"]
        if details.get("opening_hours") and not shop.hours:
            shop.hours = parse_hours(details["opening_hours"])
        if details.get("website") and not shop.website:
            shop.website = details["website"]
        if details.get("url"):
            shop.google_maps_url = details["url"]

        # Step 3: save reviews (skip duplicates)
        raw_reviews = details.get("reviews", [])
        existing_texts = {r.text for r in db.query(Review).filter_by(shop_id=shop.id).all()}

        for r in raw_reviews[:5]:
            text = r.get("text", "").strip()
            if not text or text in existing_texts:
                continue
            review = Review(
                shop_id=shop.id,
                text=text,
                rating=r.get("rating"),
                author=r.get("author_name"),
                date=r.get("relative_time_description"),
                source="google",
            )
            db.add(review)
            reviews_added += 1
            existing_texts.add(text)

        db.commit()
        updated += 1
        logger.info(f"  ✅ Updated. Rating: {shop.rating} ({shop.review_count} reviews), +{len(raw_reviews)} reviews saved")

    db.close()
    logger.info(f"\n🎉 Done! {updated}/{len(shops)} shops enriched, {reviews_added} reviews added.")


if __name__ == "__main__":
    enrich_all_shops()
