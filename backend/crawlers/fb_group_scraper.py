"""
Step 2: Facebook Group Scraper using saved session.
Scrapes public groups for gold shop reviews/comments in Da Nang.

Requires: fb_session.json (run save_fb_session.py first)
Run: python crawlers/fb_group_scraper.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import asyncio
import json
import hashlib
import logging
import re
import random
import time
from pathlib import Path
from typing import Optional
from datetime import datetime

from playwright.async_api import async_playwright, Page
from database import SessionLocal
from models import Shop, Review

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SESSION_FILE = Path(__file__).parent / "fb_session.json"

# Target groups — public groups about buying/selling + reviews in Da Nang
TARGET_GROUPS = [
    {
        "name": "Mua Bán Đà Nẵng",
        "url": "https://www.facebook.com/groups/muabandanang",
        "search": "vàng",
    },
    {
        "name": "Đà Nẵng City",
        "url": "https://www.facebook.com/groups/danangcity",
        "search": "tiệm vàng",
    },
    {
        "name": "Review Đà Nẵng",
        "url": "https://www.facebook.com/groups/reviewdanang",
        "search": "vàng",
    },
    {
        "name": "Hội mua bán Đà Nẵng",
        "url": "https://www.facebook.com/groups/hoichodanang",
        "search": "vàng bạc",
    },
]

# Additional: search posts directly
FB_SEARCH_QUERIES = [
    "tiệm vàng đà nẵng review",
    "mua vàng đà nẵng uy tín",
    "vàng PNJ đà nẵng",
    "vàng DOJI đà nẵng",
    "tiệm vàng đà nẵng recommend",
    "bán vàng đà nẵng",
]

GOLD_KEYWORDS = [
    "vàng", "tiệm vàng", "hiệu vàng", "cửa hàng vàng",
    "gold", "jewelry", "trang sức", "kim cương", "bạc",
    "SJC", "PNJ", "DOJI", "BTMC", "mua vàng", "bán vàng",
]

POSITIVE_WORDS = [
    "tốt", "uy tín", "chuyên nghiệp", "recommend", "hài lòng",
    "đẹp", "nhanh", "thân thiện", "nhiệt tình", "chất lượng",
    "giá tốt", "rẻ", "hợp lý", "tin tưởng", "đảm bảo", "good",
    "great", "excellent", "nice", "love", "thích",
]

NEGATIVE_WORDS = [
    "tệ", "lừa đảo", "giả", "kém", "thất vọng", "chậm",
    "đắt", "thái độ", "bất lịch sự", "không hài lòng",
    "tránh", "cẩn thận", "bad", "terrible", "scam",
]

SHOP_ALIASES: dict[str, list[str]] = {
    "PNJ": ["pnj", "phú nhuận", "phu nhuan jewelry"],
    "DOJI": ["doji", "trung tâm vàng bạc doji"],
    "SJC": ["sjc", "sài gòn kim cương", "saigon jewelry"],
    "Tứ Quý": ["tứ quý", "tu quy"],
    "Huy Thanh": ["huy thanh jewelry", "huy thanh"],
    "Bảo Tín": ["bảo tín", "bao tin", "baotinmanhhai"],
    "Kim Khánh": ["kim khánh", "kim khanh"],
    "Ngọc Thịnh": ["ngọc thịnh", "ngoc thinh"],
    "HanaGold": ["hanagold", "hana gold"],
    "Minh Hòa": ["minh hòa", "minh hoa"],
    "Thanh Bình": ["thanh bình", "thanh binh"],
    "Đại Hòa": ["đại hòa", "dai hoa"],
    "Kim Long": ["kim long"],
    "Ánh Kim": ["ánh kim", "anh kim"],
    "Phúc Lợi": ["phúc lợi", "phuc loi"],
}


def text_contains_gold(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in GOLD_KEYWORDS)


def classify_to_shop(text: str, shop_list: list[dict]) -> Optional[int]:
    text_lower = text.lower()

    # 1. Match by alias dict first
    for shop_key, aliases in SHOP_ALIASES.items():
        if any(alias in text_lower for alias in aliases):
            # Find shop_id in DB
            for s in shop_list:
                if shop_key.lower() in s["name"].lower():
                    return s["id"]

    # 2. Direct shop name substring match
    for s in shop_list:
        name = s["name"].lower()
        # Try partial name (first meaningful word ≥ 4 chars)
        parts = [p for p in name.split() if len(p) >= 4]
        if any(p in text_lower for p in parts):
            return s["id"]

    return None


def infer_rating(text: str) -> float:
    text_lower = text.lower()
    pos = sum(1 for w in POSITIVE_WORDS if w in text_lower)
    neg = sum(1 for w in NEGATIVE_WORDS if w in text_lower)
    if neg >= 2:
        return 1.0
    if neg == 1 and pos == 0:
        return 2.0
    if pos >= 3:
        return 5.0
    if pos >= 1:
        return 4.0
    return 3.0


def make_fingerprint(text: str) -> str:
    return hashlib.md5(text.strip().lower().encode()).hexdigest()


async def scroll_and_collect(page: Page, scroll_times: int = 5) -> list[str]:
    """Scroll page and collect all post/comment text blocks."""
    texts = []
    for i in range(scroll_times):
        await page.evaluate("window.scrollBy(0, 1200)")
        await asyncio.sleep(random.uniform(2, 4))

        # Grab text from post bodies
        elements = await page.query_selector_all(
            '[data-ad-preview="message"], [dir="auto"] span, .x1iorvi4 span'
        )
        for el in elements:
            try:
                t = await el.inner_text()
                if t and len(t) > 20 and text_contains_gold(t):
                    texts.append(t.strip())
            except Exception:
                pass

    return list(set(texts))  # dedupe by exact text


async def expand_comments(page: Page):
    """Click 'View more comments' buttons."""
    for _ in range(5):
        try:
            btn = await page.query_selector('[aria-label*="comment"], [role="button"]:has-text("View")')
            if btn:
                await btn.click()
                await asyncio.sleep(1.5)
        except Exception:
            break


async def search_group(page: Page, group_url: str, search_term: str) -> list[str]:
    """Navigate to group search results."""
    # Facebook group search: /groups/GROUPID?q=QUERY
    search_url = f"{group_url}?q={search_term.replace(' ', '%20')}"
    logger.info(f"  Searching: {search_url}")
    try:
        await page.goto(search_url, timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(3)
        return await scroll_and_collect(page, scroll_times=5)
    except Exception as e:
        logger.warning(f"  Error: {e}")
        return []


async def search_fb_posts(page: Page, query: str) -> list[str]:
    """Use Facebook search for posts."""
    url = f"https://www.facebook.com/search/posts/?q={query.replace(' ', '%20')}"
    logger.info(f"  FB Search: {url}")
    try:
        await page.goto(url, timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(3)
        return await scroll_and_collect(page, scroll_times=6)
    except Exception as e:
        logger.warning(f"  Error: {e}")
        return []


async def run(max_items: int = 500):
    if not SESSION_FILE.exists():
        logger.error(f"Session file not found: {SESSION_FILE}")
        logger.error("Run: python crawlers/save_fb_session.py first!")
        return {"error": "no_session"}

    db = SessionLocal()
    shops = [{"id": s.id, "name": s.name} for s in db.query(Shop).all()]

    # Load existing fingerprints to avoid dupes
    existing = {r.text_fingerprint for r in db.query(Review).filter(
        Review.source.like("facebook%")
    ).all() if hasattr(r, "text_fingerprint") and r.text_fingerprint}

    stats = {"scraped": 0, "classified": 0, "unclassified": 0, "dupes": 0, "saved": 0}
    all_texts: list[str] = []

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir="",  # temp
            headless=True,
            storage_state=str(SESSION_FILE),
            locale="vi-VN",
            viewport={"width": 1366, "height": 768},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        page = await context.new_page()

        # Verify session is valid
        await page.goto("https://www.facebook.com/", wait_until="domcontentloaded")
        await asyncio.sleep(3)
        if "login" in page.url:
            logger.error("Session expired! Re-run save_fb_session.py")
            await context.close()
            db.close()
            return {"error": "session_expired"}

        logger.info(f"✅ Session valid. Logged in at: {page.url}")

        # 1. Scrape groups
        for group in TARGET_GROUPS:
            logger.info(f"\n📦 Group: {group['name']}")
            texts = await search_group(page, group["url"], group["search"])
            logger.info(f"  Found {len(texts)} gold-related texts")
            all_texts.extend(texts)
            await asyncio.sleep(random.uniform(3, 6))

        # 2. Scrape search results
        for query in FB_SEARCH_QUERIES:
            logger.info(f"\n🔍 Search: '{query}'")
            texts = await search_fb_posts(page, query)
            logger.info(f"  Found {len(texts)} gold-related texts")
            all_texts.extend(texts)
            await asyncio.sleep(random.uniform(3, 6))

        await context.close()

    # Dedupe by content
    all_texts = list(set(all_texts))
    stats["scraped"] = len(all_texts)
    logger.info(f"\n📊 Total unique texts: {len(all_texts)}")

    # Classify + save
    for text in all_texts[:max_items]:
        fp = make_fingerprint(text)
        if fp in existing:
            stats["dupes"] += 1
            continue

        shop_id = classify_to_shop(text, shops)
        rating = infer_rating(text)
        source = "facebook_group"

        review = Review(
            shop_id=shop_id,
            text=text[:2000],
            rating=rating,
            author="facebook_user",
            source=source,
        )
        # Store fingerprint if model supports it
        if hasattr(review, "text_fingerprint"):
            review.text_fingerprint = fp

        db.add(review)
        existing.add(fp)

        if shop_id:
            stats["classified"] += 1
        else:
            stats["unclassified"] += 1
        stats["saved"] += 1

    db.commit()
    db.close()

    stats["total_texts"] = len(all_texts)
    logger.info(f"\n🎉 Done! {stats}")
    return stats


if __name__ == "__main__":
    result = asyncio.run(run())
    print("\nResult:", json.dumps(result, ensure_ascii=False, indent=2))
