"""
Facebook Group Scraper — connects to real Chrome via CDP (Port 9222).
Uses Chrome Profile 3 (bombaytera123@gmail.com) which is already logged in.

Requirements:
  Chrome must be running with --remote-debugging-port=9222
  Run: python crawlers/start_chrome_debug.py  (or manually)

Run: python crawlers/fb_group_scraper.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import asyncio
import json
import hashlib
import logging
import random
from typing import Optional

from playwright.async_api import async_playwright, Page
from database import SessionLocal
from models import Shop, Review

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CDP_URL = "http://localhost:9222"

_GROUPS_JSON = os.path.join(os.path.dirname(__file__), "fb_groups.json")

def _load_groups() -> list[dict]:
    if os.path.exists(_GROUPS_JSON):
        with open(_GROUPS_JSON, encoding="utf-8") as f:
            data = json.load(f)
        # Only use joined groups, sorted by priority
        groups = [g for g in data if g.get("status") == "joined"]
        groups.sort(key=lambda g: g.get("priority", 99))
        return groups
    # Fallback
    return [
        {"name": "CỘNG ĐỒNG VÀNG ĐÀ NẴNG", "url": "https://www.facebook.com/groups/989894385979411/", "search_query": "vàng"},
        {"name": "Hội Vàng Bạc Đà Nẵng",    "url": "https://www.facebook.com/groups/hoivangbacdanang/", "search_query": "vàng"},
    ]

TARGET_GROUPS = _load_groups()

FB_SEARCH_QUERIES = [
    "tiệm vàng đà nẵng",
    "mua vàng đà nẵng uy tín",
    "vàng PNJ đà nẵng",
    "vàng DOJI đà nẵng",
    "hiệu vàng đà nẵng review",
    "bán vàng đà nẵng",
]

GOLD_KEYWORDS = ["vàng", "tiệm vàng", "hiệu vàng", "gold", "jewelry", "trang sức",
                 "SJC", "PNJ", "DOJI", "BTMC", "mua vàng", "bán vàng", "kim cương", "bạc"]

POSITIVE_WORDS = ["tốt", "uy tín", "chuyên nghiệp", "recommend", "hài lòng", "đẹp",
                  "nhanh", "thân thiện", "chất lượng", "giá tốt", "rẻ", "tin tưởng",
                  "good", "great", "excellent", "thích", "ok", "được"]

NEGATIVE_WORDS = ["tệ", "lừa đảo", "giả", "kém", "thất vọng", "chậm", "đắt",
                  "thái độ", "bất lịch sự", "tránh", "cẩn thận", "bad", "scam", "không uy tín"]

SHOP_ALIASES: dict[str, list[str]] = {
    "PNJ":        ["pnj", "phú nhuận"],
    "DOJI":       ["doji"],
    "SJC":        ["sjc"],
    "Tứ Quý":    ["tứ quý", "tu quy"],
    "Huy Thanh": ["huy thanh"],
    "Bảo Tín":   ["bảo tín", "bao tin"],
    "Kim Khánh": ["kim khánh"],
    "HanaGold":  ["hanagold", "hana gold"],
    "Minh Hòa":  ["minh hòa", "minh hoa"],
}


def text_contains_gold(text: str) -> bool:
    t = text.lower()
    return any(kw.lower() in t for kw in GOLD_KEYWORDS)


def classify_to_shop(text: str, shop_list: list[dict]) -> Optional[int]:
    t = text.lower()
    for key, aliases in SHOP_ALIASES.items():
        if any(a in t for a in aliases):
            for s in shop_list:
                if key.lower() in s["name"].lower():
                    return s["id"]
    for s in shop_list:
        parts = [p for p in s["name"].lower().split() if len(p) >= 4]
        if any(p in t for p in parts):
            return s["id"]
    return None


def infer_rating(text: str) -> float:
    t = text.lower()
    pos = sum(1 for w in POSITIVE_WORDS if w in t)
    neg = sum(1 for w in NEGATIVE_WORDS if w in t)
    if neg >= 2: return 1.0
    if neg == 1 and pos == 0: return 2.0
    if pos >= 3: return 5.0
    if pos >= 1: return 4.0
    return 3.0


def make_fingerprint(text: str) -> str:
    return hashlib.md5(text.strip().lower().encode()).hexdigest()


async def collect_texts_from_page(page: Page, scroll_times: int = 20) -> list[str]:
    """
    Scroll slowly and extract text at every step.

    Facebook virtualizes its feed — old posts are REMOVED from DOM as you scroll down.
    Strategy:
      • Scroll in small steps (400px) so only a few posts are rendered at a time
      • Extract text BEFORE scrolling away (capture while still in DOM)
      • Long pause (2-4s) after each step so React has time to render new posts
      • Click "Xem thêm" / "See more" to expand truncated posts in the current viewport
    """
    texts: set[str] = set()

    # Wait for initial feed to load
    await asyncio.sleep(4)

    prev_height = 0

    for i in range(scroll_times):
        # --- 1. Expand truncated posts visible NOW (before scrolling away) ---
        try:
            btns = await page.query_selector_all('[role="button"]')
            for btn in btns[:8]:
                try:
                    txt = (await btn.inner_text()).strip().lower()
                    if txt in ("see more", "xem thêm", "more", "thêm"):
                        await btn.click()
                        await asyncio.sleep(0.8)
                except Exception:
                    pass
        except Exception:
            pass

        # --- 2. Extract ALL text visible in current viewport & full page ---
        try:
            body = await page.evaluate("document.body.innerText")
            new_count = 0
            for chunk in body.split("\n"):
                chunk = chunk.strip()
                if len(chunk) > 40 and text_contains_gold(chunk):
                    if chunk not in texts:
                        texts.add(chunk)
                        new_count += 1
            if new_count:
                logger.info(f"  Step {i+1}/{scroll_times}: +{new_count} texts (total={len(texts)})")
        except Exception as e:
            logger.warning(f"  Extract error: {e}")

        # --- 3. Check if we've truly hit the bottom (allow 2 consecutive same-height) ---
        try:
            cur_height = await page.evaluate("document.documentElement.scrollHeight")
            if cur_height == prev_height:
                # Wait a bit more — FB may still be loading
                await asyncio.sleep(3)
                cur_height2 = await page.evaluate("document.documentElement.scrollHeight")
                if cur_height2 == prev_height:
                    logger.info(f"  Reached page bottom at step {i+1}")
                    break
                else:
                    prev_height = cur_height2
                    continue
            prev_height = cur_height
        except Exception:
            pass

        # --- 4. Scroll slowly (small step) and wait for React to render ---
        scroll_step = random.randint(350, 500)  # small increments
        await page.evaluate(f"window.scrollBy(0, {scroll_step})")
        # Longer sleep = Facebook has time to load next batch before we extract
        await asyncio.sleep(random.uniform(2.5, 4.0))

    logger.info(f"  ✅ Collected {len(texts)} gold texts")
    return list(texts)


async def scrape_group_feed(page: Page, group: dict, scroll_times: int = 60) -> list[str]:
    """
    Scrape the full live feed of a group (no keyword filter on URL).
    Useful for high-activity groups where search returns too few results.
    Scroll deeply — FB virtualizes old posts, so we extract at every step.
    """
    base_url = group["url"].rstrip("/")
    logger.info(f"  [FEED] → {base_url}")
    try:
        await page.goto(base_url, wait_until="domcontentloaded", timeout=35000)
        await asyncio.sleep(5)
        return await collect_texts_from_page(page, scroll_times=scroll_times)
    except Exception as e:
        logger.warning(f"  Error: {e}")
        return []


async def scrape_group(page: Page, group: dict) -> list[str]:
    base_url = group["url"].rstrip("/")
    search_q = group.get("search_query", "vàng").replace(" ", "+")
    # Use group search URL which filters posts by keyword
    url = f"{base_url}?q={search_q}"
    logger.info(f"  [SEARCH] → {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=35000)
        await asyncio.sleep(5)
        current = page.url
        if "q=" not in current:
            logger.info(f"  Redirected to feed, scraping directly")
        return await collect_texts_from_page(page, scroll_times=25)
    except Exception as e:
        logger.warning(f"  Error: {e}")
        return []


async def scrape_search(page: Page, query: str) -> list[str]:
    url = f"https://www.facebook.com/search/posts/?q={query.replace(' ', '+')}"
    logger.info(f"  → {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(4)
        return await collect_texts_from_page(page, scroll_times=10)
    except Exception as e:
        logger.warning(f"  Error: {e}")
        return []


async def run(max_items: int = 1000) -> dict:
    db = SessionLocal()
    shops = [{"id": s.id, "name": s.name} for s in db.query(Shop).all()]
    existing_fps = {make_fingerprint(r.text) for r in db.query(Review).filter(
        Review.source.like("facebook%")).all() if r.text}

    stats = {"scraped": 0, "classified": 0, "unclassified": 0, "dupes": 0, "saved": 0}
    all_texts: list[str] = []

    async with async_playwright() as p:
        # Connect to real Chrome running with --remote-debugging-port=9222
        try:
            browser = await p.chromium.connect_over_cdp(CDP_URL)
            logger.info(f"✅ Connected to Chrome via CDP: {CDP_URL}")
        except Exception as e:
            logger.error(f"❌ Cannot connect to Chrome CDP: {e}")
            logger.error("Run: python crawlers/start_chrome_debug.py first!")
            db.close()
            return {"error": "cdp_not_available", "detail": str(e)}

        # Use existing context (has cookies/session)
        contexts = browser.contexts
        ctx = contexts[0] if contexts else await browser.new_context()

        # Open a new page for scraping (don't hijack existing tabs)
        page = await ctx.new_page()

        # Verify login
        await page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)
        body = await page.evaluate("document.body.innerText")
        is_logged_in = "Log in" not in body[:100] and len(body) > 500

        logger.info(f"Login check — body length: {len(body)}, logged_in: {is_logged_in}")
        if not is_logged_in:
            logger.error("Not logged in!")
            await page.close()
            db.close()
            return {"error": "not_logged_in"}

        logger.info(f"✅ Logged in! Body preview: {body[:100]}")

        # 1. Full feed scrape for top-priority gold groups (most valuable)
        top_groups = [g for g in TARGET_GROUPS if g.get("type") == "gold_specific"]
        other_groups = [g for g in TARGET_GROUPS if g.get("type") != "gold_specific"]

        for group in top_groups:
            logger.info(f"\n🏆 {group['name']} [FULL FEED — {group.get('posts_day',0)}+ posts/day]")
            texts = await scrape_group_feed(page, group, scroll_times=80)
            all_texts.extend(texts)
            await asyncio.sleep(random.uniform(3, 5))

        # 2. Search-filtered scrape for general groups (less noise)
        for group in other_groups:
            logger.info(f"\n📦 {group['name']} [SEARCH]")
            texts = await scrape_group(page, group)
            all_texts.extend(texts)
            await asyncio.sleep(random.uniform(2, 4))

        # 3. FB global search for extra coverage
        for query in FB_SEARCH_QUERIES:
            logger.info(f"\n🔍 '{query}'")
            texts = await scrape_search(page, query)
            all_texts.extend(texts)
            await asyncio.sleep(random.uniform(2, 4))

        await page.close()
        # Don't close the browser — it's the real user's Chrome!

    # Dedupe + classify + save
    all_texts = list(set(all_texts))
    stats["scraped"] = len(all_texts)
    logger.info(f"\n📊 Total unique gold texts: {len(all_texts)}")

    for text in all_texts[:max_items]:
        fp = make_fingerprint(text)
        if fp in existing_fps:
            stats["dupes"] += 1
            continue
        shop_id = classify_to_shop(text, shops)
        if shop_id is None:
            # Skip reviews that can't be matched — shop_id is NOT NULL in schema
            stats["unclassified"] += 1
            continue
        review = Review(
            shop_id=shop_id,
            text=text[:2000],
            rating=infer_rating(text),
            author="facebook_user",
            source="facebook_group",
        )
        db.add(review)
        existing_fps.add(fp)
        stats["classified"] += 1
        stats["saved"] += 1

    db.commit()
    db.close()
    logger.info(f"\n🎉 Done! {stats}")
    return stats


if __name__ == "__main__":
    result = asyncio.run(run())
    print("\nResult:", json.dumps(result, ensure_ascii=False, indent=2))
