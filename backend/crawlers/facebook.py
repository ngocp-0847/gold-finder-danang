"""
Facebook crawler for gold shop pages and community group posts.
- Uses FB Graph API if FB_ACCESS_TOKEN is set
- Otherwise scrapes public page info via mbasic.facebook.com
"""

import os
import requests
import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Public Facebook pages of known Da Nang gold shops
KNOWN_FB_PAGES = [
    {"page_id": "pnj.danang", "shop_name": "PNJ Đà Nẵng"},
    {"page_id": "doji.danang", "shop_name": "DOJI Đà Nẵng"},
]

# Community groups (public) where people discuss gold prices in Da Nang
COMMUNITY_GROUPS = [
    "groups/muabandanang",
    "groups/danang.community",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
}


def scrape_public_page(page_name: str) -> Optional[Dict]:
    """Scrape basic info from a public Facebook page via mbasic."""
    try:
        url = f"https://mbasic.facebook.com/{page_name}"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, "lxml")

        title = soup.find("title")
        name = title.get_text(strip=True) if title else page_name

        # Try to find posts
        posts = []
        for article in soup.find_all("div", {"data-ft": True})[:5]:
            text = article.get_text(separator=" ", strip=True)
            if text and ("vàng" in text.lower() or "gold" in text.lower() or "giá" in text.lower()):
                posts.append(text[:500])

        return {
            "page_name": page_name,
            "title": name,
            "recent_posts": posts,
        }
    except Exception as e:
        logger.error(f"FB scrape failed for {page_name}: {e}")
        return None


def get_page_posts_via_api(page_id: str, access_token: str) -> List[Dict]:
    """Get recent posts from a FB page via Graph API."""
    results = []
    try:
        url = f"https://graph.facebook.com/v19.0/{page_id}/posts"
        params = {
            "access_token": access_token,
            "fields": "message,created_time,full_picture",
            "limit": 10,
        }
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()

        for post in data.get("data", []):
            msg = post.get("message", "")
            if msg:
                results.append({
                    "text": msg[:500],
                    "date": post.get("created_time", ""),
                    "source": "facebook",
                })
    except Exception as e:
        logger.error(f"FB Graph API failed for {page_id}: {e}")
    return results


def crawl_facebook_prices() -> List[Dict]:
    """
    Try to find gold price mentions from Facebook.
    Returns list of price-related posts.
    """
    access_token = os.getenv("FB_ACCESS_TOKEN")
    results = []

    if access_token:
        for page in KNOWN_FB_PAGES:
            posts = get_page_posts_via_api(page["page_id"], access_token)
            for p in posts:
                p["shop_name"] = page["shop_name"]
                results.append(p)
        logger.info(f"FB API: got {len(results)} posts")
    else:
        logger.info("FB_ACCESS_TOKEN not set, skipping Facebook crawl")
        # Basic scrape fallback
        for page in KNOWN_FB_PAGES[:2]:
            data = scrape_public_page(page["page_id"])
            if data and data.get("recent_posts"):
                results.append({
                    "shop_name": page["shop_name"],
                    "posts": data["recent_posts"],
                    "source": "facebook_scrape",
                })

    return results
