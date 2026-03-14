"""
Facebook comment/review crawler for gold shops in Da Nang.
- Primary: mbasic.facebook.com (no login required for public content)
  → NOTE: As of 2025, Facebook redirects ALL unauthenticated traffic to login.
           This includes mbasic.facebook.com search and group pages.
- Fallback 1: Google Search (site:facebook.com "tiệm vàng" "đà nẵng")
- Fallback 2: Vietnamese forums (webtretho.com, otofun.net)
- Fallback 3: Reddit r/Vietnam
- Fallback 4: Vietnamese review sites (foody.vn, reviewdanang-style via web)

Sources confirmed working (no login required):
  ✓ Google Search (indirect FB content scraping)
  ✓ webtretho.com  
  ✓ otofun.net
  ✓ old.reddit.com/r/Vietnam
  ✗ mbasic.facebook.com (redirects to login)
  ✗ m.facebook.com (redirects to login)
  ✗ facebook.com groups/search (redirects to login)
"""

import hashlib
import logging
import re
import time
import unicodedata
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# Shop alias mapping for classification
# ─────────────────────────────────────────────────────────

SHOP_ALIASES: Dict[str, List[str]] = {
    "PNJ": ["pnj", "phú nhuận", "phu nhuan jewelry", "phu nhuan", "cửa hàng pnj",
            "trang sức pnj", "tiệm vàng pnj"],
    "DOJI": ["doji", "trung tâm vàng bạc doji", "trung tâm vàng doji",
             "vàng bạc đá quý doji"],
    "SJC": ["sjc", "sài gòn kim cương", "saigon jewelry", "vàng sjc",
            "công ty vàng bạc đá quý sài gòn", "sjc miền trung"],
    "Tứ Quý": ["tứ quý", "tu quy jewelry", "tu quy", "tứ quý jewelry"],
    "Huy Thanh": ["huy thanh jewelry", "huy thanh", "hiệu vàng huy thanh"],
    "Bảo Tín": ["bảo tín", "bao tin", "baotinmanhhai", "bảo tín đà nẵng",
                "bảo tín minh châu"],
    "Kim Khánh": ["kim khánh", "kim khanh", "tiệm vàng kim khánh",
                  "kim khánh việt hùng"],
    "Hoa Kim": ["hoa kim", "hoa kim nguyên", "tiệm vàng hoa kim"],
    "DOJI Smart": ["doji smart", "trung tâm vàng bạc đá quý doji smart"],
    "HanaGold": ["hanagold", "hana gold", "trung tâm vàng bạc trang sức hanagold"],
}

# Vietnamese positive/negative sentiment words
POSITIVE_WORDS = [
    "tốt", "uy tín", "chất lượng", "recommend", "tuyệt", "hài lòng",
    "đáng tin", "tốt bụng", "nhiệt tình", "chu đáo", "xuất sắc",
    "thân thiện", "nhanh", "đẹp", "chuẩn", "ok", "oke", "tuyệt vời",
    "thích", "thật", "ngon", "rẻ", "good", "great", "nice", "honest",
    "trusted", "reliable", "tin tưởng", "đảm bảo", "cẩn thận",
    "chính hãng", "rõ ràng", "minh bạch", "công khai",
]

NEGATIVE_WORDS = [
    "tệ", "lừa đảo", "tránh", "kém", "thất vọng", "xấu", "bể", "hỏng",
    "lừa", "đắt", "chặt chém", "không uy tín", "tránh xa", "cẩn thận",
    "nghi ngờ", "giả", "nhái", "kém chất lượng", "chênh lệch",
    "không trung thực", "gian lận", "bad", "awful", "terrible",
    "scam", "fake", "overpriced", "rip off",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

MOBILE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9",
}


# ─────────────────────────────────────────────────────────
# Utility functions
# ─────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    """Lowercase and normalize Vietnamese text for matching."""
    text = text.lower().strip()
    # Normalize unicode (NFC)
    text = unicodedata.normalize("NFC", text)
    return text


def text_fingerprint(text: str) -> str:
    """MD5 fingerprint for deduplication."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def polite_delay(seconds: float = 2.5):
    """Polite delay between requests."""
    time.sleep(seconds)


def safe_get(url: str, headers: dict = None, timeout: int = 15) -> Optional[requests.Response]:
    """HTTP GET with error handling."""
    try:
        resp = requests.get(
            url,
            headers=headers or HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )
        if resp.status_code == 200:
            return resp
        logger.warning(f"HTTP {resp.status_code} for {url}")
        return None
    except Exception as e:
        logger.error(f"GET failed for {url}: {e}")
        return None


# ─────────────────────────────────────────────────────────
# Part 1: mbasic.facebook.com (research/test only)
# ─────────────────────────────────────────────────────────

def test_mbasic_access() -> dict:
    """
    Test if mbasic.facebook.com is accessible without login.
    Returns dict with status info.
    """
    test_urls = [
        "https://mbasic.facebook.com/",
        "https://mbasic.facebook.com/groups/muabandanang",
        "https://mbasic.facebook.com/search/posts/?q=ti%E1%BB%87m+v%C3%A0ng+%C4%91%C3%A0+n%E1%BA%B5ng",
    ]
    results = {}
    for url in test_urls:
        resp = safe_get(url, headers=MOBILE_HEADERS, timeout=10)
        if resp:
            # Check if redirected to login
            final_url = resp.url
            is_login_redirect = "login" in final_url or "login" in resp.text[:500]
            results[url] = {
                "accessible": not is_login_redirect,
                "final_url": final_url,
                "status": "login_redirect" if is_login_redirect else "ok",
            }
        else:
            results[url] = {"accessible": False, "status": "error"}
        polite_delay(1)
    return results


def scrape_mbasic_search(query: str, page_count: int = 3) -> List[Dict]:
    """
    Attempt to scrape mbasic.facebook.com/search for posts about gold shops.
    NOTE: As of 2025, Facebook redirects to login page.
          This function returns empty list but logs the attempt.
    """
    results = []
    encoded_query = quote_plus(query)
    url = f"https://mbasic.facebook.com/search/posts/?q={encoded_query}"

    resp = safe_get(url, headers=MOBILE_HEADERS)
    if not resp:
        logger.info(f"mbasic search: no response for '{query}'")
        return results

    # Check for login redirect
    if "login" in resp.url or "Đăng nhập" in resp.text[:2000]:
        logger.info(
            f"mbasic.facebook.com requires login for search (redirected to {resp.url}). "
            "Falling back to alternative sources."
        )
        return results

    soup = BeautifulSoup(resp.text, "lxml")
    posts = _extract_mbasic_posts(soup)
    results.extend(posts)
    logger.info(f"mbasic search '{query}': found {len(posts)} posts")
    return results


def scrape_mbasic_group(group_url: str, page_count: int = 5) -> List[Dict]:
    """
    Attempt to scrape a public Facebook group via mbasic.
    NOTE: As of 2025, Facebook redirects to login page.
    """
    results = []
    # Convert to mbasic URL
    mbasic_url = group_url.replace("facebook.com", "mbasic.facebook.com")
    if not mbasic_url.startswith("https://mbasic"):
        mbasic_url = "https://mbasic.facebook.com/" + group_url.lstrip("/")

    resp = safe_get(mbasic_url, headers=MOBILE_HEADERS)
    if not resp:
        return results

    if "login" in resp.url or "Đăng nhập" in resp.text[:2000]:
        logger.info(
            f"mbasic.facebook.com group requires login (redirected to {resp.url}). "
            "Falling back to alternative sources."
        )
        return results

    soup = BeautifulSoup(resp.text, "lxml")
    posts = _extract_mbasic_posts(soup)
    results.extend(posts)
    return results


def _extract_mbasic_posts(soup: BeautifulSoup) -> List[Dict]:
    """Extract posts from mbasic HTML (if accessible)."""
    posts = []
    # mbasic uses div with data-ft attribute for posts
    for article in soup.find_all("div", {"data-ft": True})[:20]:
        text = article.get_text(separator=" ", strip=True)
        if len(text) < 20:
            continue
        # Only keep posts mentioning gold/jewelry
        if not _contains_gold_keyword(text):
            continue
        # Try to find post URL
        link = article.find("a", href=re.compile(r"/story\.php|/permalink/"))
        post_url = link["href"] if link else ""
        if post_url and not post_url.startswith("http"):
            post_url = "https://mbasic.facebook.com" + post_url

        posts.append({
            "text": text[:1000],
            "author": "anonymous",
            "date": "",
            "source": "facebook_mbasic",
            "url": post_url,
        })
    return posts


def _contains_gold_keyword(text: str) -> bool:
    """Check if text contains gold/jewelry related keywords."""
    keywords = [
        "vàng", "tiệm vàng", "hiệu vàng", "cửa hàng vàng",
        "pnj", "doji", "sjc", "trang sức", "nhẫn vàng",
        "dây chuyền vàng", "lắc vàng", "mua vàng", "bán vàng",
        "giá vàng", "vàng 9999", "vàng 18k", "vàng 24k",
        "kim cương", "đá quý", "gold", "jewelry",
    ]
    text_lower = normalize_text(text)
    return any(kw in text_lower for kw in keywords)


# ─────────────────────────────────────────────────────────
# Part 2: Classification engine
# ─────────────────────────────────────────────────────────

def classify_comment_to_shop(
    text: str,
    shop_list: List[Dict],  # [{"id": 1, "name": "PNJ Đà Nẵng"}, ...]
) -> Optional[int]:
    """
    Match a comment/post to a shop by fuzzy name matching.
    Returns shop_id or None.

    Strategy:
    1. Exact substring match against SHOP_ALIASES (case-insensitive)
    2. Fuzzy match against shop names from DB
    3. Return None if no match
    """
    text_lower = normalize_text(text)

    # Strategy 1: Match via SHOP_ALIASES
    for shop_key, aliases in SHOP_ALIASES.items():
        for alias in aliases:
            if alias.lower() in text_lower:
                # Find the shop in shop_list by name similarity
                shop_id = _find_shop_id_by_key(shop_key, shop_list)
                if shop_id:
                    return shop_id

    # Strategy 2: Direct name match against DB shops
    for shop in shop_list:
        shop_name_lower = normalize_text(shop["name"])
        # Check if shop name (first 8 chars) appears in text
        short_name = shop_name_lower[:8]
        if len(short_name) >= 4 and short_name in text_lower:
            return shop["id"]

    return None


def _find_shop_id_by_key(shop_key: str, shop_list: List[Dict]) -> Optional[int]:
    """Find shop_id from the DB list that best matches a shop key."""
    shop_key_lower = normalize_text(shop_key)
    # Try exact match first
    for shop in shop_list:
        name_lower = normalize_text(shop["name"])
        if shop_key_lower in name_lower:
            return shop["id"]
    return None


def extract_sentiment_rating(text: str) -> float:
    """
    Infer rating 1-5 from Vietnamese text sentiment.
    Rule-based approach:
    - Strong positive → 5
    - Positive → 4
    - Neutral → 3
    - Negative → 2
    - Strong negative → 1
    """
    text_lower = normalize_text(text)

    pos_count = sum(1 for word in POSITIVE_WORDS if word in text_lower)
    neg_count = sum(1 for word in NEGATIVE_WORDS if word in text_lower)

    # Weight: negative words count double
    score = pos_count - (neg_count * 2)

    if score >= 3:
        return 5.0
    elif score >= 1:
        return 4.0
    elif score == 0 and pos_count == 0 and neg_count == 0:
        return 3.0  # Truly neutral
    elif score >= -1:
        return 3.0
    elif score >= -2:
        return 2.0
    else:
        return 1.0


# ─────────────────────────────────────────────────────────
# Part 3: Fallback scrapers
# ─────────────────────────────────────────────────────────

def scrape_google_search(query: str, num_results: int = 20) -> List[Dict]:
    """
    Use Google Search to find discussions about gold shops in Da Nang.
    Targets snippets + titles which often contain review content.
    """
    results = []
    encoded_query = quote_plus(query)
    # Try multiple Google endpoints
    urls_to_try = [
        f"https://www.google.com/search?q={encoded_query}&num={num_results}&hl=vi&gl=vn",
        f"https://www.google.com.vn/search?q={encoded_query}&num={num_results}&hl=vi",
    ]

    headers_to_try = [
        {
            **HEADERS,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        },
        MOBILE_HEADERS,
    ]

    resp = None
    for url in urls_to_try[:1]:
        for hdrs in headers_to_try[:1]:
            resp = safe_get(url, headers=hdrs, timeout=15)
            if resp:
                break
        if resp:
            break

    if not resp:
        logger.warning(f"Google search failed for '{query}'")
        return results

    soup = BeautifulSoup(resp.text, "lxml")

    # Multiple extraction strategies for Google's changing HTML
    # Strategy 1: div with jscontroller (modern Google)
    for result in soup.find_all("div", attrs={"data-hveid": True}):
        text = result.get_text(separator=" ", strip=True)
        if len(text) < 30 or not _contains_gold_keyword(text):
            continue
        link = result.find("a", href=re.compile(r"^https?://"))
        result_url = link["href"] if link else ""
        results.append({
            "text": text[:800],
            "author": "anonymous",
            "date": "",
            "source": "google_search",
            "url": result_url,
        })

    # Strategy 2: MBG result blocks (Google mobile)
    if not results:
        for div in soup.find_all("div", class_=re.compile(r"BNeawe|s3v9rd|DnJfK|kCrYT")):
            text = div.get_text(separator=" ", strip=True)
            if len(text) < 30 or not _contains_gold_keyword(text):
                continue
            results.append({
                "text": text[:800],
                "author": "anonymous",
                "date": "",
                "source": "google_search",
                "url": "",
            })

    # Strategy 3: fallback — all paragraphs with gold keywords
    if not results:
        for p in soup.find_all(["p", "span", "div"]):
            text = p.get_text(separator=" ", strip=True)
            if 50 <= len(text) <= 500 and _contains_gold_keyword(text):
                results.append({
                    "text": text[:800],
                    "author": "anonymous",
                    "date": "",
                    "source": "google_search",
                    "url": "",
                })
            if len(results) >= num_results:
                break

    logger.info(f"Google search '{query}': found {len(results)} results")
    return results[:num_results]


def scrape_google_news(query: str) -> List[Dict]:
    """
    Scrape Google News for recent articles about gold shops in Da Nang.
    """
    results = []
    encoded = quote_plus(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=vi&gl=VN&ceid=VN:vi"

    resp = safe_get(url, timeout=15)
    if not resp:
        return results

    soup = BeautifulSoup(resp.text, "xml")
    for item in soup.find_all("item")[:20]:
        title = item.find("title")
        description = item.find("description")
        pub_date = item.find("pubDate")
        link = item.find("link")

        title_text = title.get_text(strip=True) if title else ""
        desc_text = description.get_text(separator=" ", strip=True) if description else ""
        combined = f"{title_text} {desc_text}"

        if not _contains_gold_keyword(combined):
            continue

        results.append({
            "text": combined[:800],
            "author": "news_article",
            "date": pub_date.get_text(strip=True) if pub_date else "",
            "source": "google_news",
            "url": link.get_text(strip=True) if link else "",
        })

    logger.info(f"Google News '{query}': found {len(results)} items")
    return results


def scrape_webtretho(page_count: int = 3) -> List[Dict]:
    """
    Scrape webtretho.com for discussions about gold shops in Da Nang.
    Popular Vietnamese women's forum with gold/jewelry buying discussions.
    """
    results = []

    # Try current webtretho search URL format
    search_urls = [
        "https://www.webtretho.com/search?q=ti%E1%BB%87m+v%C3%A0ng+%C4%91%C3%A0+n%E1%BA%B5ng",
        "https://www.webtretho.com/search?keywords=ti%E1%BB%87m+v%C3%A0ng+%C4%91%C3%A0+n%E1%BA%B5ng",
    ]

    for url in search_urls:
        resp = safe_get(url, timeout=15)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        # Extract thread titles and snippets (flexible selectors)
        for item in soup.find_all(["div", "li", "article"]):
            link_tag = item.find("a", href=re.compile(r"/(forum|thread|f/t|p/|bai-viet)"))
            if not link_tag:
                continue

            title = link_tag.get_text(strip=True)
            if not _contains_gold_keyword(title) or len(title) < 15:
                continue

            body_tag = item.find(["p", "span", "div"], class_=re.compile(r"desc|preview|excerpt|body"))
            body = body_tag.get_text(separator=" ", strip=True) if body_tag else ""
            combined = f"{title} {body}".strip()

            post_url = link_tag.get("href", "")
            if post_url and not post_url.startswith("http"):
                post_url = "https://www.webtretho.com" + post_url

            results.append({
                "text": combined[:800],
                "author": "anonymous",
                "date": "",
                "source": "webtretho",
                "url": post_url,
            })

        polite_delay(2)
        if results:
            break

    logger.info(f"webtretho: found {len(results)} items")
    return results


def scrape_otofun(page_count: int = 3) -> List[Dict]:
    """
    Scrape otofun.net for discussions about gold/jewelry in Da Nang.
    Large Vietnamese forum with diverse topics including shopping.
    """
    results = []
    search_url = (
        "https://www.otofun.net/threads/search?q=ti%E1%BB%87m+v%C3%A0ng+%C4%91%C3%A0+n%E1%BA%B5ng"
    )

    resp = safe_get(search_url, timeout=15)
    if not resp:
        # Try alternative URL format
        search_url2 = "https://www.otofun.net/search?q=ti%E1%BB%87m+v%C3%A0ng+%C4%91%C3%A0+n%E1%BA%B5ng&t=post"
        resp = safe_get(search_url2, timeout=15)

    if not resp:
        logger.warning("otofun.net not accessible")
        return results

    soup = BeautifulSoup(resp.text, "lxml")

    for item in soup.find_all(["div", "li"], class_=re.compile(r"thread|post|result|searchResult")):
        title = item.find(["h3", "h4", "a"])
        body = item.find(["p", "div"], class_=re.compile(r"preview|excerpt|content"))

        title_text = title.get_text(strip=True) if title else ""
        body_text = body.get_text(separator=" ", strip=True) if body else ""
        combined = f"{title_text} {body_text}".strip()

        if not combined or not _contains_gold_keyword(combined):
            continue

        link = item.find("a", href=True)
        post_url = link["href"] if link else ""
        if post_url and not post_url.startswith("http"):
            post_url = "https://www.otofun.net" + post_url

        results.append({
            "text": combined[:800],
            "author": "anonymous",
            "date": "",
            "source": "otofun",
            "url": post_url,
        })

    logger.info(f"otofun: found {len(results)} items")
    return results


def scrape_reddit_vietnam(page_count: int = 2) -> List[Dict]:
    """
    Scrape r/Vietnam on Reddit for posts about gold shops in Da Nang.
    Uses old.reddit.com for simpler HTML parsing.
    """
    results = []
    queries = [
        "gold shop da nang",
        "tiệm vàng đà nẵng",
        "PNJ DOJI SJC danang",
    ]

    for query in queries[:2]:
        encoded = quote_plus(query)
        url = f"https://old.reddit.com/r/vietnam/search?q={encoded}&restrict_sr=on&sort=relevance"

        resp = safe_get(url, timeout=15)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        for post in soup.find_all("div", class_=re.compile(r"^thing|search-result")):
            title_tag = post.find("a", class_=re.compile(r"title|search-title"))
            if not title_tag:
                continue

            title = title_tag.get_text(strip=True)
            if not _contains_gold_keyword(title) and "gold" not in title.lower():
                continue

            post_url = title_tag.get("href", "")
            if post_url and not post_url.startswith("http"):
                post_url = "https://old.reddit.com" + post_url

            # Get post score/votes as proxy for importance
            score_tag = post.find("div", class_=re.compile(r"score"))
            score = score_tag.get_text(strip=True) if score_tag else "0"

            results.append({
                "text": title[:500],
                "author": "reddit_user",
                "date": "",
                "source": "reddit_vietnam",
                "url": post_url,
                "score": score,
            })

        polite_delay(2)

    # Also try Reddit JSON API
    try:
        api_url = "https://www.reddit.com/r/vietnam/search.json?q=gold+shop+danang&restrict_sr=1&sort=relevance&limit=25"
        resp = safe_get(
            api_url,
            headers={"User-Agent": "GoldFinderBot/1.0 (research project)"},
            timeout=15,
        )
        if resp:
            data = resp.json()
            for post in data.get("data", {}).get("children", []):
                p = post.get("data", {})
                title = p.get("title", "")
                body = p.get("selftext", "")
                combined = f"{title} {body}".strip()

                if _contains_gold_keyword(combined) or "gold" in combined.lower():
                    results.append({
                        "text": combined[:800],
                        "author": p.get("author", "anonymous"),
                        "date": "",
                        "source": "reddit_vietnam",
                        "url": "https://reddit.com" + p.get("permalink", ""),
                    })
    except Exception as e:
        logger.debug(f"Reddit JSON API: {e}")

    logger.info(f"reddit r/Vietnam: found {len(results)} items")
    return results


def scrape_foody_danang(page_count: int = 3) -> List[Dict]:
    """
    Scrape foody.vn for reviews of gold shops/jewelry stores in Da Nang.
    Foody.vn is a major Vietnamese review site.
    """
    results = []
    queries = [
        "tiệm vàng đà nẵng",
        "trang sức đà nẵng",
    ]

    for query in queries[:1]:
        encoded = quote_plus(query)
        url = f"https://www.foody.vn/da-nang/tim-kiem?q={encoded}"

        resp = safe_get(url, timeout=15)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        for item in soup.find_all("div", class_=re.compile(r"item-restaurant|res-item")):
            name_tag = item.find(["h3", "h4", "a"], class_=re.compile(r"name|title"))
            review_tag = item.find("div", class_=re.compile(r"review|des|description"))
            rating_tag = item.find(["span", "div"], class_=re.compile(r"star|rating|point"))

            if not name_tag:
                continue

            name = name_tag.get_text(strip=True)
            review = review_tag.get_text(separator=" ", strip=True) if review_tag else ""
            rating_text = rating_tag.get_text(strip=True) if rating_tag else ""

            combined = f"{name} {review}".strip()
            if not _contains_gold_keyword(combined):
                continue

            # Parse rating
            try:
                rating = float(re.search(r"\d+\.?\d*", rating_text).group()) if rating_text else 3.0
                if rating > 5:
                    rating = rating / 2  # Foody uses 10-point scale sometimes
            except:
                rating = 3.0

            link = name_tag.get("href") or (item.find("a") or {}).get("href", "")
            if link and not link.startswith("http"):
                link = "https://www.foody.vn" + link

            results.append({
                "text": combined[:800],
                "author": "foody_user",
                "date": "",
                "source": "foody_vn",
                "url": link,
                "rating": rating,
            })

        polite_delay(2.5)

    logger.info(f"foody.vn: found {len(results)} items")
    return results


def scrape_google_maps_reviews_via_web(shop_name: str, city: str = "Đà Nẵng") -> List[Dict]:
    """
    Use Google search to find Google Maps reviews for a specific shop.
    Search: "tiệm vàng [name] đà nẵng review"
    """
    results = []
    query = f'"{shop_name}" "{city}" review OR đánh giá OR nhận xét'
    encoded = quote_plus(query)
    url = f"https://www.google.com/search?q={encoded}&hl=vi&gl=vn"

    resp = safe_get(url, timeout=15)
    if not resp:
        return results

    soup = BeautifulSoup(resp.text, "lxml")

    # Extract snippets from search results
    for div in soup.find_all("div", class_=re.compile(r"BNeawe|VwiC3b|s3v9rd")):
        text = div.get_text(separator=" ", strip=True)
        if len(text) < 30:
            continue
        if _contains_gold_keyword(f"{shop_name} {text}") or "sao" in text or "điểm" in text:
            results.append({
                "text": f"[Về {shop_name}] {text[:500]}",
                "author": "anonymous",
                "date": "",
                "source": "google_review_snippet",
                "url": "",
            })

    return results[:5]  # Limit per shop


# ─────────────────────────────────────────────────────────
# Part 4: Main pipeline
# ─────────────────────────────────────────────────────────

def run_facebook_crawler(db) -> dict:
    """
    Main pipeline: scrape → classify → deduplicate → save.
    
    Returns:
        dict: {saved: N, classified: M, unclassified: K, sources: {...}}
    """
    from models import Review, Shop

    logger.info("Starting Facebook/UGC crawler...")

    # Load shops from DB for classification
    shops = db.query(Shop).all()
    shop_list = [{"id": s.id, "name": s.name} for s in shops]
    logger.info(f"Loaded {len(shop_list)} shops for classification")

    # Get existing fingerprints to avoid duplicates
    existing_fingerprints = set()
    existing_reviews = db.query(Review.text).all()
    for (text,) in existing_reviews:
        if text:
            existing_fingerprints.add(text_fingerprint(text))

    # Collect raw items from all sources
    raw_items = []
    source_stats = {}

    # 1. Try mbasic Facebook (likely will fail, but log status)
    logger.info("Testing mbasic.facebook.com access...")
    mbasic_status = test_mbasic_access()
    fb_accessible = any(v.get("accessible") for v in mbasic_status.values())

    if fb_accessible:
        logger.info("mbasic.facebook.com IS accessible! Scraping...")
        queries = [
            "tiệm vàng đà nẵng",
            "mua vàng đà nẵng",
            "PNJ DOJI SJC đà nẵng",
        ]
        for query in queries:
            items = scrape_mbasic_search(query, page_count=3)
            raw_items.extend(items)
            polite_delay(2.5)

        groups = [
            "groups/muabandanang",
            "groups/danangcity",
        ]
        for group in groups:
            items = scrape_mbasic_group(group, page_count=3)
            raw_items.extend(items)
            polite_delay(2.5)

        source_stats["facebook_mbasic"] = len([i for i in raw_items if i.get("source") == "facebook_mbasic"])
    else:
        logger.info(
            "mbasic.facebook.com requires login (all URLs redirect to login). "
            "Using fallback sources."
        )
        source_stats["facebook_mbasic"] = 0
        source_stats["facebook_mbasic_status"] = "login_required"

    # 2. Google Search fallback
    logger.info("Scraping Google Search...")
    google_queries = [
        'tiệm vàng đà nẵng review đánh giá',
        'PNJ DOJI SJC đà nẵng mua vàng',
        '"tiệm vàng" "đà nẵng" uy tín',
        'site:webtretho.com "tiệm vàng" "đà nẵng"',
    ]
    for q in google_queries[:3]:
        items = scrape_google_search(q, num_results=15)
        raw_items.extend(items)
        polite_delay(3)
    source_stats["google_search"] = len([i for i in raw_items if i.get("source") == "google_search"])

    # 3. Webtretho.com
    logger.info("Scraping webtretho.com...")
    wtt_items = scrape_webtretho(page_count=3)
    raw_items.extend(wtt_items)
    source_stats["webtretho"] = len(wtt_items)
    polite_delay(2.5)

    # 4. Otofun.net
    logger.info("Scraping otofun.net...")
    otofun_items = scrape_otofun(page_count=3)
    raw_items.extend(otofun_items)
    source_stats["otofun"] = len(otofun_items)
    polite_delay(2.5)

    # 5. Reddit r/Vietnam
    logger.info("Scraping Reddit r/Vietnam...")
    reddit_items = scrape_reddit_vietnam(page_count=2)
    raw_items.extend(reddit_items)
    source_stats["reddit_vietnam"] = len(reddit_items)
    polite_delay(2)

    # 6. Foody.vn
    logger.info("Scraping foody.vn...")
    foody_items = scrape_foody_danang(page_count=2)
    raw_items.extend(foody_items)
    source_stats["foody_vn"] = len(foody_items)

    # 7. Google News RSS (usually accessible, no blocking)
    logger.info("Scraping Google News RSS...")
    news_queries = [
        "tiệm vàng đà nẵng",
        "PNJ DOJI SJC đà nẵng",
    ]
    for q in news_queries:
        items = scrape_google_news(q)
        raw_items.extend(items)
        polite_delay(2)
    source_stats["google_news"] = len([i for i in raw_items if i.get("source") == "google_news"])

    logger.info(f"Total raw items collected: {len(raw_items)}")

    # Process and save
    saved = 0
    classified = 0
    unclassified_count = 0
    skipped_duplicates = 0

    for item in raw_items:
        text = item.get("text", "").strip()
        if not text or len(text) < 20:
            continue

        # Deduplication
        fp = text_fingerprint(text)
        if fp in existing_fingerprints:
            skipped_duplicates += 1
            continue
        existing_fingerprints.add(fp)

        # Classify
        shop_id = classify_comment_to_shop(text, shop_list)

        if shop_id is None:
            unclassified_count += 1
            # NOTE: Review table requires shop_id NOT NULL.
            # Unclassified reviews are tracked but not saved to DB.
            # They can be stored in a separate table in future.
            logger.debug(f"Unclassified: {text[:80]}...")
            continue

        classified += 1

        # Infer rating
        rating_from_item = item.get("rating")
        if rating_from_item is not None:
            rating = float(rating_from_item)
        else:
            rating = extract_sentiment_rating(text)

        # Build source string with URL if available
        source = item.get("source", "ugc")
        post_url = item.get("url", "")
        if post_url:
            source_with_url = f"{source}|{post_url[:200]}"
        else:
            source_with_url = source

        review = Review(
            shop_id=shop_id,
            text=text[:2000],
            rating=rating,
            author=item.get("author", "anonymous")[:100],
            date=item.get("date", ""),
            source=source_with_url[:100],
        )
        db.add(review)
        saved += 1

        # Commit in batches
        if saved % 20 == 0:
            try:
                db.commit()
                logger.info(f"Committed batch, total saved so far: {saved}")
            except Exception as e:
                logger.error(f"Batch commit error: {e}")
                db.rollback()

    # Final commit
    try:
        db.commit()
        logger.info(f"Final commit done.")
    except Exception as e:
        logger.error(f"Final commit error: {e}")
        db.rollback()

    # Update shop review counts
    _update_shop_review_counts(db, shops)

    result = {
        "saved": saved,
        "classified": classified,
        "unclassified": unclassified_count,
        "skipped_duplicates": skipped_duplicates,
        "total_raw": len(raw_items),
        "sources": source_stats,
        "facebook_accessible": fb_accessible,
        "note": (
            "Facebook requires login for all content (including mbasic). "
            "Using Google Search, webtretho, otofun, Reddit, foody.vn as sources."
            if not fb_accessible else
            "Facebook mbasic accessible!"
        ),
    }

    logger.info(f"Crawler complete: {result}")
    return result


def _update_shop_review_counts(db, shops):
    """Update review_count on Shop records."""
    from models import Review
    from sqlalchemy import func

    try:
        counts = db.query(
            Review.shop_id,
            func.count(Review.id).label("cnt"),
        ).group_by(Review.shop_id).all()

        count_map = {row.shop_id: row.cnt for row in counts}
        for shop in shops:
            new_count = count_map.get(shop.id, 0)
            if new_count != shop.review_count:
                shop.review_count = new_count
        db.commit()
        logger.info("Updated shop review counts")
    except Exception as e:
        logger.error(f"Failed to update review counts: {e}")
        db.rollback()


# ─────────────────────────────────────────────────────────
# Export helpers
# ─────────────────────────────────────────────────────────

def export_reviews_to_csv(db, csv_path: str = "data/reviews.csv"):
    """Append Facebook/UGC reviews to reviews.csv."""
    import csv
    import os
    from models import Review

    reviews = (
        db.query(Review)
        .filter(Review.source.like("%google_search%")
                | Review.source.like("%webtretho%")
                | Review.source.like("%otofun%")
                | Review.source.like("%reddit%")
                | Review.source.like("%foody%")
                | Review.source.like("%facebook%"))
        .all()
    )

    if not reviews:
        logger.info("No UGC reviews to export")
        return 0

    # Check if file exists to determine if we need header
    file_exists = os.path.exists(csv_path)
    mode = "a" if file_exists else "w"

    written = 0
    with open(csv_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id", "shop_id", "text", "rating", "author", "date", "source"],
        )
        if not file_exists or mode == "w":
            writer.writeheader()

        for r in reviews:
            writer.writerow({
                "id": r.id,
                "shop_id": r.shop_id,
                "text": r.text,
                "rating": r.rating,
                "author": r.author,
                "date": r.date,
                "source": r.source,
            })
            written += 1

    logger.info(f"Exported {written} UGC reviews to {csv_path}")
    return written


# ─────────────────────────────────────────────────────────
# CLI runner
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os

    # Add backend directory to path and set working directory
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, backend_dir)
    os.chdir(backend_dir)  # So SQLite uses backend/gold_finder.db

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    from database import SessionLocal

    print("=" * 60)
    print("Gold Finder Đà Nẵng — Facebook/UGC Crawler")
    print("=" * 60)

    # Test mbasic access first
    print("\n[1/2] Testing mbasic.facebook.com access...")
    status = test_mbasic_access()
    for url, info in status.items():
        icon = "✓" if info.get("accessible") else "✗"
        print(f"  {icon} {url[:60]} → {info.get('status')}")

    # Run crawler
    print("\n[2/2] Running crawler...")
    db = SessionLocal()
    try:
        result = run_facebook_crawler(db)

        print("\n" + "=" * 60)
        print("RESULTS:")
        print(f"  Total raw items:    {result['total_raw']}")
        print(f"  Classified:         {result['classified']}")
        print(f"  Unclassified:       {result['unclassified']} (not saved - schema constraint)")
        print(f"  Saved to DB:        {result['saved']}")
        print(f"  Skipped (dupes):    {result['skipped_duplicates']}")
        print(f"\nBy source:")
        for src, count in result.get("sources", {}).items():
            if isinstance(count, int):
                print(f"  {src:25s}: {count}")
        print(f"\nNote: {result.get('note', '')}")
        print("=" * 60)

        # Export to CSV (write full file)
        proj_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        csv_path = os.path.join(proj_root, "data", "reviews.csv")
        # Re-open db for clean read
        db2 = SessionLocal()
        try:
            exported = export_reviews_to_csv(db2, csv_path)
        finally:
            db2.close()
        print(f"\nExported {exported} UGC reviews to {csv_path}")

    finally:
        db.close()
