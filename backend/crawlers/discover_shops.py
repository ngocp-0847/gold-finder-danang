"""
Discover ALL gold shops in Da Nang from Google Maps via browser scraping.
Searches multiple queries + districts, scrolls results, deduplicates.
Target: 100+ shops

Run: python crawlers/discover_shops.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import logging
import re
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from database import SessionLocal
from models import Shop, Review

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Search queries to maximize coverage
SEARCH_QUERIES = [
    "tiệm vàng Đà Nẵng",
    "cửa hàng vàng Đà Nẵng",
    "vàng bạc đá quý Đà Nẵng",
    "tiệm vàng Hải Châu Đà Nẵng",
    "tiệm vàng Thanh Khê Đà Nẵng",
    "tiệm vàng Sơn Trà Đà Nẵng",
    "tiệm vàng Ngũ Hành Sơn Đà Nẵng",
    "tiệm vàng Liên Chiểu Đà Nẵng",
    "tiệm vàng Cẩm Lệ Đà Nẵng",
    "tiệm vàng Hòa Vang Đà Nẵng",
    "PNJ Đà Nẵng",
    "DOJI Đà Nẵng",
    "SJC Đà Nẵng",
    "vàng miếng Đà Nẵng",
    "nhẫn vàng Đà Nẵng",
    "trang sức vàng Đà Nẵng",
    "kim hoàn Đà Nẵng",
    "tiệm vàng đường Hùng Vương Đà Nẵng",
    "tiệm vàng đường Ông Ích Khiêm Đà Nẵng",
    "tiệm vàng đường Lê Duẩn Đà Nẵng",
    "tiệm vàng đường Nguyễn Văn Linh Đà Nẵng",
    "tiệm vàng chợ Hàn Đà Nẵng",
    "tiệm vàng chợ Đống Đa Đà Nẵng",
    "tiệm vàng chợ Cồn Đà Nẵng",
]


def scroll_results(page, max_scrolls=15):
    """Scroll the results panel to load more shops."""
    panel = page.query_selector('div[role="feed"]')
    if not panel:
        return
    for i in range(max_scrolls):
        panel.evaluate('el => el.scrollTop += 800')
        page.wait_for_timeout(800)
        # Check if "You've reached the end" message
        end_msg = page.query_selector('span.HlvSq')
        if end_msg:
            logger.debug(f"  Reached end after {i+1} scrolls")
            break


def collect_shop_cards(page) -> list[dict]:
    """Collect all shop cards from search results."""
    shops = []
    seen_names = set()

    cards = page.query_selector_all('a.hfpxzc')
    for card in cards:
        try:
            aria = card.get_attribute('aria-label') or ''
            if not aria:
                continue
            name = aria.strip()
            if name in seen_names:
                continue
            seen_names.add(name)

            href = card.get_attribute('href') or ''
            shops.append({'name': name, 'url': href})
        except:
            pass

    # Also try alternative card selectors
    if not shops:
        for card in page.query_selector_all('.Nv2PK'):
            try:
                name_el = card.query_selector('.qBF1Pd, .fontHeadlineSmall')
                name = name_el.inner_text().strip() if name_el else None
                if not name or name in seen_names:
                    continue
                seen_names.add(name)
                link = card.query_selector('a')
                href = link.get_attribute('href') if link else ''
                shops.append({'name': name, 'url': href})
            except:
                pass

    return shops


def scrape_shop_detail(page, shop_url: str) -> dict:
    """Scrape full details of a shop from its Google Maps page."""
    result = {'reviews': []}
    try:
        page.goto(shop_url, timeout=20000)
        page.wait_for_timeout(2500)

        result['google_maps_url'] = page.url

        # Name
        name_el = page.query_selector('h1.DUwDvf, h1.fontHeadlineLarge')
        if name_el:
            result['name'] = name_el.inner_text().strip()

        # Address
        for sel in ['button[data-tooltip*="address"]', '[data-item-id*="address"]',
                    'button[aria-label*="địa chỉ"]', 'button[aria-label*="Address"]']:
            el = page.query_selector(sel)
            if el:
                addr = el.get_attribute('aria-label') or el.inner_text()
                addr = re.sub(r'^[Ađịa\s:]+', '', addr, flags=re.IGNORECASE).strip()
                if addr:
                    result['address'] = addr
                    break

        # Rating
        for sel in ['div.F7nice span[aria-hidden="true"]', 'span.ceNzKf[aria-hidden]']:
            el = page.query_selector(sel)
            if el:
                try:
                    val = float(el.inner_text().replace(',', '.'))
                    if 1.0 <= val <= 5.0:
                        result['rating'] = val
                        break
                except:
                    pass

        # Review count
        for sel in ['button[aria-label*="đánh giá"]', 'button[aria-label*="review"]',
                    'span[aria-label*="đánh giá"]']:
            el = page.query_selector(sel)
            if el:
                txt = el.get_attribute('aria-label') or el.inner_text()
                nums = re.findall(r'[\d\.]+', txt)
                for n in nums:
                    try:
                        v = int(n.replace('.', ''))
                        if v > 5:
                            result['review_count'] = v
                            break
                    except:
                        pass
            if result.get('review_count'):
                break

        # Phone
        for sel in ['button[data-tooltip*="phone"]', '[data-item-id*="phone"]',
                    'button[aria-label*="Điện thoại"]', 'button[aria-label*="Phone"]']:
            el = page.query_selector(sel)
            if el:
                txt = el.get_attribute('aria-label') or el.inner_text()
                phone = re.sub(r'[^\d\s\+\-]', '', txt).strip()
                if len(phone) >= 8:
                    result['phone'] = phone
                    break

        # Website
        for sel in ['a[data-item-id*="authority"]', 'a[aria-label*="website"]',
                    'a[data-tooltip*="website"]']:
            el = page.query_selector(sel)
            if el:
                href = el.get_attribute('href')
                if href and href.startswith('http'):
                    result['website'] = href
                    break

        # Category / type
        cat_el = page.query_selector('button.DkEaL')
        if cat_el:
            result['category'] = cat_el.inner_text().strip()

        # District detection from page URL or address
        addr_str = result.get('address', '') + page.url
        result['district'] = _detect_district(addr_str)

        # --- Reviews ---
        for tab_sel in ['button[aria-label*="Đánh giá"]', 'button[aria-label*="Reviews"]']:
            tab = page.query_selector(tab_sel)
            if tab:
                try:
                    tab.click()
                    page.wait_for_timeout(2000)
                    break
                except:
                    pass

        # Expand "More" buttons
        for btn in page.query_selector_all('button.w8nwRe, button[aria-label*="Xem thêm"]')[:8]:
            try:
                btn.click()
                page.wait_for_timeout(150)
            except:
                pass

        # Collect reviews
        for card in page.query_selector_all('div[data-review-id], div.jftiEf')[:5]:
            try:
                author_el = card.query_selector('div.d4r55, .jJc9Ad, .DU9Pgb')
                text_el   = card.query_selector('span.wiI7pd, .MyEned span')
                stars_el  = card.query_selector('span.kvMYJc')
                date_el   = card.query_selector('span.rsqaWe, .dehysf')

                text = text_el.inner_text().strip() if text_el else None
                if not text:
                    continue

                rating = None
                aria = (stars_el.get_attribute('aria-label') or '') if stars_el else ''
                m = re.search(r'(\d+)', aria)
                if m:
                    rating = float(m.group(1))

                result['reviews'].append({
                    'author': author_el.inner_text().strip() if author_el else None,
                    'text': text,
                    'rating': rating,
                    'date': date_el.inner_text().strip() if date_el else None,
                    'source': 'google_maps',
                })
            except:
                pass

    except PWTimeout:
        logger.warning(f"  ⏱ Timeout: {shop_url[:60]}")
    except Exception as e:
        logger.error(f"  ❌ Error: {e}")

    return result


def _detect_district(text: str) -> str:
    mapping = {
        "Hải Châu": ["Hải Châu", "Hai Chau", "Hùng Vương", "Ông Ích Khiêm", "Lê Duẩn", "Trần Phú"],
        "Thanh Khê": ["Thanh Khê", "Thanh Khe", "Điện Biên Phủ"],
        "Sơn Trà": ["Sơn Trà", "Son Tra", "Ngô Quyền", "Phạm Văn Đồng"],
        "Ngũ Hành Sơn": ["Ngũ Hành Sơn", "Ngu Hanh Son", "Non Nước", "Trường Sa"],
        "Liên Chiểu": ["Liên Chiểu", "Lien Chieu", "Nguyễn Lương Bằng"],
        "Cẩm Lệ": ["Cẩm Lệ", "Cam Le", "Cẩm Toại"],
        "Hòa Vang": ["Hòa Vang", "Hoa Vang"],
    }
    for district, keywords in mapping.items():
        for kw in keywords:
            if kw.lower() in text.lower():
                return district
    return "Đà Nẵng"


def is_gold_shop(name: str, category: str = '') -> bool:
    """Filter only actual gold/jewelry shops."""
    text = (name + ' ' + category).lower()
    gold_keywords = ['vàng', 'kim hoàn', 'trang sức', 'jewelry', 'sjc', 'pnj', 'doji', 'btmc',
                     'bảo tín', 'kim cương', 'bạc', 'đá quý', 'nữ trang']
    exclude_keywords = ['ngân hàng', 'bank', 'bệnh viện', 'trường', 'khách sạn', 'nhà hàng',
                        'quán', 'cafe', 'siêu thị', 'điện thoại']
    for kw in exclude_keywords:
        if kw in text:
            return False
    for kw in gold_keywords:
        if kw in text:
            return True
    return False


def discover_and_save():
    db = SessionLocal()

    # Get existing shop names to avoid dupes
    existing = {s.name.lower().strip() for s in db.query(Shop.name).all()}
    logger.info(f"Existing shops: {len(existing)}")

    all_discovered = {}  # name -> {url, ...}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale='vi-VN',
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 900},
        )
        page = context.new_page()

        # Accept consent
        try:
            page.goto('https://www.google.com/maps', timeout=15000)
            page.wait_for_timeout(2000)
            for btn_text in ['Accept all', 'Chấp nhận tất cả']:
                btn = page.query_selector(f'button:has-text("{btn_text}")')
                if btn:
                    btn.click()
                    page.wait_for_timeout(1000)
                    break
        except:
            pass

        # Phase 1: Discover shop names & URLs
        logger.info(f"\n=== PHASE 1: Discovering shops ({len(SEARCH_QUERIES)} queries) ===")
        for query in SEARCH_QUERIES:
            try:
                search_url = "https://www.google.com/maps/search/" + query.replace(' ', '+')
                page.goto(search_url, timeout=20000)
                page.wait_for_timeout(2500)

                scroll_results(page, max_scrolls=20)

                cards = collect_shop_cards(page)
                new_count = 0
                for card in cards:
                    name = card['name']
                    if name.lower() not in all_discovered and is_gold_shop(name):
                        all_discovered[name.lower()] = card
                        new_count += 1

                logger.info(f"  '{query}': +{new_count} new (total: {len(all_discovered)})")

            except Exception as e:
                logger.warning(f"  Query failed '{query}': {e}")
            time.sleep(1)

        logger.info(f"\nTotal discovered: {len(all_discovered)} shops")

        # Phase 2: Scrape details for new shops
        logger.info(f"\n=== PHASE 2: Scraping details ===")
        shops_to_scrape = [v for k, v in all_discovered.items() if k not in existing]
        logger.info(f"New shops to scrape: {len(shops_to_scrape)}")

        saved = 0
        reviews_added = 0

        for i, shop_info in enumerate(shops_to_scrape):
            logger.info(f"[{i+1}/{len(shops_to_scrape)}] {shop_info['name']}")

            if not shop_info.get('url'):
                continue

            data = scrape_shop_detail(page, shop_info['url'])
            time.sleep(1.2)

            name = data.get('name') or shop_info['name']

            # Skip if already in DB (check again after getting real name)
            if name.lower().strip() in existing:
                logger.info(f"  → Already exists, skipping")
                continue

            shop = Shop(
                name=name,
                address=data.get('address'),
                district=data.get('district', 'Đà Nẵng'),
                lat=None,  # TODO: geocode later
                lng=None,
                phone=data.get('phone'),
                hours=data.get('hours'),
                rating=data.get('rating', 0.0),
                review_count=data.get('review_count', 0),
                website=data.get('website'),
                google_maps_url=data.get('google_maps_url'),
                source='google_maps',
                is_verified=False,
            )
            db.add(shop)
            db.flush()  # get shop.id

            for r in data.get('reviews', []):
                if r.get('text'):
                    db.add(Review(
                        shop_id=shop.id,
                        text=r['text'],
                        rating=r.get('rating'),
                        author=r.get('author'),
                        date=r.get('date'),
                        source='google_maps',
                    ))
                    reviews_added += 1

            db.commit()
            existing.add(name.lower().strip())
            saved += 1
            logger.info(f"  ✅ Saved. rating={data.get('rating')} reviews={len(data.get('reviews', []))}")

        browser.close()

    db.close()
    logger.info(f"\n🎉 Done! Saved {saved} new shops, {reviews_added} reviews.")
    logger.info(f"Total in DB: {len(existing)} shops")


if __name__ == "__main__":
    discover_and_save()
