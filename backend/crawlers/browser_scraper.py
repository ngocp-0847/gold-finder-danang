"""
Browser-based scraper for Google Maps gold shop data.
No API key needed — uses Playwright to scrape directly.

Run: python crawlers/browser_scraper.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import logging
import re
from playwright.sync_api import sync_playwright
from database import SessionLocal
from models import Shop, Review

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_text(el):
    try:
        return el.inner_text().strip() if el else None
    except:
        return None


def get_attr(el, attr):
    try:
        return el.get_attribute(attr) if el else None
    except:
        return None


def scrape_shop(page, shop_name: str, shop_address: str) -> dict:
    """Search Google Maps and scrape shop details + reviews."""
    result = {'reviews': []}
    try:
        query = f"{shop_name} {shop_address} Đà Nẵng"
        search_url = "https://www.google.com/maps/search/" + query.replace(' ', '+').replace('/', '%2F')
        page.goto(search_url, timeout=25000)
        page.wait_for_timeout(2500)

        # If results list shown, click first one
        first = page.query_selector('a.hfpxzc')  # result card link
        if first:
            first.click()
            page.wait_for_timeout(3000)

        current_url = page.url
        if '/maps/place/' not in current_url and '/maps/search/' in current_url:
            # Try clicking first result differently
            first2 = page.query_selector('[role="feed"] a, .Nv2PK a')
            if first2:
                first2.click()
                page.wait_for_timeout(3000)

        result['google_maps_url'] = page.url

        # --- Rating ---
        for sel in ['span.ceNzKf[aria-hidden]', 'div.F7nice span[aria-hidden]', 'span[aria-hidden="true"]']:
            el = page.query_selector(sel)
            txt = get_text(el)
            if txt:
                try:
                    val = float(txt.replace(',', '.'))
                    if 1.0 <= val <= 5.0:
                        result['rating'] = val
                        break
                except:
                    pass

        # --- Review count ---
        for sel in ['button[jsaction*="pane.rating"]', 'span[aria-label*="đánh giá"]',
                    'span[aria-label*="review"]', 'div.F7nice span']:
            els = page.query_selector_all(sel)
            for el in els:
                txt = get_text(el) or get_attr(el, 'aria-label') or ''
                nums = re.findall(r'[\d,\.]+', txt)
                for n in nums:
                    try:
                        val = int(n.replace('.', '').replace(',', ''))
                        if val > 5:  # must be review count, not rating
                            result['review_count'] = val
                            break
                    except:
                        pass
                if result.get('review_count'):
                    break

        # --- Phone ---
        for sel in ['button[data-tooltip*="phone"]', '[data-item-id*="phone"]',
                    'button[aria-label*="điện thoại"]', 'button[aria-label*="phone"]']:
            el = page.query_selector(sel)
            if el:
                txt = get_attr(el, 'aria-label') or get_attr(el, 'data-tooltip') or get_text(el) or ''
                phone = re.sub(r'[^\d\s\+\-\(\)]', '', txt).strip()
                if phone and len(phone) >= 8:
                    result['phone'] = phone
                    break

        # --- Website ---
        for sel in ['a[data-tooltip*="website"]', 'a[data-item-id*="authority"]',
                    'a[aria-label*="website"]']:
            el = page.query_selector(sel)
            href = get_attr(el, 'href')
            if href and href.startswith('http'):
                result['website'] = href
                break

        # --- Scrape reviews ---
        # Click on Reviews tab
        for tab_sel in ['button[aria-label*="Đánh giá"]', 'button[aria-label*="Reviews"]',
                        'button[data-tab-index="1"]']:
            tab = page.query_selector(tab_sel)
            if tab:
                try:
                    tab.click()
                    page.wait_for_timeout(2500)
                    break
                except:
                    pass

        # Sort by newest
        try:
            sort_btn = page.query_selector('button[aria-label*="Sắp xếp"], button[aria-label*="Sort"]')
            if sort_btn:
                sort_btn.click()
                page.wait_for_timeout(1000)
                newest = page.query_selector('li[aria-label*="Mới nhất"], li[aria-label*="Newest"]')
                if newest:
                    newest.click()
                    page.wait_for_timeout(2000)
        except:
            pass

        # Expand "More" in reviews
        for btn in page.query_selector_all('button.w8nwRe, button[aria-label*="Xem thêm"]')[:8]:
            try:
                btn.click()
                page.wait_for_timeout(200)
            except:
                pass

        # Collect reviews
        review_cards = page.query_selector_all('div[data-review-id], div.jftiEf')[:5]
        for card in review_cards:
            try:
                author_el = card.query_selector('div.d4r55, .jJc9Ad, .DU9Pgb')
                text_el   = card.query_selector('span.wiI7pd, .MyEned span, .rsqaWe')
                stars_el  = card.query_selector('span.kvMYJc')
                date_el   = card.query_selector('span.rsqaWe, .dehysf')

                text = get_text(text_el)
                if not text:
                    continue

                rating = None
                aria = get_attr(stars_el, 'aria-label') or ''
                m = re.search(r'(\d+)', aria)
                if m:
                    rating = float(m.group(1))

                result['reviews'].append({
                    'author': get_text(author_el),
                    'text': text,
                    'rating': rating,
                    'date': get_text(date_el),
                    'source': 'google_maps',
                })
            except Exception as e:
                logger.debug(f"  Review parse err: {e}")

        logger.info(
            f"  ✅ rating={result.get('rating')} "
            f"total_reviews={result.get('review_count')} "
            f"scraped={len(result['reviews'])}"
        )

    except Exception as e:
        logger.error(f"  ❌ Failed scraping '{shop_name}': {e}")

    return result


def enrich_shops_browser():
    db = SessionLocal()
    shops = db.query(Shop).all()
    logger.info(f"Enriching {len(shops)} shops via browser scraping...\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale='vi-VN',
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 900},
        )
        page = context.new_page()

        # Accept Google consent if shown
        try:
            page.goto('https://www.google.com/maps', timeout=15000)
            page.wait_for_timeout(2000)
            for btn_text in ['Accept all', 'Chấp nhận tất cả', 'Đồng ý tất cả']:
                btn = page.query_selector(f'button:has-text("{btn_text}")')
                if btn:
                    btn.click()
                    page.wait_for_timeout(1000)
                    break
        except:
            pass

        updated = 0
        reviews_added = 0

        for shop in shops:
            logger.info(f"→ {shop.name}")
            data = scrape_shop(page, shop.name, shop.address or "")
            time.sleep(1.5)

            if data.get('rating'):
                shop.rating = data['rating']
            if data.get('review_count'):
                shop.review_count = data['review_count']
            if data.get('phone') and not shop.phone:
                shop.phone = data['phone']
            if data.get('hours') and not shop.hours:
                shop.hours = data['hours']
            if data.get('website') and not shop.website:
                shop.website = data['website']
            if data.get('google_maps_url') and '/maps/place/' in data.get('google_maps_url', ''):
                shop.google_maps_url = data['google_maps_url']

            # Save reviews
            existing = {r.text for r in db.query(Review).filter_by(shop_id=shop.id).all()}
            for r in data.get('reviews', []):
                if not r.get('text') or r['text'] in existing:
                    continue
                db.add(Review(
                    shop_id=shop.id,
                    text=r['text'],
                    rating=r.get('rating'),
                    author=r.get('author'),
                    date=r.get('date'),
                    source='google_maps',
                ))
                reviews_added += 1
                existing.add(r['text'])

            db.commit()
            updated += 1

        browser.close()

    db.close()
    logger.info(f"\n🎉 Done! {updated}/{len(shops)} shops updated, {reviews_added} reviews added.")


if __name__ == "__main__":
    enrich_shops_browser()
