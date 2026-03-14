"""
Seed database with known gold shops in Da Nang.
Run: python seed_data.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from database import SessionLocal, engine
from models import Base, Shop, Review, GoldPrice, PriceSource
from crawlers.gold_prices import crawl_all_prices
from datetime import datetime

Base.metadata.create_all(bind=engine)

SHOPS = [
    {
        "name": "Tiệm Vàng Kim Khánh Việt Hùng",
        "address": "215 Hùng Vương, Phước Ninh, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0544, "lng": 108.2022,
        "phone": "0236 3822 xxx",
        "hours": "07:30 - 20:00",
        "rating": 4.5, "review_count": 120,
        "description": "Tiệm vàng uy tín lâu năm tại Đà Nẵng, chuyên vàng miếng SJC và trang sức.",
        "source": "manual", "is_verified": True,
    },
    {
        "name": "PNJ Đà Nẵng - Hùng Vương",
        "address": "78 Hùng Vương, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0678, "lng": 108.2109,
        "phone": "1800 1626",
        "hours": "08:00 - 21:00",
        "rating": 4.7, "review_count": 890,
        "website": "https://www.pnj.com.vn",
        "facebook_url": "https://www.facebook.com/PNJofficial",
        "description": "Chuỗi trang sức lớn nhất Việt Nam, bảo hành trọn đời sản phẩm.",
        "source": "official", "is_verified": True, "is_chain": True,
    },
    {
        "name": "PNJ Đà Nẵng - Ông Ích Khiêm",
        "address": "265 Ông Ích Khiêm, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0521, "lng": 108.2187,
        "phone": "1800 1626",
        "hours": "08:00 - 21:00",
        "rating": 4.6, "review_count": 430,
        "website": "https://www.pnj.com.vn",
        "source": "official", "is_verified": True, "is_chain": True,
    },
    {
        "name": "DOJI Đà Nẵng",
        "address": "154 Phan Chu Trinh, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0601, "lng": 108.2144,
        "phone": "0236 3580 168",
        "hours": "08:00 - 20:30",
        "rating": 4.6, "review_count": 312,
        "website": "https://doji.vn",
        "facebook_url": "https://www.facebook.com/doji.vn",
        "description": "Tập đoàn Vàng bạc đá quý DOJI, phân phối chính thức SJC.",
        "source": "official", "is_verified": True, "is_chain": True,
    },
    {
        "name": "SJC Đà Nẵng",
        "address": "268 Trần Phú, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0632, "lng": 108.2201,
        "phone": "0236 3822 566",
        "hours": "07:30 - 11:30, 13:30 - 17:00",
        "rating": 4.4, "review_count": 178,
        "website": "https://sjc.com.vn",
        "description": "Chi nhánh SJC chính thức tại Đà Nẵng, giao dịch vàng miếng SJC.",
        "source": "official", "is_verified": True, "is_chain": True,
    },
    {
        "name": "Tiệm Vàng Hoa Kim Thành",
        "address": "123 Nguyễn Văn Linh, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0589, "lng": 108.2098,
        "phone": "0236 3821 xxx",
        "hours": "07:00 - 20:00",
        "rating": 4.3, "review_count": 87,
        "description": "Tiệm vàng gia truyền, chuyên nhẫn cưới và trang sức vàng tây.",
        "source": "manual", "is_verified": True,
    },
    {
        "name": "Tiệm Vàng Phúc Lợi",
        "address": "56 Lê Lợi, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0612, "lng": 108.2165,
        "phone": "0905 xxx xxx",
        "hours": "08:00 - 19:30",
        "rating": 4.2, "review_count": 56,
        "description": "Chuyên vàng miếng và trang sức cao cấp.",
        "source": "manual",
    },
    {
        "name": "Tứ Quý Jewelry",
        "address": "89 Trần Hưng Đạo, Sơn Trà, Đà Nẵng",
        "district": "Sơn Trà",
        "lat": 16.0778, "lng": 108.2289,
        "phone": "0236 3824 xxx",
        "hours": "08:00 - 20:00",
        "rating": 4.4, "review_count": 145,
        "description": "Tiệm vàng nổi tiếng khu vực Sơn Trà, uy tín nhiều năm.",
        "source": "manual", "is_verified": True,
    },
    {
        "name": "Tiệm Vàng Minh Hòa",
        "address": "34 Điện Biên Phủ, Thanh Khê, Đà Nẵng",
        "district": "Thanh Khê",
        "lat": 16.0811, "lng": 108.1978,
        "phone": "0236 3823 xxx",
        "hours": "07:30 - 20:00",
        "rating": 4.1, "review_count": 43,
        "source": "manual",
    },
    {
        "name": "Bảo Tín Đà Nẵng",
        "address": "12 Nguyễn Tất Thành, Thanh Khê, Đà Nẵng",
        "district": "Thanh Khê",
        "lat": 16.0834, "lng": 108.1912,
        "phone": "0236 3820 xxx",
        "hours": "08:00 - 19:00",
        "rating": 4.0, "review_count": 28,
        "description": "Chuyên mua bán vàng miếng, nữ trang các loại.",
        "source": "manual",
    },
    {
        "name": "Tiệm Vàng Thanh Bình",
        "address": "67 Lê Duẩn, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0645, "lng": 108.2134,
        "phone": "0236 3811 xxx",
        "hours": "07:30 - 20:30",
        "rating": 4.3, "review_count": 98,
        "source": "manual", "is_verified": True,
    },
    {
        "name": "PNJ Ngũ Hành Sơn",
        "address": "234 Trường Sa, Ngũ Hành Sơn, Đà Nẵng",
        "district": "Ngũ Hành Sơn",
        "lat": 16.0023, "lng": 108.2601,
        "phone": "1800 1626",
        "hours": "08:00 - 21:00",
        "rating": 4.5, "review_count": 220,
        "website": "https://www.pnj.com.vn",
        "source": "official", "is_verified": True, "is_chain": True,
    },
    {
        "name": "Tiệm Vàng Đức Thịnh",
        "address": "15 Hoàng Diệu, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0558, "lng": 108.2078,
        "phone": "0905 xxx xxx",
        "hours": "08:00 - 19:00",
        "rating": 3.9, "review_count": 22,
        "source": "manual",
    },
    {
        "name": "Tiệm Vàng Kim Long",
        "address": "45 Lý Thường Kiệt, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0567, "lng": 108.2055,
        "phone": "0236 3831 xxx",
        "hours": "07:30 - 20:00",
        "rating": 4.2, "review_count": 67,
        "source": "manual",
    },
    {
        "name": "Tiệm Vàng Ánh Kim",
        "address": "90 Núi Thành, Hải Châu, Đà Nẵng",
        "district": "Hải Châu",
        "lat": 16.0432, "lng": 108.2212,
        "phone": "0905 xxx xxx",
        "hours": "08:00 - 18:30",
        "rating": 4.0, "review_count": 31,
        "source": "manual",
    },
]

SAMPLE_REVIEWS = {
    "PNJ Đà Nẵng - Hùng Vương": [
        {"text": "Dịch vụ rất tốt, nhân viên nhiệt tình. Giá cả rõ ràng, mua nhẫn cưới ở đây rất hài lòng!", "rating": 5, "author": "Nguyễn Thị Lan", "date": "3 tuần trước"},
        {"text": "Cửa hàng rộng rãi, có nhiều mẫu đẹp. Nhân viên tư vấn nhiệt tình dù tôi chỉ xem không mua.", "rating": 5, "author": "Trần Văn Nam", "date": "1 tháng trước"},
        {"text": "Chất lượng vàng đảm bảo, có phiếu bảo hành rõ ràng.", "rating": 4, "author": "Lê Thị Hoa", "date": "2 tháng trước"},
    ],
    "DOJI Đà Nẵng": [
        {"text": "Mua vàng miếng SJC chính hãng, có hóa đơn, giá tốt so với thị trường.", "rating": 5, "author": "Phạm Văn Đức", "date": "2 tuần trước"},
        {"text": "Nhân viên chuyên nghiệp, tư vấn kỹ càng. Sẽ quay lại!", "rating": 5, "author": "Võ Thị Mai", "date": "1 tháng trước"},
    ],
    "Tiệm Vàng Kim Khánh Việt Hùng": [
        {"text": "Tiệm uy tín lâu năm, giá tốt, mua bán nhanh chóng.", "rating": 5, "author": "Ngô Văn Bình", "date": "1 tuần trước"},
        {"text": "Quen mua ở đây nhiều năm rồi, chưa bao giờ thất vọng.", "rating": 5, "author": "Huỳnh Thị Thu", "date": "3 tuần trước"},
    ],
}

PRICE_SOURCES = [
    {"name": "SJC", "url": "https://sjc.com.vn/GoldPrice/", "is_active": True, "crawl_interval_minutes": 60},
    {"name": "PNJ", "url": "https://www.pnj.com.vn/blog/gia-vang/", "is_active": True, "crawl_interval_minutes": 60},
    {"name": "DOJI", "url": "https://doji.vn/bang-gia-vang/", "is_active": True, "crawl_interval_minutes": 60},
    {"name": "BTMC", "url": "https://www.btmc.vn/bang-gia-vang.html", "is_active": True, "crawl_interval_minutes": 60},
]


def seed():
    db = SessionLocal()
    try:
        # Check if already seeded
        if db.query(Shop).count() > 0:
            print("Database already seeded. Skipping.")
            return

        print("Seeding shops...")
        shop_map = {}
        for shop_data in SHOPS:
            shop = Shop(**shop_data)
            db.add(shop)
            db.flush()
            shop_map[shop.name] = shop.id

        print("Seeding reviews...")
        for shop_name, reviews in SAMPLE_REVIEWS.items():
            shop_id = shop_map.get(shop_name)
            if shop_id:
                for r in reviews:
                    review = Review(shop_id=shop_id, source="google", **r)
                    db.add(review)

        print("Seeding price sources...")
        for ps_data in PRICE_SOURCES:
            ps = PriceSource(**ps_data)
            db.add(ps)

        print("Crawling initial gold prices...")
        try:
            prices = crawl_all_prices()
            for p in prices:
                price = GoldPrice(**p)
                db.add(price)
            print(f"Added {len(prices)} price records")
        except Exception as e:
            print(f"Price crawl failed (non-fatal): {e}")
            # Add fallback prices manually
            fallback = [
                GoldPrice(source_name="SJC", gold_type="SJC 1L, 2L, 5L, 10L, 1KG", buy_price=85500000, sell_price=87500000, unit="lượng"),
                GoldPrice(source_name="SJC", gold_type="SJC Nhẫn tròn trơn 99,99", buy_price=84300000, sell_price=85800000, unit="lượng"),
                GoldPrice(source_name="PNJ", gold_type="Vàng SJC", buy_price=85500000, sell_price=87500000, unit="lượng"),
                GoldPrice(source_name="PNJ", gold_type="Vàng PNJ 999.9", buy_price=84200000, sell_price=85700000, unit="lượng"),
                GoldPrice(source_name="PNJ", gold_type="Vàng tây 18K", buy_price=45000000, sell_price=47000000, unit="lượng"),
                GoldPrice(source_name="DOJI", gold_type="Vàng miếng SJC", buy_price=85500000, sell_price=87500000, unit="lượng"),
                GoldPrice(source_name="DOJI", gold_type="Vàng nhẫn DOJI 999.9", buy_price=84100000, sell_price=85700000, unit="lượng"),
                GoldPrice(source_name="BTMC", gold_type="Vàng SJC 99.99", buy_price=85500000, sell_price=87400000, unit="lượng"),
            ]
            for fp in fallback:
                db.add(fp)

        db.commit()
        print(f"✅ Seeded {len(SHOPS)} shops, {sum(len(v) for v in SAMPLE_REVIEWS.values())} reviews, {len(PRICE_SOURCES)} price sources")
    except Exception as e:
        db.rollback()
        print(f"❌ Seed failed: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed()
