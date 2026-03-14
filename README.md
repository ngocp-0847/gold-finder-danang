# 🏅 Gold Finder Đà Nẵng

Ứng dụng tìm kiếm và so sánh giá vàng tại các tiệm vàng ở Đà Nẵng.

## ✨ Tính năng

- 🗺️ **Bản đồ tiệm vàng** — 150+ tiệm vàng Đà Nẵng với vị trí, đánh giá, số điện thoại
- 💰 **Giá vàng realtime** — SJC, PNJ, DOJI, BTMC cập nhật mỗi 15 phút
- ⭐ **Reviews từ Google Maps** — 500+ đánh giá thực tế
- 🔍 **Lọc theo quận** — Hải Châu, Thanh Khê, Sơn Trà, Ngũ Hành Sơn, Liên Chiểu, Cẩm Lệ

## 🛠️ Tech Stack

- **Backend**: FastAPI + SQLite + SQLAlchemy
- **Frontend**: Vanilla JS + Leaflet.js
- **Crawler**: Playwright (browser automation, no API key needed)
- **Scheduler**: APScheduler (price updates every 15 min)

## 🚀 Chạy local

```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium

# Seed dữ liệu ban đầu
python seed_data.py

# Chạy server
uvicorn main:app --reload --port 8000
```

Mở `frontend/index.html` trên trình duyệt.

## 📁 Cấu trúc

```
gold-finder-danang/
├── backend/
│   ├── main.py              # FastAPI app + routes
│   ├── models.py            # SQLAlchemy models
│   ├── database.py          # DB connection
│   ├── scheduler.py         # APScheduler price updates
│   ├── seed_data.py         # Initial data seed
│   ├── requirements.txt
│   └── crawlers/
│       ├── gold_prices.py       # Basic price crawlers
│       ├── price_pipeline.py    # Realtime price pipeline
│       ├── google_maps.py       # Places API crawler
│       ├── browser_scraper.py   # Playwright shop enricher
│       └── discover_shops.py    # Playwright shop discovery
└── frontend/
    └── index.html           # Map UI
```

## 📊 Dữ liệu

- **150+ tiệm vàng** Đà Nẵng (crawl từ Google Maps)
- **527+ reviews** thực tế
- **17 loại giá vàng** realtime (SJC, PNJ, DOJI, BTMC)
- Nguồn giá: `api2.giavang.net`

## ⚙️ Environment

Tạo file `backend/.env`:
```
GOOGLE_MAPS_API_KEY=your_key_here  # optional, crawler dùng browser thay thế
```

## 📝 License

MIT
