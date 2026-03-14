# 🥇 Vàng Đà Nẵng

> Ứng dụng tìm kiếm và so sánh giá tiệm vàng tại Đà Nẵng

## Tính năng

- 🗺️ **Bản đồ** tất cả tiệm vàng tại Đà Nẵng với màu sắc theo rating
- 📍 **Tìm gần tôi** — dùng GPS browser, hiển thị tiệm gần nhất
- 💰 **So sánh giá** SJC, PNJ, DOJI, BTMC cập nhật mỗi giờ
- ⭐ **Đánh giá & reviews** từ Google Maps
- 🔍 **Lọc** theo quận, rating, loại cửa hàng
- 📊 **API đầy đủ** với FastAPI + SQLite

## Cài đặt

```bash
cd backend
pip install -r requirements.txt

# Seed database (15+ tiệm + giá vàng)
python seed_data.py

# Chạy server
uvicorn main:app --reload --port 8000
```

Mở trình duyệt: **http://localhost:8000**

API docs: **http://localhost:8000/docs**

## Cấu hình (.env)

Tạo file `backend/.env` (tùy chọn):

```env
# Google Maps API key (để crawl shops từ Google Maps)
GOOGLE_MAPS_API_KEY=your_key_here

# Facebook Access Token (để crawl giá từ Facebook pages)
FB_ACCESS_TOKEN=your_token_here

# Database path
DATABASE_URL=sqlite:///./gold_finder.db
```

## Nguồn dữ liệu giá

| Nguồn | URL | Cập nhật |
|-------|-----|----------|
| SJC | https://sjc.com.vn/GoldPrice/ | Mỗi giờ |
| PNJ | https://www.pnj.com.vn/blog/gia-vang/ | Mỗi giờ |
| DOJI | https://doji.vn/bang-gia-vang/ | Mỗi giờ |
| BTMC | https://www.btmc.vn/bang-gia-vang.html | Mỗi giờ |

## API Endpoints

```
GET  /api/shops                    # Danh sách tiệm vàng
GET  /api/shops/nearby?lat=&lng=   # Tiệm gần vị trí
GET  /api/shops/{id}               # Chi tiết tiệm
GET  /api/prices/latest            # Giá mới nhất
GET  /api/prices/compare?gold_type= # So sánh giá
POST /api/crawl/prices             # Trigger crawl giá
POST /api/crawl/shops              # Trigger crawl shops
GET  /api/stats                    # Thống kê
GET  /api/districts                # Danh sách quận
```

## Cấu trúc dự án

```
gold-finder-danang/
├── backend/
│   ├── main.py              FastAPI app + tất cả endpoints
│   ├── database.py          SQLite setup
│   ├── models.py            ORM models
│   ├── schemas.py           Pydantic schemas
│   ├── seed_data.py         15+ tiệm vàng Đà Nẵng + dữ liệu mẫu
│   ├── requirements.txt
│   └── crawlers/
│       ├── gold_prices.py   Crawl SJC, PNJ, DOJI, BTMC
│       ├── google_maps.py   Crawl Google Maps (cần API key)
│       └── facebook.py      Crawl Facebook (cần access token)
└── frontend/
    ├── index.html           SPA chính
    ├── style.css            Gold theme
    └── app.js               Vanilla JS + Leaflet map
```

## Roadmap

- [ ] Crawl tự động theo schedule
- [ ] Push notification khi giá biến động mạnh
- [ ] So sánh lịch sử giá theo biểu đồ
- [ ] Rating hệ thống do người dùng đánh giá
- [ ] Tích hợp Google Maps embed
- [ ] Mobile app (React Native hoặc PWA)
