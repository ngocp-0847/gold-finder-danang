#!/usr/bin/env python3
"""Gold price history: fetch, store, chart, story."""
import sys, sqlite3, warnings
warnings.filterwarnings("ignore")

import yfinance as yf
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.lines import Line2D
import feedparser, requests

DB = "/Users/ngocp/Projects/gold-finder-danang/backend/gold_history.db"
CHART = "/Users/ngocp/Projects/gold-finder-danang/backend/gold_timeline.png"

# --- 1. Fetch gold price history ---
print("Fetching gold price data...")
gold = yf.download("GC=F", start="2000-01-01", end="2026-03-14", interval="1d", auto_adjust=True, progress=False)
if gold.empty:
    gold = yf.download("GLD", start="2000-01-01", end="2026-03-14", interval="1d", auto_adjust=True, progress=False)
    gold["Close"] = gold["Close"] * 10  # GLD = 1/10 oz

df = gold[["Close"]].dropna()
df.columns = ["price_usd"]
df.index = pd.to_datetime(df.index)
df.index.name = "date"

# Save to SQLite
con = sqlite3.connect(DB)
df.reset_index().to_sql("gold_prices", con, if_exists="replace", index=False)
con.close()
print(f"Saved {len(df)} rows | Range: ${df.price_usd.min():.0f} – ${df.price_usd.max():.0f}")
print(f"2000→2026 gain: {((df.price_usd.iloc[-1]/df.price_usd.iloc[0])-1)*100:.0f}%")

# --- 2. Key events ---
EVENTS = [
    ("2001-02-20",  256, "Đáy 20 năm\n$256/oz", "bottom"),
    ("2005-11-01",  500, "Phá ngưỡng $500\nlần đầu kể từ 1987", "milestone"),
    ("2008-03-17",  1000, "Phá $1,000 lần đầu\nKhủng hoảng Bear Stearns", "milestone"),
    ("2008-09-17",  900, "Lehman Brothers sụp đổ\nVàng bị bán tháo", "crisis"),
    ("2009-03-01",  940, "Fed QE1: in $1.75T", "policy"),
    ("2011-09-06", 1921, "Đỉnh 2011: $1,921\nKhủng hoảng nợ Eurozone", "peak"),
    ("2013-04-15", 1355, "Sụp 9% / 2 ngày\nCyprus bán dự trữ vàng", "crash"),
    ("2015-12-16", 1050, "Fed tăng lãi suất\nlần đầu sau 9 năm", "policy"),
    ("2019-08-27", 1537, "Fed cắt lãi 3 lần\nChiến tranh thương mại", "policy"),
    ("2020-03-18", 1477, "COVID-19\nVàng bị bán để bù margin", "crisis"),
    ("2020-08-07", 2075, "Đỉnh 2020: $2,075\nFed in $3T, lãi suất 0%", "peak"),
    ("2022-03-08", 2043, "Nga xâm lược Ukraine\nVàng gần đỉnh 2020", "crisis"),
    ("2022-11-01", 1630, "Fed tăng lãi 4.25%\nVàng điều chỉnh 20%", "policy"),
    ("2023-12-04", 2135, "Đỉnh mới: $2,135\nKỳ vọng Fed pivot", "peak"),
    ("2024-04-12", 2390, "Đỉnh mới: $2,390\nIran tấn công Israel", "peak"),
    ("2024-10-30", 2790, "Đỉnh mới: $2,790\nBầu cử Mỹ + nợ công $35T", "peak"),
    ("2025-03-20", 3050, "Phá $3,000\nThuế quan Trump, USD suy yếu", "peak"),
]

# --- 3. Chart ---
print("Building chart...")
COLOR_MAP = {
    "bottom": "#22c55e", "policy": "#60a5fa", "crisis": "#f87171",
    "milestone": "#fbbf24", "peak": "#c084fc", "crash": "#fb923c",
}

fig, ax = plt.subplots(figsize=(24, 12))
fig.patch.set_facecolor("#0f172a")
ax.set_facecolor("#0f172a")

ax.plot(df.index, df["price_usd"], color="#fbbf24", linewidth=2, zorder=3)
ax.fill_between(df.index, df["price_usd"], alpha=0.12, color="#fbbf24")

# Shade historical periods
periods = [
    ("2001-01-01","2011-09-06","#22c55e","Bull Run 2001–2011",0.04),
    ("2011-09-06","2018-08-01","#ef4444","Bear Market 2011–2018",0.04),
    ("2018-08-01","2026-03-14","#a855f7","New Bull 2018–Now",0.04),
]
ymin, ymax = 0, df.price_usd.max() * 1.18
for s,e,c,lbl,a in periods:
    ax.axvspan(pd.to_datetime(s), pd.to_datetime(e), alpha=a, color=c, zorder=1)
    mid = pd.to_datetime(s) + (pd.to_datetime(e)-pd.to_datetime(s))/2
    ax.text(mid, ymax*0.97, lbl, color=c, fontsize=8, ha="center", alpha=0.8, fontweight="bold")

# Event annotations — alternate above/below
for i, (date_str, price, label, cat) in enumerate(EVENTS):
    dt = pd.to_datetime(date_str)
    color = COLOR_MAP.get(cat, "#94a3b8")
    idx = df.index.searchsorted(dt)
    actual_price = float(df["price_usd"].iloc[min(idx, len(df)-1)])

    ax.axvline(x=dt, color=color, linewidth=0.6, alpha=0.45, linestyle="--", zorder=4)
    ax.scatter(dt, actual_price, color=color, s=55, zorder=6, edgecolors="white", linewidths=0.7)

    offset = 250 if i % 2 == 0 else -300
    y_text = max(actual_price + offset, 120)
    ax.annotate(label, xy=(dt, actual_price), xytext=(dt, y_text),
        fontsize=6.2, color=color, ha="center", fontweight="bold",
        arrowprops=dict(arrowstyle="-", color=color, lw=0.7, alpha=0.6),
        bbox=dict(boxstyle="round,pad=0.2", facecolor="#0f172a", edgecolor=color, alpha=0.88, linewidth=0.7),
        zorder=7)

ax.set_xlim(df.index[0], df.index[-1])
ax.set_ylim(0, ymax)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.xaxis.set_major_locator(mdates.YearLocator(2))
ax.tick_params(colors="#94a3b8", labelsize=10)
for spine in ["top","right"]: ax.spines[spine].set_visible(False)
for spine in ["bottom","left"]: ax.spines[spine].set_color("#334155")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x,_: f"${x:,.0f}"))
ax.grid(axis="y", color="#1e293b", linewidth=0.8)
ax.grid(axis="x", color="#1e293b", linewidth=0.4)

ax.set_title("📈 Lịch sử Giá Vàng Thế Giới 2000–2026 | Cột mốc & Sự kiện",
    color="white", fontsize=17, fontweight="bold", pad=22)
ax.set_ylabel("USD / Troy Ounce", color="#94a3b8", fontsize=12)

legend_elements = [
    Line2D([0],[0], color="#22c55e", linewidth=2, label="Bull Run 2001–2011 (+660%)"),
    Line2D([0],[0], color="#ef4444", linewidth=2, label="Bear Market 2011–2018 (-45%)"),
    Line2D([0],[0], color="#a855f7", linewidth=2, label="New Bull 2018–Now (+165%)"),
    Line2D([0],[0], color="#c084fc", marker="o", linestyle="None", markersize=7, label="All-time High"),
    Line2D([0],[0], color="#f87171", marker="o", linestyle="None", markersize=7, label="Khủng hoảng"),
    Line2D([0],[0], color="#60a5fa", marker="o", linestyle="None", markersize=7, label="Chính sách Fed"),
]
ax.legend(handles=legend_elements, loc="upper left", facecolor="#1e293b",
    edgecolor="#334155", labelcolor="white", fontsize=9, framealpha=0.9)

last_price = float(df["price_usd"].iloc[-1])
ax.annotate(f"Hôm nay\n${last_price:,.0f}/oz",
    xy=(df.index[-1], last_price), xytext=(df.index[-1], last_price+300),
    fontsize=9, color="#fbbf24", fontweight="bold",
    arrowprops=dict(arrowstyle="->", color="#fbbf24", lw=1.2),
    bbox=dict(boxstyle="round,pad=0.3", facecolor="#1e293b", edgecolor="#fbbf24", alpha=0.92),
    ha="right")

plt.tight_layout()
plt.savefig(CHART, dpi=150, bbox_inches="tight", facecolor="#0f172a")
print(f"Chart saved: {CHART}")

# --- 4. RSS recent news ---
print("\n=== Tin tức gần đây ===")
RSS = [
    "https://vnexpress.net/rss/kinh-doanh/vang.rss",
    "https://tuoitre.vn/rss/kinh-doanh.rss",
]
recent_headlines = []
for url in RSS:
    try:
        feed = feedparser.parse(url)
        for e in feed.entries[:4]:
            title = e.get("title","")
            if any(k in title.lower() for k in ["vàng","vang","gold"]):
                recent_headlines.append(title)
                print(f"  • {title[:90]}")
    except: pass

# --- 5. Print the story ---
p2000 = float(df["price_usd"].iloc[0])
p_peak = float(df["price_usd"].max())
p_now  = float(df["price_usd"].iloc[-1])
gain   = (p_now/p2000 - 1)*100

story = f"""
# 🪙 Câu Chuyện Vàng: Từ $256 Đến $3,000 — Hành Trình 25 Năm

## Tóm tắt số liệu
- **2000:** ${p2000:.0f}/oz → **2026:** ${p_now:.0f}/oz
- **Tổng tăng:** +{gain:.0f}% trong 25 năm (~+{gain/25:.0f}%/năm bình quân)
- **Đỉnh lịch sử:** ${p_peak:.0f}/oz
- **Số phiên dữ liệu:** {len(df):,} ngày

---

## Phần I: Đáy của Quên Lãng (2000–2004)

Năm 2001, vàng chạm đáy $256/oz — mức thấp nhất trong 20 năm. Thế giới đang sống trong "kỷ nguyên vàng" của cổ phiếu dot-com. Ai cần vàng khi NASDAQ tăng 100%/năm?

Nhưng sau bong bóng dot-com vỡ (2000) và sự kiện 9/11 (2001), nhà đầu tư bắt đầu tìm kiếm tài sản an toàn. Vàng âm thầm tích lũy, tạo đáy và bắt đầu hành trình 10 năm thăng thiên.

**Framework: Chu kỳ siêu vàng (Supercycle)**
Vàng vận hành theo chu kỳ dài 10–15 năm, phụ thuộc vào:
1. Lãi suất thực (Real interest rate = Nominal rate – Inflation)
2. Giá trị USD (DXY index)
3. Rủi ro địa chính trị

Khi lãi suất thực âm → giữ tiền mặt lỗ → mua vàng. Đây là công thức cốt lõi.

---

## Phần II: Con Bò Vàng 10 Năm (2004–2011)

Mỗi năm vàng tăng đều đặn: $400 (2004) → $600 (2006) → $1,000 (2008) → $1,921 (2011).

**Cột mốc quan trọng:**

**Tháng 3/2008: $1,000 lần đầu tiên**
Bear Stearns — ngân hàng đầu tư lớn thứ 5 nước Mỹ — sụp đổ. Fed cứu trợ khẩn cấp. Nhà đầu tư hoảng loạn mua vàng, phá ngưỡng tâm lý $1,000.

**Tháng 9/2008: Lehman Brothers**
Nghịch lý: khi Lehman sụp, vàng *giảm* từ $1,000 xuống $730. Tại sao?
→ **Margin call theory**: Quỹ đầu tư bị call margin, buộc bán tất cả tài sản kể cả vàng để có tiền mặt.

Sau đó Fed bơm $1.75 nghìn tỷ (QE1), vàng phục hồi mạnh.

**Tháng 9/2011: Đỉnh $1,921**
Eurozone khủng hoảng nợ: Hy Lạp, Ireland, Bồ Đào Nha, Tây Ban Nha đứng bên bờ vỡ nợ. Đây là đỉnh của chu kỳ bull đầu tiên. Tổng gain từ 2001: **+650%**.

---

## Phần III: 7 Năm Đau Khổ (2011–2018)

Từ $1,921 → $1,050 — vàng mất gần 50% giá trị trong 7 năm.

**Vì sao?**
1. **Fed taper tantrum (2013)**: Bernanke úp mở sẽ dừng QE → USD mạnh → vàng yếu
2. **Cyprus shock (4/2013)**: Chính phủ Cyprus bán 14 tấn vàng dự trữ → vàng sụp 9% chỉ trong 2 ngày — ngày sập lớn nhất trong 30 năm
3. **Dollar mạnh 2014–2016**: DXY tăng từ 79 → 103, vàng giảm tương ứng
4. **Fed tăng lãi suất 12/2015**: Lần đầu sau 9 năm — lãi suất thực dương → cạnh tranh với vàng

**Bài học từ giai đoạn này:**
Vàng *không phải lúc nào cũng tăng trong khủng hoảng*. Nó tăng khi lãi suất thực âm và giảm khi Fed thắt chặt.

---

## Phần IV: Cuộc Bùng Nổ Mới (2018–2026)

**2018–2019: Tích lũy**
Chiến tranh thương mại Mỹ-Trung, Fed cắt lãi suất 3 lần năm 2019 → vàng tăng từ $1,180 → $1,560.

**Tháng 3/2020: COVID Shock**
Lại cùng pattern Lehman: vàng đầu tiên *giảm* ($1,690 → $1,477) vì margin call. Rồi khi Fed in $3 nghìn tỷ và đưa lãi suất về 0% → **vàng lên $2,075 tháng 8/2020** — phá đỉnh 9 năm.

**2022–2024: Chu kỳ thắt chặt - nới lỏng**
- Fed tăng lãi từ 0% → 5.25% (mạnh nhất 40 năm) → vàng chỉ giảm 20% rồi hồi phục
- Điều này *bất thường*: trong chu kỳ tăng lãi trước (2013–2015), vàng giảm 40%
- **Lý do khác biệt**: Ngân hàng trung ương các nước mua kỷ lục (~1,000 tấn/năm, tập trung Trung Quốc, Nga, Ấn Độ)

**2024–2025: Đỉnh kép liên tiếp**
- Tháng 4/2024: $2,390 — Iran tấn công Israel bằng 300 tên lửa
- Tháng 10/2024: $2,790 — Bầu cử Mỹ, bất ổn địa chính trị
- Tháng 3/2025: $3,000+ — Thuế quan Trump gây lo ngại stagflation

---

## Phần V: Câu Chuyện Vàng Việt Nam

Việt Nam có đặc thù riêng: **SJC premium** — giá vàng trong nước cao hơn thế giới.

Giai đoạn 2011–2014: Chênh lệch SJC lên đến **5–8 triệu/lượng** do:
- Nhà nước độc quyền nhập khẩu vàng
- Dân không tin tưởng VND (lạm phát 18% năm 2011)
- Cầu mua vàng tích trữ rất cao

Tháng 6/2012: Nghị định 24 — Nhà nước độc quyền SJC, siết nhập khẩu. Chênh lệch thu hẹp nhưng không bao giờ về 0.

2023–2024: Chênh lệch lại bùng lên $10–15 triệu/lượng khi NHNN chậm bán đấu giá vàng. Phải đến giữa 2024 NHNN mới bán đấu giá liên tục để kéo chênh lệch xuống ~3–4 triệu/lượng.

---

## Phần VI: Chúng Ta Đang Ở Đâu?

**3 kịch bản cho 2025–2027:**

**🐂 Bull case ($3,500–$4,000):**
- Fed cắt lãi suất về 3% → lãi thực âm
- Nợ công Mỹ $37T, thâm hụt 6% GDP — USD dài hạn suy yếu
- Trung Quốc + Nga tiếp tục de-dollarization
- Xung đột địa chính trị leo thang

**🐻 Bear case ($2,000–$2,300):**
- Lạm phát tái bùng nổ, Fed phải tăng lãi lên 6%+
- USD mạnh trở lại (DXY > 110)
- Risk-on: cổ phiếu AI / crypto hút tiền khỏi vàng

**➡️ Base case ($2,500–$3,200):**
- Giá giữ vùng cao, biến động ±15%/năm
- Ngân hàng trung ương tiếp tục mua
- ETF và retail dần tham gia

**Kết luận:**
Vàng không phải đầu tư ngắn hạn. Nó là bảo hiểm cho sự bất ổn của hệ thống tiền tệ. Khi chính phủ in tiền, lạm phát, chiến tranh — vàng là ngôn ngữ chung của nhân loại suốt 5,000 năm lịch sử.

$256 → $3,000. Hành trình 25 năm, +{gain:.0f}%.
"""
print(story)
print("\n=== DONE ===")
print(f"Chart: {CHART}")
print(f"DB: {DB}")
