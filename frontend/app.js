/* ─────────────────────────────────────────── */
/* Vàng Đà Nẵng - Frontend App                 */
/* ─────────────────────────────────────────── */

const API = 'http://localhost:8000/api';

let allShops = [];
let currentFilters = { district: null, minRating: 0, isChain: null, search: '' };
let map = null;
let mapMarkers = [];
let userLat = null, userLng = null;

// ─── Init ────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  loadStats();
  loadDistricts();
  loadShops();
  loadPriceWidget();
  loadPriceCompare('SJC');
});

// ─── API helpers ─────────────────────────────
async function apiFetch(path) {
  const res = await fetch(API + path);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// ─── Stats ───────────────────────────────────
async function loadStats() {
  try {
    const stats = await apiFetch('/stats');
    const el = document.getElementById('headerStats');
    el.innerHTML = `
      <span>🏪 ${stats.total_shops} tiệm</span>
      <span>💬 ${stats.total_reviews} đánh giá</span>
      <span>📡 ${stats.price_sources_count} nguồn giá</span>
    `;
  } catch (e) {
    console.warn('Stats failed:', e);
  }
}

// ─── Districts ───────────────────────────────
async function loadDistricts() {
  try {
    const districts = await apiFetch('/districts');
    const container = document.getElementById('districtFilters');
    districts.forEach(d => {
      const btn = document.createElement('button');
      btn.className = 'chip';
      btn.textContent = d;
      btn.onclick = () => filterDistrict(d, btn);
      container.appendChild(btn);
    });
  } catch (e) {
    console.warn('Districts failed:', e);
  }
}

// ─── Shops ───────────────────────────────────
async function loadShops(lat = null, lng = null) {
  const listEl = document.getElementById('shopList');
  listEl.innerHTML = '<div class="loading">⏳ Đang tải...</div>';
  try {
    let shops;
    if (lat && lng) {
      shops = await apiFetch(`/shops/nearby?lat=${lat}&lng=${lng}&radius_km=10`);
    } else {
      let qs = '?limit=100';
      if (currentFilters.district) qs += `&district=${encodeURIComponent(currentFilters.district)}`;
      if (currentFilters.minRating) qs += `&min_rating=${currentFilters.minRating}`;
      if (currentFilters.isChain !== null) qs += `&is_chain=${currentFilters.isChain}`;
      if (currentFilters.search) qs += `&search=${encodeURIComponent(currentFilters.search)}`;
      shops = await apiFetch('/shops' + qs);
    }
    allShops = shops;
    renderShopList(shops);
    if (map) updateMapMarkers(shops);
  } catch (e) {
    listEl.innerHTML = `<div class="error-msg">❌ Không thể tải danh sách: ${e.message}</div>`;
  }
}

function renderShopList(shops) {
  const el = document.getElementById('shopList');
  if (!shops.length) {
    el.innerHTML = '<div class="loading">Không tìm thấy tiệm vàng nào 😢</div>';
    return;
  }
  el.innerHTML = shops.map(shop => shopCard(shop)).join('');
}

function shopCard(shop) {
  const stars = ratingStars(shop.rating);
  const dist = shop.distance_km != null ? `<span class="distance-badge">📍 ${shop.distance_km} km</span>` : '';
  const verified = shop.is_verified ? '<div class="verified-badge" title="Đã xác minh">✓</div>' : '';
  const chain = shop.is_chain ? '<div class="chain-badge">Chuỗi</div>' : '';
  const phone = shop.phone ? `<div class="shop-phone">📞 ${shop.phone}</div>` : '';

  return `
    <div class="shop-card" onclick="openShopModal(${shop.id})">
      <div class="shop-card-header">
        ${verified}${chain}
        <div class="shop-name">${shop.name}</div>
        <div class="shop-district">📍 ${shop.district || 'Đà Nẵng'}</div>
      </div>
      <div class="shop-card-body">
        <div class="rating-row">
          <span class="stars">${stars}</span>
          <span class="rating-num">${shop.rating.toFixed(1)}</span>
          <span class="review-count">(${shop.review_count} đánh giá)</span>
        </div>
        <div class="shop-address">🏠 ${shop.address || 'Đang cập nhật'}</div>
        ${phone}
        ${dist}
        <div class="card-actions">
          <button class="btn btn-gold btn-sm" onclick="event.stopPropagation();openShopModal(${shop.id})">Chi tiết</button>
          ${shop.google_maps_url ? `<a href="${shop.google_maps_url}" target="_blank" class="btn btn-outline btn-sm" onclick="event.stopPropagation()">🗺️ Maps</a>` : ''}
        </div>
      </div>
    </div>
  `;
}

function ratingStars(rating) {
  const full = Math.floor(rating);
  const half = rating % 1 >= 0.5;
  let s = '★'.repeat(full);
  if (half) s += '½';
  return s || '☆';
}

// ─── Filters ─────────────────────────────────
function filterDistrict(district, btn) {
  currentFilters.district = district;
  document.querySelectorAll('#districtFilters .chip').forEach(c => c.classList.remove('active'));
  btn.classList.add('active');
  loadShops();
}

function filterRating(min, btn) {
  currentFilters.minRating = min;
  document.querySelectorAll('.filter-section .chip').forEach(c => {
    if (c.onclick && c.onclick.toString().includes('filterRating')) c.classList.remove('active');
  });
  btn.classList.add('active');
  loadShops();
}

function filterChain(isChain, btn) {
  currentFilters.isChain = isChain;
  document.querySelectorAll('.filter-section .chip').forEach(c => {
    if (c.onclick && c.onclick.toString().includes('filterChain')) c.classList.remove('active');
  });
  btn.classList.add('active');
  loadShops();
}

function searchShops() {
  currentFilters.search = document.getElementById('searchInput').value.trim();
  loadShops();
}

document.addEventListener('DOMContentLoaded', () => {
  const input = document.getElementById('searchInput');
  if (input) input.addEventListener('keydown', e => { if (e.key === 'Enter') searchShops(); });
});

// ─── Geolocation ─────────────────────────────
function findNearMe() {
  if (!navigator.geolocation) {
    alert('Trình duyệt không hỗ trợ định vị');
    return;
  }
  navigator.geolocation.getCurrentPosition(
    pos => {
      userLat = pos.coords.latitude;
      userLng = pos.coords.longitude;
      loadShops(userLat, userLng);
      if (map) {
        map.setView([userLat, userLng], 14);
        L.marker([userLat, userLng], {
          icon: L.divIcon({ html: '📍', className: '', iconSize: [24,24] })
        }).addTo(map).bindPopup('Vị trí của bạn');
      }
    },
    err => alert('Không thể lấy vị trí: ' + err.message)
  );
}

// ─── Map ─────────────────────────────────────
function initMap() {
  if (map) return;
  // Da Nang center
  map = L.map('map').setView([16.054, 108.202], 13);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap contributors'
  }).addTo(map);
  updateMapMarkers(allShops);
}

function updateMapMarkers(shops) {
  mapMarkers.forEach(m => m.remove());
  mapMarkers = [];
  shops.forEach(shop => {
    if (!shop.lat || !shop.lng) return;
    const color = shop.rating >= 4.5 ? '#27ae60' : shop.rating >= 4.0 ? '#D4A017' : '#e67e22';
    const icon = L.divIcon({
      html: `<div style="background:${color};width:14px;height:14px;border-radius:50%;border:2px solid white;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>`,
      className: '', iconSize: [14, 14], iconAnchor: [7, 7],
    });
    const marker = L.marker([shop.lat, shop.lng], { icon })
      .addTo(map)
      .bindPopup(`
        <strong>${shop.name}</strong><br/>
        ⭐ ${shop.rating.toFixed(1)} (${shop.review_count} đánh giá)<br/>
        📍 ${shop.address || ''}<br/>
        <a href="#" onclick="openShopModal(${shop.id});return false;" style="color:#D4A017">Xem chi tiết →</a>
      `);
    mapMarkers.push(marker);
  });
}

// ─── Tabs ─────────────────────────────────────
function switchTab(name, btn) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('tab' + name.charAt(0).toUpperCase() + name.slice(1)).classList.add('active');
  if (name === 'map') {
    setTimeout(initMap, 50);
  }
}

// ─── Price Widget ─────────────────────────────
async function loadPriceWidget() {
  const el = document.getElementById('priceWidget');
  try {
    const prices = await apiFetch('/prices/latest?source=SJC');
    if (!prices.length) { el.innerHTML = '<small>Chưa có dữ liệu giá</small>'; return; }

    const top = prices.slice(0, 4);
    el.innerHTML = top.map(p => `
      <div class="price-row">
        <span class="label">${p.gold_type.substring(0, 20)}</span>
        <span>
          <span class="buy">${formatPrice(p.buy_price)}</span> /
          <span class="sell">${formatPrice(p.sell_price)}</span>
        </span>
      </div>
    `).join('');

    // Update time
    const ts = prices[0]?.crawled_at;
    if (ts) {
      el.innerHTML += `<small style="color:#999;margin-top:4px;display:block">Cập nhật: ${timeAgo(ts)}</small>`;
    }
  } catch (e) {
    el.innerHTML = '<small>Lỗi tải giá</small>';
  }
}

async function refreshPrices() {
  try {
    await fetch(API + '/crawl/prices', { method: 'POST' });
    setTimeout(loadPriceWidget, 2000);
    setTimeout(() => loadPriceCompare(document.querySelector('.gold-tab.active')?.textContent || 'SJC'), 2000);
  } catch (e) {
    console.error('Refresh prices failed:', e);
  }
}

// ─── Price Compare ────────────────────────────
async function loadPriceCompare(goldType, btn = null) {
  if (btn) {
    document.querySelectorAll('.gold-tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
  }
  const el = document.getElementById('priceCompareTable');
  el.innerHTML = '<div class="loading">⏳ Đang tải...</div>';
  try {
    const prices = await apiFetch(`/prices/compare?gold_type=${encodeURIComponent(goldType)}`);
    if (!prices.length) {
      el.innerHTML = '<div class="loading">Không có dữ liệu giá cho loại này</div>';
      return;
    }
    const minSell = Math.min(...prices.filter(p => p.sell_price).map(p => p.sell_price));

    el.innerHTML = `
      <table class="price-table">
        <thead>
          <tr>
            <th>Nguồn</th>
            <th>Loại vàng</th>
            <th>Mua vào</th>
            <th>Bán ra</th>
            <th>Cập nhật</th>
          </tr>
        </thead>
        <tbody>
          ${prices.map(p => {
            const isBest = p.sell_price === minSell;
            return `
              <tr class="${isBest ? 'best-deal' : ''}">
                <td><strong>${p.source_name}</strong>${isBest ? '<span class="best-deal-badge">Tốt nhất</span>' : ''}</td>
                <td>${p.gold_type}</td>
                <td style="color:var(--success);font-weight:600">${formatPrice(p.buy_price)}</td>
                <td style="color:var(--danger);font-weight:600">${formatPrice(p.sell_price)}</td>
                <td style="color:var(--text-muted);font-size:0.8rem">${p.crawled_at ? timeAgo(p.crawled_at) : '—'}</td>
              </tr>
            `;
          }).join('')}
        </tbody>
      </table>
    `;
  } catch (e) {
    el.innerHTML = `<div class="error-msg">Lỗi tải giá so sánh: ${e.message}</div>`;
  }
}

// ─── Shop Modal ───────────────────────────────
async function openShopModal(shopId) {
  const modal = document.getElementById('shopModal');
  const body = document.getElementById('modalBody');
  modal.classList.remove('hidden');
  body.innerHTML = '<div class="loading">⏳ Đang tải...</div>';

  try {
    const shop = await apiFetch(`/shops/${shopId}`);
    const stars = ratingStars(shop.rating);

    const prices = shop.latest_prices?.length ? `
      <div class="modal-section">
        <h3>💰 Giá vàng tại tiệm</h3>
        <table class="price-table">
          <thead><tr><th>Loại vàng</th><th>Mua vào</th><th>Bán ra</th></tr></thead>
          <tbody>
            ${shop.latest_prices.map(p => `
              <tr>
                <td>${p.gold_type}</td>
                <td style="color:var(--success);font-weight:600">${formatPrice(p.buy_price)}</td>
                <td style="color:var(--danger);font-weight:600">${formatPrice(p.sell_price)}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    ` : '';

    const reviews = shop.recent_reviews?.length ? `
      <div class="modal-section">
        <h3>💬 Đánh giá gần đây</h3>
        ${shop.recent_reviews.map(r => `
          <div class="review-card">
            <div style="display:flex;justify-content:space-between">
              <span class="review-author">${r.author || 'Ẩn danh'}</span>
              <span class="stars">${ratingStars(r.rating || 5)}</span>
            </div>
            <div class="review-text">${r.text || ''}</div>
            <small style="color:#aaa">${r.date || ''}</small>
          </div>
        `).join('')}
      </div>
    ` : '';

    const links = [
      shop.google_maps_url && `<a href="${shop.google_maps_url}" target="_blank" class="btn btn-outline btn-sm">🗺️ Google Maps</a>`,
      shop.website && `<a href="${shop.website}" target="_blank" class="btn btn-outline btn-sm">🌐 Website</a>`,
      shop.facebook_url && `<a href="${shop.facebook_url}" target="_blank" class="btn btn-outline btn-sm">📘 Facebook</a>`,
    ].filter(Boolean).join('');

    body.innerHTML = `
      <div class="modal-shop-name">${shop.name}</div>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
        <span class="stars" style="font-size:1.1rem">${stars}</span>
        <span style="font-weight:700;font-size:1rem">${shop.rating.toFixed(1)}</span>
        <span style="color:var(--text-muted)">(${shop.review_count} đánh giá)</span>
        ${shop.is_verified ? '<span style="background:#e8f8ee;color:var(--success);padding:2px 10px;border-radius:12px;font-size:0.8rem;font-weight:600">✓ Đã xác minh</span>' : ''}
        ${shop.is_chain ? '<span style="background:#fff3e0;color:#e67e22;padding:2px 10px;border-radius:12px;font-size:0.8rem;font-weight:600">Chuỗi</span>' : ''}
      </div>
      <div class="modal-section">
        <h3>ℹ️ Thông tin</h3>
        <div class="info-grid">
          <div class="info-item"><label>Địa chỉ</label><span>${shop.address || '—'}</span></div>
          <div class="info-item"><label>Quận</label><span>${shop.district || '—'}</span></div>
          <div class="info-item"><label>Điện thoại</label><span>${shop.phone || '—'}</span></div>
          <div class="info-item"><label>Giờ mở cửa</label><span>${shop.hours || '—'}</span></div>
        </div>
        ${shop.description ? `<p style="margin-top:10px;color:var(--text-muted);font-size:0.9rem">${shop.description}</p>` : ''}
      </div>
      ${prices}
      ${reviews}
      ${links ? `<div class="modal-section"><div style="display:flex;gap:8px;flex-wrap:wrap">${links}</div></div>` : ''}
    `;
  } catch (e) {
    body.innerHTML = `<div class="error-msg">Lỗi: ${e.message}</div>`;
  }
}

function closeModal() {
  document.getElementById('shopModal').classList.add('hidden');
}

// ─── Formatters ───────────────────────────────
function formatPrice(val) {
  if (!val) return '—';
  return (val / 1000000).toFixed(2) + ' tr';
}

function timeAgo(isoStr) {
  const d = new Date(isoStr);
  const diff = (Date.now() - d.getTime()) / 1000;
  if (diff < 60) return 'vừa xong';
  if (diff < 3600) return `${Math.floor(diff / 60)} phút trước`;
  if (diff < 86400) return `${Math.floor(diff / 3600)} giờ trước`;
  return `${Math.floor(diff / 86400)} ngày trước`;
}
