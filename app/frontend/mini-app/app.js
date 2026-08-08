/**
 * Domino's Order Engine — Customer Mini App v2.0
 * Telegram WebApp + WebSocket real-time ordering platform
 */

'use strict';

// =====================================================
// STATE
// =====================================================
const State = {
  user: null,
  accessToken: null,
  cart: [],
  products: [],
  categories: [],
  savedAddresses: [],
  orders: [],
  notifications: [],
  unreadNotifCount: 0,
  config: {},
  locationPricing: { price_multiplier: 1.0, delivery_charge: 30.0, min_order_value: 149.0 },
  detectedCity: null,
  detectedLat: null,
  detectedLng: null,
  currentPage: 'home',
  ws: null,
  wsReconnectTimer: null,
  trackerMap: null,
  trackerMarker: null,
  riderMarker: null,
  trackerOrderId: null,
  paymentTimerInterval: null,
  currentProductModal: null,
  currentPaymentOrder: null,
  vegOnly: false,
  menuSearch: '',
  menuCategory: 'all',
};

// =====================================================
// API HELPERS
// =====================================================
const API_BASE = '/api';

async function apiFetch(path, opts = {}) {
  const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
  if (State.accessToken) headers['Authorization'] = `Bearer ${State.accessToken}`;
  try {
    const res = await fetch(`${API_BASE}${path}`, { ...opts, headers });
    if (res.status === 401) {
      await refreshToken();
      headers['Authorization'] = `Bearer ${State.accessToken}`;
      const retry = await fetch(`${API_BASE}${path}`, { ...opts, headers });
      return retry.ok ? retry.json() : Promise.reject(await retry.json());
    }
    return res.ok ? res.json() : Promise.reject(await res.json());
  } catch (e) {
    if (e && e.detail) {
      if (Array.isArray(e.detail)) {
        const msg = e.detail.map(d => {
          const field = d.loc ? d.loc.join('.') : 'field';
          return `${field}: ${d.msg}`;
        }).join(', ');
        throw new Error(msg);
      } else if (typeof e.detail === 'object') {
        throw new Error(JSON.stringify(e.detail));
      } else {
        throw new Error(e.detail);
      }
    }
    throw e;
  }
}

async function refreshToken() {
  try {
    const data = await fetch(`${API_BASE}/auth/refresh`, { method: 'POST', credentials: 'include' })
      .then(r => r.json());
    if (data.access_token) State.accessToken = data.access_token;
  } catch (e) {
    console.warn('Token refresh failed:', e);
  }
}

// =====================================================
// TELEGRAM WEBAPP INIT
// =====================================================
const tg = window.Telegram?.WebApp;

function initTelegram() {
  if (tg) {
    tg.ready();
    tg.expand();
    tg.enableClosingConfirmation();
    // Apply Telegram color scheme if available
    if (tg.colorScheme === 'light') {
      document.documentElement.setAttribute('data-theme', 'light');
    }
  }
}

// =====================================================
// WEBSOCKET MANAGER
// =====================================================
function connectWebSocket(userId) {
  if (!State.accessToken) return;
  if (State.ws) { State.ws.close(); State.ws = null; }
  const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${wsProto}//${location.host}/ws/${userId}?token=${encodeURIComponent(State.accessToken || '')}`;
  try {
    State.ws = new WebSocket(wsUrl);
    State.ws.onopen = () => {
      console.log('[WS] Connected');
      clearInterval(State.wsReconnectTimer);
      // Start heartbeat
      State.wsPingInterval = setInterval(() => {
        if (State.ws && State.ws.readyState === WebSocket.OPEN) {
          State.ws.send(JSON.stringify({ type: 'ping' }));
        }
      }, 25000);
    };
    State.ws.onmessage = (evt) => {
      try { handleWsMessage(JSON.parse(evt.data)); } catch (e) {}
    };
    State.ws.onclose = (evt) => {
      console.log('[WS] Disconnected, reconnecting in 3s...');
      clearInterval(State.wsPingInterval);
      if (evt && evt.code === 1008) {
        console.log('[WS] Unauthorized connection (1008). Stopping reconnect.');
        return;
      }
      State.wsReconnectTimer = setTimeout(() => connectWebSocket(userId), 3000);
    };
    State.ws.onerror = () => State.ws.close();
  } catch (e) {
    console.warn('[WS] Error:', e);
  }
}

function handleWsMessage(msg) {
  switch (msg.type) {
    case 'order_update':
      if (State.currentPage === 'orders') loadOrders();
      if (State.trackerOrderId === msg.order_id) refreshTracker(msg.order_id);
      showToast(`Order ${msg.order_id}: ${msg.status}`, 'info');
      loadNotifications();
      break;
    case 'rider_assigned':
      if (State.trackerOrderId === msg.order_id) {
        updateRiderCard(msg.rider);
        showToast(`🛵 Rider assigned: ${msg.rider.name} — ${msg.rider.phone}`, 'info');
      }
      break;
    case 'rider_location':
      if (State.trackerOrderId === msg.order_id) {
        updateRiderMapMarker(msg.lat, msg.lng);
      }
      break;
    case 'connected':
    case 'pong':
    case 'heartbeat':
      break;
    default:
      console.log('[WS] Unknown message:', msg);
  }
}

// =====================================================
// SSE FALLBACK
// =====================================================
function initSSE() {
  const token = State.accessToken || '';
  const evtSource = new EventSource(`/api/events?token=${encodeURIComponent(token)}`);
  evtSource.onmessage = (evt) => {
    try {
      const data = JSON.parse(evt.data);
      if (data.type === 'order_update') {
        if (State.currentPage === 'orders') loadOrders();
        loadNotifications();
      }
    } catch (e) {}
  };
}

// =====================================================
// LOCATION DETECTION
// =====================================================
function updateLocationPricing(pricing) {
  if (!pricing) return;
  State.locationPricing = pricing;
  const etaChargeEl = document.getElementById('eta-charge');
  if (etaChargeEl) etaChargeEl.textContent = `₹${pricing.delivery_charge}`;
  updatePriceSummary();
  const pricingNote = document.getElementById('location-pricing-note');
  if (pricingNote) {
    pricingNote.style.display = pricing.price_multiplier !== 1.0 ? '' : 'none';
  }
  
  // Re-render all product grids to show correct local prices
  renderPopularGrid();
  renderRecommendedGrid();
  renderMenuProducts();
  renderCartItems();
}

async function detectLocation(force = false) {
  const cityEl = document.getElementById('location-city');
  const detectBtn = document.getElementById('detect-location-btn');
  if (detectBtn) detectBtn.textContent = 'Detecting...';

  // If we have a saved city and are not forcing detection, load it
  if (!force && State.user && State.user.city) {
    const city = State.user.city;
    State.detectedCity = city;
    if (cityEl) cityEl.textContent = city;
    try {
      const pricing = await apiFetch(`/location/pricing?city=${encodeURIComponent(city)}`).catch(() => null);
      if (pricing) {
        updateLocationPricing(pricing);
        await loadProducts();
      }
    } catch (e) {
      console.warn('Error fetching pricing for saved city:', e);
    }
    if (detectBtn) detectBtn.textContent = 'Detect';
    return;
  }

  return new Promise((resolve) => {
    if (!navigator.geolocation) {
      if (cityEl) cityEl.textContent = 'Unknown';
      resolve(null);
      return;
    }
    navigator.geolocation.getCurrentPosition(
      async (pos) => {
        State.detectedLat = pos.coords.latitude;
        State.detectedLng = pos.coords.longitude;
        try {
          // Reverse geocode with Nominatim
          const res = await fetch(
            `https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=${State.detectedLat}&lon=${State.detectedLng}`,
            { headers: { 'Accept-Language': 'en' } }
          );
          const geo = await res.json();
          const city = geo.address?.city || geo.address?.town || geo.address?.village || geo.address?.county || 'Your Area';
          State.detectedCity = city;
          if (cityEl) cityEl.textContent = city;

          // Save to user profile database record
          try {
            await apiFetch('/users/profile', { method: 'PUT', body: JSON.stringify({ city }) });
            if (State.user) State.user.city = city;
          } catch (profileErr) {
            console.warn('Failed to save detected city to profile:', profileErr);
          }

          // Fetch location-based pricing
          const pricing = await apiFetch(`/location/pricing?city=${encodeURIComponent(city)}`).catch(() => null);
          if (pricing) {
            updateLocationPricing(pricing);
            await loadProducts();
          }

          // Update detected address display
          const addrText = document.getElementById('detected-address-text');
          if (addrText && geo.display_name) {
            addrText.textContent = geo.display_name.split(',').slice(0, 3).join(', ');
          }

          // Pre-fill delivery address input
          const addrInput = document.getElementById('delivery-address');
          if (addrInput && !addrInput.value) {
            const parts = [
              geo.address?.road,
              geo.address?.suburb || geo.address?.neighbourhood,
              geo.address?.city || geo.address?.town,
              geo.address?.state
            ].filter(Boolean);
            addrInput.value = parts.join(', ');
          }

        } catch (e) {
          if (cityEl) cityEl.textContent = 'Detected';
        }
        if (detectBtn) detectBtn.textContent = '✓ Done';
        resolve(pos);
      },
      (err) => {
        console.warn('Geolocation error:', err);
        if (cityEl) cityEl.textContent = 'Unknown';
        if (detectBtn) detectBtn.textContent = 'Detect';
        resolve(null);
      },
      { enableHighAccuracy: true, timeout: 10000 }
    );
  });
}

// =====================================================
// AUTH
// =====================================================
async function login() {
  const initData = tg?.initData || '';

  // Mock for dev testing
  if (!initData) {
    const mockInitData = `user=%7B%22id%22%3A7958236048%2C%22first_name%22%3A%22Pizza%22%2C%22last_name%22%3A%22User%22%2C%22username%22%3A%22pizzauser%22%7D&hash=mock`;
    try {
      const data = await apiFetch('/auth/login', {
        method: 'POST',
        body: JSON.stringify({ initData: mockInitData })
      });
      State.accessToken = data.access_token;
      State.user = data.user;
      return true;
    } catch (e) {
      console.error('Login failed:', e);
      return false;
    }
  }

  try {
    const data = await apiFetch('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ initData })
    });
    State.accessToken = data.access_token;
    State.user = data.user;
    return true;
  } catch (e) {
    console.error('Telegram login failed:', e);
    return false;
  }
}

// =====================================================
// PRODUCTS & MENU
// =====================================================
async function loadProducts() {
  try {
    State.products = await apiFetch('/products');
    const cats = [...new Set(State.products.map(p => p.category))];
    State.categories = cats;
    renderHomeCategories();
    renderPopularGrid();
    renderRecommendedGrid();
    renderMenuCategoryTabs();
    renderMenuProducts();
  } catch (e) {
    console.error('Failed to load products:', e);
  }
}

function getEffectivePrice(product) {
  const base = product.discounted_price ?? product.original_price;
  return Math.round(base * State.locationPricing.price_multiplier);
}

function getOriginalPrice(product) {
  return Math.round(product.original_price * State.locationPricing.price_multiplier);
}

function renderHomeCategories() {
  const container = document.getElementById('home-categories');
  if (!container) return;
  const catIcons = { 'Veg': '🥦', 'Non-Veg': '🍗', 'Cheese Burst': '🧀', 'Pizza Mania': '🎉', 'Sides': '🍟', 'Drinks': '🥤', 'Desserts': '🍰' };
  container.innerHTML = State.categories.map(cat => `
    <div class="cat-chip" data-cat="${cat}" onclick="filterByCategory('${cat}')">
      <span class="cat-chip-icon">${catIcons[cat] || '🍕'}</span>
      <span class="cat-chip-name">${cat}</span>
    </div>
  `).join('');
}

function filterByCategory(cat) {
  navigateTo('menu');
  State.menuCategory = cat;
  document.querySelectorAll('.cat-tab').forEach(t => t.classList.toggle('active', t.dataset.cat === cat));
  renderMenuProducts();
}

function renderPopularGrid() {
  const grid = document.getElementById('popular-grid');
  if (!grid) return;
  const popular = State.products.filter(p => p.is_popular && p.availability).slice(0, 4);
  grid.innerHTML = popular.map(p => renderProductCard(p)).join('');
}

function renderRecommendedGrid() {
  const grid = document.getElementById('recommended-grid');
  if (!grid) return;
  const recommended = State.products.filter(p => p.is_recommended && p.availability).slice(0, 4);
  grid.innerHTML = recommended.map(p => renderProductCard(p)).join('');
}

function renderProductCard(product) {
  const price = getEffectivePrice(product);
  const origPrice = getOriginalPrice(product);
  const hasDiscount = product.discounted_price !== null && price < origPrice;
  const badge = product.is_popular ? '<span class="card-badge popular">🔥 Popular</span>' :
    product.is_recommended ? '<span class="card-badge recommended">⭐ Rec.</span>' : '';

  return `
    <div class="product-card" onclick="openProductModal('${product.id}')">
      ${badge}
      ${product.image_url
        ? `<img class="product-card-img" src="${product.image_url}" alt="${product.name}" loading="lazy" onerror="this.style.display='none'" />`
        : `<div class="product-card-img-placeholder">🍕</div>`}
      <div class="product-card-body">
        <div class="product-card-header">
          <div class="veg-dot ${product.is_veg ? 'veg' : 'nonveg'}"></div>
          <span class="product-card-name">${product.name}</span>
        </div>
        <div class="product-card-footer">
          <div class="product-card-price">
            ${hasDiscount ? `<span class="price-original">₹${origPrice}</span>` : ''}
            <span class="price-final ${hasDiscount ? 'price-discounted' : ''}">₹${price}</span>
          </div>
          <button class="product-add-btn" onclick="event.stopPropagation(); quickAddToCart('${product.id}')">+</button>
        </div>
      </div>
    </div>`;
}

function renderMenuCategoryTabs() {
  const tabs = document.getElementById('menu-category-tabs');
  if (!tabs) return;
  tabs.innerHTML = `<button class="cat-tab ${State.menuCategory === 'all' ? 'active' : ''}" data-cat="all">All</button>` +
    State.categories.map(cat => `<button class="cat-tab ${State.menuCategory === cat ? 'active' : ''}" data-cat="${cat}">${cat}</button>`).join('');
  tabs.querySelectorAll('.cat-tab').forEach(tab => {
    tab.onclick = () => {
      State.menuCategory = tab.dataset.cat;
      tabs.querySelectorAll('.cat-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderMenuProducts();
    };
  });
}

function renderMenuProducts() {
  const list = document.getElementById('menu-product-list');
  if (!list) return;
  let filtered = State.products.filter(p => p.availability);
  if (State.menuCategory !== 'all') filtered = filtered.filter(p => p.category === State.menuCategory);
  if (State.vegOnly) filtered = filtered.filter(p => p.is_veg);
  if (State.menuSearch) {
    const q = State.menuSearch.toLowerCase();
    filtered = filtered.filter(p => p.name.toLowerCase().includes(q) || (p.description || '').toLowerCase().includes(q));
  }

  if (!filtered.length) {
    list.innerHTML = `<div class="text-center" style="padding:40px;color:var(--text-muted)">No items found</div>`;
    return;
  }

  list.innerHTML = filtered.map(product => {
    const price = getEffectivePrice(product);
    const origPrice = getOriginalPrice(product);
    const hasDiscount = product.discounted_price !== null && price < origPrice;
    return `
      <div class="product-list-item" onclick="openProductModal('${product.id}')">
        ${product.image_url
          ? `<img class="product-list-img" src="${product.image_url}" alt="${product.name}" loading="lazy" />`
          : `<div class="product-list-img-placeholder">🍕</div>`}
        <div class="product-list-content">
          <div class="product-list-header">
            <div class="veg-dot ${product.is_veg ? 'veg' : 'nonveg'}"></div>
            <span class="product-list-name">${product.name}</span>
          </div>
          <p class="product-list-desc">${product.description || ''}</p>
          <div class="product-list-footer">
            <div class="product-list-price">
              ${hasDiscount ? `<span class="list-price-original">₹${origPrice}</span>` : ''}
              <span class="list-price-final ${hasDiscount ? 'discounted' : ''}">₹${price}</span>
            </div>
            <button class="btn btn-outline btn-sm" onclick="event.stopPropagation(); openProductModal('${product.id}')">Add +</button>
          </div>
        </div>
      </div>`;
  }).join('');
}

// =====================================================
// PRODUCT MODAL
// =====================================================
function openProductModal(productId) {
  const product = State.products.find(p => p.id === productId);
  if (!product) return;
  State.currentProductModal = { product, qty: 1, crust: null, size: null };

  document.getElementById('pm-image').src = product.image_url || '';
  document.getElementById('pm-name').textContent = product.name;
  document.getElementById('pm-description').textContent = product.description || '';

  // Veg indicator
  const vegEl = document.getElementById('pm-veg-indicator');
  vegEl.className = `pm-veg-indicator ${product.is_veg ? 'veg' : 'nonveg'}`;

  // Badges
  const badgesEl = document.getElementById('pm-badges');
  badgesEl.innerHTML = [
    product.is_popular ? '<span class="card-badge popular">🔥 Popular</span>' : '',
    product.is_recommended ? '<span class="card-badge recommended">⭐ Recommended</span>' : '',
  ].join('');

  // Crust options
  const crustSection = document.getElementById('pm-crust-section');
  const crustGrid = document.getElementById('pm-crust-grid');
  const crusts = product.crust_options ? JSON.parse(product.crust_options) : [];
  if (crusts.length) {
    crustSection.style.display = '';
    State.currentProductModal.crust = crusts[0];
    crustGrid.innerHTML = crusts.map((c, i) => `
      <button class="pm-option-btn ${i === 0 ? 'active' : ''}" onclick="selectOption('crust','${c}',this)">${c}</button>
    `).join('');
  } else {
    crustSection.style.display = 'none';
  }

  // Size options
  const sizeSection = document.getElementById('pm-size-section');
  const sizeGrid = document.getElementById('pm-size-grid');
  const sizes = product.size_options ? JSON.parse(product.size_options) : [];
  if (sizes.length) {
    sizeSection.style.display = '';
    State.currentProductModal.size = sizes[0];
    sizeGrid.innerHTML = sizes.map((s, i) => `
      <button class="pm-option-btn ${i === 0 ? 'active' : ''}" onclick="selectOption('size','${s.replace(/"/g,"'")}',this)">${s}</button>
    `).join('');
  } else {
    sizeSection.style.display = 'none';
  }

  // Price
  updateProductModalPrice();

  // Show modal
  document.getElementById('product-modal-overlay').classList.remove('hidden');
}

function selectOption(type, value, btn) {
  State.currentProductModal[type] = value;
  const grid = type === 'crust' ? document.getElementById('pm-crust-grid') : document.getElementById('pm-size-grid');
  grid.querySelectorAll('.pm-option-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  updateProductModalPrice();
}

function updateProductModalPrice() {
  if (!State.currentProductModal) return;
  const { product, qty } = State.currentProductModal;
  const price = getEffectivePrice(product);
  const origPrice = getOriginalPrice(product);
  const hasDiscount = product.discounted_price !== null && price < origPrice;

  document.getElementById('pm-price').textContent = `₹${price}`;
  document.getElementById('pm-original-price').textContent = hasDiscount ? `₹${origPrice}` : '';
  const discBadge = document.getElementById('pm-discount-badge');
  if (hasDiscount) {
    const pct = Math.round((1 - price / origPrice) * 100);
    discBadge.textContent = `${pct}% OFF`;
    discBadge.style.display = '';
  } else {
    discBadge.style.display = 'none';
  }
  document.getElementById('pm-qty').textContent = qty;
  document.getElementById('pm-cart-total').textContent = `₹${price * qty}`;
}

function closeProductModal() {
  document.getElementById('product-modal-overlay').classList.add('hidden');
  State.currentProductModal = null;
}

// =====================================================
// CART MANAGEMENT
// =====================================================
function addToCart(productId, qty = 1, crust = null, size = null) {
  const product = State.products.find(p => p.id === productId);
  if (!product) return;
  const price = getEffectivePrice(product);
  const existing = State.cart.find(i => i.product_id === productId && i.crust === crust && i.size === size);
  if (existing) {
    existing.quantity += qty;
  } else {
    State.cart.push({ product_id: productId, product, quantity: qty, price, crust, size });
  }
  saveCart();
  updateCartBadge();
  renderCartItems();
  updatePriceSummary();
  animateAddToCart();
  showToast(`${product.name} added to cart! 🛒`, 'success');
}

function quickAddToCart(productId) {
  const product = State.products.find(p => p.id === productId);
  if (!product) return;
  const crusts = product.crust_options ? JSON.parse(product.crust_options) : [];
  const sizes = product.size_options ? JSON.parse(product.size_options) : [];
  addToCart(productId, 1, crusts[0] || null, sizes[0] || null);
}

function removeFromCart(productId, crust, size) {
  State.cart = State.cart.filter(i => !(i.product_id === productId && i.crust === crust && i.size === size));
  saveCart();
  updateCartBadge();
  renderCartItems();
  updatePriceSummary();
}

function updateCartQuantity(productId, crust, size, delta) {
  const item = State.cart.find(i => i.product_id === productId && i.crust === crust && i.size === size);
  if (!item) return;
  item.quantity += delta;
  if (item.quantity <= 0) {
    removeFromCart(productId, crust, size);
    return;
  }
  saveCart();
  updateCartBadge();
  renderCartItems();
  updatePriceSummary();
}

function saveCart() {
  try { localStorage.setItem('ag_cart', JSON.stringify(State.cart)); } catch (e) {}
}

function loadCartFromStorage() {
  try {
    const stored = localStorage.getItem('ag_cart');
    if (stored) {
      const parsed = JSON.parse(stored);
      // Re-join with current products to get fresh data
      State.cart = parsed.map(item => {
        const product = State.products.find(p => p.id === item.product_id);
        if (!product) return null;
        return { ...item, product, price: getEffectivePrice(product) };
      }).filter(Boolean);
    }
  } catch (e) { State.cart = []; }
}

function clearCart() {
  State.cart = [];
  saveCart();
  updateCartBadge();
  renderCartItems();
  updatePriceSummary();
}

function updateCartBadge() {
  const count = State.cart.reduce((s, i) => s + i.quantity, 0);
  const badge = document.getElementById('cart-count-badge');
  if (!badge) return;
  badge.textContent = count;
  badge.classList.toggle('hidden', count === 0);
  document.getElementById('checkout-total').textContent = `₹${getCartTotal()}`;
}

function getCartTotal() {
  const subtotal = State.cart.reduce((s, i) => s + i.price * i.quantity, 0);
  return Math.round(subtotal + State.locationPricing.delivery_charge);
}

function animateAddToCart() {
  const badge = document.getElementById('cart-count-badge');
  if (!badge) return;
  badge.style.transform = 'scale(1.5)';
  setTimeout(() => { badge.style.transform = ''; }, 300);
}

function renderCartItems() {
  const list = document.getElementById('cart-items-list');
  const empty = document.getElementById('cart-empty');
  const checkoutBtn = document.getElementById('checkout-btn');
  const couponSection = document.getElementById('coupon-section');
  const priceSummary = document.getElementById('price-summary');
  const paymentSection = document.getElementById('payment-section');
  const checkoutBtnEl = document.querySelector('.checkout-btn');

  if (!State.cart.length) {
    if (empty) empty.classList.remove('hidden');
    if (list) list.innerHTML = '';
    if (checkoutBtn) checkoutBtn.disabled = true;
    if (couponSection) couponSection.style.display = 'none';
    if (priceSummary) priceSummary.style.display = 'none';
    if (paymentSection) paymentSection.style.display = 'none';
    if (checkoutBtnEl) checkoutBtnEl.style.display = 'none';
    return;
  }

  if (empty) empty.classList.add('hidden');
  if (couponSection) couponSection.style.display = '';
  if (priceSummary) priceSummary.style.display = '';
  if (paymentSection) paymentSection.style.display = '';
  if (checkoutBtnEl) checkoutBtnEl.style.display = '';
  if (checkoutBtn) checkoutBtn.disabled = false;

  if (!list) return;
  list.innerHTML = State.cart.map(item => `
    <div class="cart-item">
      ${item.product.image_url
        ? `<img class="cart-item-img" src="${item.product.image_url}" alt="${item.product.name}" />`
        : `<div class="cart-item-img-placeholder">🍕</div>`}
      <div class="cart-item-content">
        <div class="cart-item-name">${item.product.name}</div>
        <div class="cart-item-meta">${[item.crust, item.size].filter(Boolean).join(' · ') || 'Standard'}</div>
        <div class="cart-item-price">₹${item.price * item.quantity}</div>
      </div>
      <div class="cart-item-controls">
        <div class="qty-stepper">
          <button class="qty-stepper-btn" onclick="updateCartQuantity('${item.product_id}','${item.crust}','${item.size}',-1)">−</button>
          <span class="qty-stepper-val">${item.quantity}</span>
          <button class="qty-stepper-btn" onclick="updateCartQuantity('${item.product_id}','${item.crust}','${item.size}',1)">+</button>
        </div>
        <button class="remove-btn" onclick="removeFromCart('${item.product_id}','${item.crust}','${item.size}')">✕</button>
      </div>
    </div>`).join('');
}

async function fetchEligibleCoupon() {
  if (State.eligibleCoupon) return State.eligibleCoupon;
  try {
    const res = await apiFetch('/coupons/eligible');
    State.eligibleCoupon = res.coupon;
    return res.coupon;
  } catch (e) {
    console.error('Failed to fetch eligible coupon:', e);
    return null;
  }
}

async function updatePriceSummary() {
  const el = (id) => document.getElementById(id);
  if (!State.cart || State.cart.length === 0) {
    if (el('summary-subtotal')) el('summary-subtotal').textContent = `₹0`;
    if (el('summary-discount')) el('summary-discount').textContent = `-₹0`;
    if (el('summary-delivery')) el('summary-delivery').textContent = `₹0`;
    if (el('summary-fee')) el('summary-fee').textContent = `₹0`;
    if (el('summary-total')) el('summary-total').textContent = `₹0`;
    if (el('checkout-total')) el('checkout-total').textContent = `₹0`;
    return;
  }

  const val_min = State.config && State.config.cart_promo_min ? parseFloat(State.config.cart_promo_min) : 180;
  const val_max = State.config && State.config.cart_promo_max ? parseFloat(State.config.cart_promo_max) : 220;
  const val_fixed = State.config && State.config.cart_promo_fixed ? parseFloat(State.config.cart_promo_fixed) : 100;
  const botFee = State.config && State.config.bot_fee ? parseFloat(State.config.bot_fee) : 10;

  // 1. Calculate subtotal (excluding ketchup for auto-add checks)
  let subtotalNoKetchup = State.cart
    .filter(item => item.product.name !== "Tomato Ketchup (Auto-Added)")
    .reduce((sum, item) => sum + (item.price * item.quantity), 0);

  // 2. Ketchup auto-add checks
  const diff = val_min - subtotalNoKetchup;
  let ketchupChanged = false;
  if (diff >= 10 && diff <= 20) {
    const ketchupProd = State.products && State.products.find(p => p.name === "Tomato Ketchup (Auto-Added)");
    if (ketchupProd) {
      const existingIndex = State.cart.findIndex(item => item.product_id === ketchupProd.id);
      if (existingIndex !== -1) {
        if (State.cart[existingIndex].price !== diff) {
          State.cart[existingIndex].price = diff;
          ketchupChanged = true;
        }
      } else {
        State.cart.push({
          product_id: ketchupProd.id,
          product: ketchupProd,
          quantity: 1,
          price: diff,
          crust: null,
          size: null
        });
        ketchupChanged = true;
      }
    }
  } else {
    const ketchupProd = State.products && State.products.find(p => p.name === "Tomato Ketchup (Auto-Added)");
    if (ketchupProd) {
      const existingIndex = State.cart.findIndex(item => item.product_id === ketchupProd.id);
      if (existingIndex !== -1) {
        State.cart.splice(existingIndex, 1);
        ketchupChanged = true;
      }
    }
  }

  if (ketchupChanged) {
    renderCartItems();
    saveCart();
  }

  const subtotal = State.cart.reduce((sum, item) => sum + (item.price * item.quantity), 0);

  let total = 0;
  let service_charge = 0;
  let isCapped = val_min <= subtotal && subtotal <= val_max;

  if (isCapped) {
    service_charge = botFee;
    total = val_fixed + botFee;
    
    // Auto-apply coupon
    const coupon = await fetchEligibleCoupon();
    if (coupon && !State.appliedCoupon) {
      State.appliedCoupon = coupon;
      const couponInput = el('coupon-input');
      if (couponInput) couponInput.value = coupon;
      const msg = el('coupon-msg');
      if (msg) {
        msg.textContent = `✓ Coupon "${coupon}" auto-applied!`;
        msg.className = 'coupon-msg success';
        msg.style.display = '';
      }
    }
  } else {
    service_charge = 5.0;
    total = subtotal + 5.0;
    
    // Clear applied coupon
    State.appliedCoupon = null;
    const couponInput = el('coupon-input');
    if (couponInput) couponInput.value = '';
    const msg = el('coupon-msg');
    if (msg) {
      msg.textContent = '';
      msg.style.display = 'none';
    }
  }

  if (el('summary-subtotal')) el('summary-subtotal').textContent = `₹${subtotal.toFixed(2)}`;
  if (el('summary-fee')) el('summary-fee').textContent = `₹${service_charge.toFixed(2)}`;
  if (el('summary-total')) el('summary-total').textContent = `₹${total.toFixed(2)}`;
  if (el('checkout-total')) el('checkout-total').textContent = `₹${total.toFixed(2)}`;

  const couponRow = el('coupon-row');
  if (couponRow) {
    if (isCapped && State.appliedCoupon) {
      couponRow.style.display = '';
      if (el('coupon-name')) el('coupon-name').textContent = 'Auto-Applied Cap Discount';
      if (el('coupon-saving')) el('coupon-saving').textContent = `-₹${Math.round(subtotal - val_fixed).toFixed(2)}`;
    } else {
      couponRow.style.display = 'none';
    }
  }
}

// =====================================================
// SAVED ADDRESSES
// =====================================================
async function loadSavedAddresses() {
  if (!State.accessToken) return;
  try {
    State.savedAddresses = await apiFetch('/addresses');
    renderSavedAddressChips();
    renderProfileAddresses();
  } catch (e) { console.error('Failed to load addresses:', e); }
}

function renderSavedAddressChips() {
  const row = document.getElementById('saved-addresses-row');
  if (!row) return;
  if (!State.savedAddresses.length) {
    row.innerHTML = '';
    return;
  }
  row.innerHTML = State.savedAddresses.map(addr => `
    <div class="saved-addr-chip ${addr.is_default ? 'active' : ''}" onclick="selectSavedAddress('${addr.id}', this)">
      ${addr.label === 'Home' ? '🏠' : addr.label === 'Work' ? '💼' : '📌'} ${addr.label}
    </div>`).join('');
}

function selectSavedAddress(addrId, element) {
  const addr = State.savedAddresses.find(a => a.id === addrId);
  if (!addr) return;
  const deliveryAddrInput = document.getElementById('delivery-address');
  if (deliveryAddrInput) deliveryAddrInput.value = addr.full_address;
  const deliveryLandmarkInput = document.getElementById('delivery-landmark');
  if (deliveryLandmarkInput) deliveryLandmarkInput.value = addr.landmark || '';
  if (addr.latitude) State.detectedLat = addr.latitude;
  if (addr.longitude) State.detectedLng = addr.longitude;
  document.querySelectorAll('.saved-addr-chip').forEach(c => c.classList.remove('active'));
  if (element) element.classList.add('active');
  showToast(`${addr.label} address selected`, 'info');

  // Trigger location update & product list reload if city changes
  if (addr.city) {
    State.detectedCity = addr.city;
    const cityEl = document.getElementById('location-city');
    if (cityEl) cityEl.textContent = addr.city;
    apiFetch(`/location/pricing?city=${encodeURIComponent(addr.city)}`)
      .then(pricing => {
        if (pricing) {
          updateLocationPricing(pricing);
          loadProducts();
        }
      }).catch(() => {});
  }
}

function renderProfileAddresses() {
  const container = document.getElementById('profile-addresses');
  if (!container) return;
  if (!State.savedAddresses.length) {
    container.innerHTML = '<div class="profile-empty-state">No saved addresses</div>';
    return;
  }
  container.innerHTML = State.savedAddresses.map(addr => `
    <div class="profile-addr-chip ${addr.is_default ? 'default' : ''}">
      <span class="addr-chip-label">${addr.is_default ? '✓ Default' : addr.label}</span>
      <span class="addr-chip-address">${addr.full_address}</span>
      <button class="text-btn danger" onclick="deleteAddress('${addr.id}')">✕</button>
    </div>`).join('');
}

async function deleteAddress(addrId) {
  try {
    await apiFetch(`/addresses/${addrId}`, { method: 'DELETE' });
    await loadSavedAddresses();
    showToast('Address deleted', 'info');
  } catch (e) {
    showToast('Failed to delete address', 'error');
  }
}

async function saveCurrentAddress() {
  const address = document.getElementById('delivery-address').value.trim();
  const landmark = document.getElementById('delivery-landmark').value.trim();
  const label = document.getElementById('addr-label-select').value;
  if (!address) return;
  try {
    await apiFetch('/addresses', {
      method: 'POST',
      body: JSON.stringify({
        label, full_address: address, landmark,
        city: State.detectedCity,
        latitude: State.detectedLat, longitude: State.detectedLng,
        is_default: false
      })
    });
    await loadSavedAddresses();
    showToast('Address saved!', 'success');
  } catch (e) {
    showToast('Could not save address', 'error');
  }
}

// =====================================================
// TELEGRAM ACCOUNT LINKING
// =====================================================
async function getTelegramLinkStatus() {
  if (!State.accessToken) return;
  try {
    const res = await apiFetch('/users/link-telegram/status');
    const input = document.getElementById('link-telegram-id');
    const btn = document.getElementById('link-telegram-btn');
    const instructions = document.getElementById('telegram-link-instructions');
    
    if (res.telegram_verified) {
      if (input) {
        input.value = res.telegram_id || '';
        input.disabled = true;
      }
      if (btn) {
        btn.textContent = 'Connected ✓';
        btn.disabled = true;
      }
      if (instructions) {
        instructions.innerHTML = `<span style="color:#22C55E">✓ Connected with Telegram account ID: ${res.telegram_id}</span>`;
        instructions.classList.remove('hidden');
      }
    }
  } catch (e) {
    console.error('Failed to get Telegram link status:', e);
  }
}

async function linkTelegramAccount() {
  const telegramIdInput = document.getElementById('link-telegram-id');
  const telegramId = telegramIdInput?.value?.trim();
  if (!telegramId) {
    showToast('Please enter your Telegram ID', 'error');
    return;
  }
  
  const btn = document.getElementById('link-telegram-btn');
  if (btn) {
    btn.textContent = 'Verifying...';
    btn.disabled = true;
  }
  
  try {
    const res = await apiFetch('/users/link-telegram', {
      method: 'POST',
      body: JSON.stringify({ telegram_id: telegramId })
    });
    
    const instructions = document.getElementById('telegram-link-instructions');
    if (instructions) {
      instructions.innerHTML = `
        <div style="margin-top:6px;padding:8px;background:rgba(255,146,43,0.1);border-radius:6px;border:1px solid rgba(255,146,43,0.2)">
          <strong>Verification Code: ${res.code}</strong><br>
          Please start our Telegram Bot using this link: <a href="${res.deep_link}" target="_blank" style="color:#ff922b;text-decoration:underline">Verify Account</a> or send <code>/verify ${res.code}</code> to the bot.
        </div>`;
      instructions.classList.remove('hidden');
    }
    showToast('Verification code generated!', 'success');
  } catch (e) {
    showToast(e.message || 'Failed to generate verification code', 'error');
    if (btn) {
      btn.textContent = 'Verify';
      btn.disabled = false;
    }
  }
}

// =====================================================
// ADD ADDRESS MODAL
// =====================================================
function openAddAddressModal() {
  const overlay = document.getElementById('address-modal-overlay');
  if (overlay) overlay.classList.remove('hidden');
}

function closeAddAddressModal() {
  const overlay = document.getElementById('address-modal-overlay');
  if (overlay) overlay.classList.add('hidden');
}

async function saveModalAddress() {
  const label = document.getElementById('modal-addr-label-select')?.value || 'Home';
  const address = document.getElementById('modal-delivery-address')?.value?.trim();
  const landmark = document.getElementById('modal-delivery-landmark')?.value?.trim();
  
  if (!address) {
    showToast('Please enter your full address', 'error');
    return;
  }
  
  const btn = document.getElementById('save-modal-address-btn');
  if (btn) {
    btn.textContent = 'Saving...';
    btn.disabled = true;
  }
  
  try {
    await apiFetch('/addresses', {
      method: 'POST',
      body: JSON.stringify({
        label, full_address: address, landmark,
        city: State.detectedCity || 'Your Area',
        latitude: State.detectedLat || 12.9716,
        longitude: State.detectedLng || 77.5946,
        is_default: State.savedAddresses.length === 0
      })
    });
    
    // Clear fields
    const addrInput = document.getElementById('modal-delivery-address');
    if (addrInput) addrInput.value = '';
    const landmarkInput = document.getElementById('modal-delivery-landmark');
    if (landmarkInput) landmarkInput.value = '';
    
    closeAddAddressModal();
    await loadSavedAddresses();
    showToast('Address saved successfully!', 'success');
  } catch (e) {
    showToast(e.message || 'Could not save address', 'error');
  } finally {
    if (btn) {
      btn.textContent = 'Save Address';
      btn.disabled = false;
    }
  }
}

// =====================================================
// CHECKOUT
// =====================================================
async function checkout() {
  if (!State.cart.length) { showToast('Cart is empty!', 'error'); return; }
  if (!State.user) { showToast('Please login first', 'error'); return; }

  const address = document.getElementById('delivery-address')?.value?.trim();
  const landmark = document.getElementById('delivery-landmark')?.value?.trim();
  const phone = document.getElementById('delivery-phone')?.value?.trim();

  if (!address) { showToast('Please enter delivery address', 'error'); navigateTo('cart'); return; }
  if (!phone) { showToast('Please enter phone number', 'error'); navigateTo('cart'); return; }

  const paymentMethod = document.querySelector('input[name="payment"]:checked')?.value || 'direct';
  const lat = State.detectedLat || 12.9716;
  const lng = State.detectedLng || 77.5946;

  // ── Gather Telemetry Data ────────────────────────
  const payload = {
    items: State.cart.map(i => ({ product_id: i.product_id, quantity: i.quantity })),
    payment_method: paymentMethod,
    address,
    landmark: landmark || null,
    latitude: lat,
    longitude: lng,
    phone,
    delivery_instructions: null,
    coupon_code: State.appliedCoupon || null,
    device_id: null,
    device_details: null
  };

  const btn = document.getElementById('checkout-btn');
  if (btn) {
    btn.textContent = 'Placing Order...';
    btn.disabled = true;
  }

  try {
    const result = await apiFetch('/orders', { method: 'POST', body: JSON.stringify(payload) });

    // Save address if checkbox checked
    if (document.getElementById('save-address-check')?.checked) {
      await saveCurrentAddress();
    }

    clearCart();

    if (paymentMethod === 'direct' && result.qr_code_url) {
      // Show payment modal
      State.currentPaymentOrder = result;
      openPaymentModal(result);
    } else {
      showToast(`Order ${result.order_id} placed! 🎉`, 'success');
      btn.innerHTML = `<span class="checkout-btn-icon">🍕</span> Place Order &nbsp;·&nbsp; <span id="checkout-total">₹0</span>`;
      btn.disabled = false;
      navigateTo('orders');
      loadOrders();
    }
  } catch (e) {
    showToast(e.message || 'Order failed. Please try again.', 'error');
    const tot = document.getElementById('summary-total')?.textContent || '₹0';
    btn.innerHTML = `<span class="checkout-btn-icon">🍕</span> Place Order &nbsp;·&nbsp; <span id="checkout-total">${tot}</span>`;
    btn.disabled = false;
  }
}

// =====================================================
// PAYMENT MODAL
// =====================================================
function openPaymentModal(orderResult) {
  document.getElementById('qr-image').src = orderResult.qr_code_url;
  document.getElementById('qr-amount').textContent = `₹${orderResult.total}`;
  document.getElementById('payment-upi-id').textContent = orderResult.upi_id || State.config.upi_id || 'dominos@upi';

  const linkEl = document.getElementById('qr-direct-pay-link');
  if (linkEl && orderResult.upi_uri) {
    linkEl.href = orderResult.upi_uri;
    linkEl.style.display = 'flex';
  } else if (linkEl) {
    linkEl.style.display = 'none';
  }

  document.getElementById('payment-modal-overlay').classList.remove('hidden');

  // Start 15-minute timer
  let seconds = 15 * 60;
  clearInterval(State.paymentTimerInterval);
  State.paymentTimerInterval = setInterval(() => {
    seconds--;
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    const el = document.getElementById('payment-timer');
    if (el) el.textContent = `${m}:${s.toString().padStart(2, '0')}`;
    if (seconds <= 0) {
      clearInterval(State.paymentTimerInterval);
      closePaymentModal();
      showToast('Payment window expired. Order cancelled.', 'error');
    }
  }, 1000);
}

function closePaymentModal() {
  document.getElementById('payment-modal-overlay').classList.add('hidden');
  clearInterval(State.paymentTimerInterval);
}

async function verifyUTR() {
  const utr = document.getElementById('utr-input').value.trim();
  if (!utr) { showToast('Please enter UTR number', 'error'); return; }

  const orderId = State.currentPaymentOrder?.order_id;
  if (!orderId) return;

  const btn = document.getElementById('verify-utr-btn');
  btn.textContent = 'Verifying...';
  btn.disabled = true;

  try {
    const result = await apiFetch(`/orders/${orderId}/verify-payment`, {
      method: 'POST',
      body: JSON.stringify({ utr })
    });
    closePaymentModal();
    showToast('Payment verified! Order processing 🎉', 'success');
    navigateTo('orders');
    loadOrders();
  } catch (e) {
    const msgEl = document.getElementById('utr-msg');
    msgEl.textContent = e.message || 'Verification failed';
    msgEl.className = 'utr-msg error';
    msgEl.classList.remove('hidden');
    btn.textContent = 'Verify';
    btn.disabled = false;
  }
}

// =====================================================
// ORDERS
// =====================================================
async function loadOrders() {
  try {
    State.orders = await apiFetch('/orders');
    renderOrders('all');
  } catch (e) {
    console.error('Failed to load orders:', e);
  }
}

function renderOrders(filter = 'all') {
  const list = document.getElementById('orders-list');
  const empty = document.getElementById('orders-empty');
  if (!list) return;

  const activeStatuses = ['Payment Pending', 'Payment Received', 'Order Processing', 'Preparing', 'Out for Delivery'];
  const completedStatuses = ['Delivered', 'Completed', 'Cancelled', 'Refunded'];

  let orders = State.orders || [];
  if (filter === 'active') orders = orders.filter(o => activeStatuses.includes(o.status));
  if (filter === 'completed') orders = orders.filter(o => completedStatuses.includes(o.status));

  if (!orders.length) {
    list.innerHTML = '';
    if (empty) empty.classList.remove('hidden');
    return;
  }

  if (empty) empty.classList.add('hidden');

  list.innerHTML = orders.map(order => {
    const statusClass = order.status.toLowerCase().replace(/\s+/g, '-');
    const itemsPreview = (order.items || []).slice(0, 2).map(i => `${i.quantity}× ${i.product_name || 'Item'}`).join(', ');
    const createdDate = new Date(order.created_at).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', year: 'numeric' });
    const progress = order.progress || 0;
    return `
      <div class="order-card" onclick="openTracker('${order.id}')">
        <div class="order-card-header">
          <div>
            <div class="order-id">${order.id}</div>
            <div class="order-date">${createdDate}</div>
          </div>
          <span class="status-badge ${statusClass}">${order.status_icon || ''} ${order.status}</span>
        </div>
        <div class="order-card-body">
          <div class="order-items-preview">${itemsPreview}${order.items?.length > 2 ? ` +${order.items.length - 2} more` : ''}</div>
          <div class="order-progress-bar">
            <div class="order-progress-fill" style="width:${progress}%"></div>
          </div>
          <div class="order-card-footer">
            <span class="order-total">₹${order.total_payable}</span>
            <span class="order-track-btn">Track →</span>
          </div>
        </div>
      </div>`;
  }).join('');
}

// =====================================================
// ORDER TRACKER (with live map)
// =====================================================
async function openTracker(orderId) {
  State.trackerOrderId = orderId;
  document.getElementById('tracker-modal-overlay').classList.remove('hidden');

  // Initialize map
  if (!State.trackerMap) {
    State.trackerMap = L.map('tracker-map', {
      zoomControl: false,
      attributionControl: false
    }).setView([20.5937, 78.9629], 5);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '© OSM',
      maxZoom: 18
    }).addTo(State.trackerMap);
  }

  await refreshTracker(orderId);
}

async function refreshTracker(orderId) {
  try {
    const order = await apiFetch(`/orders/${orderId}`);
    const orderIdEl = document.getElementById('tracker-order-id');
    if (orderIdEl) orderIdEl.textContent = `Order ${orderId}`;

    const statusBadge = document.getElementById('tracker-status-badge');
    if (statusBadge) {
      statusBadge.textContent = `${order.status_icon || ''} ${order.status}`;
      statusBadge.className = `tracker-status-badge status-badge ${order.status.toLowerCase().replace(/\s+/g, '-')}`;
    }

    // Populate delivery details card in tracker modal
    const addrEl = document.getElementById('tracker-address');
    if (addrEl) addrEl.textContent = order.address || '—';
    const landmarkEl = document.getElementById('tracker-landmark');
    if (landmarkEl) landmarkEl.textContent = order.landmark || '—';
    const phoneEl = document.getElementById('tracker-phone');
    if (phoneEl) phoneEl.textContent = order.phone || '—';
    const paymentEl = document.getElementById('tracker-payment');
    if (paymentEl) paymentEl.textContent = order.payment_method === 'wallet' ? 'Wallet Balance' : 'Direct UPI QR Scan';

    // Delivery location pin
    if (order.latitude && order.longitude && State.trackerMap) {
      if (!State.trackerMarker) {
        const deliveryIcon = L.divIcon({
          html: '<div style="font-size:24px">📍</div>',
          iconSize: [30, 30],
          className: ''
        });
        State.trackerMarker = L.marker([order.latitude, order.longitude], { icon: deliveryIcon })
          .addTo(State.trackerMap)
          .bindPopup('Delivery Location');
      }
      State.trackerMap.setView([order.latitude, order.longitude], 14);
      
      // Fix hidden container rendering issue
      setTimeout(() => {
        if (State.trackerMap) {
          State.trackerMap.invalidateSize();
        }
      }, 250);
    }

    // Rider info
    if (order.rider?.assigned) {
      updateRiderCard(order.rider);
      if (order.rider.lat && order.rider.lng) {
        updateRiderMapMarker(order.rider.lat, order.rider.lng);
      }
    }

    // Status timeline
    const timeline = document.getElementById('tracker-timeline');
    const steps = [
      { status: 'Payment Received', icon: '✅', label: 'Payment Confirmed' },
      { status: 'Order Processing', icon: '📋', label: 'Order Accepted' },
      { status: 'Preparing', icon: '👨‍🍳', label: 'Preparing Your Pizza' },
      { status: 'Out for Delivery', icon: '🛵', label: 'Out for Delivery' },
      { status: 'Delivered', icon: '🎉', label: 'Delivered' },
    ];

    const statusOrder = steps.map(s => s.status);
    const currentIdx = statusOrder.indexOf(order.status);

    const historyMap = {};
    (order.status_history || []).forEach(h => { historyMap[h.status] = h.timestamp; });

    timeline.innerHTML = steps.map((step, idx) => {
      const isDone = idx < currentIdx;
      const isCurrent = idx === currentIdx;
      const time = historyMap[step.status];
      const timeStr = time ? new Date(time).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }) : '';
      return `
        <div class="timeline-step ${isDone ? 'done' : isCurrent ? 'current' : ''}">
          <div class="timeline-dot">${step.icon}</div>
          <div class="timeline-content">
            <div class="timeline-label">${step.label}</div>
            ${timeStr ? `<div class="timeline-time">${timeStr}</div>` : ''}
          </div>
        </div>`;
    }).join('');

    // Items summary
    const itemsEl = document.getElementById('tracker-items');
    itemsEl.innerHTML = (order.items || []).map(item => `
      <div class="tracker-item">
        <span class="tracker-item-name">${item.product_name || item.name}</span>
        <span class="tracker-item-qty">× ${item.quantity}</span>
        <span class="tracker-item-price">₹${item.price * item.quantity}</span>
      </div>`).join('') + `
      <div class="tracker-item" style="border-top:1px solid var(--border-subtle);margin-top:8px;padding-top:8px;font-weight:700">
        <span>Total</span>
        <span></span>
        <span>₹${order.total_payable}</span>
      </div>`;

  } catch (e) {
    console.error('Tracker refresh failed:', e);
  }
}

function updateRiderCard(rider) {
  const card = document.getElementById('rider-card');
  if (!card) return;
  const nameEl = document.getElementById('rider-name');
  if (nameEl) nameEl.textContent = rider.name || '—';
  const vehicleEl = document.getElementById('rider-vehicle');
  if (vehicleEl) vehicleEl.textContent = rider.vehicle || '🛵 Domino\'s Delivery';
  const callEl = document.getElementById('rider-call-btn');
  if (callEl && rider.phone) callEl.href = `tel:${rider.phone}`;
  card.style.display = '';
}

function updateRiderMapMarker(lat, lng) {
  if (!State.trackerMap) return;
  const riderLatLng = [lat, lng];
  if (!State.riderMarker) {
    const riderIcon = L.divIcon({
      html: '<div style="font-size:24px">🛵</div>',
      iconSize: [30, 30],
      className: ''
    });
    State.riderMarker = L.marker(riderLatLng, { icon: riderIcon })
      .addTo(State.trackerMap)
      .bindPopup('Delivery Rider');
  } else {
    State.riderMarker.setLatLng(riderLatLng);
  }
  
  if (State.trackerMarker) {
    const group = new L.featureGroup([State.trackerMarker, State.riderMarker]);
    State.trackerMap.fitBounds(group.getBounds().pad(0.15));
  } else {
    State.trackerMap.setView(riderLatLng, 14);
  }
}

function closeTracker() {
  document.getElementById('tracker-modal-overlay').classList.add('hidden');
  State.trackerOrderId = null;
}

// =====================================================
// NOTIFICATIONS
// =====================================================
async function loadNotifications() {
  if (!State.accessToken) return;
  try {
    State.notifications = await apiFetch('/notifications?limit=20');
    State.unreadNotifCount = State.notifications.filter(n => !n.is_read).length;
    const badge = document.getElementById('notif-badge');
    if (badge) {
      badge.textContent = State.unreadNotifCount;
      badge.classList.toggle('hidden', State.unreadNotifCount === 0);
    }
    renderNotifications();
  } catch (e) {}
}

function renderNotifications() {
  const list = document.getElementById('notif-list');
  if (!list) return;
  if (!State.notifications.length) {
    list.innerHTML = '<div class="notif-empty">No notifications yet</div>';
    return;
  }
  list.innerHTML = State.notifications.map(n => `
    <div class="notif-item ${!n.is_read ? 'unread' : ''}" onclick="markNotifRead('${n.id}')">
      <div class="notif-item-title">${n.title}</div>
      <div class="notif-item-body">${n.body}</div>
      <div class="notif-item-time">${new Date(n.created_at).toLocaleString('en-IN')}</div>
    </div>`).join('');
}

async function markNotifRead(id) {
  await apiFetch(`/notifications/${id}/read`, { method: 'PUT' }).catch(() => {});
  await loadNotifications();
}

async function markAllRead() {
  await apiFetch('/notifications/read-all', { method: 'PUT' }).catch(() => {});
  await loadNotifications();
}

function toggleNotifPanel() {
  const panel = document.getElementById('notif-panel');
  const overlay = document.getElementById('panel-overlay');
  const isHidden = panel.classList.contains('hidden');
  panel.classList.toggle('hidden', !isHidden);
  overlay.classList.toggle('hidden', !isHidden);
}

// =====================================================
// PROFILE
// =====================================================
function renderUserInfo() {
  if (!State.user) return;
  const { display_name, photo_url, username } = State.user;

  // Top bar
  document.getElementById('user-name-display').textContent = display_name?.split(' ')[0] || 'Guest';
  document.getElementById('user-initial').textContent = (display_name || 'U')[0].toUpperCase();
  if (photo_url) {
    const photoEl = document.getElementById('user-photo');
    photoEl.src = photo_url;
    photoEl.classList.remove('hidden');
    document.getElementById('user-initial').style.display = 'none';
  }

  // Profile page
  document.getElementById('profile-name').textContent = display_name || 'User';
  document.getElementById('profile-username').textContent = `@${username || 'unknown'}`;
  document.getElementById('profile-initial').textContent = (display_name || 'U')[0].toUpperCase();
  if (photo_url) {
    const pPhoto = document.getElementById('profile-photo');
    pPhoto.src = photo_url;
    pPhoto.classList.remove('hidden');
    document.getElementById('profile-initial').style.display = 'none';
  }
  if (State.user.phone) {
    document.getElementById('profile-phone').value = State.user.phone;
  }
}

async function savePhone() {
  const phone = document.getElementById('profile-phone').value.trim();
  if (!phone) return;
  try {
    await apiFetch('/users/profile', { method: 'PUT', body: JSON.stringify({ phone }) });
    State.user.phone = phone;
    if (document.getElementById('delivery-phone')) document.getElementById('delivery-phone').value = phone;
    showToast('Phone saved!', 'success');
  } catch (e) {
    showToast('Failed to save phone', 'error');
  }
}

// =====================================================
// NAVIGATION
// =====================================================
function navigateTo(page) {
  if (State.currentPage === page) return;
  State.currentPage = page;

  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));

  const pageEl = document.getElementById(`page-${page}`);
  const navEl = document.getElementById(`nav-${page}`);
  if (pageEl) { pageEl.classList.add('active'); pageEl.scrollTop = 0; }
  if (navEl) navEl.classList.add('active');

  // Page-specific load actions
  if (page === 'cart') { renderCartItems(); updatePriceSummary(); loadSavedAddresses(); }
  if (page === 'orders') { loadOrders(); }
  if (page === 'profile') { loadSavedAddresses(); getTelegramLinkStatus(); }
  if (page === 'menu') { renderMenuCategoryTabs(); renderMenuProducts(); }
}

// =====================================================
// TOAST
// =====================================================
function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  if (!container) return;
  const icons = { success: '✓', error: '✕', info: 'ℹ', warning: '⚠' };
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span>${icons[type] || 'ℹ'}</span> ${message}`;
  container.appendChild(toast);
  setTimeout(() => toast.remove(), 3200);
}

// =====================================================
// CONFIG LOAD
// =====================================================
async function loadConfig() {
  try {
    State.config = await apiFetch('/config');
    // Pre-fill UPI ID from config
    const upiEl = document.getElementById('payment-upi-id');
    if (upiEl) upiEl.textContent = State.config.upi_id || 'dominos@upi';
  } catch (e) {}
}

// =====================================================
// EVENT LISTENERS SETUP
// =====================================================
function setupEventListeners() {
  // Bottom nav
  document.querySelectorAll('.nav-item').forEach(btn => {
    btn.addEventListener('click', () => navigateTo(btn.dataset.page));
  });

  // Hero order button
  document.getElementById('hero-order-btn')?.addEventListener('click', () => navigateTo('menu'));
  document.getElementById('see-all-btn')?.addEventListener('click', () => navigateTo('menu'));

  // Quick checkout shortcut button
  document.getElementById('quick-checkout-btn')?.addEventListener('click', async () => {
    const prod = State.products && State.products.find(p => p.name.includes("Margherita"));
    if (!prod) {
      showToast('Shortcut deal product not found in menu.', 'error');
      return;
    }
    State.cart = [{
      product_id: prod.id,
      product: prod,
      quantity: 2,
      price: prod.discounted_price || prod.original_price,
      crust: "Cheese Burst",
      size: "Medium"
    }];
    saveCart();
    showToast('2x Cheeseburst Margherita added to cart!', 'success');
    navigateTo('cart');
  });

  // Menu search
  document.getElementById('menu-search')?.addEventListener('input', (e) => {
    State.menuSearch = e.target.value;
    renderMenuProducts();
  });

  // Veg toggle
  document.getElementById('veg-only-toggle')?.addEventListener('change', (e) => {
    State.vegOnly = e.target.checked;
    renderMenuProducts();
  });

  // Product modal controls
  document.getElementById('product-modal-close')?.addEventListener('click', closeProductModal);
  document.getElementById('product-modal-overlay')?.addEventListener('click', (e) => {
    if (e.target.id === 'product-modal-overlay') closeProductModal();
  });
  document.getElementById('pm-qty-minus')?.addEventListener('click', () => {
    if (State.currentProductModal && State.currentProductModal.qty > 1) {
      State.currentProductModal.qty--;
      updateProductModalPrice();
    }
  });
  document.getElementById('pm-qty-plus')?.addEventListener('click', () => {
    if (State.currentProductModal) {
      State.currentProductModal.qty++;
      updateProductModalPrice();
    }
  });
  document.getElementById('pm-add-to-cart')?.addEventListener('click', () => {
    if (!State.currentProductModal) return;
    const { product, qty, crust, size } = State.currentProductModal;
    addToCart(product.id, qty, crust, size);
    closeProductModal();
  });

  // Location detect button
  document.getElementById('detect-location-btn')?.addEventListener('click', () => detectLocation(true));

  // Location selection modal opening when clicking the location pill
  document.getElementById('location-pill')?.addEventListener('click', async () => {
    const overlay = document.getElementById('location-modal-overlay');
    const select = document.getElementById('location-city-select');
    if (!overlay || !select) return;
    
    // Clear and show loading state
    select.innerHTML = '<option value="">Loading locations...</option>';
    overlay.classList.remove('hidden');
    
    try {
      const locations = await apiFetch('/location');
      if (locations && locations.length > 0) {
        select.innerHTML = locations
          .map(loc => `<option value="${loc.city}" ${State.detectedCity === loc.city ? 'selected' : ''}>${loc.city} (${loc.state || 'India'})</option>`)
          .join('');
      } else {
        select.innerHTML = '<option value="">No serviceable locations found</option>';
      }
    } catch (e) {
      console.error('Failed to load serviceable locations:', e);
      select.innerHTML = '<option value="">Error loading locations</option>';
    }
  });

  // Location selection modal close
  document.getElementById('location-modal-close')?.addEventListener('click', () => {
    document.getElementById('location-modal-overlay')?.classList.add('hidden');
  });
  
  document.getElementById('location-modal-overlay')?.addEventListener('click', (e) => {
    if (e.target.id === 'location-modal-overlay') {
      document.getElementById('location-modal-overlay')?.classList.add('hidden');
    }
  });

  // Save location button inside selection modal
  document.getElementById('save-location-btn')?.addEventListener('click', async () => {
    const select = document.getElementById('location-city-select');
    const city = select?.value;
    if (!city) {
      showToast('Please select a city', 'error');
      return;
    }
    
    const btn = document.getElementById('save-location-btn');
    if (btn) {
      btn.textContent = 'Saving...';
      btn.disabled = true;
    }
    
    try {
      // Save city to user profile database record
      await apiFetch('/users/profile', { method: 'PUT', body: JSON.stringify({ city }) });
      if (State.user) State.user.city = city;
      State.detectedCity = city;
      
      const cityEl = document.getElementById('location-city');
      if (cityEl) cityEl.textContent = city;
      
      // Fetch location-based pricing
      const pricing = await apiFetch(`/location/pricing?city=${encodeURIComponent(city)}`).catch(() => null);
      if (pricing) {
        updateLocationPricing(pricing);
      }
      
      // Reload products list
      await loadProducts();
      showToast(`Location updated to ${city}!`, 'success');
      document.getElementById('location-modal-overlay')?.classList.add('hidden');
    } catch (e) {
      showToast('Failed to save location', 'error');
      console.error(e);
    } finally {
      if (btn) {
        btn.textContent = 'Save Location';
        btn.disabled = false;
      }
    }
  });

  // Save address checkbox
  document.getElementById('save-address-check')?.addEventListener('change', (e) => {
    document.getElementById('addr-label-select')?.classList.toggle('hidden', !e.target.checked);
  });

  // Checkout button
  document.getElementById('checkout-btn')?.addEventListener('click', checkout);

  // Clear cart
  document.getElementById('clear-cart-btn')?.addEventListener('click', () => {
    if (confirm('Clear cart?')) clearCart();
  });

  // Browse menu buttons
  document.getElementById('browse-menu-btn')?.addEventListener('click', () => navigateTo('menu'));
  document.getElementById('start-ordering-btn')?.addEventListener('click', () => navigateTo('menu'));

  // Payment modal
  document.getElementById('payment-modal-close')?.addEventListener('click', closePaymentModal);
  document.getElementById('payment-modal-overlay')?.addEventListener('click', (e) => {
    if (e.target.id === 'payment-modal-overlay') closePaymentModal();
  });
  document.getElementById('verify-utr-btn')?.addEventListener('click', verifyUTR);
  document.getElementById('copy-upi-btn')?.addEventListener('click', () => {
    const upiId = State.config.upi_id || 'dominos@upi';
    navigator.clipboard.writeText(upiId).then(() => showToast('UPI ID copied!', 'success'));
  });

  // Tracker modal
  document.getElementById('tracker-modal-close')?.addEventListener('click', closeTracker);

  // Notifications
  document.getElementById('notif-btn')?.addEventListener('click', (e) => {
    e.stopPropagation();
    toggleNotifPanel();
  });
  document.getElementById('panel-overlay')?.addEventListener('click', toggleNotifPanel);
  document.getElementById('mark-all-read-btn')?.addEventListener('click', markAllRead);

  // Orders filter tabs
  document.querySelectorAll('.filter-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.filter-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderOrders(tab.dataset.filter);
    });
  });

  // Profile actions
  document.getElementById('save-phone-btn')?.addEventListener('click', savePhone);
  document.getElementById('order-history-link')?.addEventListener('click', () => navigateTo('orders'));
  document.getElementById('support-link')?.addEventListener('click', () => {
    showToast('Redirecting to support bot...', 'info');
    if (tg && typeof tg.openTelegramLink === 'function') {
      tg.openTelegramLink('https://t.me/dominosordersHELP_bot');
    } else {
      window.open('https://t.me/dominosordersHELP_bot', '_blank');
    }
  });
  document.getElementById('logout-btn')?.addEventListener('click', async () => {
    await apiFetch('/auth/logout', { method: 'POST' }).catch(() => {});
    State.accessToken = null;
    State.user = null;
    localStorage.removeItem('ag_cart');
    if (tg) tg.close();
  });

  document.getElementById('add-address-btn')?.addEventListener('click', openAddAddressModal);
  document.getElementById('address-modal-close')?.addEventListener('click', closeAddAddressModal);
  document.getElementById('address-modal-overlay')?.addEventListener('click', (e) => {
    if (e.target.id === 'address-modal-overlay') closeAddAddressModal();
  });
  document.getElementById('save-modal-address-btn')?.addEventListener('click', saveModalAddress);
  document.getElementById('link-telegram-btn')?.addEventListener('click', linkTelegramAccount);

  // Apply coupon with real backend validation
  document.getElementById('apply-coupon-btn')?.addEventListener('click', async () => {
    const code = document.getElementById('coupon-input').value.trim().toUpperCase();
    const msg = document.getElementById('coupon-msg');
    if (!code) return;
    try {
      const res = await apiFetch('/coupons/validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ coupon_code: code })
      });
      if (res.valid) {
        State.appliedCoupon = res.coupon_code;
        msg.textContent = `✓ Coupon "${res.coupon_code}" applied successfully!`;
        msg.className = 'coupon-msg success';
        msg.classList.remove('hidden');
        
        const couponRow = document.getElementById('coupon-row');
        if (couponRow) couponRow.style.display = '';
        const couponName = document.getElementById('coupon-name');
        if (couponName) couponName.textContent = res.coupon_code;
        const couponSaving = document.getElementById('coupon-saving');
        if (couponSaving) couponSaving.textContent = `Applied`;
        
        updatePriceSummary();
      }
    } catch (e) {
      State.appliedCoupon = null;
      msg.textContent = e.message || 'Invalid coupon code or eligibility.';
      msg.className = 'coupon-msg error';
      msg.classList.remove('hidden');
      
      const couponRow = document.getElementById('coupon-row');
      if (couponRow) couponRow.style.display = 'none';
      
      updatePriceSummary();
    }
  });

  // Payment method selection
  document.querySelectorAll('.payment-opt').forEach(opt => {
    opt.addEventListener('click', () => {
      document.querySelectorAll('.payment-opt').forEach(o => o.classList.remove('active'));
      opt.classList.add('active');
    });
  });

  // Auto-fill phone from profile
  if (State.user?.phone) {
    const phoneEl = document.getElementById('delivery-phone');
    if (phoneEl && !phoneEl.value) phoneEl.value = State.user.phone;
  }
}

// =====================================================
// MAIN INIT
// =====================================================
async function initApp() {
  initTelegram();

  // Animate splash loader
  await new Promise(r => setTimeout(r, 1500));

  // Login
  const loggedIn = await login();
  if (!loggedIn) {
    const splash = document.getElementById('splash-screen');
    if (splash) {
      splash.innerHTML = `
        <div style="text-align:center;padding:40px;color:var(--text-secondary)">
          <div style="font-size:48px;margin-bottom:16px">⚠️</div>
          <h2 style="color:var(--text-primary);margin-bottom:8px">Authentication Required</h2>
          <p>Please open this app through Telegram.</p>
        </div>`;
    }
    return;
  }

  // Load everything in parallel
  await Promise.all([
    loadConfig(),
    loadProducts(),
    loadNotifications(),
  ]);

  loadCartFromStorage();
  renderUserInfo();
  renderCartItems();
  updateCartBadge();
  updatePriceSummary();

  // Auto-detect location
  detectLocation();

  // Connect WebSocket for real-time updates
  if (State.user?.id) {
    connectWebSocket(State.user.id);
  }

  // Also connect SSE as fallback
  initSSE();

  // Setup all event listeners
  setupEventListeners();

  // Show app, hide splash
  document.getElementById('splash-screen')?.classList.add('fade-out');
  document.getElementById('app')?.classList.remove('hidden');
  document.body?.classList.remove('loading-state');

  setTimeout(() => {
    const splash = document.getElementById('splash-screen');
    if (splash) splash.style.display = 'none';
  }, 400);

  // Auto-track order if URL parameter is present
  const params = new URLSearchParams(window.location.search);
  const trackOrderId = params.get('track');
  if (trackOrderId) {
    setTimeout(() => {
      openTracker(trackOrderId);
    }, 600);
  }

  console.log('🍕 Domino\'s Order Engine App v2.0 initialized!');
}

// Start the app
document.addEventListener('DOMContentLoaded', initApp);
