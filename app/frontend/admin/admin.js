/**
 * Domino's Order Engine — Admin Dashboard v2.0
 * Full-featured admin panel with real-time updates
 */

'use strict';

// =====================================================
// STATE
// =====================================================
const AdminState = {
  token: null,
  admin: null,
  orders: [],
  products: [],
  users: [],
  giftcards: [],
  supportUsers: [],
  activeSupportUser: null,
  config: {},
  analyticsCharts: {},
  revenueChart: null,
  statusChart: null,
  sseSource: null,
  currentSection: 'overview',
  useManualOtpEndpoint: false,
};

const API = '/api';

function handleUnauthenticatedSession() {
  AdminState.token = null;
  AdminState.admin = null;
  sessionStorage.removeItem('admin_token');
  sessionStorage.removeItem('admin_user');
  localStorage.removeItem('adminToken');
  
  if (AdminState.sseSource) {
    try { AdminState.sseSource.close(); } catch(e) {}
    AdminState.sseSource = null;
  }
  if (AdminState.connectTimeout) {
    try { clearTimeout(AdminState.connectTimeout); } catch(e) {}
    AdminState.connectTimeout = null;
  }
  
  const loginScreen = document.getElementById('login-screen');
  if (loginScreen) {
    loginScreen.style.display = 'flex';
  }
  const dashboard = document.getElementById('dashboard');
  if (dashboard) {
    dashboard.classList.add('hidden');
  }
}

const pendingRequests = new Map();

// =====================================================
// API HELPERS
// =====================================================
async function adminFetch(path, opts = {}) {
  const method = opts.method || 'GET';
  const key = `${method}:${path}`;
  if (pendingRequests.has(key)) {
    return pendingRequests.get(key);
  }
  const promise = (async () => {
    try {
      const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
      if (AdminState.token) headers['Authorization'] = `Bearer ${AdminState.token}`;
      const res = await fetch(`${API}${path}`, { ...opts, headers });
      if (res.status === 401 || res.status === 403) {
        handleUnauthenticatedSession();
        throw new Error('Session expired. Please sign in again.');
      }
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: 'Request failed' }));
        let errMsg = 'Request failed';
        if (err.detail) {
          if (Array.isArray(err.detail)) {
            errMsg = err.detail.map(d => {
              const field = d.loc ? d.loc.join('.') : 'field';
              return `${field}: ${d.msg}`;
            }).join(', ');
          } else if (typeof err.detail === 'object') {
            errMsg = JSON.stringify(err.detail);
          } else {
            errMsg = String(err.detail);
          }
        }
        throw new Error(errMsg);
      }
      return res.json();
    } finally {
      pendingRequests.delete(key);
    }
  })();
  pendingRequests.set(key, promise);
  return promise;
}

async function adminFetchForm(path, formData) {
  const key = `POST_FORM:${path}`;
  if (pendingRequests.has(key)) {
    return pendingRequests.get(key);
  }
  const promise = (async () => {
    try {
      const headers = {};
      if (AdminState.token) headers['Authorization'] = `Bearer ${AdminState.token}`;
      const res = await fetch(`${API}${path}`, { method: 'POST', headers, body: formData });
      if (res.status === 401 || res.status === 403) {
        handleUnauthenticatedSession();
        throw new Error('Session expired. Please sign in again.');
      }
      if (!res.ok) {
        let errMsg = 'Request failed';
        try {
          const err = await res.json();
          if (err.detail) {
            if (Array.isArray(err.detail)) {
              errMsg = err.detail.map(d => {
                const field = d.loc ? d.loc.join('.') : 'field';
                return `${field}: ${d.msg}`;
              }).join(', ');
            } else if (typeof err.detail === 'object') {
              errMsg = JSON.stringify(err.detail);
            } else {
              errMsg = String(err.detail);
            }
          }
        } catch (_) {
          try {
            errMsg = await res.text() || 'Request failed';
          } catch (__) {}
        }
        throw new Error(errMsg);
      }
      return res.json();
    } finally {
      pendingRequests.delete(key);
    }
  })();
  pendingRequests.set(key, promise);
  return promise;
}

// =====================================================
// LOGIN
// =====================================================
document.getElementById('login-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const username = document.getElementById('admin-username').value.trim();
  const password = document.getElementById('admin-password').value.trim();
  const btnText = document.getElementById('login-btn-text');
  const errorEl = document.getElementById('login-error');
  btnText.textContent = 'Signing in...';



  try {
    const data = await fetch(`${API}/admin/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
      credentials: 'include'
    }).then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e)));

    AdminState.token = data.access_token;
    AdminState.admin = data.user;
    sessionStorage.setItem('admin_token', data.access_token);
    sessionStorage.setItem('admin_user', JSON.stringify(data.user));

    errorEl.classList.add('hidden');
    showDashboard();
  } catch (err) {
    errorEl.textContent = err.detail || err.message || 'Invalid credentials';
    errorEl.classList.remove('hidden');
    btnText.textContent = 'Sign In';
  }
});

function showDashboard() {
  document.getElementById('login-screen').style.display = 'none';
  document.getElementById('dashboard').classList.remove('hidden');
  document.getElementById('sidebar-admin-name').textContent =
    AdminState.admin?.display_name || AdminState.admin?.username || 'Admin';
  initDashboard();
}

// =====================================================
// DASHBOARD INIT
// =====================================================
async function initDashboard() {
  setupSidebarNav();
  setupClockTick();
  initSSE();
  await Promise.all([
    loadConfig(),
    loadProducts(),
    loadOverview(),
    loadOrders(),
    loadUsers(),
    loadGiftCards(),
  ]);
  renderOverviewCharts();
  setupAllEventListeners();

  // Removed 30-second auto-refresh timer to prevent unnecessary background requests
}

function setupClockTick() {
  const el = document.getElementById('topbar-time');
  const tick = () => {
    if (el) el.textContent = new Date().toLocaleTimeString('en-IN', { hour12: false });
  };
  tick();
  setInterval(tick, 1000);
}

// =====================================================
// REAL-TIME SSE - Full Live Engine
// =====================================================

const LiveFeed = { events: [], maxEvents: 50 };
const DominosTracker = {};

function initSSE() {
  let retryDelay = 2000;
  let sseFailCount = 0;
  let activePollTimeout = null;
  let lastEventTime = Date.now() / 1000.0;

  function stopPolling() {
    if (activePollTimeout) { clearTimeout(activePollTimeout); activePollTimeout = null; }
  }

  // Fallback: poll via fetch when EventSource is broken (e.g. HTTP/2 on Serveo)
  async function startPolling() {
    if (!AdminState.token) return;
    if (activePollTimeout) return;
    updateSSEIndicator(false, 'Polling');
    
    async function pollOnce() {
      if (!AdminState.token) {
        stopPolling();
        return;
      }
      try {
        const res = await fetch(`/api/events/poll?since=${lastEventTime}`, {
          headers: { 'Authorization': `Bearer ${AdminState.token}` }
        });
        if (res.status === 401 || res.status === 403) {
          handleUnauthenticatedSession();
          stopPolling();
          return;
        }
        if (res.ok) {
          const serverTime = res.headers.get('X-Server-Time');
          if (serverTime) {
            lastEventTime = parseFloat(serverTime);
          } else {
            lastEventTime = Date.now() / 1000.0;
          }
          const events = await res.json();
          if (Array.isArray(events) && events.length > 0) {
            events.forEach(handleSSEEvent);
          }
        }
      } catch (e) { /* ignore */ }
      
      // Schedule next poll every 2s after the previous one finishes
      if (activePollTimeout !== null && AdminState.token) {
        activePollTimeout = setTimeout(pollOnce, 2000);
      }
    }

    activePollTimeout = setTimeout(pollOnce, 100);
  }

  function connect() {
    if (!AdminState.token) {
      stopPolling();
      return;
    }
    if (AdminState.connectTimeout) { clearTimeout(AdminState.connectTimeout); AdminState.connectTimeout = null; }
    if (AdminState.sseSource) { AdminState.sseSource.close(); AdminState.sseSource = null; }
    stopPolling();
    const source = new EventSource(`/api/events?token=${encodeURIComponent(AdminState.token || '')}`);
    AdminState.sseSource = source;
    source.onopen = () => {
      retryDelay = 2000;
      sseFailCount = 0;
      stopPolling();
      updateSSEIndicator(true);
    };
    source.onmessage = (evt) => {
      try { 
        lastEventTime = Date.now() / 1000.0;
        handleSSEEvent(JSON.parse(evt.data)); 
      } catch(e) {}
    };
    source.onerror = () => {
      source.close();
      AdminState.sseSource = null;
      sseFailCount++;
      if (sseFailCount > 2) {
        console.warn('SSE connection failed repeatedly. Switching to polling fallback...');
        startPolling();
      } else {
        updateSSEIndicator(false, 'Connecting...');
        if (AdminState.connectTimeout) { clearTimeout(AdminState.connectTimeout); }
        AdminState.connectTimeout = setTimeout(connect, retryDelay);
        retryDelay = Math.min(retryDelay * 2, 30000);
      }
    };
  }

  // Set up manual reconnect click listener on indicator
  const indicator = document.getElementById('realtime-indicator');
  if (indicator) {
    indicator.style.cursor = 'pointer';
    indicator.title = 'Click to manually reconnect live feed';
    indicator.onclick = () => {
      showToast('Attempting to reconnect live feed...', 'info');
      initSSE();
    };
  }

  connect();
}

function updateSSEIndicator(connected, mode = '') {
  const dot = document.getElementById('sse-dot');
  const label = document.getElementById('sse-label');
  if (dot) dot.style.background = connected ? '#22C55E' : mode === 'Polling' ? '#F59E0B' : '#EF4444';
  if (label) label.textContent = connected ? 'Live' : mode === 'Polling' ? 'Polling' : 'Disconnected (Click to retry)';
}

function handleSSEEvent(data) {
  const ts = new Date().toLocaleTimeString('en-IN', { hour12: false });
  if (data.type === 'new_order') {
    pushLiveFeed('new_order', `New order <b>${data.order_id}</b> &mdash; Rs.${(data.total||0).toFixed(0)} from <b>${data.user||'User'}</b>`, ts, data.order_id);
    showToast(`New order ${data.order_id} - Rs.${(data.total||0).toFixed(0)}`, 'success');
    loadOrders().then(() => { highlightOrderRow(data.order_id); loadOverview(); });
    bumpMetric('metric-active-val', 1);
  } else if (data.type === 'order_update') {
    const label = data.order_id ? `Order <b>${data.order_id}</b> &rarr; <b>${data.status || 'updated'}</b>` : 'Orders updated';
    pushLiveFeed('order_update', label, ts, data.order_id || null);
    if (data.order_id && data.status) {
      updateOrderRowInPlace(data.order_id, data.status, data.status_icon);
      showToast(`${data.order_id}: ${data.status}`, 'info');
    }
    // Always do a full refresh so metrics + table + overview stay in sync
    loadOrders().then(() => loadOverview());
   } else if (data.type === 'wallet_update') {
    pushLiveFeed('order_update', `Wallet updated for user ${data.user_id||''}`, ts, null);
    loadOrders().then(() => loadOverview());
    if (AdminState.currentSection === 'users') loadUsers();
  } else if (data.type === 'session_update') {
    pushLiveFeed('session_update', `Domino's sessions updated`, ts, null);
    loadSessions();
    loadOverview();
  } else if (data.type === 'payment_update') {
    pushLiveFeed('payment_update', `Payments table updated`, ts, null);
    if (AdminState.currentSection === 'payments') loadPayments();
    loadOverview();
  } else if (data.type === 'proxy_update') {
    if (AdminState.currentSection === 'proxies') { loadProxies(); loadProxyLogs(); }
  } else if (data.type === 'coupon_update') {
    if (AdminState.currentSection === 'coupons') loadCoupons();
  } else if (data.type === 'giftcard_update') {
    if (AdminState.currentSection === 'giftcards') loadGiftCards();
    loadOverview();
  } else if (data.type === 'robot_log_update') {
    if (AdminState.currentSection === 'robot-logs') loadRobotLogs();
  } else if (data.type === 'dominos_progress') {
    handleDominosProgress(data, ts);
  } else if (data.type === 'error_alert') {
    console.error('[BOT/SERVER ERROR]', data.message);
    pushLiveFeed('error', data.message, ts, null);
    showToast(data.message, 'error');
  } else if (data.type === 'user_login') {
    pushLiveFeed('user_login', `${data.display_name||'User'} just opened the app`, ts, null);
  } else if (data.type === 'new_user') {
    pushLiveFeed('new_user', `New user registered: <b>${data.display_name}</b> (ID: ${data.telegram_id})`, ts, null);
    showToast(`New user registered: ${data.display_name}`, 'info');
    if (AdminState.currentSection === 'users') loadUsers();
    bumpMetric('metric-users-val', 1);
  } else if (data.type === 'dominos_otp_status') {
    // Only process events for the currently active request token
    const storedToken = document.getElementById('session-request-token')?.value;
    if (storedToken && data.request_token && data.request_token !== storedToken) return;

    const statusContainer = document.getElementById('otp-robot-status');
    const statusLog = document.getElementById('otp-status-log');

    // Always update screenshot if present
    if (data.screenshot) {
      const previewContainer = document.getElementById('otp-browser-preview-container');
      const previewImg = document.getElementById('otp-browser-preview');
      if (previewContainer && previewImg) {
        previewImg.src = data.screenshot;  // already has cache-busting ts from server
        previewContainer.classList.remove('hidden');
      }
    }

    if (statusContainer) statusContainer.classList.remove('hidden');

    // screenshot_only events just update the preview — no new log line
    if (data.screenshot_only) return;

    const msg = data.status || '';
    if (!msg || !statusLog) return;

    // Deduplicate: check if last line in log container already contains this message
    const lastLine = statusLog.lastElementChild;
    if (lastLine && lastLine.textContent.includes(msg)) {
      lastLine.textContent = `${new Date().toLocaleTimeString()} — ${msg}`;
      return;
    }


    const isError = msg.startsWith('\u274c') || msg.toLowerCase().includes('error') || msg.toLowerCase().includes('failed');
    const isDone  = msg.startsWith('\ud83d\udcf1') || msg.startsWith('\u2705') || msg.startsWith('\ud83c\udf89')
                 || msg.startsWith('\ud83d\udcac') || msg.toLowerCase().includes('otp sent');

    const line = document.createElement('div');
    line.className = 'otp-log-line';
    line.style.cssText = `font-size:12px;padding:2px 0;line-height:1.5;color:${
      isError ? '#ff6b6b' : isDone ? '#51cf66' : '#b0b8c8'
    };font-weight:${isDone || isError ? '600' : 'normal'}`;
    line.textContent = `${new Date().toLocaleTimeString()} — ${msg}`;
    statusLog.appendChild(line);
    statusLog.scrollTop = statusLog.scrollHeight;

    // Once OTP is sent: ensure inputs are enabled
    if (isDone) {
      const otpInput = document.getElementById('session-otp');
      const verifyBtn = document.getElementById('btn-verify-otp');
      if (otpInput && verifyBtn) {
        otpInput.disabled = false;
        otpInput.placeholder = 'Enter the 6-digit OTP';
        verifyBtn.disabled = false;
        setTimeout(() => otpInput.focus(), 150);
      }
    }
    
    // Auto-close modal and reload sessions list on successful login
    const isSuccess = msg.startsWith('🎉') || msg.toLowerCase().includes('saved successfully') || msg.toLowerCase().includes('saved automatically');
    if (isSuccess) {
      setTimeout(async () => {
        showToast(msg, 'success');
        const modal = document.getElementById('session-otp-modal');
        if (modal) modal.classList.add('hidden');
        await loadSessions();
      }, 1500);
    }
    
    if (msg.toLowerCase().includes('manual fallback')) {
      AdminState.useManualOtpEndpoint = true;
      const otpInput = document.getElementById('session-otp');
      const verifyBtn = document.getElementById('btn-verify-otp');
      if (otpInput && verifyBtn) {
        otpInput.disabled = false;
        otpInput.placeholder = 'Enter OTP for manual injection';
        verifyBtn.disabled = false;
        verifyBtn.textContent = 'Manually Inject OTP';
        setTimeout(() => otpInput.focus(), 150);
      }
    }
  } else if (data.type === 'robot_log') {
    const tbody = document.getElementById('robot-logs-tbody');
    if (tbody) {
      const emptyRow = tbody.querySelector('td[colspan="6"]');
      if (emptyRow) tbody.innerHTML = '';
      
      const l = data.log;
      const levelColor = { INFO: '#22C55E', WARNING: '#F59E0B', ERROR: '#EF4444' };
      const stageBg    = { otp_request:'#3B82F6', browser_launch:'#A855F7', otp_fill:'#F59E0B', session_save:'#22C55E', order_submit:'#FF6B35', error:'#EF4444' };
      const time = new Date(l.created_at).toLocaleString('en-IN', { hour12: false, hour:'2-digit', minute:'2-digit', second:'2-digit', day:'2-digit', month:'short' });
      const details = l.details && Object.keys(l.details).length ? JSON.stringify(l.details, null, 2) : '';
      
      const newRowHtml = `<tr style="border-bottom:1px solid rgba(255,255,255,0.05)">
        <td style="font-size:11px;color:var(--text-muted);white-space:nowrap">${time}</td>
        <td><code style="font-size:11px">${l.mobile_number||'—'}</code></td>
        <td><span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700;background:${levelColor[l.level]||'#888'}22;color:${levelColor[l.level]||'#888'};border:1px solid ${levelColor[l.level]||'#888'}44">${l.level}</span></td>
        <td><span style="display:inline-block;padding:2px 7px;border-radius:8px;font-size:11px;background:${(stageBg[l.stage]||'#666')}22;color:${stageBg[l.stage]||'#aaa'};border:1px solid ${(stageBg[l.stage]||'#666')}33">${l.stage}</span></td>
        <td style="font-size:12px;max-width:320px;word-break:break-word">${l.message}</td>
        <td>${details ? `<button class="btn btn-xs btn-outline" onclick="this.nextSibling.classList.toggle('hidden')">JSON</button><pre class="hidden" style="font-size:10px;background:var(--bg-glass);border-radius:6px;padding:8px;margin-top:4px;max-width:260px;overflow:auto;white-space:pre-wrap">${details}</pre>` : '—'}</td>
      </tr>`;
      tbody.insertAdjacentHTML('afterbegin', newRowHtml);
    }
  }
}

function highlightOrderRow(orderId) {
  document.querySelectorAll('#orders-tbody tr').forEach(row => {
    if (row.textContent.includes(orderId)) {
      row.style.transition = 'background 0.6s';
      row.style.background = 'rgba(255,107,53,0.18)';
      setTimeout(() => { row.style.background = ''; }, 3000);
    }
  });
}

function updateOrderRowInPlace(orderId, status, icon) {
  document.querySelectorAll('#orders-tbody tr, #recent-orders-table tr').forEach(row => {
    if (row.innerHTML.includes(orderId)) {
      const badge = row.querySelector('.badge');
      if (badge) { badge.className = `badge ${statusBadgeClass(status)}`; badge.textContent = `${icon||''} ${status}`; }
      row.style.transition = 'background 0.4s';
      row.style.background = 'rgba(168,85,247,0.12)';
      setTimeout(() => { row.style.background = ''; }, 2500);
      const cached = AdminState.orders.find(o => o.id === orderId);
      if (cached) cached.status = status;
    }
  });
}

function bumpMetric(id, delta) {
  const el = document.getElementById(id);
  if (!el) return;
  const cur = parseInt(el.textContent.replace(/\D/g,''), 10) || 0;
  el.textContent = cur + delta;
  el.style.transition = 'color 0.3s';
  el.style.color = '#22C55E';
  setTimeout(() => { el.style.color = ''; }, 1200);
}

function handleDominosProgress(data, ts) {
  const { order_id, step_label, step_message, step_index, total_steps, progress_pct, is_error } = data;
  const feedMsg = is_error
    ? `Dominos error on ${order_id}: ${step_message}`
    : `Robot ${order_id} - ${step_message} (${progress_pct}%)`;
  pushLiveFeed(is_error ? 'error' : 'dominos', feedMsg, ts, order_id);
  renderDominosCard(order_id, step_label, step_message, step_index, total_steps, progress_pct, is_error);
  const detailTitle = document.getElementById('detail-order-id');
  if (detailTitle && detailTitle.textContent.includes(order_id)) {
    const robotSection = document.getElementById('detail-dominos-status');
    if (robotSection) {
      robotSection.innerHTML = `
        <div style="background:rgba(168,85,247,0.08);border:1px solid rgba(168,85,247,0.3);border-radius:10px;padding:14px;margin-top:10px">
          <div style="font-size:13px;color:#A855F7;font-weight:700;margin-bottom:6px">&#x1F916; Robot - Live</div>
          <div style="font-size:13px;color:var(--text-primary);margin-bottom:8px">${step_message}</div>
          <div style="background:rgba(255,255,255,0.06);border-radius:6px;height:8px;overflow:hidden">
            <div style="height:100%;width:${progress_pct}%;background:linear-gradient(90deg,#A855F7,#FF6B35);transition:width 0.5s;border-radius:6px"></div>
          </div>
          <div style="font-size:11px;color:var(--text-muted);margin-top:5px">${step_index<0?'Failed':`Step ${step_index+1} of ${total_steps}`} - ${progress_pct}%</div>
        </div>`;
    }
  }
}

function renderDominosCard(orderId, stepLabel, stepMessage, stepIndex, totalSteps, progressPct, isError) {
  const container = document.getElementById('robot-activity-container');
  if (!container) return;
  container.querySelector('.no-activity-placeholder')?.remove();
  let card = document.getElementById(`dominos-card-${orderId}`);
  if (!card) {
    card = document.createElement('div');
    card.id = `dominos-card-${orderId}`;
    card.className = 'dominos-card';
    card.innerHTML = `
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
        <span>&#x1F916;</span>
        <span style="font-weight:700;font-size:12px">Order <code style="background:rgba(168,85,247,0.15);padding:1px 5px;border-radius:4px">${orderId}</code></span>
        <span id="dcard-time-${orderId}" style="margin-left:auto;font-size:11px;color:var(--text-muted)"></span>
      </div>
      <div id="dcard-status-${orderId}" style="font-size:13px;margin:8px 0;font-weight:600"></div>
      <div style="background:rgba(255,255,255,0.06);border-radius:6px;height:8px;overflow:hidden;margin-bottom:6px">
        <div id="dcard-bar-${orderId}" style="height:100%;width:0%;border-radius:6px;transition:width 0.6s"></div>
      </div>
      <div id="dcard-step-${orderId}" style="font-size:11px;color:var(--text-muted);margin-bottom:8px"></div>
      <div id="dcard-steps-${orderId}" style="font-size:11px;line-height:2"></div>`;
    container.prepend(card);
    DominosTracker[orderId] = { steps: [] };
  }
  const tracker = DominosTracker[orderId] || { steps: [] };
  if (stepIndex >= 0 && !tracker.steps.includes(stepLabel)) tracker.steps.push(stepLabel);
  DominosTracker[orderId] = tracker;
  const timeEl = document.getElementById(`dcard-time-${orderId}`);
  const statusEl = document.getElementById(`dcard-status-${orderId}`);
  const barEl = document.getElementById(`dcard-bar-${orderId}`);
  const stepEl = document.getElementById(`dcard-step-${orderId}`);
  const stepsListEl = document.getElementById(`dcard-steps-${orderId}`);
  if (timeEl) timeEl.textContent = new Date().toLocaleTimeString('en-IN',{hour12:false});
  if (statusEl) { statusEl.textContent = stepMessage; statusEl.style.color = isError ? '#EF4444' : '#A855F7'; }
  if (barEl) { barEl.style.width = Math.max(2,progressPct)+'%'; barEl.style.background = isError ? 'linear-gradient(90deg,#EF4444,#F97316)' : 'linear-gradient(90deg,#A855F7,#FF6B35)'; }
  if (stepEl) { stepEl.textContent = stepIndex < 0 ? 'Failed' : `Step ${stepIndex+1} / ${totalSteps} - ${progressPct}%`; stepEl.style.color = isError ? '#EF4444' : 'var(--text-muted)'; }
  if (stepsListEl) {
    stepsListEl.innerHTML = tracker.steps.map(s => `<div style="color:#22C55E">&#x2705; ${s}</div>`).join('')
      + (isError ? `<div style="color:#EF4444">&#x274C; ${stepLabel}</div>` : '');
  }
  card.style.borderColor = isError ? 'rgba(239,68,68,0.4)' : 'rgba(168,85,247,0.35)';
  card.style.boxShadow = isError ? '0 0 12px rgba(239,68,68,0.12)' : '0 0 12px rgba(168,85,247,0.12)';
  if (progressPct >= 100) setTimeout(() => { if(card) card.style.opacity = '0.65'; }, 2000);
}

function pushLiveFeed(type, message, ts, orderId) {
  LiveFeed.events.unshift({ type, message, ts, orderId });
  if (LiveFeed.events.length > LiveFeed.maxEvents) LiveFeed.events = LiveFeed.events.slice(0, LiveFeed.maxEvents);
  renderLiveFeed();
  const badge = document.getElementById('robot-badge');
  if (badge) { const cur = parseInt(badge.textContent,10)||0; badge.textContent = cur+1; badge.classList.remove('hidden'); }
}

function renderLiveFeed() {
  const el = document.getElementById('live-feed-list');
  if (!el) return;
  const iconMap = { new_order:'&#x1F195;', order_update:'&#x1F4E6;', dominos:'&#x1F916;', error:'&#x274C;', user_login:'&#x1F464;' };
  el.innerHTML = LiveFeed.events.slice(0,20).map(ev => `
    <div style="display:flex;gap:10px;align-items:flex-start;padding:9px 12px;border-bottom:1px solid rgba(255,255,255,0.04);cursor:${ev.orderId?'pointer':'default'}"
      ${ev.orderId ? `onclick="openOrderDetail('${ev.orderId}')"` : ''}
      onmouseover="this.style.background='rgba(255,255,255,0.03)'" onmouseout="this.style.background=''">
      <span style="font-size:16px;flex-shrink:0">${iconMap[ev.type]||'&#x1F4E1;'}</span>
      <div style="flex:1;min-width:0">
        <div style="font-size:12px;color:var(--text-primary);line-height:1.4">${ev.message}</div>
        <div style="font-size:10px;color:var(--text-muted);margin-top:2px">${ev.ts}</div>
      </div>
    </div>`).join('');
}


// =====================================================
// NAVIGATION
// =====================================================
function setupSidebarNav() {
  document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      switchSection(link.dataset.section);
    });
  });
}

function switchSection(name) {
  AdminState.currentSection = name;
  document.querySelectorAll('.nav-link').forEach(l => l.classList.toggle('active', l.dataset.section === name));
  document.querySelectorAll('.section').forEach(s => s.classList.toggle('active', s.id === `section-${name}`));

  // Clear robot logs timer when switching away
  if (name !== 'robot-logs' && typeof _robotLogsTimer !== 'undefined' && _robotLogsTimer) {
    clearInterval(_robotLogsTimer);
    _robotLogsTimer = null;
  }

  const titles = {
    overview: 'Overview', orders: 'Orders', products: 'Products', users: 'Users',
    giftcards: 'Gift Cards', support: 'Support Chat', analytics: 'Analytics',
    settings: 'Settings', logs: 'Logs', proxies: 'Proxy Manager', robot: 'Robot Live',
    sessions: "Domino's Sessions", payments: 'Payments', qrgenerator: 'QR Generator',
    coupons: 'Promo Codes', 'robot-logs': '🤖 Robot Logs'
  };
  document.getElementById('topbar-title').textContent = titles[name] || name;

  // Section-specific loads
  if (name === 'analytics') loadAnalytics();
  if (name === 'support') loadSupportUsers();
  if (name === 'logs') loadAuditLogs();
  if (name === 'settings') renderSettings();
  if (name === 'proxies') { loadProxies(); loadProxyLogs(); }
  if (name === 'sessions') { loadSessions(); }
  if (name === 'payments') { loadPayments(); }
  if (name === 'coupons') { loadCoupons(); }
  if (name === 'qrgenerator') { loadQRHistory(); }
  if (name === 'robot-logs') { renderRobotLogsSection(); loadRobotLogs(); }
  if (name === 'robot') {
    const badge = document.getElementById('robot-badge');
    if (badge) { badge.textContent = '0'; badge.classList.add('hidden'); }
    renderLiveFeed();
  }
}

// =====================================================
// OVERVIEW
// =====================================================
async function loadOverview() {
  try {
    const data = await adminFetch('/admin/analytics/summary');
    const s = data.summary || {};

    setText('metric-revenue-val', `₹${(s.today_revenue || 0).toFixed(0)}`);
    setText('metric-revenue-sub', `${s.today_orders || 0} orders today`);
    setText('metric-active-val', s.active_orders || 0);
    setText('metric-users-val', s.total_users || 0);
    setText('metric-failed-val', s.cancelled_today || 0);

    // Update active orders badge in sidebar
    const badgeEl = document.getElementById('active-orders-badge');
    if (badgeEl) {
      const count = s.active_orders || 0;
      badgeEl.textContent = count;
      badgeEl.classList.toggle('hidden', count === 0);
    }

    // Charts
    renderOverviewCharts(data);

    // Recent orders table
    renderRecentOrdersTable();

  } catch (e) {
    console.error('Overview load failed:', e);
  }
}

function renderOverviewCharts(data = {}) {
  // Revenue Chart
  const revCtx = document.getElementById('revenue-chart')?.getContext('2d');
  if (revCtx) {
    const labels = (data.daily_revenue || []).map(d => d.date?.slice(5) || '');
    const values = (data.daily_revenue || []).map(d => d.revenue || 0);

    if (AdminState.revenueChart) AdminState.revenueChart.destroy();
    AdminState.revenueChart = new Chart(revCtx, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'Revenue (₹)',
          data: values,
          borderColor: '#FF6B35',
          backgroundColor: 'rgba(255,107,53,0.08)',
          fill: true,
          tension: 0.4,
          pointBackgroundColor: '#FF6B35',
          pointRadius: 3,
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#9090B0', font: { size: 11 } }, grid: { color: 'rgba(255,255,255,0.04)' } },
          y: { ticks: { color: '#9090B0', font: { size: 11 }, callback: v => `₹${v}` }, grid: { color: 'rgba(255,255,255,0.04)' } }
        }
      }
    });
  }

  // Status chart
  const statusCtx = document.getElementById('status-chart')?.getContext('2d');
  if (statusCtx) {
    const dist = data.status_distribution || {};
    const labels = Object.keys(dist);
    const values = Object.values(dist);
    const colors = ['#FF6B35', '#A855F7', '#22C55E', '#3B82F6', '#EAB308', '#EF4444'];

    if (AdminState.statusChart) AdminState.statusChart.destroy();
    AdminState.statusChart = new Chart(statusCtx, {
      type: 'doughnut',
      data: {
        labels,
        datasets: [{ data: values, backgroundColor: colors.slice(0, labels.length), borderWidth: 0 }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { labels: { color: '#9090B0', font: { size: 11 } } }
        },
        cutout: '70%',
      }
    });
  }
}

function renderRecentOrdersTable() {
  const recent = (AdminState.orders || []).slice(0, 8);
  const tbody = document.getElementById('recent-orders-table');
  if (!tbody) return;
  tbody.innerHTML = `
    <table class="admin-table">
      <thead><tr><th>Order ID</th><th>Customer</th><th>Total</th><th>Status</th><th>Date</th><th>Actions</th></tr></thead>
      <tbody>${recent.map(o => orderTableRow(o)).join('')}</tbody>
    </table>`;
}

// =====================================================
// ORDERS
// =====================================================
async function loadOrders() {
  try {
    const data = await adminFetch('/admin/dashboard');
    AdminState.orders = data.orders || [];
    AdminState.users = data.users || [];
    renderOrdersTable();
    renderUsersTable();
  } catch (e) { console.error('Orders load failed:', e); }
}

function renderOrdersTable() {
  const tbody = document.getElementById('orders-tbody');
  if (!tbody) return;

  const search = document.getElementById('order-search')?.value.toLowerCase() || '';
  const statusFilter = document.getElementById('order-status-filter')?.value || '';

  let orders = AdminState.orders;
  if (search) orders = orders.filter(o => o.id?.toLowerCase().includes(search) || o.user_display_name?.toLowerCase().includes(search));
  if (statusFilter) orders = orders.filter(o => o.status === statusFilter);

  tbody.innerHTML = orders.map(o => orderTableRow(o)).join('');
}

function orderTableRow(o) {
  const date = new Date(o.created_at).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
  const badgeClass = statusBadgeClass(o.status);
  const items = (o.items || []).slice(0, 2).map(i => `${i.quantity}×${i.product_name || 'Item'}`).join(', ');
  const tgId = o.user?.telegram_id ? `<div style="font-size:10px;color:var(--text-muted)">TG: ${o.user.telegram_id}</div>` : '';

  return `<tr onclick="openOrderDetail('${o.id}')">
    <td><code style="font-size:11px;color:var(--text-muted)">${o.id}</code></td>
    <td>
      <div class="user-cell">
        <div class="user-cell-avatar">${(o.user_display_name || 'U')[0]}</div>
        <div><span>${o.user_display_name || '—'}</span>${tgId}</div>
      </div>
    </td>
    <td>${items}${(o.items || []).length > 2 ? ' ...' : ''}</td>
    <td><b>₹${o.total_payable}</b></td>
    <td><span class="badge ${badgeClass}">${o.status}</span></td>
    <td>${date}</td>
    <td><div class="action-btns" onclick="event.stopPropagation()">
      <button class="btn btn-xs btn-outline" onclick="openOrderDetail('${o.id}')">View</button>
    </div></td>
  </tr>`;
}

function statusBadgeClass(status) {
  const map = {
    // Real DB status strings from bot.py
    'Pending Payment':       'badge-pending',
    'Pending Verification':  'badge-pending',
    'Payment Pending':       'badge-pending',
    'Payment Received':      'badge-processing',
    'Order Processing':      'badge-processing',
    'Order Placed':          'badge-processing',
    'Preparing':             'badge-preparing',
    'Baking':                'badge-preparing',
    'Out for Delivery':      'badge-delivery',
    'On the Way':            'badge-delivery',
    'Delivered':             'badge-delivered',
    'Completed':             'badge-completed',
    'Cancelled':             'badge-cancelled',
    'Failed':                'badge-cancelled',
    'Refunded':              'badge-cancelled',
  };
  return map[status] || 'badge-processing';
}

// =====================================================
// ORDER DETAIL PANEL
// =====================================================
async function openOrderDetail(orderId) {
  const order = AdminState.orders.find(o => o.id === orderId);
  if (!order) return;

  document.getElementById('detail-order-id').textContent = `Order ${orderId}`;
  const body = document.getElementById('detail-panel-body');
  body.innerHTML = `<div style="padding:20px;text-align:center;color:var(--text-muted)">Loading...</div>`;

  // Show panel
  document.getElementById('order-detail-panel').classList.remove('hidden');
  document.getElementById('order-panel-overlay').classList.remove('hidden');

  try {
    const fullOrder = await adminFetch(`/admin/orders/${orderId}/detail`);
    renderOrderDetailPanel(fullOrder);
  } catch (e) {
    body.innerHTML = `<p style="color:var(--error)">${e.message}</p>`;
  }
}

function closeOrderDetail() {
  document.getElementById('order-detail-panel').classList.add('hidden');
  document.getElementById('order-panel-overlay').classList.add('hidden');
}

function renderOrderDetailPanel(order) {
  const body = document.getElementById('detail-panel-body');

  const statusSteps = [
    { status: 'Payment Received', icon: '✅' },
    { status: 'Order Processing', icon: '📋' },
    { status: 'Preparing', icon: '👨‍🍳' },
    { status: 'Out for Delivery', icon: '🛵' },
    { status: 'Delivered', icon: '🎉' },
  ];

  const statusOrder = statusSteps.map(s => s.status);
  const currentIdx = statusOrder.indexOf(order.status);
  const histMap = {};
  (order.status_history || []).forEach(h => { histMap[h.status] = h.timestamp; });

  const availableStatuses = [
    'Payment Received', 'Order Processing', 'Preparing', 'Out for Delivery', 'Delivered', 'Cancelled'
  ];

  body.innerHTML = `
    <!-- Status Badge & Update -->
    <div class="detail-section">
      <h4>Order Status</h4>
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
        <span class="badge ${statusBadgeClass(order.status)}">${order.status}</span>
        <select id="detail-status-select" class="form-control select-control" style="flex:1">
          ${availableStatuses.map(s => `<option value="${s}" ${s === order.status ? 'selected' : ''}>${s}</option>`).join('')}
        </select>
        <button class="btn btn-primary btn-sm" onclick="updateOrderStatusFromDetail('${order.id}')">Update</button>
      </div>
    </div>

    <!-- Order Info -->
    <div class="detail-section">
      <h4>Order Info</h4>
      <div class="detail-row"><span class="detail-row-label">Order ID</span><code class="detail-row-value">${order.id}</code></div>
      <div class="detail-row"><span class="detail-row-label">Transaction</span><code class="detail-row-value">${order.transaction_id || '—'}</code></div>
      <div class="detail-row"><span class="detail-row-label">Payment</span><span class="detail-row-value">${order.payment_method}</span></div>
      <div class="detail-row"><span class="detail-row-label">Date</span><span class="detail-row-value">${new Date(order.created_at).toLocaleString('en-IN')}</span></div>
      ${order.coupon_applied ? `<div class="detail-row"><span class="detail-row-label">Coupon</span><span class="detail-row-value">${order.coupon_applied}</span></div>` : ''}
      <button class="btn btn-outline btn-sm w-100" style="margin-top:12px; display:flex; align-items:center; justify-content:center; gap:8px" onclick="downloadReceiptPDF('${order.id}')">
        <i class="fa-solid fa-file-pdf"></i> Download PDF Receipt
      </button>
    </div>

    <!-- Customer -->
    <div class="detail-section">
      <h4>Customer</h4>
      <div class="detail-row"><span class="detail-row-label">Name</span><span class="detail-row-value">${order.user?.display_name || '—'}</span></div>
      ${order.user?.telegram_id ? `<div class="detail-row"><span class="detail-row-label">Telegram ID</span><span class="detail-row-value"><code>${order.user.telegram_id}</code></span></div>` : ''}
      ${order.user?.username ? `<div class="detail-row"><span class="detail-row-label">Username</span><span class="detail-row-value">@${order.user.username}</span></div>` : ''}
      <div class="detail-row"><span class="detail-row-label">Phone</span><span class="detail-row-value">${order.phone || '—'}</span></div>
      <div class="detail-row"><span class="detail-row-label">Address</span><span class="detail-row-value">${order.address || '—'}</span></div>
      ${order.landmark ? `<div class="detail-row"><span class="detail-row-label">Landmark</span><span class="detail-row-value">${order.landmark}</span></div>` : ''}
      ${order.city ? `<div class="detail-row"><span class="detail-row-label">City</span><span class="detail-row-value">${order.city}</span></div>` : ''}
      ${(order.latitude && order.longitude) ? `<div class="detail-row"><span class="detail-row-label">Coordinates</span><span class="detail-row-value"><a href="https://maps.google.com/?q=${order.latitude},${order.longitude}" target="_blank" style="color:var(--primary)">📍 ${order.latitude.toFixed(5)}, ${order.longitude.toFixed(5)}</a></span></div>` : ''}
    </div>

    <!-- Items -->
    <div class="detail-section">
      <h4>Items</h4>
      ${(order.items || []).map(item => `
        <div class="detail-item">
          ${item.image_url ? `<img class="detail-item-img" src="${item.image_url}" alt="" />` : `<div class="detail-item-img" style="display:flex;align-items:center;justify-content:center;font-size:28px">🍕</div>`}
          <div>
            <div class="detail-item-name">${item.product_name || item.name}</div>
            <div style="font-size:11px;color:var(--text-muted)">₹${item.price} each</div>
          </div>
          <div class="detail-item-qty">×${item.quantity}</div>
          <div class="detail-item-price">₹${item.price * item.quantity}</div>
        </div>`).join('')}
      <!-- Totals -->
      <div class="detail-row" style="margin-top:8px"><span class="detail-row-label">Subtotal</span><span>₹${order.original_total?.toFixed(2)}</span></div>
      ${(order.discount > 0) ? `<div class="detail-row"><span class="detail-row-label">Coupon Discount</span><span style="color:var(--success)">-₹${order.discount?.toFixed(2)}</span></div>` : ''}
      ${(order.gift_card?.value > 0) ? `<div class="detail-row"><span class="detail-row-label">🎁 Gift Card</span><span style="color:var(--success)">-₹${order.gift_card.value?.toFixed(2)}</span></div>` : ''}
      <div class="detail-row"><span class="detail-row-label">Delivery Fee</span><span>₹${order.delivery_charge?.toFixed(2) || '0.00'}</span></div>
      <div class="detail-row"><span class="detail-row-label">Service Charge</span><span>₹${order.service_charge?.toFixed(2) || '0.00'}</span></div>
      <div class="detail-row" style="font-weight:700;font-size:15px;border-top:1px solid rgba(255,255,255,0.1);padding-top:8px"><span>Total Payable</span><span>₹${order.total_payable?.toFixed(2)}</span></div>
    </div>

    <!-- Rider Assignment -->
    <div class="detail-section">
      <h4>Rider Assignment</h4>
      <div class="rider-assign-form">
        <input type="text" id="rider-name-input" class="form-control" placeholder="Rider Name" value="${order.rider?.name || ''}" />
        <input type="text" id="rider-phone-input" class="form-control" placeholder="Rider Phone" value="${order.rider?.phone || ''}" />
        <input type="text" id="rider-vehicle-input" class="form-control" placeholder="Vehicle Number" value="${order.rider?.vehicle || ''}" />
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
          <input type="number" id="rider-lat-input" class="form-control" placeholder="Lat" step="0.000001" value="${order.rider?.lat || ''}" />
          <input type="number" id="rider-lng-input" class="form-control" placeholder="Lng" step="0.000001" value="${order.rider?.lng || ''}" />
        </div>
        <button class="btn btn-primary btn-sm" onclick="assignRider('${order.id}')">
          ${order.rider?.assigned ? '✏️ Update Rider' : '+ Assign Rider'}
        </button>
      </div>
    </div>

    <!-- Status Timeline -->
    <div class="detail-section">
      <h4>Timeline</h4>
      <div class="detail-timeline">
        ${statusSteps.map((step, idx) => {
          const isDone = idx < currentIdx;
          const isCurrent = idx === currentIdx;
          const time = histMap[step.status];
          const timeStr = time ? new Date(time).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }) : '';
          return `
            <div class="detail-timeline-step ${isDone ? 'done' : isCurrent ? 'current' : ''}">
              <div class="detail-timeline-dot">${step.icon}</div>
              <div class="detail-timeline-content">
                <div class="detail-timeline-label">${step.status}</div>
                ${timeStr ? `<div class="detail-timeline-time">${timeStr}</div>` : ''}
              </div>
            </div>`;
        }).join('')}
      </div>
    </div>

    <!-- Admin Note -->
    <div class="detail-section">
      <h4>Add Note</h4>
      <div style="display:flex;gap:8px">
        <input type="text" id="order-note-input" class="form-control" placeholder="Internal note..." />
        <button class="btn btn-outline btn-sm" onclick="addOrderNote('${order.id}')">Add</button>
      </div>
      ${(order.notes || []).length ? `
        <div style="margin-top:10px;display:flex;flex-direction:column;gap:6px">
          ${(order.notes || []).map(n => `
            <div style="font-size:12px;background:var(--bg-glass);border-radius:6px;padding:8px 12px">
              <div style="color:var(--text-muted);margin-bottom:2px">${n.admin} · ${n.at?.slice(0,16)}</div>
              <div>${n.note}</div>
            </div>`).join('')}
        </div>` : ''}
    </div>

    <!-- Cancel Button -->
    ${!['Delivered', 'Completed', 'Cancelled'].includes(order.status) ? `
      <button class="btn btn-danger btn-full" onclick="cancelOrderAdmin('${order.id}')">Cancel Order</button>
    ` : ''}
  `;
}

async function updateOrderStatusFromDetail(orderId) {
  const status = document.getElementById('detail-status-select').value;
  try {
    await adminFetch(`/admin/orders/${orderId}/status`, {
      method: 'PUT',
      body: JSON.stringify({ status })
    });
    showToast(`Status updated: ${status}`, 'success');
    await loadOrders();
    openOrderDetail(orderId);
  } catch (e) {
    showToast(e.message, 'error');
  }
}

async function assignRider(orderId) {
  const payload = {
    rider_name: document.getElementById('rider-name-input').value.trim(),
    rider_phone: document.getElementById('rider-phone-input').value.trim(),
    vehicle_number: document.getElementById('rider-vehicle-input').value.trim(),
    rider_lat: parseFloat(document.getElementById('rider-lat-input').value) || null,
    rider_lng: parseFloat(document.getElementById('rider-lng-input').value) || null,
  };
  if (!payload.rider_name || !payload.rider_phone) {
    showToast('Rider name and phone required', 'error');
    return;
  }
  try {
    await adminFetch(`/admin/orders/${orderId}/assign-rider`, { method: 'POST', body: JSON.stringify(payload) });
    showToast('Rider assigned!', 'success');
    openOrderDetail(orderId);
  } catch (e) {
    showToast(e.message, 'error');
  }
}

async function addOrderNote(orderId) {
  const note = document.getElementById('order-note-input').value.trim();
  if (!note) return;
  try {
    await adminFetch(`/admin/orders/${orderId}/note`, { method: 'POST', body: JSON.stringify({ note }) });
    document.getElementById('order-note-input').value = '';
    showToast('Note added', 'success');
    openOrderDetail(orderId);
  } catch (e) {
    showToast(e.message, 'error');
  }
}

async function cancelOrderAdmin(orderId) {
  if (!confirm(`Cancel order ${orderId}?`)) return;
  try {
    await adminFetch(`/admin/orders/${orderId}/status`, {
      method: 'PUT',
      body: JSON.stringify({ status: 'Cancelled' })
    });
    showToast('Order cancelled', 'success');
    closeOrderDetail();
    await loadOrders();
  } catch (e) {
    showToast(e.message, 'error');
  }
}

// =====================================================
// PRODUCTS
// =====================================================
async function loadProducts() {
  try {
    AdminState.products = await adminFetch('/products');
    renderProductsGrid();
  } catch (e) { console.error('Products load failed:', e); }
}

function renderProductsGrid() {
  const grid = document.getElementById('products-grid');
  if (!grid) return;
  grid.innerHTML = AdminState.products.map(p => `
    <div class="product-admin-card">
      ${p.image_url
        ? `<img class="product-admin-img" src="${p.image_url}" alt="${p.name}" style="display:block" />`
        : `<div class="product-admin-img">🍕</div>`}
      <div class="product-admin-body">
        <div class="product-admin-name">${p.name}</div>
        <div class="product-admin-category">${p.category}</div>
        <div style="display:flex;align-items:baseline;gap:6px">
          <span class="product-admin-price">₹${p.discounted_price ?? p.original_price}</span>
          ${p.discounted_price ? `<span class="product-admin-original">₹${p.original_price}</span>` : ''}
        </div>
        <div class="product-admin-footer">
          <span class="product-status-badge ${p.availability ? 'available' : 'unavailable'}">${p.availability ? '✓ Available' : '✕ Hidden'}</span>
          <div class="action-btns">
            <button class="btn btn-xs btn-outline" onclick="openEditProduct('${p.id}')">Edit</button>
            <button class="btn btn-xs btn-danger" onclick="deleteProduct('${p.id}')">Del</button>
          </div>
        </div>
      </div>
    </div>`).join('');
}

function openAddProduct() {
  document.getElementById('product-form-title').textContent = 'Add Product';
  document.getElementById('product-id').value = '';
  document.getElementById('product-form').reset();
  document.getElementById('product-form-backdrop').classList.remove('hidden');
}

function openEditProduct(productId) {
  const p = AdminState.products.find(prod => prod.id === productId);
  if (!p) return;
  document.getElementById('product-form-title').textContent = 'Edit Product';
  document.getElementById('product-id').value = p.id;
  document.getElementById('pf-name').value = p.name;
  document.getElementById('pf-category').value = p.category;
  document.getElementById('pf-description').value = p.description || '';
  document.getElementById('pf-original-price').value = p.original_price;
  document.getElementById('pf-discounted-price').value = p.discounted_price || '';
  document.getElementById('pf-sort-order').value = p.sort_order || 0;
  document.getElementById('pf-is-veg').checked = p.is_veg;
  document.getElementById('pf-is-popular').checked = p.is_popular;
  document.getElementById('pf-is-recommended').checked = p.is_recommended;
  document.getElementById('pf-image-url').value = p.image_url || '';
  try { document.getElementById('pf-crust-options').value = p.crust_options ? JSON.parse(p.crust_options).join(', ') : ''; } catch {}
  try { document.getElementById('pf-size-options').value = p.size_options ? JSON.parse(p.size_options).join(', ') : ''; } catch {}
  document.getElementById('product-form-backdrop').classList.remove('hidden');
}

function closeProductForm() {
  document.getElementById('product-form-backdrop').classList.add('hidden');
}

async function deleteProduct(productId) {
  if (!confirm('Delete this product?')) return;
  try {
    await adminFetch(`/products/${productId}`, { method: 'DELETE' });
    showToast('Product deleted', 'success');
    await loadProducts();
  } catch (e) { showToast(e.message, 'error'); }
}

// Product form submit
document.getElementById('product-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const productId = document.getElementById('product-id').value;
  const formData = new FormData();
  formData.append('name', document.getElementById('pf-name').value);
  formData.append('description', document.getElementById('pf-description').value);
  formData.append('category', document.getElementById('pf-category').value);
  formData.append('is_veg', document.getElementById('pf-is-veg').checked);
  formData.append('original_price', document.getElementById('pf-original-price').value);
  const discPrice = document.getElementById('pf-discounted-price').value;
  if (discPrice) formData.append('discounted_price', discPrice);
  formData.append('sort_order', document.getElementById('pf-sort-order').value || 0);
  formData.append('is_popular', document.getElementById('pf-is-popular').checked);
  formData.append('is_recommended', document.getElementById('pf-is-recommended').checked);
  formData.append('availability', true);

  // Parse crust/size options
  const crusts = document.getElementById('pf-crust-options').value.split(',').map(s => s.trim()).filter(Boolean);
  if (crusts.length) formData.append('crust_options', JSON.stringify(crusts));
  const sizes = document.getElementById('pf-size-options').value.split(',').map(s => s.trim()).filter(Boolean);
  if (sizes.length) formData.append('size_options', JSON.stringify(sizes));

  // Image
  const imageFile = document.getElementById('pf-image').files[0];
  const imageUrl = document.getElementById('pf-image-url').value.trim();
  if (imageFile) formData.append('image', imageFile);
  if (!imageFile && imageUrl) formData.append('image_url', imageUrl);

  const saveBtn = document.getElementById('save-product-btn');
  saveBtn.textContent = 'Saving...';
  saveBtn.disabled = true;

  try {
    const headers = {};
    if (AdminState.token) headers['Authorization'] = `Bearer ${AdminState.token}`;
    const url = productId ? `${API}/products/${productId}` : `${API}/products`;
    const method = productId ? 'PUT' : 'POST';
    const res = await fetch(url, { method, headers, body: formData });
    if (!res.ok) throw new Error(await res.text());
    showToast(`Product ${productId ? 'updated' : 'created'}!`, 'success');
    closeProductForm();
    await loadProducts();
  } catch (e) {
    showToast('Failed: ' + e.message, 'error');
  } finally {
    saveBtn.textContent = 'Save Product';
    saveBtn.disabled = false;
  }
});

// =====================================================
// USERS
// =====================================================
async function loadUsers() {
  try {
    const data = await adminFetch('/admin/users');
    AdminState.users = data;
    renderUsersTable();
  } catch (e) {
    console.error('Users load failed:', e);
  }
}

function renderUsersTable() {
  const tbody = document.getElementById('users-tbody');
  if (!tbody) return;
  const search = document.getElementById('user-search')?.value.toLowerCase() || '';
  const users = AdminState.users.filter(u =>
    !search || u.display_name?.toLowerCase().includes(search) || u.username?.toLowerCase().includes(search)
  );
  tbody.innerHTML = users.map(u => `
    <tr onclick="openUserDetail('${u.id}')" style="cursor:pointer">
      <td>
        <div class="user-cell">
          <div class="user-cell-avatar">${(u.display_name || 'U')[0]}</div>
          <div>
            <div style="font-weight:600">${u.display_name || '—'}</div>
            <div style="font-size:11px;color:var(--text-muted)">${u.username ? '@' + u.username : 'ID: ' + u.telegram_id}</div>
          </div>
        </div>
      </td>
      <td><code style="font-size:11px">${u.telegram_id}</code></td>
      <td>${u.phone || '—'}</td>
      <td><strong>₹${(u.wallet_balance || 0).toFixed(2)}</strong></td>
      <td><span class="badge ${u.role === 'admin' ? 'badge-processing' : 'badge-available'}">${u.role}</span></td>
      <td style="color:var(--text-muted);font-size:12px">${u.created_at ? new Date(u.created_at).toLocaleDateString('en-IN') : '—'}</td>
      <td><span class="badge ${u.is_blocked ? 'badge-cancelled' : 'badge-available'}">${u.is_blocked ? 'Blocked' : 'Active'}</span></td>
      <td>
        <button class="btn btn-xs btn-outline" style="font-weight:600; cursor:pointer;" onclick="event.stopPropagation(); openUserSessionsModal('${u.id}', '${(u.display_name || '').replace(/'/g, "\\'")}')">
          👥 ${u.active_sessions || 0} active
        </button>
      </td>
      <td><div class="action-btns" onclick="event.stopPropagation()">
        <button class="btn btn-xs btn-outline" onclick="openUserDetail('${u.id}')">👤 View</button>
        <button class="btn btn-xs ${u.is_blocked ? 'btn-success' : 'btn-danger'}" onclick="toggleBlockUser('${u.id}', ${u.is_blocked})">
          ${u.is_blocked ? 'Unblock' : 'Block'}
        </button>
      </div></td>
    </tr>`).join('');
}

async function toggleBlockUser(userId, isBlocked) {
  if (!confirm(`${isBlocked ? 'Unblock' : 'Block'} this user?`)) return;
  try {
    await adminFetch(`/admin/users/${userId}/block`, { method: 'PUT', body: JSON.stringify({ is_blocked: !isBlocked }) });
    showToast(`User ${isBlocked ? 'unblocked' : 'blocked'}`, 'success');
    await loadOrders();
  } catch (e) { showToast(e.message, 'error'); }
}

async function adjustWallet(userId, name) {
  const amount = prompt(`Adjust wallet for ${name}.\nEnter amount (+ to add, - to deduct):`);
  if (!amount || isNaN(parseFloat(amount))) return;
  try {
    await adminFetch(`/admin/users/${userId}/wallet`, {
      method: 'PUT',
      body: JSON.stringify({ amount: parseFloat(amount), reason: 'Admin adjustment' })
    });
    showToast('Wallet adjusted!', 'success');
    await loadOrders();
    // Refresh detail panel if open
    const panel = document.getElementById('user-detail-panel');
    if (panel && !panel.classList.contains('hidden')) openUserDetail(userId);
  } catch (e) { showToast(e.message, 'error'); }
}

// =====================================================
// USER DETAIL PANEL
// =====================================================
let _userDetailAutoRefresh = null;

async function openUserDetail(userId) {
  document.getElementById('user-detail-title').textContent = 'Loading...';
  document.getElementById('user-detail-body').innerHTML = '<div style="padding:30px;text-align:center;color:var(--text-muted)">Loading user data...</div>';
  document.getElementById('user-detail-panel').classList.remove('hidden');
  document.getElementById('user-detail-overlay').classList.remove('hidden');
  document.getElementById('user-detail-panel').dataset.userId = userId;
  try {
    const u = await adminFetch(`/admin/users/${userId}/detail`);
    renderUserDetailPanel(u);
  } catch (e) {
    document.getElementById('user-detail-body').innerHTML = `<p style="color:var(--error);padding:20px">${e.message}</p>`;
  }
}

function closeUserDetail() {
  document.getElementById('user-detail-panel').classList.add('hidden');
  document.getElementById('user-detail-overlay').classList.add('hidden');
}

function renderUserDetailPanel(u) {
  document.getElementById('user-detail-title').textContent = u.display_name || 'User Profile';
  const body = document.getElementById('user-detail-body');
  const cityLine = [u.city, u.state].filter(Boolean).join(', ') || '—';
  const latLng   = u.latitude && u.longitude ? `${u.latitude.toFixed(4)}, ${u.longitude.toFixed(4)}` : '—';

  body.innerHTML = `
    <!-- Avatar + Core -->
    <div class="detail-section" style="display:flex;gap:16px;align-items:center">
      <div style="width:64px;height:64px;border-radius:50%;background:linear-gradient(135deg,#A855F7,#FF6B35);display:flex;align-items:center;justify-content:center;font-size:28px;font-weight:700;color:#fff;flex-shrink:0">
        ${u.photo_url ? `<img src="${u.photo_url}" style="width:64px;height:64px;border-radius:50%;object-fit:cover" />` : (u.display_name||'U')[0]}
      </div>
      <div style="flex:1">
        <div style="font-size:18px;font-weight:700">${u.display_name || '—'}</div>
        <div style="font-size:12px;color:var(--text-muted)">${u.username ? '@'+u.username : ''} &nbsp;·&nbsp; TG: <code>${u.telegram_id}</code></div>
        <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">
          <span class="badge ${u.role==='admin'?'badge-processing':'badge-available'}">${u.role}</span>
          <span class="badge ${u.is_blocked?'badge-cancelled':'badge-available'}">${u.is_blocked?'Blocked':'Active'}</span>
        </div>
      </div>
    </div>

    <!-- Stats row -->
    <div class="detail-section" style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px">
      <div style="background:var(--bg-glass);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:20px;font-weight:700;color:#22C55E">₹${(u.wallet_balance||0).toFixed(2)}</div>
        <div style="font-size:11px;color:var(--text-muted)">Wallet</div>
      </div>
      <div style="background:var(--bg-glass);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:20px;font-weight:700;color:#FF6B35">${u.total_orders||0}</div>
        <div style="font-size:11px;color:var(--text-muted)">Orders</div>
      </div>
      <div style="background:var(--bg-glass);border-radius:10px;padding:12px;text-align:center">
        <div style="font-size:20px;font-weight:700;color:#A855F7">₹${(u.total_spent||0).toFixed(0)}</div>
        <div style="font-size:11px;color:var(--text-muted)">Total Spent</div>
      </div>
    </div>

    <!-- Contact & Location -->
    <div class="detail-section">
      <h4>Contact & Location</h4>
      <div class="detail-row"><span class="detail-row-label">Phone</span><span>${u.phone||'—'}</span></div>
      <div class="detail-row"><span class="detail-row-label">City / State</span><span>${cityLine}</span></div>
      <div class="detail-row"><span class="detail-row-label">GPS Coords</span><span style="font-size:11px">${latLng}</span></div>
      <div class="detail-row"><span class="detail-row-label">Joined</span><span>${new Date(u.created_at).toLocaleString('en-IN')}</span></div>
    </div>

    <!-- Saved Addresses -->
    <div class="detail-section">
      <h4>Saved Addresses (${(u.saved_addresses||[]).length})</h4>
      ${(u.saved_addresses||[]).length === 0 ? '<div style="color:var(--text-muted);font-size:13px">No saved addresses</div>' : ''}
      ${(u.saved_addresses||[]).map(a => `
        <div style="background:var(--bg-glass);border-radius:8px;padding:10px;margin-bottom:6px;font-size:12px">
          <div style="font-weight:600;margin-bottom:3px">${a.is_default?'⭐ ':''}${a.label}</div>
          <div style="color:var(--text-muted)">${a.full_address}</div>
          <div style="color:var(--text-muted)">${[a.city,a.pincode].filter(Boolean).join(' — ')}</div>
        </div>`).join('')}
    </div>

    <!-- Wallet Transactions Ledger -->
    <div class="detail-section">
      <h4>Wallet Ledger History (${(u.wallet_transactions||[]).length})</h4>
      ${(u.wallet_transactions||[]).length === 0 ? '<div style="color:var(--text-muted);font-size:13px">No wallet activity yet</div>' : ''}
      <div style="max-height: 200px; overflow-y: auto; padding-right: 4px;">
        ${(u.wallet_transactions||[]).map(t => {
          const tColor = t.amount >= 0 ? '#22C55E' : '#EF4444';
          const tSign = t.amount >= 0 ? '+' : '';
          return `
            <div style="background:var(--bg-glass);border-radius:8px;padding:10px;margin-bottom:6px;font-size:12px">
              <div style="display:flex;justify-content:space-between;margin-bottom:3px">
                <span style="font-weight:600;color:var(--text-primary)">${t.type.toUpperCase().replace('_', ' ')}</span>
                <span style="font-weight:700;color:${tColor}">${tSign}₹${t.amount.toFixed(2)}</span>
              </div>
              <div style="color:var(--text-muted);font-size:11px">${t.description || ''}</div>
              <div style="color:var(--text-muted);font-size:10px;margin-top:4px;text-align:right">${new Date(t.created_at).toLocaleString('en-IN')}</div>
            </div>
          `;
        }).join('')}
      </div>
    </div>

    <!-- Order History -->
    <div class="detail-section">
      <h4>Order History (${(u.orders||[]).length})</h4>
      ${(u.orders||[]).length === 0 ? '<div style="color:var(--text-muted);font-size:13px">No orders yet</div>' : ''}
      ${(u.orders||[]).slice(0,8).map(o => `
        <div style="background:var(--bg-glass);border-radius:8px;padding:10px;margin-bottom:6px;cursor:pointer" onclick="closeUserDetail();openOrderDetail('${o.id}')">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <code style="font-size:11px;color:var(--text-muted)">${o.id}</code>
            <span class="badge ${statusBadgeClass(o.status)}">${o.status}</span>
          </div>
          <div style="font-size:12px;margin:4px 0;color:var(--text-primary)">${(o.items||[]).map(i=>`${i.qty}×${i.name}`).slice(0,3).join(', ')}</div>
          <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text-muted)">
            <span>₹${o.total_payable}</span>
            <span>${new Date(o.created_at).toLocaleDateString('en-IN')}</span>
          </div>
        </div>`).join('')}
    </div>

    <!-- Admin Actions -->
    <div class="detail-section">
      <h4>Admin Actions</h4>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
        <button class="btn btn-outline btn-sm" onclick="adjustWalletFromDetail('${u.id}','${(u.display_name||'').replace(/'/g,"\\'")}')">💰 Adjust Wallet</button>
        <button class="btn btn-sm ${u.is_blocked?'btn-success':'btn-danger'}" onclick="toggleBlockUserFromDetail('${u.id}',${u.is_blocked})">${u.is_blocked?'✅ Unblock':'🚫 Block'} User</button>
      </div>
    </div>
  `;
}

async function adjustWalletFromDetail(userId, name) {
  const amount = prompt(`Adjust wallet for ${name}.\nEnter amount (+ to add, - to deduct):`);
  if (!amount || isNaN(parseFloat(amount))) return;
  try {
    await adminFetch(`/admin/users/${userId}/wallet`, {
      method: 'PUT',
      body: JSON.stringify({ amount: parseFloat(amount), reason: 'Admin adjustment' })
    });
    showToast('Wallet adjusted!', 'success');
    openUserDetail(userId);
    await loadOrders();
  } catch (e) { showToast(e.message, 'error'); }
}

async function toggleBlockUserFromDetail(userId, isBlocked) {
  if (!confirm(`${isBlocked ? 'Unblock' : 'Block'} this user?`)) return;
  try {
    await adminFetch(`/admin/users/${userId}/block`, { method: 'PUT', body: JSON.stringify({ is_blocked: !isBlocked }) });
    showToast(`User ${isBlocked ? 'unblocked' : 'blocked'}`, 'success');
    openUserDetail(userId);
    await loadOrders();
  } catch (e) { showToast(e.message, 'error'); }
}

// =====================================================
// ROBOT LOGS
// =====================================================
let _robotLogsTimer = null;
let _robotLogsAutoRefresh = false;

function renderRobotLogsSection() {
  const sec = document.getElementById('section-robot-logs');
  if (!sec) return;
  sec.innerHTML = `
    <div class="section-header">
      <h2>🤖 Robot Logs</h2>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
        <input type="text" id="rl-mobile-filter" class="form-control" placeholder="Filter mobile..." style="width:160px" oninput="loadRobotLogs()" />
        <select id="rl-level-filter" class="form-control select-control" style="width:130px" onchange="loadRobotLogs()">
          <option value="ALL">All Levels</option>
          <option value="INFO">INFO</option>
          <option value="WARNING">WARNING</option>
          <option value="ERROR">ERROR</option>
        </select>
        <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
          <input type="checkbox" id="rl-autorefresh" onchange="toggleRobotLogsAutoRefresh()" /> Auto-refresh (5s)
        </label>
        <button class="btn btn-sm btn-outline" onclick="loadRobotLogs()">🔄 Refresh</button>
        <button class="btn btn-sm btn-danger" onclick="clearRobotLogs()">🗑️ Clear All</button>
      </div>
    </div>
    <div class="table-container">
      <table class="admin-table" id="robot-logs-table">
        <thead><tr>
          <th>Time</th><th>Mobile</th><th>Level</th><th>Stage</th><th>Message</th><th>Details</th>
        </tr></thead>
        <tbody id="robot-logs-tbody"><tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:20px">Loading...</td></tr></tbody>
      </table>
    </div>
  `;
}

async function loadRobotLogs() {
  const mobile = document.getElementById('rl-mobile-filter')?.value || '';
  const level  = document.getElementById('rl-level-filter')?.value  || 'ALL';
  try {
    const params = new URLSearchParams({ limit: 300 });
    if (mobile) params.append('mobile', mobile);
    if (level !== 'ALL') params.append('level', level);
    const logs = await adminFetch(`/admin/robot-logs?${params}`);
    renderRobotLogs(logs);
  } catch(e) { console.error('Robot logs load failed:', e); }
}

function renderRobotLogs(logs) {
  const tbody = document.getElementById('robot-logs-tbody');
  if (!tbody) return;
  if (!logs || logs.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:20px">No robot logs yet. Run an OTP session to see activity here.</td></tr>';
    return;
  }
  const levelColor = { INFO: '#22C55E', WARNING: '#F59E0B', ERROR: '#EF4444' };
  const stageBg    = { otp_request:'#3B82F6', browser_launch:'#A855F7', otp_fill:'#F59E0B', session_save:'#22C55E', order_submit:'#FF6B35', error:'#EF4444' };
  tbody.innerHTML = logs.map(l => {
    const time    = new Date(l.created_at).toLocaleString('en-IN', { hour12: false, hour:'2-digit', minute:'2-digit', second:'2-digit', day:'2-digit', month:'short' });
    const details = l.details && Object.keys(l.details).length ? JSON.stringify(l.details, null, 2) : '';
    return `<tr style="border-bottom:1px solid rgba(255,255,255,0.05)">
      <td style="font-size:11px;color:var(--text-muted);white-space:nowrap">${time}</td>
      <td><code style="font-size:11px">${l.mobile_number||'—'}</code></td>
      <td><span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700;background:${levelColor[l.level]||'#888'}22;color:${levelColor[l.level]||'#888'};border:1px solid ${levelColor[l.level]||'#888'}44">${l.level}</span></td>
      <td><span style="display:inline-block;padding:2px 7px;border-radius:8px;font-size:11px;background:${(stageBg[l.stage]||'#666')}22;color:${stageBg[l.stage]||'#aaa'};border:1px solid ${(stageBg[l.stage]||'#666')}33">${l.stage}</span></td>
      <td style="font-size:12px;max-width:320px;word-break:break-word">${l.message}</td>
      <td>${details ? `<button class="btn btn-xs btn-outline" onclick="this.nextSibling.classList.toggle('hidden')">JSON</button><pre class="hidden" style="font-size:10px;background:var(--bg-glass);border-radius:6px;padding:8px;margin-top:4px;max-width:260px;overflow:auto;white-space:pre-wrap">${details}</pre>` : '—'}</td>
    </tr>`;
  }).join('');
  // Update badge
  const errors = logs.filter(l => l.level === 'ERROR').length;
  const badge = document.getElementById('robot-logs-badge');
  if (badge) { badge.textContent = errors || ''; badge.classList.toggle('hidden', errors === 0); }
}

function toggleRobotLogsAutoRefresh() {
  _robotLogsAutoRefresh = document.getElementById('rl-autorefresh')?.checked;
  if (_robotLogsTimer) { clearInterval(_robotLogsTimer); _robotLogsTimer = null; }
  if (_robotLogsAutoRefresh) {
    _robotLogsTimer = setInterval(loadRobotLogs, 5000);
  }
}

async function clearRobotLogs() {
  if (!confirm('Clear all robot logs?')) return;
  try {
    await adminFetch('/admin/robot-logs', { method: 'DELETE' });
    showToast('Robot logs cleared', 'success');
    loadRobotLogs();
  } catch (e) { showToast(e.message, 'error'); }
}

// =====================================================
// GIFT CARDS
// =====================================================
async function loadGiftCards() {
  try {
    const data = await adminFetch('/admin/gift-cards');
    AdminState.giftcards = data;
    renderGiftCards();
  } catch (e) { console.error('Gift cards load failed:', e); }
}

function renderGiftCards() {
  const cards = AdminState.giftcards;
  const available = cards.filter(c => c.status === 'available');
  const used = cards.filter(c => c.status === 'used');
  const totalVal = available.reduce((s, c) => s + c.value, 0);

  setText('gc-available', available.length);
  setText('gc-used', used.length);
  setText('gc-total-value', `₹${totalVal.toFixed(0)}`);

  const tbody = document.getElementById('giftcards-tbody');
  if (!tbody) return;
  tbody.innerHTML = cards.map((c, i) => `
    <tr>
      <td>${i + 1}</td>
      <td><code>${c.code}</code></td>
      <td><code>${c.pin}</code></td>
      <td><b>₹${c.value}</b></td>
      <td><span class="badge badge-${c.status}">${c.status}</span></td>
      <td>${c.used_by_user_id ? `User #${c.used_by_user_id}` : '—'}</td>
      <td>${c.used_in_order_id || '—'}</td>
      <td style="color:var(--text-muted);font-size:12px">${c.used_at ? new Date(c.used_at).toLocaleString('en-IN') : '—'}</td>
      <td style="color:var(--text-muted);font-size:12px">${new Date(c.uploaded_at).toLocaleString('en-IN')}</td>
    </tr>`).join('');
}

// =====================================================
// SUPPORT CHAT
// =====================================================
async function loadSupportUsers() {
  try {
    const data = await adminFetch('/admin/support-messages');
    const grouped = {};
    (data || []).forEach(msg => {
      if (!grouped[msg.user_id]) {
        grouped[msg.user_id] = { user_id: msg.user_id, display_name: msg.user_display_name, messages: [] };
      }
      grouped[msg.user_id].messages.push(msg);
    });
    AdminState.supportUsers = Object.values(grouped);
    renderSupportUsersList();
  } catch (e) { console.error('Support load failed:', e); }
}

function renderSupportUsersList() {
  const container = document.getElementById('support-users-list');
  if (!container) return;
  container.innerHTML = AdminState.supportUsers.map(u => {
    const lastMsg = u.messages[u.messages.length - 1];
    const unread = u.messages.filter(m => m.sender === 'user' && !m.is_read).length;
    return `
      <div class="support-user-item ${AdminState.activeSupportUser === u.user_id ? 'active' : ''}" onclick="selectSupportUser('${u.user_id}')">
        ${unread ? `<span class="support-user-unread">${unread}</span>` : ''}
        <div class="support-user-name">${u.display_name || `User #${u.user_id}`}</div>
        <div class="support-user-preview">${lastMsg?.message?.slice(0, 50) || 'No messages'}</div>
      </div>`;
  }).join('') || '<div style="padding:20px;color:var(--text-muted);font-size:13px">No conversations yet</div>';
}

function selectSupportUser(userId) {
  AdminState.activeSupportUser = userId;
  const userData = AdminState.supportUsers.find(u => u.user_id === userId);
  document.getElementById('support-chat-header').textContent = userData?.display_name || `User #${userId}`;
  renderSupportMessages(userData?.messages || []);
  renderSupportUsersList();
}

function renderSupportMessages(messages) {
  const container = document.getElementById('support-messages');
  if (!container) return;
  container.innerHTML = messages.map(msg => `
    <div class="support-msg ${msg.sender === 'admin' ? 'admin' : 'user'}">
      <div>${msg.message}</div>
      <div class="support-msg-time">${new Date(msg.created_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })}</div>
    </div>`).join('');
  container.scrollTop = container.scrollHeight;
}

async function sendSupportReply() {
  const input = document.getElementById('support-reply-input');
  const msg = input.value.trim();
  if (!msg || !AdminState.activeSupportUser) return;
  try {
    await adminFetch('/admin/support-reply', {
      method: 'POST',
      body: JSON.stringify({ user_id: AdminState.activeSupportUser, message: msg })
    });
    input.value = '';
    await loadSupportUsers();
    selectSupportUser(AdminState.activeSupportUser);
    showToast('Reply sent!', 'success');
  } catch (e) { showToast(e.message, 'error'); }
}

// =====================================================
// ANALYTICS
// =====================================================
async function loadAnalytics() {
  try {
    const days = parseInt(document.getElementById('analytics-days')?.value || 30);
    const data = await adminFetch(`/admin/analytics/summary`);

    // Revenue chart
    const revCtx = document.getElementById('analytics-revenue-chart')?.getContext('2d');
    if (revCtx && data.daily_revenue) {
      if (AdminState.analyticsCharts.revenue) AdminState.analyticsCharts.revenue.destroy();
      AdminState.analyticsCharts.revenue = new Chart(revCtx, {
        type: 'bar',
        data: {
          labels: data.daily_revenue.map(d => d.date?.slice(5)),
          datasets: [{
            label: 'Revenue (₹)',
            data: data.daily_revenue.map(d => d.revenue || 0),
            backgroundColor: 'rgba(255,107,53,0.6)',
            borderColor: '#FF6B35',
            borderWidth: 1,
            borderRadius: 4,
          }]
        },
        options: {
          responsive: true,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: '#9090B0', font: { size: 11 } }, grid: { display: false } },
            y: { ticks: { color: '#9090B0', callback: v => `₹${v}` }, grid: { color: 'rgba(255,255,255,0.04)' } }
          }
        }
      });
    }

    // Users trend chart
    const userCtx = document.getElementById('analytics-users-chart')?.getContext('2d');
    if (userCtx && data.user_trend) {
      if (AdminState.analyticsCharts.users) AdminState.analyticsCharts.users.destroy();
      AdminState.analyticsCharts.users = new Chart(userCtx, {
        type: 'line',
        data: {
          labels: data.user_trend.map(d => d.date?.slice(5)),
          datasets: [{
            label: 'New Users',
            data: data.user_trend.map(d => d.count || 0),
            borderColor: '#22C55E',
            backgroundColor: 'rgba(34,197,94,0.1)',
            fill: true,
            tension: 0.4,
          }]
        },
        options: {
          responsive: true,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: '#9090B0', font: { size: 11 } }, grid: { display: false } },
            y: { ticks: { color: '#9090B0' }, grid: { color: 'rgba(255,255,255,0.04)' } }
          }
        }
      });
    }

    // Top products
    const topTbody = document.getElementById('top-products-tbody');
    if (topTbody && data.top_products) {
      topTbody.innerHTML = (data.top_products || []).map(p => `
        <tr>
          <td><b>${p.name}</b></td>
          <td>${p.category || '—'}</td>
          <td>${p.total_qty || 0}</td>
          <td>₹${(p.total_revenue || 0).toFixed(0)}</td>
        </tr>`).join('');
    }

    // Location pricing
    const locPricingTbody = document.getElementById('location-pricing-tbody');
    if (locPricingTbody) {
      const pricing = await adminFetch('/location/pricing');
      locPricingTbody.innerHTML = (pricing || []).map(p => `
        <tr>
          <td><b>${p.city}</b></td>
          <td>${p.state || '—'}</td>
          <td>${p.price_multiplier}×</td>
          <td>₹${p.delivery_charge}</td>
          <td>₹${p.min_order_value}</td>
          <td><span class="badge ${p.is_serviceable ? 'badge-available' : 'badge-cancelled'}">${p.is_serviceable ? 'Active' : 'Inactive'}</span></td>
        </tr>`).join('') || '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:20px">No pricing configured</td></tr>';
    }

  } catch (e) { console.error('Analytics load failed:', e); }
}

// =====================================================
// SETTINGS
// =====================================================
async function loadConfig() {
  try {
    const cfg = await adminFetch('/admin/config');
    AdminState.config = cfg;
    renderSettings();
  } catch (e) {}
}

function renderSettings() {
  const cfg = AdminState.config;
  setVal('cfg-platform-name', cfg.platform_name || '');
  setVal('cfg-upi-id', cfg.upi_id || '');
  setVal('cfg-upi-name', cfg.upi_name || '');

  setVal('cfg-mini-app-url', cfg.mini_app_url || '');
  setVal('cfg-captcha-api-key', cfg.captcha_api_key || '');
  setVal('cfg-playwright-headless', cfg.playwright_headless || 'true');
  setVal('cfg-robot-mode', cfg.robot_mode || 'auto');
  setVal('cfg-newbie-coupon', cfg.newbie_coupon || '');
  setVal('cfg-welcome-coupon', cfg.welcome_coupon || '');
  setVal('cfg-promo-min', cfg.cart_promo_min || '');
  setVal('cfg-promo-max', cfg.cart_promo_max || '');
  setVal('cfg-promo-fixed', cfg.cart_promo_fixed || '');
  setVal('cfg-bot-fee', cfg.bot_fee || '');
}

async function saveSettings() {
  const keys = [
    { id: 'cfg-platform-name', key: 'platform_name' },
    { id: 'cfg-upi-id', key: 'upi_id' },
    { id: 'cfg-upi-name', key: 'upi_name' },
    { id: 'cfg-mini-app-url', key: 'mini_app_url' },
    { id: 'cfg-captcha-api-key', key: 'captcha_api_key' },
    { id: 'cfg-playwright-headless', key: 'playwright_headless' },
    { id: 'cfg-robot-mode', key: 'robot_mode' },
    { id: 'cfg-newbie-coupon', key: 'newbie_coupon' },
    { id: 'cfg-welcome-coupon', key: 'welcome_coupon' },
    { id: 'cfg-promo-min', key: 'cart_promo_min' },
    { id: 'cfg-promo-max', key: 'cart_promo_max' },
    { id: 'cfg-promo-fixed', key: 'cart_promo_fixed' },
    { id: 'cfg-bot-fee', key: 'bot_fee' },
  ];
  try {
    let savedCount = 0;
    const savedKeys = [];
    for (const { id, key } of keys) {
      const val = document.getElementById(id)?.value?.trim();
      if (val !== undefined) {
        await adminFetch('/admin/config', { method: 'PUT', body: JSON.stringify({ key, value: val }) });
        savedKeys.push(key);
        savedCount++;
      }
    }
    showToast(`Successfully saved ${savedCount} settings: ${savedKeys.join(', ')}`, 'success');
    await loadConfig();
  } catch (e) { showToast(e.message, 'error'); }
}

// =====================================================
// AUDIT LOGS
// =====================================================
async function loadAuditLogs() {
  try {
    const data = await adminFetch('/admin/logs?limit=50');
    const auditTbody = document.getElementById('audit-tbody');
    const errorTbody = document.getElementById('error-tbody');

    if (auditTbody && data.audit_logs) {
      auditTbody.innerHTML = (data.audit_logs || []).map(l => `
        <tr>
          <td>${l.admin_username || 'System'}</td>
          <td><code style="font-size:11px">${l.action}</code></td>
          <td style="font-size:12px;color:var(--text-muted);max-width:200px;overflow:hidden;text-overflow:ellipsis">${l.details || '—'}</td>
          <td style="font-size:11px;color:var(--text-muted)">${l.ip_address || '—'}</td>
          <td style="font-size:11px;color:var(--text-muted)">${new Date(l.created_at).toLocaleString('en-IN')}</td>
        </tr>`).join('');
    }

    if (errorTbody && data.error_logs) {
      errorTbody.innerHTML = (data.error_logs || []).map(l => `
        <tr>
          <td><span class="badge badge-pending">${l.type}</span></td>
          <td style="font-size:12px;max-width:400px">${l.message}</td>
          <td style="font-size:11px;color:var(--text-muted)">${new Date(l.created_at).toLocaleString('en-IN')}</td>
        </tr>`).join('');
    }
  } catch (e) { console.error('Logs load failed:', e); }
}

// =====================================================
// EXPORT CSV
// =====================================================
function exportOrdersCSV() {
  const rows = [['Order ID', 'Customer', 'Items', 'Total', 'Status', 'Date']];
  AdminState.orders.forEach(o => {
    const items = (o.items || []).map(i => `${i.quantity}×${i.product_name}`).join('; ');
    rows.push([o.id, o.user_display_name, items, o.total_payable, o.status, o.created_at]);
  });
  downloadCSV('orders.csv', rows);
}

function exportUsersCSV() {
  const rows = [['ID', 'Name', 'Username', 'Telegram ID', 'Phone', 'Role']];
  AdminState.users.forEach(u => {
    rows.push([u.id, u.display_name, u.username, u.telegram_id, u.phone, u.role]);
  });
  downloadCSV('users.csv', rows);
}

function downloadCSV(filename, rows) {
  const csv = rows.map(r => r.map(v => `"${String(v || '').replace(/"/g, '""')}"`).join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
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
  toast.innerHTML = `<span>${icons[type]}</span> ${message}`;
  container.appendChild(toast);
  setTimeout(() => toast.remove(), 4000);
}

// =====================================================
// HELPERS
// =====================================================
function setText(id, val) { const el = document.getElementById(id); if (el) el.textContent = val; }
function setVal(id, val) { const el = document.getElementById(id); if (el) el.value = val; }

// =====================================================
// EVENT LISTENERS SETUP
// =====================================================
function setupAllEventListeners() {
  // Logout
  document.getElementById('admin-logout-btn').addEventListener('click', () => {
    sessionStorage.removeItem('admin_token');
    sessionStorage.removeItem('admin_user');
    location.reload();
  });

  // Order search / filter
  document.getElementById('order-search')?.addEventListener('input', renderOrdersTable);
  document.getElementById('order-status-filter')?.addEventListener('change', renderOrdersTable);
  document.getElementById('export-orders-btn')?.addEventListener('click', exportOrdersCSV);
  document.getElementById('export-users-btn')?.addEventListener('click', exportUsersCSV);

  // Close order detail panel
  document.getElementById('close-order-detail')?.addEventListener('click', closeOrderDetail);
  document.getElementById('order-panel-overlay')?.addEventListener('click', closeOrderDetail);

  // Products
  document.getElementById('add-product-btn')?.addEventListener('click', openAddProduct);
  document.getElementById('close-product-form')?.addEventListener('click', closeProductForm);
  document.getElementById('cancel-product-form')?.addEventListener('click', closeProductForm);

  // User search
  document.getElementById('user-search')?.addEventListener('input', renderUsersTable);

  // Settings save
  document.getElementById('save-settings-btn')?.addEventListener('click', saveSettings);

  // Logs tabs
  document.querySelectorAll('.log-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.log-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById('audit-logs-wrap').classList.toggle('hidden', tab.dataset.tab !== 'audit');
      document.getElementById('error-logs-wrap').classList.toggle('hidden', tab.dataset.tab !== 'errors');
    });
  });

  // Support
  document.getElementById('support-send-btn')?.addEventListener('click', sendSupportReply);
  document.getElementById('support-reply-input')?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') sendSupportReply();
  });

  // Analytics days
  document.getElementById('analytics-days')?.addEventListener('change', loadAnalytics);

  // Gift card CSV upload
  document.getElementById('giftcard-upload')?.addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const formData = new FormData();
    formData.append('file', file);
    const headers = {};
    if (AdminState.token) headers['Authorization'] = `Bearer ${AdminState.token}`;
    try {
      const res = await fetch(`${API}/admin/gift-cards/upload`, { method: 'POST', headers, body: formData });
      if (!res.ok) throw new Error('Upload failed');
      showToast('Gift cards uploaded!', 'success');
      await loadGiftCards();
    } catch (err) { showToast(err.message, 'error'); }
  });

  // Add Gift Cards Modal toggling
  const addGcModal = document.getElementById('add-giftcard-modal');
  document.getElementById('btn-add-giftcard')?.addEventListener('click', () => {
    addGcModal.classList.remove('hidden');
    document.getElementById('single-gc-form').reset();
    document.getElementById('bulk-gc-form').reset();
  });
  document.getElementById('add-giftcard-modal-close')?.addEventListener('click', () => {
    addGcModal.classList.add('hidden');
  });

  const tabSingle = document.getElementById('btn-tab-single-gc');
  const tabBulk = document.getElementById('btn-tab-bulk-gc');
  const formSingle = document.getElementById('single-gc-form');
  const formBulk = document.getElementById('bulk-gc-form');

  tabSingle?.addEventListener('click', () => {
    tabSingle.className = 'btn btn-sm btn-primary';
    tabBulk.className = 'btn btn-sm btn-outline';
    formSingle.classList.remove('hidden');
    formBulk.classList.add('hidden');
  });

  tabBulk?.addEventListener('click', () => {
    tabSingle.className = 'btn btn-sm btn-outline';
    tabBulk.className = 'btn btn-sm btn-primary';
    formSingle.classList.add('hidden');
    formBulk.classList.remove('hidden');
  });

  // Single Gift Card Submit
  formSingle?.addEventListener('submit', async (e) => {
    e.preventDefault();
    const code = document.getElementById('manual-gc-code').value.trim();
    const pin = document.getElementById('manual-gc-pin').value.trim();
    const value = parseFloat(document.getElementById('manual-gc-value').value);

    if (code.length !== 16 || isNaN(code)) {
      showToast('Code must be exactly 16 digits', 'error');
      return;
    }
    if (pin.length !== 6 || isNaN(pin)) {
      showToast('PIN must be exactly 6 digits', 'error');
      return;
    }

    try {
      const res = await adminFetch('/admin/gift-cards/add-manual', {
        method: 'POST',
        body: JSON.stringify({ code, pin, value })
      });
      showToast(res.message, 'success');
      addGcModal.classList.add('hidden');
      await loadGiftCards();
    } catch (err) {
      showToast(err.message, 'error');
    }
  });

  // Bulk Gift Card Submit
  formBulk?.addEventListener('submit', async (e) => {
    e.preventDefault();
    const text_data = document.getElementById('bulk-gc-text').value;

    try {
      const res = await adminFetch('/admin/gift-cards/add-bulk', {
        method: 'POST',
        body: JSON.stringify({ text_data })
      });
      if (res.errors && res.errors.length) {
        showToast(`Bulk import partially successful: ${res.added} added, ${res.errors.length} errors`, 'warning');
        console.warn('Bulk gift card import errors:', res.errors);
      } else {
        showToast(res.message, 'success');
      }
      addGcModal.classList.add('hidden');
      await loadGiftCards();
    } catch (err) {
      showToast(err.message, 'error');
    }
  });

  // Password change
  document.getElementById('change-password-btn')?.addEventListener('click', async () => {
    const pw = document.getElementById('new-admin-password').value;
    if (!pw || pw.length < 6) { showToast('Password must be at least 6 characters', 'error'); return; }
    try {
      await adminFetch('/admin/change-password', { method: 'PUT', body: JSON.stringify({ new_password: pw }) });
      showToast('Password updated!', 'success');
      document.getElementById('new-admin-password').value = '';
    } catch (e) { showToast(e.message, 'error'); }
  });

  // Add city pricing
  document.getElementById('add-city-pricing-btn')?.addEventListener('click', () => {
    const city = prompt('City name:');
    if (!city) return;
    const multiplier = parseFloat(prompt('Price multiplier (e.g. 1.1 = 10% more):', '1.0') || '1.0');
    const delivery = parseFloat(prompt('Delivery charge (₹):', '30') || '30');
    const minOrder = parseFloat(prompt('Min order value (₹):', '149') || '149');
    adminFetch('/admin/location-pricing', {
      method: 'POST',
      body: JSON.stringify({ city, price_multiplier: multiplier, delivery_charge: delivery, min_order_value: minOrder })
    }).then(() => { showToast(`${city} pricing saved!`, 'success'); loadAnalytics(); })
      .catch(e => showToast(e.message, 'error'));
  });

  // Open Add Proxy modal
  document.getElementById('add-proxy-btn')?.addEventListener('click', () => {
    document.getElementById('edit-proxy-id').value = '';
    document.getElementById('proxy-form').reset();
    document.getElementById('proxy-modal-title').textContent = 'Add New Proxy';
    document.getElementById('proxy-modal').classList.remove('hidden');
  });

  // Cancel/Close modal
  const closeProxyModal = () => {
    document.getElementById('proxy-modal').classList.add('hidden');
  };
  document.getElementById('proxy-modal-close')?.addEventListener('click', closeProxyModal);
  document.getElementById('proxy-modal-cancel')?.addEventListener('click', closeProxyModal);

  // Submit Proxy form
  document.getElementById('proxy-form')?.addEventListener('submit', async (e) => {
    e.preventDefault();
    const id = document.getElementById('edit-proxy-id').value;
    const protocol = document.getElementById('proxy-protocol').value;
    const ip = document.getElementById('proxy-ip').value.trim();
    const port = parseInt(document.getElementById('proxy-port').value, 10);
    const username = document.getElementById('proxy-user').value.trim() || null;
    const password = document.getElementById('proxy-pass').value.trim() || null;
    const is_active = document.getElementById('proxy-active').checked;

    const payload = { protocol, ip, port, username, password, is_active };
    const method = id ? 'PUT' : 'POST';
    const path = id ? `/admin/proxies/${id}` : '/admin/proxies';

    try {
      await adminFetch(path, { method, body: JSON.stringify(payload) });
      showToast(id ? 'Proxy updated!' : 'Proxy added!', 'success');
      closeProxyModal();
      loadProxies();
      loadProxyLogs();
    } catch (err) {
      showToast(err.message, 'error');
    }
  });

  // Removed second 30-second auto-refresh timer to prevent unnecessary background requests
}

// =====================================================
// PROXY MANAGER
// =====================================================
async function loadProxies() {
  try {
    const data = await adminFetch('/admin/proxies');
    AdminState.proxies = data;
    renderProxiesTable();
  } catch (err) {
    showToast(err.message, 'error');
  }
}

function renderProxiesTable() {
  const tbody = document.getElementById('proxies-tbody');
  if (!tbody) return;

  if (!AdminState.proxies || AdminState.proxies.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;">No proxies added yet.</td></tr>';
    return;
  }

  tbody.innerHTML = AdminState.proxies.map(p => {
    const statusText = p.is_active ? 'Active' : 'Inactive';
    const statusClass = p.is_active ? 'status-paid' : 'status-cancelled';
    const lastUsedStr = p.last_used ? new Date(p.last_used).toLocaleString('en-IN') : '—';
    return `
      <tr>
        <td><span class="badge ${p.protocol === 'https' ? 'badge-veg' : 'badge-nonveg'}">${p.protocol.toUpperCase()}</span></td>
        <td><b>${p.ip}</b></td>
        <td>${p.port}</td>
        <td>${p.username || '—'}</td>
        <td>${p.password ? '••••••••' : '—'}</td>
        <td><span class="status-badge ${statusClass}" onclick="toggleProxyActive('${p.id}', ${p.is_active})" style="cursor:pointer;">${statusText}</span></td>
        <td>${p.fail_count}</td>
        <td><small>${lastUsedStr}</small></td>
        <td>
          <div style="display:flex; gap:6px;">
            <button class="btn btn-outline btn-xs" onclick="testProxy('${p.id}', this)">Test</button>
            <button class="btn btn-outline btn-xs" onclick="editProxy('${p.id}')">Edit</button>
            <button class="btn btn-outline btn-xs" onclick="deleteProxy('${p.id}')" style="border-color:var(--danger); color:var(--danger);">Delete</button>
          </div>
        </td>
      </tr>
    `;
  }).join('');
}

// Bind to window to allow HTML onclick calls
window.toggleProxyActive = async function(id, currentActive) {
  try {
    await adminFetch(`/admin/proxies/${id}`, {
      method: 'PUT',
      body: JSON.stringify({ is_active: !currentActive })
    });
    showToast('Proxy status updated!', 'success');
    loadProxies();
  } catch (err) {
    showToast(err.message, 'error');
  }
};

window.testProxy = async function(id, btn) {
  const originalText = btn.textContent;
  btn.textContent = 'Testing...';
  btn.disabled = true;
  try {
    const res = await adminFetch(`/admin/proxies/${id}/test`, { method: 'POST' });
    if (res.success) {
      showToast(`Success! Latency: ${res.latency.toFixed(1)}ms (IP: ${res.ip || 'ok'})`, 'success');
    } else {
      showToast(`Failed: ${res.error}`, 'error');
    }
    loadProxies();
    loadProxyLogs();
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    btn.textContent = originalText;
    btn.disabled = false;
  }
};

window.editProxy = function(id) {
  const proxy = AdminState.proxies.find(p => p.id === id);
  if (!proxy) return;

  document.getElementById('edit-proxy-id').value = proxy.id;
  document.getElementById('proxy-protocol').value = proxy.protocol;
  document.getElementById('proxy-ip').value = proxy.ip;
  document.getElementById('proxy-port').value = proxy.port;
  document.getElementById('proxy-user').value = proxy.username || '';
  document.getElementById('proxy-pass').value = proxy.password || '';
  document.getElementById('proxy-active').checked = proxy.is_active;

  document.getElementById('proxy-modal-title').textContent = 'Edit Proxy';
  document.getElementById('proxy-modal').classList.remove('hidden');
};

window.deleteProxy = async function(id) {
  if (!confirm('Are you sure you want to delete this proxy?')) return;
  try {
    await adminFetch(`/admin/proxies/${id}`, { method: 'DELETE' });
    showToast('Proxy deleted!', 'success');
    loadProxies();
    loadProxyLogs();
  } catch (err) {
    showToast(err.message, 'error');
  }
};

async function loadProxyLogs() {
  try {
    const logs = await adminFetch('/admin/proxies/logs');
    const tbody = document.getElementById('proxy-logs-tbody');
    if (!tbody) return;

    if (!logs || logs.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;">No logs found.</td></tr>';
      return;
    }

    tbody.innerHTML = logs.map(l => {
      const statusClass = l.status === 'success' ? 'status-paid' : 'status-cancelled';
      return `
        <tr>
          <td><small>${new Date(l.timestamp).toLocaleString('en-IN')}</small></td>
          <td><b>#${l.proxy_id}</b></td>
          <td><span class="badge badge-veg">${l.action.toUpperCase()}</span></td>
          <td><span class="status-badge ${statusClass}">${l.status.toUpperCase()}</span></td>
          <td><small>${l.details || '—'}</small></td>
        </tr>
      `;
    }).join('');
  } catch (err) {
    console.error('Failed to load proxy logs:', err);
  }
}


// =====================================================
// AUTO-LOGIN FROM STORAGE
// =====================================================
(function autoLogin() {
  const token = sessionStorage.getItem('admin_token');
  const user = sessionStorage.getItem('admin_user');
  
  // Detect if this is a page reload
  const navEntries = performance.getEntriesByType('navigation');
  const isReload = navEntries.length > 0 && navEntries[0].type === 'reload';

  if (token && user) {
    if (isReload) {
      AdminState.token = token;
      AdminState.admin = JSON.parse(user);
      showDashboard();
    } else {
      // Clear session storage if it's a fresh visit (e.g. back navigation or new URL typed in address bar)
      sessionStorage.removeItem('admin_token');
      sessionStorage.removeItem('admin_user');
    }
  }
})();


// =====================================================
// DOMINOS SESSION MANAGEMENT
// =====================================================

// Load all Domino's sessions from API
async function loadSessions() {
  try {
    const data = await adminFetch('/admin/dominos/sessions');
    AdminState.sessions = data;
    renderSessionsTable();
  } catch (err) {
    showToast('Failed to load Domino\'s sessions', 'error');
  }
}

// Render the sessions list table and modern card grid
function renderSessionsTable() {
  const tbody = document.getElementById('sessions-tbody');
  const cardsGrid = document.getElementById('sessions-cards-grid');
  
  if (!AdminState.sessions || AdminState.sessions.length === 0) {
    if (tbody) tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:24px">No Domino\'s accounts added yet.</td></tr>';
    if (cardsGrid) cardsGrid.innerHTML = '<div style="grid-column:1/-1;text-align:center;color:var(--text-muted);padding:40px;background:rgba(255,255,255,0.02);border:1px dashed rgba(255,255,255,0.1);border-radius:16px;">🤖 No accounts stored.<br><span style="font-size:12px;margin-top:6px;display:inline-block">Click "+ Add via OTP" to log in a new Domino\'s user.</span></div>';
    
    // Reset stats
    document.getElementById('stat-total') && (document.getElementById('stat-total').textContent = '0');
    document.getElementById('stat-active') && (document.getElementById('stat-active').textContent = '0');
    document.getElementById('stat-expired') && (document.getElementById('stat-expired').textContent = '0');
    document.getElementById('stat-unverified') && (document.getElementById('stat-unverified').textContent = '0');
    return;
  }
  
  // Calculate Stats
  let total = AdminState.sessions.length;
  let active = 0;
  let expired = 0;
  let unverified = 0;
  
  AdminState.sessions.forEach(s => {
    if (s.is_active) active++;
    if (s.verify_status === 'expired') expired++;
    else if (!s.verify_status) unverified++;
  });
  
  // Update stats elements if they exist
  document.getElementById('stat-total') && (document.getElementById('stat-total').textContent = total);
  document.getElementById('stat-active') && (document.getElementById('stat-active').textContent = active);
  document.getElementById('stat-expired') && (document.getElementById('stat-expired').textContent = expired);
  document.getElementById('stat-unverified') && (document.getElementById('stat-unverified').textContent = unverified);

  // Render Table (Fallback)
  if (tbody) {
    tbody.innerHTML = AdminState.sessions.map(s => {
      const verifyBadge = s.verify_status === 'valid'
        ? '<span class="badge badge-success">Valid</span>'
        : s.verify_status === 'expired'
        ? '<span class="badge badge-danger">Expired</span>'
        : '<span>Not Tested</span>';
      return `
      <tr>
        <td><strong>+91 ${s.mobile_number}</strong></td>
        <td>${new Date(s.created_at).toLocaleDateString()}</td>
        <td>${verifyBadge}</td>
        <td>
          <button class="btn btn-xs" onclick="verifySession('${s.id}', this)">Verify</button>
        </td>
      </tr>`;
    }).join('');
  }

  // Render Premium Cards
  if (cardsGrid) {
    cardsGrid.innerHTML = AdminState.sessions.map(s => {
      // Build Verification indicator
      let verifyLabel = 'Not Tested';
      let verifyDotColor = '#94a3b8';
      let verifyBg = 'rgba(148,163,184,0.06)';
      let verifyBorder = 'rgba(148,163,184,0.15)';
      
      if (s.verify_status === 'valid') {
        verifyLabel = 'Authenticated';
        verifyDotColor = '#10b981';
        verifyBg = 'rgba(16,185,129,0.08)';
        verifyBorder = 'rgba(16,185,129,0.2)';
      } else if (s.verify_status === 'expired') {
        verifyLabel = 'Expired';
        verifyDotColor = '#ef4444';
        verifyBg = 'rgba(239,68,68,0.08)';
        verifyBorder = 'rgba(239,68,68,0.2)';
      } else if (s.verify_status === 'unknown' || s.verify_status === 'error') {
        verifyLabel = 'Uncertain';
        verifyDotColor = '#f59e0b';
        verifyBg = 'rgba(245,158,111,0.08)';
        verifyBorder = 'rgba(245,158,111,0.2)';
      }

      // Build Health Badge
      let healthBadge = '';
      if (s.health_status === 'fresh') {
        healthBadge = '<span class="badge" style="background:rgba(34,197,94,0.15);color:#4ade80;border:1px solid rgba(34,197,94,0.3);font-size:10px;padding:2px 8px;border-radius:12px;font-weight:700">🟢 Fresh</span>';
      } else if (s.health_status === 'expiring') {
        healthBadge = '<span class="badge" style="background:rgba(245,158,11,0.15);color:#fbbf24;border:1px solid rgba(245,158,11,0.3);font-size:10px;padding:2px 8px;border-radius:12px;font-weight:700">🟡 Expiring</span>';
      } else if (s.health_status === 'expired') {
        healthBadge = '<span class="badge" style="background:rgba(239,68,68,0.15);color:#f87171;border:1px solid rgba(239,68,68,0.3);font-size:10px;padding:2px 8px;border-radius:12px;font-weight:700">🔴 Expired</span>';
      } else {
        healthBadge = '<span class="badge" style="background:rgba(148,163,184,0.15);color:#94a3b8;border:1px solid rgba(148,163,184,0.3);font-size:10px;padding:2px 8px;border-radius:12px;font-weight:700">⚪ Unknown</span>';
      }

      const lastChecked = s.last_verified_at 
        ? new Date(s.last_verified_at).toLocaleTimeString('en-IN', {hour12:false,hour:'2-digit',minute:'2-digit'}) + ' ' + new Date(s.last_verified_at).toLocaleDateString('en-IN', {day:'numeric',month:'short'})
        : 'Never';
        
      const authKeys = (s.auth_cookie_names || []).map(name => `
        <span style="font-size:9px;background:rgba(168,85,247,0.12);color:#c084fc;padding:2px 6px;border-radius:4px;border:1px solid rgba(168,85,247,0.25);font-family:monospace">${name}</span>
      `).join('') || '<span style="font-size:9.5px;color:var(--text-muted)">None (Import issues)</span>';

      return `
      <div class="admin-card" style="margin:0;display:flex;flex-direction:column;justify-content:space-between;border:1px solid rgba(255,255,255,0.06);background:var(--bg-card);border-radius:16px;overflow:hidden;box-shadow:0 8px 30px rgba(0,0,0,0.2);position:relative;transition:transform 0.2s,box-shadow 0.2s">
        
        <!-- Card Header Info -->
        <div style="padding:18px 20px;border-bottom:1px solid rgba(255,255,255,0.04)">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
            <span style="font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;font-weight:700">Domino's Session</span>
            
            <div style="display:flex;align-items:center;gap:6px">
              ${healthBadge}
              <span style="width:7px;height:7px;border-radius:50%;background:${s.is_active?'#22c55e':'#ef4444'};box-shadow:0 0 8px ${s.is_active?'#22c55e':'#ef4444'}"></span>
              <span style="font-size:11px;font-weight:700;color:${s.is_active?'#22c55e':'#ef4444'}">${s.is_active ? 'Active' : 'Inactive'}</span>
            </div>
          </div>
          
          <div style="font-size:18px;font-weight:800;color:var(--text-primary);letter-spacing:0.5px">+91 ${s.mobile_number}</div>
          <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text-muted);margin-top:4px">
            <span>Added: ${new Date(s.created_at).toLocaleDateString('en-IN', {day:'numeric',month:'short',year:'numeric'})}</span>
            <span>Orders: <strong style="color:var(--text-primary)">${s.order_count || 0}</strong></span>
          </div>
        </div>

        <!-- Session Status & Key Badges -->
        <div style="padding:14px 20px;background:rgba(255,255,255,0.01);flex:1;display:flex;flex-direction:column;gap:12px">
          <!-- Verify Status Banner -->
          <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 12px;background:${verifyBg};border:1px solid ${verifyBorder};border-radius:10px">
            <div style="display:flex;align-items:center;gap:8px">
              <span style="width:8px;height:8px;border-radius:50%;background:${verifyDotColor}"></span>
              <span style="font-size:12px;font-weight:700;color:var(--text-primary)">${verifyLabel}</span>
            </div>
            <span style="font-size:10px;color:var(--text-muted)">Checked: ${lastChecked}</span>
          </div>

          <!-- Cookies Meta -->
          <div>
            <div style="font-size:11px;color:var(--text-muted);font-weight:600;margin-bottom:6px;display:flex;justify-content:space-between">
              <span>🔑 Stored Authentication Keys</span>
              <span style="color:var(--text-primary)">${s.cookie_count || 0} cookies</span>
            </div>
            <div style="display:flex;flex-wrap:wrap;gap:4px;min-height:22px">
              ${authKeys}
            </div>
          </div>
        </div>

        <!-- Action Row -->
        <div style="padding:12px 16px;background:rgba(0,0,0,0.15);border-top:1px solid rgba(255,255,255,0.04);display:flex;gap:6px;justify-content:space-between;align-items:center">
          <div style="display:flex;gap:4px">
            <button class="btn btn-xs" style="background:linear-gradient(135deg,#A855F7,#7C3AED);color:#fff;border:none;font-weight:700;padding:4px 10px;border-radius:6px" onclick="verifySessionCard('${s.id}', this)">🔍 Verify</button>
            <button class="btn btn-xs btn-outline" style="border-radius:6px" onclick="viewSessionJSON('${s.id}')">📋 JSON</button>
            <button class="btn btn-xs btn-outline btn-primary" style="border-radius:6px" onclick="openSessionBrowser('${s.id}')" title="Launch Playwright screen session">🌐 Browser</button>
            <button class="btn btn-xs btn-outline btn-success" style="border-radius:6px" onclick="saveSessionBrowser('${s.id}', this)" title="Extract and save cookies from open browser">💾 Save</button>
          </div>
          
          <div style="display:flex;gap:4px">
            <button class="btn btn-xs ${s.is_active ? 'btn-danger' : 'btn-success'}" style="border-radius:6px;padding:4px 8px" onclick="toggleSessionActive('${s.id}')">
              ${s.is_active ? 'Deactivate' : 'Activate'}
            </button>
            <button class="btn btn-xs btn-danger" style="border-radius:6px;padding:4px 6px" onclick="deleteSession('${s.id}')" title="Delete Account">🗑️</button>
          </div>
        </div>
      </div>`;
    }).join('');
  }
}


// Separate helper for Card Verification (reloads statistics & status badge without page refresh)
async function verifySessionCard(id, btn) {
  if (btn) {
    btn.disabled = true;
    btn.textContent = '⏳ Checking...';
  }
  try {
    const res = await adminFetch(`/admin/dominos/sessions/${id}/verify`, { method: 'POST' });
    const isOk = res.status === 'valid' || res.status === 'success' || res.is_valid === true;
    showToast(res.message || (isOk ? 'Session verified successfully!' : 'Session verification failed'), isOk ? 'success' : 'error');
    // Full state sync so stats counters update too
    await loadSessions();
  } catch (err) {
    showToast('Verification failed: ' + err.message, 'error');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = '🔍 Verify';
    }
  }
}

async function verifySession(id, btn) {
  return verifySessionCard(id, btn);
}


// View session cookies JSON in a styled in-page modal
async function viewSessionJSON(id) {
  try {
    const res = await adminFetch(`/admin/dominos/sessions/${id}/cookies`);
    
    // Remove any existing JSON viewer modal
    document.getElementById('json-viewer-modal')?.remove();

    const modal = document.createElement('div');
    modal.id = 'json-viewer-modal';
    modal.style.cssText = 'position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,0.75);display:flex;align-items:center;justify-content:center;padding:24px;backdrop-filter:blur(4px)';
    modal.innerHTML = `
      <div style="background:var(--bg-card);border:1px solid rgba(255,255,255,0.12);border-radius:16px;max-width:680px;width:100%;max-height:85vh;display:flex;flex-direction:column;box-shadow:0 24px 80px rgba(0,0,0,0.5)">
        <div style="padding:18px 20px;border-bottom:1px solid rgba(255,255,255,0.08);display:flex;justify-content:space-between;align-items:center">
          <div>
            <div style="font-weight:700;font-size:15px">📋 Session Cookies — +91${res.mobile_number}</div>
            <div style="font-size:12px;color:var(--text-muted);margin-top:2px">${res.cookie_count} cookies &bull; Auth keys: <b style="color:#A855F7">${(res.auth_cookies||[]).join(', ') || 'none'}</b></div>
          </div>
          <button onclick="document.getElementById('json-viewer-modal').remove()" style="background:none;border:none;color:var(--text-muted);font-size:20px;cursor:pointer;padding:4px 8px">✕</button>
        </div>
        <div style="padding:16px;overflow:auto;flex:1">
          <pre id="json-viewer-pre" style="font-size:11.5px;line-height:1.6;color:#E2E8F0;background:rgba(0,0,0,0.3);border-radius:10px;padding:14px;overflow:auto;white-space:pre-wrap;word-break:break-all;max-height:50vh;border:1px solid rgba(255,255,255,0.06)">${escapeHtml(res.cookies_json)}</pre>
        </div>
        <div style="padding:12px 16px;border-top:1px solid rgba(255,255,255,0.08);display:flex;gap:8px">
          <button class="btn btn-primary btn-sm" onclick="copySessionJSON('json-viewer-pre')">📋 Copy JSON</button>
          <button class="btn btn-outline btn-sm" onclick="downloadSessionJSON('${id}','${res.mobile_number}')">⬇️ Download .json</button>
          <button class="btn btn-outline btn-sm" style="margin-left:auto" onclick="document.getElementById('json-viewer-modal').remove()">Close</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
  } catch (err) {
    showToast('Failed to load cookies: ' + err.message, 'error');
  }
}

function copySessionJSON(preId) {
  const text = document.getElementById(preId)?.textContent || '';
  navigator.clipboard.writeText(text).then(() => showToast('Copied to clipboard!', 'success')).catch(() => {
    const ta = document.createElement('textarea'); ta.value = text; ta.style.position='fixed'; ta.style.opacity='0';
    document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove();
    showToast('Copied!', 'success');
  });
}

function downloadSessionJSON(id, mobile) {
  adminFetch(`/admin/dominos/sessions/${id}/cookies`).then(res => {
    const blob = new Blob([res.cookies_json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `dominos_session_${mobile}_${Date.now()}.json`;
    document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
    showToast('JSON downloaded!', 'success');
  }).catch(err => showToast('Download failed: ' + err.message, 'error'));
}

function escapeHtml(str) {
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// Open browser for session on host machine screen (loads saved cookies)
async function openSessionBrowser(id) {
  try {
    showToast('Opening browser with saved session...', 'info');
    const res = await adminFetch(`/admin/dominos/sessions/${id}/open`, { method: 'POST' });
    showToast(res.message, 'success');
  } catch (err) {
    showToast('Failed to open browser: ' + err.message, 'error');
  }
}

// Extract and save cookies/local_storage from open browser session
async function saveSessionBrowser(id, btn) {
  if (btn) {
    btn.disabled = true;
    btn.textContent = '⏳ Saving...';
  }
  try {
    showToast('Extracting cookies and saving session...', 'info');
    const res = await adminFetch(`/admin/dominos/sessions/${id}/save`, { method: 'POST' });
    showToast(res.message, 'success');
    await loadSessions();
  } catch (err) {
    showToast('Failed to save session: ' + err.message, 'error');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = '💾 Save';
    }
  }
}

// Toggle session active status
async function toggleSessionActive(id) {
  try {
    const res = await adminFetch(`/admin/dominos/sessions/${id}/toggle`, { method: 'PUT' });
    showToast(`Session ${res.is_active ? 'activated' : 'deactivated'} successfully!`, 'success');
    loadSessions();
  } catch (err) {
    showToast('Failed to toggle session status', 'error');
  }
}

// Delete session
async function deleteSession(id) {
  if (!confirm('Are you sure you want to delete this Domino\'s account session?')) return;
  try {
    await adminFetch(`/admin/dominos/sessions/${id}`, { method: 'DELETE' });
    showToast('Session deleted successfully', 'success');
    loadSessions();
  } catch (err) {
    showToast('Failed to delete session', 'error');
  }
}

// REMOVED: old extractSessionCookies (replaced by viewSessionJSON)
function extractSessionCookies(id) { viewSessionJSON(id); }


// Session Modals Initialization
function initSessionModals() {
  // Add OTP modal
  const otpModal = document.getElementById('session-otp-modal');
  const btnAddSessionOtp = document.getElementById('btn-add-session-otp');
  const otpModalClose = document.getElementById('session-otp-modal-close');
  const otpModalCancel = document.getElementById('session-otp-modal-cancel');
  
  if (btnAddSessionOtp && otpModal) {
    btnAddSessionOtp.addEventListener('click', () => {
      document.getElementById('otp-step-1').classList.remove('hidden');
      document.getElementById('otp-step-2').classList.add('hidden');
      document.getElementById('session-mobile').value = '';
      document.getElementById('session-request-token').value = ''; // clear old token
      
      // Clear and show status log panel immediately
      const statusContainer = document.getElementById('otp-robot-status');
      const statusLog = document.getElementById('otp-status-log');
      if (statusContainer && statusLog) {
        statusLog.innerHTML = '';
        statusContainer.classList.remove('hidden');
        const line = document.createElement('div');
        line.style.cssText = 'font-size:12px;padding:2px 0;color:#888;';
        line.textContent = 'Waiting for you to enter a mobile number and click Request OTP...';
        statusLog.appendChild(line);
      }
      
      otpModal.classList.remove('hidden');
      // Focus mobile input
      setTimeout(() => document.getElementById('session-mobile')?.focus(), 150);
    });
  }
  
  async function cancelActiveOtpRequest() {
    // Stop any active browser_ready polling timer
    const otpModal2 = document.getElementById('session-otp-modal');
    if (otpModal2 && otpModal2._otpPollTimer) {
      clearInterval(otpModal2._otpPollTimer);
      otpModal2._otpPollTimer = null;
    }
    const token = document.getElementById('session-request-token')?.value;
    // If a session is in progress, ask for confirmation before killing the browser
    if (token) {
      const confirmed = confirm('Cancel this OTP session?\nThis will close the browser and stop the login flow.');
      if (!confirmed) return;
      try {
        await adminFetch('/admin/dominos/sessions/cancel', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ request_token: token })
        });
      } catch (e) {
        console.error('Failed to cancel session request:', e);
      }
    }
    if (otpModal2) otpModal2.classList.add('hidden');
  }

  if (otpModalClose) otpModalClose.addEventListener('click', cancelActiveOtpRequest);
  if (otpModalCancel) otpModalCancel.addEventListener('click', cancelActiveOtpRequest);
  
  // Raw cookies modal
  const rawModal = document.getElementById('raw-cookies-modal');
  const btnImportRawCookies = document.getElementById('btn-import-raw-cookies');
  const rawModalClose = document.getElementById('raw-cookies-modal-close');
  const rawModalCancel = document.getElementById('raw-cookies-modal-cancel');
  
  if (btnImportRawCookies && rawModal) {
    btnImportRawCookies.addEventListener('click', () => {
      document.getElementById('raw-mobile').value = '';
      document.getElementById('raw-cookies-json').value = '';
      rawModal.classList.remove('hidden');
    });
  }
  
  if (rawModalClose) rawModalClose.addEventListener('click', () => rawModal.classList.add('hidden'));
  if (rawModalCancel) rawModalCancel.addEventListener('click', () => rawModal.classList.add('hidden'));
  
  // Send OTP
  const btnSendOtp = document.getElementById('btn-send-otp');
  const sessionManualMode = document.getElementById('session-manual-mode');
  if (sessionManualMode && btnSendOtp) {
    sessionManualMode.addEventListener('change', () => {
      if (sessionManualMode.checked) {
        btnSendOtp.textContent = '🌐 Open Browser & Login Manually';
      } else {
        btnSendOtp.textContent = '🚀 Launch Robot & Request OTP';
      }
    });
  }

  if (btnSendOtp) {
    btnSendOtp.addEventListener('click', async () => {
      if (btnSendOtp.disabled) return;
      AdminState.useManualOtpEndpoint = false;
      const mobile = document.getElementById('session-mobile').value.trim();
      const isManual = sessionManualMode ? sessionManualMode.checked : false;
      if (!/^\d{10}$/.test(mobile)) {
        showToast('Please enter a valid 10-digit mobile number', 'error');
        return;
      }
      
      btnSendOtp.disabled = true;
      btnSendOtp.textContent = isManual ? 'Opening...' : 'Requesting...';
      
      // Clear status log
      // Clear status log & reset browser preview
      const statusContainer = document.getElementById('otp-robot-status');
      const statusLog = document.getElementById('otp-status-log');
      const previewContainer = document.getElementById('otp-browser-preview-container');
      const previewImg = document.getElementById('otp-browser-preview');
      if (previewContainer && previewImg) {
        previewImg.src = '';
        previewContainer.classList.add('hidden');
      }
      if (statusContainer && statusLog) {
        statusContainer.classList.remove('hidden');
        statusLog.innerHTML = '';
        const initLine = document.createElement('div');
        initLine.className = 'otp-log-line';
        initLine.style.cssText = 'font-size:12px;padding:2px 0;color:#ccc;';
        initLine.textContent = `${new Date().toLocaleTimeString()} — 🤖 Contacting server...`;
        statusLog.appendChild(initLine);
      }
      
      try {
        const res = await adminFetch('/admin/dominos/sessions/request', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ mobile_number: mobile, manual_mode: isManual })
        });

        
        if (res.status === 'success') {
          document.getElementById('session-request-token').value = res.request_token;
          document.getElementById('lbl-otp-mobile').textContent = mobile;
          // Show success in log
          if (statusLog) {
            const line = document.createElement('div');
            line.style.cssText = 'font-size:12px;padding:2px 0;color:#51cf66;font-weight:600;';
            line.textContent = isManual 
              ? `${new Date().toLocaleTimeString()} — ✅ Browser window opening. Login manually in the browser...`
              : `${new Date().toLocaleTimeString()} — ✅ Token registered. Stealth browser starting...`;
            statusLog.appendChild(line);
            statusLog.scrollTop = statusLog.scrollHeight;
          }
          
          const otpInput = document.getElementById('session-otp');
          const verifyBtn = document.getElementById('btn-verify-otp');
          if (otpInput && verifyBtn) {
            otpInput.disabled = !isManual;
            otpInput.placeholder = isManual 
              ? 'Login directly in the browser...' 
              : 'Waiting for browser to send OTP...';
            verifyBtn.disabled = !isManual;
          }
          
          document.getElementById('otp-step-1').classList.add('hidden');
          document.getElementById('otp-step-2').classList.remove('hidden');
          document.getElementById('session-otp').value = '';


          // ── Polling fallback: check browser_ready every 3s ──────────────
          // This ensures the OTP input unlocks even if the SSE event was missed
          // (e.g. when running behind Serveo HTTP/2 proxy).
          const requestToken = res.request_token;
          let pollTimer = null;

          function stopOtpPoll() {
            if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
          }

          pollTimer = setInterval(async () => {
            try {
              const st = await adminFetch(`/admin/dominos/sessions/status?token=${requestToken}`);
              if (st && st.login_success) {
                stopOtpPoll();
                showToast('Session verified and stored successfully!', 'success');
                const modal = document.getElementById('session-otp-modal');
                if (modal) modal.classList.add('hidden');
                if (AdminState.currentSection === 'sessions') loadSessions();
                return;
              }
              if (st && st.browser_ready) {
                stopOtpPoll();
                if (otpInput && otpInput.disabled) {
                  otpInput.disabled = false;
                  otpInput.placeholder = 'Enter the 6-digit OTP';
                  if (verifyBtn) verifyBtn.disabled = false;
                  setTimeout(() => otpInput.focus(), 150);
                  if (statusLog) {
                    const line = document.createElement('div');
                    line.className = 'otp-log-line';
                    line.style.cssText = 'font-size:12px;padding:2px 0;color:#51cf66;font-weight:600;';
                    line.textContent = `${new Date().toLocaleTimeString()} — ✅ OTP sent! Enter your code.`;
                    statusLog.appendChild(line);
                    statusLog.scrollTop = statusLog.scrollHeight;
                  }
                }
              } else if (st && st.browser_error) {
                stopOtpPoll();
                if (statusLog) {
                  const line = document.createElement('div');
                  line.className = 'otp-log-line';
                  line.style.cssText = 'font-size:12px;padding:2px 0;color:#ff6b6b;font-weight:600;';
                  line.textContent = `${new Date().toLocaleTimeString()} — ❌ Error: ${st.browser_error}`;
                  statusLog.appendChild(line);
                  statusLog.scrollTop = statusLog.scrollHeight;
                }
                showToast(`Browser error: ${st.browser_error}`, 'error');
              }
            } catch (pollErr) {
              stopOtpPoll();
              showToast('Status polling failed: ' + pollErr.message, 'error');
            }
          }, 3000);
          const otpModal = document.getElementById('session-otp-modal');
          if (otpModal) otpModal._otpPollTimer = pollTimer;

        } else {
          showToast(res.message || 'Failed to request OTP', 'error');
        }
      } catch (err) {
        showToast('Error requesting OTP: ' + err.message, 'error');
      } finally {
        btnSendOtp.disabled = false;
        btnSendOtp.textContent = 'Request OTP';
      }
    });
  }
  
  // Back from OTP step 2
  const btnOtpBack = document.getElementById('btn-otp-back');
  if (btnOtpBack) {
    btnOtpBack.addEventListener('click', () => {
      document.getElementById('otp-step-1').classList.remove('hidden');
      document.getElementById('otp-step-2').classList.add('hidden');
    });
  }
  
  // Verify OTP
  const btnVerifyOtp = document.getElementById('btn-verify-otp');
  if (btnVerifyOtp) {
    btnVerifyOtp.addEventListener('click', async () => {
      if (btnVerifyOtp.disabled) return;
      const otp = document.getElementById('session-otp').value.trim();
      const token = document.getElementById('session-request-token').value;
      if (!otp) {
        showToast('Please enter the OTP verification code', 'error');
        return;
      }
      
      btnVerifyOtp.disabled = true;
      btnVerifyOtp.textContent = 'Verifying...';
      
      const statusText = document.getElementById('otp-status-text');
      if (statusText) {
        statusText.textContent = AdminState.useManualOtpEndpoint 
          ? '✍️ Submitting OTP for manual injection...' 
          : '✍️ Submitting OTP verification code...';
      }
      try {
        const endpoint = AdminState.useManualOtpEndpoint 
          ? '/admin/dominos/sessions/manual_otp' 
          : '/admin/dominos/sessions/verify';
        const res = await adminFetch(endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ request_token: token, otp: otp })
        });
        
        if (res.status === 'success') {
          showToast('Session verified and stored successfully!', 'success');
          otpModal.classList.add('hidden');
          if (AdminState.currentSection === 'sessions') loadSessions();
        } else {
          showToast(res.detail || 'Invalid OTP code', 'error');
        }
      } catch (err) {
        if (err.message.includes('manual fallback')) {
          showToast('⚠️ Auto-fill failed. Switched to Manual OTP Fallback mode.', 'warning');
          AdminState.useManualOtpEndpoint = true;
          const otpInput = document.getElementById('session-otp');
          if (otpInput) {
            otpInput.disabled = false;
            otpInput.placeholder = 'Enter OTP for manual injection';
            otpInput.focus();
          }
          if (statusText) {
            statusText.textContent = '⚠️ Auto-fill failed. Please enter OTP again for manual injection.';
          }
          const statusLog = document.getElementById('otp-status-log');
          if (statusLog) {
            const line = document.createElement('div');
            line.className = 'otp-log-line';
            line.style.cssText = 'font-size:12px;padding:2px 0;line-height:1.5;color:#ff922b;font-weight:600;';
            line.textContent = `${new Date().toLocaleTimeString()} — ⚠️ Auto-fill failed. Switched to manual fallback.`;
            statusLog.appendChild(line);
            statusLog.scrollTop = statusLog.scrollHeight;
          }
        } else {
          showToast('Verification failed: ' + err.message, 'error');
        }
      } finally {
        btnVerifyOtp.disabled = false;
        btnVerifyOtp.textContent = AdminState.useManualOtpEndpoint ? 'Manually Inject OTP' : 'Verify & Save';
      }
    });
  }
  
  // Import Raw Cookies Form Submit
  const rawForm = document.getElementById('raw-cookies-form');
  if (rawForm) {
    rawForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const mobile = document.getElementById('raw-mobile').value.trim();
      const cookiesJson = document.getElementById('raw-cookies-json').value.trim();
      
      if (!/^\d{10}$/.test(mobile)) {
        showToast('Please enter a valid 10-digit mobile number', 'error');
        return;
      }
      
      // Accept both JSON and raw key=value cookie header strings
      // Backend handles both formats — just ensure something was entered
      if (!cookiesJson) {
        showToast('Please paste your cookies JSON or cookie string.', 'error');
        return;
      }
      
      const btnSubmit = rawForm.querySelector('button[type="submit"]');
      btnSubmit.disabled = true;
      btnSubmit.textContent = 'Importing...';
      
      try {
        const res = await adminFetch('/admin/dominos/sessions/raw', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ mobile_number: mobile, cookies_json: cookiesJson })
        });
        
        if (res.status === 'success') {
          showToast('Cookies imported successfully!', 'success');
          rawModal.classList.add('hidden');
          if (AdminState.currentSection === 'sessions') loadSessions();
        } else {
          showToast(res.detail || 'Failed to import cookies', 'error');
        }
      } catch (err) {
        showToast('Import failed: ' + err.message, 'error');
      } finally {
        btnSubmit.disabled = false;
        btnSubmit.textContent = 'Import Session';
      }
    });
  }

  // Refresh live browser preview
  const btnRefreshPreview = document.getElementById('btn-refresh-preview');
  if (btnRefreshPreview) {
    btnRefreshPreview.addEventListener('click', async (e) => {
      e.preventDefault();
      const token = document.getElementById('session-request-token')?.value;
      if (!token) return;
      
      btnRefreshPreview.disabled = true;
      btnRefreshPreview.textContent = '🔄 Refreshing...';
      try {
        const res = await adminFetch('/admin/dominos/sessions/screenshot', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ request_token: token })
        });
        if (res.status === 'success' && res.screenshot) {
          const previewImg = document.getElementById('otp-browser-preview');
          if (previewImg) previewImg.src = res.screenshot;
          showToast('Screenshot refreshed successfully!', 'success');
        } else {
          showToast(res.message || 'No active browser page found', 'error');
        }
      } catch (err) {
        showToast('Refresh failed: ' + err.message, 'error');
      } finally {
        btnRefreshPreview.disabled = false;
        btnRefreshPreview.textContent = '🔄 Refresh Frame';
      }
    });
  }

  // Remote Action Helpers
  async function sendRemoteAction(action, text = null) {
    const token = document.getElementById('session-request-token')?.value;
    if (!token) return;
    try {
      const res = await adminFetch('/admin/dominos/sessions/action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_token: token, action, text })
      });
      if (res.status === 'success') {
        showToast(res.message, 'success');
      } else {
        showToast(res.message || 'Action failed', 'error');
      }
    } catch (err) {
      showToast('Action failed: ' + err.message, 'error');
    }
  }

  const btnRemoteLogin = document.getElementById('btn-remote-login');
  if (btnRemoteLogin) {
    btnRemoteLogin.addEventListener('click', (e) => {
      e.preventDefault();
      sendRemoteAction('click_login');
    });
  }

  const btnRemoteSendOtp = document.getElementById('btn-remote-send-otp');
  if (btnRemoteSendOtp) {
    btnRemoteSendOtp.addEventListener('click', (e) => {
      e.preventDefault();
      sendRemoteAction('click_send_otp');
    });
  }

  const btnRemoteResendOtp = document.getElementById('btn-remote-resend-otp');
  if (btnRemoteResendOtp) {
    btnRemoteResendOtp.addEventListener('click', (e) => {
      e.preventDefault();
      sendRemoteAction('click_resend_otp');
    });
  }

  const btnResendOtpLink = document.getElementById('btn-resend-otp-link');
  if (btnResendOtpLink) {
    btnResendOtpLink.addEventListener('click', (e) => {
      e.preventDefault();
      sendRemoteAction('click_resend_otp');
    });
  }

  const btnRemoteDismissOverlays = document.getElementById('btn-remote-dismiss-overlays');
  if (btnRemoteDismissOverlays) {
    btnRemoteDismissOverlays.addEventListener('click', (e) => {
      e.preventDefault();
      sendRemoteAction('dismiss_overlays');
    });
  }

  const btnRemoteCompleteProfile = document.getElementById('btn-remote-complete-profile');
  if (btnRemoteCompleteProfile) {
    btnRemoteCompleteProfile.addEventListener('click', (e) => {
      e.preventDefault();
      sendRemoteAction('complete_profile');
    });
  }

  const btnRemoteForceSave = document.getElementById('btn-remote-force-save');
  if (btnRemoteForceSave) {
    btnRemoteForceSave.addEventListener('click', async (e) => {
      e.preventDefault();
      btnRemoteForceSave.disabled = true;
      btnRemoteForceSave.textContent = '⏳ Saving...';
      const token = document.getElementById('session-request-token')?.value;
      try {
        const res = await adminFetch('/admin/dominos/sessions/action', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ request_token: token, action: 'force_save' })
        });
        if (res.status === 'success') {
          showToast(res.message, 'success');
          const modal = document.getElementById('session-otp-modal');
          if (modal) {
            modal.classList.add('hidden');
            if (modal._otpPollTimer) {
              clearInterval(modal._otpPollTimer);
              modal._otpPollTimer = null;
            }
          }
          if (AdminState.currentSection === 'sessions') loadSessions();
        } else {
          showToast(res.message || 'Action failed', 'error');
        }
      } catch (err) {
        showToast('Action failed: ' + err.message, 'error');
      } finally {
        btnRemoteForceSave.disabled = false;
        btnRemoteForceSave.textContent = '💾 Force Save';
      }
    });
  }

  const btnRemoteClickSelector = document.getElementById('btn-remote-click-selector');
  const remoteClickSelectorInput = document.getElementById('remote-click-selector');
  if (btnRemoteClickSelector && remoteClickSelectorInput) {
    btnRemoteClickSelector.addEventListener('click', (e) => {
      e.preventDefault();
      const val = remoteClickSelectorInput.value.trim();
      if (!val) return;
      sendRemoteAction('click_selector', val);
      remoteClickSelectorInput.value = '';
    });
    remoteClickSelectorInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        btnRemoteClickSelector.click();
      }
    });
  }


  const btnRemoteType = document.getElementById('btn-remote-type');
  const remoteTypeText = document.getElementById('remote-type-text');
  if (btnRemoteType && remoteTypeText) {
    btnRemoteType.addEventListener('click', (e) => {
      e.preventDefault();
      const val = remoteTypeText.value;
      if (!val) return;
      sendRemoteAction('type_text', val);
      remoteTypeText.value = '';
    });
    remoteTypeText.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        btnRemoteType.click();
      }
    });
  }

  // Keyboard navigation for OTP modal inputs
  const sessionMobile = document.getElementById('session-mobile');
  if (sessionMobile) {
    sessionMobile.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        document.getElementById('btn-send-otp')?.click();
      }
    });
  }

  const sessionOtp = document.getElementById('session-otp');
  if (sessionOtp) {
    sessionOtp.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        document.getElementById('btn-verify-otp')?.click();
      } else if (e.key === 'Escape') {
        e.preventDefault();
        document.getElementById('btn-otp-back')?.click();
      }
    });
  }

  // Global escape key — only close if there's NO active OTP session in progress
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const otpModal = document.getElementById('session-otp-modal');
      const rawModal = document.getElementById('raw-cookies-modal');
      if (otpModal && !otpModal.classList.contains('hidden')) {
        const activeToken = document.getElementById('session-request-token')?.value;
        if (!activeToken) {
          // Safe to close — no session in progress
          otpModal.classList.add('hidden');
        } else {
          // Session in progress — ask for confirmation
          cancelActiveOtpRequest();
        }
      }
      if (rawModal && !rawModal.classList.contains('hidden')) {
        rawModal.classList.add('hidden');
      }
    }
  });
}
initSessionModals();


// =====================================================
// GLOBAL SESSION EXPIRED MODAL
// =====================================================
function showSessionExpiredModal() {
  if (document.getElementById('session-expired-overlay')) return;
  const overlay = document.createElement('div');
  overlay.id = 'session-expired-overlay';
  overlay.style = 'position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.85); display: flex; align-items: center; justify-content: center; z-index: 100000; backdrop-filter: blur(10px);';
  overlay.innerHTML = `
    <div style="background: #1e1e2d; padding: 40px; border-radius: 16px; text-align: center; border: 1px solid #2f2f3f; max-width: 400px; box-shadow: 0 10px 30px rgba(0,0,0,0.5);">
      <span style="font-size: 64px;">⚠️</span>
      <h2 style="margin-top: 20px; font-weight: 700; color: #ffffff;">Session Expired</h2>
      <p style="margin-top: 10px; color: #a2a2b5; line-height: 1.6;">Your session has expired. Please re-authenticate.</p>
      <button onclick="window.location.reload();" style="margin-top: 30px; padding: 12px 24px; border-radius: 8px; border: none; background: #ff4757; color: white; font-weight: 600; cursor: pointer; transition: all 0.2s;">Re-Login</button>
    </div>
  `;
  document.body.appendChild(overlay);
}

// =====================================================
// PAYMENTS & UTR VERIFICATIONS
// =====================================================
async function loadPayments() {
  try {
    const data = await adminFetch('/admin/payments');
    const tbody = document.getElementById('payments-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    
    if (data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align: center;">No payment verification attempts found.</td></tr>';
      return;
    }
    
    data.forEach(p => {
      const tr = document.createElement('tr');
      const formattedDate = new Date(p.created_at).toLocaleString('en-IN');
      let statusClass = 'status-pending';
      let statusLabel = 'Pending Verification';
      if (p.is_successful) {
        statusClass = 'status-active';
        statusLabel = 'Verified';
      } else if (p.order_status === 'Payment Rejected') {
        statusClass = 'status-inactive';
        statusLabel = 'Rejected';
      }
      
      let actionBtn = '';
      if (!p.is_successful && p.order_status !== 'Payment Rejected') {
        actionBtn = `
          <div style="display:flex;gap:4px">
            <button class="btn btn-primary btn-sm" onclick="approvePayment('${p.id}')">Approve</button>
            <button class="btn btn-danger btn-sm" onclick="rejectPayment('${p.id}')">Reject</button>
          </div>
        `;
      } else {
        actionBtn = `<span style="color: var(--text-muted); font-size: 13px;">N/A</span>`;
      }
      
      tr.innerHTML = `
        <td>${p.id}</td>
        <td><code>${p.order_id}</code></td>
        <td><code>${p.utr}</code></td>
        <td>₹${p.order_total.toFixed(2)}</td>
        <td><span class="status-badge ${statusClass}">${statusLabel}</span></td>
        <td>${formattedDate}</td>
        <td>${actionBtn}</td>
      `;
      tbody.appendChild(tr);
    });
  } catch (e) {
    showToast('Failed to load payments: ' + e.message, 'error');
  }
}

async function approvePayment(attemptId) {
  if (!confirm('Are you sure you want to manually approve this payment? This will allocate a gift card and trigger Domino\'s check out.')) return;
  try {
    const res = await adminFetch(`/admin/payments/${attemptId}/approve`, { method: 'POST' });
    showToast(res.message || 'Payment approved and order processed successfully!', 'success');
    loadPayments();
  } catch (e) {
    showToast('Failed to approve payment: ' + e.message, 'error');
  }
}

// Make approvePayment globally accessible from button onclick
window.approvePayment = approvePayment;

// =====================================================
// QR GENERATOR & HISTORY
// =====================================================
async function loadQRHistory() {
  try {
    const data = await adminFetch('/admin/qr-history');
    const tbody = document.getElementById('qr-history-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    
    if (data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4" style="text-align: center;">No generated QRs in history.</td></tr>';
      return;
    }
    
    data.forEach(q => {
      const tr = document.createElement('tr');
      const date = new Date(q.created_at).toLocaleString('en-IN');
      tr.innerHTML = `
        <td><code>${q.order_id}</code></td>
        <td>₹${q.amount.toFixed(2)}</td>
        <td>${date}</td>
        <td><a href="${q.qr_code_url}" target="_blank" class="btn btn-outline btn-sm">View QR</a></td>
      `;
      tbody.appendChild(tr);
    });
  } catch (e) {
    showToast('Failed to load QR history: ' + e.message, 'error');
  }
}

async function generateManualQR() {
  const amountVal = document.getElementById('manual-qr-amount').value.trim();
  const labelVal = document.getElementById('manual-qr-label').value.trim();
  
  if (!amountVal || parseFloat(amountVal) <= 0) {
    showToast('Please enter a valid amount.', 'error');
    return;
  }
  
  try {
    const res = await adminFetch('/admin/qr-generate', {
      method: 'POST',
      body: JSON.stringify({
        amount: parseFloat(amountVal),
        label: labelVal || null
      })
    });
    
    // Show preview card
    const preview = document.getElementById('generated-qr-preview');
    preview.classList.remove('hidden');
    document.getElementById('generated-qr-img').src = res.qr_code_url;
    document.getElementById('generated-qr-text').innerHTML = `UPI reference code: <code>${res.order_id}</code><br>Amount: <b>₹${res.amount.toFixed(2)}</b>`;
    
    // Bind download QR button handler
    const dlBtn = document.getElementById('download-qr-btn');
    dlBtn.onclick = () => {
      if (res.qr_code_url && res.qr_code_url.startsWith('data:image/png;base64,')) {
        const link = document.createElement('a');
        link.href = res.qr_code_url;
        link.download = `UPI-QR-${res.order_id}.png`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      } else {
        window.open(res.qr_code_url, '_blank');
      }
    };
    
    showToast('Manual UPI QR generated successfully!', 'success');
    loadQRHistory();
  } catch (e) {
    showToast('Failed to generate manual QR: ' + e.message, 'error');
  }
}

// Bind QR generator button listener
function initQRListeners() {
  const btn = document.getElementById('generate-manual-qr-btn');
  if (btn) {
    btn.addEventListener('click', generateManualQR);
  }
}
setTimeout(initQRListeners, 1000);

async function downloadReceiptPDF(orderId) {
  try {
    const response = await fetch(`${API}/admin/orders/${orderId}/pdf`, {
      headers: {
        'Authorization': `Bearer ${AdminState.token}`
      }
    });
    if (!response.ok) {
      throw new Error('Failed to generate receipt PDF');
    }
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `receipt-${orderId}.pdf`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
    showToast('Receipt downloaded!', 'success');
  } catch (e) {
    showToast(e.message, 'error');
  }
}
window.downloadReceiptPDF = downloadReceiptPDF;

// =====================================================
// USER SESSIONS MANAGEMENT
// =====================================================
let selectedSessionUserId = null;

async function openUserSessionsModal(userId, displayName) {
  selectedSessionUserId = userId;
  document.getElementById('session-modal-user-name').textContent = displayName;
  document.getElementById('user-sessions-modal').classList.remove('hidden');
  await loadUserSessions(userId);
}
window.openUserSessionsModal = openUserSessionsModal;

// Close User Sessions Modal
document.getElementById('user-sessions-modal-close')?.addEventListener('click', () => {
  document.getElementById('user-sessions-modal').classList.add('hidden');
  selectedSessionUserId = null;
});

async function loadUserSessions(userId) {
  const tbody = document.getElementById('user-sessions-tbody');
  if (!tbody) return;
  tbody.innerHTML = `<tr><td colspan="4" style="text-align:center;padding:20px;">Loading sessions...</td></tr>`;
  try {
    const data = await adminFetch(`/admin/users/${userId}/sessions`);
    if (!data || data.length === 0) {
      tbody.innerHTML = `<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:20px;">No active sessions found</td></tr>`;
      return;
    }
    tbody.innerHTML = data.map(s => `
      <tr>
        <td><code>${s.ip_address || '—'}</code></td>
        <td style="font-size:12px;color:var(--text-muted)">${new Date(s.last_active).toLocaleString('en-IN')}</td>
        <td><span class="badge ${s.is_active ? 'badge-available' : 'badge-cancelled'}">${s.is_active ? 'Active' : 'Expired'}</span></td>
        <td>
          ${s.is_active ? `<button class="btn btn-xs btn-danger" onclick="terminateUserSession('${userId}', '${s.id}')">Terminate</button>` : '—'}
        </td>
      </tr>`).join('');
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="4" style="text-align:center;color:var(--danger-color);padding:20px;">Failed to load: ${e.message}</td></tr>`;
  }
}
window.loadUserSessions = loadUserSessions;

async function terminateUserSession(userId, sessionId) {
  // Double confirmation with password input
  const confirmFirst = confirm("Are you sure you want to terminate this specific user session?");
  if (!confirmFirst) return;
  
  const password = prompt("ADMIN SECURITY VERIFICATION\nPlease enter your administrator password to proceed:");
  if (!password) {
    showToast("Password required for session termination.", "error");
    return;
  }

  try {
    await adminFetch(`/admin/users/${userId}/sessions/${sessionId}`, {
      method: 'DELETE',
      headers: {
        'X-Admin-Password': password
      }
    });
    showToast("Session terminated successfully!", "success");
    await loadUserSessions(userId);
    await loadUsers(); // Refresh counts in parent table
  } catch (e) {
    showToast(e.message || "Failed to terminate session", "error");
  }
}
window.terminateUserSession = terminateUserSession;

// Terminate All Sessions button
document.getElementById('btn-terminate-all-sessions')?.addEventListener('click', async () => {
  if (!selectedSessionUserId) return;
  
  // Double confirmation with password input
  const confirmFirst = confirm("CRITICAL ACTION: Terminate ALL active sessions for this user?");
  if (!confirmFirst) return;
  
  const password = prompt("ADMIN SECURITY VERIFICATION\nPlease enter your administrator password to proceed:");
  if (!password) {
    showToast("Password required for session termination.", "error");
    return;
  }

  try {
    await adminFetch(`/admin/users/${selectedSessionUserId}/sessions/terminate`, {
      method: 'POST',
      headers: {
        'X-Admin-Password': password
      }
    });
    showToast("All sessions terminated successfully!", "success");
    await loadUserSessions(selectedSessionUserId);
    await loadUsers(); // Refresh counts in parent table
  } catch (e) {
    showToast(e.message || "Failed to terminate sessions", "error");
  }
});



// =====================================================
// COUPONS & VOUCHERS CRUD
// =====================================================
async function loadCoupons() {
  try {
    const data = await adminFetch('/admin/coupons');
    const tbody = document.getElementById('coupons-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';
    
    if (data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" style="text-align: center;">No vouchers found.</td></tr>';
      return;
    }
    
    data.forEach(c => {
      const tr = document.createElement('tr');
      const statusClass = c.is_active ? 'status-active' : 'status-inactive';
      const statusLabel = c.is_active ? 'Active' : 'Inactive';
      const redeemersList = c.redeemers && c.redeemers.length > 0 ? c.redeemers.join(', ') : '—';
      
      tr.innerHTML = `
        <td>${c.id}</td>
        <td><code>${c.code}</code></td>
        <td>₹${c.value.toFixed(2)}</td>
        <td>${c.usage_limit}</td>
        <td>${c.redeemed_count}</td>
        <td><span class="status-badge ${statusClass}">${statusLabel}</span></td>
        <td style="max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${redeemersList}">${redeemersList}</td>
        <td>
          <div style="display:flex;gap:4px">
            <button class="btn btn-outline btn-sm" onclick="openEditCouponModal('${c.id}', '${c.code}', ${c.value}, ${c.usage_limit}, ${c.is_active})">Edit</button>
            <button class="btn btn-danger btn-sm" onclick="deleteCoupon('${c.id}')">Delete</button>
          </div>
        </td>
      `;
      tbody.appendChild(tr);
    });
  } catch (e) {
    showToast('Failed to load vouchers: ' + e.message, 'error');
  }
}

function openAddCouponModal() {
  document.getElementById('coupon-modal-title').textContent = 'Add Promo Code';
  document.getElementById('coupon-id-input').value = '';
  document.getElementById('coupon-code-input').value = '';
  document.getElementById('coupon-value-input').value = '';
  document.getElementById('coupon-limit-input').value = '1';
  document.getElementById('coupon-active-input').value = 'true';
  document.getElementById('coupon-modal').classList.remove('hidden');
}

function openEditCouponModal(id, code, value, limit, isActive) {
  document.getElementById('coupon-modal-title').textContent = 'Edit Promo Code';
  document.getElementById('coupon-id-input').value = id;
  document.getElementById('coupon-code-input').value = code;
  document.getElementById('coupon-value-input').value = value;
  document.getElementById('coupon-limit-input').value = limit;
  document.getElementById('coupon-active-input').value = isActive.toString();
  document.getElementById('coupon-modal').classList.remove('hidden');
}

function closeCouponModal() {
  document.getElementById('coupon-modal').classList.add('hidden');
}

async function saveCoupon(event) {
  event.preventDefault();
  const id = document.getElementById('coupon-id-input').value;
  const code = document.getElementById('coupon-code-input').value.trim();
  const value = parseFloat(document.getElementById('coupon-value-input').value);
  const usage_limit = parseInt(document.getElementById('coupon-limit-input').value);
  const is_active = document.getElementById('coupon-active-input').value === 'true';
  
  const payload = { code, value, usage_limit, is_active };
  const method = id ? 'PUT' : 'POST';
  const url = id ? `/admin/coupons/${id}` : '/admin/coupons';
  
  try {
    const res = await adminFetch(url, {
      method,
      body: JSON.stringify(payload)
    });
    showToast(id ? 'Voucher updated successfully!' : 'Voucher created successfully!', 'success');
    closeCouponModal();
    loadCoupons();
  } catch (e) {
    showToast('Failed to save voucher: ' + e.message, 'error');
  }
}

async function deleteCoupon(id) {
  if (!confirm('Are you sure you want to delete this voucher?')) return;
  try {
    await adminFetch(`/admin/coupons/${id}`, { method: 'DELETE' });
    showToast('Voucher deleted successfully!', 'success');
    loadCoupons();
  } catch (e) {
    showToast('Failed to delete voucher: ' + e.message, 'error');
  }
}

async function rejectPayment(attemptId) {
  if (!confirm('Are you sure you want to reject this payment attempt?')) return;
  try {
    const res = await adminFetch(`/admin/payments/${attemptId}/reject`, { method: 'POST' });
    showToast(res.message || 'Payment attempt rejected successfully.', 'success');
    loadPayments();
  } catch (e) {
    showToast('Failed to reject payment: ' + e.message, 'error');
  }
}

// Global window exposure
window.openAddCouponModal = openAddCouponModal;
window.openEditCouponModal = openEditCouponModal;
window.closeCouponModal = closeCouponModal;
window.saveCoupon = saveCoupon;
window.deleteCoupon = deleteCoupon;
window.rejectPayment = rejectPayment;

// Exposed Domino's Session functions to fix click handler references and name mismatch
window.verifySessionCard = verifySessionCard;
window.verifySession = verifySessionCard; // Mapped verifySession to verifySessionCard to fix mismatch
window.viewSessionJSON = viewSessionJSON;
window.copySessionJSON = copySessionJSON;
window.downloadSessionJSON = downloadSessionJSON;
window.openSessionBrowser = openSessionBrowser;
window.saveSessionBrowser = saveSessionBrowser;
window.toggleSessionActive = toggleSessionActive;
window.deleteSession = deleteSession;
