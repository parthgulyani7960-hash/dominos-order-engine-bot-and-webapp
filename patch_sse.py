import sys

lines = open('app/frontend/admin/admin.js', encoding='utf-8').readlines()

new_sse = r"""// =====================================================
// REAL-TIME SSE - Full Live Engine
// =====================================================

const LiveFeed = { events: [], maxEvents: 50 };
const DominosTracker = {};

function initSSE() {
  let retryDelay = 2000;
  function connect() {
    if (AdminState.sseSource) { AdminState.sseSource.close(); AdminState.sseSource = null; }
    const source = new EventSource('/api/events');
    AdminState.sseSource = source;
    source.onopen = () => { retryDelay = 2000; updateSSEIndicator(true); };
    source.onmessage = (evt) => {
      try { handleSSEEvent(JSON.parse(evt.data)); } catch(e) {}
    };
    source.onerror = () => {
      updateSSEIndicator(false);
      source.close(); AdminState.sseSource = null;
      setTimeout(connect, retryDelay);
      retryDelay = Math.min(retryDelay * 1.5, 30000);
    };
  }
  connect();
}

function updateSSEIndicator(connected) {
  const dot = document.getElementById('sse-dot');
  const label = document.getElementById('sse-label');
  if (dot) dot.style.background = connected ? '#22C55E' : '#EF4444';
  if (label) label.textContent = connected ? 'Live' : 'Reconnecting...';
}

function handleSSEEvent(data) {
  const ts = new Date().toLocaleTimeString('en-IN', { hour12: false });
  if (data.type === 'new_order') {
    pushLiveFeed('new_order', `New order <b>${data.order_id}</b> &mdash; Rs.${(data.total||0).toFixed(0)} from <b>${data.user||'User'}</b>`, ts, data.order_id);
    showToast(`New order ${data.order_id} - Rs.${(data.total||0).toFixed(0)}`, 'success');
    loadOrders().then(() => { highlightOrderRow(data.order_id); if (AdminState.currentSection === 'overview') loadOverview(); });
    bumpMetric('metric-active-val', 1);
  } else if (data.type === 'order_update') {
    pushLiveFeed('order_update', `Order <b>${data.order_id||'update'}</b> changed to <b>${data.status||'status'}</b>`, ts, data.order_id);
    if (data.order_id && data.status) {
      updateOrderRowInPlace(data.order_id, data.status, data.status_icon);
      showToast(`${data.order_id}: ${data.status}`, 'info');
    } else { loadOrders(); }
    if (AdminState.currentSection === 'overview') loadOverview();
  } else if (data.type === 'dominos_progress') {
    handleDominosProgress(data, ts);
  } else if (data.type === 'error_alert') {
    pushLiveFeed('error', data.message, ts, null);
    showToast(data.message, 'error');
  } else if (data.type === 'user_login') {
    pushLiveFeed('user_login', `${data.display_name||'User'} just opened the app`, ts, null);
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

"""

# Replace lines 128-149 (0-indexed: 127-148) with new content
new_lines = lines[:127] + [new_sse + '\n'] + lines[149:]
open('app/frontend/admin/admin.js', 'w', encoding='utf-8').writelines(new_lines)
print(f'Done! Total lines now: {len(new_lines)}')
