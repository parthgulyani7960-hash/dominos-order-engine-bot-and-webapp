"""
Patch admin panel: 
1. Add Robot Activity nav link + SSE indicator to index.html
2. Add Robot section + live feed panel to index.html
3. Add dominos-card CSS to style.css
4. Patch switchSection in admin.js to load robot section
"""

# ─── 1. PATCH index.html ──────────────────────────────────────────────────────
html = open('app/frontend/admin/index.html', encoding='utf-8').read()

# 1a. Add Robot nav link after Logs link
old_logs_nav = '''        <a href="#" class="nav-link" data-section="logs" id="nav-logs">
          <span class="nav-icon">&#x1F4CB;</span> Logs
        </a>'''
new_logs_nav = old_logs_nav + '''
        <a href="#" class="nav-link" data-section="robot" id="nav-robot">
          <span class="nav-icon">&#x1F916;</span> Robot Live
          <span class="nav-badge hidden" id="robot-badge">0</span>
        </a>'''

# 1b. Replace basic realtime-indicator with SSE dot + label
old_rt = '''          <div class="realtime-indicator" id="realtime-indicator">
            <span class="rt-dot"></span>
            <span>Live</span>
          </div>'''
new_rt = '''          <div class="realtime-indicator" id="realtime-indicator" style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--text-muted)">
            <span id="sse-dot" style="width:8px;height:8px;border-radius:50%;background:#22C55E;display:inline-block;box-shadow:0 0 6px #22C55E;transition:background 0.3s"></span>
            <span id="sse-label">Live</span>
          </div>'''

# 1c. Add Robot section + Live Feed before closing </main>
robot_section = '''
      <!-- ===== ROBOT LIVE SECTION ===== -->
      <section class="section" id="section-robot">
        <div style="display:grid;grid-template-columns:1fr 340px;gap:18px;height:calc(100vh - 100px)">

          <!-- Left: Dominos Robot Cards -->
          <div style="display:flex;flex-direction:column;gap:0">
            <div class="admin-card" style="flex:1;overflow:hidden;display:flex;flex-direction:column">
              <div class="admin-card-header">
                <h3>&#x1F916; Domino&#39;s Robot — Live Order Progress</h3>
                <button class="btn btn-outline btn-sm" onclick="document.getElementById('robot-activity-container').innerHTML='<div class=no-activity-placeholder style=padding:40px;text-align:center;color:var(--text-muted)>No active robot tasks</div>'">Clear</button>
              </div>
              <div id="robot-activity-container" style="flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:12px">
                <div class="no-activity-placeholder" style="padding:60px;text-align:center;color:var(--text-muted)">
                  <div style="font-size:48px;margin-bottom:12px">&#x1F916;</div>
                  <div style="font-size:14px">No active Domino&#39;s tasks right now</div>
                  <div style="font-size:12px;margin-top:6px">Robot progress will appear here when an order is placed</div>
                </div>
              </div>
            </div>
          </div>

          <!-- Right: Live Activity Feed -->
          <div class="admin-card" style="display:flex;flex-direction:column;overflow:hidden">
            <div class="admin-card-header">
              <h3>&#x1F4E1; Activity Feed</h3>
              <button class="btn btn-outline btn-sm" onclick="LiveFeed.events=[];renderLiveFeed();const b=document.getElementById('robot-badge');if(b){b.textContent='0';b.classList.add('hidden');}">Clear</button>
            </div>
            <div id="live-feed-list" style="flex:1;overflow-y:auto;font-size:12px">
              <div style="padding:40px;text-align:center;color:var(--text-muted)">Waiting for events&#x2026;</div>
            </div>
          </div>

        </div>
      </section>

'''

# 1d. Add detail-dominos-status div inside detail panel body (placeholder)
old_detail_body = '          <!-- Populated by JS -->'
new_detail_body = '''          <!-- Populated by JS -->
          <div id="detail-dominos-status"></div>'''

html = html.replace(old_logs_nav, new_logs_nav)
html = html.replace(old_rt, new_rt)
html = html.replace('    </main>', robot_section + '    </main>')
html = html.replace(old_detail_body, new_detail_body)

open('app/frontend/admin/index.html', 'w', encoding='utf-8').write(html)
print('✅ index.html patched')

# ─── 2. PATCH style.css ──────────────────────────────────────────────────────
css = open('app/frontend/admin/style.css', encoding='utf-8').read()

robot_css = """

/* ── Dominos Robot Cards ─────────────────────────────────── */
.dominos-card {
  background: rgba(168, 85, 247, 0.05);
  border: 1px solid rgba(168, 85, 247, 0.25);
  border-radius: 12px;
  padding: 14px 16px;
  transition: border-color 0.3s, box-shadow 0.3s, opacity 0.5s;
  animation: cardSlideIn 0.35s ease;
}
@keyframes cardSlideIn {
  from { opacity: 0; transform: translateY(-10px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ── Overview section two-column layout with live feed ──── */
.overview-with-feed {
  display: grid;
  grid-template-columns: 1fr 300px;
  gap: 18px;
}

/* ── Feed items ─────────────────────────────────────────── */
.feed-item {
  display: flex;
  gap: 10px;
  align-items: flex-start;
  padding: 9px 12px;
  border-bottom: 1px solid rgba(255,255,255,0.04);
  transition: background 0.2s;
}
.feed-item:hover { background: rgba(255,255,255,0.03); }
"""

if '.dominos-card' not in css:
    css += robot_css
    open('app/frontend/admin/style.css', 'w', encoding='utf-8').write(css)
    print('✅ style.css patched')
else:
    print('ℹ️  style.css already has dominos-card styles')

# ─── 3. PATCH admin.js switchSection ─────────────────────────────────────────
js = open('app/frontend/admin/admin.js', encoding='utf-8').read()

old_switch = "  if (name === 'proxies') { loadProxies(); loadProxyLogs(); }"
new_switch = """  if (name === 'proxies') { loadProxies(); loadProxyLogs(); }
  if (name === 'robot') {
    // Clear badge when user visits robot section
    const badge = document.getElementById('robot-badge');
    if (badge) { badge.textContent = '0'; badge.classList.add('hidden'); }
    renderLiveFeed();
  }"""

# Also update the titles map
old_titles = "    analytics: 'Analytics', support: 'Support Chat', analytics: 'Analytics',"
# Find the actual titles object
old_titles = "    analytics: 'Analytics', support: 'Support Chat',"
new_titles = "    analytics: 'Analytics', support: 'Support Chat', robot: 'Robot Live',"

if old_switch in js:
    js = js.replace(old_switch, new_switch)
    print('✅ switchSection patched')
else:
    print('⚠️  switchSection target not found - checking...')
    idx = js.find("loadProxies(); loadProxyLogs()")
    print(f'  found at char {idx}: {js[idx:idx+60]}')

if old_titles in js:
    js = js.replace(old_titles, new_titles)
    print('✅ titles map patched')
else:
    print('⚠️  titles map target not found')

# Fix the titles map - find it regardless
import re
js = re.sub(
    r"(analytics: 'Analytics', support: 'Support Chat')(, analytics: 'Analytics')?",
    "analytics: 'Analytics', support: 'Support Chat', robot: 'Robot Live'",
    js
)

# Also patch renderOrderDetailPanel to include detail-dominos-status section
old_status_hist = "    <!-- Status History -->"
if old_status_hist in js:
    js = js.replace(old_status_hist, 
        "\n    <!-- Dominos Robot Status (populated by SSE) -->\n    <div id=\"detail-dominos-status\"></div>\n\n    <!-- Status History -->")
    print('✅ detail panel Dominos status placeholder added')

open('app/frontend/admin/admin.js', 'w', encoding='utf-8').write(js)
print('✅ admin.js patched')

print('\nAll patches applied successfully!')
