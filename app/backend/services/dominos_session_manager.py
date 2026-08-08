"""
dominos_session_manager.py
Advanced robot for automated Domino's India OTP login.

Architecture:
  ┌─ request_otp()       → launches _run_otp_browser() as a background task
  ├─ _run_otp_browser()  → navigates + triggers OTP (headless-safe)
  ├─ verify_otp()        → fills OTP digits + extracts session cookies
  ├─ _monitor_manual_login() → background watcher (auto-saves on manual login)
  └─ helpers: human_type, mouse_move_human, smart_wait, fill_otp_boxes
"""

import uuid
import datetime
import json
import asyncio
import os
import sys
import math
import random
import logging
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.orm import Session
from playwright.async_api import async_playwright, Page, BrowserContext
from ..database import DominosOTPRequest, DominosSession, User, RobotLog

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

LOGIN_COOKIES = {
    "customerId", "customer_id", "token", "ACCESS_TOKEN", "access_token",
    "custToken", "auth_token", "authorization", "user_token", "customer_token",
    "dominos_token", "JFL_USER", "JFL_SESSION", "ci_session", "session_id",
    "user_id", "PHPSESSID", "JSESSIONID", "ut", "at", "ct", "customerDetails",
    "auth", "jwt", "Bearer", "_dominos_session"
}

DOMINOS_URLS = [
    "https://pizzaonline.dominos.co.in",
    "https://www.dominos.co.in",
    "https://dominos.co.in",
]

# Desktop User Agents that look real and represent standard PC browsers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
]

# ─────────────────────────────────────────────────────────────────────────────
# In-memory session store
# ─────────────────────────────────────────────────────────────────────────────

ACTIVE_OTP_REQUESTS: Dict[str, Dict[str, Any]] = {}


def generate_request_token() -> str:
    return uuid.uuid4().hex


def log_robot_event(
    db: Session,
    mobile: str,
    level: str,
    stage: str,
    message: str,
    details: dict = None,
    session_id: Optional[str] = None,
):
    """Insert a RobotLog row. Silently swallows DB errors so logging never breaks the bot."""
    try:
        entry = RobotLog(
            session_id=session_id,
            mobile_number=mobile,
            level=level,
            stage=stage,
            message=message,
            details=details or {},
            created_at=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
        )
        db.add(entry)
        db.commit()

        # Dynamic broadcast to avoid circular import issues
        from .. import routes
        callback = getattr(routes, "sse_broadcast_callback", None)
        if callback:
            try:
                import asyncio
                asyncio.create_task(callback({
                    "type": "robot_log",
                    "log": {
                        "id": entry.id,
                        "session_id": entry.session_id,
                        "mobile_number": entry.mobile_number,
                        "level": entry.level,
                        "stage": entry.stage,
                        "message": entry.message,
                        "details": entry.details,
                        "created_at": entry.created_at.isoformat(),
                    }
                }))
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"[RobotLog] Failed to write log: {e}")
        try:
            db.rollback()
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────────────────────
# SSE Broadcasting
# ─────────────────────────────────────────────────────────────────────────────

async def broadcast_status(token: str, message: str, take_screenshot: bool = False):
    """Push a status log line via SSE.  Optionally capture a fresh screenshot."""
    req_data = ACTIVE_OTP_REQUESTS.get(token)
    mobile = "unknown"
    if req_data is not None:
        req_data["last_status"] = message
        mobile = req_data.get("mobile_number", "unknown")

    logger.info(f"[OTP {token[:8]}] {message}")

    from .. import routes
    callback = getattr(routes, "sse_broadcast_callback", None)
    if callback:
        try:
            await callback({
                "type": "dominos_otp_status",
                "request_token": token,
                "status": message,
                "screenshot": req_data.get("last_screenshot") if req_data else None,
            })
        except Exception:
            pass

    # Record in database RobotLog so the Admin Robot Live Log is fully updated in real-time
    from ..database import SessionLocal
    db = SessionLocal()
    try:
        level = "INFO"
        if "❌" in message or "error" in message.lower() or "failed" in message.lower():
            level = "ERROR"
        elif "⚠️" in message:
            level = "WARNING"
        log_robot_event(
            db,
            mobile=mobile,
            level=level,
            stage="otp_flow",
            message=message,
            details={"request_token": token}
        )
    finally:
        db.close()

    if take_screenshot and req_data and req_data.get("page"):
        asyncio.create_task(
            _capture_screenshot(token, req_data["page"], message)
        )


async def capture_and_broadcast_screenshot(token: str, page: Page):
    """Capture page screenshot, convert to base64 data URL, update req_data, and broadcast."""
    req_data = ACTIVE_OTP_REQUESTS.get(token)
    if not req_data or not page:
        return
    # Ensure page is alive before taking screenshot.
    if not await is_page_alive(page):
        return
    try:
        # Wait briefly for any pending navigation/render before screenshot
        await asyncio.sleep(0.8)
        try:
            await page.wait_for_load_state("networkidle", timeout=3000)
        except Exception:
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=1500)
            except Exception:
                pass
        # Take a screenshot as bytes
        screenshot_bytes = await page.screenshot(type="jpeg", quality=60, full_page=False)
        import base64
        b64_str = base64.b64encode(screenshot_bytes).decode("utf-8")
        data_url = f"data:image/jpeg;base64,{b64_str}"
        
        req_data["last_screenshot"] = data_url
        
        # Broadcast via SSE
        from .. import routes
        callback = getattr(routes, "sse_broadcast_callback", None)
        if callback:
            await callback({
                "type": "dominos_otp_status",
                "request_token": token,
                "status": req_data.get("last_status", "Screenshot updated"),
                "screenshot": data_url,
            })
    except Exception as e:
        err_msg = str(e)
        if "Target page, context or browser has been closed" not in err_msg and "Target closed" not in err_msg and "Protocol error" not in err_msg:
            logger.warning(f"Failed to capture and broadcast screenshot: {e}")


async def _capture_screenshot(token: str, page: Page, status_msg: str = ""):
    await capture_and_broadcast_screenshot(token, page)


async def is_page_alive(page: Page) -> bool:
    """
    Robustly checks whether a Playwright Page is actually usable.
    is_closed() only checks the Python-side flag; a broken CDP/WebSocket
    connection can leave is_closed() == False while the page is unusable.
    We probe with a trivial JS evaluate() to confirm the connection is live.
    """
    if page is None:
        return False
    try:
        if page.is_closed():
            return False
            
        context = page.context
        if not context:
            return False
            
        browser = context.browser
        if browser is not None and not browser.is_connected():
            return False

        # Lightweight JS roundtrip to confirm CDP connection is alive
        try:
            await page.evaluate("() => 1", timeout=800)
            return True
        except Exception as e:
            err_msg = str(e).lower()
            
            # Common patterns indicating the page/browser is actually closed, crashed or gone
            critical_errors = [
                "closed", 
                "crashed", 
                "not opened", 
                "no longer exists", 
                "connection closed",
                "target closed",
                "browser has been closed",
                "context has been closed",
                "invalid session id",
                "websocket"
            ]
            
            # Check if it's just a transient busy/navigating state
            transient_patterns = [
                "timeout",
                "navigation",
                "execution context",
                "destroyed",
                "loading",
                "interrupted",
                "detached",
                "frame"
            ]
            
            if any(k in err_msg for k in transient_patterns):
                # Page is alive, just busy or transitioning
                return True
                
            if any(k in err_msg for k in critical_errors):
                # Page is dead, need recovery
                return False
                
            # If the page is not closed according to Playwright, assume it is alive
            if not page.is_closed():
                return True
                
            return False
            
    except Exception as e:
        logger.warning(f"[is_page_alive] Outer check exception: {e}")
        return False


async def recover_page_if_needed(token: str, page: Page) -> Optional[Page]:
    """
    If the given page is dead (closed or CDP connection broken), attempts to
    open a new page from the existing browser context and navigate back to
    the last known URL.  Updates ACTIVE_OTP_REQUESTS[token]['page'] with the
    fresh page so all subsequent callers automatically use it.
    Returns the live page (either original or recovered), or None if recovery
    is impossible.
    """
    req_data = ACTIVE_OTP_REQUESTS.get(token)
    if not req_data:
        return page

    if req_data.get("recovering"):
        logger.info(f"[PageRecovery] Recovery already in progress for token {token[:8]}. Skipping.")
        return req_data.get("page") or page

    if await is_page_alive(page):
        return page  # fast path — nothing to do

    context: Optional[BrowserContext] = req_data.get("context")
    if not context:
        return None

    # Set recovering flag to prevent loops
    req_data["recovering"] = True
    ACTIVE_OTP_REQUESTS[token] = req_data

    # Try to get the last known URL from the dead page (may still be readable)
    last_url = req_data.get("last_url") or "https://m.dominos.co.in/login"
    try:
        if page and not page.is_closed():
            last_url = page.url or last_url
            # Close the old dead/broken page to avoid leaking tabs
            try:
                await page.close()
            except Exception:
                pass
    except Exception:
        pass

    logger.warning(f"[PageRecovery] Page for token {token[:8]} is dead (url={last_url}). Opening a fresh page from existing context…")

    try:
        # Close ALL existing pages in the context to prevent multiple tabs
        for existing_page in context.pages:
            try:
                await existing_page.close()
            except Exception:
                pass
        new_page = await context.new_page()
        await apply_stealth(new_page)
        try:
            await new_page.goto(last_url, wait_until="domcontentloaded", timeout=20000)
        except Exception as nav_err:
            logger.warning(f"[PageRecovery] Navigation to {last_url} failed: {nav_err}. Page still usable.")

        # Update the shared state so everyone gets the new page
        req_data["page"] = new_page
        req_data["last_url"] = last_url
        ACTIVE_OTP_REQUESTS[token] = req_data

        await broadcast_status(
            token,
            "🔄 Browser page reconnected — the previous tab was lost but a fresh tab has been opened at the same URL.",
            take_screenshot=False,
        )
        logger.info(f"[PageRecovery] Successfully recovered page for token {token[:8]}")
        return new_page
    except Exception as e:
        logger.error(f"[PageRecovery] Failed to recover page for token {token[:8]}: {e}")
        await broadcast_status(
            token,
            f"❌ Browser page lost and could not be recovered: {e}. Please start a new session.",
        )
        return None
    finally:
        if token in ACTIVE_OTP_REQUESTS:
            ACTIVE_OTP_REQUESTS[token]["recovering"] = False


# ─────────────────────────────────────────────────────────────────────────────
# Human-like interaction helpers
# ─────────────────────────────────────────────────────────────────────────────

# ── Import fast human-action primitives ───────────────────────────────────────
from .human_actions import (
    mouse_move as mouse_move_human,   # keep old name for call-site compatibility
    human_click,
    human_click_el,
    human_type,
    fill_form_fields,
    fill_otp_fast,
    fast_wait as smart_wait_fn,
    wait_for_any,
    poll_until,
    inject_human_signals,
    random_micro_action,
    human_scroll,
)


async def _fire_react_events(page: Page, selector: str):
    """Force React/Vue state update by dispatching native input events."""
    await page.evaluate(f"""() => {{
        const el = document.querySelector('{selector}');
        if (!el) return;
        const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
        if (nativeSetter) nativeSetter.call(el, el.value);
        ['input', 'change'].forEach(ev =>
            el.dispatchEvent(new Event(ev, {{ bubbles: true, cancelable: true }}))
        );
        el.dispatchEvent(new KeyboardEvent('keyup', {{ bubbles: true }}));
        el.dispatchEvent(new KeyboardEvent('keydown', {{ bubbles: true }}));
    }}""")


async def smart_wait(page: Page, ms_min: int = 150, ms_max: int = 350):
    """Faster human-paced wait (was 300-700ms, now 150-350ms)."""
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=150)
    except Exception:
        pass
    await asyncio.sleep(random.uniform(ms_min / 1000, ms_max / 1000))


async def dismiss_overlays(page: Page):
    """Dismiss common overlays: cookie banners, 'allow location', Skip screens."""
    dismiss_texts = [
        "Skip", "SKIP", "skip",
        "Continue", "CONTINUE", "continue",
        "Allow", "ALLOW", "allow",
        "Allow Location", "Locate Me", "LOCATE ME",
        "Use current location",
        "Accept", "ACCEPT", "accept",
        "OK", "Ok", "ok",
        "Got it", "Dismiss", "No thanks", "Close", "×", "✕",
    ]
    for text in dismiss_texts:
        for sel in (
            f'button:has-text("{text}")',
            f'span:has-text("{text}")',
            f'div:has-text("{text}")',
            f'a:has-text("{text}")',
            f'[aria-label="{text}"]',
            f'.modal button:has-text("{text}")',
        ):
            try:
                if await page.is_visible(sel, timeout=300):
                    await page.click(sel, timeout=500)
                    await asyncio.sleep(0.25)
                    break
            except Exception:
                continue



# ─────────────────────────────────────────────────────────────────────────────
def get_otp_fill_timeout() -> int:
    """Retrieve OTP fill timeout from environment variable or default to 5000 ms."""
    try:
        return int(os.getenv("OTP_FILL_TIMEOUT", "5000"))
    except ValueError:
        return 5000

async def wait_for_stable_page(page: Page, timeout: int = 3000) -> bool:
    """Wait for network idle and a short pause to ensure page stability."""
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout)
        await asyncio.sleep(0.5)  # 500 ms pause for DOM stability
        return True
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────────────
# Page state detection
# ─────────────────────────────────────────────────────────────────────────────

async def detect_page_state(page: Page) -> str:
    """
    Returns one of:
      'otp_boxes'   — 6 individual digit input boxes are visible
      'otp_single'  — single OTP input field is visible
      'mobile_form' — mobile number entry field is visible
      'logged_in'   — user appears to be already logged in
      'unknown'     — none of the above matched
    """
    for attempt in range(4):
        try:
            # Check for single-char OTP boxes (maxlength=1)
            single_char = await page.query_selector_all('input[maxlength="1"]')
            visible_sc = []
            for b in single_char:
                try:
                    if await b.is_visible():
                        visible_sc.append(b)
                except Exception:
                    pass
            if len(visible_sc) >= 4:
                return "otp_boxes"

            # Check for single OTP input
            for sel in (
                'input[autocomplete="one-time-code"]',
                'input[placeholder*="OTP"]', 'input[placeholder*="otp"]',
                'input[name="otp"]', 'input[id*="otp"]',
                'input[maxlength="6"]', 'input[maxlength="4"]',
            ):
                try:
                    if await page.is_visible(sel, timeout=250):
                        return "otp_single"
                except Exception:
                    continue

            # Check for mobile input
            for sel in (
                '#loginNumber', 'input[name="loginNumber"]',
                'input[type="tel"]', 'input[placeholder*="Mobile"]',
                'input[placeholder*="mobile"]', 'input[name="mobile"]',
                'input[placeholder*="Phone"]', 'input[placeholder*="Number"]',
            ):
                try:
                    if await page.is_visible(sel, timeout=250):
                        return "mobile_form"
                except Exception:
                    continue

            # Check if logged in (cookie, URL, DOM elements, or LocalStorage)
            cookies = await page.context.cookies()
            if any(c.get("name") in LOGIN_COOKIES for c in cookies):
                return "logged_in"
                
            curr_url = page.url.lower()
            if "login" not in curr_url and any(path in curr_url for path in ("/menu", "/home", "/cart", "/checkout", "/account", "/profile")):
                if len(cookies) > 2:
                    return "logged_in"

            # Check for logged-in UI elements
            has_logged_in_ui = await page.evaluate("""() => {
                const navText = document.body ? document.body.innerText : '';
                return navText.includes('Logout') || navText.includes('My Account') || navText.includes('Track Order') ||
                       document.querySelector('.user-profile, [data-testid="user-profile"], .logout-btn') !== null;
            }""")
            if has_logged_in_ui:
                return "logged_in"

            break  # Success, exit loop

        except Exception as e:
            err_str = str(e)
            if "Execution context" in err_str or "navigation" in err_str or "context was destroyed" in err_str:
                logger.warning(f"[State detect] Execution context destroyed on attempt {attempt + 1}. Retrying in 1.5s...")
                await asyncio.sleep(1.5)
                continue
            else:
                logger.debug(f"[State detect] Non-fatal exception: {e}")
                break

    return "unknown"


async def wait_for_react_render(page: Page, timeout: float = 8.0) -> bool:
    """Wait for the Domino's React SPA to finish initial rendering."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            # React renders at least some interactive elements when ready
            result = await page.evaluate("""() => {
                const body = document.body;
                if (!body) return false;
                // Check if React has mounted any meaningful content
                const interactives = body.querySelectorAll('input, button, a[href]');
                return interactives.length > 0 && body.innerHTML.length > 500;
            }""")
            if result:
                return True
        except Exception:
            pass
        await asyncio.sleep(0.3)
    return False



# ─────────────────────────────────────────────────────────────────────────────
# OTP filling (core robot action)
# ─────────────────────────────────────────────────────────────────────────────

async def fill_otp_boxes(page: Page, otp: str, token: str) -> bool:
    """Fill the 6‑digit OTP using a robust multi‑attempt strategy.

    The function attempts up to five fills, adding stabilization waits,
    explicit visibility checks, and detailed SSE logging.  A configurable
    timeout (OTP_FILL_TIMEOUT) controls the wait durations.
    """
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        await broadcast_status(token, f"Attempt {attempt}/{max_attempts}: Filling OTP...", take_screenshot=False)
        # Ensure the page is stable before interacting
        await wait_for_stable_page(page, timeout=get_otp_fill_timeout())
        # Explicitly wait for individual OTP boxes to appear (if they exist)
        try:
            await page.wait_for_selector('input[maxlength="1"]', state='visible', timeout=get_otp_fill_timeout())
        except Exception:
            # Box selector not found – the page may use a single field; continue
            pass
        # Perform a single fill attempt using the existing tiered strategy
        filled = await _fill_otp_once(page, otp, token)
        if filled:
            await broadcast_status(token, "✅ OTP filled successfully.", take_screenshot=True)
            return True
        # Back‑off before the next attempt
        await asyncio.sleep(0.4)
    
    await broadcast_status(token, "❌ All OTP fill attempts failed. Falling back to manual OTP entry.", take_screenshot=True)
    
    # Emit SSE manual fallback event to routes
    from .. import routes
    callback = getattr(routes, "sse_broadcast_callback", None)
    if callback:
        try:
            req_data = ACTIVE_OTP_REQUESTS.get(token) or {}
            await callback({
                "type": "otp_manual_fallback",
                "request_token": token,
                "mobile_number": req_data.get("mobile_number", "unknown")
            })
        except Exception as se:
            logger.error(f"Failed to emit sse manual fallback event: {se}")

    return False

async def _fill_otp_once(page: Page, otp: str, token: str) -> bool:
    """Single attempt of OTP filling using the original tiered logic.

    Returns ``True`` if the OTP was entered successfully, otherwise ``False``.
    """
    # Wait for any pending navigation / redirects to settle before touching the DOM
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=5000)
    except Exception:
        pass
    await asyncio.sleep(0.3)

    # Robust backspace and reset of any old OTP digits
    try:
        await page.evaluate("""
            () => {
                const inputs = Array.from(document.querySelectorAll('input'))
                    .filter(i => i.offsetParent !== null && i.type !== 'hidden');
                for (const inp of inputs) {
                    inp.value = '';
                    const ns = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
                    if (ns) ns.call(inp, '');
                    inp.dispatchEvent(new Event('input',  {bubbles:true}));
                    inp.dispatchEvent(new Event('change', {bubbles:true}));
                }
            }
        """)
    except Exception as cle:
        if "Target page, context or browser has been closed" not in str(cle) and "Target closed" not in str(cle):
            logger.warning(f"Failed to clear old OTP fields: {cle}")

    # ── Tier 1: Per-box keyboard simulation (most reliable for React) ────
    # Domino's React OTP form requires keydown+keypress PER BOX before enabling button
    try:
        logger.info("[OTP Fill] Attempting per-box React keyboard simulation...")
        sc_count = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('input[maxlength="1"]'))
                .filter(i => i.offsetParent !== null).length;
        }""")
        if sc_count >= 4:
            result = await page.evaluate(f"""() => {{
                const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
                const boxes = Array.from(document.querySelectorAll('input[maxlength="1"]'))
                    .filter(i => i.offsetParent !== null);
                const digits = '{otp}'.split('');
                let filled = 0;
                boxes.forEach((box, idx) => {{
                    const ch = digits[idx] || '';
                    if (!ch) return;
                    // 1. Focus the box
                    box.focus();
                    // 2. Clear existing value
                    if (nativeSetter) nativeSetter.call(box, '');
                    box.dispatchEvent(new Event('input', {{bubbles: true}}));
                    // 3. Fire keydown (React listens to this for validation)
                    box.dispatchEvent(new KeyboardEvent('keydown', {{
                        key: ch, code: 'Digit' + ch, keyCode: ch.charCodeAt(0),
                        which: ch.charCodeAt(0), bubbles: true, cancelable: true
                    }}));
                    // 4. Fire keypress
                    box.dispatchEvent(new KeyboardEvent('keypress', {{
                        key: ch, code: 'Digit' + ch, keyCode: ch.charCodeAt(0),
                        which: ch.charCodeAt(0), bubbles: true, cancelable: true
                    }}));
                    // 5. Set the value via React native setter
                    if (nativeSetter) nativeSetter.call(box, ch);
                    else box.value = ch;
                    // 6. Fire input (React state update)
                    box.dispatchEvent(new Event('input', {{bubbles: true}}));
                    box.dispatchEvent(new Event('change', {{bubbles: true}}));
                    // 7. Fire keyup
                    box.dispatchEvent(new KeyboardEvent('keyup', {{
                        key: ch, code: 'Digit' + ch, keyCode: ch.charCodeAt(0),
                        which: ch.charCodeAt(0), bubbles: true
                    }}));
                    // 8. Move focus to next box (Domino's React auto-focuses next on keyup)
                    if (idx + 1 < boxes.length) {{
                        boxes[idx + 1].focus();
                    }} else {{
                        box.blur();
                    }}
                    filled++;
                }});
                // Force-enable the submit button after all digits filled
                const btns = Array.from(document.querySelectorAll('button, input[type="submit"]'));
                btns.forEach(btn => {{
                    if (btn.disabled && btn.offsetParent !== null) {{
                        btn.disabled = false;
                        btn.removeAttribute('disabled');
                        // Remove any CSS classes that make it grey
                        btn.classList.remove('disabled', 'btn--disabled', 'inactive');
                    }}
                }});
                return filled;
            }}""")
            if result and int(result) >= len(otp):
                logger.info(f"[OTP Fill] Per-box React simulation filled {result} boxes ✅")
                await asyncio.sleep(0.3)  # Let React process state changes
                return True
            logger.warning(f"[OTP Fill] Per-box simulation only filled {result} boxes. Trying Tier 2.")
    except Exception as je:
        logger.warning(f"[OTP Fill] Per-box simulation failed: {je}. Falling back to keyboard typing...")

    # ── Tier 2: Fast JS React-native Value Injection ─────────────────────
    try:
        logger.info("[OTP Fill] Attempting fast JS React-native value injection...")
        js_filled = await page.evaluate(f"""
            () => {{
                const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
                function setReact(el, val) {{
                    el.focus();
                    if (nativeSetter) nativeSetter.call(el, val);
                    ['keydown','keypress','input','change','keyup'].forEach(evName => {{
                        let ev;
                        if (evName.startsWith('key')) {{
                            ev = new KeyboardEvent(evName, {{key: val, bubbles: true, cancelable: true}});
                        }} else {{
                            ev = new Event(evName, {{bubbles: true}});
                        }}
                        el.dispatchEvent(ev);
                    }});
                }}
                const all = Array.from(document.querySelectorAll('input'))
                    .filter(i => i.offsetParent !== null && i.type !== 'hidden');

                // Try single-char boxes
                const sc = all.filter(i => i.maxLength === 1 || i.getAttribute('maxlength')==='1');
                if (sc.length >= 4) {{
                    '{otp}'.split('').forEach((ch, i) => {{ if (sc[i]) setReact(sc[i], ch); }});
                    // Force-enable submit button
                    document.querySelectorAll('button[disabled], button.disabled').forEach(b => {{
                        b.disabled = false; b.removeAttribute('disabled');
                    }});
                    return 'sc:' + sc.length;
                }}

                // Try OTP-matching input
                const otpEl = all.find(i =>
                    i.maxLength === 6 || i.maxLength === 4 ||
                    (i.placeholder || '').toLowerCase().match(/otp|code|verif|digit/) ||
                    (i.name || '').toLowerCase().includes('otp') ||
                    (i.id   || '').toLowerCase().includes('otp')
                ) || all.find(i => i.value.length < 4 && !i.readOnly);

                if (otpEl) {{ setReact(otpEl, '{otp}'); return 'single'; }}
                return false;
            }}
        """)
        if js_filled:
            logger.info(f"[OTP Fill] Fast JS injection success: {js_filled}")
            return True
    except Exception as je:
        logger.warning(f"[OTP Fill] Fast JS injection failed: {je}. Falling back to keyboard typing...")

    # ── Tier 3: Individual maxlength=1 boxes (keyboard fallback) ─────────
    state = await detect_page_state(page)
    logger.info(f"[OTP Fill] Page state: {state}")

    if state == "otp_boxes":
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=4000)
        except Exception:
            pass
        try:
            single_char_inputs = await page.query_selector_all('input[maxlength="1"]')
        except Exception as nav_err:
            logger.warning(f"[OTP Fill A] Context error after nav guard: {nav_err}")
            single_char_inputs = []
        visible = [b for b in single_char_inputs if await _is_visible_safe(b)]
        await broadcast_status(token, f"✍️ Filling {len(visible)} OTP digit boxes…")
        for idx, box in enumerate(visible[: len(otp)]):
            try:
                box_bb = await box.bounding_box()
                if box_bb:
                    await mouse_move_human(
                        page,
                        int(box_bb["x"] + box_bb["width"] / 2),
                        int(box_bb["y"] + box_bb["height"] / 2),
                    )
                await box.click(timeout=2000)
                await asyncio.sleep(random.uniform(0.04, 0.10))
                await box.fill("", timeout=500)
                await asyncio.sleep(random.uniform(0.04, 0.08))
                await box.focus()
                for ch in otp[idx]:
                    await page.keyboard.press(ch, delay=random.randint(60, 120))
                await asyncio.sleep(random.uniform(0.08, 0.15))
                await page.evaluate(f"""
                    () => {{
                        const boxes = Array.from(document.querySelectorAll('input[maxlength="1"]'))
                            .filter(i => i.offsetParent !== null);
                        const el = boxes[{idx}];
                        if (!el) return;
                        const ns = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
                        if (ns) ns.call(el, '{otp[idx]}');
                        el.dispatchEvent(new Event('input',  {{bubbles:true}}));
                        el.dispatchEvent(new Event('change', {{bubbles:true}}));
                        el.dispatchEvent(new KeyboardEvent('keyup', {{bubbles:true}}));
                    }}
                """)
            except Exception as ex:
                logger.warning(f"[OTP Fill A] Box #{idx}: {ex}")
        filled_count = await page.evaluate("""
            () => {
                return Array.from(document.querySelectorAll('input[maxlength="1"]'))
                    .filter(i => i.offsetParent !== null && i.value.length > 0).length;
            }
        """)
        if filled_count == len(otp):
            logger.info(f"[OTP Fill A] Verified: all {filled_count} boxes filled successfully")
            return True
        else:
            logger.warning(f"[OTP Fill A] Only {filled_count}/{len(otp)} boxes were filled. Falling back to Tier B/C...")

    # ── Tier 3: Single unified OTP input (Keyboard fallback) ───────────
    otp_selectors = (
        'input[autocomplete="one-time-code"]',
        'input[placeholder*="OTP"]',  'input[placeholder*="otp"]',
        'input[name="otp"]',           'input[name="OTP"]',
        'input[id*="otp"]',            'input[class*="otp"]',
        'input[id*="digit"]',          'input[class*="digit"]',
        'input[data-testid="otp-input"]',
        'input[placeholder*="Enter"]', 'input[placeholder*="Code"]',
        'input[placeholder*="Verif"]',
        'input[maxlength="6"]',        'input[maxlength="4"]',
        '.otp-input',
        'input[type="number"]',        'input[type="tel"]',
    )
    for sel in otp_selectors:
        try:
            if await page.is_visible(sel, timeout=1200):
                await page.fill(sel, "", timeout=500)
                await human_type(page, sel, otp)
                logger.info(f"[OTP Fill B] Entered via '{sel}'")
                return True
        except Exception:
            continue

    # ── Tier 4: JS React-native full dispatch (Last resort) ────────────────
    logger.info("[OTP Fill C] JS React-native dispatch")
    filled = await page.evaluate(f"""
        () => {{
            const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
            function setReact(el, val) {{
                if (nativeSetter) nativeSetter.call(el, val);
                ['input','change'].forEach(ev => el.dispatchEvent(new Event(ev, {{bubbles:true}})));
                el.dispatchEvent(new KeyboardEvent('keyup', {{bubbles:true}}));
            }}
            const all = Array.from(document.querySelectorAll('input'))
                .filter(i => i.offsetParent !== null && i.type !== 'hidden');

            // Try single-char boxes
            const sc = all.filter(i => i.maxLength === 1 || i.getAttribute('maxlength')==='1');
            if (sc.length >= 4) {{
                '{otp}'.split('').forEach((ch, i) => {{ if (sc[i]) setReact(sc[i], ch); }});
                return 'sc:' + sc.length;
            }}

            // Try OTP-matching input
            const otpEl = all.find(i =>
                i.maxLength === 6 || i.maxLength === 4 ||
                (i.placeholder || '').toLowerCase().match(/otp|code|verif|digit/) ||
                (i.name || '').toLowerCase().includes('otp') ||
                (i.id   || '').toLowerCase().includes('otp')
            ) || all.find(i => i.value.length < 4 && !i.readOnly);

            if (otpEl) {{ setReact(otpEl, '{otp}'); return 'single'; }}
            return false;
        }}
    """)
    if filled:
        logger.info(f"[OTP Fill C] JS result: {filled}")
        return True

async def _is_visible_safe(element) -> bool:
    try:
        return await element.is_visible()
    except Exception:
        return False


async def solve_recaptcha_if_present(page: Page, db: Session, token: str) -> bool:
    """Detect reCAPTCHA and solve it using 2Captcha API if key is configured."""
    try:
        # Check if reCAPTCHA iframe exists on the page
        frames = page.frames
        recaptcha_frame = None
        for f in frames:
            try:
                if "api2/anchor" in f.url:
                    recaptcha_frame = f
                    break
            except Exception:
                continue
                
        if not recaptcha_frame:
            # Check if there is any visible recaptcha element on the main page
            has_captcha = await page.evaluate("() => document.querySelector('.g-recaptcha, iframe[src*=\"api2/anchor\"]') !== null")
            if not has_captcha:
                return True # No captcha present
                
        await broadcast_status(token, "🧩 Captcha detected on the page! Attempting to extract sitekey...")
        
        sitekey = None
        if recaptcha_frame:
            try:
                sitekey = await recaptcha_frame.evaluate("() => { const el = document.getElementById('recaptcha-anchor'); return el ? el.dataset.sitekey : null; }")
            except Exception:
                pass
        if not sitekey:
            try:
                sitekey = await page.evaluate("() => { const el = document.querySelector('[data-sitekey]'); return el ? el.getAttribute('data-sitekey') : null; }")
            except Exception:
                pass
                
        if not sitekey:
            # Look inside any iframes
            for f in frames:
                try:
                    sitekey = await f.evaluate("() => { const el = document.querySelector('[data-sitekey]'); return el ? el.getAttribute('data-sitekey') : null; }")
                    if sitekey:
                        break
                except Exception:
                    continue

        if not sitekey:
            await broadcast_status(token, "⚠️ Captcha present, but sitekey could not be extracted.")
            return False

        # Get API key from SystemConfig
        from ..database import SystemConfig
        cfg = db.query(SystemConfig).filter(SystemConfig.key == "captcha_api_key").first()
        if not cfg or not cfg.value:
            await broadcast_status(token, "⚠️ Captcha detected, but 2Captcha API key is not configured.")
            return False
            
        api_key = cfg.value.strip()
        if not api_key:
            await broadcast_status(token, "⚠️ Captcha detected, but 2Captcha API key is empty.")
            return False

        await broadcast_status(token, f"🧩 Sitekey found: {sitekey[:8]}... Submitting to 2Captcha...")
        
        import httpx
        payload = {
            "key": api_key,
            "method": "userrecaptcha",
            "googlekey": sitekey,
            "pageurl": page.url,
            "json": 1,
        }
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post("http://2captcha.com/in.php", data=payload)
            data = resp.json()
            if data.get("status") != 1:
                err_code = data.get("request", "Unknown Error")
                await broadcast_status(token, f"❌ 2Captcha submission failed: {err_code}")
                return False
                
            captcha_id = data["request"]
            await broadcast_status(token, f"⏳ Captcha submitted successfully (ID: {captcha_id}). Polling for solution...")
            
            for attempt in range(24): # Poll up to 120 seconds
                await asyncio.sleep(5)
                try:
                    poll = await client.get(
                        "http://2captcha.com/res.php", 
                        params={"key": api_key, "action": "get", "id": captcha_id, "json": 1}
                    )
                    res_data = poll.json()
                    if res_data.get("status") == 1:
                        solution_token = res_data["request"]
                        await broadcast_status(token, "✅ Captcha solved! Injecting solution...")
                        
                        # Inject into g-recaptcha-response
                        await page.evaluate(f"""(tok) => {{
                            const el = document.getElementById('g-recaptcha-response');
                            if (el) el.innerHTML = tok;
                            // Trigger callback if defined
                            if (window.___grecaptcha_cfg && window.___grecaptcha_cfg.clients) {{
                                for (const client of window.___grecaptcha_cfg.clients) {{
                                    if (client && client.l && typeof client.l.callback === 'function') {{
                                        try {{ client.l.callback(tok); }} catch (e) {{}}
                                    }}
                                }}
                            }}
                        }}""", solution_token)
                        
                        await broadcast_status(token, "🎉 Captcha bypassed successfully!")
                        await asyncio.sleep(1)
                        return True
                    elif res_data.get("request") != "CAPCHA_NOT_READY":
                        await broadcast_status(token, f"❌ 2Captcha error during poll: {res_data.get('request')}")
                        return False
                except Exception as pe:
                    logger.debug(f"[Captcha Poll] Attempt {attempt} failed: {pe}")
                    
            await broadcast_status(token, "❌ 2Captcha timeout after 120 seconds.")
            return False
            
    except Exception as e:
        logger.exception(f"[solve_recaptcha_if_present] Error: {e}")
        await broadcast_status(token, f"⚠️ Error solving captcha: {str(e)[:100]}")
        return False


async def handle_post_login_navigation(page: Page, token: str):
    """Handle post-login overlays, profile completion forms, or location requests to get to a clean menu/home page state."""
    try:
        await broadcast_status(token, "⏳ Post-login check: Handling overlays, profile or location screens...")
        await asyncio.sleep(2.0) # wait for page/state to settle after login
        
        # 1. Dismiss overlays (Skip, Allow, Location prompts)
        await dismiss_overlays(page)
        
        # 2. Check for Profile Completion Form (Name, Email, etc.)
        # Common selectors: input[placeholder*="Name"], input[placeholder*="Email"]
        name_input = await page.query_selector('input[placeholder*="Name"], input[placeholder*="name"], input[id*="name"]')
        email_input = await page.query_selector('input[placeholder*="Email"], input[placeholder*="email"], input[id*="email"]')
        
        if name_input or email_input:
            # Get admin details from DB to fill real name/email
            from ..database import SessionLocal, User
            db = SessionLocal()
            admin_name = "Domino Customer"
            admin_email = "customer@dominos-engine.com"
            try:
                req_data = ACTIVE_OTP_REQUESTS.get(token)
                if req_data and req_data.get("admin_id"):
                    admin = db.query(User).filter(User.id == req_data["admin_id"]).first()
                    if admin:
                        admin_name = admin.display_name or admin.username or "Domino Customer"
                        admin_email = f"{admin.username or 'customer'}@dominos-engine.com"
            except Exception:
                pass
            finally:
                db.close()

            await broadcast_status(token, f"Profile completion screen detected! Filling details for {admin_name}...")
            if name_input:
                try:
                    await name_input.click()
                    await name_input.fill(admin_name)
                except Exception:
                    pass
            if email_input:
                try:
                    await email_input.click()
                    await email_input.fill(admin_email)
                except Exception:
                    pass
            
            # Click Save/Submit Profile
            submit_btn = await page.query_selector('button:has-text("Save"), button:has-text("Submit"), button:has-text("Continue"), button.btn--red')
            if submit_btn:
                try:
                    await submit_btn.click()
                    await broadcast_status(token, "📝 Profile form submitted.")
                    await asyncio.sleep(2.0)
                except Exception:
                    pass
        
        # 3. Double-check for Location overlay or Skip buttons
        await dismiss_overlays(page)
        
        # 4. Redirect to menu to ensure session is 100% active and initialized
        current_url = page.url
        if "login" in current_url.lower():
            await broadcast_status(token, "🌐 Still on login URL — redirecting to menu...")
            try:
                await page.goto("https://m.dominos.co.in/menu", wait_until="domcontentloaded", timeout=15000)
                await dismiss_overlays(page)
            except Exception:
                pass
                
        await broadcast_status(token, "✅ Post-login session validation complete!")
    except Exception as e:
        logger.warning(f"[handle_post_login_navigation] Exception: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Browser stealth setup
# ─────────────────────────────────────────────────────────────────────────────

async def apply_stealth(page: Page):
    """Inject JS patches that won't break client-side JS/React applications."""
    await page.add_init_script("""
    // Remove/override webdriver flag
    Object.defineProperty(navigator, 'webdriver', { get: () => false });

    // Expose Chrome properties if not present (since User-Agent is Chrome)
    window.chrome = {
        app: {
            isInstalled: false,
            InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' },
            RunningState: { CAN_RUN: 'can_run', CANNOT_RUN: 'cannot_run', RUNNING: 'running' }
        },
        runtime: {
            OnInstalledReason: { INSTALL: 'install', UPDATE: 'update', CHROME_UPDATE: 'chrome_update', SHARED_MODULE_UPDATE: 'shared_module_update' },
            OnRestartRequiredReason: { APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' },
            PlatformArch: { ARM: 'arm', ARM64: 'arm64', X86_32: 'x86-32', X86_64: 'x86-64', MIPS: 'mips', MIPS64: 'mips64' },
            PlatformNaclArch: { ARM: 'arm', X86_32: 'x86-32', X86_64: 'x86-64', MIPS: 'mips', MIPS64: 'mips64' },
            PlatformOs: { MAC: 'mac', WIN: 'win', ANDROID: 'android', CROS: 'cros', LINUX: 'linux', OPENBSD: 'openbsd' },
            RequestUpdateCheckStatus: { THROTTLED: 'throttled', NO_UPDATE: 'no_update', UPDATE_AVAILABLE: 'update_available' }
        },
        loadTimes: function() {},
        csi: function() {}
    };

    // Mock languages
    Object.defineProperty(navigator, 'languages', { get: () => ['en-IN', 'en-GB', 'en-US', 'en'] });

    // Mock WebGL Vendor/Renderer to hide SwiftShader/Software renderer
    const getParam = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) return 'Intel Inc.'; // UNMASKED_VENDOR_WEBGL
        if (parameter === 37446) return 'Intel(R) Iris(R) Xe Graphics'; // UNMASKED_RENDERER_WEBGL
        return getParam.apply(this, arguments);
    };

    // Override permissions safely
    const origQuery = window.navigator.permissions?.query?.bind(window.navigator.permissions);
    if (origQuery) {
        window.navigator.permissions.query = (params) =>
            params.name === 'notifications'
                ? Promise.resolve({ state: 'denied' })
                : origQuery(params);
    }

    // Track mouse position
    document.addEventListener('mousemove', e => {
        window.mouseX = e.clientX;
        window.mouseY = e.clientY;
    }, { passive: true });
    """)


async def cleanup_stale_otp_requests():
    """Purge OTP requests older than 10 minutes from memory to free browser resources."""
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    cutoff = now - datetime.timedelta(minutes=10)
    for token, req_data in list(ACTIVE_OTP_REQUESTS.items()):
        created_at = req_data.get("created_at")
        if created_at and created_at < cutoff:
            logger.info(f"[Cleanup] Purging stale OTP request token {token[:8]}...")
            context = req_data.get("context")
            if context:
                try: await context.close()
                except Exception: pass
            browser = req_data.get("browser")
            if browser:
                try: await browser.close()
                except Exception: pass
            pw_ctx = req_data.get("playwright_ctx")
            if pw_ctx:
                try: await pw_ctx.stop()
                except Exception: pass
            ACTIVE_OTP_REQUESTS.pop(token, None)


# ─────────────────────────────────────────────────────────────────────────────
# Public API: request_otp
# ─────────────────────────────────────────────────────────────────────────────

async def request_otp(db: Session, admin: User, mobile_number: str, manual_mode: bool = False) -> Dict[str, Any]:
    """Register a request token and launch the OTP browser task in the background."""
    await cleanup_stale_otp_requests()
    # ── Check and clean up any existing active requests for the same mobile number ──
    for old_token, req_data in list(ACTIVE_OTP_REQUESTS.items()):
        if req_data.get("mobile_number") == mobile_number:
            logger.info(f"[Cleanup] Found existing active request for +91{mobile_number}. Cancelling it...")
            req_data["cancelled"] = True  # Signal the running task to stop
            context = req_data.get("context")
            if context:
                try: await context.close()
                except Exception: pass
            browser = req_data.get("browser")
            if browser:
                try: await browser.close()
                except Exception: pass
            pw_ctx = req_data.get("playwright_ctx")
            if pw_ctx:
                try: await pw_ctx.stop()
                except Exception: pass
            ACTIVE_OTP_REQUESTS.pop(old_token, None)
            await broadcast_status(old_token, "❌ Session request superseded by a new request for this mobile number.")

    # ── Delete any existing sessions in the database for this mobile number ──
    db.query(DominosSession).filter(
        DominosSession.mobile_number == mobile_number
    ).delete()
    db.commit()

    token = generate_request_token()

    otp_req = DominosOTPRequest(
        mobile_number=mobile_number,
        request_token=token,
        created_at=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
    )
    db.add(otp_req)
    db.commit()  # ← single commit — duplicate removed


    # ── Real mode ──
    ACTIVE_OTP_REQUESTS[token] = {
        "mobile_number": mobile_number,
        "is_mock": False,
        "manual_mode": manual_mode,
        "cancelled": False,
        "browser_ready": False,
        "playwright_ctx": None,
        "browser": None,
        "context": None,
        "page": None,
        "last_screenshot": None,
        "last_status": "",
        "browser_error": None,
        "created_at": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
    }

    asyncio.create_task(_run_otp_browser(token, mobile_number, admin.id, manual_mode))

    # Robot log: OTP requested
    log_robot_event(db, mobile_number, "INFO", "otp_request",
        f"OTP browser launched for +91{mobile_number} (Manual Mode: {manual_mode})",
        {"request_token": token, "admin_id": admin.id, "manual_mode": manual_mode})

    return {
        "status": "success",
        "request_token": token,
        "message": (
            f"Browser launched for +91{mobile_number}. "
            "Watch the Robot Status panel below — enter the OTP when you receive it."
            if not manual_mode else
            f"Headed browser window opened on your laptop for +91{mobile_number}. The bot will type your mobile and send OTP automatically."
        ),
    }




# ─────────────────────────────────────────────────────────────────────────────
# Background task: navigate Domino's and trigger OTP SMS
# ─────────────────────────────────────────────────────────────────────────────

async def _run_otp_browser(token: str, mobile_number: str, admin_id: int, manual_mode: bool = False):
    """Full Playwright automation: open Domino's, enter mobile, click 'Send OTP'."""
    context: Optional[BrowserContext] = None
    page:    Optional[Page] = None
    pw_instance = None
    browser_instance = None

    def is_token_cancelled() -> bool:
        """Returns True if this request was superseded/cancelled by a newer request."""
        req = ACTIVE_OTP_REQUESTS.get(token)
        return req is None or req.get("cancelled", False)

    try:
        # ── 1. Launch browser context ──────────────────────────────────────
        ua = random.choice(USER_AGENTS)
        width = int(os.getenv("BROWSER_WIDTH", "1280"))
        height = int(os.getenv("BROWSER_HEIGHT", "900"))
        vp = {"width": width, "height": height}

        if manual_mode:
            await broadcast_status(token, "🌐 Launching visible browser on your laptop for manual login…")
            from playwright.async_api import async_playwright
            if sys.platform == "win32":
                try:
                    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                except Exception:
                    pass
            pw_instance = await async_playwright().start()
            browser_instance = await pw_instance.chromium.launch(
                headless=False,
                args=[
                    "--log-level=3",
                    "--disable-logging",
                    "--silent",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--window-size=1280,900",
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled",
                ]
            )
            context = await browser_instance.new_context(
                user_agent=ua,
                locale="en-IN",
                viewport=vp,
                geolocation={"latitude": 19.0760, "longitude": 72.8777},
                permissions=["geolocation"]
            )
            page = await context.new_page()

            ACTIVE_OTP_REQUESTS[token].update({
                "playwright_ctx": pw_instance,
                "browser": browser_instance,
                "context": context,
                "page": page,
            })
        else:
            await broadcast_status(token, "🤖 Launching stealth browser context…")
            from .browser_pool import browser_pool

            # Get static proxy if configured
            proxy_config = None
            from ..database import SessionLocal
            db_for_proxy = SessionLocal()
            try:
                from .proxy_manager import ProxyManager
                pm = ProxyManager(db_for_proxy)
                proxy_config = pm.get_proxy()
            except Exception as proxy_err:
                logger.debug(f"No proxy configured or error fetching proxy: {proxy_err}")
            finally:
                db_for_proxy.close()

            context_args = {
                "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                "locale": "en-IN",
                "timezone_id": "Asia/Kolkata",
                "viewport": {"width": 375, "height": 812},
                "device_scale_factor": 3,
                "is_mobile": True,
                "has_touch": True,
                "java_script_enabled": True,
                "geolocation": {"latitude": 19.0760, "longitude": 72.8777},
                "permissions": ["geolocation"]
            }
            if proxy_config:
                context_args["proxy"] = proxy_config

            context = await browser_pool.create_context(**context_args)
            page = await context.new_page()
            
            # Test proxy connectivity
            if proxy_config:
                try:
                    resp = await page.goto("https://m.dominos.co.in", wait_until="commit", timeout=5000)
                    status = resp.status if resp else None
                    title = await page.title()
                    is_blocked = False
                    if status and status in (403, 407, 408, 502, 503, 504):
                        is_blocked = True
                    if title and any(x in title.lower() for x in ("access denied", "forbidden", "just a moment", "cloudflare", "attention required")):
                        is_blocked = True
                        
                    if is_blocked:
                        raise Exception(f"Proxy blocked (status={status}, title='{title}')")
                except Exception as conn_err:
                    logger.warning(f"[OTP Browser] Proxy connectivity check failed inside _run_otp_browser: {conn_err}. Recreating context without proxy...")
                    try:
                        await context.close()
                    except Exception:
                        pass
                    context_args.pop("proxy", None)
                    context = await browser_pool.create_context(**context_args)
                    page = await context.new_page()

            ACTIVE_OTP_REQUESTS[token].update({
                "playwright_ctx": browser_pool.playwright_ctx,
                "browser": browser_pool.browser,
                "context": context,
                "page": page,
            })

        # ── 2. Apply stealth patches ───────────────────────────────────────
        await apply_stealth(page)

        # Block unnecessary resources to reduce page load time
        if not manual_mode:
            async def handle_route(route):
                req = route.request
                url = req.url.lower()
                r_type = req.resource_type
                if r_type in ("image", "media", "font") or any(
                    x in url for x in ("google-analytics", "analytics", "facebook", "doubleclick", "hotjar", "amplitude", "clarity", "clevertap", "wizrocket", "mixpanel", "sentry")
                ):
                    try: await route.abort()
                    except Exception: pass
                else:
                    try: await route.continue_()
                    except Exception: pass

            await page.route("**/*", handle_route)

        # ── 3. Navigate to Domino's ────────────────────────────────────────
        if is_token_cancelled():
            logger.info(f"[OTP {token[:8]}] ❌ Cancelled before navigation. Exiting.")
            return
        await broadcast_status(token, "🌐 Navigating directly to Domino's login page…")

        loaded = False
        # Direct mobile login URL is the most logical and efficient to avoid multiple page loads.
        for url in ["https://m.dominos.co.in/login", "https://pizzaonline.dominos.co.in"]:
            try:
                try:
                    await page.goto(url, wait_until="networkidle", timeout=15000)
                except Exception:
                    # Fallback to domcontentloaded if networkidle takes too long
                    await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                await smart_wait(page, 800, 1200)
                loaded = True
                logger.info(f"[OTP Browser] Loaded URL: {url}")
                break
            except Exception as e:
                logger.warning(f"[OTP Browser] Failed to load {url}: {e}")

        if not loaded:
            raise Exception("Could not reach Domino's India — check network connection.")

        if is_token_cancelled():
            logger.info(f"[OTP {token[:8]}] ❌ Cancelled after page load. Exiting.")
            return

        await broadcast_status(token, "🌐 Page loaded — dismissing overlays…", take_screenshot=True)
        await dismiss_overlays(page)
        await asyncio.sleep(0.5)
        # Wait for React SPA to fully render before querying the DOM
        await wait_for_react_render(page, timeout=8.0)

        # Check and solve recaptcha if present after navigating to Domino's
        from ..database import SessionLocal
        db_sess = SessionLocal()
        try:
            await solve_recaptcha_if_present(page, db_sess, token)
        finally:
            db_sess.close()

        # ── 4. Navigate to login ───────────────────────────────────────────
        # Check current page state first
        state = await detect_page_state(page)
        logger.info(f"[OTP Browser] Post-load state: {state}")

        if state not in ("mobile_form", "otp_boxes", "otp_single", "logged_in"):
            curr_url = page.url.lower()
            if "login" not in curr_url and ("home" in curr_url or "jfl-discovery-ui" in curr_url):
                await broadcast_status(token, "🔑 Opening profile drawer to access Login...")
                # Click the top-right profile button/drawer trigger
                drawer_opened = False
                for sel in ("button.profile-drawer-btn", ".profile-drawer-btn", '[data-testid="profile-btn"]', ".profile-btn"):
                    try:
                        if await page.is_visible(sel, timeout=2000):
                            await page.click(sel)
                            logger.info(f"[OTP Browser] Profile drawer button '{sel}' clicked.")
                            await asyncio.sleep(0.8)
                            drawer_opened = True
                            break
                    except Exception:
                        continue
                
                if drawer_opened:
                    # Click Login button inside the drawer
                    for sel in ('button:has-text("Login")', 'button.profile-drawer-btn', 'a:has-text("Login")', '.profile-drawer-btn'):
                        try:
                            if await page.is_visible(sel, timeout=2000):
                                await page.click(sel)
                                logger.info(f"[OTP Browser] Login button inside drawer '{sel}' clicked.")
                                await asyncio.sleep(1.5)
                                break
                        except Exception:
                            continue

            # Re-detect state
            state = await detect_page_state(page)
            logger.info(f"[OTP Browser] State after drawer login click: {state}")

        if state not in ("mobile_form", "otp_boxes", "otp_single", "logged_in"):
            await broadcast_status(token, "🔑 Opening login…")

            # Wait up to 10 seconds for the React login input or buttons to render
            try:
                await page.wait_for_selector('#loginNumber, input[type="tel"], input[maxlength="1"], button:has-text("Login"), a:has-text("Login")', timeout=10000)
            except Exception:
                pass

            # Try clicking the login button
            login_found = False
            for sel in (
                'button:has-text("Login")', 'a:has-text("Login")',
                'button:has-text("Sign In")', 'a:has-text("Sign In")',
                '.btn-login', 'text="Login"', '[data-testid="login-btn"]',
                '[href*="login"]', 'button:has-text("LOG IN")',
            ):
                try:
                    if await page.is_visible(sel, timeout=1200):
                        el = await page.query_selector(sel)
                        if el:
                            bb = await el.bounding_box()
                            if bb:
                                await mouse_move_human(
                                    page,
                                    int(bb["x"] + bb["width"] / 2),
                                    int(bb["y"] + bb["height"] / 2),
                                )
                        await page.click(sel, timeout=2000)
                        login_found = True
                        logger.info(f"[OTP Browser] Login clicked via '{sel}'")
                        await smart_wait(page, 600, 1000)
                        break
                except Exception:
                    continue

            if not login_found:
                logger.info("[OTP Browser] Login button not found, trying direct URL")
                try:
                    await page.goto(
                        "https://m.dominos.co.in/login",
                        wait_until="domcontentloaded",
                        timeout=20000,
                    )
                    await smart_wait(page, 800, 1200)
                except Exception:
                    pass

            await dismiss_overlays(page)
            # Wait for React to render the login form after navigation
            await wait_for_react_render(page, timeout=8.0)

        await broadcast_status(token, "🔑 Login page open — checking state…", take_screenshot=True)

        # ── 5. Detect page state and act ──────────────────────────────────
        if is_token_cancelled():
            logger.info(f"[OTP {token[:8]}] ❌ Cancelled before state detection. Exiting.")
            return
        state = await detect_page_state(page)
        logger.info(f"[OTP Browser] Pre-entry state: {state}")

        # Check and handle logged_in mismatch first
        if state == "logged_in":
            # Before reporting "already logged in", verify that the active Domino's account exactly matches the requested mobile number.
            is_match = await verify_logged_in_mobile(page, mobile_number)
            if is_match:
                await broadcast_status(token, "✅ Account already logged in and matches mobile! Saving session…")
                ACTIVE_OTP_REQUESTS[token]["browser_ready"] = True
                asyncio.create_task(_monitor_manual_login(token, mobile_number, admin_id))
                return
            else:
                logger.info(f"[OTP Browser] Logged in account does not match mobile +91{mobile_number}. Discarding session cookies and clearing cache to start a fresh login flow.")
                try:
                    await page.context.clear_cookies()
                    await page.evaluate("() => localStorage.clear()")
                    await page.evaluate("() => sessionStorage.clear()")
                    # Reload page to get a clean login form
                    await page.goto("https://m.dominos.co.in/login", wait_until="domcontentloaded", timeout=20000)
                    await smart_wait(page, 800, 1200)
                except Exception as clear_err:
                    logger.warning(f"[OTP Browser] Failed to clear session of mismatched account: {clear_err}")
                
                # Re-detect state after clearing
                state = await detect_page_state(page)

        submit_clicked = False

        if state in ("otp_boxes", "otp_single"):
            await broadcast_status(token, "📋 Already on OTP entry screen — waiting for your code!")
            submit_clicked = True  # OTP was already sent (or shows on screen)

        else:
            # ── Enter mobile number ────────────────────────────────────────
            if is_token_cancelled():
                logger.info(f"[OTP {token[:8]}] ❌ Cancelled before mobile entry. Exiting.")
                return
            await broadcast_status(token, f"✍️ Entering +91{mobile_number}…")
            mobile_entered = False

            # Wait for React to render the login form (up to 12 seconds)
            wait_selectors = (
                '#loginNumber, input[name="loginNumber"], input[type="tel"], '
                'input[placeholder*="Mobile"], input[placeholder*="mobile"], '
                'input[placeholder*="Phone"], input[placeholder*="Number"], '
                'input[maxlength="10"], input[maxlength="12"]'
            )
            try:
                await page.wait_for_selector(wait_selectors, timeout=12000, state="visible")
                logger.info("[OTP Browser] Mobile input field rendered.")
            except Exception:
                logger.warning("[OTP Browser] Timed out waiting for mobile input — will try all selectors anyway.")

            phone_selectors = (
                "#loginNumber",
                'input[name="loginNumber"]',
                'input[data-testid="user-input"]',
                'input[data-testid="mobile-input"]',
                'input[data-testid="phone-input"]',
                'input[type="tel"]',
                'input[placeholder*="Mobile"]',
                'input[placeholder*="mobile"]',
                'input[placeholder*="Enter Mobile"]',
                'input[placeholder*="Phone Number"]',
                'input[placeholder*="phone"]',
                'input[placeholder*="Number"]',
                'input[maxlength="10"]',
                'input[maxlength="12"]',
                'input[name="mobile"]',
                'input[name="phone"]',
                'input[inputmode="numeric"]',
                'input[inputmode="tel"]',
            )

            for sel in phone_selectors:
                try:
                    if await page.is_visible(sel, timeout=1500):
                        # Hover → click → type (human pattern)
                        el = await page.query_selector(sel)
                        if el:
                            bb = await el.bounding_box()
                            if bb:
                                await mouse_move_human(
                                    page,
                                    int(bb["x"] + bb["width"] / 2),
                                    int(bb["y"] + bb["height"] / 2),
                                )
                        await human_type(page, sel, mobile_number)
                        mobile_entered = True
                        logger.info(f"[OTP Browser] Mobile entered via '{sel}'")
                        break
                except Exception:
                    continue

            if not mobile_entered:
                # JS React-aware fallback: tries nativeInputValueSetter for controlled components
                filled = await page.evaluate(f"""() => {{
                    const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
                    // Prefer visible, non-readonly inputs likely used for phone
                    const candidates = Array.from(document.querySelectorAll(
                        'input[type="tel"],input[type="number"],input[type="text"],input[inputmode="numeric"],input[inputmode="tel"]'
                    )).filter(el => el.offsetParent !== null && !el.readOnly && !el.disabled);
                    // Sort: prefer maxlength=10/12 (Indian mobile), then any
                    candidates.sort((a, b) => {{
                        const aLen = parseInt(a.getAttribute('maxlength') || '0', 10);
                        const bLen = parseInt(b.getAttribute('maxlength') || '0', 10);
                        const aScore = (aLen === 10 || aLen === 12) ? 1 : 0;
                        const bScore = (bLen === 10 || bLen === 12) ? 1 : 0;
                        return bScore - aScore;
                    }});
                    for (const inp of candidates) {{
                        inp.focus();
                        if (nativeSetter) nativeSetter.call(inp, '{mobile_number}');
                        else inp.value = '{mobile_number}';
                        inp.dispatchEvent(new Event('input',  {{bubbles: true}}));
                        inp.dispatchEvent(new Event('change', {{bubbles: true}}));
                        inp.dispatchEvent(new KeyboardEvent('keyup', {{bubbles: true}}));
                        return true;
                    }}
                    return false;
                }}""")
                if filled:
                    mobile_entered = True
                    logger.info("[OTP Browser] Mobile entered via JS React fallback")
                else:
                    # Last resort: dump all visible inputs to help debug
                    try:
                        input_dump = await page.evaluate("""() => {
                            return Array.from(document.querySelectorAll('input')).map(el => ({
                                type: el.type, id: el.id, name: el.name,
                                placeholder: el.placeholder, visible: el.offsetParent !== null,
                                maxlen: el.getAttribute('maxlength'), inputmode: el.getAttribute('inputmode')
                            }));
                        }""")
                        logger.warning(f"[OTP Browser] All inputs on page: {input_dump}")
                    except Exception:
                        pass

            if not mobile_entered:
                raise Exception(
                    "Could not locate the mobile number input field. "
                    "Domino's may have changed their login page layout."
                )

            await asyncio.sleep(random.uniform(0.4, 0.8))
            await broadcast_status(token, "🚀 Clicking 'Send OTP' button…", take_screenshot=True)

            # ── Click Send OTP ──────────────────────────────────────────────
            otp_btn_selectors = (
                'button.btn--red',
                'button:has-text("Send OTP")',
                'button:has-text("SEND OTP")',
                'span:has-text("Send OTP")',
                'button:has-text("GET OTP")',
                'button:has-text("Get OTP")',
                'button:has-text("Submit")',
                'button:has-text("SUBMIT")',
                'button:has-text("Continue")',
                'button:has-text("CONTINUE")',
                'button:has-text("Proceed")',
                'input[type="submit"]',
                '.login-btn', '[data-testid="submit-btn"]',
                'button[type="submit"]',
                '.otp-btn', '.send-otp-btn', 'button.btn-primary',
            )
            for sel in otp_btn_selectors:
                try:
                    if await page.is_visible(sel, timeout=1800):
                        el = await page.query_selector(sel)
                        if el:
                            bb = await el.bounding_box()
                            if bb:
                                await mouse_move_human(
                                    page,
                                    int(bb["x"] + bb["width"] / 2),
                                    int(bb["y"] + bb["height"] / 2),
                                )
                                await asyncio.sleep(random.uniform(0.10, 0.25))
                        await page.click(sel, timeout=3000)
                        submit_clicked = True
                        logger.info(f"[OTP Browser] Send-OTP clicked via '{sel}'")
                        break
                except Exception:
                    continue

            if not submit_clicked:
                # JS click fallback
                js_clicked = await page.evaluate("""() => {
                    const allBtns = Array.from(
                        document.querySelectorAll('button,input[type="submit"]')
                    );
                    const btn = allBtns.find(b => {
                        const t = (b.textContent || b.value || '').toLowerCase();
                        return t.includes('otp') || t.includes('send') ||
                               t.includes('submit') || t.includes('continue') ||
                               t.includes('get') || t.includes('proceed');
                    });
                    if (btn && btn.offsetParent !== null && !btn.disabled) {
                        btn.click(); return true;
                    }
                    // Last resort: any visible enabled button
                    const vis = allBtns.find(b => b.offsetParent !== null && !b.disabled);
                    if (vis) { vis.click(); return true; }
                    return false;
                }""")
                if js_clicked:
                    submit_clicked = True
                    logger.info("[OTP Browser] Send-OTP clicked via JS fallback")

        await smart_wait(page, 600, 1000)  # reduced for speed

        # Check if there is an error shown on the page (e.g. suspended, limit reached)
        page_err = await check_page_errors(page)
        if page_err:
            raise Exception(f"OTP request failed: {page_err}")

        # Check and solve recaptcha if prompted after clicking Send OTP
        from ..database import SessionLocal
        db_sess = SessionLocal()
        try:
            await solve_recaptcha_if_present(page, db_sess, token)
        finally:
            db_sess.close()

        ACTIVE_OTP_REQUESTS[token]["browser_ready"] = True

        # ── 6. Confirm state after OTP send ───────────────────────────────
        post_state = await detect_page_state(page)
        logger.info(f"[OTP Browser] Post-send state: {post_state}")

        if submit_clicked or post_state in ("otp_boxes", "otp_single"):
            await broadcast_status(
                token,
                f"📱 OTP sent to +91{mobile_number}! Enter the 6-digit code you received.",
                take_screenshot=True,
            )
        else:
            await broadcast_status(
                token,
                "⚠️ Could not auto-click 'Send OTP'. "
                "If the browser window is open on your screen, click it manually, "
                "then enter the OTP below.",
                take_screenshot=True,
            )

        # Start background watcher for manual completion
        asyncio.create_task(_monitor_manual_login(token, mobile_number, admin_id))

    except Exception as e:
        err_msg = str(e)
        # Superseded/cancelled session — exit silently, don't show error to admin
        if is_token_cancelled():
            logger.info(f"[OTP {token[:8]}] Closed cleanly after supersession: {err_msg[:80]}")
            return
        if "Target page, context or browser has been closed" in err_msg or "Target closed" in err_msg or "context or browser has been closed" in err_msg:
            logger.info(f"[OTP Browser] Context/page closed for token {token[:8]}")
        else:
            logger.exception(f"[OTP Browser] Error for token {token}: {err_msg}")
            logger.error(f"[OTP BROWSER ERROR] token={token[:8]} mobile=+91{mobile_number}\n  {err_msg}")
        if token in ACTIVE_OTP_REQUESTS:
            ACTIVE_OTP_REQUESTS[token]["browser_error"] = err_msg
            ACTIVE_OTP_REQUESTS[token]["browser_ready"] = False
        # Keep browser open for manual recovery — do NOT close context
        await broadcast_status(
            token,
            f"❌ Robot error: {err_msg}. "
            "If the browser is visible on screen you can complete the login manually.",
        )
        # Also surface to admin Live Feed via error_alert
        from .. import routes
        callback = getattr(routes, "sse_broadcast_callback", None)
        if callback:
            try:
                await callback({
                    "type": "error_alert",
                    "message": f"[OTP Browser] +91{mobile_number}: {err_msg}",
                })
            except Exception:
                pass


async def check_page_errors(page: Page) -> Optional[str]:
    """Check if there is a visible error message on the page."""
    error_selectors = [
        ".error-msg", ".error-text", ".login-error", ".err-msg",
        "[class*=\"error\"]", "[class*=\"Error\"]"
    ]
    for sel in error_selectors:
        try:
            if await page.is_visible(sel, timeout=300):
                txt = await page.inner_text(sel)
                if txt and len(txt.strip()) > 3:
                    return txt.strip()
        except Exception:
            continue
    
    # Check div, span, p for common error phrases
    try:
        err = await page.evaluate("""() => {
            const el = Array.from(document.querySelectorAll('div,span,p,h1,h2,h3'))
                .find(e => {
                    const t = (e.textContent || '').trim().toLowerCase();
                    return (t.includes('incorrect otp') || t.includes('invalid otp') || 
                            t.includes('maximum limit') || t.includes('maximum attempts') ||
                            t.includes('limit reached') || t.includes('exceeded') ||
                            t.includes('blocked') || t.includes('suspended') ||
                            t.includes('daily limit') || t.includes('unable to send') ||
                            t.includes('enter a valid') || t.includes('validation code') ||
                            t.includes('something went wrong') || t.includes('please try again') ||
                            t.includes('verification failed'));
                });
            return el ? el.textContent.trim() : null;
        }""")
        if err:
            return err
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Public API: verify_otp
# ─────────────────────────────────────────────────────────────────────────────

async def clear_previous_errors(page: Page):
    """Clean up any error messages from previous attempts to prevent false-positives."""
    try:
        await page.evaluate("""() => {
            const errorSelectors = [
                ".error-msg", ".error-text", ".login-error", ".err-msg",
                "[class*='error']", "[class*='Error']"
            ];
            for (const sel of errorSelectors) {
                document.querySelectorAll(sel).forEach(el => {
                    try { el.remove(); } catch(e) {}
                });
            }
            Array.from(document.querySelectorAll('div,span,p,h1,h2,h3')).forEach(el => {
                const txt = el.textContent || '';
                if (txt.includes('Incorrect OTP') || txt.includes('Invalid OTP') || 
                    txt.includes('incorrect OTP') || txt.includes('invalid OTP') ||
                    txt.includes('verification code') || txt.includes('Verification code')) {
                    try { el.remove(); } catch(e) {}
                }
            });
        }""")
        logger.info("[verify_otp] Cleared any previous error elements from the page.")
    except Exception as e:
        logger.warning(f"Failed to clear previous errors: {e}")


async def verify_otp(db: Session, admin: User, request_token: str, otp: str) -> DominosSession:
    """Fill the OTP into the Playwright page and extract session cookies."""
    req_data = ACTIVE_OTP_REQUESTS.get(request_token)
    if not req_data:
        # Fallback 1: check if already-running background task saved session in DB
        otp_req = db.query(DominosOTPRequest).filter(DominosOTPRequest.request_token == request_token).first()
        if otp_req:
            time_limit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=10)
            cached_session = db.query(DominosSession).filter(
                DominosSession.mobile_number == otp_req.mobile_number,
                DominosSession.is_active == True,
                DominosSession.created_at >= time_limit
            ).order_by(DominosSession.created_at.desc()).first()
            if cached_session:
                logger.info(f"[verify_otp] Active session found in database for +91{otp_req.mobile_number}. Returning cached session.")
                return cached_session
        
        # Fallback 2: search sessions table by request token directly
        time_limit2 = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=10)
        cached_by_token = db.query(DominosSession).filter(
            DominosSession.created_at >= time_limit2,
            DominosSession.is_active == True
        ).order_by(DominosSession.created_at.desc()).first()
        if cached_by_token:
            logger.info(f"[verify_otp] Found recent session in DB (server restart recovery). Returning session {cached_by_token.id}.")
            return cached_by_token
                
        raise ValueError(
            "Session expired — the server may have restarted. "
            "Click 'Request OTP' again to start a fresh login flow."
        )

    # Signal to _monitor_manual_login that verify_otp is active — don't interfere
    req_data["verifying"] = True
    ACTIVE_OTP_REQUESTS[request_token] = req_data

    try:
        return await _verify_otp_internal(db, admin, request_token, otp)
    finally:
        if request_token in ACTIVE_OTP_REQUESTS:
            ACTIVE_OTP_REQUESTS[request_token]["verifying"] = False


def save_or_update_session(db: Session, mobile_number: str, cookies: list, local_storage_data: dict, is_active: bool, verify_status: str, admin_id: int) -> DominosSession:
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    session = db.query(DominosSession).filter(DominosSession.mobile_number == mobile_number).first()
    if session:
        session.cookies = sanitize_cookies(cookies)
        session.local_storage = local_storage_data
        session.is_active = is_active
        session.verify_status = verify_status
        session.last_verified_at = now_utc
        session.expires_at = now_utc + datetime.timedelta(days=14)
        if admin_id:
            session.admin_id = admin_id
        logger.info(f"[SessionManager] Updated existing session for +91{mobile_number} (ID: {session.id})")
    else:
        session = DominosSession(
            mobile_number=mobile_number,
            cookies=sanitize_cookies(cookies),
            local_storage=local_storage_data,
            is_active=is_active,
            verify_status=verify_status,
            last_verified_at=now_utc,
            expires_at=now_utc + datetime.timedelta(days=14),
            admin_id=admin_id,
            created_at=now_utc,
        )
        db.add(session)
        logger.info(f"[SessionManager] Created new session for +91{mobile_number} (ID: {session.id})")
    db.commit()
    db.refresh(session)
    
    # Broadcast session_update SSE event
    try:
        from app.backend.main import sse_broadcast_callback
        if sse_broadcast_callback:
            import asyncio
            asyncio.create_task(sse_broadcast_callback({"type": "session_update"}))
    except Exception:
        pass
        
    return session


async def _verify_otp_internal(db: Session, admin: User, request_token: str, otp: str) -> DominosSession:
    req_data = ACTIVE_OTP_REQUESTS.get(request_token)
    if not req_data:
        raise ValueError(
            "Session expired — the server may have restarted. "
            "Click 'Request OTP' again to start a fresh login flow."
        )

    mobile_number = req_data["mobile_number"]

    # ── Wait for browser_ready, fast-fail on browser_error ──────
    from ..settings import Settings
    _settings = Settings()
    wait_secs = 0
    while not req_data.get("browser_ready") and wait_secs < _settings.OTP_WAIT_TIMEOUT:
        # Fast-fail: if the browser task already errored, stop waiting immediately
        if req_data.get("browser_error"):
            raise ValueError(
                f"Browser automation failed: {req_data['browser_error']}. "
                "Click 'Request OTP' again to start a fresh login flow."
            )
        if wait_secs % 6 == 0:  # broadcast every 6 seconds, not every 2
            await broadcast_status(
                request_token,
                f"⏳ Waiting for browser to finish sending OTP… ({wait_secs}s)"
            )
        await asyncio.sleep(2)
        wait_secs += 2
        req_data = ACTIVE_OTP_REQUESTS.get(request_token, req_data)

    # Final check after wait loop
    if req_data.get("browser_error"):
        raise ValueError(
            f"Browser automation failed: {req_data['browser_error']}. "
            "Click 'Request OTP' again to start a fresh login flow."
        )

    if not req_data.get("page"):
        raise ValueError(
            "Browser task failed to initialise — please try requesting OTP again."
        )

    context: BrowserContext = req_data["context"]
    page:    Page            = req_data["page"]
    is_manual = req_data.get("manual_mode", False)

    try:
        # Clear any previous error elements from screen so we don't catch old validations
        await clear_previous_errors(page)

        # Screenshot before filling
        asyncio.create_task(_capture_screenshot(request_token, page, "✍️ About to enter OTP…"))

        # Check if already logged in first to skip inputs and clicks
        state = await detect_page_state(page)
        if state == "logged_in":
            is_match = await verify_logged_in_mobile(page, mobile_number)
            if is_match:
                await broadcast_status(request_token, "✅ Page is already logged in and matches mobile! Extracting cookies…")
            else:
                await broadcast_status(request_token, "⚠️ Logged in account does not match requested mobile. Forcing a fresh login...")
                try:
                    await page.context.clear_cookies()
                    await page.evaluate("() => localStorage.clear()")
                    await page.evaluate("() => sessionStorage.clear()")
                    await page.goto("https://m.dominos.co.in/login", wait_until="domcontentloaded", timeout=20000)
                    await smart_wait(page, 800, 1200)
                except Exception:
                    pass
                state = "mobile_form"
        else:
            # Wait up to 10 seconds for the OTP inputs or logged_in state to load/render
            await broadcast_status(request_token, "⏳ Waiting for OTP screen to load...")
            for wait_attempt in range(20):  # 20 * 0.5 = 10 seconds max wait
                state = await detect_page_state(page)
                if state in ("otp_boxes", "otp_single", "logged_in"):
                    break
                # Fast check for page errors
                page_err = await check_page_errors(page)
                # Ignore non-fatal validation errors like "Incorrect OTP" or "Invalid OTP" during initial wait
                if page_err and not any(k in page_err.lower() for k in ("incorrect otp", "invalid otp")):
                    raise Exception(page_err)
                await asyncio.sleep(0.5)
            
            # Detect final state after wait
            state = await detect_page_state(page)
            if state == "logged_in":
                await broadcast_status(request_token, "✅ Page is already logged in! Extracting cookies…")
            else:
                await broadcast_status(request_token, "✍️ Locating OTP input on the page…")

                # ── Fill OTP ────────────────────────────────────────────────────────
                filled = False
                for fill_attempt in range(3):
                    try:
                        filled = await fill_otp_boxes(page, otp, request_token)
                        break
                    except Exception as fe:
                        err_str = str(fe)
                        if "Execution context" in err_str or "navigation" in err_str or "context was destroyed" in err_str:
                            logger.warning(f"[verify_otp] Execution context destroyed during fill (attempt {fill_attempt+1}). Retrying in 1.5s...")
                            await asyncio.sleep(1.5)
                            continue
                        else:
                            raise fe

                if not filled:
                    raise Exception(
                        "manual fallback: Could not auto-fill OTP boxes after all retries."
                    )

                await asyncio.sleep(0.15)
                asyncio.create_task(_capture_screenshot(request_token, page, "🚀 OTP entered — submitting…"))
                await broadcast_status(request_token, "🚀 Submitting OTP…")

                # ── Click Submit ────────────────────────────────────────────────────
                # First: force-enable any disabled buttons via JS (handles grey button case)
                try:
                    await page.evaluate("""() => {
                        // Find the login/submit button and force-enable it
                        const candidates = Array.from(document.querySelectorAll('button, input[type="submit"]'))
                            .filter(b => b.offsetParent !== null);
                        candidates.forEach(btn => {
                            const txt = (btn.textContent || btn.value || '').trim().toLowerCase();
                            if (txt.includes('login') || txt.includes('submit') || txt.includes('verify') ||
                                txt.includes('continue') || txt.includes('confirm') || btn.type === 'submit') {
                                btn.disabled = false;
                                btn.removeAttribute('disabled');
                                btn.classList.remove('disabled', 'btn--disabled', 'inactive', 'grey');
                            }
                        });
                    }""")
                    await asyncio.sleep(0.2)  # Give React a moment to re-render
                except Exception:
                    pass

                verify_selectors = (
                    'button:has-text("Login")',     'button:has-text("LOGIN")',
                    'button:has-text("Submit")',    'button:has-text("SUBMIT")',
                    'button:has-text("Verify")',    'button:has-text("VERIFY")',
                    'button:has-text("Confirm")',   'button:has-text("CONFIRM")',
                    'button:has-text("Continue")',  'button:has-text("CONTINUE")',
                    'button:has-text("Proceed")',   'button:has-text("Done")',
                    'button.btn--red',              'button[type="submit"]',
                    'input[type="submit"]',         '.verify-btn', '.submit-otp-btn',
                    'div:has-text("Submit")',       'div:has-text("SUBMIT")',
                    'div:has-text("Verify")',       'div:has-text("VERIFY")',
                    'span:has-text("Submit")',      'span:has-text("SUBMIT")',
                    'span:has-text("Verify")',      'span:has-text("VERIFY")',
                    '[role="button"]:has-text("Submit")', '[role="button"]:has-text("Verify")',
                )
                verify_clicked = False
                for sel in verify_selectors:
                    try:
                        if await page.is_visible(sel, timeout=1800):
                            el = await page.query_selector(sel)
                            if el:
                                # Force-enable even if still disabled
                                try:
                                    await page.evaluate("el => { el.disabled = false; el.removeAttribute('disabled'); }", el)
                                except Exception:
                                    pass
                                bb = await el.bounding_box()
                                if bb:
                                    await mouse_move_human(
                                        page,
                                        int(bb["x"] + bb["width"] / 2),
                                        int(bb["y"] + bb["height"] / 2),
                                    )
                                    await asyncio.sleep(random.uniform(0.10, 0.25))
                            await page.click(sel, timeout=3000, force=True)
                            verify_clicked = True
                            logger.info(f"[verify_otp] Submit clicked via '{sel}'")
                            break
                    except Exception as click_err:
                        err_str = str(click_err)
                        if "Execution context" in err_str or "context was destroyed" in err_str or "navigation" in err_str:
                            verify_clicked = True
                            logger.info(f"[verify_otp] Submit clicked via '{sel}' triggered navigation/refresh.")
                            break
                        continue

                if not verify_clicked:
                    try:
                        js_clicked = await page.evaluate("""() => {
                            const elements = Array.from(document.querySelectorAll('button, input[type="submit"], a, div, span'));
                            const btn = elements.find(el => {
                                if (el.offsetParent === null) return false;
                                const txt = (el.textContent || el.value || '').trim().toLowerCase();
                                const hasText = txt === 'submit' || txt === 'submit otp' || 
                                                txt === 'verify' || txt === 'verify otp' ||
                                                txt === 'confirm' || txt === 'login' ||
                                                txt === 'continue' || txt === 'proceed' ||
                                                txt.includes('submit') || txt.includes('verify');
                                if (!hasText) return false;
                                if (el.disabled) return false;
                                const className = (el.className || '').toLowerCase();
                                const role = (el.getAttribute('role') || '').toLowerCase();
                                const isClickable = el.tagName === 'BUTTON' || 
                                                    el.tagName === 'A' || 
                                                    role === 'button' ||
                                                    className.includes('btn') || 
                                                    className.includes('button') || 
                                                    className.includes('submit') || 
                                                    className.includes('verify') ||
                                                    className.includes('red') ||
                                                    el.style.cursor === 'pointer';
                                return isClickable;
                            });
                            if (btn) {
                                btn.click();
                                return true;
                            }
                            const anyBtn = Array.from(document.querySelectorAll('button, input[type="submit"]')).find(b => b.offsetParent !== null && !b.disabled);
                            if (anyBtn) {
                                anyBtn.click();
                                return true;
                            }
                            return false;
                        }""")
                        if js_clicked:
                            verify_clicked = True
                            logger.info("[verify_otp] Submit clicked via JS fallback")
                    except Exception as je:
                        err_str = str(je)
                        if "Execution context" in err_str or "context was destroyed" in err_str or "navigation" in err_str:
                            verify_clicked = True
                            logger.info("[verify_otp] JS fallback triggered navigation/refresh.")
                        else:
                            raise je

                if not verify_clicked:
                    try:
                        cookies = await context.cookies()
                        has_cookie = any(c.get("name") in LOGIN_COOKIES for c in cookies)
                        state = await detect_page_state(page)
                        if has_cookie or state == "logged_in":
                            verify_clicked = True
                            logger.info("[verify_otp] Already logged in / cookies present. Skipping submit button click.")
                    except Exception:
                        pass

                if not verify_clicked:
                    logger.warning(
                        "[verify_otp] OTP submit button not clicked/found. "
                        "Proceeding to wait for authentication cookies in case of auto-submit."
                    )

        # ── Wait for authentication cookies ────────────────────────────────
        await broadcast_status(request_token, "⏳ Waiting for authentication cookies…")
        # Give the page a moment to navigate after submit before touching the DOM
        await asyncio.sleep(0.3)

        # Check and solve recaptcha if prompted after clicking OTP Submit
        await solve_recaptcha_if_present(page, db, request_token)
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=8000)
        except Exception:
            pass
        # Check if there is an immediate error shown on page (e.g. incorrect OTP)
        if "login" in page.url.lower() or "otp" in page.url.lower():
            page_err = await check_page_errors(page)
            if page_err:
                raise Exception(page_err)

        cookies: list = []
        for attempt in range(120):   # up to 15 seconds (120 * 0.125s)
            # Dynamic check on each iteration to detect errors fast (only if we're still on the login page)
            if "login" in page.url.lower() or "otp" in page.url.lower():
                page_err = await check_page_errors(page)
                if page_err:
                    raise Exception(page_err)

            try:
                cookies = await context.cookies()
                has_cookie = any(c.get("name") in LOGIN_COOKIES for c in cookies)
                current_url = page.url.lower()
                is_logged_in_page = (
                    "login" not in current_url
                    and "otp" not in current_url
                    and any(k in current_url for k in ("store", "menu", "home", "order", "checkout", "profile", "account", "m.dominos", "dominos.co.in"))
                ) or (
                    # Root domain after redirect (e.g. https://m.dominos.co.in/)
                    "login" not in current_url
                    and "otp" not in current_url
                    and current_url.rstrip("/").endswith(".co.in")
                )
                if has_cookie or (is_logged_in_page and len(cookies) >= 2):
                    logger.info(f"[verify_otp] Login success detected on attempt {attempt + 1}")
                    break
            except Exception as ce:
                err_str = str(ce)
                if "Target page" in err_str or "Target closed" in err_str or "context was destroyed" in err_str:
                    # Browser closed during wait — check if DB already has the session
                    logger.warning(f"[verify_otp] Browser closed during cookie wait — checking DB for saved session")
                    req_obj2 = db.query(DominosOTPRequest).filter(DominosOTPRequest.request_token == request_token).first()
                    if req_obj2:
                        tl = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=5)
                        cs = db.query(DominosSession).filter(
                            DominosSession.mobile_number == req_obj2.mobile_number,
                            DominosSession.is_active == True,
                            DominosSession.created_at >= tl
                        ).order_by(DominosSession.created_at.desc()).first()
                        if cs:
                            return cs
                    break  # exit wait loop - will handle below
                logger.warning(f"[verify_otp] Cookie check attempt {attempt}: {ce}")
            await asyncio.sleep(0.12)
            if attempt % 8 == 7:
                try:
                    asyncio.create_task(
                        _capture_screenshot(request_token, page, "⏳ Waiting for authentication…")
                    )
                except Exception:
                    pass


        # Handle profile completions, overlays or redirects after login
        await handle_post_login_navigation(page, request_token)

        # ── Double check that login cookies or page state confirms login ──
        await asyncio.sleep(1.0)  # extra wait after post-login nav to allow cookies to settle
        cookies = await context.cookies()
        has_auth_cookie = any(c.get("name") in LOGIN_COOKIES for c in cookies)
        
        if not has_auth_cookie:
            # Fallback 1: check if page state says logged_in
            try:
                page_state = await detect_page_state(page)
                if page_state == "logged_in":
                    has_auth_cookie = True
                    logger.info("[verify_otp] Cookies absent but page state is logged_in — accepting login.")
            except Exception:
                pass

        if not has_auth_cookie:
            # Fallback 2: check localStorage for JWT / auth tokens
            try:
                ls_str = await page.evaluate("() => JSON.stringify(localStorage)")
                if ls_str:
                    import json as _json
                    ls = _json.loads(ls_str)
                    ls_auth_keys = {"token", "access_token", "accessToken", "authToken", "jwt",
                                    "customerToken", "custToken", "user_token", "customer_id",
                                    "customerId", "JFL_USER", "ut", "at", "ct"}
                    if any(k in ls for k in ls_auth_keys) and any(ls.get(k) for k in ls_auth_keys if k in ls):
                        has_auth_cookie = True
                        logger.info("[verify_otp] Auth token found in localStorage — accepting login.")
            except Exception:
                pass

        if not has_auth_cookie:
            page_err = await check_page_errors(page)
            if page_err:
                raise ValueError(f"OTP verification failed: {page_err}")
            else:
                raise ValueError("OTP verification failed: Incorrect OTP or login session was not established in the browser.")

        # ── Save session ────────────────────────────────────────────────────
        await broadcast_status(
            request_token, "🔑 Cookies captured! Saving session…", take_screenshot=True
        )

        # Extract localStorage data
        local_storage_data = None
        try:
            ls_str = await page.evaluate("() => JSON.stringify(localStorage)")
            if ls_str:
                local_storage_data = json.loads(ls_str)
                logger.info(f"[verify_otp] LocalStorage keys captured: {list(local_storage_data.keys())}")
        except Exception as lse:
            logger.warning(f"[verify_otp] Failed to extract localStorage: {lse}")

        # Double-check HTTP validation of cookies before saving!
        import httpx
        cookie_jar = {c["name"]: c["value"] for c in cookies if c.get("name") and c.get("value")}
        verify_http_ok = False
        resp_status = None
        
        # Check proxy settings
        proxy_url = os.getenv("STATIC_PROXY")
        normalized_proxy_url = None
        if proxy_url:
            try:
                from .proxy_manager import parse_proxy_string
                parsed = parse_proxy_string(proxy_url)
                normalized_proxy_url = parsed["normalized_url"]
            except Exception:
                pass

        try:
            async with httpx.AsyncClient(timeout=5.0, proxy=normalized_proxy_url) as client:
                resp = await client.get(
                    "https://m.dominos.co.in/api/en/v2/customer",
                    headers={
                        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                        "Accept": "application/json",
                        "Referer": "https://m.dominos.co.in/",
                    },
                    cookies=cookie_jar
                )
                resp_status = resp.status_code
            if resp.status_code == 200:
                verify_http_ok = True
                logger.info("[verify_otp] Fast HTTP verification succeeded before DB save!")
            else:
                logger.warning(f"[verify_otp] Fast HTTP verification returned status {resp.status_code} before DB save.")
        except Exception as ve:
            logger.warning(f"[verify_otp] Fast HTTP verification failed before DB save: {ve}")

        if resp_status == 401:
            raise ValueError("OTP submission succeeded, but the resulting session cookie was rejected by Domino's as unauthorized (401).")

        session = save_or_update_session(
            db=db,
            mobile_number=mobile_number,
            cookies=cookies,
            local_storage_data=local_storage_data,
            is_active=True,
            verify_status="valid" if (verify_http_ok or resp_status != 401) else "error",
            admin_id=admin.id
        )

        # Robot log: session saved
        log_robot_event(db, mobile_number, "INFO", "session_save",
            f"Session saved successfully for +91{mobile_number} (ID: {session.id})",
            {"session_id": session.id, "cookie_count": len(session.cookies or [])},
            session_id=session.id)

        # Clear verifying flag before cleanup
        if request_token in ACTIVE_OTP_REQUESTS:
            ACTIVE_OTP_REQUESTS[request_token]["login_success"] = True
            ACTIVE_OTP_REQUESTS[request_token]["verifying"] = False
        
        # Delay popping to allow frontend poll to detect login_success
        async def delayed_pop(tok):
            await asyncio.sleep(5)
            ACTIVE_OTP_REQUESTS.pop(tok, None)
        asyncio.create_task(delayed_pop(request_token))
        
        await broadcast_status(request_token, f"🎉 Session for +91{mobile_number} saved successfully!")

        if not is_manual:
            try:
                await context.close()
            except Exception:
                pass

        return session

    except Exception as e:
        err_msg = str(e)
        
        # Check if the background monitor already saved it in the DB (preventing false-negative crash)
        req_obj = db.query(DominosOTPRequest).filter(DominosOTPRequest.request_token == request_token).first()
        if req_obj:
            time_limit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=3)
            cached_session = db.query(DominosSession).filter(
                DominosSession.mobile_number == req_obj.mobile_number,
                DominosSession.is_active == True,
                DominosSession.created_at >= time_limit
            ).order_by(DominosSession.created_at.desc()).first()
            if cached_session:
                logger.info(f"[verify_otp] Exception caught, but session was successfully saved in DB by background monitor. Returning cached session.")
                if request_token in ACTIVE_OTP_REQUESTS:
                    ACTIVE_OTP_REQUESTS[request_token]["verifying"] = False
                ACTIVE_OTP_REQUESTS.pop(request_token, None)
                return cached_session

        # Log traceback only for real failures
        if "Target page, context or browser has been closed" in err_msg or "Target closed" in err_msg or "Browser has been closed" in err_msg:
            logger.warning(f"[verify_otp] Browser closed for {request_token}: {err_msg}")
        else:
            logger.exception(f"[verify_otp] Error for {request_token}: {err_msg}")

        # Robot log: verify_otp error
        _mobile = req_obj.mobile_number if req_obj else mobile_number
        log_robot_event(db, _mobile, "ERROR", "error",
            f"OTP verification failed: {err_msg[:300]}",
            {"request_token": request_token})
        # CRITICAL: Do NOT pop ACTIVE_OTP_REQUESTS here — keep the browser alive
        # so the admin can retry entering OTP without requesting a new one.
        # Only pop on context-level failures (browser_error already set) or navigation destroyed.
        is_fatal = (
            "browser_error" in (req_data or {})
            or "Target page, context or browser has been closed" in err_msg
            or "Browser has been closed" in err_msg
            or "Target closed" in err_msg
        )

        if is_fatal:
            ACTIVE_OTP_REQUESTS.pop(request_token, None)
            await broadcast_status(
                request_token,
                f"❌ Browser context lost. Click 'Request OTP' again to start fresh.\n({err_msg[:120]})"
            )
        else:
            # Non-fatal: keep browser open so admin can retry OTP entry
            if request_token in ACTIVE_OTP_REQUESTS:
                ACTIVE_OTP_REQUESTS[request_token]["browser_error"] = None  # reset so retry works
                ACTIVE_OTP_REQUESTS[request_token]["verifying"] = False  # let monitor resume
            await broadcast_status(
                request_token,
                f"❌ OTP verification failed — please try entering the OTP again.\n({err_msg[:120]})"
            )
        raise e


def sanitize_cookies(cookies_list: list) -> list:
    """Sanitizes and normalizes cookies list so that it is guaranteed to be accepted by Playwright."""
    if not isinstance(cookies_list, list):
        return []
        
    sanitized = []
    for c in cookies_list:
        if not isinstance(c, dict):
            continue
        # Check required fields
        name = c.get("name") or c.get("Name")
        value = c.get("value") if c.get("value") is not None else c.get("Value")
        if not name or value is None:
            continue
            
        cookie = {
            "name": str(name),
            "value": str(value)
        }
        
        # Domain validation
        domain = c.get("domain") or c.get("Domain")
        if domain:
            domain_str = str(domain)
            # Remove any port if present
            if ":" in domain_str:
                domain_str = domain_str.split(":")[0]
            cookie["domain"] = domain_str
        else:
            cookie["domain"] = ".dominos.co.in" # Default fallback
            
        # Path validation
        path = c.get("path") or c.get("Path")
        if path:
            cookie["path"] = str(path)
        else:
            cookie["path"] = "/"
            
        # Expires validation (must be float or int in seconds)
        expires = c.get("expires") or c.get("Expires") or c.get("expirationDate")
        if expires is not None:
            try:
                # If expires is a string/float, try to convert it
                expires_val = float(expires)
                if expires_val > 0:
                    cookie["expires"] = expires_val
            except (ValueError, TypeError):
                pass # Skip expires if invalid type or negative
                
        # HttpOnly and Secure boolean conversion
        http_only_val = c.get("httpOnly") if c.get("httpOnly") is not None else c.get("HttpOnly")
        if http_only_val is not None:
            if isinstance(http_only_val, str):
                cookie["httpOnly"] = http_only_val.lower() == "true"
            else:
                cookie["httpOnly"] = bool(http_only_val)

        secure_val = c.get("secure") if c.get("secure") is not None else c.get("Secure")
        if secure_val is not None:
            if isinstance(secure_val, str):
                cookie["secure"] = secure_val.lower() == "true"
            else:
                cookie["secure"] = bool(secure_val)
                    
        # SameSite normalization
        same_site = c.get("sameSite") or c.get("SameSite")
        if same_site:
            same_site_str = str(same_site).lower()
            if same_site_str in ("lax", "strict", "none"):
                # Capitalize correctly
                cookie["sameSite"] = same_site_str.capitalize()
            # If it's invalid (like "no_restriction"), just omit sameSite so browser uses default
            
        sanitized.append(cookie)
    return sanitized


def extract_auth_from_json(obj, target_cookies=None, profile_data=None):
    if target_cookies is None:
        target_cookies = []
    if profile_data is None:
        profile_data = {}
        
    if isinstance(obj, dict):
        for k, v in obj.items():
            k_lower = k.lower().replace("-", "_")
            if k_lower in {
                "token", "access_token", "id_token", "customer_token", "cust_token", "auth_token",
                "customerid", "customer_id", "client_id", "user_id", "userid", "jfl_user", "ci_session", "session_id"
            }:
                if isinstance(v, (str, int, float)) and v:
                    target_cookies.append({"name": k, "value": str(v), "domain": ".dominos.co.in", "path": "/"})
            if k_lower in {"mobile", "mobile_number", "mobile_no", "mobileno", "phone", "phone_number", "phonenumber"}:
                if isinstance(v, (str, int)) and v:
                    digits = "".join(c for c in str(v) if c.isdigit())
                    if len(digits) >= 10:
                        profile_data["mobile"] = digits[-10:]
            if isinstance(v, (dict, list)):
                extract_auth_from_json(v, target_cookies, profile_data)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                extract_auth_from_json(item, target_cookies, profile_data)
    return target_cookies, profile_data

def add_raw_session(
    db: Session, admin: User, mobile_number: str, cookies_json: str
) -> DominosSession:
    """Manually import raw JSON cookies (or raw HTTP Cookie header strings) for a Domino's session."""
    cookies_list = []
    extracted_mobile = None
    raw_str = cookies_json.strip()
    
    # 1. Attempt JSON parsing first
    try:
        parsed = json.loads(raw_str)
        extracted_cookies, profile_data = extract_auth_from_json(parsed)
        if profile_data.get("mobile"):
            extracted_mobile = profile_data["mobile"]
            
        if extracted_cookies:
            cookies_list = extracted_cookies
        elif isinstance(parsed, list):
            cookies_list = parsed
        elif isinstance(parsed, dict):
            if "name" in parsed and "value" in parsed:
                cookies_list = [parsed]
            else:
                cookies_list = [{"name": k, "value": str(v), "domain": ".dominos.co.in", "path": "/"} for k, v in parsed.items()]
    except Exception:
        # 2. If JSON fails, fall back to parsing as standard HTTP Cookie header string
        pairs = raw_str.split(";")
        for pair in pairs:
            if "=" in pair:
                parts = pair.split("=", 1)
                k = parts[0].strip()
                v = parts[1].strip()
                if k and v:
                    cookies_list.append({
                        "name": k,
                        "value": v,
                        "domain": ".dominos.co.in",
                        "path": "/"
                    })

    if not cookies_list:
        raise ValueError("Invalid cookie format. Provide a valid JSON array/object or a raw Cookie header string (name=value; name2=value2).")

    # Sanitize the imported cookies first
    sanitized_list = sanitize_cookies(cookies_list)
    if not sanitized_list:
        raise ValueError("No valid name/value cookies could be extracted from the provided text.")

    # Ensure at least one required authentication cookie exists in the sanitized cookies
    has_auth_cookie = any(c.get("name") in LOGIN_COOKIES for c in sanitized_list)
    if not has_auth_cookie:
        raise ValueError(
            "Authentication cookies not found. The imported session must contain at least one valid "
            f"auth cookie: {', '.join(sorted(list(LOGIN_COOKIES)))}."
        )

    # Clean mobile number
    if not mobile_number or len(mobile_number.strip()) < 10:
        if extracted_mobile:
            mobile_number = extracted_mobile
        else:
            raise ValueError("Mobile number is required and could not be auto-extracted from the JSON.")

    # Clean/format mobile number to be 10 digits without country prefix
    clean_mob = "".join(c for c in mobile_number if c.isdigit())
    if len(clean_mob) >= 10:
        mobile_number = clean_mob[-10:]

    db.query(DominosSession).filter(
        DominosSession.mobile_number == mobile_number
    ).delete()

    # Sync validation of imported cookies before database save
    import httpx
    cookie_jar = {c["name"]: c["value"] for c in sanitized_list if c.get("name") and c.get("value")}
    verify_status = "unknown"
    proxy_url = os.getenv("STATIC_PROXY")
    normalized_proxy_url = None
    if proxy_url:
        try:
            from .proxy_manager import parse_proxy_string
            parsed = parse_proxy_string(proxy_url)
            normalized_proxy_url = parsed["normalized_url"]
        except Exception:
            pass

    try:
        with httpx.Client(timeout=4.0, proxy=normalized_proxy_url) as client:
            resp = client.get(
                "https://m.dominos.co.in/api/en/v2/customer",
                headers={
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                    "Accept": "application/json",
                    "Referer": "https://m.dominos.co.in/",
                },
                cookies=cookie_jar
            )
            if resp.status_code == 200:
                verify_status = "valid"
            elif resp.status_code == 401:
                raise ValueError("Domino's rejected these cookies as unauthorized (HTTP 401). They might have expired.")
            else:
                logger.info(f"[Raw Import] HTTP verification returned {resp.status_code}. Allowing import.")
    except httpx.HTTPStatusError as hse:
        if hse.response.status_code == 401:
            raise ValueError("Domino's rejected these cookies as unauthorized (HTTP 401).")
    except Exception as e:
        if "rejected" in str(e) or "unauthorized" in str(e):
            raise e
        logger.info(f"[Raw Import] HTTP verification network error: {e}. Allowing import.")

    local_storage_data = {}
    for c in sanitized_list:
        name = c.get("name")
        val = c.get("value")
        if name and val:
            if name in {"token", "customerToken", "ACCESS_TOKEN", "access_token"}:
                local_storage_data["token"] = val
                local_storage_data["customerToken"] = val
                local_storage_data["accessToken"] = val
            if name in {"customerId", "customer_id", "user_id"}:
                local_storage_data["customerId"] = val

    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    session = DominosSession(
        mobile_number=mobile_number,
        cookies=sanitized_list,
        local_storage=local_storage_data,
        is_active=True,
        verify_status=verify_status,
        last_verified_at=now_utc,
        expires_at=now_utc + datetime.timedelta(days=14),
        admin_id=admin.id,
        created_at=now_utc,
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return session


# ─────────────────────────────────────────────────────────────────────────────
# Background monitor — auto-detect manual login
# ─────────────────────────────────────────────────────────────────────────────

async def _monitor_manual_login(token: str, mobile_number: str, admin_id: int):
    """Poll the page for up to OTP_WAIT_TIMEOUT; auto-save if login detected.
    This monitor ONLY observes — it does NOT attempt to recover dead pages."""
    req_data = ACTIVE_OTP_REQUESTS.get(token)
    if not req_data or not req_data.get("page"):
        return

    page    = req_data["page"]
    context = req_data["context"]
    browser = req_data["browser"]
    pw_ctx  = req_data["playwright_ctx"]

    last_screenshot_tick = -999

    # Use configurable timeout instead of hard-coded 4 minutes
    from ..settings import Settings
    _settings = Settings()
    poll_interval = 1.5  # seconds between checks
    max_ticks = int(_settings.OTP_WAIT_TIMEOUT / poll_interval)

    for tick in range(max_ticks):
        await asyncio.sleep(poll_interval)

        if token not in ACTIVE_OTP_REQUESTS:
            return  # verify_otp() already handled it

        # Always read the freshest references
        req_data = ACTIVE_OTP_REQUESTS.get(token)
        if not req_data:
            return

        # If verify_otp is currently active, skip this tick entirely to avoid races
        if req_data.get("verifying"):
            continue

        page    = req_data.get("page")
        context = req_data.get("context")
        browser = req_data.get("browser")
        pw_ctx  = req_data.get("playwright_ctx")

        try:
            # Observe-only: if the page is dead, stop monitoring and tell the user.
            # Do NOT attempt recovery — that creates duplicate pages and terminal load.
            if not await is_page_alive(page):
                logger.info(f"[Monitor] Page for token {token[:8]} is dead. Stopping monitor (no recovery).")
                await broadcast_status(token,
                    "⚠️ Browser page was closed. Click 'Request OTP' to start a new session.")
                # Pop from active list and clean up browser resources
                req_data = ACTIVE_OTP_REQUESTS.pop(token, None)
                if req_data:
                    browser = req_data.get("browser")
                    pw_ctx = req_data.get("playwright_ctx")
                    if browser:
                        try: await browser.close()
                        except Exception: pass
                    if pw_ctx:
                        try: await pw_ctx.stop()
                        except Exception: pass
                    context = req_data.get("context")
                    if context:
                        try: await context.close()
                        except Exception: pass
                return

            cookies      = await context.cookies()
            current_url  = page.url
            has_cookie   = any(c.get("name") in LOGIN_COOKIES for c in cookies)
            is_home      = (
                "login" not in current_url.lower()
                and any(k in current_url.lower() for k in ("store", "menu", "home", "order"))
            )

            # Periodic screenshot every 10 ticks (~15s) for faster feedback
            if tick - last_screenshot_tick >= 10:
                asyncio.create_task(
                    _capture_screenshot(
                        token, page,
                        f"🔍 Monitoring browser… ({int((tick + 1) * poll_interval)}s elapsed)"
                    )
                )
                last_screenshot_tick = tick

            if has_cookie or (is_home and len(cookies) > 5):
                # Verify that the active logged-in mobile number matches the requested one!
                is_match = await verify_logged_in_mobile(page, mobile_number)
                if not is_match:
                    logger.warning(f"[Monitor] Logged-in account does not match requested mobile +91{mobile_number}. Wiping mismatch context...")
                    try:
                        await page.context.clear_cookies()
                        await page.evaluate("() => localStorage.clear()")
                        await page.evaluate("() => sessionStorage.clear()")
                        await page.goto("https://m.dominos.co.in/login", wait_until="domcontentloaded", timeout=20000)
                    except Exception as clear_err:
                        logger.warning(f"[Monitor] Failed to clear mismatched account cookies: {clear_err}")
                    continue # Try again on next tick
                
                await broadcast_status(token, "✅ Login detected! Saving session…", take_screenshot=True)

                # Handle profile completions, overlays or redirects
                try:
                    await handle_post_login_navigation(page, token)
                except Exception:
                    pass

                # Re-fetch cookies after navigation completes
                try:
                    cookies = await context.cookies()
                except Exception:
                    pass

                from ..database import SessionLocal, DominosSession as DS
                db = SessionLocal()
                try:
                    # Extract localStorage data
                    local_storage_data = None
                    try:
                        ls_str = await page.evaluate("() => JSON.stringify(localStorage)")
                        if ls_str:
                            local_storage_data = json.loads(ls_str)
                            logger.info(f"[manual_monitor] LocalStorage keys captured: {list(local_storage_data.keys())}")
                    except Exception as lse:
                        logger.warning(f"[manual_monitor] Failed to extract localStorage: {lse}")

                    # Double-check HTTP validation of cookies before saving!
                    import httpx
                    cookie_jar = {c["name"]: c["value"] for c in cookies if c.get("name") and c.get("value")}
                    verify_http_ok = False
                    resp_status = None
                    
                    # Check proxy settings
                    proxy_url = os.getenv("STATIC_PROXY")
                    normalized_proxy_url = None
                    if proxy_url:
                        try:
                            from .proxy_manager import parse_proxy_string
                            parsed = parse_proxy_string(proxy_url)
                            normalized_proxy_url = parsed["normalized_url"]
                        except Exception:
                            pass

                    try:
                        async with httpx.AsyncClient(timeout=5.0, proxy=normalized_proxy_url) as client:
                            resp = await client.get(
                                "https://m.dominos.co.in/api/en/v2/customer",
                                headers={
                                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                                    "Accept": "application/json",
                                    "Referer": "https://m.dominos.co.in/",
                                },
                                cookies=cookie_jar
                            )
                            resp_status = resp.status_code
                        if resp.status_code == 200:
                            verify_http_ok = True
                            logger.info("[manual_monitor] Fast HTTP verification succeeded before DB save!")
                        else:
                            logger.warning(f"[manual_monitor] Fast HTTP verification returned status {resp.status_code} before DB save.")
                    except Exception as ve:
                        logger.warning(f"[manual_monitor] Fast HTTP verification failed before DB save: {ve}")

                    if resp_status == 401:
                        logger.warning("[manual_monitor] Wiping mismatch / invalid session on 401 status.")
                        try:
                            await page.context.clear_cookies()
                            await page.evaluate("() => localStorage.clear()")
                            await page.goto("https://m.dominos.co.in/login", wait_until="domcontentloaded", timeout=20000)
                        except Exception:
                            pass
                        continue # Try again on next tick

                    save_or_update_session(
                        db=db,
                        mobile_number=mobile_number,
                        cookies=cookies,
                        local_storage_data=local_storage_data,
                        is_active=True,
                        verify_status="valid" if (verify_http_ok or resp_status != 401) else "error",
                        admin_id=admin_id
                    )
                finally:
                    db.close()


                if token in ACTIVE_OTP_REQUESTS:
                    ACTIVE_OTP_REQUESTS[token]["login_success"] = True
                
                # Delay popping to allow frontend poll to detect login_success
                async def delayed_pop_mon(tok):
                    await asyncio.sleep(5)
                    ACTIVE_OTP_REQUESTS.pop(tok, None)
                asyncio.create_task(delayed_pop_mon(token))
                
                await broadcast_status(token, f"🎉 Account +91{mobile_number} saved automatically!")

                is_manual = req_data.get("manual_mode", False) if req_data else False
                if is_manual:
                    try: await browser.close()
                    except Exception: pass
                    try: await pw_ctx.stop()
                    except Exception: pass
                else:
                    try: await context.close()
                    except Exception: pass
                return

        except Exception as e:
            logger.debug(f"[Monitor] Tick {tick}: {e}")

    # If the loop finishes without returning, it means it timed out
    logger.info(f"[Monitor] Token {token[:8]} manual login monitor timed out after {_settings.OTP_WAIT_TIMEOUT}s.")
    try:
        await broadcast_status(
            token,
            f"❌ Login request timed out after {_settings.OTP_WAIT_TIMEOUT}s. Please click 'Request OTP' again to start a fresh login flow."
        )
    except Exception:
        pass

    req_data = ACTIVE_OTP_REQUESTS.pop(token, None)
    if req_data:
        is_manual = req_data.get("manual_mode", False)
        if is_manual:
            browser = req_data.get("browser")
            pw_ctx = req_data.get("playwright_ctx")
            if browser:
                try: await browser.close()
                except Exception: pass
            if pw_ctx:
                try: await pw_ctx.stop()
                except Exception: pass
        else:
            context = req_data.get("context")
            if context:
                try: await context.close()
                except Exception: pass


async def verify_logged_in_mobile(page: Page, target_mobile: str) -> bool:
    """Verify that the currently logged-in account on the page matches the target mobile number."""
    try:
        # First check page state! If we're on login or OTP page, we are NOT logged in!
        state = await detect_page_state(page)
        if state in ("otp_boxes", "otp_single", "mobile_form"):
            logger.info(f"[verify_logged_in_mobile] Page is in '{state}' state. Definitely not logged in.")
            return False

        res = await page.evaluate("""(target) => {
            // 1. Scan localStorage for target mobile
            for (let i = 0; i < localStorage.length; i++) {
                const k = localStorage.key(i);
                const v = localStorage.getItem(k);
                if (v && v.includes(target)) return true;
            }
            // 2. Scan cookies
            if (document.cookie.includes(target)) return true;
            
            // 3. Scan DOM text for the target mobile number
            const text = document.body ? document.body.innerText : '';
            if (text.includes(target)) return true;
            
            return false;
        }""", target_mobile)
        return bool(res)
    except Exception:
        return False


async def delete_session_resources(db: Session, session_id: str, mobile_number: str):
    """Completely clean up and delete a Domino's session from memory, cache, storage, database, browser context, cookies, and any in-memory registry."""
    # 1. Clean up from ACTIVE_OTP_REQUESTS (in-memory registry)
    for token, req_data in list(ACTIVE_OTP_REQUESTS.items()):
        if req_data.get("mobile_number") == mobile_number:
            logger.info(f"[Cleanup] Closing active OTP browser request for +91{mobile_number} on session deletion.")
            page = req_data.get("page")
            if page:
                try:
                    await page.context.clear_cookies()
                    await page.evaluate("() => localStorage.clear()")
                    await page.evaluate("() => sessionStorage.clear()")
                except Exception:
                    pass
            context = req_data.get("context")
            if context:
                try: await context.close()
                except Exception: pass
            browser = req_data.get("browser")
            if browser:
                try: await browser.close()
                except Exception: pass
            pw_ctx = req_data.get("playwright_ctx")
            if pw_ctx:
                try: await pw_ctx.stop()
                except Exception: pass
            ACTIVE_OTP_REQUESTS.pop(token, None)

    from app.backend import routes as _routes_mod
    if hasattr(_routes_mod, "OPENED_ADMIN_BROWSERS"):
        for b in list(_routes_mod.OPENED_ADMIN_BROWSERS):
            if b.get("session_id") == session_id:
                logger.info(f"[Cleanup] Closing open admin browser for session {session_id} on deletion.")
                page = b.get("page")
                if page:
                    try:
                        await page.context.clear_cookies()
                        await page.evaluate("() => localStorage.clear()")
                        await page.evaluate("() => sessionStorage.clear()")
                    except Exception:
                        pass
                try: await b["browser"].close()
                except Exception: pass
                try: await b["playwright"].stop()
                except Exception: pass
                if b in _routes_mod.OPENED_ADMIN_BROWSERS:
                    _routes_mod.OPENED_ADMIN_BROWSERS.remove(b)

    # 3. Clean up OTP requests in DB
    db.query(DominosOTPRequest).filter(DominosOTPRequest.mobile_number == mobile_number).delete()
    db.commit()


async def validate_and_get_session(db: Session, mobile_number: str) -> Optional[DominosSession]:
    """
    Validates if an active session exists in DB for this mobile number,
    and checks if the cookies/authentication are still valid via a quick HTTP check.
    Only discards the session if a definitive 401 Unauthorized is returned.
    """
    session = db.query(DominosSession).filter(
        DominosSession.mobile_number == mobile_number,
        DominosSession.is_active == True
    ).order_by(DominosSession.created_at.desc()).first()
    
    if not session:
        return None
        
    # Check if expired by date/time
    if session.expires_at and session.expires_at < datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None):
        return None
        
    # Validate cookies via quick HTTP call to Domino's customer API
    import httpx
    cookies_list = session.cookies or []
    cookie_jar = {c["name"]: c["value"] for c in cookies_list if c.get("name") and c.get("value")}
    try:
        proxy_url = os.getenv("STATIC_PROXY")
        normalized_proxy_url = None
        if proxy_url:
            try:
                from .proxy_manager import parse_proxy_string
                parsed = parse_proxy_string(proxy_url)
                normalized_proxy_url = parsed["normalized_url"]
            except Exception as pe:
                logger.warning(f"Failed to parse STATIC_PROXY: {pe}")
        async with httpx.AsyncClient(timeout=4.0, proxy=normalized_proxy_url) as client:
            resp = await client.get(
                "https://m.dominos.co.in/api/en/v2/customer",
                headers={
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                    "Accept": "application/json",
                    "Referer": "https://m.dominos.co.in/",
                },
                cookies=cookie_jar
            )
            if resp.status_code == 200:
                return session
            elif resp.status_code == 401:
                logger.warning(f"Session for +91{mobile_number} is definitively invalid (HTTP 401). Deactivating in DB.")
                session.is_active = False
                session.verify_status = "expired"
                db.commit()
                return None
            else:
                # 403 Cloudflare block or 503 Service Unavailable: Trust session for Playwright fallback
                logger.info(f"Session HTTP validation returned status {resp.status_code} (Cloudflare block?). Trusting session for Playwright use.")
                return session
    except Exception as e:
        logger.info(f"Session HTTP validation connection error/timeout ({e}). Trusting session for Playwright use.")
        return session


