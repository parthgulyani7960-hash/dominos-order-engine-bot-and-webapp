"""
human_actions.py — Ultra-fast, human-like browser automation primitives.

Design principles:
  1. Fast by default   — delays are realistic minimums (30-80 ms), not safety buffers.
  2. Human entropy     — randomness follows empirical typing/mouse stats.
  3. Concurrent fills  — multiple form fields filled in parallel via asyncio.gather().
  4. Bot-evasion       — cubic Bezier mouse curves, key-chord timing jitter.
  5. Zero hard sleeps  — every wait is either a random sample or condition-based poll.
"""

import asyncio
import random
from typing import Optional, List, Tuple

from playwright.async_api import Page, ElementHandle

# Typing speed profiles (min_sec, max_sec per character) - Optimized for speed
_SPEED_PROFILES = {
    "fast":   (0.008, 0.020),
    "normal": (0.015, 0.035),
    "slow":   (0.040, 0.080),
}
_SLOW_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+{}|:~")
_PAUSE_AFTER = {',': 0.02, '.': 0.03, ' ': 0.01, '@': 0.02}


async def mouse_move(page: Page, x: int, y: int, steps: int = 4) -> None:
    """Move mouse along a cubic Bezier curve. Ultra-fast (4 steps)."""
    try:
        cur = await page.evaluate(
            "() => ({ x: window._mx || Math.random()*400+50, y: window._my || Math.random()*300+50 })"
        )
        cx = float(cur.get("x", random.randint(80, 360)))
        cy = float(cur.get("y", random.randint(80, 280)))
    except Exception:
        cx, cy = random.randint(80, 360), random.randint(80, 280)

    cp1x = cx + random.randint(-40, 40)
    cp1y = cy + random.randint(-20, 20)
    cp2x = x  + random.randint(-20, 20)
    cp2y = y  + random.randint(-15, 15)

    try:
        for i in range(1, steps + 1):
            t = i / steps
            mt = 1 - t
            bx = mt**3*cx + 3*mt**2*t*cp1x + 3*mt*t**2*cp2x + t**3*x
            by = mt**3*cy + 3*mt**2*t*cp1y + 3*mt*t**2*cp2y + t**3*y
            if i == steps // 2:
                bx += random.uniform(-1, 1)
                by += random.uniform(-1, 1)
            await page.mouse.move(bx, by)
            await asyncio.sleep(random.uniform(0.001, 0.003))
        await page.evaluate(f"() => {{ window._mx={x}; window._my={y}; }}")
    except Exception:
        try:
            await page.mouse.move(x, y)
        except Exception:
            pass


async def human_click(page: Page, selector: str, timeout: int = 3000) -> bool:
    """Natural mouse move + click on selector. Returns True on success."""
    try:
        el = await page.wait_for_selector(selector, state="visible", timeout=timeout)
        if not el:
            return False
        bb = await el.bounding_box()
        if bb:
            cx = int(bb["x"] + bb["width"]  * random.uniform(0.35, 0.65))
            cy = int(bb["y"] + bb["height"] * random.uniform(0.35, 0.65))
            await mouse_move(page, cx, cy)
            await asyncio.sleep(random.uniform(0.04, 0.08))
        await el.click()
        return True
    except Exception:
        try:
            await page.click(selector, timeout=timeout)
            return True
        except Exception:
            return False


async def human_click_el(page: Page, el: ElementHandle) -> bool:
    """Human click on an already-found ElementHandle."""
    try:
        bb = await el.bounding_box()
        if bb:
            cx = int(bb["x"] + bb["width"]  * random.uniform(0.35, 0.65))
            cy = int(bb["y"] + bb["height"] * random.uniform(0.35, 0.65))
            await mouse_move(page, cx, cy)
            await asyncio.sleep(random.uniform(0.02, 0.06))
        await el.click()
        return True
    except Exception:
        return False


async def _react_set_value(page: Page, selector: str, value: str) -> bool:
    try:
        result = await page.evaluate(
            """([sel, val]) => {
                const el = document.querySelector(sel);
                if (!el) return false;
                const ns = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
                if (ns) ns.call(el, val); else el.value = val;
                ['input','change'].forEach(ev => el.dispatchEvent(new Event(ev,{bubbles:true})));
                el.dispatchEvent(new KeyboardEvent('keyup',{bubbles:true}));
                return true;
            }""",
            [selector, value]
        )
        return bool(result)
    except Exception:
        return False


async def _fire_events(page: Page, selector: str) -> None:
    try:
        await page.evaluate(
            """(sel) => {
                const el = document.querySelector(sel);
                if (!el) return;
                const ns = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value')?.set;
                if (ns) ns.call(el, el.value);
                ['input','change'].forEach(ev => el.dispatchEvent(new Event(ev,{bubbles:true})));
                el.dispatchEvent(new KeyboardEvent('keyup',{bubbles:true}));
            }""",
            selector
        )
    except Exception:
        pass


async def human_type(
    page: Page,
    selector: str,
    text: str,
    speed: str = "normal",
    clear_first: bool = True,
) -> bool:
    """Type text into selector with human-like character-by-character keystroke timing."""
    min_d, max_d = _SPEED_PROFILES.get(speed, _SPEED_PROFILES["normal"])
    try:
        await human_click(page, selector, timeout=3000)
        await asyncio.sleep(random.uniform(0.05, 0.12))

        if clear_first:
            await page.keyboard.press("Control+a")
            await asyncio.sleep(random.uniform(0.03, 0.08))
            await page.keyboard.press("Delete")
            await asyncio.sleep(random.uniform(0.03, 0.08))

        # Always type character-by-character for anti-bot bypass
        for ch in text:
            delay = random.uniform(min_d, max_d)
            if ch in _SLOW_CHARS:
                delay *= 1.4
            extra = _PAUSE_AFTER.get(ch, 0)
            await page.keyboard.type(ch, delay=int(delay * 1000) if hasattr(page.keyboard, 'type') else 0)
            await asyncio.sleep(delay + extra)

        await _fire_events(page, selector)
        return True
    except Exception:
        return False


async def fill_form_fields(
    page: Page,
    fields: List[Tuple[str, str]],
    speed: str = "fast",
) -> dict:
    """
    Fill multiple form fields SIMULTANEOUSLY using asyncio.gather().
    Returns {selector: True/False} result map.
    """
    results = await asyncio.gather(
        *[human_type(page, sel, val, speed=speed) for sel, val in fields],
        return_exceptions=True
    )
    return {sel: (r is True) for (sel, _), r in zip(fields, results)}


async def human_scroll(page: Page, direction: str = "down", amount: int = 250) -> None:
    """Scroll with natural speed — short bursts with micro-pauses."""
    sign = 1 if direction == "down" else -1
    chunks = random.randint(2, 4)
    per_chunk = amount // chunks
    for _ in range(chunks):
        delta = per_chunk + random.randint(-15, 15)
        await page.mouse.wheel(0, sign * delta)
        await asyncio.sleep(random.uniform(0.04, 0.09))


async def fast_wait(page: Page, min_ms: int = 80, max_ms: int = 250) -> None:
    """Minimal human-paced wait. Much faster than old smart_wait(800-2000ms)."""
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=100)
    except Exception:
        pass
    await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


async def wait_for_any(
    page: Page,
    selectors: List[str],
    timeout: int = 5000,
) -> Optional[str]:
    """Wait for ANY selector to appear. Returns first visible selector or None."""
    import time
    deadline = time.monotonic() + timeout / 1000
    while time.monotonic() < deadline:
        for sel in selectors:
            try:
                if await page.is_visible(sel, timeout=100):
                    return sel
            except Exception:
                pass
        await asyncio.sleep(0.07)
    return None


async def poll_until(page: Page, js_expr: str, timeout: int = 8000, interval: int = 120) -> bool:
    """Poll JS expression until truthy or timeout. Fast condition-based wait."""
    import time
    deadline = time.monotonic() + timeout / 1000
    while time.monotonic() < deadline:
        try:
            if await page.evaluate(f"() => !!({js_expr})"):
                return True
        except Exception:
            pass
        await asyncio.sleep(interval / 1000)
    return False


async def fill_otp_fast(page: Page, otp: str) -> bool:
    """
    Fill OTP as fast as possible using parallel JS injection.
    Covers both single-char box grids and unified input fields.
    """
    # Strategy 1: JS batch inject into all OTP boxes simultaneously
    try:
        result = await page.evaluate(
            """(otp) => {
                const ns = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value')?.set;
                function sv(el, v) {
                    if (ns) ns.call(el,v); else el.value=v;
                    ['input','change'].forEach(ev=>el.dispatchEvent(new Event(ev,{bubbles:true})));
                    el.dispatchEvent(new KeyboardEvent('keyup',{bubbles:true}));
                }
                const boxes = Array.from(document.querySelectorAll('input[maxlength="1"]'))
                    .filter(i=>i.offsetParent!==null);
                if (boxes.length >= 4) {
                    otp.split('').forEach((ch,i)=>{ if(boxes[i]) sv(boxes[i],ch); });
                    return 'boxes:'+boxes.length;
                }
                const single = document.querySelector(
                    'input[autocomplete="one-time-code"],input[maxlength="6"],' +
                    'input[maxlength="4"],input[name*="otp" i],input[id*="otp" i]'
                );
                if (single) { sv(single, otp); return 'single'; }
                return false;
            }""",
            otp
        )
        if result:
            return True
    except Exception:
        pass

    # Strategy 2: keyboard per digit — minimal delay
    try:
        boxes = await page.query_selector_all('input[maxlength="1"]')
        visible = [b for b in boxes if await _safe_vis(b)]
        if visible:
            for i, box in enumerate(visible[:len(otp)]):
                await box.click()
                await asyncio.sleep(random.uniform(0.020, 0.045))
                await page.keyboard.press(otp[i], delay=random.randint(20, 50))
            return True
    except Exception:
        pass
    return False


async def _safe_vis(el: ElementHandle) -> bool:
    try:
        return await el.is_visible()
    except Exception:
        return False


async def inject_human_signals(page: Page) -> None:
    """
    Inject stealth JS before any page load:
    - Removes webdriver flag
    - Fakes plugins/languages/touchpoints
    - Adds mouse position tracking
    - Realistic screen dims (iPhone 14 Pro)
    """
    try:
        await page.add_init_script("""
            (() => {
                Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
                Object.defineProperty(navigator,'plugins',{
                    get:()=>[1,2,3,4,5].map(i=>({name:Plugin ,filename:p.dll,description:P,length:1}))
                });
                Object.defineProperty(navigator,'languages',{get:()=>['en-IN','en-US','en','hi']});
                Object.defineProperty(navigator,'maxTouchPoints',{get:()=>5});
                window.ontouchstart=null;
                window._mx=Math.random()*350+80;
                window._my=Math.random()*250+80;
                document.addEventListener('mousemove',e=>{window._mx=e.clientX;window._my=e.clientY},{passive:true});
                Object.defineProperty(screen,'width',{get:()=>393});
                Object.defineProperty(screen,'height',{get:()=>852});
                Object.defineProperty(screen,'availWidth',{get:()=>393});
                Object.defineProperty(screen,'availHeight',{get:()=>852});
                // Chrome runtime object
                if (!window.chrome) {
                    window.chrome = { runtime: {} };
                }
            })();
        """)
    except Exception:
        pass


async def random_micro_action(page: Page) -> None:
    """Random tiny human action between major steps (scroll/mouse drift)."""
    action = random.choice(["scroll", "move", "nothing", "nothing"])
    try:
        if action == "scroll":
            await page.mouse.wheel(0, random.choice([-1,1]) * random.randint(20, 50))
        elif action == "move":
            await page.mouse.move(random.randint(80, 300), random.randint(100, 380))
        await asyncio.sleep(random.uniform(0.03, 0.08))
    except Exception:
        pass
