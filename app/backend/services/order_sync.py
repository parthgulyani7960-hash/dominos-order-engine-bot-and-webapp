import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

from .captcha_solver import CaptchaSolver
from .proxy_manager import ProxyManager
from .human_actions import (
    mouse_move as mouse_move_human,
    human_click,
    human_type,
    fill_form_fields,
    fill_otp_fast,
    fast_wait,
    wait_for_any,
    poll_until,
    inject_human_signals,
    random_micro_action,
    human_scroll,
)


logger = logging.getLogger(__name__)

ORDER_SEMAPHORE = asyncio.Semaphore(5)
BUSY_SESSION_IDS = set()


def build_order_steps(order) -> List[Tuple[str, str]]:
    """
    Builds the dynamic ordered step list for a specific order.
    Only includes steps that are relevant Ã¢â¬â e.g. gift card step only when order has one.
    Progress % is computed against len(build_order_steps(order)) so the live log
    is always accurate regardless of which optional steps are included.
    """
    steps = [
        ("Domino's: Pre-flight Check",     "Ã°Å¸âÂ Validating order data..."),
        ("Domino's: Launching Browser",    "Ã°Å¸Å¡â¬ Launching browser with session cookies..."),
        ("Domino's: Setting Location",     "Ã°Å¸âÂ Finding nearest Domino's store for your area..."),
        ("Domino's: Adding Items",         "Ã°Å¸âºâ Adding items to cart on Domino's..."),
        ("Domino's: Verifying Cart",       "Ã¢Åâ¦ Verifying cart items and totals..."),
        ("Domino's: Opening Checkout",     "Ã°Å¸âÂ³ Proceeding to checkout page..."),
        ("Domino's: Filling Address",      "Ã°Å¸ÂÂ  Filling delivery address and phone..."),
    ]
    if getattr(order, 'gift_card', None):
        steps.append(("Domino's: Applying Gift Card", "Ã°Å¸Å½Â Applying gift card voucher & PIN..."))
    steps += [
        ("Domino's: Solving CAPTCHA",      "Ã°Å¸Â¤â Solving CAPTCHA challenge..."),
        ("Domino's: Finalizing Order",     "Ã°Å¸âÂ¦ Placing the order on Domino's..."),
        ("Domino's: Completed",            "Ã¢Åâ¦ Order placed successfully on Domino's!"),
    ]
    return steps


# Keep for backwards compat Ã¢â¬â callers that don't have an order object
ORDER_STEPS = [
    ("Domino's: Pre-flight Check",     "Ã°Å¸âÂ Validating order data..."),
    ("Domino's: Launching Browser",    "Ã°Å¸Å¡â¬ Launching browser with session cookies..."),
    ("Domino's: Setting Location",     "Ã°Å¸âÂ Finding nearest Domino's store for your area..."),
    ("Domino's: Adding Items",         "Ã°Å¸âºâ Adding items to cart on Domino's..."),
    ("Domino's: Verifying Cart",       "Ã¢Åâ¦ Verifying cart items and totals..."),
    ("Domino's: Opening Checkout",     "Ã°Å¸âÂ³ Proceeding to checkout page..."),
    ("Domino's: Filling Address",      "Ã°Å¸ÂÂ  Filling delivery address and phone..."),
    ("Domino's: Applying Gift Card",   "Ã°Å¸Å½Â Applying gift card voucher & PIN..."),
    ("Domino's: Solving CAPTCHA",      "Ã°Å¸Â¤â Solving CAPTCHA challenge..."),
    ("Domino's: Finalizing Order",     "Ã°Å¸âÂ¦ Placing the order on Domino's..."),
    ("Domino's: Completed",            "Ã¢Åâ¦ Order placed successfully on Domino's!"),
]


def map_to_dominos_name(product_name: str) -> str:
    """Map internal product names to standard Domino's India menu items."""
    name_map = {
        # Custom internal names Ã¢â â Domino's India names
        "Margherita Classic":         "Margherita",
        "Pepperoni Feast":            "Pepperoni",
        "Garden Veggie Supreme":      "Veg Extravaganza",
        "BBQ Smoked Chicken":         "Chicken Golden Delight",
        "Double Cheese Romano":       "Double Cheese Margherita",
        "Cheeseburst Margherita":     "Margherita",
        "Tomato Onion Pizza Mania":   "Tomato",
        "Golden Corn Pizza Mania":    "Golden Corn",
        "Truffle Mushroom Artisan":   "Peppy Paneer",
        # Additional common internal names
        "Paneer Special":             "Paneer Makhani",
        "Farmhouse Veggie":           "Farmhouse",
        "Chicken Supreme":            "Non Veg Supreme",
        "Spicy Chicken Pizza":        "Spicy Chicken",
        "Simply Cheese":              "Simply Veggie",
        "Butter Chicken Pizza":       "Chicken Tikka",
    }
    return name_map.get(product_name, product_name)


class OrderSyncer:
    """Synchronize an internal order with Domino's website using Playwright.

    Instantiated per request with a SQLAlchemy session so that the static proxy
    can be logged via ``ProxyManager``. ``place_order`` performs the full browser
    workflow in a try/except block and returns a simple result dictionary.
    """

    def __init__(self, db_session):
        self.db = db_session
        self.captcha_solver = None
        try:
            self.captcha_solver = CaptchaSolver()
        except Exception as e:
            logger.warning(f"CaptchaSolver initialization skipped: {e}")
        # Initialize ProxyManager with support for empty proxy fallback
        self.proxy_manager = None
        try:
            self.proxy_manager = ProxyManager(db_session)
        except Exception as e:
            logger.warning(f"ProxyManager initialization skipped: {e}")
        self.base_url = os.getenv("DOMINOS_BASE_URL", "https://www.dominos.co.in")
        self._sse_callback = None  # injected after init if needed

    async def _broadcast(self, order, step_label: str, step_message: str, step_index: int, total_steps: int):
        """Broadcasts real-time step progress via SSE to the admin panel."""
        try:
            from .. import routes
            if routes.sse_broadcast_callback:
                await routes.sse_broadcast_callback({
                    "type": "dominos_progress",
                    "order_id": order.id,
                    "step_label": step_label,
                    "step_message": step_message,
                    "step_index": step_index,
                    "total_steps": total_steps,
                    "progress_pct": int((step_index / total_steps) * 100),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            logger.warning(f"Failed to broadcast step update: {e}")

        # Also write to RobotLog for the Admin Robot Live Log tab
        try:
            from .dominos_session_manager import log_robot_event
            level = "INFO"
            if "fail" in step_message.lower() or "error" in step_message.lower() or "cancel" in step_message.lower() or "timeout" in step_message.lower():
                level = "ERROR"
            elif "warning" in step_message.lower():
                level = "WARNING"
                
            log_robot_event(
                db=self.db,
                mobile=order.phone or "unknown",
                level=level,
                stage="order_placement",
                message=f"[Order {order.id}] {step_label}: {step_message}",
                details={"order_id": order.id, "progress_pct": int((step_index / total_steps) * 100)},
                session_id=order.id
            )
        except Exception as e:
            logger.warning(f"Failed to write order step to RobotLog: {e}")

    async def _update_db_status(self, order, status_text: str):
        """Updates the order status in the DB and commits."""
        try:
            order.status = status_text
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to update DB status: {e}")

    async def _solve_captcha(self, page: Page) -> None:
        """Detect the reCAPTCHA widget, extract its site-key and solve it."""
        if not self.captcha_solver:
            logger.info("CaptchaSolver not initialized (API key missing) - skipping captcha solve step")
            return
        try:
            # Check if captcha is actually present with a short timeout to prevent hanging
            try:
                await page.wait_for_selector('iframe[src*="recaptcha"]', timeout=4000)
            except Exception:
                logger.info("reCAPTCHA element not found on page Ã¢â¬â skipping solve")
                return

            element = await page.query_selector('.g-recaptcha')
            if not element:
                logger.warning("reCAPTCHA element class not found Ã¢â¬â skipping solve")
                return
            site_key = await element.get_attribute('data-sitekey')
            if not site_key:
                logger.warning("reCAPTCHA site-key missing Ã¢â¬â skipping solve")
                return
            token = self.captcha_solver.solve_recaptcha(site_key, page.url)
            await page.evaluate("""
                (token) => {
                    let textarea = document.getElementById('g-recaptcha-response');
                    if (!textarea) {
                        textarea = document.createElement('textarea');
                        textarea.id = 'g-recaptcha-response';
                        textarea.name = 'g-recaptcha-response';
                        textarea.style.display = 'none';
                        document.body.appendChild(textarea);
                    }
                    textarea.value = token;
                }
            """, token)
            logger.info("reCAPTCHA solved and token injected")
        except Exception as e:
            logger.exception(f"Captcha solving failed: {e}")

    async def _switch_category(self, page: Page, category: str):
        cat_name = category.lower()
        target_tab = None
        if "veg" in cat_name and "non" not in cat_name:
            target_tab = "Veg Pizza"
        elif "non" in cat_name:
            target_tab = "Non-Veg Pizza"
        elif "mania" in cat_name or "everyday" in cat_name:
            target_tab = "Pizza Mania"
        elif "side" in cat_name:
            target_tab = "Sides"
        elif "dessert" in cat_name:
            target_tab = "Desserts"
        elif "drink" in cat_name or "beverage" in cat_name:
            target_tab = "Beverages"
            
        if not target_tab:
            return
            
        logger.info(f"[Category] Switching to Domino's navigation tab: {target_tab}")
        for sel in [f'span:has-text("{target_tab}")', f'div:has-text("{target_tab}")', f'button:has-text("{target_tab}")', f'[data-category*="{target_tab}"]']:
            try:
                if await page.is_visible(sel, timeout=1200):
                    if await human_click(page, sel):
                        await fast_wait(page, 400, 900)
                        return
            except Exception:
                pass

    async def _set_dominos_location(self, page: Page, order, lat: Optional[float], lng: Optional[float]) -> str:
        """
        4-phase store location setting for Domino's India.

        Phase 0: Override browser navigator.geolocation with exact user coordinates BEFORE page loads.
        Phase 1: Inject location into Domino's localStorage keys (lastLocationNew, storeId, CHILD_STORE_ID_IS).
                 Use session-authenticated API call to get nearest storeId.
        Phase 2: Navigate directly to that store's menu URL (fastest path).
        Phase 3: Fallback Ã¢â¬â Address text search in the search box.

        Returns: store description string for logging.
        """
        store_name = "Unknown Store"
        store_id: Optional[str] = None

        # Ã¢ââ¬Ã¢ââ¬ Phase 0: Intercept navigator.geolocation to return user's exact GPS Ã¢ââ¬Ã¢ââ¬
        if lat and lng:
            try:
                logger.info(f"[Location P0] Injecting GPS override: lat={lat}, lng={lng}")
                # Register init script so it runs even before first page load
                await page.context.add_init_script(f"""
                    (() => {{
                        const _coords = {{ latitude: {lat}, longitude: {lng}, accuracy: 10 }};
                        const _position = {{
                            coords: _coords,
                            timestamp: Date.now()
                        }};
                        // Override getCurrentPosition
                        navigator.geolocation.getCurrentPosition = (success, error, opts) => {{
                            setTimeout(() => success(_position), 50);
                        }};
                        // Override watchPosition
                        navigator.geolocation.watchPosition = (success, error, opts) => {{
                            setTimeout(() => success(_position), 50);
                            return 1;
                        }};
                        // Permissions API override
                        if (navigator.permissions && navigator.permissions.query) {{
                            const _origQuery = navigator.permissions.query.bind(navigator.permissions);
                            navigator.permissions.query = (desc) => {{
                                if (desc && desc.name === 'geolocation') {{
                                    return Promise.resolve({{ state: 'granted', onchange: null }});
                                }}
                                return _origQuery(desc);
                            }};
                        }}
                        console.log('[GPS Override] Geolocation set to lat={lat}, lng={lng}');
                    }})();
                """)
                logger.info("[Location P0] Ã¢Åâ¦ GPS override registered in browser context.")
            except Exception as p0_err:
                logger.warning(f"[Location P0] GPS override failed (non-fatal): {p0_err}")

        # Ã¢ââ¬Ã¢ââ¬ Phase 1: Inject Domino's localStorage location state Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
        if lat and lng:
            try:
                logger.info(f"[Location P1] Querying Domino's authenticated store API for lat={lat}, lng={lng}")

                # Build cookie jar from session cookies
                # Try multiple known Domino's India store locator endpoints
                api_endpoints = [
                    ("https://m.dominos.co.in/api/en/v1/store/nearme", {"lat": lat, "lng": lng, "type": "delivery"}),
                    ("https://m.dominos.co.in/api/en/v2/store/nearme", {"lat": lat, "lng": lng, "orderMode": "DELIVERY"}),
                    ("https://m.dominos.co.in/api/en/v1/storelocator", {"lat": lat, "lng": lng}),
                    ("https://api.dominos.co.in/storelocator/api/v1/store/search", {"latitude": lat, "longitude": lng, "orderMode": "DELIVERY"}),
                ]

                for endpoint_url, params in api_endpoints:
                    try:
                        # Use browser fetch to query the endpoint directly from the browser context
                        # to inherit valid session cookies, headers, user-agent, and TLS fingerprints
                        method = "POST" if "search" in endpoint_url else "GET"
                        logger.info(f"[Location P1] Querying Domino's API {endpoint_url} via browser fetch...")
                        
                        js_code = """async (args) => {
                            try {
                                const fetchOptions = {
                                    method: args.method,
                                    headers: {
                                        "Accept": "application/json, text/plain, */*",
                                        "Content-Type": "application/json"
                                    }
                                };
                                if (args.method === "POST") {
                                    fetchOptions.body = JSON.stringify(args.params);
                                }
                                const url = args.method === "GET" 
                                    ? args.url + "?" + new URLSearchParams(args.params).toString()
                                    : args.url;
                                const resp = await fetch(url, fetchOptions);
                                if (resp.ok) {
                                    return await resp.json();
                                }
                                return { error: `HTTP ${resp.status}` };
                            } catch (e) {
                                return { error: e.message };
                            }
                        }"""
                        
                        data = await page.evaluate(js_code, {
                            "url": endpoint_url,
                            "method": method,
                            "params": params
                        })
                        
                        if data and not data.get("error"):
                            logger.info(f"[Location P1] ✅ API {endpoint_url} returned JSON: {str(data)[:300]}")
                            # Parse different known response shapes
                            stores = (
                                data.get("stores") or data.get("storeList") or
                                data.get("data", {}).get("stores") or
                                data.get("result") or
                                (data if isinstance(data, list) else [])
                            )
                            if stores and isinstance(stores, list) and len(stores) > 0:
                                nearest = stores[0]
                                store_id = str(
                                    nearest.get("storeId") or nearest.get("store_id") or
                                    nearest.get("id") or nearest.get("storeID") or ""
                                )
                                store_name = (
                                    nearest.get("storeName") or nearest.get("name") or
                                    nearest.get("storeAddress") or "Store"
                                )
                                if store_id:
                                    logger.info(f"[Location P1] ✅ Got store_id={store_id}, name={store_name} from {endpoint_url}")
                                    break
                        else:
                            logger.warning(f"[Location P1] API {endpoint_url} returned error/status: {data.get('error') if data else 'None'}")
                    except Exception as api_err:
                        logger.debug(f"[Location P1] API {endpoint_url} call failed: {api_err}")

                if not store_id:
                    try:
                        from .dominos_browser import DominosBrowser
                        dom_b = DominosBrowser()
                        res_store = await dom_b.find_nearest_store(lat, lng, db=self.db)
                        if res_store and res_store.get("store_id"):
                            store_id = str(res_store["store_id"])
                            store_name = res_store.get("name") or store_name
                            logger.info(f"[Location P1] â DominosBrowser resolved store_id={store_id}, name={store_name}")
                    except Exception as db_store_err:
                        logger.warning(f"[Location P1] DominosBrowser store resolution fallback failed: {db_store_err}")

                # Inject location data into Domino's localStorage regardless of whether we got store_id
                location_data = {
                    "lat": lat,
                    "lng": lng,
                    "label": order.address or "Delivery Location",
                    "locality": (order.city or (order.address or "").split(",")[-1].strip() or ""),
                    "locationType": "GPS",
                    "isGPS": True,
                    "orderMode": "Delivery",
                }
                ls_inject = {
                    "lastLocationNew": json.dumps(location_data),
                    "userLocationData": json.dumps({"latitude": lat, "longitude": lng}),
                    "isLocationPermissionGiven": "true",
                    "orderMode": "Delivery",
                }
                if store_id:
                    ls_inject["storeId"] = store_id
                    ls_inject["CHILD_STORE_ID_IS"] = store_id

                await page.evaluate("""(lsData) => {
                    for (const [k, v] of Object.entries(lsData)) {
                        try { localStorage.setItem(k, v); } catch(e) {}
                    }
                }""", ls_inject)
                logger.info(f"[Location P1] â Injected location into localStorage (store_id={store_id or 'unknown'})")

            except Exception as p1_err:
                logger.warning(f"[Location P1] localStorage injection failed: {p1_err}")

            # If store search returned 403 or no store resolved, write a robust client-side geolocation mock 
            # to Domino's local state keys to force the PWA to load nearby store context
            if not store_id or store_id == "1234":
                try:
                    logger.info("[Location P1] Store search API was blocked or returned fallback. Writing client-side GPS override state...")
                    client_override_code = f"""() => {{
                        try {{
                            const lat = {lat};
                            const lng = {lng};
                            const locData = {{
                                "lat": lat,
                                "lng": lng,
                                "label": "Delivery Location",
                                "locality": "Delivery Location",
                                "locationType": "GPS",
                                "isGPS": true,
                                "orderMode": "Delivery"
                            }};
                            localStorage.setItem("lastLocationNew", JSON.stringify(locData));
                            localStorage.setItem("userLocationData", JSON.stringify({{ "latitude": lat, "longitude": lng }}));
                            localStorage.setItem("isLocationPermissionGiven", "true");
                            localStorage.setItem("orderMode", "Delivery");
                        }} catch (e) {{}}
                    }}"""
                    await page.evaluate(client_override_code)
                except Exception as override_err:
                    logger.warning(f"[Location P1] Client-side GPS override write failed: {override_err}")

        # ââ Phase 2: Direct store menu URL navigation ââââââââââââââââââââââââââ
        if store_id == '1234':
            store_id = None
        if store_id:
            try:
                logger.info(f"[Location P2] Navigating directly to store menu: store_id={store_id}")
                menu_urls = [
                    f"https://m.dominos.co.in/jfl-discovery-ui/en/pwa/menu-v1/{store_id}",
                    f"https://m.dominos.co.in/jfl-discovery-ui/en/pwa/home",
                    f"https://m.dominos.co.in/menu-v1/{store_id}",
                    f"https://m.dominos.co.in/home/{store_id}",
                ]
                for menu_url in menu_urls:
                    try:
                        await page.goto(menu_url, wait_until="domcontentloaded", timeout=20000)
                        await fast_wait(page, 400, 800)

                        # Dismiss login/welcome splash if shown
                        if "login" in page.url.lower():
                            for close_btn in ['button:has-text("Skip")', 'button:has-text("Close")', 'text="Skip"', '.skip-btn', '[class*="close"]']:
                                try:
                                    if await page.is_visible(close_btn, timeout=600):
                                        await human_click(page, close_btn)
                                        await fast_wait(page, 200, 400)
                                except Exception:
                                    pass

                        if any(p in page.url for p in ["/menu-v", "/home", "/menu/", "jfl-discovery-ui"]):
                            logger.info(f"[Location P2] â Landed on store menu: {page.url}")
                            return f"{store_name} (ID: {store_id})"
                    except Exception as nav_err:
                        logger.debug(f"[Location P2] {menu_url} failed: {nav_err}")
            except Exception as p2_err:
                logger.warning(f"[Location P2] Direct navigation failed: {p2_err}. Falling back to Phase 3.")

        # ââ Phase 3: Navigate homepage with GPS auto-set, then address search ââ
        logger.info("[Location P3] Navigating to Domino's homepage with injected GPSâ¦")
        try:
            await page.goto("https://m.dominos.co.in/jfl-discovery-ui/en/pwa/home", wait_until="domcontentloaded", timeout=25000)
            await fast_wait(page, 500, 1000)

            # Dismiss overlays/skips
            for skip_sel in ['text="Skip"', 'button:has-text("Skip")', '.skip-btn', '[class*="skip"]']:
                if await page.is_visible(skip_sel, timeout=600):
                    if await human_click(page, skip_sel):
                        await fast_wait(page, 150, 300)
                        break

            # Click Change/Set Location if there's an existing store set
            for change_sel in ['text="Change"', '.change-loc-btn', 'text="CHANGE"', 'button:has-text("Change Location")']:
                if await page.is_visible(change_sel, timeout=600):
                    if await human_click(page, change_sel):
                        await fast_wait(page, 300, 600)
                        logger.info("[Location P3] Cleared existing location.")
                        break

            # Click "Use Current Location" / "Locate Me" â GPS override is active
            for locate_sel in [
                'button:has-text("Use Current Location")', 'text="Use Current Location"',
                'text="Locate Me"', 'button:has-text("Locate Me")',
                '.locate-me-btn', 'button:has-text("Use my location")',
                'text="Detect My Location"', 'button:has-text("Detect")',
            ]:
                if await page.is_visible(locate_sel, timeout=800):
                    if await human_click(page, locate_sel):
                        await fast_wait(page, 800, 1500)  # wait for location response
                        logger.info(f"[Location P3] Clicked locate button: '{locate_sel}'")
                        break

            if any(p in page.url for p in ["/home", "/menu-v", "/menu/", "jfl-discovery-ui"]):
                logger.info(f"[Location P3] â GPS auto-set store. URL: {page.url}")
                return f"Store (GPS: {lat}, {lng})"

            # Address text search fallback
            addr = order.address or ""
            parts = [p.strip() for p in addr.split(",") if p.strip()]
            search_query = ""
            for part in reversed(parts):
                clean = re.sub(r'\d{6}', '', part).strip()
                if len(clean) >= 3:
                    search_query = clean
                    break
            if not search_query:
                search_query = order.city or (", ".join(parts[:2]) if len(parts) >= 2 else addr)

            logger.info(f"[Location P3] Trying address search: '{search_query}'")

            search_selectors = [
                'input[placeholder*="delivery address"]', 'input[placeholder*="Locate"]',
                'input[placeholder*="Search"]', 'input[placeholder*="address"]',
                'input[placeholder*="location"]', '.search-input input',
                '#search-input', 'input[type="search"]',
            ]
            for sel in search_selectors:
                if await page.is_visible(sel, timeout=600):
                    await human_type(page, sel, search_query, speed="fast")
                    await fast_wait(page, 400, 800)

                    for sug in ['.suggestion-item', '.address-suggestion', '.locality-list li:first-child',
                                'div.suggestion:first-child', '[role="option"]:first-child']:
                        if await page.is_visible(sug, timeout=800):
                            if await human_click(page, sug):
                                await fast_wait(page, 500, 1000)
                                break
                    break

            # Confirm location if needed
            for confirm_sel in ['button:has-text("Confirm Location")', 'button:has-text("Set Store")',
                                 'button:has-text("Confirm")', 'button:has-text("Save")']:
                if await page.is_visible(confirm_sel, timeout=600):
                    if await human_click(page, confirm_sel):
                        await fast_wait(page, 400, 900)
                        break

            current_url = page.url
            if any(p in current_url for p in ["/home", "/menu-v", "/menu/", "jfl-discovery-ui"]):
                logger.info(f"[Location P3] â Address search succeeded. URL: {current_url}")
                return f"Store (address: {search_query})"

        except Exception as p3_err:
            logger.warning(f"[Location P3] Failed: {p3_err}")

        # Final Fallback Phase 4: Direct PWA discovery home fallback if store_id was injected
        try:
            await page.goto("https://m.dominos.co.in/jfl-discovery-ui/en/pwa/home", wait_until="domcontentloaded", timeout=15000)
            await fast_wait(page, 500, 1000)
            current_url = page.url
            if "login" not in current_url.lower():
                logger.info(f"[Location P4] â Discovery home fallback succeeded. URL: {current_url}")
                return f"Store (PWA home fallback)"
        except Exception as p4_err:
            logger.warning(f"[Location P4] Fallback failed: {p4_err}")

        # All phases exhausted â raise with clear diagnostic
        raise Exception(
            f"â Could not set Domino's delivery location after all phases.\n"
            f"  GPS: ({lat}, {lng}), Address: '{order.address}', City: '{getattr(order, 'city', None)}'\n"
            f"  Current URL: {page.url}\n"
            f"  Hint: Verify coordinates are within a Domino's India delivery zone."
        )



    async def _verify_cart(self, page: Page, order) -> dict:
        """
        Reads the current Domino's cart and cross-checks it against order.items.
        Returns a summary dict with verified_items, cart_total, mismatches.

        This runs AFTER adding all items but BEFORE proceeding to checkout.
        Takes a screenshot and broadcasts the cart state to the admin.
        """
        logger.info("[CartVerify] Reading cart contents from Domino's page...")
        result = {"verified_items": [], "cart_total": 0.0, "mismatches": [], "ok": True}

        try:
            for cart_btn_sel in ['.cart-btn', '.cart-icon', '[class*="cart"]', 'button:has-text("View Cart")']:
                try:
                    if await page.is_visible(cart_btn_sel, timeout=1000):
                        if await human_click(page, cart_btn_sel):
                            await fast_wait(page, 300, 700)
                            break
                except Exception:
                    pass

            # Read cart items via JavaScript
            cart_data = await page.evaluate("""
                () => {
                    const items = [];
                    // Try multiple possible cart item selectors
                    const selectors = ['.cart-item', '.order-item', '[class*="cart-item"]', '[class*="orderItem"]'];
                    let itemEls = [];
                    for (const sel of selectors) {
                        const found = document.querySelectorAll(sel);
                        if (found.length > 0) { itemEls = found; break; }
                    }
                    for (const el of itemEls) {
                        const nameEl = el.querySelector('.item-name, .product-name, [class*="name"], [class*="title"]');
                        const qtyEl  = el.querySelector('.quantity, .qty, [class*="qty"], [class*="count"]');
                        const priceEl= el.querySelector('.price, .item-price, [class*="price"]');
                        items.push({
                            name:  nameEl  ? nameEl.textContent.trim()  : '',
                            qty:   qtyEl   ? qtyEl.textContent.trim()   : '1',
                            price: priceEl ? priceEl.textContent.trim() : '0',
                        });
                    }

                    // Try to read cart total
                    const totalEl = document.querySelector('.cart-total, .total-price, [class*="total"], .order-total');
                    const cartTotal = totalEl ? totalEl.textContent.trim() : '';

                    return { items, cartTotal };
                }
            """)

            cart_items = cart_data.get("items", [])
            cart_total_text = cart_data.get("cartTotal", "")

            # Parse total
            total_match = re.search(r'[\d,]+\.?\d*', cart_total_text.replace(',', ''))
            if total_match:
                result["cart_total"] = float(total_match.group(0))

            result["verified_items"] = cart_items

            # Cross-check against order items
            expected_names = {map_to_dominos_name(item.product.name).lower() for item in (order.items or []) if item.product}
            found_names = {ci["name"].lower() for ci in cart_items if ci["name"]}

            for expected in expected_names:
                if not any(expected in found or found in expected for found in found_names):
                    result["mismatches"].append(f"Expected '{expected}' not found in Domino's cart")
                    result["ok"] = False

            # Log result
            if result["mismatches"]:
                logger.warning(f"[CartVerify] Cart mismatches: {result['mismatches']}")
            else:
                logger.info(f"[CartVerify] Ã¢Åâ¦ Cart verified: {len(cart_items)} items, total Ã¢âÂ¹{result['cart_total']}")

        except Exception as e:
            logger.warning(f"[CartVerify] Could not verify cart (non-fatal): {e}")
            result["ok"] = False  # mark as unverified but don't raise

        return result

    async def _fill_address(self, page: Page, order) -> None:
        """Fill address fields on the Domino's checkout page â all fields concurrently."""
        # 1. Ensure address form is open
        try:
            addr_visible = (
                await page.is_visible('input[name="address"]', timeout=800) or
                await page.is_visible('input[name="addressLine1"]', timeout=800)
            )
            if not addr_visible:
                add_addr_selectors = [
                    'text="Add New Address"', 'text="Add Address"', 'text="+ Add Address"',
                    'button:has-text("Add Address")', 'button:has-text("Add New Address")',
                    '.add-address-btn', '.btn--add-address', 'button:has-text("ADD NEW ADDRESS")'
                ]
                for sel in add_addr_selectors:
                    if await human_click(page, sel, timeout=1200):
                        await fast_wait(page, 200, 500)
                        break
        except Exception as e:
            logger.warning(f"Failed to open Add Address form: {e}")

        # 2. Validate address
        if not order.address or not order.address.strip():
            raise Exception("Order address is empty â cannot proceed with delivery.")

        # 3. Extract PIN code
        pin_code = "400070"
        pin_match = re.search(r'\b\d{6}\b', order.address)
        if pin_match:
            pin_code = pin_match.group(0)
            logger.info(f"Extracted PIN {pin_code} from address")

        # 4. Resolve name and phone
        name_val = "Customer"
        if order.user:
            name_val = order.user.display_name or order.user.username or "Customer"

        phone_val = order.phone or "9999999999"
        phone_digits = ''.join(c for c in phone_val if c.isdigit())[-10:] or "9999999999"

        # 5. Build (selector, value) pairs â detect which selectors are present
        addr_sel  = 'input[name="address"]' if await page.is_visible('input[name="address"]', timeout=600) else 'input[name="addressLine1"]'
        phone_sel = 'input[name="phone"]'   if await page.is_visible('input[name="phone"]',   timeout=600) else 'input[name="mobile"]'
        name_sel_found = None
        for ns in ['input[name="name"]', 'input[placeholder*="Name"]', 'input[placeholder*="name"]']:
            if await page.is_visible(ns, timeout=400):
                name_sel_found = ns
                break

        # 6. Fill all visible fields CONCURRENTLY
        fields = [(addr_sel, order.address)]
        if await page.is_visible('input[name="pin"]', timeout=400):
            fields.append(('input[name="pin"]', pin_code))
        if await page.is_visible(phone_sel, timeout=400):
            fields.append((phone_sel, phone_digits))
        if name_sel_found:
            fields.append((name_sel_found, name_val))
        if hasattr(order, 'landmark') and order.landmark:
            if await page.is_visible('input[name="landmark"]', timeout=400):
                fields.append(('input[name="landmark"]', order.landmark))

        fill_results = await fill_form_fields(page, fields)
        logger.info(f"[AddressFill] Concurrent fill results: {fill_results}")

        # 7. Random micro-action between form and submit (looks human)
        await random_micro_action(page)
        await fast_wait(page, 100, 250)

        # 8. Click Save / Continue
        save_sel = await wait_for_any(page, [
            'button:has-text("Continue")', 'button:has-text("Save")',
            'button[type="submit"]', 'button:has-text("SAVE")',
        ], timeout=2000)
        if save_sel:
            await human_click(page, save_sel, timeout=2000)
            await fast_wait(page, 300, 700)

        # 9. Verify â no validation errors remaining
        try:
            if await page.is_visible('input[name="address"]', timeout=500) or \
               await page.is_visible('input[name="addressLine1"]', timeout=500):
                err_text = await page.evaluate("""() => {
                    const el = Array.from(document.querySelectorAll('div, span, p')).find(e => {
                        const t = e.textContent || '';
                        return t.toLowerCase().includes('required') ||
                               t.toLowerCase().includes('invalid') ||
                               t.toLowerCase().includes('error') ||
                               t.toLowerCase().includes('must be') ||
                               t.toLowerCase().includes('pin code');
                    });
                    return el ? el.textContent.trim() : null;
                }""")
                if err_text:
                    raise Exception(f"Address validation error on site: {err_text}")
                else:
                    raise Exception("Address form still open â possibly invalid pin/missing fields.")
        except Exception as err:
            logger.warning(f"[Address submission check] {err}")



    async def _apply_coupon_code(self, page: Page, order) -> None:
        """Fills and applies coupon code on the checkout page using fast human actions."""
        coupon = getattr(order, 'coupon_applied', None)
        if not coupon:
            try:
                from ...database import Order
                orders_count = self.db.query(Order).filter(
                    Order.user_id == order.user_id,
                    Order.status == "Completed"
                ).count()
                coupon = "NEWBIE100" if orders_count == 0 else "WELCOME90"
            except Exception:
                coupon = "NEWBIE100"

        logger.info(f"Attempting to apply coupon code: {coupon}")
        try:
            coupon_input_selectors = [
                'input[placeholder*="Coupon"]',
                'input[placeholder*="Promo"]',
                '.coupon-input input',
                '#coupon-code-input'
            ]
            coupon_input = None
            for selector in coupon_input_selectors:
                if await page.is_visible(selector, timeout=800):
                    coupon_input = selector
                    break

            if not coupon_input:
                apply_btn_selectors = [
                    'text="Apply Coupon"',
                    'button:has-text("Apply Coupon")',
                    '.apply-coupon-btn'
                ]
                for selector in apply_btn_selectors:
                    if await page.is_visible(selector, timeout=600):
                        if await human_click(page, selector):
                            await fast_wait(page, 150, 300)
                            break
                
                # Recheck
                for selector in coupon_input_selectors:
                    if await page.is_visible(selector, timeout=800):
                        coupon_input = selector
                        break

            if coupon_input:
                await human_type(page, coupon_input, coupon, speed="fast")
                await fast_wait(page, 100, 200)
                apply_btn = 'button:has-text("Apply"), button:has-text("APPLY"), .apply-btn'
                await human_click(page, apply_btn)
                await fast_wait(page, 500, 1000)
                logger.info(f"Applied coupon '{coupon}' successfully")
            else:
                logger.warning("Coupon input field not found on checkout page")
        except Exception as e:
            logger.warning(f"Could not apply coupon: {e}")

    async def _apply_gift_card(self, page: Page, order) -> None:
        """If the order has an associated gift card, fill code and pin in the checkout inputs."""
        if order.gift_card:
            from ..utils import decrypt_data
            from ..database import AuditLog
            import json
            try:
                card_code = decrypt_data(order.gift_card.code_encrypted)
                card_pin = decrypt_data(order.gift_card.pin_encrypted)
                
                logger.info(f"[GIFT CARD FLOW] Starting gift card application for code length {len(card_code)}")
                
                option_selectors = [
                    'text="Gift Card / E-Voucher"',
                    'text="Gift Cards / E-Vouchers"',
                    'text="Gift Card"',
                    'text="E-Voucher"',
                    '[data-testid="giftCard"]',
                    '.payment-option:has-text("Gift Card")',
                    '.payment-option:has-text("Voucher")'
                ]
                
                option_clicked = False
                for sel in option_selectors:
                    if await page.is_visible(sel, timeout=800):
                        if await human_click(page, sel):
                            option_clicked = True
                            await fast_wait(page, 200, 400)
                            break
                        
                if not option_clicked:
                    logger.warning("Could not find explicit Gift Card option. Proceeding directly.")
                
                # 2. Enter the 16-digit code
                code_selectors = [
                    'input[placeholder*="Card Number"]',
                    'input[placeholder*="Voucher"]',
                    'input[placeholder*="Gift Card"]',
                    'input[type="tel"]',
                    'input[type="number"]',
                    '#voucher_code'
                ]
                
                code_entered = False
                for sel in code_selectors:
                    if await page.is_visible(sel, timeout=800):
                        await human_type(page, sel, card_code, speed="fast")
                        code_entered = True
                        break
                        
                if not code_entered:
                    raise Exception("Could not find the 16-digit Gift Card code input field.")
                    
                await fast_wait(page, 200, 400)
                
                # 3. Click proceed/pay
                proceed_selectors = [
                    'button:has-text("Pay")',
                    'button:has-text("Submit")',
                    'button:has-text("Proceed")',
                    'button:has-text("Apply")',
                    'button:has-text("Next")',
                    '.btn-proceed',
                    'button.btn--red'
                ]
                
                proceed_clicked = False
                for sel in proceed_selectors:
                    if await page.is_visible(sel, timeout=800):
                        if await human_click(page, sel):
                            proceed_clicked = True
                            break
                        
                if not proceed_clicked:
                    logger.warning("Could not find proceed button. Trying to fill PIN directly.")
                    
                await fast_wait(page, 500, 1000)
                
                # 4. Enter the 6-digit PIN
                pin_selectors = [
                    'input[placeholder*="PIN"]',
                    'input[placeholder*="pin"]',
                    'input[name="pin"]',
                    'input[type="password"]',
                    '#voucher_pin'
                ]
                
                pin_entered = False
                for sel in pin_selectors:
                    try:
                        await page.wait_for_selector(sel, state="visible", timeout=3000)
                        await human_type(page, sel, card_pin, speed="fast")
                        pin_entered = True
                        break
                    except Exception:
                        continue
                        
                if not pin_entered:
                    raise Exception("Could not find the 6-digit PIN input field.")
                    
                await fast_wait(page, 200, 400)
                
                # 5. Click final confirmation
                confirm_selectors = [
                    'button:has-text("Apply")',
                    'button:has-text("Submit")',
                    'button:has-text("Pay")',
                    'button:has-text("Confirm")',
                    '.btn-confirm'
                ]
                
                confirm_clicked = False
                for sel in confirm_selectors:
                    if await page.is_visible(sel, timeout=800):
                        if await human_click(page, sel):
                            confirm_clicked = True
                            break
                
                await fast_wait(page, 500, 1000)
                
                # 6. Check if application failed
                error_selectors = [
                    '.voucher-error',
                    '.error-msg',
                    'text="Invalid"',
                    'text="Expired"',
                    'text="Voucher could not be applied"',
                    'text="insufficient"'
                ]
                for err_sel in error_selectors:
                    if await page.is_visible(err_sel, timeout=400):
                        err_txt = await page.inner_text(err_sel)
                        raise Exception(f"Domino's Rejected Gift Card: {err_txt}")
                
                logger.info("Applied gift card voucher and pin successfully")
            except Exception as e:
                err_msg = f"Gift card application failed for order {order.id}: {e}"
                logger.warning(err_msg)
                
                audit = AuditLog(
                    admin_id=1,
                    admin_username="system_bot",
                    action="GIFT_CARD_FAILED",
                    details=json.dumps({"order_id": order.id, "error": str(e)})
                )
                self.db.add(audit)
                self.db.commit()
                raise Exception(err_msg)
        else:
            logger.info("Gift-card step skipped â no gift card associated with order")

    async def _finalize_order(self, page: Page) -> None:
        """Select Cash on Delivery payment option and place the order."""
        logger.info("Selecting Cash on Delivery payment method...")
        try:
            cod_selectors = [
                'text="Cash on Delivery"',
                'text="COD"',
                'input[value="COD"]',
                '//span[contains(text(), "Cash on Delivery")]',
                '//div[contains(text(), "Cash on Delivery")]'
            ]
            cod_clicked = False
            for selector in cod_selectors:
                if await page.is_visible(selector, timeout=800):
                    if await human_click(page, selector):
                        cod_clicked = True
                        logger.info(f"Clicked COD option: {selector}")
                        await fast_wait(page, 200, 450)
                        break
            
            if not cod_clicked:
                logger.warning("Could not find or click Cash on Delivery selector - defaulting to pre-selected option")
        except Exception as e:
            logger.warning(f"Payment selection error: {e}")

        # 2. Click Place Order
        try:
            place_order_selectors = [
                'button:has-text("Place Order")',
                'button:has-text("PLACE ORDER")',
                'button[type="submit"]',
                '.place-order-btn'
            ]
            order_placed = False
            for selector in place_order_selectors:
                if await page.is_visible(selector, timeout=800):
                    if await human_click(page, selector):
                        order_placed = True
                        logger.info(f"Clicked Place Order button: {selector}")
                        break
            if not order_placed:
                raise Exception("Place Order button not found or clickable")
        except Exception as e:
            logger.warning(f"Failed to click Place Order: {e}")
        try:
            await page.wait_for_selector('.order-confirmation, .order-success, .success-title', timeout=15000)
            logger.info("Order placed successfully on Domino's site")
        except Exception as e:
            logger.warning(f"Order confirmation element not found: {e}. Order might still have gone through.")

    async def place_order(self, order) -> Dict:
        """Public method called from status transitions.
        Serializes concurrent orders using a global Semaphore to prevent browser crashes.
        """
        async with ORDER_SEMAPHORE:
            return await self._place_order_internal(order)

    async def _place_order_internal(self, order) -> Dict:
        """
        Full browser automation to place an order on Domino's India.
        Flow:
          0. Pre-flight validation
          1. Launch browser with saved session cookies
          2. Set delivery location (3-phase: API Ã¢â â GPS Ã¢â â address search)
          3. Add items to cart
          4. Verify cart contents match order
          5. Open checkout
          6. Fill address & phone
          7. Apply gift card (if any)
          8. Solve CAPTCHA (if any)
          9. Place order (COD)
          10. Capture reference and auto-save cookies
        """
        # Ã¢ââ¬Ã¢ââ¬ Build dynamic step list for this specific order Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
        steps = build_order_steps(order)
        total_steps = len(steps)
        step_idx = 0

        selected_session = None
        context = None
        page = None

        def get_step(name: str):
            """Find step index and tuple by name."""
            for i, (lbl, msg) in enumerate(steps):
                if name in lbl:
                    return i, lbl, msg
            return 0, name, name

        try:
            # Ã¢ââ¬Ã¢ââ¬ Step 0: Pre-flight Validation Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            step_idx, label, msg = get_step("Pre-flight")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)

            from .order_validator import validate_order_for_robot, format_validation_report, OrderValidationError
            validation = await validate_order_for_robot(order, self.db)

            # Broadcast the validation report (errors + warnings)
            report = format_validation_report(validation, order.id)
            await self._broadcast(order, label, report, step_idx, total_steps)

            if not validation.ok:
                raise OrderValidationError(validation.errors, validation.warnings)

            # Log warnings even if validation passed
            for w in validation.warnings:
                logger.warning(f"[PreFlight] {w}")

            # Ã¢ââ¬Ã¢ââ¬ Step 1: Resolve proxy and session Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            proxy_config = None
            if self.proxy_manager:
                try:
                    proxy_config = self.proxy_manager.get_proxy()
                except Exception as proxy_err:
                    logger.warning(f"Could not load proxy: {proxy_err}. Proceeding without proxy.")

            from .dominos_session_manager import validate_and_get_session
            selected_session = None
            
            # Check if order has a specific requested phone number, validate that session first
            if order.phone:
                selected_session = await validate_and_get_session(self.db, order.phone)
                if selected_session and selected_session.id in BUSY_SESSION_IDS:
                    # Session exists but is currently busy; wait for it
                    for attempt in range(15):
                        if selected_session.id not in BUSY_SESSION_IDS:
                            break
                        logger.info(f"[SESSION LOCK] Matching session +91{order.phone} is busy. Waiting 2s...")
                        await asyncio.sleep(2)
                
                if selected_session and selected_session.id not in BUSY_SESSION_IDS:
                    BUSY_SESSION_IDS.add(selected_session.id)
                    logger.info(f"[SESSION LOCK] Selected matching valid session +91{selected_session.mobile_number}")

            # If no valid session for this mobile number, attempt auto-assign fallback if enabled
            if not selected_session:
                from ..settings import settings
                if getattr(settings, "AUTO_ASSIGN_SESSIONS", True):
                    # Query any other active, valid session from the database (not currently busy)
                    from ..database import DominosSession
                    fallback_session = self.db.query(DominosSession).filter(
                        DominosSession.is_active == True,
                        DominosSession.verify_status == "valid",
                        ~DominosSession.id.in_(list(BUSY_SESSION_IDS)) if BUSY_SESSION_IDS else True
                    ).order_by(DominosSession.created_at.desc()).first()
                    
                    if fallback_session:
                        selected_session = fallback_session
                        BUSY_SESSION_IDS.add(selected_session.id)
                        logger.info(
                            f"[AUTO-ASSIGN] No matching session for +91{order.phone}. "
                            f"Assigned active session +91{selected_session.mobile_number} (ID: {selected_session.id}) to order {order.id}."
                        )

            # If no valid session for this mobile number and auto-assign failed, automatically trigger a fresh request
            if not selected_session:
                logger.info(f"No valid session found for +91{order.phone} and auto-assign was unsuccessful. Triggering a fresh login flow.")
                from .dominos_session_manager import request_otp
                from ..database import User
                admin = self.db.query(User).filter(User.role == 'admin').first()
                if admin and order.phone:
                    try:
                        await request_otp(self.db, admin, order.phone, manual_mode=False)
                    except Exception as req_err:
                        logger.error(f"Failed to auto-create session for +91{order.phone}: {req_err}")
                raise Exception(f"No valid Domino's session found for +91{order.phone}. A fresh login/OTP request has been automatically triggered. Please verify the OTP in the Admin panel.")

            selected_session_id = selected_session.id
            selected_session_mobile = selected_session.mobile_number

            # Add system trace note recording the assigned session for admin traceability
            try:
                from ..database import OrderNote
                trace_note = OrderNote(
                    order_id=order.id,
                    note=f"System: Assigned Domino's session +91{selected_session_mobile} (ID: {selected_session_id}) to place this order.",
                    admin_username="system_bot"
                )
                self.db.add(trace_note)
                self.db.commit()
            except Exception as note_err:
                logger.warning(f"Could not create order assignment trace note: {note_err}")

            # ââ Step 1: Launch Browser ââââââââââââââââââââââââââââââââââââââââââ
            step_idx, label, msg = get_step("Launching Browser")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)

            # Extract validated coordinates
            lat = float(order.latitude) if order.latitude else None
            lng = float(order.longitude) if order.longitude else None

            # Fallback: geocode if no coords
            if lat is None or lng is None:
                try:
                    from ..routes import geocode_address
                    lat, lng = await geocode_address(order.address or "Mumbai")
                    logger.info(f"[Browser] Geocoded address to: lat={lat}, lng={lng}")
                except Exception as geo_err:
                    logger.warning(f"[Browser] Geocoding failed: {geo_err}. Will use address text only.")

            from .browser_pool import browser_pool

            geo_args = {}
            if lat and lng:
                geo_args = {"geolocation": {"latitude": lat, "longitude": lng}, "permissions": ["geolocation"]}

            context_args = {
                "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                "locale": "en-IN",
                "timezone_id": "Asia/Kolkata",
                "viewport": {"width": 375, "height": 812},
                "device_scale_factor": 3,
                "is_mobile": True,
                "has_touch": True,
                "java_script_enabled": True,
                "ignore_https_errors": True,
                **geo_args
            }
            if proxy_config:
                context_args["proxy"] = proxy_config
            try:
                context = await browser_pool.create_context(**context_args)
            except Exception as ctx_err:
                if "proxy" in context_args:
                    logger.warning(f"[Browser] Context creation with proxy failed ({ctx_err}). Retrying without proxy...")
                    context_args.pop("proxy", None)
                    context = await browser_pool.create_context(**context_args)
                else:
                    raise ctx_err

            if lat and lng:
                try:
                    await context.grant_permissions(["geolocation"], origin="https://m.dominos.co.in")
                    logger.info("[Browser] Geolocation permissions granted for https://m.dominos.co.in")
                except Exception as perm_err:
                    logger.warning(f"[Browser] Failed to grant geolocation permissions: {perm_err}")

            page = await context.new_page()
            # ââ Verify proxy connectivity before placing order ââ
            if "proxy" in context_args:
                try:
                    logger.info("[Browser] Testing proxy connectivity to Domino's website...")
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
                    logger.warning(f"[Browser] Proxy connectivity check failed: {conn_err}. Recreating context without proxy...")
                    try:
                        await context.close()
                    except Exception:
                        pass
                    context_args.pop("proxy", None)
                    context = await browser_pool.create_context(**context_args)
                    page = await context.new_page()

            # ââ Inject human/stealth signals into every page in this context ââ
            await inject_human_signals(page)
            
            # Re-add init script via context (applies to all pages)
            try:
                await context.add_init_script("""
                    (() => {
                        Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
                        Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5].map(i=>({name:`Plugin ${i}`,filename:`p${i}.dll`,description:`P${i}`,length:1}))});
                        Object.defineProperty(navigator,'languages',{get:()=>['en-IN','en-US','en','hi']});
                        Object.defineProperty(navigator,'maxTouchPoints',{get:()=>5});
                        window.ontouchstart=null;
                        window._mx=Math.random()*350+80; window._my=Math.random()*250+80;
                        document.addEventListener('mousemove',e=>{window._mx=e.clientX;window._my=e.clientY},{passive:true});
                        if(!window.chrome){window.chrome={runtime:{}};}
                    })();
                """)
            except Exception:
                pass

            # Pre-populate localStorage via init script to prevent React SPA race conditions
            if selected_session.local_storage:
                try:
                    ls_data = selected_session.local_storage if isinstance(selected_session.local_storage, dict) else {}
                    if ls_data:
                        import json as _json
                        ls_json = _json.dumps(ls_data)
                        await context.add_init_script(f"""
                            try {{
                                const lsData = {ls_json};
                                for (const [k, v] of Object.entries(lsData)) {{
                                    localStorage.setItem(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
                                }}
                            }} catch(e) {{}}
                        """)
                        logger.info(f"[Browser] Pre-injected {len(ls_data)} localStorage keys via init script")
                except Exception as ls_init_err:
                    logger.warning(f"[Browser] Failed to add localStorage init script: {ls_init_err}")

            # Load saved session cookies into context
            if selected_session.cookies:
                try:
                    from .dominos_session_manager import sanitize_cookies
                    clean_cookies = sanitize_cookies(selected_session.cookies)
                    await context.add_cookies(clean_cookies)
                    logger.info(f"[Browser] Loaded {len(clean_cookies)} cookies for +91{selected_session_mobile}")
                except Exception as cookie_err:
                    logger.warning(f"[Browser] Failed to load session cookies: {cookie_err}. Proceeding with blank session.")

            # Block unnecessary resources to reduce page load time
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

            # Navigate to the main site to initialize the session and cookies on the correct domain origin
            try:
                await page.goto("https://m.dominos.co.in", wait_until="domcontentloaded", timeout=20000)
                logger.info("[Browser] Initial page load to m.dominos.co.in complete")
            except Exception as load_err:
                logger.warning(f"[Browser] Initial page load failed (non-fatal): {load_err}")


            # Ã¢ââ¬Ã¢ââ¬ Step 2: Set Delivery Location Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            step_idx, label, msg = get_step("Setting Location")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)

            store_name = await self._set_dominos_location(page, order, lat, lng)
            await self._broadcast(order, label, f"Ã°Å¸âÂ Store set: {store_name}", step_idx, total_steps)

            # Ã¢ââ¬Ã¢ââ¬ Step 3: Add items to cart Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            step_idx, label, msg = get_step("Adding Items")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)

            # Navigate to menu if needed
            if "/menu-v" not in page.url:
                menu_btn_selectors = ['text="Menu"', '//span[text()="Menu"]', '//div[text()="Menu"]', '.menu-btn']
                clicked_menu = False
                for sel in menu_btn_selectors:
                    if await page.is_visible(sel, timeout=1000):
                        if await human_click(page, sel):
                            clicked_menu = True
                            await fast_wait(page, 400, 800)
                            break
                if not clicked_menu:
                    store_id_match = re.search(r'/home/(\d+)', page.url)
                    if store_id_match:
                        await page.goto(
                            f"https://m.dominos.co.in/jfl-discovery-ui/en/pwa/menu-v1/{store_id_match.group(1)}",
                            wait_until="domcontentloaded",
                            timeout=20000
                        )
                        await fast_wait(page, 500, 1000)

            items_added = 0
            for item in order.items:
                product_name = item.product.name if item.product else "Margherita Classic"
                dominos_name = map_to_dominos_name(product_name)
                qty = item.quantity or 1
                logger.info(f"[Cart] Adding {qty}x '{dominos_name}'")

                # Switch to correct category tab on Domino's UI
                category = item.product.category if item.product else "Veg"
                await self._switch_category(page, category)
                await fast_wait(page, 300, 600)

                # Search and locate product details
                prod_details = await page.evaluate("""
                    (args) => {
                        let { dominosName } = args;
                        let cards = document.querySelectorAll('.card-item, .product-card, .menu-item, [class*="product"], [class*="prod"]');
                        let targetCard = null;
                        for (let card of cards) {
                            let titleEl = card.querySelector('.pizza-title, .title, [class*="title"], [class*="name"]');
                            if (titleEl && titleEl.innerText.trim().toLowerCase().includes(dominosName.toLowerCase())) {
                                targetCard = card; break;
                            }
                        }
                        if (!targetCard) {
                            for (let el of document.querySelectorAll('span, div, h3, p')) {
                                if (el.textContent.trim().toLowerCase().includes(dominosName.toLowerCase())) {
                                    let parent = el.closest('.card-item, .product-card, .menu-item, [class*="product"], [class*="card"]');
                                    if (parent) { targetCard = parent; break; }
                                }
                            }
                        }
                        if (!targetCard) return null;
                        
                        let priceEl = targetCard.querySelector('.price, .item-price, [class*="price"]');
                        let priceText = priceEl ? priceEl.textContent.trim() : "0";
                        
                        return {
                            found: true,
                            priceText: priceText,
                            cardIndex: Array.from(cards).indexOf(targetCard)
                        };
                    }
                """, {"dominosName": dominos_name})

                # If not found directly on current tab, fallback to search bar
                if not prod_details or not prod_details.get("found"):
                    for s_btn in ['.search-icon', '[alt*="search"]', '.ico-search', '[class*="search"]', 'button.search']:
                        if await page.is_visible(s_btn, timeout=600):
                            if await human_click(page, s_btn):
                                await fast_wait(page, 150, 350)
                                break

                    for s_in in ['input[placeholder*="Search"]', 'input[placeholder*="pizza"]', 'input[type="search"]', 'input[type="text"]']:
                        if await page.is_visible(s_in, timeout=600):
                            await human_type(page, s_in, dominos_name, speed="normal")
                            await fast_wait(page, 300, 600)
                            break
                            
                    # Re-evaluate details after search
                    prod_details = await page.evaluate("""
                        (args) => {
                            let { dominosName } = args;
                            let cards = document.querySelectorAll('.card-item, .product-card, .menu-item, [class*="product"], [class*="prod"]');
                            let targetCard = null;
                            for (let card of cards) {
                                let titleEl = card.querySelector('.pizza-title, .title, [class*="title"], [class*="name"]');
                                if (titleEl && titleEl.innerText.trim().toLowerCase().includes(dominosName.toLowerCase())) {
                                    targetCard = card; break;
                                }
                            }
                            if (!targetCard) {
                                for (let el of document.querySelectorAll('span, div, h3, p')) {
                                    if (el.textContent.trim().toLowerCase().includes(dominosName.toLowerCase())) {
                                        let parent = el.closest('.card-item, .product-card, .menu-item, [class*="product"], [class*="card"]');
                                        if (parent) { targetCard = parent; break; }
                                    }
                                }
                            }
                            if (!targetCard) return null;
                            
                            let priceEl = targetCard.querySelector('.price, .item-price, [class*="price"]');
                            let priceText = priceEl ? priceEl.textContent.trim() : "0";
                            
                            return {
                                found: true,
                                priceText: priceText,
                                cardIndex: Array.from(cards).indexOf(targetCard)
                            };
                        }
                    """, {"dominosName": dominos_name})

                success = False
                if prod_details and prod_details.get("found"):
                    # Check and match pricing in real-time
                    try:
                        price_str = "".join([c for c in prod_details.get("priceText", "0") if c.isdigit() or c == "."])
                        real_price = float(price_str) if price_str else 0.0
                        if real_price > 0 and item.product:
                            db_price = item.product.discounted_price if item.product.discounted_price is not None else item.product.original_price
                            if abs(real_price - db_price) > 1.0:
                                logger.info(f"[Price Match] Pricing mismatch for '{product_name}' at location. DB: â‚¹{db_price}, Domino's: â‚¹{real_price}. Syncing database.")
                                if item.product.discounted_price is not None:
                                    item.product.discounted_price = real_price
                                else:
                                    item.product.original_price = real_price
                                self.db.commit()
                    except Exception as price_err:
                        logger.warning(f"[Price Match] Mismatch parsing price: {price_err}")
                        
                    # Click Add button
                    card_idx = prod_details.get("cardIndex", 0)
                    success = await page.evaluate("""
                        (args) => {
                            let { idx } = args;
                            let cards = document.querySelectorAll('.card-item, .product-card, .menu-item, [class*="product"], [class*="prod"]');
                            if (idx < 0 || idx >= cards.length) return false;
                             let card = cards[idx];
                             let addBtn = card.querySelector('button.cta-add, button.add-to-cart, [class*="add-btn"], button');
                             if (!addBtn) {
                                 for (let el of card.querySelectorAll('button, div, span, a')) {
                                     if (el.textContent.trim().toUpperCase() === 'ADD') {
                                         addBtn = el;
                                         break;
                                     }
                                 }
                             }
                             if (addBtn && !addBtn.disabled) { addBtn.click(); return true; }
                             return false;
                        }
                    """, {"idx": card_idx})

                if success:
                    items_added += 1
                    await fast_wait(page, 400, 800)
                    # Confirm size/customize dialog if shown
                    for confirm_sel in ['button:has-text("Add")', 'button:has-text("Confirm")', '.btn-customize-add', 'button:has-text("ADD")']:
                        if await page.is_visible(confirm_sel, timeout=800):
                            if await human_click(page, confirm_sel):
                                await fast_wait(page, 200, 450)
                                break
                    # Add more if qty > 1
                    for _ in range(qty - 1):
                        for plus_sel in ['button:has-text("+")', '.ico-plus', '.plus-btn', '[class*="plus"]']:
                            if await page.is_visible(plus_sel, timeout=500):
                                if await human_click(page, plus_sel):
                                    await fast_wait(page, 200, 400)
                                    break
                else:
                    logger.warning(f"[Cart] Could not find or click '{dominos_name}' on menu. Item may be unavailable in this area.")

                await random_micro_action(page)
                await fast_wait(page, 150, 300)

            await self._broadcast(order, label, f"Ã°Å¸âºâ Added {items_added}/{len(order.items)} items to cart", step_idx, total_steps)

            # Ã¢ââ¬Ã¢ââ¬ Step 4: Verify Cart Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            step_idx, label, msg = get_step("Verifying Cart")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)

            cart_result = await self._verify_cart(page, order)
            if cart_result["ok"]:
                await self._broadcast(
                    order, label,
                    f"Ã¢Åâ¦ Cart verified: {len(cart_result['verified_items'])} items, Total Ã¢âÂ¹{cart_result['cart_total']}",
                    step_idx, total_steps
                )
            else:
                mismatches = "; ".join(cart_result["mismatches"][:3])
                await self._broadcast(
                    order, label,
                    f"Ã¢Å¡Â Ã¯Â¸Â Cart has mismatches (continuing anyway): {mismatches}",
                    step_idx, total_steps
                )

            # Ã¢ââ¬Ã¢ââ¬ Step 5: Open Checkout Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            step_idx, label, msg = get_step("Opening Checkout")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)

            try:
                await page.goto("https://m.dominos.co.in/postorder-ui/checkout", wait_until="domcontentloaded", timeout=25000)
                await fast_wait(page, 400, 900)
            except Exception:
                for cart_btn in ['button:has-text("View Cart")', '.cart-btn', '.cart-icon']:
                    if await page.is_visible(cart_btn, timeout=1200):
                        if await human_click(page, cart_btn):
                            await fast_wait(page, 400, 900)
                            break

            # Ã¢ââ¬Ã¢ââ¬ Step 6: Fill Address Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            step_idx, label, msg = get_step("Filling Address")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)
            await self._fill_address(page, order)

            # Apply coupon code
            await self._apply_coupon_code(page, order)

            # Ã¢ââ¬Ã¢ââ¬ Step 7 (optional): Apply Gift Card Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            if order.gift_card:
                step_idx, label, msg = get_step("Applying Gift Card")
                await self._broadcast(order, label, msg, step_idx, total_steps)
                await self._update_db_status(order, label)
                await self._apply_gift_card(page, order)

            # Ã¢ââ¬Ã¢ââ¬ Step 8: Solve CAPTCHA Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬Ã¢ââ¬
            # ── Step 8: Solve CAPTCHA ──────────────────────────────────────────
            step_idx, label, msg = get_step("Solving CAPTCHA")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)
            await self._solve_captcha(page)

            # ── Step 9: Place Order ──────────────────────────────────────────
            step_idx, label, msg = get_step("Finalizing Order")
            await self._broadcast(order, label, msg, step_idx, total_steps)
            await self._update_db_status(order, label)
            await self._finalize_order(page)

            # Extract order reference from confirmation page & capture screenshot
            dominos_ref = None
            try:
                dominos_ref = await page.evaluate("""
                    () => {
                        const el = document.querySelector('.order-id, .orderReference, [class*="orderId"], [class*="order-num"]');
                        return el ? el.textContent.trim() : null;
                    }
                """)
            except Exception:
                pass

            try:
                # Capture proof of purchase screenshot
                import os
                screenshot_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "frontend", "static", "screenshots")
                os.makedirs(screenshot_dir, exist_ok=True)
                screenshot_path = os.path.join(screenshot_dir, f"order_{order.id}.png")
                try:
                    await page.wait_for_load_state("networkidle", timeout=3000)
                except Exception:
                    pass
                await page.screenshot(path=screenshot_path, full_page=False)
                logger.info(f"[OrderSyncer] Saved order confirmation screenshot to {screenshot_path}")
            except Exception as ss_err:
                logger.error(f"[OrderSyncer] Failed to capture order confirmation screenshot: {ss_err}")

            # ── Step 10: Completed ──────────────────────────────────────────
            step_idx, label, msg = get_step("Completed")
            await self._broadcast(order, label, msg, step_idx, total_steps)

            return {
                "success": True,
                "message": "Order placed on Domino's",
                "dominos_reference": dominos_ref,
                "store_name": store_name,
                "session_used": selected_session_mobile,
            }

        except Exception as exc:
            logger.exception(f"[OrderSyncer] Failed to place order #{order.id}: {exc}")
            try:
                from .. import routes
                if routes.sse_broadcast_callback:
                    await routes.sse_broadcast_callback({
                        "type": "dominos_progress",
                        "order_id": order.id,
                        "step_label": "Domino's: Failed",
                        "step_message": f"❌ Error: {str(exc)[:300]}",
                        "step_index": -1,
                        "total_steps": total_steps,
                        "progress_pct": 0,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "is_error": True,
                    })
            except Exception:
                pass
            return {"success": False, "message": str(exc)}

        finally:
            # ── Always auto-save refreshed cookies before closing ──────────
            if context and selected_session_id:
                try:
                    fresh_cookies = await context.cookies()
                    ls_str = None
                    if page:
                        try:
                            ls_str = await page.evaluate("() => JSON.stringify(localStorage)")
                        except Exception:
                            pass

                    if fresh_cookies:
                        from ..database import SessionLocal, DominosSession
                        from .dominos_session_manager import sanitize_cookies
                        import json as _json
                        db_session = SessionLocal()
                        try:
                            sess = db_session.query(DominosSession).filter(DominosSession.id == selected_session_id).first()
                            if sess:
                                sess.cookies = sanitize_cookies(fresh_cookies)
                                if ls_str:
                                    try:
                                        sess.local_storage = _json.loads(ls_str)
                                    except Exception:
                                        pass
                                sess.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                                db_session.commit()
                                logger.info(f"[AutoSave] ✅ Saved {len(fresh_cookies)} refreshed cookies for +91{selected_session_mobile}")
                        except Exception as save_err:
                            logger.warning(f"[AutoSave] Failed to save cookies: {save_err}")
                        finally:
                            db_session.close()
                except Exception as capture_err:
                    logger.warning(f"[AutoSave] Failed to capture cookies: {capture_err}")

            # ── Always close browser context and release session lock ──────
            if context:
                try:
                    await context.close()
                except Exception:
                    pass

            if selected_session_id:
                BUSY_SESSION_IDS.discard(selected_session_id)
                logger.info(f"[SESSION LOCK] Released session +91{selected_session_mobile} (ID: {selected_session_id})")
