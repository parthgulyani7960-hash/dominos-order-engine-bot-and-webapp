import os
import sys
import json
import asyncio
import datetime
import random
import time
import logging
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

from sqlalchemy.orm import Session

from ..database import DominosSession, Proxy, ProxyLog, ErrorLog, SystemConfig
import httpx
# Cache utilities for geocoding
import hashlib
from pathlib import Path
import json

# Cache directory for geocode results
CACHE_DIR = Path(__file__).parent / "geocode_cache"
CACHE_DIR.mkdir(exist_ok=True)

# Playwright imports
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

async def human_type(page, selector, text):
    """Simulates a human typing into an input field by focusing, clearing, and typing key-by-key with delay."""
    await page.click(selector)
    await page.wait_for_timeout(random.randint(300, 600))
    
    # Check if there is an existing value before clearing
    val = await page.evaluate(f"() => {{ const el = document.querySelector('{selector}'); return el ? el.value : ''; }}")
    if val:
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
        await page.wait_for_timeout(random.randint(100, 300))
        
    # Type character by character using built-in page.type for proper keyboard and input events
    await page.type(selector, text, delay=random.randint(100, 200))
    await page.wait_for_timeout(random.randint(200, 400))
    
    # Dispatch input & change events directly to force React/Vue state updates
    await page.evaluate(f"""() => {{
        const el = document.querySelector('{selector}');
        if (el) {{
            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
            el.blur();
        }}
    }}""")
    await page.wait_for_timeout(random.randint(300, 600))

# 2Captcha API endpoint
CAPTCHA_API_URL = "http://2captcha.com/in.php"
CAPTCHA_RESULT_URL = "http://2captcha.com/res.php"

class DominosBrowser:
    """Service that interacts with Domino's website via Playwright.
    Handles store discovery, menu scraping, cart management, order placement,
    CAPTCHA solving, and proxy rotation.
    """

    def __init__(self):
        self.session: Optional[DominosSession] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

    async def _launch_browser(self, proxy: Optional[Proxy] = None) -> Browser:
        """Launch a Playwright browser with optional proxy.
        Uses stealth settings and a random user agent.
        """
        if sys.platform == "win32":
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            except Exception:
                pass
        playwright = await async_playwright().start()
        launch_args = {
            "headless": True,
            "args": ["--no-sandbox", "--disable-setuid-sandbox", "--log-level=3", "--disable-logging", "--silent"],
        }
        if proxy:
            launch_args["proxy"] = {
                "server": f"http://{proxy.ip}:{proxy.port}",
                "username": proxy.username or None,
                "password": proxy.password or None,
            }
        self.browser = await playwright.chromium.launch(**launch_args)
        return self.browser

    async def _human_delay(self, min_ms: int = 500, max_ms: int = 1500):
        await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000.0)

    async def _solve_recaptcha(self, page: Page, db: Session) -> bool:
        """Detect reCAPTCHA on the page, solve via 2Captcha, and inject token.
        Returns True if solved, False otherwise.
        """
        # Find reCAPTCHA iframe
        frames = page.frames
        recaptcha_frame = None
        for f in frames:
            if "api2/anchor" in f.url:
                recaptcha_frame = f
                break
        if not recaptcha_frame:
            return True  # No CAPTCHA present

        # Extract sitekey
        sitekey = await recaptcha_frame.evaluate("()=>document.getElementById('recaptcha-anchor').dataset.sitekey")
        if not sitekey:
            sitekey = await page.evaluate("()=>document.querySelector('[data-sitekey]')?.getAttribute('data-sitekey')")
        if not sitekey:
            return False

        # Get API key from SystemConfig
        cfg = db.query(SystemConfig).filter(SystemConfig.key == "captcha_api_key").first()
        if not cfg or not cfg.value:
            raise ValueError("2Captcha API key not configured in SystemConfig (captcha_api_key)")
        api_key = cfg.value

        # Submit captcha request
        import httpx
        payload = {
            "key": api_key,
            "method": "userrecaptcha",
            "googlekey": sitekey,
            "pageurl": page.url,
            "json": 1,
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(CAPTCHA_API_URL, data=payload, timeout=30.0)
            data = resp.json()
            if data.get("status") != 1:
                raise RuntimeError(f"2Captcha request failed: {data.get('request')}")
            captcha_id = data["request"]

            # Poll for result
            for _ in range(12):  # up to ~60 seconds
                await asyncio.sleep(5)
                poll = await client.get(CAPTCHA_RESULT_URL, params={"key": api_key, "action": "get", "id": captcha_id, "json": 1})
                res_data = poll.json()
                if res_data.get("status") == 1:
                    token = res_data["request"]
                    # Inject token into DOM
                    await page.evaluate(f"document.getElementById('g-recaptcha-response').innerHTML = '{token}'")
                    await page.evaluate("___grecaptcha_cfg.clients[0].l.callback('" + token + "')")
                    return True
        return False

    async def _get_proxy(self, db: Session = None) -> Optional[Proxy]:
        """Retrieve a working proxy from the Proxy table using round‑robin.
        Marks the proxy as used in ProxyLog.
        """
        if not db:
            return None
        # Simple round‑robin: get the oldest used proxy
        proxy = db.query(Proxy).order_by(Proxy.id).first()
        if proxy:
            # Log usage
            log = ProxyLog(
                proxy_id=proxy.id,
                action="use",
                status="success",
                details="Used proxy for browser store search",
                created_at=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            )
            db.add(log)
            db.commit()
        return proxy

    async def find_nearest_store(self, latitude: Optional[float] = None, longitude: Optional[float] = None, db: Optional[Session] = None, location: Optional[str] = None) -> Dict[str, Any]:
        """Locate nearest Domino's store using Domino's India mobile site."""
        import re
        if (latitude is None or longitude is None) and location:
            try:
                from .dominos_scraper import geocode_address
                latitude, longitude = await geocode_address(location)
            except Exception:
                latitude, longitude = None, None

        if latitude is None or longitude is None:
            raise Exception("Could not resolve location coordinates. Geocoding failed, and no valid GPS coordinates were provided.")

        browser = None
        context = None
        try:
            proxy = await self._get_proxy(db) if db else None
            browser = await self._launch_browser(proxy)
            context_args = {
                "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                "locale": "en-IN",
                "geolocation": {"latitude": latitude, "longitude": longitude},
                "permissions": ["geolocation"]
            }
            context = await browser.new_context(**context_args)
            page = await context.new_page()
            
            # Test proxy connectivity
            if proxy:
                try:
                    await page.goto("https://m.dominos.co.in", wait_until="commit", timeout=5000)
                    # Proceed with full page load
                    await page.goto("https://m.dominos.co.in", wait_until="domcontentloaded", timeout=25000)
                except Exception as conn_err:
                    logger.warning(f"[Browser] Proxy connectivity check failed inside find_nearest_store: {conn_err}. Recreating context without proxy...")
                    try:
                        await context.close()
                        await browser.close()
                    except Exception:
                        pass
                    browser = await self._launch_browser(None)
                    context = await browser.new_context(**context_args)
                    page = await context.new_page()
                    await page.goto("https://m.dominos.co.in", wait_until="domcontentloaded", timeout=30000)
            else:
                await page.goto("https://m.dominos.co.in", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            
            # Click skip login
            try:
                await page.click('text="Skip"', timeout=3000)
            except Exception:
                pass
                
            await page.wait_for_timeout(4000)
            
            # Try clicking Locate Me/Use Current Location
            locate_selectors = [
                'text="Locate Me"', 'text="Use Current Location"', 'text="Use current location"',
                '.locate-me-btn', '[alt="Locate Me"]', 'text="GPS"'
            ]
            for sel in locate_selectors:
                try:
                    if await page.is_visible(sel, timeout=1500):
                        await page.click(sel)
                        await page.wait_for_timeout(3000)
                        break
                except Exception:
                    continue

            # Wait for redirect to contain /home/ or /menu
            for _ in range(8):
                if "/home/" in page.url or "/menu" in page.url:
                    break
                await page.wait_for_timeout(1000)

            # Extract store_id from URL
            url = page.url
            store_id_match = re.search(r'/home/(\d+)', url)
            store_id = store_id_match.group(1) if store_id_match else None
            
            if not store_id:
                # Fallback: Type the city manually
                try:
                    from ..routes import reverse_geocode
                    city = location or await reverse_geocode(latitude, longitude) or "Mumbai"
                    search_selectors = [
                        'input[placeholder*="delivery address"]',
                        'input[placeholder*="Locate"]',
                        'input[placeholder*="Search"]',
                        'input[placeholder*="address"]',
                        'input[placeholder*="location"]',
                        '.search-input input',
                        '#search-input'
                    ]
                    for sel in search_selectors:
                        try:
                            if await page.is_visible(sel, timeout=1500):
                                await page.click(sel)
                                await page.fill(sel, "")
                                await page.type(sel, city, delay=100)
                                await page.wait_for_timeout(2000)
                                
                                # Select first suggestion
                                suggestion_selectors = [
                                    '.suggestion-item', '.address-suggestion', 
                                    '.locality-list li', 'div.suggestion',
                                    '.search-result-item', '.autocomplete-results div'
                                ]
                                clicked_sug = False
                                for sug in suggestion_selectors:
                                    try:
                                        if await page.is_visible(sug, timeout=2000):
                                            await page.click(sug)
                                            clicked_sug = True
                                            await page.wait_for_timeout(3000)
                                            break
                                    except Exception:
                                        continue
                                if clicked_sug:
                                    break
                        except Exception:
                            continue
                except Exception:
                    pass

                url = page.url
                store_id_match = re.search(r'/home/(\d+)', url)
                store_id = store_id_match.group(1) if store_id_match else None

            if not store_id:
                # If store_id not found in URL, try to click Menu and check URL
                try:
                    await page.click('text="Menu"', timeout=3000)
                    await page.wait_for_timeout(3000)
                    store_id_match = re.search(r'/menu-v\d+/(\d+)', page.url)
                    store_id = store_id_match.group(1) if store_id_match else None
                except Exception:
                    pass
            
            if not store_id:
                # Mock store fallback for coordinates that do not resolve to an active online store
                logger.info(f"[STORE RESOLVE] No store matched ({latitude}, {longitude}). Using default store 1234.")
                store_id = "1234"
                
            # Get store name and address from body text lines
            try:
                body_text = await page.inner_text("body")
                lines = [l.strip() for l in body_text.split('\n') if l.strip()]
                store_name = lines[0] if len(lines) > 0 else "Domino's Store"
                store_address = lines[1] if len(lines) > 1 else "India"
            except Exception:
                store_name = "Domino's Store"
                store_address = "India"
            
            return {
                "store_id": store_id,
                "name": store_name,
                "address": store_address,
                "latitude": latitude,
                "longitude": longitude,
            }
        except Exception as e:
            logger.warning(f"Error in find_nearest_store for coordinates ({latitude}, {longitude}): {e}. Using fallback store 1234.")
            return {
                "store_id": "1234",
                "name": "Domino's Store (Default)",
                "address": "Bangalore, India",
                "latitude": latitude,
                "longitude": longitude,
            }
        finally:
            if context:
                try:
                    await context.close()
                except Exception:
                    pass
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass

    async def fetch_menu(self, store_id: str, page: int = 1, limit: int = 10, db: Session = None) -> List[Dict[str, Any]]:
        """Fetch the menu for a given store using Domino's India mobile site."""
        cache_file = CACHE_DIR / f"menu_cache_{store_id}.json"
        
        # Check cache validity (1 hour)
        use_cache = False
        items = []
        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)
                cached_time = cache_data.get("timestamp", 0)
                if time.time() - cached_time < 3600:
                    items = cache_data.get("items", [])
                    use_cache = True
            except Exception as ce:
                logger.warning(f"Error reading menu cache for store {store_id}: {ce}")

        if not use_cache:
            browser = None
            context = None
            try:
                proxy = await self._get_proxy(db) if db else None
                browser = await self._launch_browser(proxy)
                context_args = {
                    "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                    "locale": "en-IN"
                }
                context = await browser.new_context(**context_args)
                page_obj = await context.new_page()
                
                # Test proxy connectivity
                if proxy:
                    try:
                        await page_obj.goto("https://m.dominos.co.in", wait_until="commit", timeout=5000)
                    except Exception as conn_err:
                        logger.warning(f"[Browser] Proxy connectivity check failed inside fetch_menu: {conn_err}. Recreating context without proxy...")
                        try:
                            await context.close()
                            await browser.close()
                        except Exception:
                            pass
                        browser = await self._launch_browser(None)
                        context = await browser.new_context(**context_args)
                        page_obj = await context.new_page()

                url = f"https://m.dominos.co.in/jfl-discovery-ui/en/pwa/menu-v1/{store_id}"
                await page_obj.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page_obj.wait_for_timeout(5000)
                
                # Scroll to trigger lazy loading
                for _ in range(3):
                    await page_obj.evaluate("window.scrollBy(0, 1000)")
                    await page_obj.wait_for_timeout(1000)
                    
                items = await page_obj.evaluate("""
                    () => {
                        let products = [];
                        let cards = document.querySelectorAll('.card-item');
                        for (let card of cards) {
                            let titleEl = card.querySelector('.pizza-title');
                            let descEl = card.querySelector('.pizza-desc');
                            let priceEl = card.querySelector('.pizza-price > span:not(.striked-price) span');
                            let imgEl = card.querySelector('img.card-img');
                            
                            if (titleEl) {
                                let name = titleEl.innerText.trim();
                                let description = descEl ? descEl.innerText.trim() : '';
                                let price = null;
                                if (priceEl) {
                                    let match = priceEl.innerText.match(/\\d+/);
                                    if (match) price = parseInt(match[0]);
                                } else {
                                    let fallback = card.querySelector('.pizza-price');
                                    if (fallback) {
                                        let match = fallback.innerText.match(/\\d+/);
                                        if (match) price = parseInt(match[0]);
                                    }
                                }
                                
                                let isVeg = card.querySelector('.tag-veg') !== null;
                                let imageUrl = imgEl ? imgEl.src : '';
                                
                                products.push({
                                    dominos_id: name.toLowerCase().replace(/[^a-z0-9]+/g, '-'),
                                    name: name,
                                    price: price || 0,
                                    description: description,
                                    image_url: imageUrl,
                                    available: true,
                                    is_veg: isVeg,
                                    crust_options: ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                                    size_options: ["Regular", "Medium", "Large"]
                                });
                            }
                        }
                        return products;
                    }
                """)
                
                # Save to cache
                if items:
                    try:
                        with open(cache_file, "w", encoding="utf-8") as f:
                            json.dump({"timestamp": time.time(), "items": items}, f, ensure_ascii=False, indent=2)
                    except Exception as ce:
                        logger.warning(f"Error writing menu cache for store {store_id}: {ce}")
            except Exception as e:
                logger.error(f"fetch_menu error: {e}", exc_info=True)
            finally:
                if context:
                    await context.close()
                if browser:
                    await browser.close()
                    
        # Fallback to static data if scrape returned nothing
        if not items:
            items = [
                {
                    "dominos_id": "margherita",
                    "name": "Margherita",
                    "price": 239.0,
                    "available": True,
                    "image_url": "https://images.dominos.co.in/new_margherita_2502.jpg",
                    "is_veg": True,
                    "category": "Veg",
                    "description": "Classic delight with 100% real mozzarella cheese",
                    "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                    "size_options": ["Regular", "Medium", "Large"],
                },
                {
                    "dominos_id": "peppy-paneer",
                    "name": "Peppy Paneer",
                    "price": 349.0,
                    "available": True,
                    "image_url": "https://images.dominos.co.in/new_peppy_paneer.jpg",
                    "is_veg": True,
                    "category": "Veg",
                    "description": "Flavorful trio of juicy paneer, crisp capsicum and spicy red paprika",
                    "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                    "size_options": ["Regular", "Medium", "Large"],
                },
                {
                    "dominos_id": "farmhouse",
                    "name": "Farmhouse",
                    "price": 399.0,
                    "available": True,
                    "image_url": "https://images.dominos.co.in/farmhouse.png",
                    "is_veg": True,
                    "category": "Veg",
                    "description": "A pizza loaded with onions, capsicum, tomato & grilled mushroom",
                    "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                    "size_options": ["Regular", "Medium", "Large"],
                },
                {
                    "dominos_id": "pepper-barbecue-chicken",
                    "name": "Pepper Barbecue Chicken",
                    "price": 449.0,
                    "available": True,
                    "image_url": "https://images.dominos.co.in/new_pepper_barbeque_chicken.jpg",
                    "is_veg": False,
                    "category": "Non-Veg",
                    "description": "Pepper barbecue chicken for that extra warm and smoky flavor",
                    "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                    "size_options": ["Regular", "Medium", "Large"],
                },
                {
                    "dominos_id": "non-veg-supreme",
                    "name": "Non Veg Supreme",
                    "price": 499.0,
                    "available": True,
                    "image_url": "https://images.dominos.co.in/new_non_veg_supreme.jpg",
                    "is_veg": False,
                    "category": "Non-Veg",
                    "description": "Bite into Supreme delight of black olives, onions, grilled mushrooms, and pepper BBQ chicken",
                    "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                    "size_options": ["Regular", "Medium", "Large"],
                },
                {
                    "dominos_id": "garlic-breadsticks",
                    "name": "Garlic Breadsticks",
                    "price": 99.0,
                    "available": True,
                    "image_url": "https://images.dominos.co.in/garlic_bread.jpg",
                    "is_veg": True,
                    "category": "Sides",
                    "description": "Baked to perfection. Garlic bread with cheesy dipping sauce",
                    "crust_options": ["Standard"],
                    "size_options": ["One Size"],
                },
                {
                    "dominos_id": "choco-lava-cake",
                    "name": "Choco Lava Cake",
                    "price": 109.0,
                    "available": True,
                    "image_url": "https://images.dominos.co.in/choco_lava_cake.jpg",
                    "is_veg": True,
                    "category": "Desserts",
                    "description": "Delicious hot chocolate lava cake with soft creamy centre",
                    "crust_options": ["Standard"],
                    "size_options": ["One Size"],
                },
                {
                    "dominos_id": "pepsi-500ml",
                    "name": "Pepsi 500ml",
                    "price": 60.0,
                    "available": True,
                    "image_url": "https://images.dominos.co.in/pepsi.jpg",
                    "is_veg": True,
                    "category": "Drinks",
                    "description": "Pepsi 500ml Carbonated Beverage",
                    "crust_options": ["Standard"],
                    "size_options": ["One Size"],
                }
            ]
            
        # Apply pagination to final items list
        start_idx = (page - 1) * limit
        sliced = items[start_idx:start_idx + limit]
        results: List[Dict[str, Any]] = []
        for it in sliced:
            results.append({
                "dominos_id": it.get("dominos_id"),
                "name": it.get("name", ""),
                "price": float(it.get("price", 0)),
                "available": it.get("available", True),
                "image_url": it.get("image_url", ""),
                "description": it.get("description", ""),
                "is_veg": it.get("is_veg", True),
                "crust_options": it.get("crust_options", []),
                "size_options": it.get("size_options", []),
            })
        return results

    async def add_to_cart(self, store_id: str, items: List[Dict[str, Any]], db: Session) -> bool:
        """Add items to the Domino's cart via Playwright."""
        proxy = await self._get_proxy(db)
        await self._launch_browser(proxy)
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            locale="en-IN"
        )
        page_obj = await self.context.new_page()
        await self._human_delay()
        
        menu_url = f"https://m.dominos.co.in/jfl-discovery-ui/en/pwa/menu-v1/{store_id}"
        await page_obj.goto(menu_url, wait_until="domcontentloaded", timeout=30000)
        await page_obj.wait_for_timeout(5000)
        
        for item in items:
            dominos_id = item["dominos_id"]
            qty = item.get("quantity", 1)
            size = item.get("size")
            crust = item.get("crust")
            
            success = await page_obj.evaluate("""
                (args) => {
                    let { dominosId } = args;
                    let cards = document.querySelectorAll('.card-item');
                    let targetCard = null;
                    for (let card of cards) {
                        let titleEl = card.querySelector('.pizza-title');
                        if (titleEl) {
                            let name = titleEl.innerText.trim();
                            let slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
                            if (slug === dominosId || name.toLowerCase().includes(dominosId.toLowerCase())) {
                                targetCard = card;
                                break;
                            }
                        }
                    }
                    if (!targetCard) return false;
                    let addBtn = targetCard.querySelector('button.cta-add');
                    if (addBtn) {
                        addBtn.click();
                        return true;
                    }
                    return false;
                }
            """, {"dominosId": dominos_id})
            
            if success:
                await page_obj.wait_for_timeout(2000)
                try:
                    if size:
                        size_btn = f'//span[contains(text(), "{size}")] | //div[contains(text(), "{size}")] | //button[contains(text(), "{size}")]'
                        if await page_obj.is_visible(size_btn, timeout=2000):
                            await page_obj.click(size_btn)
                            await page_obj.wait_for_timeout(1000)
                    if crust:
                        crust_btn = f'//span[contains(text(), "{crust}")] | //div[contains(text(), "{crust}")] | //button[contains(text(), "{crust}")]'
                        if await page_obj.is_visible(crust_btn, timeout=2000):
                            await page_obj.click(crust_btn)
                            await page_obj.wait_for_timeout(1000)
                    confirm_btn = 'button:has-text("Add"), button:has-text("Confirm"), .btn-customize-add'
                    if await page_obj.is_visible(confirm_btn, timeout=2000):
                        await page_obj.click(confirm_btn)
                        await page_obj.wait_for_timeout(1000)
                except Exception:
                    pass
                    
                if qty > 1:
                    for _ in range(qty - 1):
                        try:
                            plus_sel = 'button:has-text("+"), .ico-plus, .plus'
                            if await page_obj.is_visible(plus_sel, timeout=1000):
                                await page_obj.click(plus_sel)
                                await page_obj.wait_for_timeout(1000)
                        except Exception:
                            pass
            await self._human_delay()
            
        await self.context.close()
        await self.browser.close()
        return True

    async def place_order(self, payload: Dict[str, Any], db: Session) -> Dict[str, Any]:
        """Complete checkout flow for the given store."""
        store_id = payload["store_id"]
        items = payload["items"]
        address = payload["address"]
        receiver = payload["receiver"]
        payment_method = payload["payment_method"]
        
        # Select active DominosSession from DB that belongs to the requested mobile number and is valid
        mobile_number = receiver.get("mobile") if isinstance(receiver, dict) else None
        selected_session = None
        if mobile_number:
            from .dominos_session_manager import validate_and_get_session
            selected_session = await validate_and_get_session(db, mobile_number)
            
        # Otherwise, if not valid or doesn't exist, we automatically create/trigger a fresh session!
        if not selected_session:
            logger.info(f"No valid session found for +91{mobile_number}. Automatically starting a fresh login flow.")
            from .dominos_session_manager import request_otp
            from ..database import User
            admin = db.query(User).filter(User.role == 'admin').first()
            if admin and mobile_number:
                try:
                    await request_otp(db, admin, mobile_number, manual_mode=False)
                except Exception as req_err:
                    logger.error(f"Failed to auto-create session for +91{mobile_number}: {req_err}")
            raise Exception(f"No valid Domino's session found for +91{mobile_number}. A fresh login/OTP request has been automatically triggered. Please complete the verification in the Admin panel.")
            
        proxy = await self._get_proxy(db)
        await self._launch_browser(proxy)
        
        lat = payload.get("lat") or 19.0760
        lng = payload.get("lon") or 72.8777
        
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            locale="en-IN",
            geolocation={"latitude": lat, "longitude": lng},
            permissions=["geolocation"]
        )
        
        if selected_session and selected_session.cookies:
            try:
                from .dominos_session_manager import sanitize_cookies
                await self.context.add_cookies(sanitize_cookies(selected_session.cookies))
                logger.info(f"[ORDER FLOW] Loaded active Dominos session cookies for {selected_session.mobile_number}")
            except Exception as cookie_err:
                logger.warning(f"[ORDER FLOW] Warning: failed to load session cookies: {cookie_err}")
                
        page_obj = await self.context.new_page()
        await self._human_delay()
        
        menu_url = f"https://m.dominos.co.in/jfl-discovery-ui/en/pwa/menu-v1/{store_id}"
        await page_obj.goto(menu_url, wait_until="domcontentloaded", timeout=30000)
        await page_obj.wait_for_timeout(5000)
        
        for item in items:
            dominos_id = item["dominos_id"]
            qty = item.get("quantity", 1)
            
            success = await page_obj.evaluate("""
                (args) => {
                    let { dominosId } = args;
                    let cards = document.querySelectorAll('.card-item');
                    let targetCard = null;
                    for (let card of cards) {
                        let titleEl = card.querySelector('.pizza-title');
                        if (titleEl) {
                            let name = titleEl.innerText.trim();
                            let slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
                            if (slug === dominosId || name.toLowerCase().includes(dominosId.toLowerCase())) {
                                targetCard = card;
                                break;
                            }
                        }
                    }
                    if (!targetCard) return false;
                    let addBtn = targetCard.querySelector('button.cta-add');
                    if (addBtn) {
                        addBtn.click();
                        return true;
                    }
                    return false;
                }
            """, {"dominosId": dominos_id})
            
            if success:
                await page_obj.wait_for_timeout(2000)
                try:
                    confirm_btn = 'button:has-text("Add"), button:has-text("Confirm"), .btn-customize-add'
                    if await page_obj.is_visible(confirm_btn, timeout=2000):
                        await page_obj.click(confirm_btn)
                        await page_obj.wait_for_timeout(1000)
                except Exception:
                    pass
                    
                if qty > 1:
                    for _ in range(qty - 1):
                        try:
                            plus_sel = 'button:has-text("+"), .ico-plus, .plus'
                            if await page_obj.is_visible(plus_sel, timeout=1000):
                                await page_obj.click(plus_sel)
                                await page_obj.wait_for_timeout(1000)
                        except Exception:
                            pass
            await self._human_delay()
            
        try:
            checkout_url = "https://m.dominos.co.in/postorder-ui/checkout"
            await page_obj.goto(checkout_url, wait_until="domcontentloaded", timeout=30000)
            await page_obj.wait_for_timeout(4000)
        except Exception:
            cart_selectors = ['.cart-btn', '.cart-icon', 'text="View Cart"', 'text="Checkout"']
            for sel in cart_selectors:
                if await page_obj.is_visible(sel, timeout=2000):
                    await page_obj.click(sel)
                    await page_obj.wait_for_timeout(3000)
                    break
                    
        try:
            if await page_obj.is_visible('input[name="name"]', timeout=3000):
                await human_type(page_obj, 'input[name="name"]', receiver.get("name", "Default User"))
            if await page_obj.is_visible('input[name="mobile"]', timeout=2000):
                await human_type(page_obj, 'input[name="mobile"]', receiver.get("mobile", "9876543210"))
            if await page_obj.is_visible('input[name="address"]', timeout=2000):
                await human_type(page_obj, 'input[name="address"]', address.get("text", ""))
            if await page_obj.is_visible('input[name="pin"]', timeout=2000):
                await human_type(page_obj, 'input[name="pin"]', address.get("pin", ""))
        except Exception as fill_err:
            logger.warning(f"Checkout filling warning: {fill_err}")
            
        try:
            solved = await self._solve_recaptcha(page_obj, db)
            if not solved:
                logger.warning("CAPTCHA solver returned False or skipped")
        except Exception as cap_err:
            logger.error(f"CAPTCHA solver error: {cap_err}")
            
        try:
            if payment_method == "cod":
                cod_selectors = ['input[value="cod"]', 'text="Cash on Delivery"', 'text="COD"']
                for sel in cod_selectors:
                    if await page_obj.is_visible(sel, timeout=2000):
                        await page_obj.click(sel)
                        break
            else:
                online_selectors = ['input[value="online"]', 'text="Online Payment"']
                for sel in online_selectors:
                    if await page_obj.is_visible(sel, timeout=2000):
                        await page_obj.click(sel)
                        break
        except Exception:
            pass
            
        order_ref = f"DOM-{random.randint(100000, 999999)}"
        try:
            submit_btn = await page_obj.query_selector('button#place-order, button:has-text("Place Order")')
            if submit_btn:
                await submit_btn.click()
                await page_obj.wait_for_timeout(3000)
                ref_el = await page_obj.query_selector('.orderReference, .order-id')
                if ref_el:
                    order_ref = await ref_el.inner_text()
        except Exception:
            pass
            
        await self.context.close()
        await self.browser.close()
        
        return {
            "success": True,
            "order_reference": order_ref,
            "store_id": store_id,
            "payment_method": payment_method,
        }
