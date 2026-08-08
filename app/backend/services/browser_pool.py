import asyncio
import logging
import os
import sys

# Ensure Windows Proactor event loop policy for Playwright subprocess compatibility
if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

from playwright.async_api import async_playwright, Browser, BrowserContext

logger = logging.getLogger(__name__)


class BrowserPoolManager:
    def __init__(self):
        self.playwright_ctx = None
        self.browser = None
        self.is_headless = True   # default; updated on each launch
        self.lock = asyncio.Lock()

    async def get_browser(self) -> Browser:
        """Retrieve or initialise the shared Chromium browser instance."""
        # Shared browser pool is always headless for background stability
        is_headless = True

        async with self.lock:
            # If the headless option changed, close the existing browser and restart it with the new option
            if self.browser and self.browser.is_connected():
                if self.is_headless != is_headless:
                    logger.info(f"Closing browser because headless mode changed from {self.is_headless} to {is_headless}...")
                    await self.close_all_internal()
                else:
                    return self.browser

            logger.info("Initialising Browser Pool Chromium instance…")
            self.is_headless = is_headless
            width  = int(os.getenv("BROWSER_WIDTH",  "1366"))
            height = int(os.getenv("BROWSER_HEIGHT", "768"))

            await self.close_all_internal()

            if sys.platform == "win32":
                try:
                    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                except Exception:
                    pass

            self.playwright_ctx = await async_playwright().start()
            self.browser = await self.playwright_ctx.chromium.launch(
                headless=is_headless,
                # Small visual delay only when the window is visible (makes actions watchable)
                slow_mo=0 if is_headless else 45,
                handle_sigint=False,
                handle_sigterm=False,
                handle_sighup=False,
                args=[
                    "--log-level=3",
                    "--disable-logging",
                    "--silent",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-renderer-backgrounding",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-ipc-flooding-protection",
                    "--no-first-run",
                    "--no-default-browser-check",
                    # Anti-bot detection
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-site-isolation-trials",
                    "--disable-web-security",
                    "--allow-running-insecure-content",
                    "--disable-plugins-discovery",
                    "--no-pings",
                    "--disable-logging",
                    "--ignore-certificate-errors",
                    "--mute-audio",
                    # Stable render
                    "--disable-software-rasterizer",
                    "--disable-translate",
                    "--disable-hang-monitor",
                    "--disable-popup-blocking",
                    "--disable-prompt-on-repost",
                    "--disable-sync",
                    "--metrics-recording-only",
                    "--safebrowsing-disable-auto-update",
                    # Window size (matters even in headless for correct layout)
                    f"--window-size={width},{height}",
                ],
            )
            logger.info(
                f"Browser launched: headless={is_headless}, "
                f"viewport={width}×{height}"
            )
            return self.browser

    async def create_context(self, **kwargs) -> BrowserContext:
        """Create a fresh, isolated BrowserContext from the pool browser."""
        browser = await self.get_browser()
        try:
            return await browser.new_context(**kwargs)
        except Exception as e:
            logger.warning(
                f"Failed to create browser context from cached browser: {e}. "
                "Re-initialising browser pool and trying again..."
            )
            async with self.lock:
                await self.close_all_internal()
            browser = await self.get_browser()
            return await browser.new_context(**kwargs)

    async def close_all_internal(self):
        if self.browser:
            try:
                await self.browser.close()
            except Exception:
                pass
            self.browser = None
        if self.playwright_ctx:
            try:
                await self.playwright_ctx.stop()
            except Exception:
                pass
            self.playwright_ctx = None

    async def close_all(self):
        """Gracefully close the browser and Playwright context (thread-safe)."""
        async with self.lock:
            await self.close_all_internal()


browser_pool = BrowserPoolManager()
