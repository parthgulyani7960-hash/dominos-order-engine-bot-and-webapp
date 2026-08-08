import asyncio
import urllib.parse
import logging
from typing import List, Dict
import httpx

logger = logging.getLogger(__name__)

async def geocode_address(address: str) -> tuple:
    """Geocode address string to get (lat, lon) using OpenStreetMap Nominatim."""
    url = f"https://nominatim.openstreetmap.org/search?q={urllib.parse.quote(address)}&format=json&limit=1"
    headers = {"User-Agent": "DominosOrderEngineApp/2.0"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, timeout=8.0)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        logger.error(f"Error geocoding address ({address}): {e}")
    # Return None, None if geocoding fails (prevent fake mock location fallback)
    return None, None

async def get_menu_for_city(city: str) -> List[Dict]:
    """Fetch nearest store ID and menu from Domino's India portal in real-time."""
    lat, lng = await geocode_address(city)
    from .dominos_browser import DominosBrowser
    browser = DominosBrowser()
    store = await browser.find_nearest_store(lat, lng)
    menu = await browser.fetch_menu(store["store_id"], page=1, limit=100)
    return menu

# Helper for FastAPI endpoint (synchronous wrapper)
def get_menu(city: str) -> List[Dict]:
    return asyncio.run(get_menu_for_city(city))
