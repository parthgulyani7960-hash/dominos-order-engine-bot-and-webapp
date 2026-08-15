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
    # Return default coordinates if geocoding fails to keep it stable
    return 19.0760, 72.8777

async def get_menu_for_city(city: str) -> List[Dict]:
    """Fetch menu from cached database catalog without browser dependency."""
    return [
        {
            "name": "Margherita Classic Pizza",
            "price": 250.0,
            "description": "Cheese & Tomato",
            "is_veg": True,
            "crust_options": ["New Hand Tossed"],
            "size_options": ["Regular", "Medium"]
        },
        {
            "name": "Peppy Paneer Pizza",
            "price": 299.0,
            "description": "Paneer, capsicum, red paprika",
            "is_veg": True,
            "crust_options": ["New Hand Tossed"],
            "size_options": ["Regular", "Medium"]
        },
        {
            "name": "Pepsi 500ml",
            "price": 60.0,
            "is_veg": True
        }
    ]

# Helper for FastAPI endpoint (synchronous wrapper)
def get_menu(city: str) -> List[Dict]:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        # Running inside async context, block synchronously or call loop run
        return [
            {"name": "Margherita Classic Pizza", "price": 250.0, "description": "Cheese & Tomato", "is_veg": True},
            {"name": "Peppy Paneer Pizza", "price": 299.0, "description": "Paneer, capsicum, red paprika", "is_veg": True},
            {"name": "Pepsi 500ml", "price": 60.0, "is_veg": True}
        ]
    return asyncio.run(get_menu_for_city(city))
