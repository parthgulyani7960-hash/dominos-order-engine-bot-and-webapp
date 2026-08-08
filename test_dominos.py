import asyncio
from app.backend.services.dominos_browser import DominosBrowser
from app.backend.database import get_db, init_db, SystemConfig

async def main():
    # Initialize DB and ensure captcha config exists
    init_db()
    db_gen = get_db()
    db = next(db_gen)
    if not db.query(SystemConfig).filter(SystemConfig.key == "captcha_api_key").first():
        # Placeholder; replace with real key if available
        db.add(SystemConfig(key="captcha_api_key", value="YOUR_2CAPTCHA_KEY"))
        db.commit()
    # Sample coordinates (Bangalore)
    lat, lon = 12.9716, 77.5946
    browser = DominosBrowser()
    store = await browser.find_nearest_store(lat, lon, db)
    print("Store:", store)
    # Increase limit to fetch more items
    menu = await browser.fetch_menu(store["store_id"], page=1, limit=20, db=db)
    print(f"Fetched {len(menu)} menu items")
    if not menu:
        print("No menu items retrieved; skipping cart and order steps.")
        return
    # Add first item to cart
    await browser.add_to_cart(store["store_id"], [{"dominos_id": menu[0]["dominos_id"], "quantity": 1}], db)
    # Place a mock order (COD)
    payload = {
        "store_id": store["store_id"],
        "items": [{"dominos_id": menu[0]["dominos_id"], "quantity": 1}],
        "address": {"pin": "560001", "text": "123 Test St"},
        "receiver": {"name": "Test User", "mobile": "9999999999"},
        "payment_method": "cod"
    }
    result = await browser.place_order(payload, db)
    print("Order result:", result)

if __name__ == "__main__":
    asyncio.run(main())
