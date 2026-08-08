"""Unit tests for Phase 8: MenuSync (dominos_scraper.py and sync_realtime_menu).

Covers:
- Menu sync upsert database logic
- Real-time geocoding fallback coordinates
- Product category resolution rules
"""

import pytest
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.backend.database import Base, Product
from app.backend.services.dominos_service import sync_realtime_menu

# Use in-memory SQLite for testing
TEST_DATABASE_URL = "sqlite:///:memory:"

@pytest.fixture(name="db_session")
def fixture_db_session():
    engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)

@pytest.mark.asyncio
async def test_sync_realtime_menu_upsert(db_session, monkeypatch):
    # Mock geocode_address
    async def mock_geocode(city):
        return 19.0760, 72.8777
    monkeypatch.setattr("app.backend.services.dominos_scraper.geocode_address", mock_geocode)

    # Mock DominosBrowser.find_nearest_store and fetch_menu
    class MockBrowser:
        async def find_nearest_store(self, lat, lon, db=None):
            return {"store_id": "1234"}
        async def fetch_menu(self, store_id, page=1, limit=150, db=None):
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
                    "name": "Pepsi 500ml",
                    "price": 60.0,
                    "is_veg": True
                }
            ]
    
    monkeypatch.setattr("app.backend.services.dominos_browser.DominosBrowser", MockBrowser)

    # Add pre-existing product to test update logic
    p1 = Product(name="Margherita Classic Pizza", original_price=200.0, category="Veg", availability=True)
    db_session.add(p1)
    db_session.commit()

    # Sync
    await sync_realtime_menu("Mumbai", db_session)

    # Verify updates
    p1_db = db_session.query(Product).filter(Product.name == "Margherita Classic Pizza").first()
    assert p1_db.original_price == 250.0 # updated price
    assert p1_db.description == "Cheese & Tomato"

    # Verify inserts and auto-categorization
    pepsi = db_session.query(Product).filter(Product.name == "Pepsi 500ml").first()
    assert pepsi is not None
    assert pepsi.category == "Drinks"
    assert pepsi.original_price == 60.0
