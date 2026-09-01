import pytest
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
async def test_sync_realtime_menu_upsert(db_session):
    # Sync
    await sync_realtime_menu("Mumbai", db_session)

    # Verify that products are seeded
    from app.backend.database import SessionLocal
    db = SessionLocal()
    try:
        products = db.query(Product).all()
        assert len(products) > 0
        
        # Verify one of the seeded products exists (e.g., Margherita)
        margherita = db.query(Product).filter(Product.name == "Margherita").first()
        assert margherita is not None
        assert margherita.original_price == 239.0
        assert margherita.is_veg is True
    finally:
        db.close()

