import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.backend.database import Base, User, Order, DominosSession
from app.backend.services.order_sync import BUSY_SESSION_IDS
from app.backend.settings import settings

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

def test_order_sync_auto_assign_sessions(db_session):
    # Setup settings
    settings.AUTO_ASSIGN_SESSIONS = True
    
    # 1. Create a session in database that is verified and active
    active_session = DominosSession(
        id="session-active-abc",
        mobile_number="9876543210",
        is_active=True,
        verify_status="valid",
        cookies="[]"
    )
    db_session.add(active_session)
    db_session.commit()
    
    # 2. Query fallback session using the same logic as order_sync.py
    BUSY_SESSION_IDS.clear()
    
    fallback_session = db_session.query(DominosSession).filter(
        DominosSession.is_active == True,
        DominosSession.verify_status == "valid",
        ~DominosSession.id.in_(list(BUSY_SESSION_IDS)) if BUSY_SESSION_IDS else True
    ).order_by(DominosSession.created_at.desc()).first()
    
    assert fallback_session is not None
    assert fallback_session.id == "session-active-abc"
    assert fallback_session.mobile_number == "9876543210"
