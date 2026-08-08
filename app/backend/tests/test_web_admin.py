"""Unit tests for Phase 7: WebAdmin (Admin stats and configuration endpoints).

Covers:
- /admin/stats endpoint logic and calculations
- Session and configuration verification
"""

import pytest
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.backend.database import Base, User, Order, UserSession, GiftCard, ErrorLog
from app.backend.routes import get_admin_stats

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

def test_admin_stats_calculations(db_session):
    # Setup test admin and users
    admin = User(telegram_id="111", username="admin", role="admin")
    user = User(telegram_id="222", username="user", role="user", wallet_balance=250.0)
    db_session.add_all([admin, user])
    db_session.commit()

    # Create dummy orders
    o1 = Order(user_id=user.id, transaction_id="TXN1", payment_method="wallet", original_total=100.0, total_payable=100.0, status="Completed")
    o2 = Order(user_id=user.id, transaction_id="TXN2", payment_method="wallet", original_total=150.0, total_payable=150.0, status="Payment Pending")
    o3 = Order(user_id=user.id, transaction_id="TXN3", payment_method="wallet", original_total=50.0, total_payable=50.0, status="Cancelled")
    o4 = Order(user_id=user.id, transaction_id="TXN4", payment_method="wallet", original_total=80.0, total_payable=80.0, status="Refunded")
    db_session.add_all([o1, o2, o3, o4])

    # Create active sessions
    s1 = UserSession(id="session1", user_id=user.id, refresh_token="tok1", is_active=True, last_active=datetime.datetime.now(datetime.timezone.utc))
    db_session.add(s1)

    # Create gift cards
    g1 = GiftCard(code_encrypted="encrypted_code1", code_hash="hash1", pin_encrypted="pin1", value=100.0, status="available")
    g2 = GiftCard(code_encrypted="encrypted_code2", code_hash="hash2", pin_encrypted="pin2", value=100.0, status="used")
    db_session.add_all([g1, g2])

    # Create error logs
    e1 = ErrorLog(type="payment", message="Failed payment transaction")
    db_session.add(e1)

    db_session.commit()

    # Run get_admin_stats logic directly
    stats = get_admin_stats(db_session, admin)
    
    assert stats["revenue"] == 250.0 # completed + pending (cancelled/refunded excluded)
    assert stats["active_users"] == 2 # admin + user
    assert stats["online_users"] == 1
    assert stats["active_orders"] == 1 # 'Payment Pending' (Completed/Cancelled/Refunded excluded)
    assert stats["completed_orders"] == 1
    assert stats["failed_payments"] == 1
    assert stats["refunds"]["count"] == 1
    assert stats["refunds"]["total_amount"] == 80.0
    assert stats["wallet_balances_total"] == 250.0
    assert stats["gift_cards"]["available"] == 1
    assert stats["gift_cards"]["used"] == 1
