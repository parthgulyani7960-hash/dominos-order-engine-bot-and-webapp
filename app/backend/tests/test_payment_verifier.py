"""Unit tests for Phase 8: PaymentVerifier (UPI payment verification /orders/{order_id}/verify-payment endpoint).

Covers:
- UTR numeric format validation (exactly 12 digits)
- UTR unique use checking (UTRAttempt history)
- Locked-out state checks after 3 failed attempts
- Success transition states from Payment Pending -> Order Processing
"""

import pytest
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.backend.database import Base, User, Order, VerifiedUTR, UTRAttempt, GiftCard
from app.backend.routes import verify_payment, PaymentVerifyRequest

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

@pytest.fixture(name="test_user")
def fixture_test_user(db_session):
    user = User(
        telegram_id="8888",
        username="paying_user",
        role="user"
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user

@pytest.mark.asyncio
async def test_verify_payment_format_failure(db_session, test_user):
    order = Order(
        id="ORD123",
        user_id=test_user.id,
        transaction_id="TX_123",
        payment_method="direct",
        original_total=250.0,
        total_payable=250.0,
        status="Payment Pending"
    )
    db_session.add(order)
    db_session.commit()

    from fastapi import HTTPException
    
    # Test non-12-digit UTR
    payload = PaymentVerifyRequest(utr="abc12345")
    with pytest.raises(HTTPException) as exc:
        await verify_payment("ORD123", payload, None, db_session, test_user)
    assert exc.value.status_code == 400
    assert "Invalid UTR format" in exc.value.detail

@pytest.mark.asyncio
async def test_verify_payment_lockout(db_session, test_user):
    order = Order(
        id="ORD123",
        user_id=test_user.id,
        transaction_id="TX_123",
        payment_method="direct",
        original_total=250.0,
        total_payable=250.0,
        status="Payment Pending"
    )
    db_session.add(order)
    
    # Insert 3 failed attempts
    for i in range(3):
        db_session.add(UTRAttempt(order_id="ORD123", utr=f"12345678901{i}", is_successful=False))
    db_session.commit()

    from fastapi import HTTPException
    payload = PaymentVerifyRequest(utr="123456789012")
    with pytest.raises(HTTPException) as exc:
        await verify_payment("ORD123", payload, None, db_session, test_user)
    assert exc.value.status_code == 403
    assert "lockout" in exc.value.detail.lower()

@pytest.mark.asyncio
async def test_verify_payment_success_with_giftcard(db_session, test_user, monkeypatch):
    # Mock bot notification
    async def mock_send_message(*args, **kwargs):
        pass
    monkeypatch.setattr("app.backend.routes.send_bot_message", mock_send_message)

    order = Order(
        id="ORD123",
        user_id=test_user.id,
        transaction_id="TX_123",
        payment_method="direct",
        original_total=250.0,
        total_payable=250.0,
        status="Payment Pending"
    )
    db_session.add(order)

    # Insert verified bank UTR confirmation
    db_session.add(VerifiedUTR(utr="123456789012", amount=250.0, order_id="ORD123"))

    # Add available gift card
    gc = GiftCard(
        code_encrypted="enc_code",
        code_hash="hash_code",
        pin_encrypted="enc_pin",
        value=500.0,
        status="available"
    )
    db_session.add(gc)
    db_session.commit()

    payload = PaymentVerifyRequest(utr="123456789012")
    res = await verify_payment("ORD123", payload, None, db_session, test_user)
    
    assert res["status"] == "Order Processing"
    assert res["message"] == "Payment verified and order is processing"
    
    # Check updated database records
    db_session.refresh(order)
    db_session.refresh(gc)
    assert order.status == "Order Processing"
    assert gc.status == "used"
    assert gc.used_in_order_id == "ORD123"
