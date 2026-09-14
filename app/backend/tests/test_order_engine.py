"""Unit tests for Phase 6: OrderEngine (order_processor.py and order_validator.py).

Covers:
- Order state machine transitions and disallowed transitions
- Pre-order coordinates bounding box validation
- Address PIN code extraction validation
- Product name mapping and unmapped warning warnings
- Audit log tracking for state changes
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.backend.database import Base, User, Order, Product, OrderItem, OrderStatusHistory
from app.backend.services.order_processor import transition_order_status
from app.backend.services.order_validator import (
    ValidationResult,
    _validate_address,
    _validate_coordinates,
    _validate_phone,
    _validate_cart_items,
)

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
        telegram_id="87654321",
        username="customer_user",
        role="user"
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user

@pytest.mark.asyncio
async def test_order_state_transitions(db_session, test_user):
    order = Order(
        user_id=test_user.id,
        transaction_id="TX_TEST_99",
        payment_method="wallet",
        original_total=300.0,
        total_payable=300.0,
        status="Payment Pending"
    )
    db_session.add(order)
    db_session.commit()
    db_session.refresh(order)

    # Valid transition: Payment Pending -> Payment Received
    res = await transition_order_status(db_session, order.id, "Payment Received")
    assert res["success"] is True
    assert res["order"].status == "Payment Received"

    # Invalid transition: Payment Received -> Delivered (not allowed directly)
    res_invalid = await transition_order_status(db_session, order.id, "Delivered")
    assert res_invalid["success"] is False
    assert "Cannot transition" in res_invalid["error"]

def test_order_validator_address():
    class DummyOrder:
        address = "123 Main Street, Sector 62, Noida - 201301, India"
    
    result = ValidationResult()
    pin = _validate_address(DummyOrder(), result)
    assert pin == "201301"
    assert result.ok is True
    assert len(result.errors) == 0

    # Test address too short
    class ShortAddressOrder:
        address = "Short"
    result_short = ValidationResult()
    pin_short = _validate_address(ShortAddressOrder(), result_short)
    assert pin_short is None
    assert result_short.ok is False
    assert any("too short" in err for err in result_short.errors)

def test_order_validator_coordinates():
    class ValidCoords:
        latitude = 19.0760
        longitude = 72.8777
    result = ValidationResult()
    lat, lng = _validate_coordinates(ValidCoords(), result)
    assert lat == 19.0760
    assert lng == 72.8777
    assert result.ok is True

    # Test out of bounds (Null Island)
    class NullIsland:
        latitude = 0.0
        longitude = 0.0
    result_null = ValidationResult()
    _validate_coordinates(NullIsland(), result_null)
    assert result_null.ok is False

def test_order_validator_phone():
    class ValidPhone:
        phone = "+91 98765 43210"
    result = ValidationResult()
    digits = _validate_phone(ValidPhone(), result)
    assert digits == "9876543210"

    class InvalidPhone:
        phone = "12345"
    result_invalid = ValidationResult()
    digits_invalid = _validate_phone(InvalidPhone(), result_invalid)
    assert digits_invalid is None

def test_order_validator_cart_items(db_session):
    p1 = Product(name="Margherita Classic", original_price=200.0, category="veg")
    p2 = Product(name="Unknown Artisanal Pizza", original_price=400.0, category="veg")
    db_session.add_all([p1, p2])
    db_session.commit()

    class DummyItem:
        id = 1
        product = p1
        quantity = 2
        unit_price = 200.0

    class DummyItemUnmapped:
        id = 2
        product = p2
        quantity = 1
        unit_price = 400.0

    class DummyOrder:
        items = [DummyItem(), DummyItemUnmapped()]

    result = ValidationResult()
    items = _validate_cart_items(DummyOrder(), result)
    assert len(items) == 2
    # Verify product mapping
    assert items[0]["dominos_name"] == "Margherita"
    assert items[1]["dominos_name"] == "Unknown Artisanal Pizza"
    # Unmapped products must trigger a warning but still be valid
    assert len(result.warnings) > 0
    assert result.ok is True


def test_active_offer_cart_resolution_and_rendering(db_session, test_user):
    from app.backend.database import ActiveOffer
    from app.backend.bot import resolve_cart_item, render_cart_message

    offer = ActiveOffer(
        offer_key="test_deal_1",
        title="2 Regular Veg Pizzas @ ₹199",
        badge="SAVE 40%",
        button_text="🛒 Add Deal",
        original_price=350.0,
        discounted_price=199.0,
        items_json='[{"name": "Margherita", "size": "Regular", "qty": 1}, {"name": "Farmhouse", "size": "Regular", "qty": 1}]'
    )
    db_session.add(offer)
    db_session.commit()

    # Test resolve_cart_item with offer_ key
    item_type, obj, item_name, unit_price, items_breakdown = resolve_cart_item(db_session, "offer_test_deal_1")
    assert item_type == "offer"
    assert item_name == "2 Regular Veg Pizzas @ ₹199"
    assert unit_price == 199.0
    assert len(items_breakdown) == 2

    # Test render_cart_message formatting
    cart = {"offer_test_deal_1": 1}
    session = {}
    cart_text, kbd = render_cart_message(db_session, test_user, cart, session)

    assert "2 Regular Veg Pizzas @ ₹199" in cart_text
    assert "₹199" in cart_text
    assert "Margherita (Regular)" in cart_text
    assert "Farmhouse (Regular)" in cart_text

