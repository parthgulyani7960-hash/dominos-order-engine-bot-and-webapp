"""
order_validator.py
Pre-Order Validation Engine for Domino's Robot

Runs BEFORE the browser launches. Validates every piece of user/order data
and returns a list of specific, human-readable errors. This catches problems
early so the admin sees a clear error within seconds, not after 3+ minutes.

Usage:
    from .order_validator import validate_order_for_robot
    errors = await validate_order_for_robot(order, db)
    if errors:
        raise OrderValidationError(errors)
"""

import re
import logging
from typing import List, Tuple, Optional
from dataclasses import dataclass, field
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# India bounding box — coordinates outside this range are invalid
INDIA_LAT_MIN, INDIA_LAT_MAX = 6.5, 37.5
INDIA_LNG_MIN, INDIA_LNG_MAX = 68.0, 97.5

# Known Domino's India menu items — used to check product name mapping
KNOWN_DOMINOS_ITEMS = {
    "margherita", "pepperoni", "veg extravaganza", "chicken golden delight",
    "double cheese margherita", "paneer makhani", "peppy paneer",
    "farmhouse", "deluxe veggie", "mexican green wave",
    "spicy chicken", "keema do pyaza", "chicken tikka",
    "non veg supreme", "fiery red pepper pizza", "simply veggie",
    "pizza mania", "tomato", "golden corn", "simply non veg",
}

# Internal product name → Domino's name mapping
PRODUCT_NAME_MAP = {
    "Margherita Classic": "Margherita",
    "Pepperoni Feast": "Pepperoni",
    "Garden Veggie Supreme": "Veg Extravaganza",
    "BBQ Smoked Chicken": "Chicken Golden Delight",
    "Double Cheese Romano": "Double Cheese Margherita",
    "Cheeseburst Margherita": "Margherita",
    "Tomato Onion Pizza Mania": "Tomato",
    "Golden Corn Pizza Mania": "Golden Corn",
    "Truffle Mushroom Artisan": "Peppy Paneer",
}


# ─────────────────────────────────────────────────────────────────────────────
# Result Types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    ok: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    corrected: dict = field(default_factory=dict)   # suggested auto-corrections

    def add_error(self, msg: str):
        self.ok = False
        self.errors.append(msg)

    def add_warning(self, msg: str):
        self.warnings.append(msg)

    def add_correction(self, field: str, old_val, new_val, reason: str):
        self.corrected[field] = {
            "from": str(old_val),
            "to": str(new_val),
            "reason": reason
        }


class OrderValidationError(Exception):
    """Raised when an order fails pre-flight validation."""
    def __init__(self, errors: List[str], warnings: List[str] = None):
        self.errors = errors
        self.warnings = warnings or []
        super().__init__("Order validation failed:\n" + "\n".join(f"  ✗ {e}" for e in errors))


# ─────────────────────────────────────────────────────────────────────────────
# Individual Validators
# ─────────────────────────────────────────────────────────────────────────────

def _validate_address(order, result: ValidationResult) -> Optional[str]:
    """
    Validates and extracts PIN code from order address.
    Returns the cleaned PIN code string if found, else None.
    Attempts to auto-correct common address issues.
    """
    addr = order.address or ""
    addr = addr.strip()

    if not addr:
        result.add_error("❌ Delivery address is empty. The customer must provide a delivery address.")
        return None

    if len(addr) < 6:
        result.add_error(f"❌ Delivery address is too short ({len(addr)} chars). Minimum 6 characters required: '{addr}'")
        return None

    if len(addr) > 500:
        truncated = addr[:500]
        result.add_warning(f"⚠️ Address is very long ({len(addr)} chars). Will truncate to 500 chars.")
        result.add_correction("address", addr, truncated, "Address truncated to 500 chars for Domino's form")

    # Check for PIN code
    pin_match = re.search(r'\b(\d{6})\b', addr)
    if not pin_match:
        result.add_warning(
            f"⚠️ No 6-digit PIN code found in address: '{addr}'. "
            "The robot will attempt to locate the store by coordinates only."
        )
        return None

    pin_code = pin_match.group(1)

    # Basic Indian PIN code validation (first digit 1-9, not all same digit)
    if pin_code[0] == '0' or len(set(pin_code)) == 1:
        result.add_warning(f"⚠️ PIN code '{pin_code}' looks suspicious. Please verify it.")

    return pin_code


def _validate_coordinates(order, result: ValidationResult) -> Tuple[Optional[float], Optional[float]]:
    """
    Validates lat/lng coordinates are within India bounding box.
    Returns (lat, lng) if valid or correctable, else (None, None).
    """
    lat = order.latitude
    lng = order.longitude

    if lat is None or lng is None:
        result.add_warning(
            "⚠️ No GPS coordinates on this order. "
            "The robot will try to use the address text to find the delivery store. "
            "For best results, ask the customer to share their precise location."
        )
        return None, None

    try:
        lat = float(lat)
        lng = float(lng)
    except (TypeError, ValueError):
        result.add_error(f"❌ Invalid coordinate format: lat={order.latitude}, lng={order.longitude}")
        return None, None

    if not (INDIA_LAT_MIN <= lat <= INDIA_LAT_MAX):
        result.add_error(
            f"❌ Latitude {lat} is outside India ({INDIA_LAT_MIN}–{INDIA_LAT_MAX}). "
            "The customer's location pin is placed outside India."
        )
        return None, None

    if not (INDIA_LNG_MIN <= lng <= INDIA_LNG_MAX):
        result.add_error(
            f"❌ Longitude {lng} is outside India ({INDIA_LNG_MIN}–{INDIA_LNG_MAX}). "
            "The customer's location pin is placed outside India."
        )
        return None, None

    # Check for suspicious default/null-island coordinates
    if lat == 0.0 and lng == 0.0:
        result.add_error("❌ Coordinates are (0, 0) — this is the null island, not a real location.")
        return None, None

    if lat == 28.6139 and lng == 77.2090:
        result.add_warning("⚠️ Coordinates point to New Delhi default location. Please verify the customer's address.")

    return lat, lng


def _validate_phone(order, result: ValidationResult) -> Optional[str]:
    """Validates and cleans the customer phone number."""
    phone = order.phone or ""
    # Strip non-digits
    digits = re.sub(r'\D', '', phone)

    # Remove country code if present
    if digits.startswith('91') and len(digits) == 12:
        digits = digits[2:]
    elif digits.startswith('0') and len(digits) == 11:
        digits = digits[1:]

    if len(digits) != 10:
        result.add_warning(
            f"⚠️ Phone number '{phone}' is not a valid 10-digit Indian mobile. "
            "A default placeholder number will be used."
        )
        return None

    # Check valid Indian mobile prefixes (6–9)
    if digits[0] not in '6789':
        result.add_warning(
            f"⚠️ Phone '{digits}' doesn't start with 6-9. May not be a valid mobile number."
        )

    return digits


def _validate_cart_items(order, result: ValidationResult) -> List[dict]:
    """
    Validates order items and returns a cleaned item list.
    Each item: {name, dominos_name, qty}
    """
    items = getattr(order, 'items', None) or []

    if not items:
        result.add_error("❌ Order has no items in the cart. Cannot place an empty order.")
        return []

    cleaned_items = []
    unmapped = []

    for item in items:
        product = getattr(item, 'product', None)
        if not product:
            result.add_warning(f"⚠️ Cart item ID {item.id} has no product attached. Skipping.")
            continue

        name = product.name or ""
        if not name.strip():
            result.add_warning(f"⚠️ Product ID {product.id} has no name. Skipping.")
            continue

        qty = max(1, int(item.quantity or 1))
        dominos_name = PRODUCT_NAME_MAP.get(name, name)

        # Check if the dominos_name is recognizable
        if dominos_name.lower() not in KNOWN_DOMINOS_ITEMS:
            unmapped.append(f"'{name}' → '{dominos_name}'")

        cleaned_items.append({
            "internal_name": name,
            "dominos_name": dominos_name,
            "qty": qty,
            "unit_price": float(getattr(item, 'unit_price', 0) or 0),
        })

    if not cleaned_items:
        result.add_error("❌ No valid items found in cart after validation.")
        return []

    if unmapped:
        result.add_warning(
            f"⚠️ Some products don't have a confirmed Domino's menu mapping: {', '.join(unmapped)}. "
            "The robot will search for them by name — they may not be found on the menu."
        )

    return cleaned_items


def _validate_gift_card(order, result: ValidationResult):
    """Validates gift card if present on the order."""
    gc = getattr(order, 'gift_card', None)
    if not gc:
        return  # No gift card — fine

    try:
        from ..utils import decrypt_data
        code = decrypt_data(gc.code_encrypted)
        pin = decrypt_data(gc.pin_encrypted)
    except Exception as e:
        result.add_error(f"❌ Gift card decryption failed: {e}. Cannot apply gift card.")
        return

    # Code length check (Domino's uses 16-digit codes)
    code_digits = re.sub(r'\D', '', code)
    if len(code_digits) < 12:
        result.add_error(
            f"❌ Gift card code has only {len(code_digits)} digits. "
            "Domino's India gift cards are 16 digits."
        )

    # PIN length check (Domino's uses 6-digit PINs)
    pin_digits = re.sub(r'\D', '', pin)
    if len(pin_digits) < 4:
        result.add_error(
            f"❌ Gift card PIN has only {len(pin_digits)} digits. "
            "Domino's India gift card PINs are 6 digits."
        )

    if hasattr(gc, 'expires_at') and gc.expires_at:
        import datetime
        if gc.expires_at < datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None):
            result.add_error(f"❌ Gift card expired on {gc.expires_at.date()}. Cannot use it.")


def _validate_dominos_session(db: Session, result: ValidationResult):
    """Checks that at least one usable Domino's session exists."""
    from ..database import DominosSession
    active_sessions = db.query(DominosSession).filter(DominosSession.is_active == True).count()
    if active_sessions == 0:
        result.add_error(
            "❌ No active Domino's account sessions found. "
            "Go to Admin → Domino's Accounts → Add via OTP to add one."
        )


def _validate_order_total(order, items: List[dict], result: ValidationResult):
    """Sanity-checks the order total against computed item sum."""
    if not items:
        return

    computed = sum(i["unit_price"] * i["qty"] for i in items)
    stored = float(getattr(order, 'total_price', 0) or 0)

    if stored <= 0:
        result.add_warning("⚠️ Order total_price is ₹0 or not set.")
        return

    diff = abs(computed - stored)
    if diff > 100:
        result.add_warning(
            f"⚠️ Order total (₹{stored}) differs from computed item sum (₹{computed:.2f}) "
            f"by ₹{diff:.2f}. Verify pricing."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Main Validation Entry Point
# ─────────────────────────────────────────────────────────────────────────────

async def validate_order_for_robot(order, db: Session) -> ValidationResult:
    """
    Full pre-order validation. Runs ALL checks and returns a ValidationResult.
    Callers should check result.ok and raise OrderValidationError if False.

    This is deliberately NON-blocking: all validations run to completion
    so the admin gets a comprehensive list of issues, not just the first one.
    """
    result = ValidationResult()

    logger.info(f"[Validator] Starting pre-order validation for Order #{order.id}")

    # 1. Address
    pin_code = _validate_address(order, result)

    # 2. Coordinates
    lat, lng = _validate_coordinates(order, result)

    # 3. If neither coordinates nor pin code — that's a hard error
    if lat is None and lng is None and pin_code is None:
        result.add_error(
            "❌ CRITICAL: No usable location data. Order has no GPS coordinates AND no PIN code in address. "
            "The robot cannot determine which Domino's store to deliver from."
        )

    # 4. Phone
    phone = _validate_phone(order, result)

    # 5. Cart items
    items = _validate_cart_items(order, result)

    # 6. Gift card (if any)
    _validate_gift_card(order, result)

    # 7. Session availability
    _validate_dominos_session(db, result)

    # 8. Order total sanity
    _validate_order_total(order, items, result)

    # Log summary
    if result.ok:
        logger.info(
            f"[Validator] Order #{order.id} PASSED — "
            f"{len(items)} items, lat={lat}, lng={lng}, pin={pin_code}, "
            f"warnings={len(result.warnings)}"
        )
        if result.warnings:
            for w in result.warnings:
                logger.warning(f"[Validator] {w}")
    else:
        logger.error(
            f"[Validator] Order #{order.id} FAILED with {len(result.errors)} errors:\n"
            + "\n".join(f"  {e}" for e in result.errors)
        )

    return result


def format_validation_report(result: ValidationResult, order_id: int) -> str:
    """Formats a ValidationResult into a readable broadcast message."""
    lines = [f"📋 Pre-order check for Order #{order_id}:"]

    if result.ok:
        lines.append("✅ All checks passed!")
    else:
        lines.append(f"❌ {len(result.errors)} issue(s) found:")
        for e in result.errors:
            lines.append(f"  {e}")

    if result.warnings:
        lines.append(f"⚠️ {len(result.warnings)} warning(s):")
        for w in result.warnings:
            lines.append(f"  {w}")

    if result.corrected:
        lines.append("🔧 Auto-corrections applied:")
        for field_name, corr in result.corrected.items():
            lines.append(f"  {field_name}: '{corr['from']}' → '{corr['to']}' ({corr['reason']})")

    return "\n".join(lines)
