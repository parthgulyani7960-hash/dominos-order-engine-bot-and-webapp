"""Database layer – SQLAlchemy ORM models.

Design:
- UUID primary keys (String(36)) on all tables – portable across SQLite & PostgreSQL.
- Optimistic locking (version column) on User, Order, DominosSession.
- TimestampMixin – auto-managed created_at / updated_at.
- Composite indexes for all high-traffic query paths.
- Alembic-aware init_db(): only creates tables on a fresh DB with no alembic_version table.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import uuid

logger = logging.getLogger(__name__)

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Index, Integer, String, Text, JSON, event, inspect,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy import create_engine

# ---------------------------------------------------------------------------
# Engine configuration
# ---------------------------------------------------------------------------

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
os.makedirs(DATA_DIR, exist_ok=True)

DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{os.path.join(DATA_DIR, 'pizza.db')}",
)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

_IS_SQLITE: bool = DATABASE_URL.startswith("sqlite")
_connect_args: dict = {"check_same_thread": False, "timeout": 60} if _IS_SQLITE else {}

_IS_MEMORY_SQLITE: bool = DATABASE_URL == "sqlite:///:memory:"

_engine_kwargs: dict = dict(
    connect_args=_connect_args,
    pool_pre_ping=True,
    future=True,
)
if not _IS_MEMORY_SQLITE:
    # Memory SQLite uses SingletonThreadPool which rejects these args
    _engine_kwargs.update(
        pool_size=5 if _IS_SQLITE else 20,
        max_overflow=0 if _IS_SQLITE else 40,
        pool_timeout=60,
    )

engine = create_engine(DATABASE_URL, **_engine_kwargs)

if _IS_SQLITE:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA cache_size=-65536")  # 64 MB
        cur.execute("PRAGMA temp_store=MEMORY")
        cur.execute("PRAGMA busy_timeout=60000")
        cur.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
Base = declarative_base()


# ---------------------------------------------------------------------------
# Helpers / Mixins
# ---------------------------------------------------------------------------

def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def _uuid_default() -> str:
    return str(uuid.uuid4())


class TimestampMixin:
    """Adds created_at and updated_at to any model."""
    created_at = Column(DateTime, default=_now, nullable=False)
    updated_at = Column(DateTime, default=_now, onupdate=_now, nullable=False)


import datetime


class User(TimestampMixin, Base):
    __tablename__ = "users"

    id             = Column(String(36), primary_key=True, default=_uuid_default)
    telegram_id    = Column(String, unique=True, index=True, nullable=False)
    username       = Column(String, nullable=True)
    display_name   = Column(String, nullable=True)
    photo_url      = Column(String, nullable=True)
    phone          = Column(String, nullable=True)
    wallet_balance = Column(Float, default=0.0, nullable=False)
    role           = Column(String, default="user", nullable=False)  # user | admin
    is_blocked     = Column(Boolean, default=False, nullable=False)
    city           = Column(String, nullable=True)
    state          = Column(String, nullable=True)
    latitude       = Column(Float, nullable=True)
    longitude      = Column(Float, nullable=True)
    bot_state      = Column(String, nullable=True)
    bot_cart       = Column(Text, nullable=True)
    telegram_verification_code = Column(String, nullable=True)
    telegram_verified = Column(Boolean, default=False, nullable=False)
    admin_expires_at = Column(DateTime, nullable=True)
    version        = Column(Integer, default=0, nullable=False)  # optimistic locking

    sessions        = relationship("UserSession",  back_populates="user", cascade="all, delete-orphan")
    orders          = relationship("Order",         back_populates="user")
    used_gift_cards = relationship("GiftCard",      back_populates="used_by_user",
                                    foreign_keys="[GiftCard.used_by_user_id]")
    saved_addresses = relationship("SavedAddress",  back_populates="user", cascade="all, delete-orphan")
    notifications   = relationship("Notification",  back_populates="user", cascade="all, delete-orphan")

    @property
    def address(self) -> str | None:
        if self.saved_addresses:
            default_addr = next((sa for sa in self.saved_addresses if sa.is_default), self.saved_addresses[0])
            return default_addr.full_address if default_addr else None
        return None

    __table_args__ = (
        Index("ix_users_role_created", "role", "created_at"),
        Index("ix_users_wallet", "wallet_balance"),
    )
    __mapper_args__ = {"version_id_col": version}


class UserSession(TimestampMixin, Base):
    __tablename__ = "sessions"

    id            = Column(String(36), primary_key=True, default=_uuid_default)
    user_id       = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    refresh_token = Column(String, index=True, nullable=False)
    ip_address    = Column(String, nullable=True)
    user_agent    = Column(String, nullable=True)
    last_active   = Column(DateTime, default=_now, nullable=False)
    is_active     = Column(Boolean, default=True, nullable=False)

    user = relationship("User", back_populates="sessions")

    __table_args__ = (
        Index("ix_sessions_user_active", "user_id", "is_active"),
        Index("ix_sessions_last_active", "last_active"),
    )


class SavedAddress(TimestampMixin, Base):
    __tablename__ = "saved_addresses"

    id           = Column(String(36), primary_key=True, default=_uuid_default)
    user_id      = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    label        = Column(String, nullable=False)  # Home | Work | Other
    full_address = Column(Text, nullable=False)
    landmark     = Column(String, nullable=True)
    city         = Column(String, nullable=True)
    state        = Column(String, nullable=True)
    pincode      = Column(String, nullable=True)
    latitude     = Column(Float, nullable=True)
    longitude    = Column(Float, nullable=True)
    is_default   = Column(Boolean, default=False)

    user = relationship("User", back_populates="saved_addresses")


class Product(TimestampMixin, Base):
    __tablename__ = "products"

    id               = Column(String(36), primary_key=True, default=_uuid_default)
    name             = Column(String, nullable=False)
    description      = Column(Text, nullable=True)
    category         = Column(String, nullable=False, index=True)
    is_veg           = Column(Boolean, default=True)
    original_price   = Column(Float, nullable=False)
    discounted_price = Column(Float, nullable=True)
    image_url        = Column(String, nullable=True)
    availability     = Column(Boolean, default=True, index=True)
    sort_order       = Column(Integer, default=0)
    is_popular       = Column(Boolean, default=False)
    is_recommended   = Column(Boolean, default=False)
    crust_options    = Column(Text, nullable=True)  # JSON string
    size_options     = Column(Text, nullable=True)  # JSON string

    __table_args__ = (
        Index("ix_products_category_avail", "category", "availability"),
        Index("ix_products_sort", "sort_order"),
    )


class LocationPricing(Base):
    """City-level price multiplier for location-based pricing."""
    __tablename__ = "location_pricing"

    id               = Column(String(36), primary_key=True, default=_uuid_default)
    city             = Column(String, unique=True, index=True, nullable=False)
    state            = Column(String, nullable=True)
    price_multiplier = Column(Float, default=1.0)
    delivery_charge  = Column(Float, default=30.0)
    min_order_value  = Column(Float, default=149.0)
    is_serviceable   = Column(Boolean, default=True)


class Order(TimestampMixin, Base):
    __tablename__ = "orders"

    id                    = Column(String(36), primary_key=True, default=_uuid_default)
    user_id               = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    transaction_id        = Column(String, unique=True, index=True, nullable=False)
    payment_method        = Column(String, nullable=False)  # upi | wallet | cod
    original_total        = Column(Float, nullable=False)
    discount              = Column(Float, default=0.0)
    delivery_charge       = Column(Float, default=0.0)
    service_charge        = Column(Float, default=0.0)
    total_payable         = Column(Float, nullable=False)
    # Statuses: Payment Pending → Payment Received → Order Processing →
    # Preparing → Out for Delivery → Delivered → Completed | Cancelled
    status                = Column(String, default="Payment Pending", nullable=False, index=True)
    address               = Column(Text, nullable=True)
    landmark              = Column(String, nullable=True)
    city                  = Column(String, nullable=True)
    latitude              = Column(Float, nullable=True)
    longitude             = Column(Float, nullable=True)
    phone                 = Column(String, nullable=True)
    delivery_instructions = Column(Text, nullable=True)
    cancellation_reason   = Column(Text, nullable=True)
    estimated_delivery    = Column(DateTime, nullable=True)
    gift_card_id          = Column(String(36), ForeignKey("gift_cards.id"), nullable=True)
    coupon_applied        = Column(String, nullable=True)
    dominos_reference     = Column(String, nullable=True)
    device_id             = Column(String, nullable=True)
    device_details        = Column(Text, nullable=True)
    sector_store          = Column(String, nullable=True)
    screenshot_url        = Column(String, nullable=True)
    version               = Column(Integer, default=0, nullable=False)  # optimistic locking

    user           = relationship("User", back_populates="orders")
    gift_card      = relationship("GiftCard", foreign_keys="[Order.gift_card_id]")
    items          = relationship("OrderItem",          back_populates="order", cascade="all, delete-orphan")
    status_history = relationship("OrderStatusHistory", back_populates="order", cascade="all, delete-orphan")
    rider          = relationship("RiderAssignment",    back_populates="order", uselist=False, cascade="all, delete-orphan")
    notes          = relationship("OrderNote",          back_populates="order", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_orders_user_status",  "user_id", "status"),
        Index("ix_orders_status_time",  "status", "created_at"),
    )
    __mapper_args__ = {"version_id_col": version}


class OrderItem(Base):
    __tablename__ = "order_items"

    id         = Column(String(36), primary_key=True, default=_uuid_default)
    order_id   = Column(String(36), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    product_id = Column(String(36), ForeignKey("products.id"), nullable=False)
    quantity   = Column(Integer, nullable=False)
    price      = Column(Float, nullable=False)  # Price at time of purchase
    crust      = Column(String, nullable=True)
    size       = Column(String, nullable=True)

    order   = relationship("Order", back_populates="items")
    product = relationship("Product")


class OrderStatusHistory(Base):
    __tablename__ = "order_status_history"

    id         = Column(String(36), primary_key=True, default=_uuid_default)
    order_id   = Column(String(36), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    status     = Column(String, nullable=False)
    note       = Column(String, nullable=True)
    created_at = Column(DateTime, default=_now)

    order = relationship("Order", back_populates="status_history")


class RiderAssignment(Base):
    """Delivery rider assigned to an order."""
    __tablename__ = "rider_assignments"

    id             = Column(String(36), primary_key=True, default=_uuid_default)
    order_id       = Column(String(36), ForeignKey("orders.id", ondelete="CASCADE"), unique=True, nullable=False)
    rider_name     = Column(String, nullable=False)
    rider_phone    = Column(String, nullable=False)
    vehicle_number = Column(String, nullable=True)
    rider_lat      = Column(Float, nullable=True)
    rider_lng      = Column(Float, nullable=True)
    assigned_at    = Column(DateTime, default=_now)
    updated_at     = Column(DateTime, default=_now, onupdate=_now)

    order = relationship("Order", back_populates="rider")
class DominosSession(TimestampMixin, Base):
    __tablename__ = "dominos_sessions"

    id               = Column(String(36), primary_key=True, default=_uuid_default)
    mobile_number    = Column(String, nullable=True, index=True)
    is_active        = Column(Boolean, default=True, nullable=False)
    cookies          = Column(JSON, nullable=False)
    local_storage    = Column(JSON, nullable=True)
    expires_at       = Column(DateTime, nullable=True)
    last_verified_at = Column(DateTime, nullable=True)
    verify_status    = Column(String, nullable=True)  # valid | expired | error | unknown
    admin_id         = Column(String(36), ForeignKey("users.id"), nullable=True)
    version          = Column(Integer, default=0, nullable=False)  # optimistic locking

    max_orders_per_day   = Column(Integer, default=10, nullable=False)
    today_orders_count   = Column(Integer, default=0, nullable=False)
    last_order_placed_at = Column(DateTime, nullable=True)
    allowed_stores       = Column(String, nullable=True)
    assigned_admins      = Column(String, nullable=True)
    terms_accepted       = Column(Boolean, default=False, nullable=False)
    total_orders_placed  = Column(Integer, default=0, nullable=False)

    admin = relationship("User", backref="dominos_sessions")

    __table_args__ = (
        Index("ix_dominos_sessions_mobile_active", "mobile_number", "is_active"),
        Index("ix_dominos_sessions_expires", "expires_at"),
    )
    __mapper_args__ = {"version_id_col": version}


class DominosOTPRequest(Base):
    __tablename__ = "dominos_otp_requests"

    id            = Column(String(36), primary_key=True, default=_uuid_default)
    mobile_number = Column(String, nullable=False, index=True)
    request_token = Column(String, nullable=False, unique=True)
    otp           = Column(String, nullable=True)
    created_at    = Column(DateTime, default=_now)


class OrderNote(Base):
    """Admin notes on orders."""
    __tablename__ = "order_notes"

    id             = Column(String(36), primary_key=True, default=_uuid_default)
    order_id       = Column(String(36), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    admin_username = Column(String, nullable=True)
    note           = Column(Text, nullable=False)
    created_at     = Column(DateTime, default=_now)

    order = relationship("Order", back_populates="notes")


class GiftCard(TimestampMixin, Base):
    __tablename__ = "gift_cards"

    id               = Column(String(36), primary_key=True, default=_uuid_default)
    code_encrypted   = Column(String, nullable=False)
    code_hash        = Column(String, unique=True, index=True, nullable=False)
    pin_encrypted    = Column(String, nullable=False)
    value            = Column(Float, nullable=False)
    status           = Column(String, default="available")  # available | used
    used_by_user_id  = Column(String(36), ForeignKey("users.id"), nullable=True)
    # NOTE: used_in_order_id is stored as a plain string (not FK) to break the
    # circular dependency between gift_cards <-> orders.
    used_in_order_id = Column(String(36), nullable=True, index=True)
    used_at          = Column(DateTime, nullable=True)

    used_by_user = relationship("User", foreign_keys="[GiftCard.used_by_user_id]", back_populates="used_gift_cards")


class Notification(TimestampMixin, Base):
    """Persistent notifications for users."""
    __tablename__ = "notifications"

    id           = Column(String(36), primary_key=True, default=_uuid_default)
    user_id      = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    title        = Column(String, nullable=False)
    body         = Column(Text, nullable=False)
    type         = Column(String, default="info")  # order_update | promo | info | rider
    image_url    = Column(String, nullable=True)
    reference_id = Column(String, nullable=True)
    is_read      = Column(Boolean, default=False)

    user = relationship("User", back_populates="notifications")
    __table_args__ = (Index("ix_notifications_user_unread", "user_id", "is_read"),)


class SupportMessage(TimestampMixin, Base):
    __tablename__ = "support_messages"

    id                 = Column(String(36), primary_key=True, default=_uuid_default)
    user_id            = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    sender_type        = Column(String, nullable=False)  # user | admin
    message            = Column(Text, nullable=False)
    is_read            = Column(Boolean, default=False)
    attachment_file_id = Column(String, nullable=True)   # Telegram file_id for photo/document
    attachment_type    = Column(String, nullable=True)   # photo | document
    __table_args__ = (Index("ix_support_user_unread", "user_id", "is_read"),)



class AuditLog(Base):
    __tablename__ = "audit_logs"

    id             = Column(String(36), primary_key=True, default=_uuid_default)
    admin_id       = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    admin_username = Column(String, nullable=True)
    action         = Column(String, nullable=False, index=True)
    details        = Column(Text, nullable=True)
    ip_address     = Column(String, nullable=True)
    created_at     = Column(DateTime, default=_now)
    __table_args__ = (Index("ix_audit_logs_action_time", "action", "created_at"),)


class ErrorLog(Base):
    __tablename__ = "error_logs"

    id          = Column(String(36), primary_key=True, default=_uuid_default)
    type        = Column(String, nullable=False, index=True)
    message     = Column(Text, nullable=False)
    stack_trace = Column(Text, nullable=True)
    created_at  = Column(DateTime, default=_now)


class SystemConfig(Base):
    """Key/value config store. Key is the natural PK – no UUID needed."""
    __tablename__ = "system_configs"
    key   = Column(String, primary_key=True)
    value = Column(String, nullable=False)


class LoginAttempt(Base):
    __tablename__ = "login_attempts"
    id           = Column(String(36), primary_key=True, default=_uuid_default)
    username     = Column(String, nullable=False, index=True)
    ip_address   = Column(String, nullable=True)
    status       = Column(String, nullable=False)
    attempted_at = Column(DateTime, default=_now)
    __table_args__ = (Index("ix_login_attempts_user_time", "username", "attempted_at"),)


class VerifiedUTR(Base):
    __tablename__ = "verified_utrs"
    utr        = Column(String, primary_key=True)
    order_id   = Column(String(36), nullable=False, index=True)
    amount     = Column(Float, nullable=False)
    created_at = Column(DateTime, default=_now)


class UTRAttempt(Base):
    __tablename__ = "utr_attempts"
    id            = Column(String(36), primary_key=True, default=_uuid_default)
    order_id      = Column(String(36), index=True, nullable=False)
    utr           = Column(String, nullable=False)
    is_successful = Column(Boolean, default=False)
    attempted_at  = Column(DateTime, default=_now)


class Proxy(TimestampMixin, Base):
    __tablename__ = "proxies"
    id         = Column(String(36), primary_key=True, default=_uuid_default)
    ip         = Column(String, nullable=False)
    port       = Column(Integer, nullable=False)
    username   = Column(String, nullable=True)
    password   = Column(String, nullable=True)  # stored encrypted
    protocol   = Column(String, default="http")  # http | https | socks5
    is_active  = Column(Boolean, default=True)
    fail_count = Column(Integer, default=0)
    last_used  = Column(DateTime, nullable=True)


class ProxyLog(Base):
    __tablename__ = "proxy_logs"
    id         = Column(String(36), primary_key=True, default=_uuid_default)
    proxy_id   = Column(String(36), ForeignKey("proxies.id", ondelete="CASCADE"), nullable=False, index=True)
    order_id   = Column(String, nullable=True)
    action     = Column(String, nullable=False)  # test | order_submit
    status     = Column(String, nullable=False)  # success | failed
    details    = Column(Text, nullable=True)
    created_at = Column(DateTime, default=_now)


class QRGenerationHistory(Base):
    __tablename__ = "qr_generation_history"
    id          = Column(String(36), primary_key=True, default=_uuid_default)
    order_id    = Column(String, nullable=True)
    user_id     = Column(String(36), nullable=True)
    upi_uri     = Column(String, nullable=False)
    amount      = Column(Float, nullable=False)
    qr_code_url = Column(String, nullable=False)
    created_at  = Column(DateTime, default=_now)


class RobotLog(Base):
    """Logs every significant Playwright/robot automation event for admin visibility."""
    __tablename__ = "robot_logs"

    id            = Column(String(36), primary_key=True, default=_uuid_default)
    session_id    = Column(String(36), ForeignKey("dominos_sessions.id", ondelete="SET NULL"), nullable=True, index=True)
    mobile_number = Column(String, nullable=True)
    level         = Column(String, default="INFO")  # INFO | WARNING | ERROR
    stage         = Column(String, nullable=False)  # otp_request | browser_launch | otp_fill | session_save | order_submit | error
    message       = Column(Text, nullable=False)
    details       = Column(JSON, nullable=True)
    created_at    = Column(DateTime, default=_now)
    __table_args__ = (Index("ix_robot_logs_session_stage", "session_id", "stage"),)


class Coupon(TimestampMixin, Base):
    __tablename__ = "coupons"

    id             = Column(String(36), primary_key=True, default=_uuid_default)
    code           = Column(String, unique=True, index=True, nullable=False)
    value          = Column(Float, default=100.0)   # wallet top-up amount
    usage_limit    = Column(Integer, default=1)      # max total redemptions
    redeemed_count = Column(Integer, default=0)
    is_active      = Column(Boolean, default=True)


class CouponRedemption(Base):
    __tablename__ = "coupon_redemptions"

    id          = Column(String(36), primary_key=True, default=_uuid_default)
    coupon_id   = Column(String(36), ForeignKey("coupons.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id     = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    redeemed_at = Column(DateTime, default=_now)

    coupon = relationship("Coupon", backref="redemptions")
    user   = relationship("User",   backref="coupon_redemptions")
    __table_args__ = (Index("ix_coupon_redemptions_user_coupon", "user_id", "coupon_id"),)


class WalletTransaction(TimestampMixin, Base):
    __tablename__ = "wallet_transactions"

    id          = Column(String(36), primary_key=True, default=_uuid_default)
    user_id     = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    type        = Column(String, nullable=False)  # deposit | payment | refund | admin_adjustment
    amount      = Column(Float, nullable=False)
    description = Column(String, nullable=True)

    user = relationship("User", backref="wallet_transactions")


class WithdrawalRequest(TimestampMixin, Base):
    __tablename__ = "withdrawal_requests"

    id           = Column(String(36), primary_key=True, default=_uuid_default)
    user_id      = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    amount       = Column(Float, nullable=False)
    upi_id       = Column(String, nullable=False)
    status       = Column(String, default="Pending", nullable=False)  # Pending | Approved | Rejected
    admin_note   = Column(Text, nullable=True)
    processed_at = Column(DateTime, nullable=True)

    user = relationship("User", backref="withdrawal_requests")


    pass


# ---------------------------------------------------------------------------
# Database Persistence & Corruption Prevention System
# ---------------------------------------------------------------------------

PERSISTENT_BACKUP_PATH = os.path.join(DATA_DIR, "db_persistent_state.json")
PERSISTENT_FILE_ID_PATH = os.path.join(DATA_DIR, "latest_snapshot_file_id.txt")

def upload_snapshot_to_telegram_cloud(file_path: str) -> bool:
    """Saves persistent database snapshot quietly and pins it to Telegram Cloud Storage for guaranteed restore on container redeploys."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    backup_chat = os.getenv("SNAPSHOT_CHANNEL_ID", "").strip() or os.getenv("ADMIN_TELEGRAM_ID", "7958236048").strip()
    if not bot_token or not backup_chat or not os.path.exists(file_path):
        return True
    try:
        import urllib.request, uuid
        boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
        url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
        
        with open(file_path, "rb") as f:
            file_bytes = f.read()

        body = []
        body.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n{backup_chat}\r\n".encode("utf-8"))
        body.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"caption\"\r\n\r\n[AUTO_DB_SNAPSHOT_v1.1]\r\n".encode("utf-8"))
        body.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"disable_notification\"\r\n\r\ntrue\r\n".encode("utf-8"))
        filename = os.path.basename(file_path)
        body.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"document\"; filename=\"{filename}\"\r\nContent-Type: application/json\r\n\r\n".encode("utf-8"))
        body.append(file_bytes)
        body.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
        
        full_body = b"".join(body)
        req = urllib.request.Request(
            url,
            data=full_body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=12) as resp:
            res_data = json.loads(resp.read().decode("utf-8"))
            if res_data.get("ok"):
                msg_id = res_data.get("result", {}).get("message_id")
                file_id = res_data.get("result", {}).get("document", {}).get("file_id")
                if file_id:
                    try:
                        with open(PERSISTENT_FILE_ID_PATH, "w", encoding="utf-8") as file_id_out:
                            file_id_out.write(file_id)
                    except Exception:
                        pass
                
                # Pin the snapshot document in Telegram chat so getChat can ALWAYS retrieve it on fresh boots
                if msg_id and backup_chat:
                    try:
                        pin_url = f"https://api.telegram.org/bot{bot_token}/pinChatMessage"
                        pin_body = json.dumps({"chat_id": backup_chat, "message_id": msg_id, "disable_notification": True}).encode("utf-8")
                        pin_req = urllib.request.Request(pin_url, data=pin_body, headers={"Content-Type": "application/json"}, method="POST")
                        urllib.request.urlopen(pin_req, timeout=5)
                    except Exception as pe:
                        logger.warning(f"[PERSISTENCE] pinChatMessage failed: {pe}")

                logger.info("[PERSISTENCE] Successfully backed up DB snapshot quietly & pinned in Telegram Cloud Storage!")
                return True
    except Exception as e:
        logger.warning(f"[PERSISTENCE] Silent cloud backup failed: {e}")
    return False


def download_snapshot_from_telegram_cloud() -> dict | None:
    """Downloads the most recent persistent JSON snapshot from Telegram Cloud Storage."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    admin_id = os.getenv("SNAPSHOT_CHANNEL_ID", "").strip() or os.getenv("ADMIN_TELEGRAM_ID", "7958236048").strip()
    if not bot_token:
        return None
    try:
        import urllib.request
        target_file_id = None

        # 1. Try reading pinned message from Admin / Backup Chat
        if admin_id:
            try:
                chat_url = f"https://api.telegram.org/bot{bot_token}/getChat?chat_id={admin_id}"
                req = urllib.request.Request(chat_url, method="GET")
                with urllib.request.urlopen(req, timeout=8) as chat_resp:
                    chat_data = json.loads(chat_resp.read().decode("utf-8"))
                    if chat_data.get("ok"):
                        pinned = chat_data.get("result", {}).get("pinned_message", {})
                        doc = pinned.get("document")
                        caption = pinned.get("caption", "")
                        if doc and "[AUTO_DB_SNAPSHOT" in caption:
                            target_file_id = doc.get("file_id")
            except Exception as e:
                logger.warning(f"[PERSISTENCE] Failed getChat pinned_message lookup: {e}")

        # 2. Try saved local file_id fallback if pinned_message look up didn't yield file_id
        if not target_file_id and os.path.exists(PERSISTENT_FILE_ID_PATH):
            try:
                with open(PERSISTENT_FILE_ID_PATH, "r", encoding="utf-8") as f:
                    target_file_id = f.read().strip()
            except Exception:
                pass

        # 3. Fallback to getUpdates
        if not target_file_id:
            try:
                updates_url = f"https://api.telegram.org/bot{bot_token}/getUpdates?allowed_updates=[\"message\"]&limit=100"
                req = urllib.request.Request(updates_url, method="GET")
                with urllib.request.urlopen(req, timeout=8) as resp:
                    res_data = json.loads(resp.read().decode("utf-8"))
                    if res_data.get("ok"):
                        updates = res_data.get("result", [])
                        for up in reversed(updates):
                            msg = up.get("message", {})
                            caption = msg.get("caption", "")
                            doc = msg.get("document")
                            if doc and "[AUTO_DB_SNAPSHOT" in caption:
                                target_file_id = doc.get("file_id")
                                break
            except Exception:
                pass

        if not target_file_id:
            logger.warning("[PERSISTENCE] No pinned snapshot found on Telegram Cloud.")
            return None

        # Resolve Telegram file_path using file_id
        file_url = f"https://api.telegram.org/bot{bot_token}/getFile?file_id={target_file_id}"
        with urllib.request.urlopen(urllib.request.Request(file_url, method="GET"), timeout=8) as file_resp:
            file_info = json.loads(file_resp.read().decode("utf-8"))
            if not file_info.get("ok"):
                return None
            file_path = file_info.get("result", {}).get("file_path")
            if not file_path:
                return None

        # Download content from Telegram CDN
        dl_url = f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
        with urllib.request.urlopen(urllib.request.Request(dl_url, method="GET"), timeout=10) as dl_resp:
            content_str = dl_resp.read().decode("utf-8")
            snapshot_data = json.loads(content_str)
            logger.info(f"[PERSISTENCE] Downloaded latest DB snapshot from Telegram Cloud Storage (saved_at: {snapshot_data.get('saved_at')})!")
            return snapshot_data
    except Exception as e:
        logger.warning(f"[PERSISTENCE] Telegram Cloud backup download failed: {e}")
    return None


def auto_save_persistent_db_state(db=None) -> bool:
    """Saves non-volatile user balances, addresses, orders, wallet transactions, and coupons to a persistent JSON snapshot."""
    close_after = False
    if db is None:
        db = SessionLocal()
        close_after = True
    try:
        users = db.query(User).all()
        orders = db.query(Order).all()
        saved_addresses = db.query(SavedAddress).all()
        txs = db.query(WalletTransaction).all()
        coupons = db.query(Coupon).all()
        
        users_data = []
        for u in users:
            users_data.append({
                "id": u.id,
                "telegram_id": u.telegram_id,
                "username": u.username,
                "display_name": u.display_name,
                "phone": u.phone,
                "city": u.city,
                "address": u.saved_addresses[0].full_address if u.saved_addresses else None,
                "latitude": float(u.latitude) if u.latitude is not None else None,
                "longitude": float(u.longitude) if u.longitude is not None else None,
                "wallet_balance": float(u.wallet_balance or 0.0),
                "is_admin": (u.role == "admin"),
                "bot_state": u.bot_state,
                "bot_cart": u.bot_cart,
                "telegram_verified": bool(u.telegram_verified),
                "created_at": u.created_at.isoformat() if u.created_at else None,
            })

        addresses_data = []
        for sa in saved_addresses:
            addresses_data.append({
                "id": sa.id,
                "user_id": sa.user_id,
                "label": sa.label,
                "full_address": sa.full_address,
                "latitude": float(sa.latitude) if sa.latitude is not None else None,
                "longitude": float(sa.longitude) if sa.longitude is not None else None,
                "city": sa.city,
                "is_default": bool(sa.is_default),
            })

        orders_data = []
        for o in orders:
            orders_data.append({
                "id": o.id,
                "user_id": o.user_id,
                "total_payable": float(o.total_payable or 0.0),
                "status": o.status,
                "payment_method": o.payment_method,
                "address": o.address,
                "phone": o.phone,
                "dominos_reference": getattr(o, "dominos_reference", None),
                "transaction_id": o.transaction_id,
                "created_at": o.created_at.isoformat() if o.created_at else None,
            })

        tx_data = []
        for t in txs:
            tx_data.append({
                "id": t.id,
                "user_id": t.user_id,
                "type": t.type,
                "amount": float(t.amount or 0.0),
                "description": t.description,
                "created_at": t.created_at.isoformat() if t.created_at else None,
            })

        coupons_data = []
        for c in coupons:
            coupons_data.append({
                "id": c.id,
                "code": c.code,
                "value": float(c.value or 0.0),
                "usage_limit": c.usage_limit,
                "redeemed_count": c.redeemed_count,
                "is_active": bool(c.is_active),
            })

        data = {
            "version": 1.1,
            "saved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "users": users_data,
            "saved_addresses": addresses_data,
            "orders": orders_data,
            "wallet_transactions": tx_data,
            "coupons": coupons_data,
        }

        with open(PERSISTENT_BACKUP_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            
        # Also upload off-instance snapshot to Telegram Cloud
        upload_snapshot_to_telegram_cloud(PERSISTENT_BACKUP_PATH)
        return True
    except Exception as e:
        logger.error(f"[PERSISTENCE] Failed to save DB snapshot: {e}")
        return False
    finally:
        if close_after:
            db.close()


def auto_restore_persistent_db_state(db) -> bool:
    """Restores user accounts, wallet balances, saved addresses, orders, transactions, and coupons if the database file was reset or replaced on deployment."""
    data = download_snapshot_from_telegram_cloud()
    if not data and os.path.exists(PERSISTENT_BACKUP_PATH):
        try:
            with open(PERSISTENT_BACKUP_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"[PERSISTENCE] Error reading local backup file: {e}")
            
    if not data:
        return False
    try:

        restored_users = 0
        for u_data in data.get("users", []):
            tg_id = str(u_data.get("telegram_id", ""))
            existing = db.query(User).filter(
                (User.telegram_id == tg_id) | (User.id == u_data["id"])
            ).first()
            if not existing:
                u = User(
                    id=u_data["id"],
                    telegram_id=tg_id,
                    username=u_data.get("username"),
                    display_name=u_data.get("display_name"),
                    phone=u_data.get("phone"),
                    city=u_data.get("city", "India"),
                    address=u_data.get("address"),
                    latitude=u_data.get("latitude"),
                    longitude=u_data.get("longitude"),
                    wallet_balance=float(u_data.get("wallet_balance", 0.0)),
                    role="admin" if bool(u_data.get("is_admin", False)) else "user",
                    bot_state=u_data.get("bot_state"),
                    bot_cart=u_data.get("bot_cart"),
                    telegram_verified=bool(u_data.get("telegram_verified", False))
                )
                db.add(u)
                restored_users += 1
            else:
                # Synchronize details if higher or missing in existing record
                b_bal = float(u_data.get("wallet_balance", 0.0))
                if b_bal > existing.wallet_balance:
                    existing.wallet_balance = b_bal
                if not existing.address and u_data.get("address"):
                    existing.address = u_data.get("address")
                if not existing.phone and u_data.get("phone"):
                    existing.phone = u_data.get("phone")
                if existing.latitude is None and u_data.get("latitude") is not None:
                    existing.latitude = u_data.get("latitude")
                    existing.longitude = u_data.get("longitude")

        restored_addrs = 0
        for sa_data in data.get("saved_addresses", []):
            existing_sa = db.query(SavedAddress).filter(SavedAddress.id == sa_data["id"]).first()
            if not existing_sa:
                sa = SavedAddress(
                    id=sa_data["id"],
                    user_id=sa_data["user_id"],
                    label=sa_data.get("label", "Home"),
                    full_address=sa_data.get("full_address"),
                    latitude=sa_data.get("latitude"),
                    longitude=sa_data.get("longitude"),
                    city=sa_data.get("city"),
                    is_default=bool(sa_data.get("is_default", True))
                )
                db.add(sa)
                restored_addrs += 1

        restored_orders = 0
        for o_data in data.get("orders", []):
            existing_order = db.query(Order).filter(Order.id == o_data["id"]).first()
            if not existing_order:
                o = Order(
                    id=o_data["id"],
                    user_id=o_data["user_id"],
                    original_total=float(o_data.get("original_total") or o_data.get("total_payable", 0.0)),
                    total_payable=float(o_data.get("total_payable", 0.0)),
                    status=o_data.get("status", "Pending"),
                    payment_method=o_data.get("payment_method", "wallet"),
                    address=o_data.get("address"),
                    phone=o_data.get("phone"),
                    dominos_reference=o_data.get("dominos_reference") or o_data.get("dominos_order_id"),
                    transaction_id=o_data.get("transaction_id", f"TXN-{uuid.uuid4().hex[:10].upper()}")
                )
                db.add(o)
                restored_orders += 1

        for tx_rec in data.get("wallet_transactions", []):
            existing_tx = db.query(WalletTransaction).filter(WalletTransaction.id == tx_rec["id"]).first()
            if not existing_tx:
                t = WalletTransaction(
                    id=tx_rec["id"],
                    user_id=tx_rec["user_id"],
                    type=tx_rec.get("type", "topup"),
                    amount=float(tx_rec.get("amount", 0.0)),
                    description=tx_rec.get("description")
                )
                db.add(t)

        for c_rec in data.get("coupons", []):
            existing_c = db.query(Coupon).filter(Coupon.id == c_rec["id"]).first()
            if not existing_c:
                c = Coupon(
                    id=c_rec["id"],
                    code=c_rec["code"],
                    value=float(c_rec.get("value", 0.0)),
                    usage_limit=c_rec.get("usage_limit", 1),
                    redeemed_count=c_rec.get("redeemed_count", 0),
                    is_active=bool(c_rec.get("is_active", True))
                )
                db.add(c)

        db.commit()
        if restored_users > 0 or restored_orders > 0 or restored_addrs > 0:
            logger.info(f"[PERSISTENCE] Restored {restored_users} users, {restored_addrs} addresses & {restored_orders} orders from persistent backup state!")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"[PERSISTENCE] Error restoring persistent state: {e}")
        return False


# ---------------------------------------------------------------------------
# Database initialization (Alembic-aware & Corruption Proof)
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Create all tables, auto-repair corruption, and auto-restore user records on startup."""
    from sqlalchemy import text
    if _IS_SQLITE:
        try:
            with engine.connect() as conn:
                res = conn.execute(text("PRAGMA integrity_check")).fetchone()
                if res and res[0] != "ok":
                    logger.error(f"[DB INTEGRITY REPAIR] Corruption detected ({res[0]}). Executing VACUUM & REINDEX...")
                    conn.execute(text("PRAGMA journal_mode=DELETE;"))
                    conn.execute(text("VACUUM;"))
                    conn.execute(text("REINDEX;"))
                    conn.execute(text("PRAGMA journal_mode=WAL;"))
        except Exception as e:
            logger.error(f"[DB INTEGRITY CHECK ERROR] {e}")

    # Always call create_all to ensure any missing tables for defined models are created.
    # Safe to call as it does CREATE TABLE IF NOT EXISTS.
    Base.metadata.create_all(bind=engine)
        
    # Ensure newer columns are added if they don't exist (for existing databases)
    with engine.begin() as conn:
        insp = inspect(engine)
        # User verification & expiry columns
        user_cols = [c["name"] for c in insp.get_columns("users")]
        if "telegram_verification_code" not in user_cols:
            conn.execute(text("ALTER TABLE users ADD COLUMN telegram_verification_code VARCHAR"))
        if "telegram_verified" not in user_cols:
            conn.execute(text("ALTER TABLE users ADD COLUMN telegram_verified BOOLEAN DEFAULT 0"))
        if "admin_expires_at" not in user_cols:
            conn.execute(text("ALTER TABLE users ADD COLUMN admin_expires_at DATETIME"))

        # Order telemetry columns
        order_cols = [c["name"] for c in insp.get_columns("orders")]
        if "device_id" not in order_cols:
            conn.execute(text("ALTER TABLE orders ADD COLUMN device_id VARCHAR"))
        if "device_details" not in order_cols:
            conn.execute(text("ALTER TABLE orders ADD COLUMN device_details TEXT"))
        if "sector_store" not in order_cols:
            conn.execute(text("ALTER TABLE orders ADD COLUMN sector_store VARCHAR"))
            conn.execute(text("ALTER TABLE orders ADD COLUMN screenshot_url VARCHAR"))
            
        # Support messages columns
        support_cols = [c["name"] for c in insp.get_columns("support_messages")]
        if "attachment_file_id" not in support_cols:
            conn.execute(text("ALTER TABLE support_messages ADD COLUMN attachment_file_id VARCHAR"))
        if "attachment_type" not in support_cols:
            conn.execute(text("ALTER TABLE support_messages ADD COLUMN attachment_type VARCHAR"))

        # Create withdrawal_requests table if not exists
        if not insp.has_table("withdrawal_requests"):
            conn.execute(text("""
                CREATE TABLE withdrawal_requests (
                    id VARCHAR(36) PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    amount FLOAT NOT NULL,
                    upi_id VARCHAR NOT NULL,
                    status VARCHAR NOT NULL DEFAULT 'Pending',
                    admin_note TEXT,
                    processed_at DATETIME,
                    created_at DATETIME,
                    updated_at DATETIME,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
            """))

    # Auto-restore persistent state if new database instance
    db = SessionLocal()
    try:
        auto_restore_persistent_db_state(db)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Session dependency
# ---------------------------------------------------------------------------

def get_db():
    """FastAPI dependency – yields a synchronous DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

