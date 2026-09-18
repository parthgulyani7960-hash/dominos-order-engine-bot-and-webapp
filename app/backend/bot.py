from typing import Optional, List, Dict
import asyncio
import os
import sys

if sys.platform == 'win32':
    if 'unittest' not in sys.modules and 'pytest' not in sys.modules:
        try:
            import io
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
        except Exception:
            pass
import datetime
import uuid
import traceback
import httpx
import hashlib
import json
import html
from .utils import escape_html
from sqlalchemy.orm import Session
from .database import SessionLocal, User, SupportMessage, ActiveOffer, ErrorLog, SystemConfig, Product, Order, OrderItem, OrderStatusHistory, GiftCard, AuditLog, LocationPricing, Notification, UTRAttempt, VerifiedUTR, SavedAddress, Coupon, CouponRedemption, WalletTransaction, WithdrawalRequest, OrderNote, RiderAssignment, Proxy, ProxyLog, DominosSession, auto_save_persistent_db_state, auto_restore_persistent_db_state
try:
    from .database import UserSession, DominosSession
except ImportError:
    UserSession = None
    DominosSession = None
DbUser = User
DbOrder = Order
DbTxn = WalletTransaction
DbWithdrawal = WithdrawalRequest
from sqlalchemy import func as sql_func
from .utils import encrypt_data, decrypt_data, generate_upi_qr_details
import logging

logger = logging.getLogger(__name__)

# Rate limit and concurrency protection maps
USER_LAST_MSG_TIME = {}
USER_LAST_CB_TIME = {}
USER_PROCESSING_LOCKS = {}
USER_CALLBACK_TASKS = {}
USER_MSG_TIMESTAMPS = {}
USER_CB_TIMESTAMPS = {}
USER_LAST_WARNING_TIME = {}

# Telegram Inline Keyboard callback_data 64-byte limit safeguard
SHORT_CB_MAP: Dict[str, str] = {}

def unpack_cb_data(data: str) -> str:
    """Unpacks short callback token back to its original callback_data string."""
    if not data:
        return ""
    data_str = str(data)
    if data_str in SHORT_CB_MAP:
        return SHORT_CB_MAP[data_str]
    return data_str

def pack_cb_data(data: str) -> str:
    """Ensures callback_data never exceeds Telegram's 64-byte limit.
    If byte length exceeds 60 bytes, registers a short hash token and returns it.
    """
    if not data:
        return ""
    data_str = str(data)
    if len(data_str.encode('utf-8')) <= 60:
        return data_str
    h = hashlib.md5(data_str.encode('utf-8')).hexdigest()[:12]
    short_token = f"cb_h_{h}"
    SHORT_CB_MAP[short_token] = data_str
    return short_token

def sanitize_reply_markup(reply_markup: Optional[Dict]) -> Optional[Dict]:
    """Recursively inspects reply_markup and ensures all inline callback_data strings fit within 64 bytes."""
    if not reply_markup or not isinstance(reply_markup, dict):
        return reply_markup
    
    if "inline_keyboard" in reply_markup and isinstance(reply_markup["inline_keyboard"], list):
        new_keyboard = []
        for row in reply_markup["inline_keyboard"]:
            if not isinstance(row, list):
                continue
            new_row = []
            for btn in row:
                if isinstance(btn, dict) and "callback_data" in btn:
                    btn_copy = dict(btn)
                    btn_copy["callback_data"] = pack_cb_data(btn_copy["callback_data"])
                    new_row.append(btn_copy)
                else:
                    new_row.append(btn)
            new_keyboard.append(new_row)
        return {"inline_keyboard": new_keyboard}
    return reply_markup

def check_rate_limit(telegram_id: str, is_callback: bool = False) -> bool:
    """Returns True if rate limit is exceeded, False otherwise.
    
    Generous threshold: Max 200 callbacks or messages per 5 seconds.
    """
    import time
    now = time.time()
    user_key = str(telegram_id)
    timestamps_dict = USER_CB_TIMESTAMPS if is_callback else USER_MSG_TIMESTAMPS
    
    if user_key not in timestamps_dict:
        timestamps_dict[user_key] = []
        
    # Filter out timestamps older than 5 seconds
    timestamps_dict[user_key] = [t for t in timestamps_dict[user_key] if now - t < 5.0]
    
    limit = 200
    if len(timestamps_dict[user_key]) >= limit:
        return True
        
    timestamps_dict[user_key].append(now)
    return False


def load_env_file():
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                        v = v[1:-1]
                    if k not in os.environ:
                        os.environ[k] = v

load_env_file()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MINI_APP_URL = os.getenv("MINI_APP_URL", "https://dominos-order-engine-bot-and-webapp-1.onrender.com")

# Reference to the SSE broadcast callback (injected by main.py)
sse_broadcast_callback = None

# In-memory user sessions for shopping cart and flow state management
USER_BOT_SESSION = {}

# Shared HTTP client — tuned for high-throughput Telegram API calls.
# - Connection pool: up to 20 simultaneous connections to api.telegram.org
# - connect_timeout: fail fast if TCP handshake takes >5s (avoids long hangs)
# - read_timeout: 35s for long-polling getUpdates; shorter for send/edit calls
# - keepalive_expiry: reuse TLS sessions for 30s to avoid handshake overhead
_http_client = httpx.AsyncClient(
    timeout=httpx.Timeout(connect=5.0, read=35.0, write=10.0, pool=5.0),
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30),
    http2=False,  # Telegram API doesn't support HTTP/2; keep HTTP/1.1 for compatibility
)

# Separate fast client for send/edit/delete operations (shorter timeouts)
_fast_client = httpx.AsyncClient(
    timeout=httpx.Timeout(connect=4.0, read=8.0, write=8.0, pool=4.0),
    limits=httpx.Limits(max_connections=30, max_keepalive_connections=15, keepalive_expiry=30),
)

def html_escape(text: str) -> str:
    """Escapes HTML special characters for Telegram messages."""
    if not text:
        return ""
    return escape_html(str(text))

async def send_bot_typing(telegram_id: str):
    """Sends a 'typing...' status to Telegram — makes bot feel human while processing."""
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendChatAction"
        await _fast_client.post(url, json={"chat_id": str(telegram_id), "action": "typing"}, timeout=2.0)
    except Exception:
        pass


async def edit_bot_message_reply_markup(telegram_id: str, message_id: int, reply_markup: dict) -> bool:
    """Edits only the reply markup (inline keyboard) of an existing message without touching text or caption."""
    reply_markup = sanitize_reply_markup(reply_markup)
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN" or not message_id:
        return True
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup"
    payload = {
        "chat_id": telegram_id,
        "message_id": message_id,
        "reply_markup": reply_markup
    }
    try:
        resp = await _fast_client.post(url, json=payload)
        return resp.status_code == 200
    except Exception:
        return False


async def edit_bot_message(telegram_id: str, message_id: int, text: str, reply_markup: dict = None) -> bool:
    """Edits an existing text message on the user's screen (in-place text updates).
    Falls back to editMessageCaption if target message has media, or send_bot_message if edit fails.
    """
    text = text or ""
    reply_markup = sanitize_reply_markup(reply_markup)
    if not text and reply_markup:
        return await edit_bot_message_reply_markup(telegram_id, message_id, reply_markup)

    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT EDIT] Chat: {telegram_id}, Msg: {message_id}, Text: {text}")
        return True
        
    if not message_id:
        return await send_bot_message(telegram_id, text, reply_markup)

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id": telegram_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
        
    try:
        resp = await _fast_client.post(url, json=payload)
        if resp.status_code == 200:
            return True
            
        # Fallback to editing caption if message has media/animation/photo
        cap_url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageCaption"
        cap_text = text[:995] + "..." if len(text) > 1000 else text
        cap_payload = {
            "chat_id": telegram_id,
            "message_id": message_id,
            "caption": cap_text,
            "parse_mode": "HTML"
        }
        if reply_markup:
            cap_payload["reply_markup"] = reply_markup
            
        cap_resp = await _fast_client.post(cap_url, json=cap_payload)
        if cap_resp.status_code == 200:
            return True
    except Exception as e:
        logger.error(f"[edit_bot_message] Error editing msg {message_id}: {e}")
        
    # If in-place edit fails for any reason (e.g. message deleted, media change disallowed, text unchanged),
    # send a fresh message so the user's action always receives a response!
    try:
        await delete_bot_message(telegram_id, message_id)
    except Exception:
        pass
    return await send_bot_message(telegram_id, text, reply_markup)


async def delete_bot_message(telegram_id: str, message_id: int) -> bool:
    """Deletes an existing message from the user's screen."""
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT DELETE] Chat: {telegram_id}, Msg: {message_id}")
        return True
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage"
    payload = {
        "chat_id": telegram_id,
        "message_id": message_id
    }
    try:
        resp = await _fast_client.post(url, json=payload)
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"Error deleting bot message: {e}")
        return False


async def answer_callback_query(callback_query_id: str, text: str = None, show_alert: bool = False, alert: bool = False, url: str = None) -> bool:
    """Dismisses the loading spinner icon on the Telegram client button. If show_alert=True or alert=True, shows a popup alert instead of a toast."""
    if alert:
        show_alert = True
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        return True
    tg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    if show_alert:
        payload["show_alert"] = True
    if url:
        payload["url"] = url
    try:
        # Telegram API limits text to 200 characters for answerCallbackQuery
        if text and len(text) > 200:
            payload["text"] = text[:197] + "..."
            
        resp = await _fast_client.post(tg_url, json=payload, timeout=10.0)
        if resp.status_code != 200:
            logger.error(f"[answer_callback_query] Failed: {resp.text}")
            return False
        return True
    except Exception as e:
        logger.error(f"[answer_callback_query] Exception: {e}")
        return False

async def send_bot_message(telegram_id: str, text: str, reply_markup: dict = None) -> bool:
    """
    Sends a direct message to a Telegram user.
    Returns True if successful, False otherwise.
    Splits long messages (> 4000 characters) into smaller chunks safely.
    """
    reply_markup = sanitize_reply_markup(reply_markup)
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT NOTIFICATION] To {telegram_id}: {text}")
        return True
        
    # If the text is too long, split it recursively by line or chunk size
    if len(text) > 4000:
        lines = text.split("\n")
        chunks = []
        current_chunk = []
        current_length = 0
        for line in lines:
            if current_length + len(line) + 1 > 4000:
                if current_chunk:
                    chunks.append("\n".join(current_chunk))
                current_chunk = [line]
                current_length = len(line)
            else:
                current_chunk.append(line)
                current_length += len(line) + 1
        if current_chunk:
            chunks.append("\n".join(current_chunk))
            
        success = True
        for chunk in chunks:
            # Only attach reply_markup to the final chunk
            is_last = chunk == chunks[-1]
            markup = reply_markup if is_last else None
            res = await send_bot_message(telegram_id, chunk, markup)
            if not res:
                success = False
        return success

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": telegram_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
        
    try:
        resp = await _fast_client.post(url, json=payload)
        if resp.status_code == 200:
            res_data = resp.json()
            return res_data.get("result", {}).get("message_id", True)
        else:
            # Log notification failure
            db = SessionLocal()
            try:
                err = ErrorLog(
                    type="notification",
                    message=f"Failed to send bot message to {telegram_id}. Code: {resp.status_code}, Body: {resp.text}"
                )
                db.add(err)
                db.commit()
            except Exception:
                db.rollback()
            finally:
                db.close()
            return False
    except Exception as e:
        db = SessionLocal()
        try:
            err = ErrorLog(
                type="notification",
                message=f"Error sending bot message: {str(e)}",
                stack_trace=traceback.format_exc()
            )
            db.add(err)
            db.commit()
        except Exception:
            db.rollback()
        finally:
            db.close()
        return False

async def notify_admins(db: Session, text: str, reply_markup: dict = None) -> bool:
    """Sends a notification message to the primary ADMIN_TELEGRAM_ID and all DB admins."""
    admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
    targets = {admin_tg_id}
    try:
        admins = db.query(User).filter(User.role == 'admin').all()
        for admin in admins:
            if admin.telegram_id:
                targets.add(admin.telegram_id)
    except Exception:
        pass
    
    success = True
    for tg_id in targets:
        if tg_id:
            res = await send_bot_message(tg_id, text, reply_markup)
            if not res:
                success = False
async def send_admin_user_details(telegram_id: str, target_user_id: str, db: Session, message_id: int = None):
    target_user = db.query(User).filter(User.id == target_user_id).first()
    if not target_user:
        await send_bot_message(telegram_id, "❌ User not found!")
        return
        
    status_text = "🚫 BLOCKED (Suspended)" if target_user.is_blocked else "🟢 ACTIVE"
    expiry_text = target_user.admin_expires_at.strftime("%d-%m-%Y %I:%M %p UTC") if target_user.admin_expires_at else "Permanent / N/A"
    
    # Advanced stats queries
    orders_count = db.query(Order).filter(Order.user_id == target_user.id).count()
    saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == target_user.id).first()
    address_disp = saved_addr.full_address if (saved_addr and saved_addr.full_address) else "—"
    
    gps_url = f"https://www.google.com/maps?q={target_user.latitude},{target_user.longitude}" if (target_user.latitude and target_user.longitude) else None
    gps_disp = f"<a href='{gps_url}'>🗺️ Click to View ({target_user.latitude:.6f}, {target_user.longitude:.6f})</a>" if gps_url else "—"
    
    # Fetch last 3 wallet transactions
    txs = db.query(WalletTransaction).filter(WalletTransaction.user_id == target_user.id).order_by(WalletTransaction.created_at.desc()).limit(3).all()
    txs_lines = []
    for t in txs:
        t_sign = "+" if t.amount >= 0 else ""
        txs_lines.append(f"  • {t.type.upper()}: {t_sign}₹{t.amount:.2f} ({t.created_at.strftime('%d-%m-%Y')})")
    txs_disp = "\n".join(txs_lines) if txs_lines else "  • No transactions yet"
    
    # Fetch last 3 orders
    last_orders = db.query(Order).filter(Order.user_id == target_user.id).order_by(Order.created_at.desc()).limit(3).all()
    orders_lines = []
    for o in last_orders:
        orders_lines.append(f"  • <code>{o.id}</code>: ₹{o.total_payable:.2f} ({o.status})")
    orders_disp = "\n".join(orders_lines) if orders_lines else "  • No orders placed yet"

    msg = (
        f"👤 <b>User Management Console</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"• <b>Display Name:</b> {target_user.display_name}\n"
        f"• <b>Telegram ID:</b> <code>{target_user.telegram_id}</code>\n"
        f"• <b>Username:</b> @{target_user.username or '—'}\n"
        f"• <b>Phone Number:</b> <code>{target_user.phone or '—'}</code>\n"
        f"• <b>City:</b> <code>{target_user.city or '—'}</code>\n"
        f"• <b>GPS Coordinates:</b> {gps_disp}\n"
        f"• <b>Delivery Address:</b> <i>{address_disp}</i>\n"
        f"• <b>Total Orders:</b> <code>{orders_count}</code>\n"
        f"• <b>Wallet Balance:</b> <b>₹{target_user.wallet_balance:.2f}</b>\n"
        f"• <b>Role:</b> <code>{target_user.role.upper()}</code>\n"
        f"• <b>Status:</b> <b>{status_text}</b>\n"
        f"• <b>Admin Expiration:</b> <code>{expiry_text}</code>\n\n"
        f"📈 <b>Recent Transactions:</b>\n{txs_disp}\n\n"
        f"📦 <b>Recent Orders:</b>\n{orders_disp}\n"
    )
    
    block_btn_text = "🟢 Unblock User" if target_user.is_blocked else "🚫 Block User"
    buttons = [
        [
            {"text": "💰 Adjust Balance", "callback_data": f"admin_user_wallet_{target_user.id}"},
            {"text": block_btn_text, "callback_data": f"admin_user_block_{target_user.id}"}
        ],
        [
            {"text": "👑 Change Role", "callback_data": f"admin_user_role_{target_user.id}"}
        ],
        [
            {"text": "📜 Wallet Logs", "callback_data": f"adm_u_txs_{target_user.id}_1"},
            {"text": "📦 Order History", "callback_data": f"adm_u_ord_{target_user.id}_1"}
        ],
        [
            {"text": "📍 Saved Addresses", "callback_data": f"adm_u_addrs_{target_user.id}"}
        ],
        [
            {"text": "💬 Send Message to User", "callback_data": f"admin_msg_user_{target_user.id}"}
        ],
        [
            {"text": "🔙 Back to Users List", "callback_data": "admin_manage_users"}
        ]
    ]
    
    if message_id:
        await edit_bot_message(telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
    else:
        await send_bot_message(telegram_id, msg, reply_markup={"inline_keyboard": buttons})


def render_admin_order_notification_card(db: Session, order: Order, action_mode: str = "review") -> tuple[str, dict]:
    """
    Renders an ultra-rich, beautifully formatted Admin Order Card & Markup.
    Supports modes:
      - 'review': For new order notification / pending admin verification
      - 'approved': Shows final approved state summary
      - 'rejected': Shows final rejected/cancelled state summary
    """
    user = order.user
    user_disp = escape_html(user.display_name) if (user and user.display_name) else f"User {order.user_id}"
    user_tg = user.telegram_id if (user and user.telegram_id) else "N/A"
    user_uname = f"@{user.username}" if (user and user.username) else "No Username"
    user_wallet = user.wallet_balance if user else 0.0

    customer_phone = order.phone or (user.phone if user else None) or "Not provided"
    phone_link = f"<a href='tel:{customer_phone}'>{customer_phone}</a>" if customer_phone != "Not provided" else "Not provided"

    # Items breakdown
    item_lines = []
    if order.items:
        for item in order.items:
            p_name = item.item_name or (item.product.name if item.product else "Pizza Item")
            qty = item.quantity or 1
            unit_price = item.price or 0.0
            line_total = unit_price * qty
            is_veg = getattr(item.product, "is_veg", True) if item.product else True
            veg_dot = "🟢" if is_veg else "🔴"
            
            extra = []
            if getattr(item, "size", None): extra.append(item.size)
            if getattr(item, "crust", None): extra.append(item.crust)
            extra_str = f" (<i>{', '.join(extra)}</i>)" if extra else ""

            item_lines.append(f"  {veg_dot} <code>{qty}x</code> <b>{escape_html(p_name)}</b>{extra_str}\n     {qty} × ₹{unit_price:.0f}  —  <b>₹{line_total:.2f}</b>")
            if getattr(item, "item_details", None):
                for sub in item.item_details.split("\n"):
                    if sub.strip():
                        item_lines.append(f"     └ <i>{escape_html(sub.strip())}</i>")
    items_summary = "\n".join(item_lines) if item_lines else "  • <i>No items recorded</i>"

    # Pricing audit
    subtotal = sum((it.price or 0.0) * (it.quantity or 1) for it in (order.items or []))
    if subtotal <= 0:
        subtotal = order.total_payable

    bot_fee = get_bot_fee(db)
    discount_val = getattr(order, "discount_amount", 0.0) or 0.0
    payment_method = (order.payment_method or "upi").upper()
    utr_val = order.transaction_id or "Not Submitted"

    wallet_applied = getattr(order, "wallet_applied", 0.0) or 0.0
    if payment_method == "WALLET" and wallet_applied == 0.0:
        wallet_applied = order.total_payable

    upi_payable = max(0.0, order.total_payable - wallet_applied)

    # Google Maps URL
    import urllib.parse
    lat = user.latitude if user else None
    lon = user.longitude if user else None
    if lat and lon:
        maps_url = f"https://www.google.com/maps?q={lat},{lon}"
    else:
        clean_addr = urllib.parse.quote(order.address or "India")
        maps_url = f"https://www.google.com/maps/search/?api=1&query={clean_addr}"

    # Wallet Balance & Risk Assessment
    wallet_warning = ""
    if wallet_applied > 0:
        if user_wallet < wallet_applied:
            wallet_warning = (
                f"\n⚠️ <b>INSUFFICIENT WALLET BALANCE ALERT:</b>\n"
                f"   • User Current Wallet: <b>₹{user_wallet:.2f}</b>\n"
                f"   • Required Wallet Payment: <b>₹{wallet_applied:.2f}</b>\n"
                f"   • <b>Shortfall: ₹{wallet_applied - user_wallet:.2f}</b>\n"
                f"   <i>(Approval will fail/require user top-up)</i>\n"
            )
        else:
            wallet_warning = f"   • Wallet Check: 🟢 Sufficient (Bal: ₹{user_wallet:.2f})\n"

    is_topup = order.id.startswith("TOPUP-")
    # Headers and Statuses
    if action_mode == "approved":
        header_title = f"✅ <b>{'DEPOSIT APPROVED' if is_topup else 'ORDER APPROVED & DISPATCHED BY ADMIN'}</b>"
        status_badge = f"🟢 {'Deposit Approved' if is_topup else 'Order Processing / Dispatched'}"
    elif action_mode == "rejected":
        header_title = f"❌ <b>{'DEPOSIT REJECTED' if is_topup else 'ORDER REJECTED & CANCELLED BY ADMIN'}</b>"
        status_badge = f"🔴 {'Deposit Rejected' if is_topup else 'Cancelled / Refunded'}"
    else:
        header_title = f"🔔 <b>{'NEW WALLET DEPOSIT REQUEST' if is_topup else 'NEW PIZZA ORDER FOR ADMIN APPROVAL'}</b>"
        status_badge = f"⏳ {order.status}"

    created_time = order.created_at.strftime("%d-%m-%Y %I:%M %p IST") if order.created_at else "Now"

    card_text = (
        f"{header_title}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <b>{'Deposit ID' if is_topup else 'Order ID'}:</b> <code>{order.id}</code>\n"
        f"📅 <b>Placed At:</b> <code>{created_time}</code>\n"
        f"🏷️ <b>Current Status:</b> <b>{status_badge}</b>\n\n"
        f"👤 <b>CUSTOMER PROFILE:</b>\n"
        f"   • <b>Name:</b> <b>{user_disp}</b> ({user_uname})\n"
        f"   • <b>Telegram ID:</b> <code>{user_tg}</code>\n"
        f"   • <b>Wallet Balance:</b> <b>₹{user_wallet:.2f}</b>\n"
        f"   • <b>Contact Phone:</b> {phone_link}\n\n"
        + (f"🛒 <b>ORDERED ITEMS:</b>\n{items_summary}\n\n" if not is_topup else "")
        + (f"📝 <b>Delivery Instructions:</b> <i>{escape_html(order.delivery_instructions)}</i>\n\n" if order.delivery_instructions and not is_topup else "") +
        f"💰 <b>FINANCIAL SUMMARY:</b>\n"
        f"   • Subtotal: ₹{subtotal:.2f}\n"
        + (f"   • Service Fee: ₹{bot_fee:.2f}\n" if bot_fee > 0 and not is_topup else "")
        + (f"   • Promo Discount: -₹{discount_val:.2f}\n" if discount_val > 0 and not is_topup else "") +
        f"   • {'Deposit Amount' if is_topup else 'Grand Total'}: <b>₹{order.total_payable:.2f}</b>\n"
        f"   • Payment Method: <b>{payment_method}</b>\n"
        + (f"   • Wallet Deducted: ₹{wallet_applied:.2f}\n" if wallet_applied > 0 else "")
        + (f"   • UPI/Cash Due: ₹{upi_payable:.2f}\n" if upi_payable > 0 else "") +
        f"   • Transaction / UTR Ref: <code>{utr_val}</code>\n"
        + wallet_warning +
        (f"\n🏡 <b>DELIVERY ADDRESS:</b>\n<code>{order.address or 'Address pending'}</code>\n\n" if not is_topup else "\n") +
        (f"🏪 <b>FULFILLMENT DATA:</b>\n"
        f"   • Sector Store: <code>{order.sector_store or 'Pending'}</code>\n"
        f"   • Domino's Ref: <code>{order.dominos_reference or 'Pending'}</code>\n" if not is_topup else "")
    )

    # Keyboards
    if action_mode == "approved" or action_mode == "rejected":
        markup = {
            "inline_keyboard": [
                [{"text": "📍 Open Google Maps Location", "url": maps_url}],
                [{"text": "🔍 Detailed View", "callback_data": f"admin_order_detail_{order.id}"}]
            ]
        }
    else:
        markup = {
            "inline_keyboard": [
                [{"text": "📍 Open Google Maps Location", "url": maps_url}],
                [
                    {"text": f"✅ Approve {'Deposit' if is_topup else '& Dispatch Order'}", "callback_data": f"admin_dep_approve_{order.id}" if is_topup else f"admin_approve_direct_order_{order.id}"},
                    {"text": f"❌ Reject {'Deposit' if is_topup else '& Refund'}", "callback_data": f"admin_dep_reject_{order.id}" if is_topup else f"admin_reject_direct_order_{order.id}"}
                ],
                [
                    {"text": "💬 Reply to Customer", "callback_data": f"admin_reply_support_{user_tg}"}
                ] + ([{"text": "✏️ Domino's Ref", "callback_data": f"admin_edit_ref_{order.id}"}] if not is_topup else []),
                [
                    {"text": "💬 Tpl: Delay", "callback_data": f"admin_tpl_delay_{order.id}"},
                    {"text": "💬 Tpl: Pay Pending", "callback_data": f"admin_tpl_nopay_{order.id}"}
                ]
            ]
        }

    return card_text, markup


async def send_admin_order_details(telegram_id: str, order_id: str, db: Session, message_id: int = None):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        await send_bot_message(telegram_id, "❌ Order not found!")
        return
        
    card_text, _ = render_admin_order_notification_card(db, order, action_mode="review")

    rider_name = order.rider.rider_name if order.rider else "None"
    rider_phone = order.rider.rider_phone if order.rider else "None"
    rider_phone_link = f"<a href='tel:{rider_phone}'>{rider_phone}</a>" if (rider_phone and rider_phone != "None") else "None"

    import urllib.parse
    lat = order.user.latitude if order.user else None
    lon = order.user.longitude if order.user else None
    if lat and lon:
        maps_url = f"https://www.google.com/maps?q={lat},{lon}"
    else:
        clean_addr = urllib.parse.quote(order.address or (order.user.address if order.user else "India"))
        maps_url = f"https://www.google.com/maps/search/?api=1&query={clean_addr}"

    is_topup = order.id.startswith("TOPUP-")
    buttons = [
        [
            {"text": "📍 Open Google Maps Location", "url": maps_url}
        ],
        [
            {"text": f"✅ Approve {'Deposit' if is_topup else '& Dispatch'}", "callback_data": f"admin_dep_approve_{order.id}" if is_topup else f"admin_approve_direct_order_{order.id}"},
            {"text": f"❌ Reject {'Deposit' if is_topup else '& Refund'}", "callback_data": f"admin_dep_reject_{order.id}" if is_topup else f"admin_reject_direct_order_{order.id}"}
        ]
    ]
    if not is_topup:
        buttons.extend([
            [
                {"text": "✏️ Domino's Ref", "callback_data": f"admin_edit_ref_{order.id}"},
                {"text": "✏️ Sector Store", "callback_data": f"admin_edit_store_{order.id}"}
            ],
            [
                {"text": "✏️ Rider Name", "callback_data": f"admin_edit_rider_name_{order.id}"},
                {"text": "✏️ Rider Phone", "callback_data": f"admin_edit_rider_phone_{order.id}"}
            ],
            [
                {"text": "🖼️ Attach Screenshot", "callback_data": f"admin_order_attach_sc_{order.id}"},
                {"text": "✂️ Partial Item Cancel", "callback_data": f"admin_item_cancel_menu_{order.id}"}
            ]
        ])
    else:
        buttons.append([
            {"text": "🖼️ Attach Screenshot", "callback_data": f"admin_order_attach_sc_{order.id}"}
        ])

    buttons.extend([
        [
            {"text": "🔄 Change Status", "callback_data": f"admin_change_status_menu_{order.id}"},
            {"text": "🔙 Back", "callback_data": "admin_view_pending_deposits" if is_topup else "admin_view_pending_orders"}
        ],
        [
            {"text": "💬 Tpl: Delay", "callback_data": f"admin_tpl_delay_{order.id}"},
            {"text": "💬 Tpl: Pay Pending", "callback_data": f"admin_tpl_nopay_{order.id}"}
        ]
    ])
    
    if order.screenshot_url:
        buttons.insert(4, [
            {"text": "👁️ View Screenshot", "callback_data": f"admin_order_view_sc_{order.id}"},
            {"text": "🗑️ Delete Screenshot", "callback_data": f"admin_order_del_sc_{order.id}"}
        ])
        
    if message_id:
        await edit_bot_message(telegram_id, message_id, card_text, reply_markup={"inline_keyboard": buttons})
    else:
        await send_bot_message(telegram_id, card_text, reply_markup={"inline_keyboard": buttons})


async def broadcast_config_change_to_admins(admin_tg_id: str, parameter: str, old_val: str, new_val: str, db: Session):
    admins = db.query(DbUser).filter(DbUser.role == "admin").all()
    msg = (
        f"📢 <b>System Configuration Modification Alert</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"An administrator has modified a platform parameter:\n\n"
        f"• <b>Parameter:</b> <code>{parameter}</code>\n"
        f"• <b>Old Value:</b> <code>{old_val}</code>\n"
        f"• <b>New Value:</b> <code>{new_val}</code>\n\n"
        f"Modified by Admin TG ID: <code>{admin_tg_id}</code>"
    )
    for a in admins:
        if str(a.telegram_id) != str(admin_tg_id):
            try:
                await send_bot_message(a.telegram_id, msg)
            except Exception:
                pass


def render_wallet_view(db: Session, user: User, offset: int = 0, limit: int = 5):
    """Renders the clean My Wallet overview card without transaction history spam."""
    wallet_text = (
        f"💰 <b>My Wallet Overview</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Account: <b>{escape_html(user.display_name)}</b>\n"
        f"💵 Available Balance: <b>₹{user.wallet_balance:.2f}</b>\n\n"
        f"💡 <i>Tap <b>Add Funds</b> to top-up your balance or <b>Transaction History</b> to view past activity.</i>"
    )

    inline_buttons = [
        [
            {"text": "💳 Add Funds", "callback_data": "wallet_add"},
            {"text": "🎫 Add Promo Code", "callback_data": "wallet_promo"}
        ],
        [
            {"text": "📜 View Transaction History", "callback_data": "wallet_tx_history_page_1"}
        ],
        [
            {"text": "🍕 View Menu & Order", "callback_data": "menu_view"}
        ]
    ]

    return wallet_text, {"inline_keyboard": inline_buttons}

async def send_bot_photo(telegram_id: str, photo_url: str, caption: str = None, reply_markup: dict = None) -> bool:
    """
    Sends a photo to a Telegram user with optional inline buttons.
    Supports: public URL, local file path, or telegram_file:<file_id> (direct file_id).
    Falls back to send_bot_message on failure.
    """
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT PHOTO] To {telegram_id}: Photo: {photo_url}, Caption: {caption}, ReplyMarkup: {reply_markup}")
        return True
        
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    
    import os
    
    target_path = photo_url
    if photo_url and "/uploads/" in photo_url:
        filename = photo_url.split("/uploads/")[-1].split("?")[0]
        possible_paths = [
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "uploads", filename)),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "uploads", filename)),
            os.path.abspath(os.path.join("uploads", filename)),
            os.path.abspath(os.path.join("app", "uploads", filename))
        ]
        for p in possible_paths:
            if os.path.exists(p):
                target_path = p
                break

    # Support telegram_file:<file_id> — pass file_id directly to Telegram API
    is_telegram_file = str(target_path).startswith("telegram_file:")
    is_local_file = (not is_telegram_file) and os.path.exists(target_path)
    
    try:
        if is_telegram_file:
            # Extract the raw Telegram file_id and send it directly
            file_id = photo_url.replace("telegram_file:", "", 1).strip()
            payload = {
                "chat_id": telegram_id,
                "photo": file_id,
            }
            if caption:
                payload["caption"] = caption
                payload["parse_mode"] = "HTML"
            if reply_markup:
                payload["reply_markup"] = reply_markup
            resp = await _http_client.post(url, json=payload, timeout=15.0)
        elif is_local_file:
            # Send as multipart/form-data
            data = {
                "chat_id": telegram_id,
            }
            if caption:
                data["caption"] = caption
                data["parse_mode"] = "HTML"
            if reply_markup:
                import json
                data["reply_markup"] = json.dumps(reply_markup)
                
            with open(target_path, "rb") as f:
                files = {
                    "photo": f
                }
                resp = await _http_client.post(url, data=data, files=files, timeout=20.0)
        else:
            # Send as json url link
            payload = {
                "chat_id": telegram_id,
                "photo": target_path
            }
            if caption:
                payload["caption"] = caption
                payload["parse_mode"] = "HTML"
            if reply_markup:
                payload["reply_markup"] = reply_markup
            resp = await _http_client.post(url, json=payload, timeout=15.0)
            
        if resp.status_code == 200:
            return True
        else:
            # Check if Telegram returned document file type error
            if "Document as Photo" in resp.text:
                try:
                    doc_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
                    doc_payload = {"chat_id": telegram_id, "document": file_id if is_telegram_file else target_path}
                    if caption: doc_payload["caption"] = caption; doc_payload["parse_mode"] = "HTML"
                    if reply_markup: doc_payload["reply_markup"] = reply_markup
                    doc_resp = await _http_client.post(doc_url, json=doc_payload, timeout=15.0)
                    if doc_resp.status_code == 200:
                        return True
                except Exception:
                    pass

            db = SessionLocal()
            try:
                err = ErrorLog(
                    type="notification",
                    message=f"Failed to send bot photo to {telegram_id}. Code: {resp.status_code}, Body: {resp.text}"
                )
                db.add(err)
                db.commit()
            except Exception:
                db.rollback()
            finally:
                db.close()
            
            # Fallback to plain text message
            logger.error(f"[BOT] Photo failed (Code {resp.status_code}). Falling back to text...")
            fallback_text = caption or "Domino's Order Engine Photo"
            if photo_url and photo_url.startswith("http"):
                fallback_text = f"{fallback_text}\n\n🔗 Image Link: {photo_url}"
            return await send_bot_message(telegram_id, fallback_text, reply_markup)
    except Exception as e:
        db = SessionLocal()
        try:
            err = ErrorLog(
                type="notification",
                message=f"Error sending bot photo: {str(e)}",
                stack_trace=traceback.format_exc()
            )
            db.add(err)
            db.commit()
        except Exception:
            db.rollback()
        finally:
            db.close()
        
        # Fallback to plain text message
        logger.error(f"[BOT] Photo exception: {str(e)}. Falling back to text...")
        fallback_text = caption or "Domino's Order Engine Photo"
        return await send_bot_message(telegram_id, fallback_text, reply_markup)


async def send_bot_photo_bytes(telegram_id: str, image_bytes: bytes, filename: str = "image.png", caption: str = None, reply_markup: dict = None) -> bool:
    """
    Sends a raw image bytes buffer as a photo via Telegram's sendPhoto API (multipart upload).
    Use this for locally-generated QR codes or any image that must NOT rely on an external URL fetch.
    Falls back to send_bot_message on failure.
    """
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT PHOTO BYTES] To {telegram_id}: {filename}, Caption: {caption}")
        return True

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    try:
        import io as _io
        data = {"chat_id": str(telegram_id)}
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"
        if reply_markup:
            import json
            data["reply_markup"] = json.dumps(reply_markup)

        files = {"photo": (filename, _io.BytesIO(bytes(image_bytes)), "image/png")}
        resp = await _http_client.post(url, data=data, files=files, timeout=20.0)
        if resp.status_code == 200:
            return True
        logger.error(f"[BOT] Photo bytes upload failed (Code {resp.status_code}): {resp.text}")
        fallback_text = caption or "Payment QR Code"
        return await send_bot_message(telegram_id, fallback_text, reply_markup)
    except Exception as e:
        logger.error(f"[BOT] Photo bytes exception: {e}. Falling back to text...")
        fallback_text = caption or "Payment QR Code"
        return await send_bot_message(telegram_id, fallback_text, reply_markup)


async def send_bot_document(telegram_id: str, file_bytes: bytes, filename: str, caption: str = None, reply_markup: dict = None) -> bool:
    """Sends a raw bytes file to a Telegram user as a document (e.g. PDF report, DB backup)."""
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT DOCUMENT] To {telegram_id}: Document: {filename}, Caption: {caption}, ReplyMarkup: {reply_markup}")
        return True
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        data = {
            "chat_id": str(telegram_id),
        }
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"
        if reply_markup:
            import json
            data["reply_markup"] = json.dumps(reply_markup)
            
        import io as _io
        files = {
            "document": (filename, _io.BytesIO(bytes(file_bytes)), "application/octet-stream")
        }
        resp = await _http_client.post(url, data=data, files=files, timeout=40.0)
        if resp.status_code == 200:
            return True
        logger.error(f"Failed to send bot document: Code {resp.status_code}, Response: {resp.text}")
        return False
    except Exception as e:
        logger.error(f"Error sending bot document: {e}")
        return False

async def download_animation_in_background(url: str, local_path: str):
    """Asynchronously caches a remote GIF in the background to avoid blocking user threads."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 200:
                with open(local_path, "wb") as f:
                    f.write(resp.content)
                logger.debug(f"[BOT ANIM] Successfully cached remote animation: {url}")
    except Exception as e:
        logger.error(f"[BOT ANIM WARNING] Failed to cache animation in background: {e}")

async def get_local_animation_path(url: str) -> Optional[str]:
    """Checks if GIF is cached locally. If not, spawns a background task to download it and returns None immediately to avoid blocking."""
    if not url.startswith("http"):
        return None
        
    import hashlib
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    filename = f"anim_{url_hash[:12]}.gif"
    
    uploads_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "uploads"))
    os.makedirs(uploads_dir, exist_ok=True)
    local_path = os.path.join(uploads_dir, filename)
    
    if os.path.exists(local_path):
        return local_path
        
    # Queue caching asynchronously in the background so it is ready for subsequent requests
    asyncio.create_task(download_animation_in_background(url, local_path))
    return None


async def send_bot_animation(telegram_id: str, animation_url: str, caption: str = None, reply_markup: dict = None) -> bool:
    """
    Sends an animation (GIF) to a Telegram user.
    Uses Telegram's direct URL send to leverage server-side caching and prevent slow uploads.
    Safely falls back to send_bot_message if caption is too long (> 1000 chars) or animation send fails.
    """
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT ANIMATION] To {telegram_id}: Animation: {animation_url}, Caption: {caption}")
        return True
        
    # Telegram API sendAnimation caption limit is 1024 chars. If text is long, fallback to send_bot_message directly
    if caption and len(caption) > 1000:
        logger.info(f"[BOT ANIMATION] Caption length ({len(caption)}) > 1000 chars. Falling back to send_bot_message.")
        return await send_bot_message(telegram_id, caption, reply_markup)

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendAnimation"
    payload = {
        "chat_id": telegram_id,
        "animation": animation_url
    }
    if caption:
        payload["caption"] = caption
        payload["parse_mode"] = "HTML"
    if reply_markup:
        payload["reply_markup"] = reply_markup
        
    try:
        resp = await _http_client.post(url, json=payload, timeout=15.0)
        if resp.status_code == 200:
            res_data = resp.json()
            return res_data.get("result", {}).get("message_id", True)
        logger.error(f"[BOT ANIM FAIL] sendAnimation by URL -> {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"[BOT ANIM ERR] sendAnimation by URL exception: {e}")
        
    # Local fallback
    local_path = await get_local_animation_path(animation_url)
    if local_path:
        try:
            with open(local_path, "rb") as f:
                cap_text = caption[:995] + "..." if (caption and len(caption) > 1000) else caption
                req_data = {"chat_id": telegram_id, "caption": cap_text, "parse_mode": "HTML"}
                if reply_markup:
                    req_data["reply_markup"] = json.dumps(reply_markup) if isinstance(reply_markup, dict) else reply_markup
                resp = await _http_client.post(url, data=req_data, files={"animation": f}, timeout=25.0)
                if resp.status_code == 200:
                    res_data = resp.json()
                    return res_data.get("result", {}).get("message_id", True)
        except Exception:
            pass
            
    # Final fallback to standard send_bot_message
    fallback_text = caption or "Domino's Order Engine update"
    return await send_bot_message(telegram_id, fallback_text, reply_markup)


async def reverse_geocode(lat: float, lon: float) -> Optional[str]:
    """Reverse geocode latitude and longitude to get city name using OpenStreetMap Nominatim.
    Tries up to 2 times with a 5-second timeout for reliability."""
    url = f"https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat={lat}&lon={lon}&zoom=10&addressdetails=1"
    headers = {"User-Agent": "DominosOrderEngineBot/2.0 (contact@dominosorderengine.in)"}

    CITY_NORMALIZATIONS = {
        "bengaluru": "Bangalore", "bangalore": "Bangalore",
        "mumbai": "Mumbai", "bombay": "Mumbai",
        "delhi": "Delhi", "new delhi": "Delhi",
        "kolkata": "Kolkata", "calcutta": "Kolkata",
        "chennai": "Chennai", "madras": "Chennai",
        "hyderabad": "Hyderabad", "secunderabad": "Hyderabad",
        "pune": "Pune", "poona": "Pune",
        "ahmedabad": "Ahmedabad", "amdavad": "Ahmedabad",
        "jaipur": "Jaipur",
        "surat": "Surat",
        "lucknow": "Lucknow",
        "kanpur": "Kanpur",
        "nagpur": "Nagpur",
        "indore": "Indore",
        "thane": "Thane",
        "bhopal": "Bhopal",
        "visakhapatnam": "Visakhapatnam", "vizag": "Visakhapatnam",
        "pimpri": "Pune", "chinchwad": "Pune",
        "patna": "Patna",
        "vadodara": "Vadodara", "baroda": "Vadodara",
        "ghaziabad": "Ghaziabad",
        "ludhiana": "Ludhiana",
        "agra": "Agra",
        "nashik": "Nashik",
        "faridabad": "Faridabad",
        "meerut": "Meerut",
        "rajkot": "Rajkot",
        "kalyan": "Kalyan",
        "vasai": "Vasai",
        "coimbatore": "Coimbatore",
        "madurai": "Madurai",
        "noida": "Noida",
        "gurugram": "Gurugram", "gurgaon": "Gurugram",
        "navi mumbai": "Navi Mumbai",
    }

    for attempt in range(2):
        try:
            resp = await _http_client.get(url, headers=headers, timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                address = data.get("address", {})
                # Priority order: city > town > district > village > suburb > county
                city_val = (
                    address.get("city")
                    or address.get("town")
                    or address.get("district")
                    or address.get("village")
                    or address.get("suburb")
                    or address.get("municipality")
                    or address.get("city_district")
                    or address.get("county")
                    or address.get("state_district")
                )
                if city_val:
                    city_name = str(city_val).strip()
                    norm = city_name.lower()
                    # Check normalizations first
                    for key, canonical in CITY_NORMALIZATIONS.items():
                        if key in norm:
                            return canonical
                    return city_name
        except Exception as e:
            logger.warning(f"[reverse_geocode] Attempt {attempt+1} failed ({lat}, {lon}): {e}")
            if attempt == 0:
                await asyncio.sleep(1.0)  # brief wait before retry

    logger.error(f"[reverse_geocode] All attempts failed for ({lat}, {lon})")
    return None






def generate_menu_composite(db: Session) -> str:
    """Downloads all product images, creates a composite grid image, and saves it locally."""
    upload_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "uploads"))
    os.makedirs(upload_dir, exist_ok=True)
    out_path = os.path.join(upload_dir, "menu_composite.png")
    
    # Simple caching: return existing file if it's less than 10 minutes old
    if os.path.exists(out_path):
        mtime = os.path.getmtime(out_path)
        if (datetime.datetime.now().timestamp() - mtime) < 600:
            return "/uploads/menu_composite.png"
            
    from PIL import Image, ImageDraw, ImageFont
    import io
    import requests

    products = db.query(Product).filter(Product.availability == True).order_by(Product.original_price.asc()).all()
    if not products:
        return ""

    cols = 3
    rows = (len(products) + cols - 1) // cols
    thumb_w, thumb_h = 300, 200
    padding = 20
    text_height = 40
    
    # Grid cell size
    cell_w = thumb_w
    cell_h = thumb_h + text_height
    
    img_w = cols * cell_w + (cols + 1) * padding
    img_h = rows * cell_h + (rows + 1) * padding
    
    # Create dark composite canvas
    canvas = Image.new("RGB", (img_w, img_h), color="#1e1e2f")
    draw = ImageDraw.Draw(canvas)
    
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

    for idx, p in enumerate(products):
        r = idx // cols
        c = idx % cols
        
        x = c * cell_w + (c + 1) * padding
        y = r * cell_h + (r + 1) * padding
        
        p_img = None
        if p.image_url:
            try:
                resp = requests.get(p.image_url, timeout=4.0)
                if resp.status_code == 200:
                    p_img = Image.open(io.BytesIO(resp.content))
                    p_img = p_img.resize((thumb_w, thumb_h), Image.Resampling.LANCZOS)
            except Exception as e:
                logger.error(f"Error downloading image for {p.name}: {e}")
                
        if p_img is None:
            p_img = Image.new("RGB", (thumb_w, thumb_h), color="#A855F7" if p.is_veg else "#EF4444")
            p_draw = ImageDraw.Draw(p_img)
            p_draw.text((10, 80), "🍕 No Image", fill="white", font=font)
            
        canvas.paste(p_img, (x, y))
        
        label = f"Code {p.id}: {p.name[:22]}"
        draw.text((x + 5, y + thumb_h + 10), label, fill="white", font=font)

    canvas.save(out_path)
    return "/uploads/menu_composite.png"


def get_order_progress_bar(status: str) -> str:
    status_lower = status.lower() if status else ""
    if "pending" in status_lower:
        return "⏳ <code>[▒░░░░░░░░░] 10%</code> — Order Received & Pending Admin Review"
    elif "accepted" in status_lower:
        return "✅ <code>[▓▓░░░░░░░░] 25%</code> — Order Accepted by Admin"
    elif "placed" in status_lower:
        return "📝 <code>[▓▓▓▓░░░░░░] 40%</code> — Order Placed on Domino's"
    elif "processing" in status_lower or "baking" in status_lower:
        return "🍕 <code>[▓▓▓▓▓░░░░░] 50%</code> — Processing on Domino's"
    elif "kitchen" in status_lower or "preparing" in status_lower:
        return "👨‍🍳 <code>[▓▓▓▓▓▓▓░░░] 70%</code> — Preparing in Kitchen"
    elif "delivery" in status_lower or "out" in status_lower or "route" in status_lower:
        return "🛵 <code>[▓▓▓▓▓▓▓▓▓░] 85%</code> — Rider is Out for Delivery"
    elif "delivered" in status_lower or "complete" in status_lower or "success" in status_lower:
        return "🎉 <code>[▓▓▓▓▓▓▓▓▓▓] 100%</code> — Delivered! Enjoy your meal! 🍕"
    elif "cancel" in status_lower or "fail" in status_lower or "refund" in status_lower:
        return "❌ <code>[XXXXXXXXXX]</code> — Order Cancelled / Refunded"
    return f"ℹ️ {status}"


_MINI_APP_URL_CACHE = None
_MINI_APP_URL_LAST_UPDATE = 0

def get_mini_app_url(db: Session = None) -> str:
    """Helper to fetch the mini app URL from the database config or env fallback (cached for performance)."""
    global _MINI_APP_URL_CACHE, _MINI_APP_URL_LAST_UPDATE
    import time
    now = time.time()
    prod_domain = "https://dominos-order-engine-bot-and-webapp-1.onrender.com"
    if _MINI_APP_URL_CACHE is None or now - _MINI_APP_URL_LAST_UPDATE > 10.0:
        close_db = False
        if db is None:
            db = SessionLocal()
            close_db = True
        try:
            cfg = db.query(SystemConfig).filter(SystemConfig.key == "mini_app_url").first()
            if cfg and cfg.value:
                val = cfg.value.strip()
                if not any(bad in val.lower() for bad in ("testserver", "serveo", "ngrok", "loca.lt")):
                    _MINI_APP_URL_CACHE = val
                else:
                    _MINI_APP_URL_CACHE = prod_domain
                    cfg.value = prod_domain
                    db.commit()
            else:
                _MINI_APP_URL_CACHE = prod_domain
                db.add(SystemConfig(key="mini_app_url", value=prod_domain))
                db.commit()
            _MINI_APP_URL_LAST_UPDATE = now
        except Exception:
            pass
        finally:
            if close_db:
                db.close()
    return _MINI_APP_URL_CACHE if _MINI_APP_URL_CACHE is not None else os.getenv("MINI_APP_URL", prod_domain)

_BOT_FEE_CACHE = None
_BOT_FEE_LAST_UPDATE = 0

def get_bot_fee(db: Session) -> float:
    """Helper to fetch flat bot service fee (cached for performance)."""
    global _BOT_FEE_CACHE, _BOT_FEE_LAST_UPDATE
    import time
    now = time.time()
    if _BOT_FEE_CACHE is None or now - _BOT_FEE_LAST_UPDATE > 10.0:
        try:
            bot_fee_cfg = db.query(SystemConfig).filter(SystemConfig.key == "bot_fee").first()
            if bot_fee_cfg:
                _BOT_FEE_CACHE = float(bot_fee_cfg.value)
                _BOT_FEE_LAST_UPDATE = now
        except Exception:
            pass
    if _BOT_FEE_CACHE is not None:
        return _BOT_FEE_CACHE
    return 10.0

_PRODUCT_MAPPING_CACHE = None
_PRODUCT_MAPPING_LAST_UPDATE = 0

def get_product_mappings(db: Session):
    """Generates sequential 1-based display codes for active products (cached for performance)."""
    global _PRODUCT_MAPPING_CACHE, _PRODUCT_MAPPING_LAST_UPDATE
    import time
    now = time.time()
    if _PRODUCT_MAPPING_CACHE is None or now - _PRODUCT_MAPPING_LAST_UPDATE > 10.0:
        products = db.query(Product).filter(Product.availability == True).order_by(Product.original_price.asc()).all()
        code_to_id = {}
        id_to_code = {}
        for idx, p in enumerate(products, start=1):
            code_to_id[str(idx)] = p.id
            id_to_code[p.id] = idx
        _PRODUCT_MAPPING_CACHE = (code_to_id, id_to_code)
        _PRODUCT_MAPPING_LAST_UPDATE = now
    return _PRODUCT_MAPPING_CACHE


async def display_delivery_location_menu(db: Session, user: User):
    session = USER_BOT_SESSION.setdefault(user.telegram_id, {"cart": {}, "state": None})
    session["state"] = "in_location_menu"
    
    has_gps = (user.latitude is not None and user.longitude is not None)
    if has_gps:
        city_lbl = f" ({user.city})" if user.city and user.city != "GPS Location" else ""
        coord_line = f"📡 <b>GPS Location:</b> ✅ <code>{user.latitude:.5f}, {user.longitude:.5f}</code>{city_lbl}"
        gps_btn_text = "📍 Update GPS Location"
    else:
        coord_line = f"📡 <b>GPS Location:</b> 🔴 <b>NOT SET (REQUIRED)</b>\n  └ <i>Tap button below to share coordinates for Domino's store mapping</i>"
        gps_btn_text = "🔴 📍 Share GPS Location (REQUIRED)"
        
    saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
    has_addr = False
    full_addr = ""
    if saved_addr and saved_addr.full_address and saved_addr.full_address != "GPS Location" and len(saved_addr.full_address.strip()) > 3:
        has_addr = True
        full_addr = saved_addr.full_address
    elif user.address and user.address != "GPS Location" and len(user.address.strip()) > 3:
        has_addr = True
        full_addr = user.address
        
    if has_addr:
        addr_line = f"\n🏠 <b>Doorstep Address:</b> ✅ <code>{escape_html(full_addr)}</code>"
        addr_btn_text = "🏠 Edit Delivery Address"
    else:
        addr_line = f"\n🏠 <b>Doorstep Address:</b> ⚠️ <b>NOT SET</b>\n  └ <i>Flat/Building, Street & Landmark required for delivery rider</i>"
        addr_btn_text = "⚠️ 🏠 Enter Doorstep Address"
        
    has_phone = (user.phone is not None and len(str(user.phone).strip()) >= 10)
    if has_phone:
        phone_line = f"\n📱 <b>Phone Number:</b> ✅ <code>{escape_html(user.phone)}</code>"
        phone_btn_text = "📱 Edit Phone Number"
    else:
        phone_line = f"\n📱 <b>Phone Number:</b> ⚠️ <b>NOT SET</b>\n  └ <i>10-digit mobile number required for order updates</i>"
        phone_btn_text = "⚠️ 📱 Enter Mobile Number"

    has_cart = bool(session.get("cart"))
    all_confirmed = (has_gps and has_addr and has_phone)
    
    loc_msg = (
        f"📍 <b>Delivery Location & Details</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{coord_line}"
        f"{addr_line}"
        f"{phone_line}\n\n"
    )

    if all_confirmed:
        if has_cart:
            confirm_btn_text = "✅ Confirm Details & Return to Cart"
            loc_msg += "✅ <b>All delivery details confirmed!</b> Tap below to proceed directly to your shopping cart/checkout."
        else:
            confirm_btn_text = "✅ Confirm Location & View Menu"
            loc_msg += "✅ <b>All delivery details confirmed!</b> You are ready to order."
    else:
        confirm_btn_text = "🛒 Return to Cart" if has_cart else "🍕 View Pizza Menu"
        loc_msg += "<i>Please complete any missing details marked with ⚠️/🔴 below to enable delivery:</i>"

    keyboard_rows = []
    if all_confirmed:
        keyboard_rows.append([{"text": confirm_btn_text}])
    
    keyboard_rows.append([{"text": gps_btn_text, "request_location": True}])
    keyboard_rows.append([{"text": addr_btn_text}])
    keyboard_rows.append([{"text": phone_btn_text}])
    
    if not all_confirmed:
        keyboard_rows.append([{"text": confirm_btn_text}])
        
    keyboard_rows.append([{"text": "🔙 Back"}])

    loc_options_keyboard = {
        "keyboard": keyboard_rows,
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    await send_bot_message(
        user.telegram_id,
        loc_msg,
        reply_markup=loc_options_keyboard
    )


async def display_pizza_menu(db: Session, user: User, reply_markup: dict, page: int = 1, category: str = "All", edit_message_id: int = None):
    """Displays the menu containing all pizzas as rich text with full pricing, types, and category filters."""
    query = db.query(Product).filter(Product.availability == True)
    if category != "All":
        if category.lower() in ("veg", "non-veg"):
            is_v = (category.lower() == "veg")
            query = query.filter(Product.is_veg == is_v, ~Product.category.ilike("Sides"), ~Product.category.ilike("Desserts"), ~Product.category.ilike("Drinks"), ~Product.category.ilike("Mania"))
        else:
            query = query.filter(Product.category.ilike(category))
    products = query.order_by(Product.sort_order.asc(), Product.original_price.asc()).all()

    if not products:
        empty_text = (
            f"🍽️ <b>No items found in '{category}'</b>\n\n"
            f"Try selecting a different category or tap <b>⭐ All Categories</b> below."
        )
        back_markup = {"inline_keyboard": [
            [{"text": "⭐ All Categories", "callback_data": "menu_category_All"}],
            [{"text": "🛒 View Cart", "callback_data": "cart_view"}]
        ]}
        if edit_message_id:
            await edit_bot_message(user.telegram_id, edit_message_id, empty_text, reply_markup=back_markup)
        else:
            await send_bot_message(user.telegram_id, empty_text, reply_markup=back_markup)
        return

    code_to_id, id_to_code = get_product_mappings(db)

    items_per_page = 5
    total_pages = (len(products) + items_per_page - 1) // items_per_page
    page = max(1, min(page, total_pages))

    start_idx = (page - 1) * items_per_page
    page_products = products[start_idx:start_idx + items_per_page]

    category_emoji = {
        "veg": "🟢 Veg", "non-veg": "🔴 Non-Veg", "sides": "🍟 Sides",
        "drinks": "🥤 Drinks", "desserts": "🍰 Desserts", "mania": "🍕 Pizza Mania", "all": "🍕 All Items"
    }
    cat_label = category_emoji.get(category.lower(), f"🍽️ {category}")

    menu_lines = [
        f"🍕 <b>DOMINO'S PIZZA MENU</b>",
        f"━━━━━━━━━━━━━━━━━━━━━━",
        f"Category: <b>{cat_label}</b>",
        f"📄 Page <b>{page}/{total_pages}</b> ({len(products)} items available)",
        "━━━━━━━━━━━━━━━━━━━━━━\n"
    ]

    for p in page_products:
        original = float(round(p.original_price))
        effective = float(round(p.discounted_price)) if p.discounted_price is not None else original

        veg_dot = "🟢" if p.is_veg else "🔴"
        display_code = id_to_code.get(p.id, "—")
        
        if p.discounted_price is not None and p.discounted_price < p.original_price:
            price_str = f"<s>₹{original:.0f}</s> <b>₹{effective:.0f}</b> <i>(Save ₹{original-effective:.0f}!)</i>"
        else:
            price_str = f"<b>₹{effective:.0f}</b>"

        badges = []
        if p.is_popular:
            badges.append("🔥 Best Seller")
        if p.is_recommended:
            badges.append("⭐ Chef Pick")
        badge_str = "  " + " ".join(badges) if badges else ""

        menu_lines.append(
            f"{veg_dot} <b>{p.name}</b> [{display_code}]{badge_str}\n"
            f"   💳 MRP Price: {price_str}\n"
            f"   📝 <i>{(p.description or 'Freshly prepared Domino\'s item with 100% mozzarella cheese.')[:90]}</i>"
        )

    menu_lines.append(
        "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 <i>Tap any item button below to add to your cart!</i>"
    )

    menu_text = "\n\n".join(menu_lines)

    # Inline add-to-cart grid
    grid = []
    row = []
    for p in page_products:
        effective = float(round(p.discounted_price)) if p.discounted_price is not None else float(round(p.original_price))
        name_limit = p.name[:14] + "…" if len(p.name) > 16 else p.name
        veg_icon = "🟢" if p.is_veg else "🔴"
        row.append({"text": f"➕ {veg_icon} {name_limit} (₹{effective:.0f})", "callback_data": f"cart_add_{p.id}"})
        if len(row) == 2:
            grid.append(row)
            row = []
    if row:
        grid.append(row)

    # Navigation row
    nav_row = []
    if page > 1:
        nav_row.append({"text": "⬅️ Prev", "callback_data": f"menu_page_{page-1}_{category}"})
    nav_row.append({"text": f"📄 {page}/{total_pages}", "callback_data": f"menu_noop_{page}_{total_pages}_{category}"})
    if page < total_pages:
        nav_row.append({"text": "Next ➡️", "callback_data": f"menu_page_{page+1}_{category}"})
    grid.append(nav_row)

    # Category filters
    grid.append([
        {"text": "⭐ All",       "callback_data": "menu_category_All"},
        {"text": "🟢 Veg",      "callback_data": "menu_category_Veg"},
        {"text": "🔴 Non-Veg",  "callback_data": "menu_category_Non-Veg"}
    ])
    grid.append([
        {"text": "🍕 Mania",    "callback_data": "menu_category_Mania"},
        {"text": "🍟 Sides",    "callback_data": "menu_category_Sides"},
        {"text": "🥤 Drinks",   "callback_data": "menu_category_Drinks"},
        {"text": "🍰 Desserts", "callback_data": "menu_category_Desserts"}
    ])

    # Cart button with live count
    session = USER_BOT_SESSION.get(str(user.telegram_id), {})
    cart = session.get("cart", {})
    cart_count = sum(cart.values()) if isinstance(cart, dict) else 0
    cart_label = f"🛒 View Shopping Cart ({cart_count} items)" if cart_count > 0 else "🛒 View Shopping Cart (Empty)"
    grid.append([{"text": cart_label, "callback_data": "cart_view"}])

    markup = {"inline_keyboard": grid}

    if edit_message_id:
        edited = await edit_bot_message(user.telegram_id, edit_message_id, menu_text, reply_markup=markup)
        if not edited:
            await send_bot_message(user.telegram_id, menu_text, reply_markup=markup)
    else:
        res = await send_bot_message(user.telegram_id, menu_text, reply_markup=markup)
        if str(user.telegram_id) not in USER_BOT_SESSION:
            USER_BOT_SESSION[str(user.telegram_id)] = {"state": None, "cart": {}}
        if isinstance(res, int):
            USER_BOT_SESSION[str(user.telegram_id)]["last_bot_msg_id"] = res




USER_LAST_PHOTO_SYNC = {}


async def sync_user_profile_photo(telegram_id: str, user_db_id: str):
    """Fetches the user's Telegram profile photo and updates user.photo_url if changed."""
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        return
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    if now - USER_LAST_PHOTO_SYNC.get(telegram_id, 0) < 3600:
        return
    USER_LAST_PHOTO_SYNC[telegram_id] = now
    
    from .database import SessionLocal as _SL
    from .database import User as _User
    db = _SL()
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUserProfilePhotos"
        resp = await _http_client.post(url, json={"user_id": int(telegram_id), "limit": 1}, timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("ok") and data.get("result", {}).get("total_count", 0) > 0:
                photos = data["result"]["photos"]
                photo_sizes = photos[0]
                largest_photo = photo_sizes[-1]
                file_id = largest_photo["file_id"]
                
                file_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getFile"
                file_resp = await _http_client.post(file_url, json={"file_id": file_id}, timeout=5.0)
                if file_resp.status_code == 200:
                    file_data = file_resp.json()
                    if file_data.get("ok"):
                        file_path = file_data["result"]["file_path"]
                        photo_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
                        
                        user = db.query(_User).filter(_User.id == user_db_id).first()
                        if user and user.photo_url != photo_url:
                            user.photo_url = photo_url
                            db.commit()
    except Exception as e:
        logger.error(f"[Warning] Failed to sync profile photo for {telegram_id}: {e}")
    finally:
        db.close()


async def process_auto_pay_for_user(db: Session, target_user: User):
    """Checks for pending orders for user and auto-pays them if wallet balance is sufficient."""
    try:
        pending_orders = db.query(Order).filter(
            Order.user_id == target_user.id,
            Order.status == "Pending Payment",
            Order.payment_method != "direct_upi",
            ~Order.id.like("TOPUP-%")
        ).order_by(Order.created_at.asc()).all()

        for pending_order in pending_orders:
            if target_user.wallet_balance >= pending_order.total_payable:
                target_user.wallet_balance -= pending_order.total_payable
                
                tx = WalletTransaction(
                    user_id=target_user.id,
                    type="payment",
                    amount=-pending_order.total_payable,
                    description=f"Auto-payment for pending order: {pending_order.id}"
                )
                db.add(tx)
                
                h1 = OrderStatusHistory(order_id=pending_order.id, status="Payment Received", note="Auto-paid from approved wallet deposit")
                db.add(h1)
                h2 = OrderStatusHistory(order_id=pending_order.id, status="Order Processing", note="Queued for Domino's processing")
                db.add(h2)
                pending_order.status = "Order Processing"
                db.commit()
                
                auto_msg = (
                    f"🎉 <b>Pending Order Auto-Paid & Activated!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Your deposit was approved and your pending order <code>{pending_order.id}</code> (<b>₹{pending_order.total_payable:.2f}</b>) has been automatically paid from your wallet balance!\n\n"
                    f"💰 <b>Remaining Wallet Balance:</b> ₹{target_user.wallet_balance:.2f}\n"
                    f"🍕 Status: <b>Order Processing</b>"
                )
                await send_bot_message(target_user.telegram_id, auto_msg)
                
                admin_auto_text = (
                    f"🔔 <b>Pending Order Auto-Paid & Activated!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🆔 <b>Order ID:</b> <code>{pending_order.id}</code>\n"
                    f"👤 <b>Customer:</b> {target_user.display_name} (ID: <code>{target_user.telegram_id}</code>)\n"
                    f"💰 <b>Amount Paid:</b> ₹{pending_order.total_payable:.2f}\n"
                    f"🏡 <b>Address:</b> <code>{pending_order.address or 'N/A'}</code>"
                )
                await notify_admins(db, admin_auto_text)
    except Exception as e:
        logger.error(f"Error in process_auto_pay_for_user: {e}")


def render_admin_command_center(db: Session) -> tuple[str, dict]:
    """Generates the unified admin command center dashboard text and inline keyboard markup."""
    func = sql_func
    total_users = db.query(DbUser).count()
    today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    total_orders = db.query(Order).count()
    today_orders = db.query(Order).filter(Order.created_at >= today_start).count()
    today_completed_orders = db.query(Order).filter(
        Order.status == "Completed",
        Order.created_at >= today_start
    ).all()
    today_revenue = sum(o.total_payable for o in today_completed_orders)
    total_wallets = db.query(func.sum(DbUser.wallet_balance)).scalar() or 0.0
    pending_orders_count = db.query(Order).filter(Order.status.in_(["Paid", "Pending Payment", "Pending Verification", "Order Processing"]), ~Order.id.like("TOPUP-%")).count()
    pending_deposits_count = db.query(Order).filter(Order.id.like("TOPUP-%"), Order.status == "Pending Verification").count()
    
    maint_cfg = db.query(SystemConfig).filter(SystemConfig.key == "maintenance_mode").first()
    maint_val = maint_cfg.value if maint_cfg else "false"
    maint_status = "⚠️ MAINTENANCE ON" if maint_val == "true" else "🟢 ONLINE"

    admin_dashboard_text = (
        f"🤖 <b>Platform Admin Command Center</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🛠️ <b>Platform Status:</b> <code>{maint_status}</code>\n"
        f"👥 <b>Total Registered Users:</b> <code>{total_users}</code>\n"
        f"💳 <b>Total Wallet Holdings:</b> <code>₹{total_wallets:.2f}</code>\n\n"
        f"📊 <b>Orders Overview:</b>\n"
        f"• Total Orders placed: <code>{total_orders}</code>\n"
        f"• Orders Today: <code>{today_orders}</code>\n"
        f"• Revenue Today: <b>₹{today_revenue:.2f}</b>\n\n"
        f"⚠️ <b>Action Needed:</b>\n"
        f"• Pending Orders: <b>{pending_orders_count}</b>\n"
        f"• Pending Deposits: <b>{pending_deposits_count}</b>\n\n"
        f"<i>Use the control panel options below to approve actions manually:</i>"
    )

    admin_inline_markup = {
        "inline_keyboard": [
            [
                {"text": "📊 Refresh Stats", "callback_data": "admin_refresh_stats"},
                {"text": "📦 Pending Orders", "callback_data": "admin_view_pending_orders"}
            ],
            [
                {"text": "🛒 Manage Orders", "callback_data": "admin_manage_orders_menu"},
                {"text": "🏦 Payment Management", "callback_data": "admin_payment_management"}
            ],
            [
                {"text": "👥 Manage Users", "callback_data": "admin_manage_users"},
                {"text": "🎟️ Manage Promo Codes", "callback_data": "admin_promo_menu"}
            ],
            [
                {"text": "🎉 Manage Active Offers", "callback_data": "admin_offers_menu"},
                {"text": "⚙️ System Config", "callback_data": "admin_sys_config"}
            ],
            [
                {"text": "📢 Broadcast Message", "callback_data": "admin_broadcast_menu"},
                {"text": "💬 Support Tickets (24h)", "callback_data": "admin_view_support_tickets"}
            ],
            [
                {"text": "📊 Reports & Backup", "callback_data": "admin_reports_menu"},
                {"text": "⚠️ View Error Logs", "callback_data": "admin_view_error_logs"}
            ],
            [
                {"text": "🗑️ Clear Database (Main Admin)", "callback_data": "admin_clear_db_confirm"}
            ]
        ]
    }
    return admin_dashboard_text, admin_inline_markup


def resolve_cart_item(db: Session, key: str):
    """
    Resolves a cart key to either an ActiveOffer or a Product.
    Returns tuple: (type_str, obj, item_name, unit_price, items_breakdown_list)
    """
    key_str = str(key).strip()
    if key_str.startswith("offer_"):
        offer_key = key_str[len("offer_"):]
        offer = db.query(ActiveOffer).filter((ActiveOffer.offer_key == offer_key) | (ActiveOffer.id == offer_key)).first()
        if offer:
            items_list = []
            if offer.items_json:
                try:
                    p_items = json.loads(offer.items_json)
                    if isinstance(p_items, list):
                        for itm in p_items:
                            name = itm.get("name") or itm.get("category") or "Item"
                            size = itm.get("size", "")
                            qty = itm.get("qty") or itm.get("quantity") or 1
                            sz_str = f" ({size})" if size else ""
                            items_list.append(f"{qty}x {name}{sz_str}")
                except Exception:
                    pass
            return ("offer", offer, offer.title, float(offer.discounted_price), items_list)

    # Fallback to product
    prod_id = key_str.replace("prod_", "")
    p = db.query(Product).filter(Product.id == prod_id).first()
    if not p:
        code_to_id, _ = get_product_mappings(db)
        if prod_id in code_to_id:
            p = db.query(Product).filter(Product.id == code_to_id[prod_id]).first()
    if not p:
        p = resolve_cart_item_product(db, key_str)
    if p:
        price = float(round(p.discounted_price if p.discounted_price is not None else p.original_price))
        return ("product", p, p.name, price, [])

    return (None, None, "Domino's Pizza Item", 0.0, [])


def render_cart_message(db: Session, user: User, cart: dict, session: dict) -> tuple[str, dict]:
    """
    Renders the shopping cart view text and inline keyboard controls.
    Supports both regular products and active offer/deal items.
    """
    valid_items = {}
    subtotal = 0.0
    item_lines = []

    for key_str, raw_qty in list(cart.items()):
        qty = parse_cart_quantity(raw_qty)
        if qty <= 0:
            continue
        item_type, obj, item_name, unit_price, items_breakdown = resolve_cart_item(db, key_str)
        valid_items[key_str] = qty
        line_total = unit_price * qty
        subtotal += line_total

        if item_type == "offer":
            badge_str = f" {obj.badge}" if (obj and obj.badge) else ""
            block = f"🎉 <b>{escape_html(item_name)}</b>{badge_str}\n   {qty} × ₹{unit_price:.0f}  —  <b>₹{line_total:.0f}</b>"
            if items_breakdown:
                for b in items_breakdown:
                    block += f"\n   └ <i>{b}</i>"
            item_lines.append(block)
        else:
            veg_dot = "🟢" if (obj and getattr(obj, "is_veg", True)) else "🔴"
            item_lines.append(f"{veg_dot} <b>{escape_html(item_name)}</b>\n   {qty} × ₹{unit_price:.0f}  —  <b>₹{line_total:.0f}</b>")

    bot_fee = get_bot_fee(db)
    final_total = subtotal + bot_fee

    wallet_bal = user.wallet_balance or 0.0
    wallet_usable = min(wallet_bal, final_total)
    remaining_upi = final_total - wallet_usable

    header = "🛒 <b>YOUR SHOPPING CART</b>\n" + "━" * 28 + "\n\n"
    if item_lines:
        items_str = "\n\n".join(item_lines) + "\n\n"
    else:
        items_str = "<i>Your cart is empty. Browse the menu or active offers to add delicious items!</i>\n\n"

    divider = "━" * 28 + "\n"
    summary = f"💰 <b>Subtotal:</b> ₹{subtotal:.2f}\n"
    if bot_fee > 0:
        summary += f"⚡ <b>Platform / Service Fee:</b> ₹{bot_fee:.2f}\n"
    summary += f"🧾 <b>Grand Total:</b> <b>₹{final_total:.2f}</b>\n\n"

    if wallet_usable >= final_total:
        wallet_note = f"💳 <i>Full amount will be deducted from your wallet balance (Balance: ₹{wallet_bal:.2f}).</i>\n"
    elif wallet_usable > 0:
        wallet_note = f"🌗 <i>₹{wallet_usable:.2f} wallet balance will be applied, leaving ₹{remaining_upi:.2f} for UPI.</i>\n"
    else:
        wallet_note = f"💡 <i>Wallet Balance: ₹{wallet_bal:.2f}</i>\n"

    cart_text = header + items_str + divider + summary + wallet_note

    keyboard = []
    if valid_items:
        for k_str, q in valid_items.items():
            item_type, obj, item_name, unit_price, items_breakdown = resolve_cart_item(db, k_str)
            short_name = (item_name[:16] + "…") if len(item_name) > 18 else item_name
            kbd_row = [
                {"text": "➖", "callback_data": f"cart_dec_{k_str}"},
                {"text": f"{short_name} ({q})", "callback_data": f"cart_info_{k_str}"},
                {"text": "➕", "callback_data": f"cart_inc_{k_str}"},
                {"text": "🗑️", "callback_data": f"cart_del_{k_str}"}
            ]
            keyboard.append(kbd_row)

        keyboard.append([
            {"text": "🧹 Clear Cart", "callback_data": "clear_cart"},
            {"text": "🚀 Proceed to Checkout", "callback_data": "initiate_checkout"}
        ])

    keyboard.append([{"text": "🍕 Back to Menu", "callback_data": "show_categories"}])

    return cart_text, {"inline_keyboard": keyboard}


def render_order_confirmation_screen(db: Session, user: User, session: dict) -> tuple[str, dict]:
    address = session.get("temp_address")
    phone   = session.get("temp_phone")

    cart = session.get("cart", {})
    subtotal = 0.0
    item_lines = []

    for key_str, raw_qty in list(cart.items()):
        qty = parse_cart_quantity(raw_qty)
        if qty <= 0:
            continue
        item_type, obj, item_name, unit_price, items_breakdown = resolve_cart_item(db, key_str)
        line_total = unit_price * qty
        subtotal += line_total

        if item_type == "offer":
            badge_str = f" {obj.badge}" if (obj and obj.badge) else ""
            item_lines.append(f"  🎉 <b>{escape_html(item_name)}</b>{badge_str} ×{qty}  —  <b>₹{line_total:.0f}</b>")
            if items_breakdown:
                for b in items_breakdown:
                    item_lines.append(f"     └ <i>{b}</i>")
        else:
            veg_dot = "🟢" if (obj and getattr(obj, "is_veg", True)) else "🔴"
            item_lines.append(f"  {veg_dot} <b>{escape_html(item_name)}</b> ×{qty}  —  <b>₹{line_total:.0f}</b>")

    items_text = "\n".join(item_lines) if item_lines else "  • Pizza Items"

    bot_fee = get_bot_fee(db)
    total_payable = subtotal + bot_fee

    wallet_bal = user.wallet_balance or 0.0
    wallet_usable = min(wallet_bal, total_payable)
    remaining_upi = total_payable - wallet_usable

    if wallet_usable >= total_payable:
        pay_info = f"💳 <b>Payment Mode: Full Wallet Deduction</b>\n  └ <b>₹{total_payable:.2f}</b> will be deducted from your wallet balance (Balance: ₹{wallet_bal:.2f})"
    elif wallet_usable > 0:
        pay_info = f"🌗 <b>Payment Mode: Partial Wallet + UPI</b>\n  └ <b>₹{wallet_usable:.2f}</b> deducted from wallet + <b>₹{remaining_upi:.2f}</b> payable via UPI QR"
    else:
        pay_info = f"📱 <b>Payment Mode: Direct UPI QR Code</b>\n  └ <b>₹{total_payable:.2f}</b> payable via UPI QR"

    order_note = session.get("order_note", "")
    note_line = f"\n✏️ <b>Order Note:</b> <i>{order_note}</i>" if order_note else ""
    note_btn_label = "✏️ Edit Note" if order_note else "📝 Add Note to Order"

    addr_disp = escape_html(address) if address else "Not provided"
    phone_disp = escape_html(phone) if phone else "Not provided"

    confirm_text = (
        "📋 <b>Review Your Order</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🛒 <b>Items:</b>\n{items_text}\n\n"
        f"🏠 <b>Delivery Address:</b> <code>{addr_disp}</code>\n"
        f"📱 <b>Phone:</b> <code>{phone_disp}</code>"
        f"{note_line}\n\n"
        "💰 <b>Price Breakdown:</b>\n"
        f"  Item Subtotal:    ₹{subtotal:.2f}\n"
        f"  Bot Service Fee:  +₹{bot_fee:.2f}\n"
        "  ─────────────────────\n"
        f"  <b>Total Payable:  ₹{total_payable:.2f}</b>\n\n"
        f"{pay_info}\n\n"
        "Select your preferred payment method below to finalize:"
    )
    
    btn_wallet_label = f"💳 Pay via Wallet (₹{wallet_usable:.0f})" if wallet_usable > 0 else f"💳 Wallet (₹{wallet_bal:.0f} - Low)"
    confirm_markup = {
        "inline_keyboard": [
            [
                {"text": btn_wallet_label, "callback_data": "order_confirm_place_wallet"},
                {"text": "📱 Pay via UPI QR Code", "callback_data": "order_confirm_place_direct_qr"}
            ],
            [
                {"text": note_btn_label, "callback_data": "checkout_add_note"},
                {"text": "✏️ Edit Details", "callback_data": "checkout_edit_details"}
            ],
            [
                {"text": "❌ Cancel Order", "callback_data": "order_cancel_place"}
            ]
        ]
    }
    return confirm_text, confirm_markup


async def initiate_checkout(db: Session, user: User, session: dict, edit_message_id: int = None):
    """Starts or resumes the checkout flow using a unified message layout."""
    cart = session.get("cart", {})
    active_cart = {k: parse_cart_quantity(v) for k, v in cart.items() if parse_cart_quantity(v) > 0}
    if not active_cart:
        empty_markup = {"inline_keyboard": [[{"text": "🍕 View Menu", "callback_data": "menu_view"}]]}
        if edit_message_id:
            await edit_bot_message(user.telegram_id, edit_message_id, "🛒 <b>Your cart is empty! Please add items to proceed.</b>", reply_markup=empty_markup)
        else:
            await send_bot_message(user.telegram_id, "🛒 <b>Your cart is empty! Please add items to proceed.</b>", reply_markup=empty_markup)
        return

    saved_addr = db.query(SavedAddress).filter(
        SavedAddress.user_id == user.id, SavedAddress.is_default == True
    ).first()
    if not saved_addr:
        saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()

    if (user.latitude is not None and user.longitude is not None):
        default_full_addr = (
            (user.address and str(user.address).strip())
            or (f"{user.city}, Location Pin" if (user.city and user.city != "GPS Location") else None)
            or (f"GPS Location ({user.latitude:.4f}, {user.longitude:.4f})" if (user.latitude is not None and user.longitude is not None) else None)
            or (user.city if user.city else None)
        ) or "Saved Location"
        if not saved_addr:
            saved_addr = SavedAddress(
                user_id=user.id,
                label="Home",
                full_address=default_full_addr,
                is_default=True,
                latitude=user.latitude,
                longitude=user.longitude,
                city=user.city
            )
            db.add(saved_addr)
            db.commit()
        else:
            if not saved_addr.full_address or not saved_addr.full_address.strip():
                saved_addr.full_address = default_full_addr
            if saved_addr.latitude is None or saved_addr.longitude is None:
                saved_addr.latitude = user.latitude
                saved_addr.longitude = user.longitude
            db.commit()
    elif saved_addr:
        if saved_addr.latitude is not None and saved_addr.longitude is not None:
            user.latitude = saved_addr.latitude
            user.longitude = saved_addr.longitude
            if not user.city and saved_addr.city:
                user.city = saved_addr.city
        if not saved_addr.full_address or not saved_addr.full_address.strip():
            saved_addr.full_address = (user.address and str(user.address).strip()) or "Saved Address"
        db.commit()

    has_coords = (user.latitude is not None and user.longitude is not None)
    city = user.city
    if not city and has_coords:
        city = "GPS Location"

    latest_order = db.query(Order).filter(
        Order.user_id == user.id
    ).order_by(Order.created_at.desc()).first()

    saved_address = html_escape(saved_addr.full_address) if (saved_addr and saved_addr.full_address) else (html_escape(latest_order.address) if (latest_order and latest_order.address) else None)
    saved_phone   = html_escape(user.phone) if user.phone else (html_escape(latest_order.phone) if (latest_order and latest_order.phone) else None)
    city          = html_escape(city) if city else None
    has_doorstep_address = (
        saved_address is not None 
        and saved_address != "GPS Location" 
        and (not city or saved_address.strip().lower() != city.strip().lower())
        and len(saved_address.strip()) > 3
    )

    if (has_coords or has_doorstep_address or saved_phone):
        if (has_doorstep_address or has_coords) and saved_phone and not session.get("force_address_entry"):
            # AUTO-SKIP: If we already have their location/address and phone, go straight to order confirmation!
            session["temp_address"] = saved_address or (f"GPS ({user.latitude:.4f}, {user.longitude:.4f})" if has_coords else "Saved Address")
            session["temp_phone"]   = saved_phone
            session["state"] = "waiting_for_confirm"
            sync_user_db_session(db, user, session)
            prompt, confirm_markup = render_order_confirmation_screen(db, user, session)
            if edit_message_id:
                await edit_bot_message(user.telegram_id, edit_message_id, prompt, reply_markup=confirm_markup)
            else:
                await send_bot_message(user.telegram_id, prompt, reply_markup=confirm_markup)
            return

        # Show status of fields
        addr_line  = f"\n✅ <b>Saved Address:</b> <code>{saved_address}</code>" if has_doorstep_address else "\n⚠️ <b>Saved Address:</b> <i>Missing / Required</i>"
        phone_line = f"\n✅ <b>Phone:</b> <code>{saved_phone}</code>" if saved_phone else "\n⚠️ <b>Phone:</b> <i>Missing / Required</i>"
        coords_line = f"\n✅ <b>GPS Coordinates:</b> <code>{user.latitude:.5f}, {user.longitude:.5f}</code>" if has_coords else "\n⚠️ <b>GPS Coordinates:</b> <i>Optional / Not shared</i>"
        
        prompt = (
            "📍 <b>Delivery Details Required</b>\n\n"
            f"{addr_line}"
            f"{phone_line}"
            f"{coords_line}\n\n"
            "Please confirm or update your details below to proceed to order review."
        )
        inline = []
        if (has_doorstep_address or has_coords) and saved_phone:
            inline.append([{"text": "✅ Confirm & Use These Details", "callback_data": "checkout_confirm_location"}])
            
        inline.append([
            {"text": "🏠 Update Address", "callback_data": "checkout_enter_new"},
            {"text": "📱 Update Phone",   "callback_data": "checkout_enter_phone"}
        ])
        inline.append([
            {"text": "📍 Share GPS Location", "callback_data": "checkout_change_location"}
        ])
        inline.append([{"text": "🛒 Back to Cart",                     "callback_data": "cart_view"}])
        
        if saved_address: session["temp_address"] = saved_address
        if saved_phone:   session["temp_phone"]   = saved_phone
        session["state"] = "waiting_for_details"
        sync_user_db_session(db, user, session)
        
        confirm_markup = {"inline_keyboard": inline}
        if edit_message_id:
            await edit_bot_message(user.telegram_id, edit_message_id, prompt, reply_markup=confirm_markup)
        else:
            await send_bot_message(user.telegram_id, prompt, reply_markup=confirm_markup)
        return
    else:
        # No coordinates or city — prompt to share GPS using a single, clear reply keyboard
        session["checkout_pending"] = True
        prompt = (
            "📍 <b>GPS Location Required for Checkout</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "We need your GPS coordinates to place your Domino's order.\n"
            "Please use the <b>📍 Share Current Location</b> button below on your keyboard to share your location, "
            "or click <b>🔙 Back</b> to return."
        )
        loc_keyboard = {
            "keyboard": [
                [{"text": "📍 Share Current Location", "request_location": True}],
                [{"text": "🔙 Back"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        if edit_message_id:
            await delete_bot_message(user.telegram_id, edit_message_id)
        await send_bot_message(user.telegram_id, prompt, reply_markup=loc_keyboard)
        return


def sync_user_db_session(db: Session, user: User, session: dict):
    """
    Persists in-memory session changes (cart, active state) directly into the database
    so that state updates are 100% immediate, live, and survived across server/session restarts
    without requiring the user to re-run /start.
    """
    if not user or session is None:
        return
    try:
        import json
        user.bot_state = session.get("state")
        dump_data = {
            "cart": session.get("cart", {}),
            "temp_address": session.get("temp_address"),
            "temp_phone": session.get("temp_phone")
        }
        user.bot_cart = json.dumps(dump_data)
        db.commit()
        from .database import auto_save_persistent_db_state
        auto_save_persistent_db_state(db)
    except Exception as e:
        logger.error(f"[DB Session Sync Error] {e}")
        db.rollback()


async def handle_bot_message(db: Session, telegram_id: str, first_name: str, last_name: str, username: str, text: str, location: dict = None, message_id: int = None, photo: list = None, document: dict = None):
    """
    Handles an incoming message sent to the Telegram bot with custom keyboards, commands, and looping GIFs.
    """
    # Show typing indicator immediately — makes the bot feel human & responsive
    await send_bot_typing(str(telegram_id))

    global MINI_APP_URL
    MINI_APP_URL = get_mini_app_url(db)

    user = db.query(User).filter(User.telegram_id == str(telegram_id)).first()
    display_name = html_escape(f"{first_name or ''} {last_name or ''}".strip() or username or f"User_{telegram_id}")
    username = html_escape(username) if username else ""
    
    if not user:
        user = User(
            telegram_id=str(telegram_id),
            username=username,
            display_name=display_name,
            wallet_balance=0.0,
            city="India",
            role="user"
        )
        db.add(user)
        db.commit()
        if sse_broadcast_callback:
            try:
                asyncio.create_task(sse_broadcast_callback({
                    "type": "new_user",
                    "user_id": user.id,
                    "telegram_id": user.telegram_id,
                    "username": user.username,
                    "display_name": user.display_name,
                    "wallet_balance": user.wallet_balance
                }))
            except Exception as e:
                logger.error(f"[SSE Broadcast Error] Failed to send new_user event: {e}")
        # Notify admin of new user registration
        try:
            uname_display = f"@{username}" if username else "No username"
            new_user_admin_text = (
                "🆕 <b>New User Registered!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>Name:</b> {display_name}\n"
                f"📱 <b>Username:</b> {uname_display}\n"
                f"🆔 <b>Telegram ID:</b> <code>{telegram_id}</code>\n"
                f"💰 <b>Starting Wallet:</b> ₹0.00"
            )
            asyncio.create_task(notify_admins(db, new_user_admin_text))
        except Exception as e:
            logger.error(f"[Admin Notification] Failed to notify admin of new user: {e}")
    else:
        # Check if username or display name has changed and update them
        changed = False
        if user.username != username:
            user.username = username
            changed = True
        if user.display_name != display_name:
            user.display_name = display_name
            changed = True
        if changed:
            db.commit()

    # Sync profile photo asynchronously in background
    asyncio.create_task(sync_user_profile_photo(str(telegram_id), str(user.id)))

    # Restore or sync session state live from DB
    saved_cart = {}
    temp_address = None
    temp_phone = None
    if user.bot_cart:
        try:
            parsed = json.loads(user.bot_cart)
            if isinstance(parsed, dict) and "cart" in parsed:
                saved_cart = parsed.get("cart", {})
                temp_address = parsed.get("temp_address")
                temp_phone = parsed.get("temp_phone")
            else:
                saved_cart = parsed
        except Exception:
            pass

    if str(telegram_id) not in USER_BOT_SESSION:
        USER_BOT_SESSION[str(telegram_id)] = {
            "state": user.bot_state,
            "cart": saved_cart,
            "temp_address": temp_address,
            "temp_phone": temp_phone
        }
    else:
        session = USER_BOT_SESSION[str(telegram_id)]
        if not session.get("state") and user.bot_state is not None:
            session["state"] = user.bot_state
        if ("cart" not in session or not session.get("cart")) and saved_cart:
            session["cart"] = saved_cart

    session = USER_BOT_SESSION[str(telegram_id)]

    is_media = (photo is not None) or (document is not None)
    current_state = session.get("state") or ""
    if is_media and current_state != "waiting_for_support_message" and not current_state.startswith("admin_waiting_order_screenshot_"):
        await send_bot_message(
            user.telegram_id,
            "📷 <b>Media received!</b>\n\nTo attach an image to a support message, please tap <b>📞 Contact Support</b> first. If you want to attach a screenshot to an order, please do so from the admin panel."
        )
        return


    text = str(text) if text is not None else ""
    text_clean = text.strip()
    text_lower = text_clean.lower()

    if user and user.role == "admin" and user.admin_expires_at:
        if datetime.datetime.utcnow() > user.admin_expires_at:
            user.role = "user"
            user.admin_expires_at = None
            db.commit()
            logger.info(f"Demoted user {user.display_name} due to expired admin role duration.")

    admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
    is_admin = str(telegram_id) == str(admin_tg_id) or (user and user.role == "admin")

    main_keyboard = {
        "keyboard": [
            [{"text": "🍕 View Menu"}, {"text": "💰 My Wallet"}],
            [{"text": "📍 Change Location"}, {"text": "📦 Track Orders"}],
            [{"text": "🎉 Active Offers"}, {"text": "💬 Contact Support"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    if is_admin:
        main_keyboard["keyboard"].append([
            {"text": "🔑 Admin Center"}
        ])

    # Handle Cancel / Back command — context-aware fallback
    if text and text.strip().lower() in ("❌ cancel", "cancel", "🔙 back", "back", "🏠 main menu"):
        prev_state = session.get("state")
        session["state"] = None
        user.bot_state = None
        try:
            db.commit()
        except Exception:
            pass

        if prev_state == "waiting_for_support_message":
            session["support_relation"] = None
            await send_bot_message(user.telegram_id, "❌ <b>Support message cancelled.</b>", reply_markup=main_keyboard)
            return
        
        # Admin cancels an input while in Order Editor wizards
        if prev_state and any(prev_state.startswith(x) for x in ["admin_waiting_edit_ref_", "admin_waiting_store_", "admin_waiting_rider_name_", "admin_waiting_rider_phone_", "admin_waiting_order_screenshot_", "admin_waiting_ref_"]):
            order_id = None
            for prefix in ["admin_waiting_edit_ref_", "admin_waiting_store_", "admin_waiting_rider_name_", "admin_waiting_rider_phone_", "admin_waiting_order_screenshot_", "admin_waiting_ref_"]:
                if prev_state.startswith(prefix):
                    order_id = prev_state.replace(prefix, "").strip()
                    break
            if order_id:
                await send_bot_message(user.telegram_id, "❌ Action cancelled.", reply_markup=main_keyboard)
                await send_admin_order_details(user.telegram_id, order_id, db)
                return
                
        elif prev_state and prev_state.startswith("admin_waiting_wallet_adj_"):
            target_id = prev_state.replace("admin_waiting_wallet_adj_", "").strip()
            target_user = db.query(User).filter(User.id == target_id).first()
            if target_user:
                await send_bot_message(user.telegram_id, "❌ Action cancelled.", reply_markup=main_keyboard)
                
                # Show user details console
                await send_admin_user_details(user.telegram_id, target_user.id, db)
                return
                
        elif prev_state in ("admin_waiting_promo_code", "admin_waiting_promo_value", "admin_waiting_promo_limit"):
            await send_bot_message(user.telegram_id, "❌ Action cancelled.", reply_markup=main_keyboard)
            
            # Show promo code menu
            limit = 5
            offset = 0
            total_coupons = db.query(Coupon).count()
            import math
            total_pages = max(1, math.ceil(total_coupons / limit))
            coupons = db.query(Coupon).order_by(Coupon.created_at.desc()).offset(offset).limit(limit).all()
            
            msg = f"🎟️ <b>Promo Codes Management (Page 1/{total_pages}):</b>\n\n"
            buttons = []
            if not coupons:
                msg += "<i>No promo codes created yet.</i>\n"
            else:
                for c in coupons:
                    status = "🟢 Active" if (c.is_active and c.redeemed_count < c.usage_limit) else "🔴 Inactive"
                    msg += f"• <b>Code:</b> <code>{c.code}</code>\n  Value: ₹{c.value:.2f} | Limit: {c.redeemed_count}/{c.usage_limit} | Status: {status}\n\n"
                    buttons.append([
                        {"text": f"❌ Delete {c.code}", "callback_data": f"admin_promo_delete_{c.id}"}
                    ])
            nav_row = []
            if total_pages > 1:
                nav_row.append({"text": "Next ➡️", "callback_data": "admin_promo_page_2"})
            if nav_row:
                buttons.append(nav_row)
            buttons.append([
                {"text": "➕ Create Promo Code", "callback_data": "admin_promo_create"},
                {"text": "🔙 Back", "callback_data": "admin_refresh_stats"}
            ])
            await send_bot_message(user.telegram_id, msg, reply_markup={"inline_keyboard": buttons})
            return
            
        elif prev_state in ("admin_waiting_upi_id", "admin_waiting_upi_name"):
            await send_bot_message(user.telegram_id, "❌ Action cancelled.", reply_markup=main_keyboard)
            
            # Show system config panel
            upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
            upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
            maint_cfg = db.query(SystemConfig).filter(SystemConfig.key == "maintenance_mode").first()
            upi_id = upi_id_cfg.value if upi_id_cfg else "pranjalottery@fam"
            upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
            maint_val = maint_cfg.value if maint_cfg else "false"
            maint_status = "⚠️ MAINTENANCE ON" if maint_val == "true" else "🟢 ONLINE"
            
            msg = (
                f"⚙️ <b>System Configuration Control Panel</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"• <b>UPI ID:</b> <code>{upi_id}</code>\n"
                f"• <b>UPI Name:</b> <code>{upi_name}</code>\n"
                f"• <b>Platform Status:</b> <code>{maint_status}</code>\n\n"
                f"<i>Use the settings below to adjust system parameters directly in real-time:</i>"
            )
            buttons = [
                [
                    {"text": "💳 Update UPI ID", "callback_data": "admin_conf_upi_id"},
                    {"text": "👤 Update UPI Name", "callback_data": "admin_conf_upi_name"}
                ],
                [
                    {"text": "🛠️ Toggle Maintenance", "callback_data": "admin_toggle_maintenance"}
                ],
                [
                    {"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}
                ]
            ]
            await send_bot_message(user.telegram_id, msg, reply_markup={"inline_keyboard": buttons})
            return
            


        # Fallback to checkout details if we were in checkout
        if session.get("checkout_pending") or prev_state in ("waiting_for_address", "waiting_for_phone", "waiting_for_confirm"):
            session["checkout_pending"] = False
            session["temp_address"] = None
            session["temp_phone"] = None
            if user.latitude is None or user.longitude is None:
                cart = session.get("cart", {})
                cart_text, cart_markup = render_cart_message(db, user, cart, session)
                await send_bot_message(user.telegram_id, cart_text, reply_markup=cart_markup)
            else:
                await initiate_checkout(db, user, session)
            return
            
        # Fallback to location settings menu if we were updating location details (sub-views)
        if prev_state in ("waiting_for_address", "waiting_for_phone_update"):
            await display_delivery_location_menu(db, user)
            return
            
        # Default fallback: Home main menu
        session["checkout_pending"] = False
        session["temp_address"] = None
        session["temp_phone"] = None
        session["temp_lat"] = None
        session["temp_lon"] = None
        session["active_deal"] = None
        await send_bot_message(
            user.telegram_id,
            f"🏠 <b>Main Menu</b>\n\nHello {user.display_name}! What would you like to do?\n\n"
            f"💰 Wallet: <b>₹{user.wallet_balance:.2f}</b>  •  "
            f"📍 Location: <b>{user.city or 'Not set'}</b>",
            reply_markup=main_keyboard
        )
        return

    # Ensure user has a valid default city
    if not user.city:
        user.city = "India"
        db.commit()

    # Handle shared Telegram location message
    if location:
        # send_bot_message returns the integer message_id on success, True in mock mode, or False on failure
        status_msg_id = await send_bot_message(user.telegram_id, "📍 <b>Resolving GPS location...</b>")
        await asyncio.sleep(0.5)
        
        lat = location.get("latitude")
        lon = location.get("longitude")
        city = await reverse_geocode(lat, lon)
        
        if not city:
            # Reverse geocode failed — save coords but mark city as unknown
            city = "GPS Location"
            logger.warning(f"[Bot Location] reverse_geocode returned None for ({lat}, {lon}), using fallback")

        old_city = user.city
        saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
        has_doorstep = (
            saved_addr and saved_addr.full_address
            and saved_addr.full_address != "GPS Location"
            and len(saved_addr.full_address.strip()) > 3
        )
        
        user.city = city
        user.latitude = lat
        user.longitude = lon
        db.commit()
        
        # Save/update Default SavedAddress in database
        default_full_addr = (
            (city if (city and city != "GPS Location") else None)
            or (user.address and str(user.address).strip())
            or (f"GPS Location ({lat:.4f}, {lon:.4f})" if (lat is not None and lon is not None) else None)
        ) or "GPS Location"
        if not saved_addr:
            saved_addr = SavedAddress(
                user_id=user.id,
                label="Home",
                full_address=default_full_addr,
                is_default=True,
                latitude=lat,
                longitude=lon,
                city=city
            )
            db.add(saved_addr)
            
        if not has_doorstep or not saved_addr.full_address or not saved_addr.full_address.strip():
            saved_addr.full_address = default_full_addr
            
        saved_addr.latitude = lat
        saved_addr.longitude = lon
        saved_addr.city = city
        db.commit()
        auto_save_persistent_db_state(db)
        
        session["state"] = None
        # Only force address re-entry if they changed cities or lack a doorstep address
        if not old_city or old_city.lower() != city.lower() or not has_doorstep:
            session["force_address_entry"] = True
        else:
            session["force_address_entry"] = False

        # Update the status message if we got a real message_id back
        status_mid = status_msg_id if isinstance(status_msg_id, int) else None
        if status_mid:
            await edit_bot_message(user.telegram_id, status_mid, "🔄 <b>Syncing Domino's store menu for your area...</b>")
            await asyncio.sleep(0.4)
        
        try:
            from .services.dominos_service import sync_realtime_menu, sync_realtime_menu_bg
            # Fetch store-specific menu and dynamic pricing based on exact GPS coordinates
            if db.query(Product).count() > 5:
                asyncio.create_task(sync_realtime_menu_bg(city, lat=lat, lon=lon))
            else:
                await sync_realtime_menu(city, db, lat=lat, lon=lon)
        except Exception as e:
            logger.error(f"Error syncing menu in bot location handler: {e}")

        if status_mid:
            await edit_bot_message(user.telegram_id, status_mid, "✅ <b>Location & Menu Synced!</b>")
            await asyncio.sleep(0.4)
            await delete_bot_message(user.telegram_id, status_mid)
        
        if session.get("checkout_pending") or session.get("cart"):
            session["checkout_pending"] = False
            await initiate_checkout(db, user, session)
        else:
            city_disp = f" ({city})" if city and city != "GPS Location" else ""
            await send_bot_message(
                user.telegram_id, 
                f"✅ <b>Location coordinates saved!</b>{city_disp}\nStore pricing and parameters updated automatically."
            )
            await display_delivery_location_menu(db, user)
        return

    # Reset waiting state if user sends a command or main keyboard button
    text_clean = text.strip() if text else ""
    text_lower = text_clean.lower()
    is_action_command = (
        text_clean.startswith("/") or
        text_lower in [
            "🍕 view menu", "💰 my wallet", "📍 change location", "📦 track orders",
            "💬 contact support", "🍕 order app", "🍕 order app (link)",
            "🌐 open admin portal", "🌐 open admin portal (link)", "🔑 get admin secret key",
            "💳 add funds", "wallet_add", "🎉 active offers", "🎫 add promo code", "wallet_promo"
        ]
    )
    if is_action_command:
        session["state"] = None

    # Preset amount checks or wallet cancel checks
    is_preset_amount = False
    preset_val = 0.0
    if text_clean in ["₹50", "₹100", "₹200", "₹500", "₹1000"]:
        is_preset_amount = True
        preset_val = float(text_clean.replace("₹", ""))

    if is_preset_amount:
        session["state"] = None
        session["topup_amount"] = preset_val
        confirm_text = (
            f"📋 <b>Confirm Deposit Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 Amount to Deposit: <b>₹{preset_val:.2f}</b>\n\n"
            f"Are you sure you want to proceed with this deposit?"
        )
        confirm_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ Yes, Confirm", "callback_data": f"wallet_confirm_deposit_{preset_val}"},
                    {"text": "❌ Cancel", "callback_data": "wallet_cancel_deposit_unconfirmed"}
                ]
            ]
        }
        res = await send_bot_message(user.telegram_id, confirm_text, reply_markup=confirm_markup)
        if isinstance(res, int):
            session["last_bot_msg_id"] = res
        return

    elif text_clean == "Custom Amount":
        session["state"] = "waiting_for_topup_amount"
        res = await send_bot_message(
            user.telegram_id,
            "💳 <b>Enter Custom Amount</b>\n\nPlease type the amount in Rupees you would like to add (e.g. 150):",
            reply_markup=main_keyboard
        )
        if isinstance(res, int):
            session["last_bot_msg_id"] = res
        return

    elif text_clean in ("❌ Cancel", "cancel", "🔙 Back", "back", "/cancel") or text_lower.startswith("/cancel"):
        prev_state = session.get("state", "") or ""
        session["state"] = None
        
        # If in Admin Offer creation/editing state, return cleanly to Admin Offers Menu
        if is_admin and (isinstance(prev_state, str) and prev_state.startswith("admin_waiting_offer")):
            offers = db.query(ActiveOffer).order_by(ActiveOffer.sort_order.asc()).all()
            msg = "🎉 <b>Active Offers & Deals Management</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            buttons = []
            for o in offers:
                status_icon = "🟢 ACTIVE" if o.is_active else "🔴 DISABLED"
                badge_str = f" [{o.badge}]" if o.badge else ""
                msg += f"• <b>{escape_html(o.title)}</b>{badge_str} ({status_icon})\n  └ Price: <b>₹{o.discounted_price:.2f}</b>\n\n"
                toggle_txt = "🔴 Disable" if o.is_active else "🟢 Enable"
                buttons.append([
                    {"text": f"{toggle_txt}", "callback_data": f"admin_offer_toggle_{o.id}"},
                    {"text": "✏️ Price", "callback_data": f"admin_offer_price_{o.id}"},
                    {"text": "🏷️ Badge", "callback_data": f"admin_offer_badge_{o.id}"},
                    {"text": "❌ Delete", "callback_data": f"admin_offer_del_{o.id}"}
                ])
            buttons.append([{"text": "➕ Create New Deal", "callback_data": "admin_offer_create_start"}])
            buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
            await send_bot_message(user.telegram_id, msg, reply_markup={"inline_keyboard": buttons})
            return

        if session.get("checkout_pending") or session.get("cart"):
            session["checkout_pending"] = False
            cart = session.get("cart", {})
            if cart:
                await initiate_checkout(db, user, session)
            else:
                await send_bot_message(user.telegram_id, "❌ <b>Action cancelled.</b>", reply_markup=main_keyboard)
        else:
            cancel_confirm = (
                "❌ <b>Action Cancelled</b>\n\n"
                "Your active request has been cancelled successfully."
            )
            await send_bot_message(user.telegram_id, cancel_confirm, reply_markup=main_keyboard)
        return

    elif text_clean == "🎫 Add Promo Code" or text_lower == "wallet_promo":
        session["state"] = "waiting_for_promo_code"
        await send_bot_message(
            user.telegram_id,
            "🎫 <b>Enter Promo Code</b>\n\nPlease type your voucher code below:",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return


    # --- 1. Promo Code (Voucher) Redemption ---
    code_cleaned = text_clean.replace("-", "").replace(" ", "").upper()
    
    # 1.1 Auto-redeem 16-digit Gift Cards if directly pasted in chat
    if len(code_cleaned) == 16 and code_cleaned.isalnum():
        import hashlib
        code_hash = hashlib.sha256(code_cleaned.encode("utf-8")).hexdigest()
        gc = db.query(GiftCard).filter(GiftCard.code_hash == code_hash, GiftCard.status == "available").first()
        if gc:
            # Mark gift card as used
            gc.status = "used"
            gc.used_by_user_id = user.id
            gc.used_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            
            # Increase user wallet balance
            user.wallet_balance += gc.value
            
            # Record ledger transaction
            tx = WalletTransaction(
                user_id=user.id,
                type="deposit",
                amount=gc.value,
                description=f"Redeemed gift card: {code_cleaned[:4]}************"
            )
            db.add(tx)
            db.commit()
            
            success_text = (
                f"💳 <b>Gift Card Redeemed!</b>\n\n"
                f"• Code: <code>{code_cleaned[:4]}************</code>\n"
                f"• Value: <b>₹{gc.value:.2f}</b>\n"
                f"• New Wallet Balance: <b>₹{user.wallet_balance:.2f}</b>\n\n"
                f"Your balance has been topped up instantly!"
            )
            await send_bot_message(user.telegram_id, success_text, reply_markup=main_keyboard)
            return
        else:
            await send_bot_message(user.telegram_id, "❌ Invalid or already used Gift Card code. Please check and try again.")
            return
    is_promo_candidate = (
        (session.get("state") == "waiting_for_promo_code" and not is_action_command) or
        (session.get("state") == "waiting_for_promo_code" and len(code_cleaned) >= 4)
    )
    if is_promo_candidate:
        code = code_cleaned
        
        # Look up coupon in database
        coupon = db.query(Coupon).filter(
            Coupon.code == code,
            Coupon.is_active == True
        ).first()
        
        if not coupon:
            await send_bot_message(user.telegram_id, "❌ Invalid or inactive promo code. Please check and try again.")
            return
            
        # Check overall usage limit
        if coupon.redeemed_count >= coupon.usage_limit:
            await send_bot_message(user.telegram_id, "❌ This promo code usage limit has been reached.")
            return
            
        # Check if already redeemed by this user
        existing_redemption = db.query(CouponRedemption).filter(
            CouponRedemption.coupon_id == coupon.id,
            CouponRedemption.user_id == user.id
        ).first()
        if existing_redemption:
            await send_bot_message(user.telegram_id, "❌ You have already redeemed this promo code once.")
            return
            
        # Perform Redemption
        coupon.redeemed_count += 1
        redemption = CouponRedemption(coupon_id=coupon.id, user_id=user.id)
        db.add(redemption)
        
        user.wallet_balance += coupon.value
        
        # Log WalletTransaction
        tx = WalletTransaction(
            user_id=user.id,
            type="deposit",
            amount=coupon.value,
            description=f"Redeemed promo code: {code}"
        )
        db.add(tx)
        db.commit()
        
        session["state"] = None
        
        success_text = (
            f"🎫 <b>Promo Code Redeemed!</b>\n\n"
            f"• Code: <code>{code}</code>\n"
            f"• Voucher Value: <b>₹{coupon.value:.2f}</b>\n"
            f"• New Wallet Balance: <b>₹{user.wallet_balance:.2f}</b>\n\n"
            f"Your balance has been topped up instantly!"
        )
        await send_bot_message(user.telegram_id, success_text, reply_markup=main_keyboard)
        
        # Notify admin via Telegram
        admin_text = (
            "🔔 <b>Promo Code Redeemed</b>\n\n"
            f"👤 <b>User:</b> {user.display_name} (ID: {user.telegram_id})\n"
            f"🎫 <b>Code:</b> <code>{code}</code>\n"
            f"💰 <b>Value:</b> ₹{coupon.value:.2f}\n"
            f"📊 <b>Usage:</b> {coupon.redeemed_count}/{coupon.usage_limit}"
        )
        await notify_admins(db, admin_text)
        
        # Broadcast SSE
        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "wallet_update", "user_id": user.id, "balance": user.wallet_balance})
            except Exception:
                pass
        return



    if text_clean in (
        "✅ Confirm Details & Return to Cart", "✅ Confirm Location & View Menu",
        "🛒 Return to Cart", "✅ Confirm Details", "✅ Confirm Location"
    ):
        session["state"] = None
        if session.get("cart"):
            await initiate_checkout(db, user, session)
        else:
            await display_pizza_menu(db, user, reply_markup=main_keyboard)
        return

    if text_clean in (
        "🏠 Edit Delivery Address", "⚠️ 🏠 Enter Doorstep Address",
        "🏠 Update Delivery Address", "update delivery address", "update doorstep address"
    ):
        session["state"] = "waiting_for_address_update"
        await send_bot_message(
            user.telegram_id,
            "🏠 <b>Update Doorstep Delivery Address</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Please type your full doorstep delivery address below (e.g. <code>Flat 402, Sunshine Apartments, MG Road</code>):\n\n"
            "<i>Note: Your GPS coordinates remain saved separately.</i>",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    if text_clean in (
        "📱 Edit Phone Number", "⚠️ 📱 Enter Mobile Number",
        "📱 Update Phone Number", "update phone number"
    ):
        session["state"] = "waiting_for_phone_update"
        current_phone = f"Current phone: <code>{user.phone}</code>\n\n" if user.phone else ""
        await send_bot_message(
            user.telegram_id,
            f"📱 <b>Update Phone Number</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n{current_phone}"
            "Please type your contact mobile number (e.g. <code>+919876543210</code>):",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    if session.get("state") in ("waiting_for_phone", "waiting_for_phone_update"):
        phone_raw = text.strip().replace(" ", "").replace("-", "") if text else ""
        digits_only = "".join(c for c in phone_raw if c.isdigit())
        if not (10 <= len(digits_only) <= 15):
            await send_bot_message(
                user.telegram_id,
                "❌ <b>Invalid format.</b> Please enter a valid number with country code.\n"
                "Format: <code>+91XXXXXXXXXX</code>",
            )
            return
        phone_formatted = ("+" + digits_only) if phone_raw.startswith("+") else ("+91" + digits_only if len(digits_only) == 10 else "+" + digits_only)
        user.phone = phone_formatted
        db.commit()
        
        is_checkout = session.get("checkout_pending") or (session.get("state") == "waiting_for_phone")
        session["state"] = None
        
        if is_checkout:
            session["temp_phone"] = phone_formatted
            session["checkout_pending"] = False
            sync_user_db_session(db, user, session)
            await send_bot_message(user.telegram_id, "✅ Phone verified.", reply_markup={"remove_keyboard": True})
            await initiate_checkout(db, user, session)
            return
            
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Phone updated to <code>{phone_formatted}</code></b>\n\nThis number will be used for all future orders.",
            reply_markup={"remove_keyboard": True}
        )
        await display_delivery_location_menu(db, user)
        return

    state = session.get("state") or ""
    if state.startswith("admin_replying_to_"):
        target_tg_id = state.replace("admin_replying_to_", "").strip()
        text_clean_lower = text_clean.lower()
        if text_clean_lower in ("cancel", "❌ cancel", "/cancel"):
            session["state"] = None
            await send_bot_message(user.telegram_id, "❌ <b>Admin reply cancelled.</b>")
            return

        reply_text = text.strip() if text else ""
        file_id = None
        attachment_type = None
        if photo:
            largest = max(photo, key=lambda p: p.get("file_size", 0))
            file_id = largest["file_id"]
            attachment_type = "photo"
            if not reply_text:
                reply_text = "[Photo Attachment]"
        elif document:
            file_id = document.get("file_id")
            attachment_type = "document"
            if not reply_text:
                reply_text = f"[Document: {document.get('file_name', 'Attachment')}]"

        if not reply_text and not file_id:
            await send_bot_message(user.telegram_id, "⚠️ Please send a valid message text or attachment.")
            return

        target_user = db.query(DbUser).filter(
            (DbUser.telegram_id == target_tg_id) | (DbUser.id == target_tg_id)
        ).first()

        if target_user:
            try:
                sup = SupportMessage(
                    user_id=target_user.id,
                    sender_type="admin",
                    message=reply_text,
                    attachment_file_id=file_id,
                    attachment_type=attachment_type
                )
                db.add(sup)
                db.commit()
                if sse_broadcast_callback:
                    try:
                        await sse_broadcast_callback({
                            "type": "support_message",
                            "user_id": target_user.id,
                            "sender_type": "admin",
                            "message": reply_text,
                            "created_at": sup.created_at.isoformat()
                        })
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Could not save support message reply: {e}")

            cust_msg = f"💬 <b>Support Agent Reply:</b>\n{reply_text}"
            if file_id:
                sent_ok = await send_bot_photo(target_user.telegram_id, file_id, caption=cust_msg)
            else:
                sent_ok = await send_bot_message(target_user.telegram_id, cust_msg)

            session["state"] = None
            if sent_ok:
                await send_bot_message(
                    user.telegram_id,
                    f"✅ <b>Reply sent to customer</b> (TG ID: <code>{target_user.telegram_id}</code>)."
                )
            else:
                await send_bot_message(
                    user.telegram_id,
                    f"⚠️ <b>Saved to database, but failed to deliver via TG bot</b> (User TG ID: <code>{target_user.telegram_id}</code>)."
                )
        else:
            session["state"] = None
            await send_bot_message(user.telegram_id, f"❌ Target user (TG ID: {target_tg_id}) not found.")
        return

    if session.get("state") == "waiting_for_support_message":
        text_clean_lower = text_clean.lower()
        if text_clean_lower in ("cancel", "❌ cancel", "back", "🔙 back", "/cancel", "/start", "main menu", "🍕 view menu", "💰 my wallet", "📍 change location", "📦 track orders", "💬 contact support"):
            session["state"] = None
            session["support_relation"] = None
            await send_bot_message(
                user.telegram_id,
                "❌ <b>Support message cancelled.</b>",
                reply_markup=main_keyboard
            )
            return

        msg_text = text.strip() if text else ""
        if not photo and not document and not msg_text:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Empty message.</b>\n\nPlease enter text or attach an image/file to send to support.",
            )
            return

        # Rate Limit Check: Max 3 consecutive user support messages without admin reply
        last_admin_msg = db.query(SupportMessage).filter(
            SupportMessage.user_id == user.id,
            SupportMessage.sender_type == "admin"
        ).order_by(SupportMessage.created_at.desc()).first()

        if last_admin_msg:
            unreplied_count = db.query(SupportMessage).filter(
                SupportMessage.user_id == user.id,
                SupportMessage.sender_type == "user",
                SupportMessage.created_at > last_admin_msg.created_at
            ).count()
        else:
            unreplied_count = db.query(SupportMessage).filter(
                SupportMessage.user_id == user.id,
                SupportMessage.sender_type == "user"
            ).count()

        if unreplied_count >= 3:
            session["state"] = None
            session["support_relation"] = None
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Support Rate Limit Reached</b>\n\n"
                "You have sent 3 consecutive support messages without a reply. "
                "Please wait for our support team to respond before sending additional messages.",
                reply_markup=main_keyboard
            )
            return
        
        file_id = None
        attachment_type = None
        if photo:
            largest = max(photo, key=lambda p: p.get("file_size", 0))
            file_id = largest["file_id"]
            attachment_type = "photo"
            if not msg_text:
                msg_text = "[Image Attachment]"
        elif document:
            file_id = document.get("file_id")
            attachment_type = "document"
            if not msg_text:
                msg_text = f"[Document: {document.get('file_name', 'Attachment')}]"
                
        try:
            sup = SupportMessage(
                user_id=user.id,
                sender_type="user",
                message=msg_text,
                attachment_file_id=file_id,
                attachment_type=attachment_type
            )
            db.add(sup)
            db.commit()
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({
                        "type": "support_message",
                        "user_id": user.id,
                        "message": msg_text,
                        "display_name": user.display_name,
                        "has_attachment": file_id is not None
                    })
                except Exception:
                    pass
        except Exception as db_err:
            logger.warning(f"Could not save support message: {db_err}")
        
        session["state"] = None
        await send_bot_message(
            user.telegram_id,
            "✅ <b>Support message sent!</b>\n\n"
            "Our team has received your message and will reply directly in this chat shortly.\n\n"
            "<i>Your message:</i>\n" + f"<blockquote>{escape_html(msg_text[:300])}</blockquote>",
            reply_markup=main_keyboard
        )
        
        # Forward to admin
        admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
        relation = session.get("support_relation", "General Query")
        session["support_relation"] = None
        
        # Build relation detail for admin
        relation_detail = f"<b>{escape_html(relation)}</b>"
        if relation.startswith("Order: "):
            oid = relation.replace("Order: ", "").strip()
            bg_order = db.query(Order).filter(Order.id == oid).first()
            if bg_order:
                relation_detail += f" (Status: {bg_order.status}, Paid: ₹{bg_order.total_payable:.2f}, Phone: {bg_order.phone})"
                
        admin_ticket_text = (
            f"💬 <b>Support Ticket from {escape_html(user.display_name)}</b>\n"
            f"• User ID: <code>{user.id}</code>\n"
            f"• Telegram ID: <code>{user.telegram_id}</code>\n"
            f"• Username: @{user.username or '—'}\n"
            f"• Phone Number: <code>{user.phone or '—'}</code>\n"
            f"• Relates to: {relation_detail}\n\n"
            f"✉️ <b>Message:</b>\n"
            f"<blockquote>{escape_html(msg_text)}</blockquote>"
        )
        admin_ticket_markup = {
            "inline_keyboard": [
                [{"text": "💬 Custom Reply", "callback_data": f"admin_reply_support_{user.telegram_id}"}],
                [
                    {"text": "📋 Order Placed", "callback_data": f"admin_tmpl_placed_{user.telegram_id}"},
                    {"text": "💸 Refund Done", "callback_data": f"admin_tmpl_refund_{user.telegram_id}"}
                ],
                [
                    {"text": "⚠️ Payment Issue", "callback_data": f"admin_tmpl_utr_{user.telegram_id}"},
                    {"text": "🕒 Delay Alert", "callback_data": f"admin_tmpl_delay_{user.telegram_id}"}
                ]
            ]
        }
        if file_id:
            await send_bot_photo(admin_tg_id, file_id, caption=admin_ticket_text, reply_markup=admin_ticket_markup)
        else:
            await send_bot_message(admin_tg_id, admin_ticket_text, reply_markup=admin_ticket_markup)
        return

    if (session.get("state") or "").startswith("waiting_for_utr_"):
        order_id = session.get("state").replace("waiting_for_utr_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        utr_raw = text.strip() if text else ""
        text_lower = utr_raw.lower()

        if text_lower in ("cancel", "❌ cancel", "/cancel"):
            session["state"] = None
            await send_bot_message(user.telegram_id, "❌ UTR submission cancelled.", reply_markup=main_keyboard)
            return

        if text_lower in ("skip", "skip utr", "no utr", "/skip"):
            utr_code = f"NO-UTR-{uuid.uuid4().hex[:6].upper()}"
        else:
            clean_utr = "".join(c for c in utr_raw if c.isalnum()).upper()
            if len(clean_utr) < 10 or len(clean_utr) > 18:
                await send_bot_message(
                    user.telegram_id,
                    "⚠️ <b>Invalid UTR Format</b>\n\n"
                    "A valid UPI UTR / Transaction Reference ID is usually 12 digits (e.g. <code>423456789012</code>).\n\n"
                    "Please reply with your exact 12-digit UTR number, or tap <b>⏩ Skip UTR</b> below.",
                    reply_markup={
                        "inline_keyboard": [
                            [{"text": "⏩ Skip UTR", "callback_data": f"pay_skip_utr_{order_id}"}],
                            [{"text": "❌ Cancel", "callback_data": f"cancel_order_{order_id}"}]
                        ]
                    }
                )
                return
            utr_code = clean_utr

            # Check duplicate verified UTR
            duplicate_utr = db.query(VerifiedUTR).filter(VerifiedUTR.utr == utr_code).first()
            if duplicate_utr and duplicate_utr.order_id != order_id:
                await send_bot_message(
                    user.telegram_id,
                    "⚠️ <b>Duplicate UTR Detected</b>\n\n"
                    f"The UTR <code>{utr_code}</code> has already been verified for another order.\n"
                    "Please check your UPI app transaction details and submit the correct UTR."
                )
                return

        if order:
            order.status = "Pending Verification"
            order.transaction_id = utr_code

            attempt = UTRAttempt(
                order_id=order.id,
                utr=utr_code,
                is_successful=False
            )
            db.add(attempt)

            h = OrderStatusHistory(order_id=order.id, status="Pending Verification", note=f"Customer submitted UTR: {utr_code}")
            db.add(h)
            db.commit()
            auto_save_persistent_db_state(db)

            session["state"] = None

            is_topup = order.id.startswith("TOPUP-")
            if is_topup:
                success_msg = (
                    f"✅ <b>Deposit Payment Submitted!</b>\n\n"
                    f"• <b>Ref ID:</b> <code>{order.id}</code>\n"
                    f"• <b>UTR Ref:</b> <code>{utr_code}</code>\n"
                    f"• <b>Amount:</b> <b>₹{order.total_payable:.2f}</b>\n\n"
                    f"Your deposit request has been submitted for admin verification. Wallet balance will update once approved!"
                )
            else:
                success_msg = (
                    f"✅ <b>Payment Submitted for Verification!</b>\n\n"
                    f"• <b>Order ID:</b> <code>{order.id}</code>\n"
                    f"• <b>UTR Ref:</b> <code>{utr_code}</code>\n"
                    f"• <b>Total Paid:</b> <b>₹{order.total_payable:.2f}</b>\n\n"
                    f"Our team will verify your transaction shortly and start preparing your order!"
                )

            await send_bot_message(user.telegram_id, success_msg, reply_markup=main_keyboard)

            admin_text = (
                "🔔 <b>New Payment & UTR Submitted:</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🆔 <b>Ref ID:</b> <code>{order.id}</code>\n"
                f"👤 <b>User:</b> {user.display_name} (ID: <code>{user.telegram_id}</code>)\n"
                f"💰 <b>Amount:</b> ₹{order.total_payable:.2f}\n"
                f"🔢 <b>Submitted UTR:</b> <code>{utr_code}</code>\n"
            )
            if not is_topup:
                admin_text += (
                    f"🏡 <b>Address:</b> <code>{order.address}</code>\n"
                    f"📱 <b>Phone:</b> {order.phone}\n"
                )

            approve_cb = f"admin_dep_approve_{order.id}" if is_topup else f"admin_order_approve_{order.id}"
            reject_cb = f"admin_dep_reject_{order.id}" if is_topup else f"admin_order_reject_{order.id}"

            admin_markup = {
                "inline_keyboard": [
                    [
                        {"text": "✅ Approve Payment", "callback_data": approve_cb},
                        {"text": "❌ Reject Payment", "callback_data": reject_cb}
                    ]
                ]
            }
            asyncio.create_task(notify_admins(db, admin_text, reply_markup=admin_markup))
        return

    if session.get("state") == "waiting_for_promo_code":
        # Code was entered but not valid — the actual validation happens in promo_candidate block above
        await send_bot_message(user.telegram_id, "❌ Invalid or expired promo code. Please double-check the code and try again, or send /start to cancel.")
        return

    if session.get("state") == "waiting_for_order_note":
        note_text = text.strip() if text else ""
        if len(note_text) > 300:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Note too long.</b>\n\nPlease keep your order note under 300 characters.",
                reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
            )
            return
        if note_text.lower() in ("cancel", "skip", "❌ cancel"):
            session["state"] = "waiting_for_confirm"
            session["order_note"] = ""
            confirm_text, confirm_markup = render_order_confirmation_screen(db, user, session)
            await send_bot_message(user.telegram_id, confirm_text, reply_markup=confirm_markup)
            return
        
        session["order_note"] = note_text
        session["state"] = "waiting_for_confirm"
        confirm_text, confirm_markup = render_order_confirmation_screen(db, user, session)
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Note saved!</b>\n\n📝 <i>\"{note_text[:100]}{'...' if len(note_text) > 100 else ''}\"</i>\n\nHere's your updated order summary:",
        )
        await send_bot_message(user.telegram_id, confirm_text, reply_markup=confirm_markup)
        return



    if session.get("state") in ("waiting_for_address", "waiting_for_address_update"):
        addr_stripped = text.strip() if text else ""
        
        # Check if the input looks like GPS coordinates: e.g. "19.0760, 72.8777"
        import re
        coords_match = re.match(r"^\s*[-+]?[0-9]*\.?[0-9]+\s*,\s*[-+]?[0-9]*\.?[0-9]+\s*$", addr_stripped)
        if coords_match:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>It looks like you typed GPS coordinates.</b>\n\n"
                "Please enter your <b>written delivery address</b> (flat/house number, building, street name, city) instead, or click the location button to share location directly.",
                reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
            )
            return

        # Check if input looks like a phone number
        digits_only = "".join(c for c in addr_stripped if c.isdigit())
        if digits_only == addr_stripped.replace("+", "").replace("-", "").replace(" ", "") and len(digits_only) >= 10:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>It looks like you typed a mobile number.</b>\n\n"
                "Please enter your <b>written delivery address</b> (flat/house number, building, street name, city) instead.",
                reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
            )
            return

        # Validate: just check not empty
        if not addr_stripped:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Please enter a valid written address.</b>",
                reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
            )
            return
        session["temp_address"] = addr_stripped
        
        # Save doorstep delivery address to database without geocoding or coordinate modification
        saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
        if not saved_addr:
            saved_addr = SavedAddress(user_id=user.id, label="Home", full_address=addr_stripped, is_default=True)
            db.add(saved_addr)
        saved_addr.full_address = addr_stripped
        user.address = addr_stripped
        db.commit()
        auto_save_persistent_db_state(db)
        
        logger.info(f"[Bot Checkout] Saved doorstep address without geocoding: {addr_stripped}")
            
        if session.get("checkout_pending"):
            if user.phone:
                session["state"] = None
                session["checkout_pending"] = False
                await send_bot_message(user.telegram_id, "✅ Address verified.", reply_markup={"remove_keyboard": True})
                await initiate_checkout(db, user, session)
                return
            else:
                session["state"] = "waiting_for_phone"
                phone_keyboard = {
                    "keyboard": [[{"text": "❌ Cancel"}]],
                    "resize_keyboard": True,
                    "one_time_keyboard": True
                }
                await send_bot_message(
                    user.telegram_id,
                    f"📱 <b>Phone Number Required:</b>\n\n"
                    f"Please enter your contact number with country code.\n"
                    f"Format: <code>+91XXXXXXXXXX</code>",
                    reply_markup=phone_keyboard
                )
                return
        if session.get("cart"):
            session["state"] = None
            await initiate_checkout(db, user, session)
            return
        else:
            session["state"] = None
            await send_bot_message(
                user.telegram_id,
                "✅ <b>Delivery address updated successfully!</b>"
            )
            await display_delivery_location_menu(db, user)
            return

    elif session.get("state") and session.get("state").startswith("admin_waiting_order_screenshot_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        order_id = session.get("state").replace("admin_waiting_order_screenshot_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await send_bot_message(user.telegram_id, "❌ Order not found!")
            session["state"] = None
            return
            
        file_id = None
        if photo:
            largest = max(photo, key=lambda p: p.get("file_size", 0))
            file_id = largest["file_id"]
        elif document:
            file_id = document.get("file_id")
            
        if not file_id:
            await send_bot_message(user.telegram_id, "❌ Please send/upload a photo or document screenshot to attach to the order:")
            return
            
        order.screenshot_url = f"telegram_file:{file_id}"
        db.commit()
        
        session["state"] = None
        await send_bot_message(user.telegram_id, f"✅ <b>Receipt screenshot attached successfully to order {order_id}!</b>", reply_markup=main_keyboard)
        await send_admin_order_details(user.telegram_id, order_id, db)
        return

    elif session.get("state") == "admin_waiting_manual_credit_user":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        search_query = text.strip()
        clean_q = search_query.lstrip("@").strip()
        
        from sqlalchemy import or_
        matched_users = db.query(DbUser).filter(
            or_(
                DbUser.id == clean_q,
                DbUser.telegram_id == clean_q,
                DbUser.username.like(f"%{clean_q}%"),
                DbUser.display_name.like(f"%{clean_q}%")
            )
        ).limit(10).all()
        
        if not matched_users:
            await send_bot_message(
                user.telegram_id,
                f"❌ <b>No users found matching:</b> <code>{search_query}</code>. Please try again or type ❌ Cancel:",
            )
            return
            
        if len(matched_users) == 1:
            target_user = matched_users[0]
            session["state"] = f"admin_waiting_wallet_adj_{target_user.id}"
            cancel_keyboard = {
                "keyboard": [[{"text": "❌ Cancel"}]],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            await send_bot_message(
                user.telegram_id,
                f"💰 <b>Adjust Wallet Balance:</b>\n\n"
                f"👤 User: <b>{target_user.display_name}</b>\n"
                f"• Current Balance: <b>₹{target_user.wallet_balance:.2f}</b>\n\n"
                f"Please enter the amount to adjust (e.g. <code>+500</code> to credit or <code>-250</code> to debit):",
                reply_markup=cancel_keyboard
            )
            return
            
        msg = f"🔍 <b>Multiple matches found for:</b> <code>{search_query}</code>\n\nSelect a user below to adjust balance:\n\n"
        buttons = []
        for u in matched_users:
            msg += f"• <b>{u.display_name}</b> (Balance: ₹{u.wallet_balance:.2f} | ID: <code>{u.id}</code>)\n"
            buttons.append([{"text": f"💰 Credit {u.display_name[:15]}", "callback_data": f"admin_user_wallet_{u.id}"}])
            
        buttons.append([{"text": "🔙 Back to Payment Management", "callback_data": "admin_payment_management"}])
        await send_bot_message(user.telegram_id, msg, reply_markup={"inline_keyboard": buttons})
        session["state"] = None
        return

    elif session.get("state") == "admin_waiting_search_user":
        # Admin typed a user search query after pressing 'Search User' button
        if not is_admin:
            await send_bot_message(user.telegram_id, "\u274c Unauthorized!")
            return
        if text_clean.lower() in ("cancel", "\u274c cancel", "back", "\U0001f519 back"):
            session["state"] = None
            await send_bot_message(user.telegram_id, "\u274c Search cancelled.", reply_markup=main_keyboard)
            return
        search_query = text.strip()
        clean_q = search_query.lstrip("@").strip()
        from sqlalchemy import or_
        matched_users = db.query(DbUser).filter(
            or_(
                DbUser.id == clean_q,
                DbUser.telegram_id == clean_q,
                DbUser.username.like(f"%{clean_q}%"),
                DbUser.display_name.like(f"%{clean_q}%")
            )
        ).limit(10).all()
        if not matched_users:
            await send_bot_message(
                user.telegram_id,
                f"\u274c <b>No users found matching:</b> <code>{escape_html(search_query)}</code>\n\nPlease try again or send \u274c Cancel:",
            )
            return
        if len(matched_users) == 1:
            target_user = matched_users[0]
            session["state"] = None
            await send_admin_user_details(user.telegram_id, target_user.id, db)
            return
        # Multiple matches — show selection list
        msg = f"\U0001f50d <b>Multiple matches for:</b> <code>{escape_html(search_query)}</code>\n\nSelect a user below:\n\n"
        buttons = []
        for u in matched_users:
            uname_tag = f" (@{u.username})" if u.username else ""
            msg += f"\u2022 <b>{escape_html(u.display_name)}</b>{uname_tag} | Balance: \u20b9{u.wallet_balance:.2f} | ID: <code>{u.id}</code>\n"
            buttons.append([{"text": f"\U0001f464 {u.display_name[:20]}", "callback_data": f"admin_user_detail_{u.id}"}])
        buttons.append([{"text": "\U0001f519 Back to Users", "callback_data": "admin_users_list"}])
        await send_bot_message(user.telegram_id, msg, reply_markup={"inline_keyboard": buttons})
        session["state"] = None
        return

    elif session.get("state") == "admin_waiting_offer_title":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        title = text.strip()
        if len(title) < 3:
            await send_bot_message(user.telegram_id, "❌ Title too short. Please enter a valid deal title:")
            return
        session["new_offer_title"] = title
        session["state"] = "admin_waiting_offer_desc"
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Title saved:</b> <code>{escape_html(title)}</code>\n\n"
            f"Please enter the <b>included items / description</b> for this deal (e.g. <code>2x Medium Pizzas + Garlic Bread + Pepsi 1.25L</code>):",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    elif session.get("state") == "admin_waiting_offer_desc":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        desc = text.strip()
        session["new_offer_desc"] = desc
        session["state"] = "admin_waiting_offer_price"
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Description saved:</b> <i>{escape_html(desc)}</i>\n\n"
            f"Please enter the <b>discounted deal price</b> in ₹ (e.g. <code>399</code>):",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    elif session.get("state") == "admin_waiting_offer_price":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        try:
            price = float(text_clean.replace("₹", "").replace(",", "").strip())
            if price <= 0:
                raise ValueError()
        except ValueError:
            await send_bot_message(user.telegram_id, "❌ Invalid price. Please enter a positive number (e.g. <code>399</code>):")
            return

        session["new_offer_price"] = price
        session["state"] = "admin_waiting_offer_orig_price"
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Discounted price saved: ₹{price:.2f}</b>\n\n"
            f"Please enter the <b>original M.R.P. price</b> in ₹ to show strikethrough savings (e.g. <code>699</code>):",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    elif session.get("state") == "admin_waiting_offer_orig_price":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        try:
            orig_price = float(text_clean.replace("₹", "").replace(",", "").strip())
            if orig_price <= 0:
                raise ValueError()
        except ValueError:
            await send_bot_message(user.telegram_id, "❌ Invalid price. Please enter a positive number (e.g. <code>699</code>):")
            return

        session["new_offer_orig_price"] = orig_price
        session["state"] = "admin_waiting_offer_badge"
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Original M.R.P. saved: ₹{orig_price:.2f}</b>\n\n"
            f"Please enter the <b>promo badge text</b> (e.g. <code>🔥 45% OFF</code> or <code>⚡ BEST VALUE</code>), or send 'skip':",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    elif session.get("state") == "admin_waiting_offer_badge":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        badge_val = text.strip()
        badge = "" if badge_val.lower() == "skip" else badge_val

        import uuid
        title = session.get("new_offer_title", "New Deal")
        desc = session.get("new_offer_desc", f"{title} - Combo deal")
        price = float(session.get("new_offer_price", 199.0))
        orig_price = float(session.get("new_offer_orig_price", price * 1.5))
        key = f"deal_{uuid.uuid4().hex[:6]}"
        
        new_offer = ActiveOffer(
            offer_key=key,
            title=title,
            badge=badge if badge else "🔥 SPECIAL DEAL",
            description=desc,
            discounted_price=price,
            original_price=orig_price,
            button_text=f"🛒 {title[:25]} (₹{price:.0f})",
            is_active=True,
            sort_order=0
        )
        db.add(new_offer)
        db.commit()
        auto_save_persistent_db_state(db)
        
        session["state"] = None
        session["new_offer_title"] = None
        session["new_offer_desc"] = None
        session["new_offer_price"] = None
        session["new_offer_orig_price"] = None
        
        offer_markup = {
            "inline_keyboard": [
                [{"text": "🎉 Manage Active Offers", "callback_data": "admin_offers_menu"}],
                [{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]
            ]
        }
        await send_bot_message(
            user.telegram_id,
            f"🎉 <b>New Offer Created Successfully!</b>\n\n"
            f"• Title: <b>{escape_html(title)}</b>\n"
            f"• Items: <i>{escape_html(desc)}</i>\n"
            f"• Deal Price: <b>₹{price:.2f}</b> (Was ₹{orig_price:.2f})\n"
            f"• Badge: <b>{escape_html(badge if badge else 'None')}</b>\n"
            f"• Key: <code>{key}</code>\n\n"
            f"This offer is now live for all customers and backed up safely!",
            reply_markup=offer_markup
        )
        return

    elif session.get("state") and session.get("state").startswith("admin_waiting_offer_price_edit_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        off_id = session["state"].replace("admin_waiting_offer_price_edit_", "").strip()
        try:
            new_price = float(text_clean.replace("₹", "").replace(",", "").strip())
            if new_price <= 0: raise ValueError()
        except ValueError:
            await send_bot_message(user.telegram_id, "❌ Invalid price. Enter a positive number:")
            return
        
        off = db.query(ActiveOffer).filter((ActiveOffer.id == off_id) | (ActiveOffer.offer_key == off_id)).first()
        if off:
            off.discounted_price = new_price
            off.button_text = f"🛒 {off.title[:22]} (₹{new_price:.0f})"
            db.commit()
            auto_save_persistent_db_state(db)
            session["state"] = None
            offer_markup = {
                "inline_keyboard": [
                    [{"text": "🎉 Manage Active Offers", "callback_data": "admin_offers_menu"}]
                ]
            }
            await send_bot_message(user.telegram_id, f"✅ <b>Price updated to ₹{new_price:.2f}!</b>", reply_markup=offer_markup)
        return

    elif session.get("state") and session.get("state").startswith("admin_waiting_offer_badge_edit_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        off_id = session["state"].replace("admin_waiting_offer_badge_edit_", "").strip()
        new_badge = text.strip()
        off = db.query(ActiveOffer).filter((ActiveOffer.id == off_id) | (ActiveOffer.offer_key == off_id)).first()
        if off:
            off.badge = new_badge
            db.commit()
            auto_save_persistent_db_state(db)
            session["state"] = None
            offer_markup = {
                "inline_keyboard": [
                    [{"text": "🎉 Manage Active Offers", "callback_data": "admin_offers_menu"}]
                ]
            }
            await send_bot_message(user.telegram_id, f"✅ <b>Badge updated to '{escape_html(new_badge)}'!</b>", reply_markup=offer_markup)
        return

    elif session.get("state") == "waiting_for_topup_amount":
        if text_clean.lower() in ("cancel", "❌ cancel", "back", "🔙 back", "main menu", "🍕 view menu", "💰 my wallet", "📍 change location", "📦 track orders", "💬 contact support"):
            session["state"] = None
            session["topup_amount"] = None
            await send_bot_message(
                user.telegram_id,
                "❌ <b>Deposit request cancelled.</b>\n\nReturning to main menu.",
                reply_markup=main_keyboard
            )
            return

        if text_clean.lower() in ("custom amount", "custom", "custom_amount"):
            await send_bot_message(
                user.telegram_id,
                "💰 <b>Enter Custom Deposit Amount (₹):</b>\n\n"
                "Please type the amount in rupees you wish to deposit into your wallet:",
                reply_markup={"force_reply": True, "input_field_placeholder": "Enter amount in ₹ (e.g. 500)"}
            )
            return

        try:
            cleaned_val = text_clean.replace("₹", "").replace(",", "").strip()
            amount = float(cleaned_val)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            await send_bot_message(
                user.telegram_id,
                "❌ <b>Invalid Amount!</b>\n\nPlease enter a valid positive number for the amount (e.g. 200, 500) or send /cancel to return.",
                reply_markup={"force_reply": True, "input_field_placeholder": "Enter amount in ₹ (e.g. 500)"}
            )
            return
            
        session["state"] = None
        session["topup_amount"] = amount
        
        confirm_text = (
            f"📋 <b>Confirm Deposit Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 Amount to Deposit: <b>₹{amount:.2f}</b>\n\n"
            f"Are you sure you want to proceed with this deposit?"
        )
        confirm_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ Yes, Confirm", "callback_data": f"wallet_confirm_deposit_{amount}"},
                    {"text": "❌ Cancel", "callback_data": "wallet_view"}
                ]
            ]
        }
        res = await send_bot_message(user.telegram_id, confirm_text, reply_markup=confirm_markup)
        if isinstance(res, int):
            session["last_bot_msg_id"] = res
        return

    elif session.get("state") and session.get("state").startswith("admin_sending_user_msg_"):
        if not is_admin:
            session["state"] = None
            await send_bot_message(telegram_id, "Unauthorized.")
            return
            
        target_id = session.get("state").replace("admin_sending_user_msg_", "").strip()
        target_user = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_user:
            await send_bot_message(telegram_id, "❌ User not found.", reply_markup=main_keyboard)
        else:
            try:
                msg_text = f"📩 <b>Message from Admin:</b>\n\n{text}"
                success = await send_bot_message(target_user.telegram_id, msg_text)
                if success:
                    await send_bot_message(telegram_id, f"✅ Message successfully sent to {target_user.display_name}!", reply_markup=main_keyboard)
                else:
                    await send_bot_message(telegram_id, f"❌ Failed to send message to {target_user.display_name}. They may have blocked the bot.", reply_markup=main_keyboard)
            except Exception as e:
                await send_bot_message(telegram_id, f"❌ Error sending message: {e}", reply_markup=main_keyboard)
                
        session["state"] = None
        db.commit()
        return

    elif session.get("state") and session.get("state").startswith("admin_waiting_wallet_adj_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        target_id = session.get("state").replace("admin_waiting_wallet_adj_", "").strip()
        target_user = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_user:
            await send_bot_message(user.telegram_id, "❌ User not found!")
            session["state"] = None
            return
            
        # Parse value
        text_val = text_clean
        try:
            is_negative = text_val.startswith("-")
            val_clean = text_val.lstrip("+-").strip()
            amount = float(val_clean)
            if is_negative:
                amount = -amount
        except ValueError:
            await send_bot_message(user.telegram_id, "❌ Invalid number format. Please enter a valid decimal number (e.g. +500 or -250):")
            return
            
        target_user.wallet_balance += amount
        
        # Log WalletTransaction
        txn_type = "refund" if amount > 0 else "payment"
        tx = WalletTransaction(
            user_id=target_user.id,
            type=txn_type,
            amount=amount,
            description=f"Admin Adjustment ({'+' if amount >= 0 else ''}{amount:.2f})"
        )
        db.add(tx)
        db.commit()
        
        session["state"] = None
        notify_msg = (
            f"✅ <b>Wallet balance adjusted successfully!</b>\n\n"
            f"👤 User: <b>{target_user.display_name}</b>\n"
            f"• Amount: <b>{'+' if amount >= 0 else ''}{amount:.2f}</b>\n"
            f"• New Balance: <b>₹{target_user.wallet_balance:.2f}</b>"
        )
        await send_bot_message(user.telegram_id, notify_msg, reply_markup=main_keyboard)
        
        # Show user details console
        await send_admin_user_details(user.telegram_id, target_user.id, db)
        
        # Notify target user if they have started the bot
        try:
            notify_msg = (
                f"💰 <b>Wallet Balance Update</b>\n\n"
                f"An administrator adjusted your wallet balance by <b>{'+' if amount >= 0 else ''}{amount:.2f}</b>.\n"
                f"• New Balance: <b>₹{target_user.wallet_balance:.2f}</b>"
            )
            await send_bot_message(target_user.telegram_id, notify_msg)
        except Exception:
            pass
        return

    elif session.get("state") == "admin_waiting_promo_code":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        code = text_clean.upper()
        if len(code) < 3:
            await send_bot_message(user.telegram_id, "❌ Code must be at least 3 characters. Please try again:")
            return
            
        exists = db.query(Coupon).filter(Coupon.code == code).first()
        if exists:
            await send_bot_message(user.telegram_id, "❌ Promo code already exists. Please try a different code:")
            return
            
        session["new_promo_code"] = code
        session["state"] = "admin_waiting_promo_value"
        await send_bot_message(
            user.telegram_id,
            f"🎟️ <b>Promo Code: {code}</b>\n\n"
            f"Please enter the top-up value in Rupees (e.g. <code>100</code> or <code>250</code>):"
        )
        return

    elif session.get("state") == "admin_waiting_promo_value":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        try:
            val = float(text_clean)
            if val <= 0:
                raise ValueError()
        except ValueError:
            await send_bot_message(user.telegram_id, "❌ Please enter a valid positive number for the promo value:")
            return
            
        session["new_promo_value"] = val
        session["state"] = "admin_waiting_promo_limit"
        await send_bot_message(
            user.telegram_id,
            f"🎟️ <b>Promo Code: {session.get('new_promo_code')}</b>\n"
            f"💰 <b>Value: ₹{val:.2f}</b>\n\n"
            f"Please enter the usage limit (e.g. <code>1</code> for single-use, <code>100</code> for multi-use):"
        )
        return

    elif session.get("state") == "admin_waiting_promo_limit":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        try:
            limit = int(text_clean)
            if limit <= 0:
                raise ValueError()
        except ValueError:
            await send_bot_message(user.telegram_id, "❌ Please enter a valid positive integer for the limit:")
            return
            
        code = session.get("new_promo_code")
        val = session.get("new_promo_value")
        
        coupon = Coupon(
            code=code,
            value=val,
            usage_limit=limit,
            is_active=True
        )
        db.add(coupon)
        db.commit()
        
        session["state"] = None
        session["new_promo_code"] = None
        session["new_promo_value"] = None
        
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Promo Code Created Successfully!</b>\n\n"
            f"• Code: <code>{code}</code>\n"
            f"• Value: <b>₹{val:.2f}</b>\n"
            f"• Usage Limit: <b>{limit}</b>",
            reply_markup=main_keyboard
        )
        
        # Show promo code menu
        limit_count = 5
        offset = 0
        total_coupons = db.query(Coupon).count()
        import math
        total_pages = max(1, math.ceil(total_coupons / limit_count))
        coupons = db.query(Coupon).order_by(Coupon.created_at.desc()).offset(offset).limit(limit_count).all()
        
        msg = f"🎟️ <b>Promo Codes Management (Page 1/{total_pages}):</b>\n\n"
        buttons = []
        if not coupons:
            msg += "<i>No promo codes created yet.</i>\n"
        else:
            for c in coupons:
                status = "🟢 Active" if (c.is_active and c.redeemed_count < c.usage_limit) else "🔴 Inactive"
                msg += f"• <b>Code:</b> <code>{c.code}</code>\n  Value: ₹{c.value:.2f} | Limit: {c.redeemed_count}/{c.usage_limit} | Status: {status}\n\n"
                buttons.append([
                    {"text": f"❌ Delete {c.code}", "callback_data": f"admin_promo_delete_{c.id}"}
                ])
        nav_row = []
        if total_pages > 1:
            nav_row.append({"text": "Next ➡️", "callback_data": "admin_promo_page_2"})
        if nav_row:
            buttons.append(nav_row)
        buttons.append([
            {"text": "➕ Create Promo Code", "callback_data": "admin_promo_create"},
            {"text": "🔙 Back", "callback_data": "admin_refresh_stats"}
        ])
        await send_bot_message(user.telegram_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif session.get("state") == "admin_waiting_broadcast_all_text":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        session["temp_broadcast_text"] = text
        session["state"] = None
        
        all_count = db.query(DbUser).filter(DbUser.telegram_id.isnot(None)).count()
        preview_text = (
            f"📢 <b>Broadcast Preview ({all_count} users target):</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{text}\n\n"
            f"<i>Tap below to confirm and send broadcast to all registered users:</i>"
        )
        buttons = [
            [{"text": "✅ Confirm & Send Broadcast", "callback_data": "admin_broadcast_all_confirm"}],
            [{"text": "❌ Cancel", "callback_data": "admin_refresh_stats"}]
        ]
        await send_bot_message(user.telegram_id, preview_text, reply_markup={"inline_keyboard": buttons})
        return

    elif session.get("state") == "admin_waiting_direct_user":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        query_val = text_clean.lstrip("@").strip()
        target_u = db.query(DbUser).filter(
            (DbUser.telegram_id == query_val) |
            (DbUser.username.ilike(query_val)) |
            (DbUser.display_name.ilike(f"%{query_val}%")) |
            (DbUser.id == query_val)
        ).first()
        
        if not target_u:
            await send_bot_message(user.telegram_id, "❌ Target user not found. Please try entering Telegram ID or Username again:")
            return
            
        session["target_direct_user_id"] = target_u.id
        session["state"] = "admin_waiting_direct_text"
        await send_bot_message(
            user.telegram_id,
            f"💬 <b>Direct Messaging:</b> <b>{target_u.display_name}</b> (ID: <code>{target_u.telegram_id}</code>)\n\n"
            f"Please type the message you want to send directly to this user:",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    elif session.get("state") == "admin_waiting_direct_text":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        target_id = session.get("target_direct_user_id")
        target_u = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_u:
            await send_bot_message(user.telegram_id, "❌ Target user lost. Action cancelled.")
            session["state"] = None
            return
            
        direct_msg = (
            f"💬 <b>Direct Message from Platform Admin:</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{text}"
        )
        ok = await send_bot_message(target_u.telegram_id, direct_msg)
        session["state"] = None
        session["target_direct_user_id"] = None
        
        if ok:
            await send_bot_message(user.telegram_id, f"✅ <b>Message sent successfully to {target_u.display_name}!</b>", reply_markup=main_keyboard)
        else:
            await send_bot_message(user.telegram_id, f"❌ Failed to send message to {target_u.display_name} (User may have blocked the bot).", reply_markup=main_keyboard)
        return

    elif session.get("state") == "admin_waiting_upi_id":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        upi_id = text_clean
        if "@" not in upi_id or len(upi_id) < 5:
            await send_bot_message(user.telegram_id, "❌ Invalid UPI ID format. It must contain the '@' symbol (e.g. <code>merchant@bank</code>). Please try again:")
            return
        cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        old_val = cfg.value if cfg else "None"
        if not cfg:
            cfg = SystemConfig(key="upi_id", value=upi_id)
            db.add(cfg)
        else:
            cfg.value = upi_id
        db.commit()
        session["state"] = None
        
        # Broadcast configuration update to other admins
        asyncio.create_task(broadcast_config_change_to_admins(user.telegram_id, "UPI ID", old_val, upi_id, db))
        
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Merchant UPI ID updated successfully!</b>\n\n• New UPI ID: <code>{upi_id}</code>",
            reply_markup=main_keyboard
        )
        
        # Show system config panel
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        maint_cfg = db.query(SystemConfig).filter(SystemConfig.key == "maintenance_mode").first()
        
        upi_id_val = upi_id_cfg.value if upi_id_cfg else "pranjalottery@fam"
        upi_name_val = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        maint_val = maint_cfg.value if maint_cfg else "false"
        maint_status = "⚠️ MAINTENANCE ON" if maint_val == "true" else "🟢 ONLINE"
        
        msg = (
            f"⚙️ <b>System Configuration Control Panel</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>UPI ID:</b> <code>{upi_id_val}</code>\n"
            f"• <b>UPI Name:</b> <code>{upi_name_val}</code>\n"
            f"• <b>Platform Status:</b> <code>{maint_status}</code>\n\n"
            f"<i>Use the settings below to adjust system parameters directly in real-time:</i>"
        )
        buttons = [
            [
                {"text": "💳 Update UPI ID", "callback_data": "admin_conf_upi_id"},
                {"text": "👤 Update UPI Name", "callback_data": "admin_conf_upi_name"}
            ],
            [
                {"text": "🛠️ Toggle Maintenance", "callback_data": "admin_toggle_maintenance"}
            ],
            [
                {"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}
            ]
        ]
        await send_bot_message(user.telegram_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif session.get("state") == "admin_waiting_upi_name":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        upi_name = text.strip()
        if len(upi_name) < 2:
            await send_bot_message(user.telegram_id, "❌ Invalid merchant name. Please enter a valid name (minimum 2 characters):")
            return
        cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        old_val = cfg.value if cfg else "None"
        if not cfg:
            cfg = SystemConfig(key="upi_name", value=upi_name)
            db.add(cfg)
        else:
            cfg.value = upi_name
        db.commit()
        session["state"] = None
        
        # Broadcast configuration update to other admins
        asyncio.create_task(broadcast_config_change_to_admins(user.telegram_id, "UPI Display Name", old_val, upi_name, db))
        
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Merchant UPI Display Name updated successfully!</b>\n\n• New Display Name: <code>{upi_name}</code>",
            reply_markup=main_keyboard
        )
        
        # Show system config panel
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        maint_cfg = db.query(SystemConfig).filter(SystemConfig.key == "maintenance_mode").first()
        
        upi_id_val = upi_id_cfg.value if upi_id_cfg else "pranjalottery@fam"
        upi_name_val = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        maint_val = maint_cfg.value if maint_cfg else "false"
        maint_status = "⚠️ MAINTENANCE ON" if maint_val == "true" else "🟢 ONLINE"
        
        msg = (
            f"⚙️ <b>System Configuration Control Panel</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>UPI ID:</b> <code>{upi_id_val}</code>\n"
            f"• <b>UPI Name:</b> <code>{upi_name_val}</code>\n"
            f"• <b>Platform Status:</b> <code>{maint_status}</code>\n\n"
            f"<i>Use the settings below to adjust system parameters directly in real-time:</i>"
        )
        buttons = [
            [
                {"text": "💳 Update UPI ID", "callback_data": "admin_conf_upi_id"},
                {"text": "👤 Update UPI Name", "callback_data": "admin_conf_upi_name"}
            ],
            [
                {"text": "🛠️ Toggle Maintenance", "callback_data": "admin_toggle_maintenance"}
            ],
            [
                {"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}
            ]
        ]
        await send_bot_message(user.telegram_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif session.get("state") == "admin_waiting_search_order_id":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        
        search_id = text.strip()
        order = db.query(Order).filter(Order.id == search_id).first()
        session["state"] = None
        
        if not order:
            await send_bot_message(
                user.telegram_id,
                f"❌ <b>Order not found:</b> <code>{search_id}</code>\n\nCould not find any order with this ID.",
                reply_markup=main_keyboard
            )
            return
            
        # Re-display detail panel
        rider_name = order.rider.rider_name if order.rider else "None"
        rider_phone = order.rider.rider_phone if order.rider else "None"
        detail_msg = (
            f"🛒 <b>Order Editor: {order.id}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>Status:</b> <code>{order.status}</code>\n"
            f"• <b>User:</b> {order.user.display_name} (ID: <code>{order.user.telegram_id}</code>)\n"
            f"• <b>Total Payable:</b> ₹{order.total_payable:.2f} ({order.payment_method.upper()})\n"
            f"• <b>Domino's Ref:</b> <code>{order.dominos_reference or 'None'}</code>\n"
            f"• <b>Sector Store:</b> <code>{order.sector_store or 'None'}</code>\n"
            f"• <b>Rider Name:</b> <code>{rider_name}</code>\n"
            f"• <b>Rider Phone:</b> <code>{rider_phone}</code>\n"
        )
        buttons = [
            [
                {"text": "✏️ Domino's Ref", "callback_data": f"admin_edit_ref_{order.id}"},
                {"text": "✏️ Sector Store", "callback_data": f"admin_edit_store_{order.id}"}
            ],
            [
                {"text": "✏️ Rider Name", "callback_data": f"admin_edit_rider_name_{order.id}"},
                {"text": "✏️ Rider Phone", "callback_data": f"admin_edit_rider_phone_{order.id}"}
            ],
            [
                {"text": "🔄 Change Status", "callback_data": f"admin_change_status_menu_{order.id}"}
            ],
            [
                {"text": "🔙 Back to Orders List", "callback_data": "admin_manage_orders_menu"}
            ]
        ]
        await send_bot_message(user.telegram_id, detail_msg, reply_markup={"inline_keyboard": buttons})
        return

    elif session.get("state") and session.get("state").startswith("admin_waiting_edit_ref_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        order_id = session.get("state").replace("admin_waiting_edit_ref_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        session["state"] = None
        if not order:
            await send_bot_message(user.telegram_id, "❌ Order not found!", reply_markup=main_keyboard)
            return
            
        ref_val = text.strip()
        if ref_val.lower() == "none" or ref_val == "":
            order.dominos_reference = None
        else:
            order.dominos_reference = ref_val
            
        db.commit()
        await send_bot_message(user.telegram_id, f"✅ Domino's Reference updated successfully to <code>{order.dominos_reference or 'None'}</code>", reply_markup=main_keyboard)
        return

    elif session.get("state") and session.get("state").startswith("admin_waiting_store_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        order_id = session.get("state").replace("admin_waiting_store_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        session["state"] = None
        if not order:
            await send_bot_message(user.telegram_id, "❌ Order not found!", reply_markup=main_keyboard)
            return
            
        store_val = text.strip()
        if store_val.lower() == "none" or store_val == "":
            order.sector_store = None
        else:
            order.sector_store = store_val
            
        db.commit()
        await send_bot_message(user.telegram_id, f"✅ Sector Store updated successfully to <code>{order.sector_store or 'None'}</code>", reply_markup=main_keyboard)
        return

    elif session.get("state") and session.get("state").startswith("admin_waiting_rider_name_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        order_id = session.get("state").replace("admin_waiting_rider_name_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        session["state"] = None
        if not order:
            await send_bot_message(user.telegram_id, "❌ Order not found!", reply_markup=main_keyboard)
            return
            
        name_val = text.strip()
        if name_val.lower() == "none" or name_val == "":
            if order.rider:
                db.delete(order.rider)
        else:
            if order.rider:
                order.rider.rider_name = name_val
            else:
                new_rider = RiderAssignment(order_id=order.id, rider_name=name_val, rider_phone="None")
                db.add(new_rider)
                
        db.commit()
        await send_bot_message(user.telegram_id, f"✅ Rider Name updated successfully!", reply_markup=main_keyboard)
        return

    elif session.get("state") and session.get("state").startswith("admin_waiting_rider_phone_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        order_id = session.get("state").replace("admin_waiting_rider_phone_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        session["state"] = None
        if not order:
            await send_bot_message(user.telegram_id, "❌ Order not found!", reply_markup=main_keyboard)
            return
            
        phone_val = text.strip()
        if phone_val.lower() == "none" or phone_val == "":
            if order.rider:
                order.rider.rider_phone = "None"
        else:
            if order.rider:
                order.rider.rider_phone = phone_val
            else:
                new_rider = RiderAssignment(order_id=order.id, rider_name="Rider", rider_phone=phone_val)
                db.add(new_rider)
                
        db.commit()
        await send_bot_message(user.telegram_id, f"✅ Rider Phone updated successfully!", reply_markup=main_keyboard)
        return

    elif session.get("state") and session.get("state").startswith("admin_waiting_ref_"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ Unauthorized!")
            return
        order_id = session.get("state").replace("admin_waiting_ref_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await send_bot_message(user.telegram_id, "❌ Order not found!")
            session["state"] = None
            return
            
        ref_val = text.strip()
        if ref_val.lower() == "none" or ref_val == "":
            ref_val = None
            
        order.status = "Completed"
        if ref_val:
            order.dominos_reference = ref_val
            
        h = OrderStatusHistory(
            order_id=order.id,
            status="Completed",
            note=f"Manually completed by admin: {user.username or 'admin'}"
        )
        db.add(h)
        db.commit()
        
        session["state"] = None
        
        # Notify admin
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Order {order.id} marked as Completed!</b>\n\n"
            f"Reference: <code>{ref_val or 'None'}</code>\n"
            f"Customer has been notified.",
            reply_markup=main_keyboard
        )
        
        # Notify customer
        customer_msg = (
            f"🎉 <b>Order Placed Successfully!</b>\n\n"
            f"Your order <code>{order.id}</code> has been completed/placed successfully by our administrators!\n"
        )
        if ref_val:
            customer_msg += f"🎫 <b>Domino's Ref No:</b> <code>{ref_val}</code>\n"
            
        customer_msg += f"\n<b>Progress:</b>\n{get_order_progress_bar('Completed')}"
        
        try:
            await send_bot_message(order.user.telegram_id, customer_msg)
        except Exception:
            pass
        return
    elif session.get("state") == "admin_waiting_db_wipe":
        if text.strip() == "WIPE DB":
            try:
                run_backup(db)
            except Exception:
                pass
            try:
                from app.backend.database import QRGenerationHistory as _QRGenerationHistory
                main_admin_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048").strip()
                db.query(OrderNote).delete(synchronize_session=False)
                db.query(OrderStatusHistory).delete(synchronize_session=False)
                db.query(OrderItem).delete(synchronize_session=False)
                db.query(UTRAttempt).delete(synchronize_session=False)
                db.query(_QRGenerationHistory).delete(synchronize_session=False)
                db.query(RiderAssignment).delete(synchronize_session=False)
                db.query(WalletTransaction).delete(synchronize_session=False)
                db.query(WithdrawalRequest).delete(synchronize_session=False)
                db.query(SavedAddress).delete(synchronize_session=False)
                db.query(Order).delete(synchronize_session=False)
                db.query(SupportMessage).delete(synchronize_session=False)
                db.query(ErrorLog).delete(synchronize_session=False)
                
                db.query(User).filter(User.telegram_id != str(main_admin_id)).delete(synchronize_session=False)
                db.commit()
                auto_save_persistent_db_state(db)
                session["state"] = None
                await send_bot_message(user.telegram_id, "✅ <b>Database cleared and reset successfully!</b>", reply_markup=main_keyboard)
            except Exception as e:
                db.rollback()
                await send_bot_message(user.telegram_id, f"❌ Database reset failed: {e}", reply_markup=main_keyboard)
        else:
            session["state"] = None
            await send_bot_message(user.telegram_id, "❌ Verification failed. Database wipe aborted.", reply_markup=main_keyboard)
        return



    elif session.get("state") == "waiting_for_phone":

        # Check if it contains letters
        has_letters = any(c.isalpha() for c in text)
        digits_only = "".join(c for c in text if c.isdigit())
        if has_letters or not (10 <= len(digits_only) <= 15):
            await send_bot_message(
                user.telegram_id,
                "❌ <b>Invalid Mobile Number:</b>\n\n"
                "Please enter your <b>10-digit mobile number</b> (digits only, e.g. <code>9999999999</code> or starting with <code>+91</code>).",
                reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
            )
            return
        if not (10 <= len(digits_only) <= 15):
            await send_bot_message(
                user.telegram_id,
                "❌ <b>Invalid Phone Number Format:</b>\n\n"
                "Please enter a valid 10 to 15 digit contact number (e.g. +919999999999)."
            )
            return
            
        # Normalise: strip spaces, ensure starts with + or digits only
        phone_raw = text.strip().replace(" ", "").replace("-", "")
        digits_only = "".join(c for c in phone_raw if c.isdigit())
        if not (10 <= len(digits_only) <= 15):
            await send_bot_message(
                user.telegram_id,
                "❌ <b>Invalid Phone Number.</b>\n\n"
                "Please enter a valid mobile number with country code.\n"
                "Format: <code>+91XXXXXXXXXX</code> (10–15 digits)",
            )
            return
        # Format cleanly: +91XXXXXXXXXX
        if phone_raw.startswith("+"):
            phone_formatted = "+" + digits_only
        elif len(digits_only) == 10:
            phone_formatted = "+91" + digits_only
        else:
            phone_formatted = "+" + digits_only
        # Save phone to user profile for future pre-fill
        user.phone = phone_formatted
        db.commit()
        session["temp_phone"] = phone_formatted
        session["state"] = None
        
        if not session.get("checkout_pending"):
            await send_bot_message(
                user.telegram_id,
                f"✅ <b>Phone updated to <code>{phone_formatted}</code></b>\n\nThis number will be used for all future orders."
            )
            await display_delivery_location_menu(db, user)
            return
            
        session["checkout_pending"] = False
        session["state"] = None
        await initiate_checkout(db, user, session)
        return

    # 2. Setup keyboards & permissions
    if user and user.role == "admin" and user.admin_expires_at:
        if datetime.datetime.utcnow() > user.admin_expires_at:
            user.role = "user"
            user.admin_expires_at = None
            db.commit()
            logger.info(f"Demoted user {user.display_name} due to expired admin role duration.")

    admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
    is_admin = str(telegram_id) == str(admin_tg_id) or (user and user.role == "admin")

    # Check maintenance mode
    maintenance_cfg = db.query(SystemConfig).filter(SystemConfig.key == "maintenance_mode").first()
    if maintenance_cfg and maintenance_cfg.value == "true" and not is_admin:
        await send_bot_message(
            telegram_id,
            "⚠️ <b>System Under Maintenance</b>\n\n"
            "We are currently performing scheduled system upgrades. "
            "The platform will be back online shortly. Thank you for your patience! 🍕"
        )
        return

    main_keyboard = {
        "keyboard": [
            [{"text": "🍕 View Menu"}, {"text": "💰 My Wallet"}],
            [{"text": "📍 Change Location"}, {"text": "📦 Track Orders"}],
            [{"text": "🎉 Active Offers"}, {"text": "💬 Contact Support"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    
    if is_admin:
        main_keyboard["keyboard"].append([
            {"text": "🔑 Admin Center"}
        ])

    text_lower = text.strip().lower()

    if text_lower == "/secret_key":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
            
        import secrets
        session_key = secrets.token_hex(32)
        
        cfg = db.query(SystemConfig).filter(SystemConfig.key == "admin_session_key").first()
        if not cfg:
            cfg = SystemConfig(key="admin_session_key", value=session_key)
            db.add(cfg)
        else:
            cfg.value = session_key
        db.commit()
        
        await send_bot_message(
            user.telegram_id,
            f"🔑 <b>Admin Session Key Generated!</b>\n\n"
            f"Use this temporary session key to log in to the admin portal:\n"
            f"<code>{session_key}</code>\n\n"
            f"<i>Keep this key secret. Generating a new key invalidates the previous one.</i>"
        )
        return


    elif text_clean.startswith("/admin_msg "):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
        parts = text_clean.split(" ", 2)
        if len(parts) < 3:
            await send_bot_message(user.telegram_id, "⚠️ <b>Usage:</b> <code>/admin_msg &lt;user_id/telegram_id/phone&gt; &lt;text&gt;</code>")
            return
        target = parts[1].strip()
        msg_text = parts[2].strip()
        
        target_user = db.query(User).filter(
            (User.id == target) | (User.telegram_id == target) | (User.phone == target)
        ).first()
        if not target_user:
            await send_bot_message(user.telegram_id, f"❌ User with ID/Telegram ID/Phone <code>{target}</code> not found.")
            return
        s_msg = SupportMessage(
            user_id=target_user.id,
            sender_type="admin",
            message=msg_text
        )
        db.add(s_msg)
        db.commit()
        
        res = await send_bot_message(target_user.telegram_id, f"💬 <b>Support Agent Reply:</b>\n{msg_text}")
        if res:
            await send_bot_message(user.telegram_id, f"✅ Message sent to <b>{target_user.display_name}</b> (Telegram ID: <code>{target_user.telegram_id}</code>).")

            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({
                        "type": "support_message",
                        "user_id": target_user.id,
                        "sender_type": "admin",
                        "message": msg_text,
                        "created_at": s_msg.created_at.isoformat()
                    })
                except Exception:
                    pass
        else:
            await send_bot_message(user.telegram_id, "❌ Failed to send message to user via bot. Ensure they have started the bot.")
        return

    elif text_clean.startswith("/admin_broadcast "):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
        parts = text_clean.split(" ", 1)
        broadcast_text = parts[1].strip()
        
        users = db.query(User).filter(User.role != "admin").all()
        sent_count = 0
        for u in users:
            if u.telegram_id:
                res = await send_bot_message(u.telegram_id, f"📢 <b>Broadcast Message from Admin:</b>\n\n{broadcast_text}")
                if res:
                    sent_count += 1
        await send_bot_message(user.telegram_id, f"✅ Broadcast sent to <b>{sent_count}</b> users successfully!")
        return

    elif text_clean.startswith("/admin_orders"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
        
        orders = db.query(Order).order_by(Order.created_at.desc()).limit(10).all()
        if not orders:
            await send_bot_message(user.telegram_id, "📦 No orders found in the database.")
            return
            
        lines = []
        for o in orders:
            u = db.query(User).filter(User.id == o.user_id).first()
            user_disp = u.display_name if u else "Unknown"
            lines.append(
                f"🍕 <b>Order ID:</b> <code>{o.id}</code>\n"
                f"👤 User: {user_disp} (ID: <code>{o.user_id}</code>)\n"
                f"💰 Total: ₹{o.total_payable:.2f}  •  Status: <b>{o.status}</b>\n"
                f"🏡 Address: {o.address}\n"
                f"📱 Phone: {o.phone}\n"
                f"📍 GPS: {o.latitude or 'None'}, {o.longitude or 'None'}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            )
        await send_bot_message(user.telegram_id, "📦 <b>10 Most Recent Orders:</b>\n\n" + "\n".join(lines))
        return

    elif text_clean.startswith("/admin_users"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
            
        users = db.query(User).order_by(User.created_at.desc()).limit(10).all()
        lines = []
        for u in users:
            lines.append(
                f"👤 <b>{u.display_name}</b>\n"
                f"• ID: <code>{u.id}</code>\n"
                f"• TG ID: <code>{u.telegram_id}</code>\n"
                f"• Wallet: ₹{u.wallet_balance:.2f}\n"
                f"• Phone: {u.phone or 'Not set'}\n"
                f"• GPS: {u.latitude or 'None'}, {u.longitude or 'None'}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            )
        await send_bot_message(user.telegram_id, "👤 <b>Recent Registered Users:</b>\n\n" + "\n".join(lines))
        return

    elif text_clean.startswith("/admin_approve "):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
        target_id = text_clean.split(" ", 1)[1].strip()
        
        from .routes import approve_payment_manually
        try:
            res = await approve_payment_manually(target_id, db=db, admin=user)
            await send_bot_message(user.telegram_id, f"✅ <b>Approved:</b> {res.get('message', 'Success')}")
        except Exception as e:
            await send_bot_message(user.telegram_id, f"❌ <b>Error:</b> {str(e)}")
        return

    elif text_clean.startswith("/admin_reject "):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
        target_id = text_clean.split(" ", 1)[1].strip()
        
        from .routes import reject_payment_manually
        try:
            res = await reject_payment_manually(target_id, db=db, admin=user)
            await send_bot_message(user.telegram_id, f"❌ <b>Rejected:</b> {res.get('message', 'Success')}")
        except Exception as e:
            await send_bot_message(user.telegram_id, f"❌ <b>Error:</b> {str(e)}")
        return

    elif text_lower == "/admin" or text_lower == "/admin_dashboard" or text_lower == "🔑 admin center":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return

        func = sql_func

        admin_dashboard_text, admin_inline_markup = render_admin_command_center(db)
        await send_bot_message(user.telegram_id, admin_dashboard_text, reply_markup=admin_inline_markup)
        return

    elif text.startswith("/start"):
        # Check deep-linking parameter for Telegram account linking
        parts = text.split(" ")
        if len(parts) > 1 and parts[1].startswith("verify_"):
            code = parts[1].replace("verify_", "").strip()
            # Perform verification
            target_user = db.query(User).filter(User.telegram_verification_code == code).first()
            if target_user:
                # Update user details
                target_user.telegram_id = str(user.telegram_id)
                target_user.username = user.username
                target_user.display_name = user.display_name
                target_user.telegram_verified = True
                target_user.telegram_verification_code = None
                db.commit()
                
                await send_bot_message(
                    user.telegram_id,
                    f"✅ <b>Telegram account linked successfully!</b>\n\n"
                    f"Your account on display <b>{target_user.display_name}</b> has been linked with this Telegram account.\n"
                    f"You can now open the Web App from the menu below."
                )
                return
            else:
                await send_bot_message(user.telegram_id, "❌ <b>Verification failed!</b> Invalid or expired code.")
                return

        if not user.city:
            user.city = "India"
            db.commit()

        welcome_text = (
            f"🍕 <b>Welcome to Domino's Order Engine!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Hello <b>{user.display_name}</b>! 🎉 Welcome to the ultimate pizza ordering platform.\n\n"
            f"✨ <b>Features at a glance:</b>\n"
            f"• 🍕 <b>Real-time Menu:</b> Flat India-wide pricing on all delicious pizzas!\n"
            f"• 💰 <b>Instant Wallet:</b> Fast 1-tap top-ups & automated checkout.\n"
            f"• 📦 <b>Live Order Tracker:</b> Automated status updates & delivery notifications.\n"
            f"• 🏷️ <b>Promos & Coupons:</b> Exclusive discounts & bonus wallet cashbacks.\n\n"
            f"💰 <b>Wallet Balance:</b> ₹{user.wallet_balance:.2f}\n"
            f"📍 <b>Location:</b> {user.city or 'India'}\n\n"
            f"<i>Select an option below to start your order! 👇</i>"
        )
        await send_bot_animation(
            user.telegram_id,
            "https://i.giphy.com/l0G18bM1hFkuTlhSg.gif", # High resolution spinning pizza GIF
            caption=welcome_text,
            reply_markup=main_keyboard
        )
        return
    elif text_lower.startswith("/verify"):
        parts = text.split(" ")
        if len(parts) < 2:
            await send_bot_message(user.telegram_id, "❌ <b>Usage:</b> <code>/verify <code></code>")
            return
        code = parts[1].strip()
        target_user = db.query(User).filter(User.telegram_verification_code == code).first()
        if target_user:
            target_user.telegram_id = str(user.telegram_id)
            target_user.username = user.username
            target_user.display_name = user.display_name
            target_user.telegram_verified = True
            target_user.telegram_verification_code = None
            db.commit()
            await send_bot_message(
                user.telegram_id,
                f"✅ <b>Telegram account linked successfully!</b>\n\n"
                f"Your account on display <b>{target_user.display_name}</b> has been linked with this Telegram account."
            )
        else:
            await send_bot_message(user.telegram_id, "❌ <b>Verification failed!</b> Invalid or expired code.")
        return

    elif text_lower.startswith("/set_url"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
        parts = text.split(" ", 1)
        if len(parts) < 2:
            await send_bot_message(user.telegram_id, "❌ <b>Usage:</b> <code>/set_url <https://your-public-url.com></code>")
            return
        new_url = parts[1].strip()
        if not (new_url.startswith("http://") or new_url.startswith("https://")):
            await send_bot_message(user.telegram_id, "❌ <b>Invalid URL!</b> URL must start with http:// or https://")
            return
            
        cfg = db.query(SystemConfig).filter(SystemConfig.key == "mini_app_url").first()
        if not cfg:
            cfg = SystemConfig(key="mini_app_url", value=new_url)
            db.add(cfg)
        else:
            cfg.value = new_url
        db.commit()
        
        # update global MINI_APP_URL
        MINI_APP_URL = new_url
        
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Mini-App URL Updated!</b>\n\nAll keyboard buttons and links will now use:\n<code>{new_url}</code>",
            reply_markup=main_keyboard
        )
        return

    elif text_lower == "🍕 order app (link)":
        link_text = (
            "🔗 <b>Ordering Mini-App Link</b>\n\n"
            "Telegram direct WebApp buttons require a secure HTTPS URL.\n\n"
            "Since the platform is running in local HTTP development mode, please click the link below to open the application in your browser:\n\n"
            f"👉 {MINI_APP_URL}/"
        )
        await send_bot_message(user.telegram_id, link_text, reply_markup=main_keyboard)
        return

    elif text_lower == "🌐 open admin portal (link)":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
        link_text = (
            "🌐 <b>Admin Portal Link</b>\n\n"
            "Click the link below to open the Admin Dashboard in your browser:\n\n"
            f"👉 {MINI_APP_URL}/admin/"
        )
        await send_bot_message(user.telegram_id, link_text, reply_markup=main_keyboard)
        return

    elif text_lower == "📍 change location" or text.startswith("/location"):
        session["state"] = "waiting_for_city"
        await display_delivery_location_menu(db, user)
        return

    elif text_clean == "🏠 Update Delivery Address":
        session["state"] = "waiting_for_address"
        session["temp_address"] = None
        coord_info = ""
        if user.latitude and user.longitude:
            coord_info = f"\n📡 GPS: <code>{user.latitude:.4f}, {user.longitude:.4f}</code>"
        await send_bot_message(
            user.telegram_id,
            f"🏠 <b>Enter Your Delivery Address</b>\n\n"
            f"Please type your full delivery address and press send.{coord_info}\n\n"
            f"<i>Example: Flat 4B, Sunrise Apartments, MG Road, Bengaluru 560001</i>",
            reply_markup={"keyboard": [[{"text": "🔙 Back"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    elif text_clean == "📱 Update Phone Number":
        session["state"] = "waiting_for_phone_update"
        saved_ph = user.phone or ""
        hint = f"\nCurrent: <code>{saved_ph}</code>" if saved_ph else ""
        await send_bot_message(
            user.telegram_id,
            f"📱 <b>Update Phone Number</b>{hint}\n\n"
            f"Enter your mobile number with country code:\n"
            f"Format: <code>+91XXXXXXXXXX</code>",
            reply_markup={"keyboard": [[{"text": "🔙 Back"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    elif text_lower == "🍕 view menu" or text.startswith("/menu"):
        if not user.city:
            user.city = "India"
            db.commit()
        await display_pizza_menu(db, user, main_keyboard)
        return

    elif text_lower in ("💰 my wallet", "💳 my wallet", "my wallet", "/wallet", "/balance"):
        wallet_text, wallet_markup = render_wallet_view(db, user, offset=0, limit=5)
        last_msg_id = session.get("last_bot_msg_id")
        if last_msg_id:
            edited = await edit_bot_message(user.telegram_id, last_msg_id, wallet_text, reply_markup=wallet_markup)
            if not edited:
                res = await send_bot_message(user.telegram_id, wallet_text, reply_markup=wallet_markup)
                if isinstance(res, int):
                    session["last_bot_msg_id"] = res
        else:
            res = await send_bot_message(user.telegram_id, wallet_text, reply_markup=wallet_markup)
            if isinstance(res, int):
                session["last_bot_msg_id"] = res
        return

    elif text_clean.lower() in ("🛒 my cart", "🛒 cart", "my cart", "cart", "/cart", "view cart", "🛒 view cart"):
        cart = session.get("cart", {})
        cart_msg, cart_markup = render_cart_message(db, user, cart, session)
        await send_bot_message(user.telegram_id, cart_msg, reply_markup=cart_markup)
        return

    elif any(kw in text_clean.lower() for kw in ("proceed to checkout", "checkout", "/checkout", "place order")) or text_clean.lower() in ("pay", "proceed"):
        await initiate_checkout(db, user, session)
        return

    elif text_lower in ("wallet_add", "💳 add funds", "💳 deposit", "deposit", "add deposit", "topup") or text_clean.startswith("/addfunds") or text_clean.startswith("/deposit"):
        session["state"] = "waiting_for_topup_amount"
        amount_reply_markup = {
            "keyboard": [
                [{"text": "₹50"}, {"text": "₹100"}, {"text": "₹200"}],
                [{"text": "₹500"}, {"text": "₹1000"}, {"text": "Custom Amount"}],
                [{"text": "❌ Cancel"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        add_funds_prompt = (
            "💳 <b>Add Funds to Wallet</b>\n\n"
            "Select a deposit amount from the options below or choose 'Custom Amount' to type a different value:"
        )
        
        # Avoid spam: delete previous message and send a new one with reply keyboard
        last_msg_id = session.get("last_bot_msg_id")
        if last_msg_id:
            await delete_bot_message(user.telegram_id, last_msg_id)
            
        res = await send_bot_message(user.telegram_id, add_funds_prompt, reply_markup=amount_reply_markup)
        if isinstance(res, int):
            session["last_bot_msg_id"] = res
        return


    elif text_lower == "📦 track orders" or text.startswith("/track") or text.startswith("/orders") or text.startswith("/status"):
        import datetime as _dt
        _ist_offset = _dt.timedelta(hours=5, minutes=30)
        _now_utc = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        _cutoff_utc = _now_utc - _dt.timedelta(hours=24)
        orders = db.query(Order).filter(
            Order.user_id == user.id,
            ~Order.id.like("TOPUP-%"),
            Order.created_at >= _cutoff_utc
        ).order_by(Order.created_at.desc()).limit(5).all()
        if not orders:
            track_text = (
                "📦 <b>Track Orders:</b>\n\n"
                "No active orders placed in the last 24 hours!\n\n"
                "👉 Tap <b>🍕 View Menu</b> below to browse delicious pizzas & place your order!"
            )
            await send_bot_animation(
                user.telegram_id,
                "https://i.giphy.com/26FL34o80tNnJjS24.gif",
                caption=track_text,
                reply_markup=main_keyboard
            )
            return
            
        track_lines = ["📦 <b>Your Orders (Last 24 Hours):</b>\n"]
        inline_keyboard = []
        for o in orders:
            # Query latest status from history
            history = db.query(OrderStatusHistory).filter(OrderStatusHistory.order_id == o.id).order_by(OrderStatusHistory.created_at.desc()).first()
            current_status = history.status if history else o.status
            
            # Format order items summary
            items = db.query(OrderItem).filter(OrderItem.order_id == o.id).all()
            items_desc = ", ".join([f"{item.product.name} x{item.quantity}" for item in items if item.product])
            
            # Format timestamp in IST 12h format
            _ist_time = o.created_at + _ist_offset
            date_str = _ist_time.strftime("%d %b %Y, %I:%M %p IST")

            # Rider / store details if set
            rider_line = ""
            if o.rider:
                rider_line = f"  🏍️ <b>Rider:</b> {o.rider.rider_name}"
                if o.rider.rider_phone:
                    rider_line += f" · {o.rider.rider_phone}"
                rider_line += "\n"
            store_line = ""
            if o.sector_store:
                store_line = f"  🏪 <b>Store:</b> {o.sector_store}\n"
            dominos_ref_line = ""
            if o.dominos_reference:
                dominos_ref_line = f"  🆔 <b>Domino's Ref:</b> <code>{o.dominos_reference}</code>\n"

            track_lines.append(
                f"• <b>Order ID:</b> <code>{o.id}</code>\n"
                f"  <b>Items:</b> {items_desc or 'Pizza Order'}\n"
                f"  <b>Total:</b> ₹{o.total_payable:.2f} ({o.payment_method.upper()})\n"
                f"  <b>Status:</b>\n  {get_order_progress_bar(current_status)}\n"
                + dominos_ref_line + rider_line + store_line +
                f"  <b>Placed At:</b> {date_str}\n"
            )
            
            # Add interactive row buttons for each order
            row = []
            # 1. In-bot tracker refresh button
            short_id = o.id.split("-")[-1]
            row.append({"text": f"🔄 Track {short_id}", "callback_data": f"track_refresh_{o.id}"})

            # 2. Allow user cancellation within 2 minutes if still "Order Processing"
            import datetime as _dt2
            _age_seconds = (_dt2.datetime.utcnow() - o.created_at).total_seconds()
            if current_status in ("Order Processing", "Placed") and _age_seconds < 120:
                row.append({"text": f"❌ Cancel {short_id}", "callback_data": f"user_cancel_order_{o.id}"})
                
            # 2. Pay or Cancel button if Pending Payment
            if current_status == "Pending Payment":
                row.append({"text": "💳 Pay Now", "callback_data": f"pay_now_{o.id}"})
                row.append({"text": "❌ Cancel", "callback_data": f"cancel_order_{o.id}"})
                
            inline_keyboard.append(row)
            
        track_markup = {"inline_keyboard": inline_keyboard} if inline_keyboard else main_keyboard
        
        await send_bot_animation(
            user.telegram_id,
            "https://i.giphy.com/26FL34o80tNnJjS24.gif", # Scooter Delivery
            caption="\n".join(track_lines),
            reply_markup=track_markup
        )
        return

    elif text_lower == "🎉 active offers" or "/offers" in text_lower or "offer" in text_lower:
        bot_fee = get_bot_fee(db)
        
        db_offers = db.query(ActiveOffer).filter(ActiveOffer.is_active == True).order_by(ActiveOffer.sort_order.asc()).all()
        
        if not db_offers:
            offers_text = (
                "🎉 <b>Special Active Deals:</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "<i>No active promotional deals available right now. Please check back soon or browse our full menu!</i>"
            )
            offers_markup = {
                "inline_keyboard": [
                    [{"text": "🍕 View Menu", "callback_data": "menu_view"}],
                    [{"text": "🛒 View Cart", "callback_data": "cart_view"}]
                ]
            }
        else:
            lines = ["🎉 <b>Active Promotional Deals & Combos:</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"]
            buttons = []
            
            for idx, off in enumerate(db_offers, 1):
                badge_str = f" [{off.badge}]" if off.badge else ""
                orig_price_str = f" <s>₹{off.original_price:.0f}</s>" if off.original_price and off.original_price > off.discounted_price else ""
                desc_str = f"<i>{escape_html(off.description)}</i>\n" if off.description else ""
                
                # Parse item details from items_json if available
                items_breakdown = ""
                if off.items_json:
                    try:
                        p_items = json.loads(off.items_json)
                        if isinstance(p_items, list) and len(p_items) > 0:
                            items_breakdown = "  <b>Included Items:</b>\n"
                            for itm in p_items:
                                name = itm.get("name") or itm.get("category") or "Item"
                                size = itm.get("size", "")
                                qty = itm.get("qty") or itm.get("quantity") or 1
                                sz_str = f" ({size})" if size else ""
                                items_breakdown += f"  • {qty}x <b>{escape_html(name)}</b>{sz_str}\n"
                    except Exception:
                        pass
                        
                savings_str = ""
                if off.original_price and off.original_price > off.discounted_price:
                    savings = off.original_price - off.discounted_price
                    pct = int((savings / off.original_price) * 100)
                    savings_str = f"  ✨ <b>You Save: ₹{savings:.0f} ({pct}% OFF)!</b>\n"
                
                lines.append(
                    f"{idx}️⃣ <b>{escape_html(off.title)}</b>{badge_str}\n"
                    f"{desc_str}"
                    f"{items_breakdown}"
                    f"  💰 <b>Deal Price: ₹{off.discounted_price:.2f}</b>{orig_price_str}\n"
                    f"{savings_str}"
                )
                
                btn_cb = f"apply_offer_{off.offer_key}"
                buttons.append([{"text": off.button_text or f"🛒 Grab Deal {idx} @ ₹{off.discounted_price:.0f}", "callback_data": btn_cb}])
            
            lines.append("💡 <i>Tap any deal button below to load the combo into your cart instantly!</i>")
            offers_text = "\n".join(lines)
            buttons.append([{"text": "🛒 View Cart", "callback_data": "cart_view"}])
            offers_markup = {"inline_keyboard": buttons}
        
        last_msg_id = session.get("last_bot_msg_id")
        edited = False
        if last_msg_id:
            edited = await edit_bot_message(user.telegram_id, last_msg_id, offers_text, reply_markup=offers_markup)
        if not edited:
            res = await send_bot_animation(
                user.telegram_id,
                "https://i.giphy.com/3o7iMClCoYV72aXf6o.gif",
                caption=offers_text,
                reply_markup=offers_markup
            )
            if isinstance(res, int):
                session["last_bot_msg_id"] = res
        return

    elif text_lower == "💬 contact support" or text.startswith("/support"):
        session["state"] = "waiting_for_support_message"  # Enable support message entry
        support_help = (
            "💬 <b>Contact Support</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Need help with your order, payment, or wallet?\n\n"
            "• <b>Send Message</b>: Type your query and we'll reply in this chat\n"
            "• <b>FAQs</b>: Tap below for instant answers to common questions"
        )
        support_markup = {
            "inline_keyboard": [
                [{"text": "💬 Send a Message to Support", "callback_data": "support_send_message"}],
                [{"text": "📖 FAQ: How to Order?", "callback_data": "faq_how_to_order"}],
                [{"text": "💳 FAQ: Wallet & UPI?", "callback_data": "faq_wallet_upi"}],
                [{"text": "📦 FAQ: Where is my Order?", "callback_data": "faq_where_order"}],
                [{"text": "🍕 FAQ: Bulk Orders & Parties?", "callback_data": "faq_bulk_order"}]
            ]
        }
        
        last_msg_id = session.get("last_bot_msg_id")
        edited = False
        if last_msg_id:
            edited = await edit_bot_message(user.telegram_id, last_msg_id, support_help, reply_markup=support_markup)
        if not edited:
            res = await send_bot_message(
                user.telegram_id,
                support_help,
                reply_markup=support_markup
            )
            if isinstance(res, int):
                session["last_bot_msg_id"] = res
        return

    elif "/help" in text_lower or "how to order" in text_lower or "how to place" in text_lower or "help" in text_lower:
        help_text = (
            "📖 <b>How to Place an Order:</b>\n"
            "1. Click the 'Order Delicious Pizza 🍕' button or start the app to open the menu.\n"
            "2. Select your favorite pizzas and add them to the cart.\n"
            "3. Go to the Checkout tab, enter your address and phone number, and drop a pin on the map.\n"
            "4. Choose your payment method (Wallet or Card) and click 'Place Order'.\n"
            "5. You can view your order status in the My Orders tab."
        )
        await send_bot_animation(
            user.telegram_id,
            "https://i.giphy.com/3o7iMClCoYV72aXf6o.gif",
            caption=help_text,
            reply_markup=main_keyboard
        )
        return

    elif text.startswith("/pay"):
        parts = text_clean.split(" ")
        if len(parts) < 3:
            await send_bot_message(user.telegram_id, "⚠️ <b>Usage:</b> <code>/pay &lt;order_id&gt; &lt;utr&gt;</code>\nExample: <code>/pay PIZZA-12345678 123456789012</code>")
            return
            
        order_id = parts[1].strip()
        utr = parts[2].strip()
        
        # Generate JWT token for the user to authenticate the API request
        from .auth import create_access_token
        token = create_access_token({"sub": str(user.id), "role": user.role})
        headers = {"Authorization": f"Bearer {token}"}
        
        # Send loading animation message
        status_msg_id = None
        res_send = await send_bot_message(user.telegram_id, "⏳ <b>Initiating UPI Payment Verification...</b>\n\n🔍 Connecting to merchant bank nodes...")
        if isinstance(res_send, int):
            status_msg_id = res_send
            
        if status_msg_id:
            await asyncio.sleep(0.6)
            await edit_bot_message(user.telegram_id, status_msg_id, "⏳ <b>UPI Payment Verification in progress...</b>\n\n[██░░░░░░░░] 20% - Fetching bank ledger...")
            await asyncio.sleep(0.6)
            await edit_bot_message(user.telegram_id, status_msg_id, "⏳ <b>UPI Payment Verification in progress...</b>\n\n[██████░░░░] 60% - Matching transaction UTR...")
            await asyncio.sleep(0.6)
            await edit_bot_message(user.telegram_id, status_msg_id, "⏳ <b>UPI Payment Verification in progress...</b>\n\n[██████████] 100% - Confirming order status...")
            await asyncio.sleep(0.4)

        # Make API call to verify-payment endpoint
        api_url = f"http://localhost:8000/api/orders/{order_id}/verify-payment"
        try:
            resp = await _http_client.post(api_url, json={"utr": utr}, headers=headers, timeout=10.0)
            if resp.status_code == 200:
                success_txt = f"✅ <b>Verification Successful!</b>\n\nYour payment with UTR <code>{utr}</code> has been verified. Order <code>{order_id}</code> is now being processed! 🍕"
                if status_msg_id:
                    await edit_bot_message(user.telegram_id, status_msg_id, success_txt)
                else:
                    await send_bot_message(user.telegram_id, success_txt)
            else:
                err_data = resp.json()
                detail = err_data.get("detail", "Payment verification failed.")
                fail_txt = f"❌ <b>Verification Failed:</b> {detail}"
                if status_msg_id:
                    await edit_bot_message(user.telegram_id, status_msg_id, fail_txt)
                else:
                    await send_bot_message(user.telegram_id, fail_txt)
        except Exception as e:
            err_txt = f"❌ <b>Error:</b> Could not connect to verification server: {str(e)}"
            if status_msg_id:
                await edit_bot_message(user.telegram_id, status_msg_id, err_txt)
            else:
                await send_bot_message(user.telegram_id, err_txt)
        return

    elif text.startswith("/order"):
        # 1. Menu view if no arguments
        parts = text.split(" ", 3)
        if len(parts) < 4:
            code_to_id, id_to_code = get_product_mappings(db)
            products = db.query(Product).filter(Product.availability == True).order_by(Product.original_price.asc()).all()
            menu_lines = []
            for p in products:
                price = p.discounted_price if p.discounted_price is not None else p.original_price
                display_code = id_to_code.get(p.id, p.id)
                menu_lines.append(f"🍕 <b>[{display_code}] {p.name}</b> - ₹{price:.2f}\n<i>{p.description}</i>")
            
            menu_text = (
                "🍽️ <b>Domino's Order Engine Menu:</b>\n\n" +
                "\n\n".join(menu_lines) +
                "\n\n👉 <b>To order via chat, type:</b>\n<code>/order &lt;product_id&gt; &lt;quantity&gt; &lt;delivery_address&gt;</code>\n"
                "Example: <code>/order 1 2 123 Main Street</code>"
            )
            await send_bot_animation(
                user.telegram_id,
                "https://i.giphy.com/10kxE34bJPaDPy.gif", # Pizza Baking
                caption=menu_text,
                reply_markup=main_keyboard
            )
            return

        # 2. Process Order placement
        try:
            input_product_code = parts[1].strip()
            code_to_id, id_to_code = get_product_mappings(db)
            if input_product_code in code_to_id:
                product_id = code_to_id[input_product_code]
            else:
                product_id = input_product_code  # Try as raw UUID string
            quantity = int(parts[2])
            address = parts[3].strip()
        except ValueError:
            await send_bot_message(user.telegram_id, "⚠️ <b>Invalid Product ID or Quantity!</b> Please enter numbers.", reply_markup=main_keyboard)
            return

        if quantity <= 0:
            await send_bot_message(user.telegram_id, "⚠️ Quantity must be at least 1!", reply_markup=main_keyboard)
            return

        product = db.query(Product).filter(Product.id == product_id, Product.availability == True).first()
        if not product:
            await send_bot_message(user.telegram_id, f"❌ Product ID <code>{parts[1]}</code> is out of stock or does not exist.", reply_markup=main_keyboard)
            return

        # Price calculations
        unit_price = product.discounted_price if product.discounted_price is not None else product.original_price
        subtotal = unit_price * quantity
        bot_fee = get_bot_fee(db)
        total_payable = subtotal + bot_fee
        original_total = subtotal
        discount_total = 0.0
        service_charge = bot_fee

        if user.wallet_balance < total_payable:
            await send_bot_message(
                user.telegram_id, 
                f"❌ <b>Insufficient Balance!</b>\nOrder Total: <b>₹{total_payable:.2f}</b>\nYour Balance: <b>₹{user.wallet_balance:.2f}</b>\n\nPlease add cash in the app settings.",
                reply_markup=main_keyboard
            )
            return

        # Deduct wallet
        user.wallet_balance -= total_payable

        # Gift card allocation
        gift_card = db.query(GiftCard).filter(GiftCard.status == "available", GiftCard.value >= total_payable).order_by(GiftCard.uploaded_at.asc()).first()
        if not gift_card:
            gift_card = db.query(GiftCard).filter(GiftCard.status == "available").order_by(GiftCard.uploaded_at.asc()).first()

        txn_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
        order_id = f"PIZZA-{uuid.uuid4().hex[:8].upper()}"

        order = Order(
            id=order_id,
            user_id=user.id,
            transaction_id=txn_id,
            payment_method="wallet",
            original_total=original_total,
            discount=discount_total,
            service_charge=service_charge,
            total_payable=total_payable,
            status="Payment Received",
            address=address,
            phone="Provided via Telegram",
            latitude=user.latitude or 19.0760,
            longitude=user.longitude or 72.8777,
            estimated_delivery=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(minutes=30)
        )
        db.add(order)
        db.flush()

        item = OrderItem(order_id=order.id, product_id=product.id, quantity=quantity, price=unit_price)
        db.add(item)

        h1 = OrderStatusHistory(order_id=order.id, status="Payment Received")
        db.add(h1)
        db.flush()

        if not gift_card:
            db.commit()
            # Log error
            err = ErrorLog(type="giftcard", message=f"Gift Card Exhausted! Bot Order: {order.id}. Cost: {total_payable}.")
            db.add(err)
            db.commit()

            await send_bot_message(
                user.telegram_id,
                f"💳 <b>Payment Confirmed!</b>\nWe deducted <b>₹{total_payable:.2f}</b> from your wallet for Order ID: <code>{order.id}</code>.\n\n"
                f"⚠️ <b>Order Status Notification:</b>\n"
                f"Your order has been accepted and is currently being processed. Our dispatch team has been notified and we will update you shortly!",
                reply_markup=main_keyboard
            )

            if sse_broadcast_callback:
                await sse_broadcast_callback({"type": "error_alert", "message": f"Critical: Gift card inventory is empty! Bot Order {order.id}."})
                await sse_broadcast_callback({"type": "order_update"})
            return

        # Allocate giftcard
        gift_card.status = "used"
        gift_card.used_by_user_id = user.id
        gift_card.used_in_order_id = order.id
        gift_card.used_at = datetime.datetime.now(datetime.timezone.utc)

        order.gift_card_id = gift_card.id
        order.status = "Order Processing"

        h2 = OrderStatusHistory(order_id=order.id, status="Gift Card Applied")
        db.add(h2)
        h3 = OrderStatusHistory(order_id=order.id, status="Order Processing")
        db.add(h3)

        audit = AuditLog(admin_id=None, action="GIFT_CARD_APPLIED", details=f"Bot Order: {order.id}, value: {gift_card.value}")
        db.add(audit)
        db.commit()
        
        try:
            from .services.dominos_service import submit_dominos_order
            await submit_dominos_order(order, db)
        except Exception as e:
            # Refund wallet
            user.wallet_balance += total_payable
            # Release gift card
            gift_card.status = "available"
            gift_card.used_by_user_id = None
            gift_card.used_in_order_id = None
            gift_card.used_at = None
            
            order.status = "Failed"
            h_fail = OrderStatusHistory(order_id=order.id, status="Failed", note=f"Auto-submission failed: {str(e)}")
            db.add(h_fail)
            
            err = ErrorLog(
                type="integration",
                message=f"Failed to submit order {order.id} to Domino's automatically from bot flow: {e}",
                stack_trace=traceback.format_exc()
            )
            db.add(err)
            db.commit()
            
            if sse_broadcast_callback:
                await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Failed"})
                
            await send_bot_message(
                user.telegram_id,
                f"❌ <b>Order Submission Failed</b>\n"
                f"We were unable to place your order <code>{order.id}</code> on Domino's.\n"
                f"Your payment of <b>₹{total_payable:.2f}</b> has been refunded to your wallet.",
                reply_markup=main_keyboard
            )
            return

        success_text = (
            f"💳 <b>Payment Confirmed!</b>\nWe deducted <b>₹{total_payable:.2f}</b> from your wallet for Order ID: <code>{order.id}</code>.\n\n"
            f"👩‍🍳 <b>Order Status: Processing</b>\n"
            f"Your pizza is now being prepared. Estimated delivery in 30 minutes!"
        )
        await send_bot_animation(
            user.telegram_id,
            "https://i.giphy.com/10kxE34bJPaDPy.gif", # Pizza Baking
            caption=success_text,
            reply_markup=main_keyboard
        )

        if sse_broadcast_callback:
            await sse_broadcast_callback({"type": "new_order", "order_id": order.id, "total": total_payable, "user": user.display_name, "status": "Order Processing"})
            await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Order Processing"})
        return

    else:
        msg_text = text.strip() if text else ""
        current_state = session.get("state")
        
        if current_state == "waiting_for_support_message":
            # Rate Limit Check: Max 3 consecutive user support messages without admin reply
            last_admin_msg = db.query(SupportMessage).filter(
                SupportMessage.user_id == user.id,
                SupportMessage.sender_type == "admin"
            ).order_by(SupportMessage.created_at.desc()).first()

            if last_admin_msg:
                unreplied_count = db.query(SupportMessage).filter(
                    SupportMessage.user_id == user.id,
                    SupportMessage.sender_type == "user",
                    SupportMessage.created_at > last_admin_msg.created_at
                ).count()
            else:
                unreplied_count = db.query(SupportMessage).filter(
                    SupportMessage.user_id == user.id,
                    SupportMessage.sender_type == "user"
                ).count()

            if unreplied_count >= 3:
                session["state"] = None
                session["support_relation"] = None
                await send_bot_message(
                    user.telegram_id,
                    "⚠️ <b>Support Rate Limit Reached</b>\n\n"
                    "You have sent 3 consecutive support messages without a reply. "
                    "Please wait for our support team to respond before sending additional messages.",
                    reply_markup=main_keyboard
                )
                return

            # Explicit support mode: save as support message & send ticket to admin
            session["state"] = None
            file_id = None
            attachment_type = None
            if photo:
                largest = max(photo, key=lambda p: p.get("file_size", 0))
                file_id = largest["file_id"]
                attachment_type = "photo"
                if not msg_text: msg_text = "[Image Attachment]"
            elif document:
                file_id = document.get("file_id")
                attachment_type = "document"
                if not msg_text: msg_text = f"[Document: {document.get('file_name', 'Attachment')}]"

            try:
                sup = SupportMessage(
                    user_id=user.id,
                    sender_type="user",
                    message=msg_text or "[Attachment]",
                    attachment_file_id=file_id,
                    attachment_type=attachment_type
                )
                db.add(sup)
                db.commit()
                if sse_broadcast_callback:
                    try:
                        await sse_broadcast_callback({
                            "type": "support_message",
                            "user_id": user.id,
                            "message": msg_text,
                            "display_name": user.display_name,
                            "has_attachment": file_id is not None
                        })
                    except Exception:
                        pass
            except Exception as db_err:
                logger.warning(f"Could not save support message: {db_err}")

            await send_bot_message(
                user.telegram_id,
                "✅ <b>Support message sent!</b>\n\n"
                "Our support team has received your message and will reply directly in this chat shortly.\n\n"
                "<i>Your message:</i>\n" + f"<blockquote>{escape_html(msg_text[:300])}</blockquote>",
                reply_markup=main_keyboard
            )

            # Forward support ticket to admin
            admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
            support_rel = session.get("support_relation", "General Query")
            admin_ticket_text = (
                f"💬 <b>Support Ticket from {escape_html(user.display_name)}</b>\n"
                f"• User ID: <code>{user.id}</code>\n"
                f"• Telegram ID: <code>{user.telegram_id}</code>\n"
                f"• Username: @{user.username or '—'}\n"
                f"• Phone Number: <code>{user.phone or '—'}</code>\n"
                f"• Relates to: <b>{escape_html(support_rel)}</b>\n\n"
                f"✉️ <b>Message:</b>\n"
                f"<blockquote>{escape_html(msg_text)}</blockquote>"
            )
            admin_ticket_markup = {
                "inline_keyboard": [
                    [{"text": "💬 Custom Reply", "callback_data": f"admin_reply_support_{user.telegram_id}"}],
                    [
                        {"text": "📋 Order Placed", "callback_data": f"admin_tmpl_placed_{user.telegram_id}"},
                        {"text": "💸 Refund Done", "callback_data": f"admin_tmpl_refund_{user.telegram_id}"}
                    ],
                    [
                        {"text": "⚠️ Payment Issue", "callback_data": f"admin_tmpl_utr_{user.telegram_id}"},
                        {"text": "🕒 Delay Alert", "callback_data": f"admin_tmpl_delay_{user.telegram_id}"}
                    ]
                ]
            }
            await send_bot_message(admin_tg_id, admin_ticket_text, reply_markup=admin_ticket_markup)
            return

        # General unhandled message: clear state & show unrecognized command guidance
        session["state"] = None
        help_reply = (
            f"👋 <b>Hello {escape_html(user.display_name)}!</b>\n\n"
            f"I couldn't recognize that message.\n\n"
            f"Please tap one of the quick menu options below to browse pizza deals, check your wallet, or contact our support team!"
        )
        help_markup = {
            "inline_keyboard": [
                [{"text": "🍕 View Menu", "callback_data": "menu_view"}, {"text": "💰 My Wallet", "callback_data": "wallet_view"}],
                [{"text": "📦 Track Orders", "callback_data": "menu_my_orders"}, {"text": "📞 Contact Support", "callback_data": "menu_support"}]
            ]
        }
        await send_bot_message(user.telegram_id, help_reply, reply_markup=help_markup)
        return

def parse_cart_quantity(raw_qty) -> int:
    """Safely extracts integer quantity from int, float, str, or dict."""
    if isinstance(raw_qty, dict):
        q = raw_qty.get("quantity") or raw_qty.get("qty") or raw_qty.get("count") or 1
        try:
            return max(1, int(q))
        except Exception:
            return 1
    try:
        return max(1, int(raw_qty))
    except Exception:
        return 1


def resolve_cart_item_product(db: Session, key: str):
    """Robust product lookup by UUID ID, exact name, or partial name fallback."""
    if not key:
        return db.query(Product).first()
    key_str = str(key).strip()
    
    # 1. Try UUID / direct ID match
    p = db.query(Product).filter(Product.id == key_str).first()
    if p:
        return p
        
    # 2. Try exact name match (case-insensitive)
    p = db.query(Product).filter(Product.name.ilike(key_str)).first()
    if p:
        return p
        
    # 3. Try partial name match
    p = db.query(Product).filter(Product.name.ilike(f"%{key_str}%")).first()
    if p:
        return p
        
    # 4. Fallback to any product in DB
    return db.query(Product).first()




async def handle_bot_callback(db: Session, telegram_id: str, first_name: str, last_name: str, username: str, data: str, message_id: int, callback_query_id: str):
    """Processes interactive inline button actions by editing messages on the user's screen."""
    # Show typing indicator immediately — makes the bot feel human & responsive
    await send_bot_typing(str(telegram_id))

    global MINI_APP_URL
    MINI_APP_URL = get_mini_app_url(db)

    user = db.query(User).filter(User.telegram_id == str(telegram_id)).first()
    display_name = html_escape(f"{first_name or ''} {last_name or ''}".strip() or username or f"User_{telegram_id}")
    username = html_escape(username) if username else ""

    if not user:
        user = User(
            telegram_id=str(telegram_id),
            username=username,
            display_name=display_name,
            wallet_balance=0.0,
            city="India",
            role="user"
        )
        db.add(user)
        db.commit()
        
    # Restore or sync session state live from DB
    import json
    saved_cart = {}
    temp_address = None
    temp_phone = None
    if user.bot_cart:
        try:
            parsed = json.loads(user.bot_cart)
            if isinstance(parsed, dict) and "cart" in parsed:
                saved_cart = parsed.get("cart", {})
                temp_address = parsed.get("temp_address")
                temp_phone = parsed.get("temp_phone")
            else:
                saved_cart = parsed
        except Exception:
            pass

    if str(telegram_id) not in USER_BOT_SESSION:
        USER_BOT_SESSION[str(telegram_id)] = {
            "state": user.bot_state,
            "cart": saved_cart,
            "temp_address": temp_address,
            "temp_phone": temp_phone
        }
    else:
        session = USER_BOT_SESSION[str(telegram_id)]
        if not session.get("state") and user.bot_state is not None:
            session["state"] = user.bot_state
        if ("cart" not in session or not session.get("cart")) and saved_cart:
            session["cart"] = saved_cart

    session = USER_BOT_SESSION[str(telegram_id)]
    
    # If the user clicks any inline button, clear any active text input states
    # to prevent them from being stuck in a waiting state if they navigate away.
    if session.get("state") in ("waiting_for_address", "waiting_for_phone", "waiting_for_promo_code", "waiting_for_topup_amount"):
        session["state"] = None
        session["checkout_pending"] = False

    if user and user.role == "admin" and user.admin_expires_at:
        if datetime.datetime.utcnow() > user.admin_expires_at:
            user.role = "user"
            user.admin_expires_at = None
            db.commit()
            logger.info(f"Demoted user {user.display_name} due to expired admin role duration.")

    admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
    is_admin = str(telegram_id) == str(admin_tg_id) or (user and user.role == "admin")
    
    main_keyboard = {
        "keyboard": [
            [{"text": "🍕 View Menu"}, {"text": "💰 My Wallet"}],
            [{"text": "📍 Change Location"}, {"text": "📦 Track Orders"}],
            [{"text": "🎉 Active Offers"}, {"text": "💬 Contact Support"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    if is_admin:
        main_keyboard["keyboard"].append([
            {"text": "🔑 Admin Center"}
        ])

    if data == "menu_view":
        if not user.city:
            user.city = "India"
            db.commit()
        await display_pizza_menu(db, user, main_keyboard, page=1, category="All", edit_message_id=message_id)
        await answer_callback_query(callback_query_id)
        
    elif data.startswith("menu_noop_") or data == "menu_page_noop":
        parts = data.split("_")
        if len(parts) >= 4:
            p_num, p_tot, p_cat = parts[2], parts[3], parts[4]
            await answer_callback_query(callback_query_id, f"📄 Page {p_num} of {p_tot} ({p_cat})")
        else:
            await answer_callback_query(callback_query_id, "📄 Page Indicator")
        return
        
    elif data.startswith("menu_page_"):
        parts = data.split("_")
        if len(parts) >= 3 and parts[2] == "noop":
            await answer_callback_query(callback_query_id, "📄 Page Indicator")
            return
        try:
            page = int(parts[2])
        except (IndexError, ValueError):
            await answer_callback_query(callback_query_id, "📄 Page Indicator")
            return
        category = parts[3] if len(parts) > 3 else "All"
        await display_pizza_menu(db, user, main_keyboard, page=page, category=category, edit_message_id=message_id)
        await answer_callback_query(callback_query_id)
        
    elif data.startswith("apply_offer_") or data.startswith("apply_deal_"):
        # Unified offer handler — always store offer as a single offer_ cart key
        key = data.replace("apply_offer_", "").replace("apply_deal_", "").strip()
        offer = db.query(ActiveOffer).filter(
            (ActiveOffer.offer_key == key) | (ActiveOffer.id == key)
        ).filter(ActiveOffer.is_active == True).first()

        if not offer:
            await answer_callback_query(callback_query_id, "⚠️ This offer is no longer available.", show_alert=True)
            return

        # Store as a single offer_ key — resolve_cart_item will use ActiveOffer.discounted_price
        cart_key = f"offer_{offer.offer_key}"
        session["cart"] = {cart_key: 1}
        session["active_deal"] = offer.offer_key
        session["deal_price"] = float(offer.discounted_price)
        sync_user_db_session(db, user, session)

        await answer_callback_query(callback_query_id, f"🎉 '{offer.title}' added! ₹{offer.discounted_price:.0f}")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)
        return

    # Legacy apply_deal_1..6 handlers are now merged into apply_offer_ above.

    elif data == "support_menu":
        support_help = (
            "💬 <b>Support & Assistance Hub</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Need assistance with an order, wallet deposit, or want a custom bulk pizza combo?\n\n"
            "• <b>Custom Orders / Bulk Requests:</b> Tap below to request special combos\n"
            "• <b>Direct Message:</b> Send a message directly to our support team\n"
            "• <b>Instant FAQs:</b> Tap any topic below for quick answers"
        )
        support_markup = {
            "inline_keyboard": [
                [{"text": "🍕 Request Custom / Bulk Pizza Combo", "callback_data": "support_custom_order"}],
                [{"text": "💬 Send Direct Message to Support", "callback_data": "support_send_message"}],
                [{"text": "📖 FAQ: How to Order?", "callback_data": "faq_how_to_order"}],
                [{"text": "💳 FAQ: Wallet & Deposits?", "callback_data": "faq_wallet_upi"}],
                [{"text": "📦 FAQ: Where is my Order?", "callback_data": "faq_where_order"}],
                [{"text": "🏠 Main Menu", "callback_data": "menu_view"}]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, support_help, reply_markup=support_markup)
        await answer_callback_query(callback_query_id)

    elif data == "support_custom_order":
        session["state"] = "waiting_for_support_message"
        session["support_relation"] = "Custom Order Request"
        prompt_text = (
            "🍕 <b>Custom / Bulk Order Template Request</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Need a custom pizza deal, party bulk order, or special combination?\n\n"
            "Please type your details in this format:\n"
            "• <b>Pizzas & Quantity:</b> (e.g. 10x Medium Farmhouse + 5x Garlic Bread)\n"
            "• <b>Delivery Location / City:</b>\n"
            "• <b>Preferred Time:</b>\n\n"
            "<i>Our team will review your request and quote a custom discounted price directly in this chat! 💬</i>"
        )
        await send_bot_message(user.telegram_id, prompt_text, reply_markup=main_keyboard)
        await answer_callback_query(callback_query_id)

    elif data == "support_send_message":
        session["state"] = "waiting_for_support_message"
        session["support_relation"] = "General Support"
        await send_bot_message(
            user.telegram_id,
            "💬 <b>Send Support Message</b>\n\n"
            "Please type your message below and press send.\n"
            "Our support team will review it and reply directly in this chat.\n\n"
            "<i>Describe your issue clearly, e.g.:\n"
            "\"My order #BOT-12345 has not been delivered yet.\"</i>",
            reply_markup=main_keyboard
        )
        await answer_callback_query(callback_query_id)

    elif data == "faq_how_to_order":
        faq_text = (
            "📖 <b>FAQ: How to Order?</b>\n\n"
            "1️⃣ Tap <b>🍕 View Menu</b> to browse Domino's pizzas\n"
            "2️⃣ Tap <b>➕ Add to Cart</b> on any item you want\n"
            "3️⃣ Tap <b>🛒 View Cart</b> and then <b>Checkout</b>\n"
            "4️⃣ Confirm your delivery address and phone number\n"
            "5️⃣ Choose to pay with <b>💳 Wallet</b> or <b>📱 UPI QR</b>\n"
            "6️⃣ Confirm your order — done! 🍕\n\n"
            "<b>Quick Tip:</b> Top-up your wallet first (💰 Wallet → Add Funds) for the fastest checkout!"
        )
        back_markup = {
            "inline_keyboard": [
                [
                    {"text": "🔙 Support Menu", "callback_data": "support_menu"},
                    {"text": "🏠 Main Menu", "callback_data": "menu_view"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data == "faq_wallet_upi":
        faq_text = (
            "💳 <b>FAQ: Wallet & Deposits?</b>\n\n"
            "<b>How to Add Funds (Top-Up Wallet):</b>\n"
            "1️⃣ Go to <b>💰 My Wallet</b> → tap <b>💳 Add Funds</b>\n"
            "2️⃣ Select or type the amount\n"
            "3️⃣ Scan the QR code shown and pay the exact amount via any UPI app\n"
            "4️⃣ Tap <b>✅ I Have Paid</b> — our admin will verify and approve your wallet instantly\n\n"
            "<b>How to Pay (Checkout):</b>\n"
            "• At checkout, choose <b>💳 Pay with Wallet</b> (if balance is enough) or <b>📱 Pay via UPI QR</b>\n"
            "• Your balance is shown at checkout so you always know\n\n"
            "<b>Top-Up ID:</b> Every deposit gets a unique <code>TOPUP-XXXXXX</code> ID. Admins verify by this ID!"
        )
        back_markup = {
            "inline_keyboard": [
                [
                    {"text": "🔙 Support Menu", "callback_data": "support_menu"},
                    {"text": "🏠 Main Menu", "callback_data": "menu_view"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data == "faq_where_order":
        faq_text = (
            "📦 <b>FAQ: Where is my Order?</b>\n\n"
            "• Tap <b>📦 Track Orders</b> in the bot to see all your orders from the last 24 hours, including status, rider details, and store info.\n"
            "• Order tracking shows Domino's reference ID, rider name, and your delivery store once your order is dispatched.\n"
            "• You can cancel your order within <b>2 minutes</b> of placing it if it's still in 'Order Processing' status."
        )
        back_markup = {
            "inline_keyboard": [
                [
                    {"text": "🔙 Support Menu", "callback_data": "support_menu"},
                    {"text": "🏠 Main Menu", "callback_data": "menu_view"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data == "faq_bulk_order":
        faq_text = (
            "🍕 <b>FAQ: Bulk Orders & Parties</b>\n\n"
            "Planning a party, corporate event, or large gathering?\n\n"
            "• <b>Special Bulk Discounts:</b> Orders over 10 pizzas or ₹2,000 qualify for custom bulk discounts!\n"
            "• <b>Advance Scheduling:</b> Place bulk orders in advance so Domino's can prepare them on time.\n"
            "• <b>Custom Combos:</b> Want to add side orders, drinks, or custom toppings for 20+ people?\n\n"
            "<b>Contact Support directly or message the admin to arrange bulk orders!</b>"
        )
        back_markup = {
            "inline_keyboard": [
                [
                    {"text": "💬 Message Support", "callback_data": "support_send_message"},
                    {"text": "🔙 Support Menu", "callback_data": "support_menu"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data == "faq_custom_deals":
        faq_text = (
            "🤝 <b>FAQ: Custom Deals & Bulk Orders</b>\n\n"
            "We offer custom pizza combinations at special prices for parties, events, and bulk orders!\n\n"
            "To request a custom deal:\n"
            "1️⃣ Tap <b>🍕 Request Custom / Bulk Pizza Combo</b> in the Support Menu\n"
            "2️⃣ Tell us which pizzas and quantities you need\n"
            "3️⃣ Our team will quote you a discounted bundle price directly in this chat! 🎉"
        )
        back_markup = {
            "inline_keyboard": [
                [
                    {"text": "🔙 Support Menu", "callback_data": "support_menu"},
                    {"text": "🏠 Main Menu", "callback_data": "menu_view"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data.startswith("menu_category_") or data == "show_categories":
        category = data.split("_")[-1] if data.startswith("menu_category_") else "All"
        await display_pizza_menu(db, user, main_keyboard, page=1, category=category, edit_message_id=message_id)
        await answer_callback_query(callback_query_id)
        
    elif data.startswith("cart_add_") or data.startswith("cart_inc_"):
        key_str = data.replace("cart_add_", "").replace("cart_inc_", "").strip()
        cart = session.setdefault("cart", {})
        cart[key_str] = cart.get(key_str, 0) + 1
        sync_user_db_session(db, user, session)
        
        _, _, item_name, _, _ = resolve_cart_item(db, key_str)
        await answer_callback_query(callback_query_id, f"Added 1x {item_name}")
        cart_text, cart_markup = render_cart_message(db, user, cart, session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)
        
    elif data.startswith("cart_sub_") or data.startswith("cart_dec_"):
        key_str = data.replace("cart_sub_", "").replace("cart_dec_", "").strip()
        cart = session.setdefault("cart", {})
        if key_str in cart:
            cart[key_str] -= 1
            if cart[key_str] <= 0:
                del cart[key_str]
        sync_user_db_session(db, user, session)
                
        _, _, item_name, _, _ = resolve_cart_item(db, key_str)
        await answer_callback_query(callback_query_id, f"Updated {item_name}")
        cart_text, cart_markup = render_cart_message(db, user, cart, session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)
        
    elif data.startswith("cart_del_"):
        key_str = data[len("cart_del_"):]
        cart = session.setdefault("cart", {})
        if key_str in cart:
            del cart[key_str]
        sync_user_db_session(db, user, session)
            
        _, _, item_name, _, _ = resolve_cart_item(db, key_str)
        await answer_callback_query(callback_query_id, f"Removed {item_name} from cart")
        cart_text, cart_markup = render_cart_message(db, user, cart, session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data.startswith("cart_info_"):
        key_str = data[len("cart_info_"):]
        _, _, item_name, price, items_breakdown = resolve_cart_item(db, key_str)
        desc = f"{item_name} @ ₹{price:.0f}"
        if items_breakdown:
            desc += f" ({', '.join(items_breakdown)})"
        await answer_callback_query(callback_query_id, desc, show_alert=True)
        
    elif data == "cart_view":
        cart = session.get("cart", {})
        cart_text, cart_markup = render_cart_message(db, user, cart, session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)
        await answer_callback_query(callback_query_id)
        
    elif data in ("cart_empty", "clear_cart"):
        session["cart"] = {}
        session["active_deal"] = None
        session["deal_price"] = None
        sync_user_db_session(db, user, session)
        await edit_bot_message(user.telegram_id, message_id, "🛒 <b>Your Cart is empty!</b>", {
            "inline_keyboard": [[{"text": "🍕 View Menu", "callback_data": "menu_view"}]]
        })
        await answer_callback_query(callback_query_id, "Cart cleared!")
        
    elif data in ("cart_checkout", "initiate_checkout"):
        await initiate_checkout(db, user, session, edit_message_id=message_id)
        await answer_callback_query(callback_query_id)

    elif data == "checkout_confirm_location":
        # User confirmed existing city + saved details
        address = session.get("temp_address")
        phone   = session.get("temp_phone")
        
        if not address:
            saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
            if saved_addr and saved_addr.full_address and saved_addr.full_address != "GPS Location":
                address = saved_addr.full_address
                session["temp_address"] = address
            elif user.address:
                address = user.address
                session["temp_address"] = address

        if not phone and user.phone:
            phone = user.phone
            session["temp_phone"] = phone

        if address and phone:
            session["state"] = "waiting_for_confirm"
            confirm_text, confirm_markup = render_order_confirmation_screen(db, user, session)
            await edit_bot_message(user.telegram_id, message_id, confirm_text, reply_markup=confirm_markup)
            await answer_callback_query(callback_query_id)
        else:
            # Have city but no saved address — ask for address
            session["state"] = "waiting_for_address"
            prompt = (
                "🏡 <b>Delivery Checkout:</b>\n\n"
                "Please type your <b>full delivery address</b> in this chat and press enter."
            )
            # Send clean keyboard containing only Cancel option
            address_keyboard = {
                "keyboard": [[{"text": "❌ Cancel"}]],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            await delete_bot_message(user.telegram_id, message_id)
            await send_bot_message(
                user.telegram_id,
                prompt,
                reply_markup=address_keyboard
            )
            await answer_callback_query(callback_query_id)

    elif data == "checkout_change_location":
        session["checkout_pending"] = True
        session["state"] = "waiting_for_location"
        session["force_address_entry"] = True
        loc_keyboard = {
            "keyboard": [
                [{"text": "📍 Share Current Location", "request_location": True}],
                [{"text": "🔙 Back"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await send_bot_message(
            user.telegram_id,
            "📍 <b>Share Delivery Location</b>\n\n"
            "Tap the <b>📍 Share Current Location</b> button below on your keyboard to share your GPS location.",
            reply_markup=loc_keyboard
        )
        await answer_callback_query(callback_query_id)

    elif data == "checkout_clear_note":
        session["order_note"] = ""
        session["state"] = "waiting_for_confirm"
        await answer_callback_query(callback_query_id, "Note removed.")
        confirm_text, confirm_markup = render_order_confirmation_screen(db, user, session)
        await edit_bot_message(user.telegram_id, message_id, confirm_text, reply_markup=confirm_markup)
        return

    elif data == "checkout_edit_details":
        session["force_address_entry"] = True
        await initiate_checkout(db, user, session, edit_message_id=message_id)
        await answer_callback_query(callback_query_id)
        return

    elif data == "checkout_add_note":
        session["state"] = "waiting_for_order_note"
        current_note = session.get("order_note", "")
        hint = f"\n\n<i>Current note:</i>\n<blockquote>{current_note}</blockquote>" if current_note else ""
        await answer_callback_query(callback_query_id)
        await edit_bot_message(
            user.telegram_id, message_id,
            f"📝 <b>Add a Note to Your Order</b>\n\n"
            f"Type any special delivery instructions, preferences, or notes for the delivery agent:{hint}\n\n"
            f"<i>Examples: \"Please ring the bell\", \"Leave at door\", \"Extra napkins please\", \"No onions\"</i>",
            reply_markup={"inline_keyboard": [[{"text": "❌ Skip / Remove Note", "callback_data": "checkout_clear_note"}]]}
        )
        return



    elif data == "checkout_enter_new":
        session["state"] = "waiting_for_address"
        session["checkout_pending"] = True
        session["temp_address"] = None
        session["temp_phone"]   = None
        sync_user_db_session(db, user, session)
        prompt = (
            "🏡 <b>Delivery Checkout:</b>\n\n"
            "Please type your <b>full delivery address</b> in this chat and press enter."
        )
        address_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            prompt,
            reply_markup=address_keyboard
        )
        await answer_callback_query(callback_query_id)

    elif data == "checkout_enter_phone":
        session["state"] = "waiting_for_phone"
        session["checkout_pending"] = True
        sync_user_db_session(db, user, session)
        prompt = (
            "📱 <b>Phone Number Required:</b>\n\n"
            "Please type your contact number in this chat (e.g. <code>+919999999999</code>) and press enter."
        )
        phone_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(user.telegram_id, prompt, reply_markup=phone_keyboard)
        await answer_callback_query(callback_query_id)

    elif data == "checkout_use_saved":
        address = session.get("temp_address")
        phone   = session.get("temp_phone")

        if not address:
            saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
            if saved_addr and saved_addr.full_address and saved_addr.full_address != "GPS Location":
                address = saved_addr.full_address
                session["temp_address"] = address
            elif user.address:
                address = user.address
                session["temp_address"] = address

        if not phone and user.phone:
            phone = user.phone
            session["temp_phone"] = phone

        if not address or not phone:
            await answer_callback_query(callback_query_id, "Saved details incomplete.")
            if not address:
                session["state"] = "waiting_for_address"
                await send_bot_message(user.telegram_id, "🏡 <b>Please type your full delivery address:</b>")
            elif not phone:
                session["state"] = "waiting_for_phone"
                await send_bot_message(user.telegram_id, "📱 <b>Please enter your contact mobile number:</b>")
            return
            
        session["state"] = "waiting_for_confirm"
        confirm_text, confirm_markup = render_order_confirmation_screen(db, user, session)
        await edit_bot_message(user.telegram_id, message_id, confirm_text, reply_markup=confirm_markup)
        await answer_callback_query(callback_query_id)
        
    elif data == "wallet_view" or data.startswith("wallet_tx_more_"):
        offset = 0
        if data.startswith("wallet_tx_more_"):
            offset = int(data.replace("wallet_tx_more_", ""))
            
        wallet_text, wallet_markup = render_wallet_view(db, user, offset=offset, limit=5)
        await edit_bot_message(user.telegram_id, message_id, wallet_text, wallet_markup)
        await answer_callback_query(callback_query_id)

    elif data.startswith("wallet_tx_history_page_"):
        try:
            page = int(data.replace("wallet_tx_history_page_", "").strip())
        except ValueError:
            page = 1
        limit = 5
        
        all_records = []
        txs = db.query(WalletTransaction).filter(WalletTransaction.user_id == user.id).all()
        for t in txs:
            _ist = (t.created_at + datetime.timedelta(hours=5, minutes=30)) if t.created_at else datetime.datetime.now()
            t_type = (t.type or "tx").lower()
            is_deduction = t_type in ("payment", "debit", "withdrawal", "order", "purchase", "deduction")
            
            sign = "-" if is_deduction else "+"
            icon = "🔴" if is_deduction else "🟢"
            disp_type = "ORDER PAYMENT" if is_deduction else t.type.upper()
            
            all_records.append({
                "id": f"TXN-{t.id[:8].upper()}",
                "type": disp_type,
                "amount": abs(t.amount or 0.0),
                "sign": sign,
                "icon": icon,
                "description": t.description or "Wallet Transaction",
                "date": _ist,
                "date_str": _ist.strftime("%d %b %Y, %I:%M %p IST")
            })

        topup_orders = db.query(Order).filter(Order.user_id == user.id, Order.id.like("TOPUP-%")).all()
        for o in topup_orders:
            ref_id_str = f"TXN-{o.id[:8].upper()}"
            if not any(r["id"] == ref_id_str or r["id"] == o.id for r in all_records):
                _ist = (o.created_at + datetime.timedelta(hours=5, minutes=30)) if o.created_at else datetime.datetime.now()
                is_comp = o.status in ("Completed", "Paid", "Approved")
                all_records.append({
                    "id": o.id,
                    "type": "DEPOSIT",
                    "amount": abs(o.total_payable or 0.0),
                    "sign": "+" if is_comp else "",
                    "icon": "🟢" if is_comp else "🟡",
                    "description": f"UPI Deposit ({'Approved' if is_comp else 'Pending Verification'})",
                    "date": _ist,
                    "date_str": _ist.strftime("%d %b %Y, %I:%M %p IST")
                })
        
        all_records.sort(key=lambda x: x["date"], reverse=True)
        
        total_count = len(all_records)
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        page = max(1, min(page, total_pages))
        offset = (page - 1) * limit
        
        page_records = all_records[offset:offset+limit]
        
        msg = (
            f"📜 <b>Your Wallet Transaction History</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>Account:</b> {user.display_name}\n"
            f"💵 <b>Current Balance:</b> ₹{user.wallet_balance:.2f}\n"
            f"📖 <b>Page {page} of {total_pages}</b> ({total_count} total records)\n\n"
        )
        for r in page_records:
            msg += (
                f"{r['icon']} <b>{r['sign']}₹{r['amount']:.2f}</b> — <b>{r['type']}</b>\n"
                f"  └ <b>Ref:</b> <code>{r['id']}</code>\n"
                f"  └ <b>Details:</b> <i>{r['description']}</i>\n"
                f"  └ <b>Date:</b> {r['date_str']}\n\n"
            )
            
        if not page_records:
            msg += "<i>No transaction history recorded yet.</i>\n"
            
        buttons = []
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"wallet_tx_history_page_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"wallet_tx_history_page_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
        buttons.append([{"text": "🔙 Back to My Wallet", "callback_data": "wallet_view"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "menu_my_orders" or data.startswith("my_orders_page_"):
        page = 1
        if data.startswith("my_orders_page_"):
            try:
                page = int(data.replace("my_orders_page_", "").strip())
            except ValueError:
                page = 1
        limit = 5
        offset = (page - 1) * limit
        
        total_orders = db.query(Order).filter(Order.user_id == user.id).count()
        total_pages = (total_orders + limit - 1) // limit if total_orders > 0 else 1
        page = max(1, min(page, total_pages))
        
        user_orders = db.query(Order).filter(Order.user_id == user.id).order_by(Order.created_at.desc()).offset(offset).limit(limit).all()
        
        msg = f"📦 <b>Your Orders History & Live Status</b>\n"
        msg += f"<i>Page {page} of {total_pages} ({total_orders} total orders)</i>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        if not user_orders:
            msg += "<i>You haven't placed any orders yet.</i>\n\n"
            msg += "🍕 Tap <b>View Menu</b> below to order delicious pizzas!"
        else:
            for o in user_orders:
                _ist = (o.created_at + datetime.timedelta(hours=5, minutes=30)) if o.created_at else datetime.datetime.now()
                date_str = _ist.strftime("%d %b %Y, %I:%M %p IST")
                
                status_icon = "⏳"
                if o.status in ("Completed", "Paid", "Approved"):
                    status_icon = "✅"
                elif o.status in ("Order Processing", "Preparing"):
                    status_icon = "🍕"
                elif o.status in ("Out for Delivery", "Delivering"):
                    status_icon = "🛵"
                elif o.status in ("Cancelled", "Rejected", "Failed", "Expired"):
                    status_icon = "❌"
                    
                items_str = ", ".join([f"{item.quantity}x {item.item_name or 'Pizza'}" for item in (o.items or [])]) or "Domino's Order"
                if len(items_str) > 35:
                    items_str = items_str[:32] + "..."
                    
                msg += (
                    f"{status_icon} <b>Ref ID:</b> <code>{o.id}</code>\n"
                    f"  🍕 <b>Items:</b> {escape_html(items_str)}\n"
                    f"  💵 <b>Amount:</b> <b>₹{o.total_payable:.2f}</b>\n"
                    f"  📊 <b>Status:</b> <b>{o.status}</b>\n"
                    f"  🕒 <b>Date:</b> {date_str}\n\n"
                )
                
        buttons = []
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"my_orders_page_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"my_orders_page_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
            
        buttons.append([
            {"text": "🍕 Order Pizza Now", "callback_data": "menu_view"},
            {"text": "💰 My Wallet", "callback_data": "wallet_view"}
        ])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("pay_now_"):
        order_id = data.replace("pay_now_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        # Construct merchant UPI Payment URI
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        upi_id = upi_id_cfg.value if upi_id_cfg else "pranjalottery@fam"
        upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        
        pay_amount = order.upi_paid if getattr(order, "upi_paid", 0.0) > 0 else order.total_payable
        upi_details = generate_upi_qr_details(upi_id, upi_name, pay_amount, order.id, f"Order {order.id}")
        upi_uri = upi_details["upi_uri"]
        qr_url = upi_details["qr_code_url"]
        qr_data_url = upi_details.get("qr_data_url", "")
        
        breakdown_text = f"• <b>Total Payable:</b> ₹{order.total_payable:.2f}\n"
        if getattr(order, "wallet_applied", 0.0) > 0:
            breakdown_text += f"• <b>Wallet Applied:</b> -₹{order.wallet_applied:.2f}\n"
            breakdown_text += f"• <b>Amount to Pay (UPI):</b> <b>₹{pay_amount:.2f}</b>\n\n"
        else:
            breakdown_text = f"• <b>Amount:</b> <b>₹{pay_amount:.2f}</b>\n\n"
        
        payment_text = (
            f"💳 <b>UPI Payment Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>Order ID:</b> <code>{order.id}</code>\n"
            f"{breakdown_text}"
            f"👉 <a href=\"{upi_uri}\"><b>📱 Click Here to Pay via UPI App</b></a> (mobile) or scan the QR code above.\n\n"
            f"📝 <b>After Payment:</b>\n"
            f"• Tap <b>✅ I Have Paid</b> below after completing payment in your UPI app."
        )
        
        payment_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ I Have Paid / Submit UTR", "callback_data": f"wallet_marked_paid_{order.id}"}
                ],
                [
                    {"text": "❌ Cancel Order", "callback_data": f"cancel_order_{order.id}"}
                ]
            ]
        }
        
        # Send locally-generated QR PNG bytes directly — no external URL fetch needed
        if qr_data_url and qr_data_url.startswith("data:image/png;base64,"):
            import base64 as _b64
            qr_png_bytes = _b64.b64decode(qr_data_url.split(",", 1)[1])
            await send_bot_photo_bytes(user.telegram_id, qr_png_bytes, "upi_qr.png", payment_text, reply_markup=payment_markup)
        else:
            await send_bot_photo(user.telegram_id, qr_url, payment_text, reply_markup=payment_markup)
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("pay_skip_utr_"):
        order_id = data.replace("pay_skip_utr_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        if order.status in ["Pending Verification", "Paid", "Order Processing", "Preparing", "Out for Delivery", "Delivered", "Completed"]:
            await answer_callback_query(callback_query_id, "⚠️ Order already submitted & pending verification!", show_alert=True)
            return

        order.status = "Pending Verification"
        order.transaction_id = f"NO-UTR-{uuid.uuid4().hex[:6].upper()}"
        db.commit()
        
        await edit_bot_message(
            user.telegram_id,
            message_id,
            f"✅ <b>Payment Submitted for Verification!</b>\n\n"
            f"Ref ID: <code>{order.id}</code>\n\n"
            f"We have queued your payment for admin verification. We will update you shortly once confirmed!"
        )
        
        gps_text = f"<a href='https://www.google.com/maps?q={user.latitude},{user.longitude}'>🗺️ View on Google Maps ({user.latitude:.6f}, {user.longitude:.6f})</a>" if (user.latitude is not None and user.longitude is not None) else "Not provided"
        admin_text = (
            "🔔 <b>New Payment Submitted for Order:</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 <b>Order ID:</b> <code>{order.id}</code>\n"
            f"👤 <b>User:</b> {user.display_name} (ID: <code>{user.telegram_id}</code>)\n"
            f"💰 <b>Total Paid:</b> ₹{order.total_payable:.2f}\n"
            f"🏡 <b>Address:</b> <code>{order.address}</code>\n"
            f"📱 <b>Phone:</b> {order.phone}\n"
            f"📍 <b>GPS Coordinates:</b> {gps_text}\n\n"
            "👩‍🍳 <b>Actions:</b>"
        )
        
        action_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ Accept & Complete", "callback_data": f"admin_act_complete_{order.id}"},
                    {"text": "❌ Reject & Refund", "callback_data": f"admin_act_reject_{order.id}"}
                ],
                [
                    {"text": "💬 Reply to Customer", "callback_data": f"admin_reply_support_{user.telegram_id}"}
                ]
            ]
        }
        await notify_admins(db, admin_text, reply_markup=action_markup)
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("cancel_order_") or data.startswith("user_cancel_order_"):
        order_id = data.replace("cancel_order_", "").replace("user_cancel_order_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        cancellable = ["Pending Payment", "Payment Pending"]
        if order.status not in cancellable:
            await answer_callback_query(callback_query_id, "⚠️ Order is already submitted or accepted by admin! Contact support to request cancellation.", show_alert=True)
            return
            
        # Calculate refund for wallet funds used
        refund_amt = 0.0
        if getattr(order, "wallet_applied", 0.0) and order.wallet_applied > 0:
            refund_amt += order.wallet_applied
        elif order.payment_method == "wallet":
            refund_amt += order.total_payable

        if refund_amt > 0:
            user.wallet_balance += refund_amt
            tx = WalletTransaction(
                user_id=user.id,
                type="refund",
                amount=refund_amt,
                description=f"Refund for cancelled order: {order.id}"
            )
            db.add(tx)
            
        order.status = "Cancelled"
        h = OrderStatusHistory(order_id=order.id, status="Cancelled", note="Cancelled by customer")
        db.add(h)
        db.commit()
        auto_save_persistent_db_state(db)
        
        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Cancelled"})
                await sse_broadcast_callback({"type": "wallet_update", "user_id": user.id, "balance": user.wallet_balance})
            except Exception:
                pass
                
        refund_str = f"\n\n💰 <b>₹{refund_amt:.2f}</b> has been credited back to your wallet balance." if refund_amt > 0 else ""
        await send_bot_message(user.telegram_id, f"❌ <b>Order Cancelled</b>\n\nOrder <code>{order.id}</code> has been cancelled successfully.{refund_str}", reply_markup=main_keyboard)
        await answer_callback_query(callback_query_id, "Order cancelled successfully")
        return

    elif data == "wallet_promo":
        confirm_promo_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ Yes, Redeem", "callback_data": "confirm_redeem_yes"},
                    {"text": "❌ No, Cancel", "callback_data": "confirm_redeem_no"}
                ]
            ]
        }
        await edit_bot_message(
            user.telegram_id,
            message_id,
            "🎫 <b>Redeem Promo Code</b>\n\nDo you want to redeem a promo code now?",
            reply_markup=confirm_promo_markup
        )
        await answer_callback_query(callback_query_id)

    elif data == "confirm_redeem_yes":
        session["state"] = "waiting_for_promo_code"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        res = await send_bot_message(
            user.telegram_id,
            "🎫 <b>Redeem Promo Code</b>\n\nPlease type your promo / voucher code directly in this chat:\n\n<i>💡 Coupon codes from admin can be any length. 16-character alphanumeric codes are gift cards that top up your wallet directly.</i>",
            reply_markup=cancel_keyboard
        )
        if isinstance(res, int):
            session["last_bot_msg_id"] = res
        await answer_callback_query(callback_query_id)

    elif data == "confirm_redeem_no":
        session["state"] = None
        wallet_text = (
            "💰 <b>My Wallet Status:</b>\n\n"
            f"• Current Balance: <b>₹{user.wallet_balance:.2f}</b>\n\n"
            "💡 Select an option below to add funds or redeem a promo code:"
        )
        wallet_markup = {
            "inline_keyboard": [
                [
                    {"text": "💳 Add Funds", "callback_data": "wallet_add"},
                    {"text": "🎫 Add Promo Code", "callback_data": "wallet_promo"}
                ],
                [{"text": "🍕 View Menu", "callback_data": "menu_view"}]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, wallet_text, reply_markup=wallet_markup)
        await answer_callback_query(callback_query_id)

    elif data == "admin_payment_management":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        pending_deposits_count = db.query(Order).filter(Order.id.like("TOPUP-%"), Order.status == "Pending Verification").count()
        
        today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today_completed_orders = db.query(Order).filter(
            Order.status == "Completed",
            Order.created_at >= today_start
        ).all()
        today_revenue = sum(o.total_payable for o in today_completed_orders)
        
        msg = (
            "🏦 <b>Payment & Deposit Management Center</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>Pending Deposits:</b> <code>{pending_deposits_count}</code>\n"
            f"• <b>Today's Revenue:</b> <code>₹{today_revenue:.2f}</code>\n\n"
            "Select an option below to process deposits, view 24h/all-time history, or issue manual credits:"
        )
        
        buttons = [
            [
                {"text": "📥 Pending Verification", "callback_data": "admin_view_pending_deposits"},
                {"text": "📜 Deposit History (All)", "callback_data": "admin_dep_all_page_1"}
            ],
            [
                {"text": "🕒 Last 24 Hours Deposits", "callback_data": "admin_dep_24h_page_1"},
                {"text": "🟢 Approved Deposits", "callback_data": "admin_dep_approved_page_1"}
            ],
            [
                {"text": "💰 Manual Wallet Credit", "callback_data": "admin_payment_manual_credit_start"}
            ],
            [
                {"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}
            ]
        ]
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif (data.startswith("admin_deposit_history_page_") or 
          data.startswith("admin_dep_24h_page_") or 
          data.startswith("admin_dep_all_page_") or 
          data.startswith("admin_dep_approved_page_") or 
          data.startswith("admin_dep_pending_page_") or 
          data.startswith("admin_dep_rejected_page_")):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        mode = "all"
        if "dep_24h_page_" in data:
            mode = "24h"
            page_str = data.replace("admin_dep_24h_page_", "").strip()
        elif "dep_approved_page_" in data:
            mode = "approved"
            page_str = data.replace("admin_dep_approved_page_", "").strip()
        elif "dep_pending_page_" in data:
            mode = "pending"
            page_str = data.replace("admin_dep_pending_page_", "").strip()
        elif "dep_rejected_page_" in data:
            mode = "rejected"
            page_str = data.replace("admin_dep_rejected_page_", "").strip()
        elif "dep_all_page_" in data:
            mode = "all"
            page_str = data.replace("admin_dep_all_page_", "").strip()
        else:
            page_str = data.replace("admin_deposit_history_page_", "").strip()
            
        try:
            page = int(page_str)
        except ValueError:
            page = 1
            
        limit = 5
        query = db.query(Order).filter(Order.id.like("TOPUP-%"))
        
        if mode == "24h":
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
            query = query.filter(Order.created_at >= cutoff)
        elif mode == "approved":
            query = query.filter(Order.status.in_(["Completed", "Approved", "Paid"]))
        elif mode == "pending":
            query = query.filter(Order.status == "Pending Verification")
        elif mode == "rejected":
            query = query.filter(Order.status.in_(["Cancelled", "Rejected"]))
            
        total_count = query.count()
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        page = max(1, min(page, total_pages))
        
        deposits = query.order_by(Order.created_at.desc()).offset((page - 1) * limit).limit(limit).all()
        
        upi_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        active_upi_id = upi_cfg.value if upi_cfg else "pranjalottery@fam"
        
        mode_label = "🕒 Last 24 Hours" if mode == "24h" else "🟢 Approved" if mode == "approved" else "🟡 Pending" if mode == "pending" else "🔴 Rejected" if mode == "rejected" else "📜 All Time"
        
        msg = f"📜 <b>Deposit History — {mode_label}</b>\n<i>Page {page} of {total_pages} ({total_count} total records)</i>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for d in deposits:
            status_emoji = "🟢" if d.status in ("Completed", "Approved", "Paid") else "🟡" if d.status == "Pending Verification" else "🔴"
            utr_lbl = d.transaction_id or "No UTR"
            
            _ist_time = d.created_at + datetime.timedelta(hours=5, minutes=30) if d.created_at else None
            date_str = _ist_time.strftime("%d %b %Y, %I:%M %p IST") if _ist_time else "—"
            
            u_name = d.user.display_name if d.user else f"User_{d.user_id}"
            u_tg = d.user.telegram_id if d.user else d.user_id
            
            note_rec = db.query(OrderNote).filter(OrderNote.order_id == d.id).order_by(OrderNote.created_at.desc()).first()
            approved_by = note_rec.admin_username if note_rec else ("System Auto" if d.status in ("Completed", "Approved") else "—")
            
            msg += (
                f"{status_emoji} <b>ID:</b> <code>{d.id}</code> — <b>₹{d.total_payable:.2f}</b> ({d.status})\n"
                f"  👤 <b>User:</b> {u_name} (ID: <code>{u_tg}</code>)\n"
                f"  💳 <b>UPI ID:</b> <code>{active_upi_id}</code> | 🔢 <b>UTR:</b> <code>{utr_lbl}</code>\n"
                f"  👮 <b>Processed By:</b> <code>{approved_by}</code>\n"
                f"  📅 <b>Date:</b> {date_str}\n"
                f"  ━━━━━━━━━━━━━━━━━━━━━━\n"
            )
            
        if not deposits:
            msg += "<i>No deposit requests found under this filter.</i>\n"
            
        cb_prefix = f"admin_dep_{mode}_page_"
        buttons = [
            [
                {"text": ("▶️ 🕒 24 Hours" if mode == "24h" else "🕒 24 Hours"), "callback_data": "admin_dep_24h_page_1"},
                {"text": ("▶️ 🟢 Approved" if mode == "approved" else "🟢 Approved"), "callback_data": "admin_dep_approved_page_1"}
            ],
            [
                {"text": ("▶️ 🟡 Pending" if mode == "pending" else "🟡 Pending"), "callback_data": "admin_dep_pending_page_1"},
                {"text": ("▶️ 🔴 Rejected" if mode == "rejected" else "🔴 Rejected"), "callback_data": "admin_dep_rejected_page_1"}
            ],
            [
                {"text": ("▶️ 📜 All Time" if mode == "all" else "📜 All Time"), "callback_data": "admin_dep_all_page_1"}
            ]
        ]
        
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"{cb_prefix}{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"{cb_prefix}{page+1}"})
        if nav_row:
            buttons.append(nav_row)
            
        buttons.append([{"text": "🔙 Back to Payment Management", "callback_data": "admin_payment_management"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_payment_manual_credit_start":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        session["state"] = "admin_waiting_manual_credit_user"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "💰 <b>Manual Wallet Credit:</b>\n\nPlease enter the Username (e.g. <code>@name</code>), Display Name, or Telegram ID of the user you want to credit funds to:",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_offers_menu":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        
        offers = db.query(ActiveOffer).order_by(ActiveOffer.sort_order.asc()).all()
        
        if not offers:
            msg = (
                "🎉 <b>Active Offers & Deals Management</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "<i>No promotional deals found in database. Click below to add a new deal!</i>"
            )
            buttons = [
                [{"text": "➕ Create New Deal", "callback_data": "admin_offer_create_start"}],
                [{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]
            ]
        else:
            msg = (
                "🎉 <b>Active Offers & Deals Management</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "<i>Manage all live deals and promotional combos shown to customers:</i>\n\n"
            )
            buttons = []
            for off in offers:
                status_icon = "🟢 ACTIVE" if off.is_active else "🔴 DISABLED"
                badge_str = f" [{off.badge}]" if off.badge else ""
                orig_price_str = f" (Was ₹{off.original_price:.0f})" if off.original_price and off.original_price > off.discounted_price else ""
                msg += f"• <b>{escape_html(off.title)}</b>{badge_str} ({status_icon})\n  └ Price: <b>₹{off.discounted_price:.2f}</b>{orig_price_str} | Key: <code>{off.offer_key}</code>\n\n"
                
                toggle_txt = "🔴 Disable" if off.is_active else "🟢 Enable"
                buttons.append([
                    {"text": f"{toggle_txt}", "callback_data": f"admin_offer_toggle_{off.id}"},
                    {"text": "✏️ Price", "callback_data": f"admin_offer_price_{off.id}"},
                    {"text": "🏷️ Badge", "callback_data": f"admin_offer_badge_{off.id}"},
                    {"text": "❌ Delete", "callback_data": f"admin_offer_del_{off.id}"}
                ])
            
            buttons.append([{"text": "➕ Create New Deal", "callback_data": "admin_offer_create_start"}])
            buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])

        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_offer_toggle_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        off_id = data.replace("admin_offer_toggle_", "").strip()
        off = db.query(ActiveOffer).filter((ActiveOffer.id == off_id) | (ActiveOffer.offer_key == off_id)).first()
        if off:
            off.is_active = not off.is_active
            db.commit()
            await answer_callback_query(callback_query_id, f"Offer status changed: {'ACTIVE' if off.is_active else 'DISABLED'}")
        else:
            await answer_callback_query(callback_query_id, "Offer not found!")
        
        offers = db.query(ActiveOffer).order_by(ActiveOffer.sort_order.asc()).all()
        msg = "🎉 <b>Active Offers & Deals Management</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        buttons = []
        for o in offers:
            status_icon = "🟢 ACTIVE" if o.is_active else "🔴 DISABLED"
            badge_str = f" [{o.badge}]" if o.badge else ""
            msg += f"• <b>{escape_html(o.title)}</b>{badge_str} ({status_icon})\n  └ Price: <b>₹{o.discounted_price:.2f}</b>\n\n"
            toggle_txt = "🔴 Disable" if o.is_active else "🟢 Enable"
            buttons.append([
                {"text": f"{toggle_txt}", "callback_data": f"admin_offer_toggle_{o.id}"},
                {"text": "✏️ Price", "callback_data": f"admin_offer_price_{o.id}"},
                {"text": "🏷️ Badge", "callback_data": f"admin_offer_badge_{o.id}"},
                {"text": "❌ Delete", "callback_data": f"admin_offer_del_{o.id}"}
            ])
        buttons.append([{"text": "➕ Create New Deal", "callback_data": "admin_offer_create_start"}])
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif data.startswith("admin_offer_price_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        off_id = data.replace("admin_offer_price_", "").strip()
        session["state"] = f"admin_waiting_offer_price_edit_{off_id}"
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "✏️ <b>Enter New Price (₹):</b>\n\nPlease type the new discounted price in rupees:",
            reply_markup={"force_reply": True, "input_field_placeholder": "Enter price in ₹ (e.g. 299)"}
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_offer_badge_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        off_id = data.replace("admin_offer_badge_", "").strip()
        session["state"] = f"admin_waiting_offer_badge_edit_{off_id}"
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "🏷️ <b>Enter New Badge Text:</b>\n\nPlease type the promo badge (e.g. <code>🔥 50% OFF</code> or <code>⚡ BEST VALUE</code>):",
            reply_markup={"force_reply": True, "input_field_placeholder": "Enter badge text"}
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_offer_del_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        off_id = data.replace("admin_offer_del_", "").strip()
        off = db.query(ActiveOffer).filter((ActiveOffer.id == off_id) | (ActiveOffer.offer_key == off_id)).first()
        if off:
            db.delete(off)
            db.commit()
            await answer_callback_query(callback_query_id, "Offer deleted!")
        else:
            await answer_callback_query(callback_query_id, "Offer not found!")

        offers = db.query(ActiveOffer).order_by(ActiveOffer.sort_order.asc()).all()
        msg = "🎉 <b>Active Offers & Deals Management</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        buttons = []
        for o in offers:
            status_icon = "🟢 ACTIVE" if o.is_active else "🔴 DISABLED"
            badge_str = f" [{o.badge}]" if o.badge else ""
            msg += f"• <b>{escape_html(o.title)}</b>{badge_str} ({status_icon})\n  └ Price: <b>₹{o.discounted_price:.2f}</b>\n\n"
            toggle_txt = "🔴 Disable" if o.is_active else "🟢 Enable"
            buttons.append([
                {"text": f"{toggle_txt}", "callback_data": f"admin_offer_toggle_{o.id}"},
                {"text": "✏️ Price", "callback_data": f"admin_offer_price_{o.id}"},
                {"text": "🏷️ Badge", "callback_data": f"admin_offer_badge_{o.id}"},
                {"text": "❌ Delete", "callback_data": f"admin_offer_del_{o.id}"}
            ])
        buttons.append([{"text": "➕ Create New Deal", "callback_data": "admin_offer_create_start"}])
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif data == "admin_offer_create_start":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        session["state"] = "admin_waiting_offer_title"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "🎉 <b>Create New Deal / Offer</b>\n\n"
            "Please enter the title for the new deal (e.g. <code>🔥 Deal 7: Super Pizza Combo</code>):",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_refresh_stats":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        admin_dashboard_text, admin_inline_markup = render_admin_command_center(db)
        await edit_bot_message(user.telegram_id, message_id, admin_dashboard_text, reply_markup=admin_inline_markup)
        await answer_callback_query(callback_query_id, "Stats Refreshed!")
        return

    elif data == "admin_clear_db_confirm":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        
        main_admin_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048").strip()
        if str(user.telegram_id).strip() != str(main_admin_id).strip():
            await answer_callback_query(
                callback_query_id,
                "⚠️ Only the Primary Main Admin can clear the database!",
                show_alert=True
            )
            return

        session["state"] = "admin_waiting_db_wipe"
        confirm_text = (
            "⚠️ <b>DANGER ZONE: Clear Platform Database</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Are you sure you want to completely reset the platform database?\n\n"
            "This will permanently delete all orders, transactions, error logs, support tickets, and users.\n\n"
            "<b>To proceed, you must type and send the exact phrase:</b>\n"
            "<code>WIPE DB</code>\n\n"
            "<i>(Or tap Cancel below to abort)</i>"
        )
        confirm_markup = {
            "inline_keyboard": [
                [{"text": "❌ Cancel & Return to Command Center", "callback_data": "admin_refresh_stats"}]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, confirm_text, reply_markup=confirm_markup)
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_clear_db_execute":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        
        main_admin_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048").strip()
        if str(user.telegram_id).strip() != str(main_admin_id).strip():
            await answer_callback_query(
                callback_query_id,
                "⚠️ Only the Primary Main Admin can execute database clear!",
                show_alert=True
            )
            return

        # Perform snapshot backup first
        try:
            run_backup(db)
        except Exception:
            pass

        # Clear non-admin tables
        try:
            db.query(OrderNote).delete(synchronize_session=False)
            db.query(OrderStatusHistory).delete(synchronize_session=False)
            db.query(OrderItem).delete(synchronize_session=False)
            db.query(UTRAttempt).delete(synchronize_session=False)
            db.query(QRGenerationHistory).delete(synchronize_session=False)
            db.query(RiderAssignment).delete(synchronize_session=False)
            db.query(WalletTransaction).delete(synchronize_session=False)
            db.query(WithdrawalRequest).delete(synchronize_session=False)
            db.query(SavedAddress).delete(synchronize_session=False)
            db.query(Order).delete(synchronize_session=False)
            db.query(SupportMessage).delete(synchronize_session=False)
            
            # Delete non-admin users
            db.query(User).filter(User.telegram_id != str(main_admin_id)).delete(synchronize_session=False)
            db.commit()
            
            # Save empty persistent DB snapshot to Firebase & disk
            auto_save_persistent_db_state(db)
            
            await answer_callback_query(callback_query_id, "✅ Database reset successfully!", show_alert=True)
        except Exception as e:
            db.rollback()
            await answer_callback_query(callback_query_id, f"❌ Database reset failed: {e}", show_alert=True)
            return

        admin_dashboard_text, admin_inline_markup = render_admin_command_center(db)
        await edit_bot_message(user.telegram_id, message_id, "✅ <b>Database cleared and reset successfully!</b>\n\n" + admin_dashboard_text, reply_markup=admin_inline_markup)
        return

    elif data == "admin_sys_config":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        maint_cfg = db.query(SystemConfig).filter(SystemConfig.key == "maintenance_mode").first()
        
        upi_id = upi_id_cfg.value if upi_id_cfg else "pranjalottery@fam"
        upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        maint_val = maint_cfg.value if maint_cfg else "false"
        maint_status = "⚠️ MAINTENANCE ON" if maint_val == "true" else "🟢 ONLINE"
        
        msg = (
            f"⚙️ <b>System Configuration Control Panel</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>UPI ID:</b> <code>{upi_id}</code>\n"
            f"• <b>UPI Name:</b> <code>{upi_name}</code>\n"
            f"• <b>Platform Status:</b> <code>{maint_status}</code>\n\n"
            f"<i>Use the settings below to adjust system parameters directly in real-time:</i>"
        )
        buttons = [
            [
                {"text": "💳 Update UPI ID", "callback_data": "admin_conf_upi_id"},
                {"text": "👤 Update UPI Name", "callback_data": "admin_conf_upi_name"}
            ],
            [
                {"text": "🛠️ Toggle Maintenance", "callback_data": "admin_toggle_maintenance"}
            ],
            [
                {"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}
            ]
        ]
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_conf_upi_id":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        session["state"] = "admin_waiting_upi_id"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "💳 <b>Update UPI ID:</b>\n\nPlease type the new UPI ID to accept customer payments (e.g. <code>store@upi</code>):",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_conf_upi_name":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        session["state"] = "admin_waiting_upi_name"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "👤 <b>Update UPI Display Name:</b>\n\nPlease type the merchant name that will appear on the payment screen (e.g. <code>Domino's Order Engine</code>):",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_reports_menu":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        msg = (
            "📊 <b>System Reports Center</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Generate a dynamic system PDF report listing user registry details, wallet transaction ledgers, and order history."
        )
        buttons = [
            [
                {"text": "📊 Generate System PDF", "callback_data": "admin_get_pdf"},
                {"text": "💾 Download DB Backup", "callback_data": "admin_get_db"}
            ],
            [
                {"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}
            ]
        ]
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_get_pdf":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        await edit_bot_message(user.telegram_id, message_id, "⏳ <b>Generating User-by-User PDF Report...</b> Please wait.")
        
        from fpdf import FPDF
        import io
        
        users = db.query(DbUser).all()
        
        class SystemReportPDF(FPDF):
            def header(self):
                self.set_fill_color(24, 38, 86)
                self.rect(0, 0, 210, 20, "F")
                self.set_y(4)
                self.set_font("Helvetica", "B", 12)
                self.set_text_color(255, 255, 255)
                self.cell(0, 10, "DOMINO'S ORDER ENGINE SYSTEM REPORT", align="C", ln=True)
                self.ln(5)

            def footer(self):
                self.set_y(-15)
                self.set_font("Helvetica", "I", 8)
                self.set_text_color(128, 128, 128)
                self.cell(0, 5, f"Generated: {datetime.datetime.now().strftime('%d-%m-%Y %I:%M %p')} | Page {self.page_no()}", align="C", ln=True)

        pdf = SystemReportPDF()
        pdf.set_margins(15, 25, 15)
        
        pdf.add_page()
        pdf.set_y(30)
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(24, 38, 86)
        pdf.cell(0, 10, "Executive Summary & System Overview", ln=True)
        pdf.ln(5)
        
        total_orders = db.query(Order).count()
        total_wallets = db.query(sql_func.sum(DbUser.wallet_balance)).scalar() or 0.0
        
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 8, f"Total Registered Users: {len(users)}", ln=True)
        pdf.cell(0, 8, f"Total Orders Placed: {total_orders}", ln=True)
        pdf.cell(0, 8, f"Current Total Wallet Holdings: INR {total_wallets:.2f}", ln=True)
        pdf.ln(10)
        
        def _clean_str(val) -> str:
            if val is None:
                return "N/A"
            return str(val).encode('latin-1', 'replace').decode('latin-1')

        for u in users[:50]:
            pdf.add_page()
            
            pdf.set_font("Helvetica", "B", 13)
            pdf.set_text_color(24, 38, 86)
            pdf.cell(0, 8, _clean_str(f"User Profile: {u.display_name or 'N/A'}"), ln=True)
            pdf.set_font("Helvetica", "", 10)
            pdf.set_text_color(50, 50, 50)
            pdf.cell(0, 6, _clean_str(f"Telegram ID: {u.telegram_id}  |  Username: @{u.username or 'N/A'}"), ln=True)
            pdf.cell(0, 6, _clean_str(f"Phone: {u.phone or 'N/A'}  |  Role: {u.role.upper() if u.role else 'USER'}"), ln=True)
            pdf.cell(0, 6, _clean_str(f"Current Wallet Balance: INR {u.wallet_balance:.2f}  |  Status: {'Blocked' if u.is_blocked else 'Active'}"), ln=True)
            pdf.ln(6)
            
            user_orders = db.query(Order).filter(Order.user_id == u.id, ~Order.id.like("TOPUP-%")).order_by(Order.created_at.desc()).limit(10).all()
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(24, 38, 86)
            pdf.cell(0, 6, "Recent Orders (Max 10):", ln=True)
            pdf.ln(2)
            
            if user_orders:
                pdf.set_font("Helvetica", "B", 8)
                pdf.set_fill_color(230, 235, 245)
                pdf.cell(40, 6, "Order ID", 1, 0, "L", True)
                pdf.cell(45, 6, "Date Placed", 1, 0, "L", True)
                pdf.cell(30, 6, "Total Paid", 1, 0, "R", True)
                pdf.cell(30, 6, "Method", 1, 0, "C", True)
                pdf.cell(35, 6, "Status", 1, 1, "C", True)
                
                pdf.set_font("Helvetica", "", 8)
                pdf.set_text_color(0, 0, 0)
                for o in user_orders:
                    pdf.cell(40, 6, _clean_str(str(o.id)), 1)
                    pdf.cell(45, 6, _clean_str(o.created_at.strftime('%d-%m-%Y %I:%M %p') if o.created_at else 'N/A'), 1)
                    pdf.cell(30, 6, _clean_str(f"INR {o.total_payable:.2f}"), 1, 0, "R")
                    pdf.cell(30, 6, _clean_str(str(o.payment_method).upper()), 1, 0, "C")
                    pdf.cell(35, 6, _clean_str(str(o.status)), 1, 1, "C")
            else:
                pdf.set_font("Helvetica", "I", 9)
                pdf.set_text_color(128, 128, 128)
                pdf.cell(0, 6, "No orders placed.", ln=True)
            pdf.ln(6)
            
            user_txns = db.query(WalletTransaction).filter(WalletTransaction.user_id == u.id).order_by(WalletTransaction.created_at.desc()).limit(10).all()
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(24, 38, 86)
            pdf.cell(0, 6, "Wallet Transactions (Max 10):", ln=True)
            pdf.ln(2)
            
            if user_txns:
                pdf.set_font("Helvetica", "B", 8)
                pdf.set_fill_color(230, 235, 245)
                pdf.cell(45, 6, "Date", 1, 0, "L", True)
                pdf.cell(30, 6, "Type", 1, 0, "C", True)
                pdf.cell(35, 6, "Amount", 1, 0, "R", True)
                pdf.cell(70, 6, "Description", 1, 1, "L", True)
                
                pdf.set_font("Helvetica", "", 8)
                pdf.set_text_color(0, 0, 0)
                for tx in user_txns:
                    pdf.cell(45, 6, _clean_str(tx.created_at.strftime('%d-%m-%Y %I:%M %p') if tx.created_at else 'N/A'), 1)
                    pdf.cell(30, 6, _clean_str(str(tx.type).upper()), 1, 0, "C")
                    pdf.cell(35, 6, _clean_str(f"INR {tx.amount:.2f}"), 1, 0, "R")
                    pdf.cell(70, 6, _clean_str((tx.description or 'N/A')[:40]), 1, 1, "L")
            else:
                pdf.set_font("Helvetica", "I", 9)
                pdf.set_text_color(128, 128, 128)
                pdf.cell(0, 6, "No wallet transactions.", ln=True)

        try:
            # fpdf2 v2+ returns bytearray from output(dest="S"); normalise to bytes
            _raw = pdf.output(dest="S")
            if isinstance(_raw, (bytearray, memoryview)):
                pdf_bytes = bytes(_raw)
            elif isinstance(_raw, str):
                pdf_bytes = _raw.encode("latin1")
            else:
                pdf_bytes = _raw
                
            res = await send_bot_document(
                user.telegram_id,
                pdf_bytes,
                "system_audit_report.pdf",
                "📊 <b>Domino's Order Engine User-by-User Report PDF</b>"
            )
            if res:
                msg_menu = (
                    "📊 <b>System Reports Center</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "✅ User-by-User PDF Report generated and uploaded successfully!"
                )
                buttons_menu = [
                    [
                        {"text": "📊 Generate System PDF", "callback_data": "admin_get_pdf"}
                    ],
                    [
                        {"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}
                    ]
                ]
                await edit_bot_message(user.telegram_id, message_id, msg_menu, reply_markup={"inline_keyboard": buttons_menu})
            else:
                await edit_bot_message(user.telegram_id, message_id, "❌ Failed to upload PDF report document via Telegram.")
        except Exception as ex:
            logger.error(f"Error generating system PDF report: {ex}", exc_info=True)
            await edit_bot_message(user.telegram_id, message_id, f"❌ Error generating PDF: {ex}")
            
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_get_db":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        await edit_bot_message(
            user.telegram_id,
            message_id,
            "⚠️ <b>Security Policy Alert:</b>\n\nRaw database backup downloads are disabled by system security policy to protect user details.",
            reply_markup={"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "admin_reports_menu"}]]}
        )
        await answer_callback_query(callback_query_id, "Disabled by Security Policy!")
        return

    elif data == "admin_view_error_logs":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        logs = db.query(ErrorLog).order_by(ErrorLog.created_at.desc()).limit(10).all()
        if not logs:
            await edit_bot_message(
                user.telegram_id,
                message_id,
                "🟢 <b>No system error logs found!</b>",
                reply_markup={"inline_keyboard": [[{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]]}
            )
            await answer_callback_query(callback_query_id)
            return
            
        msg = "⚠️ <b>System Exception & Error Logs (Latest 10):</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for l in logs:
            date_str = l.created_at.strftime("%d %b %H:%M:%S") if l.created_at else "—"
            msg += f"• <b>[{date_str}]</b> [Type: <code>{l.type}</code>]\n<code>{escape_html((l.message or '')[:250])}</code>\n\n"
            
        buttons = [
            [{"text": "🧹 Clear Error Logs", "callback_data": "admin_clear_error_logs"}],
            [{"text": "🔄 Refresh Logs", "callback_data": "admin_view_error_logs"}, {"text": "🔙 Back", "callback_data": "admin_refresh_stats"}]
        ]
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_clear_error_logs":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        db.query(ErrorLog).delete()
        db.commit()
        await edit_bot_message(
            user.telegram_id,
            message_id,
            "✅ <b>All system error logs have been cleared!</b>",
            reply_markup={"inline_keyboard": [[{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]]}
        )
        await answer_callback_query(callback_query_id, "Logs cleared!")
        return

    elif data == "admin_view_support_tickets":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        
        # Cleanup expired tickets first (>24h)
        cutoff_24h = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
        try:
            db.query(SupportMessage).filter(SupportMessage.created_at < cutoff_24h).delete(synchronize_session=False)
            db.commit()
        except Exception:
            pass

        messages = db.query(SupportMessage).filter(
            SupportMessage.created_at >= cutoff_24h
        ).order_by(SupportMessage.created_at.desc()).all()

        if not messages:
            msg = (
                "💬 <b>Support Ticket Center (24 Hours)</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🟢 <b>No active support tickets in the last 24 hours!</b>\n\n"
                "<i>Support tickets automatically expire after 24 hours.</i>"
            )
            buttons = [[{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]]
            await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
            await answer_callback_query(callback_query_id)
            return

        # Group messages by user
        user_tickets = {}
        for m in messages:
            if m.user_id not in user_tickets:
                user_tickets[m.user_id] = m

        ticket_msg = (
            "💬 <b>Active Support Tickets (Last 24 Hours)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Active customer chats: <b>{len(user_tickets)}</b>\n\n"
        )
        buttons = []
        for uid, last_m in user_tickets.items():
            u = db.query(DbUser).filter(DbUser.id == uid).first()
            u_name = u.display_name if u else "Customer"
            u_tg = u.telegram_id if u else "—"
            time_str = last_m.created_at.strftime("%I:%M %p") if last_m.created_at else ""
            sender_badge = "👤 Customer" if last_m.sender_type == "user" else "🛠️ Admin"

            ticket_msg += f"• <b>{escape_html(u_name)}</b> (TG ID: <code>{u_tg}</code>)\n"
            ticket_msg += f"  [{time_str}] {sender_badge}: <i>{escape_html((last_m.message or '')[:60])}</i>\n\n"

            buttons.append([
                {"text": f"💬 Reply {u_name[:12]}", "callback_data": f"admin_reply_support_{u_tg}"},
                {"text": f"📜 History ({u_name[:10]})", "callback_data": f"admin_history_support_{u_tg}"}
            ])

        buttons.append([{"text": "🔄 Refresh Tickets", "callback_data": "admin_view_support_tickets"}])
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])

        await edit_bot_message(user.telegram_id, message_id, ticket_msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_history_support_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        target_tg = data.replace("admin_history_support_", "").strip()
        target_user = db.query(DbUser).filter(DbUser.telegram_id == target_tg).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return

        cutoff_24h = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
        chat_msgs = db.query(SupportMessage).filter(
            SupportMessage.user_id == target_user.id,
            SupportMessage.created_at >= cutoff_24h
        ).order_by(SupportMessage.created_at.asc()).limit(15).all()

        hist_text = (
            f"📜 <b>Support Chat History (24h): {escape_html(target_user.display_name)}</b>\n"
            f"• Telegram ID: <code>{target_user.telegram_id}</code> | Phone: <code>{target_user.phone or '—'}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        if not chat_msgs:
            hist_text += "<i>No messages exchanged in the last 24 hours.</i>"
        else:
            for cm in chat_msgs:
                s_icon = "👤" if cm.sender_type == "user" else "🛠️ Admin"
                t_str = cm.created_at.strftime("%I:%M %p") if cm.created_at else ""
                hist_text += f"{s_icon} <b>[{t_str}] {cm.sender_type.capitalize()}:</b>\n<blockquote>{escape_html(cm.message)}</blockquote>\n"

        buttons = [
            [{"text": "💬 Reply to Customer", "callback_data": f"admin_reply_support_{target_tg}"}],
            [{"text": "🔙 Back to Support Tickets", "callback_data": "admin_view_support_tickets"}]
        ]
        await edit_bot_message(user.telegram_id, message_id, hist_text, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_broadcast_menu":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        b_msg = (
            "📢 <b>Admin Announcement & Direct Messaging Center</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Select an option below to broadcast a message to all users or message a specific user directly:"
        )
        b_buttons = [
            [
                {"text": "📢 Broadcast to ALL Users", "callback_data": "admin_broadcast_all_start"}
            ],
            [
                {"text": "💬 Direct Message User", "callback_data": "admin_direct_msg_start"}
            ],
            [
                {"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}
            ]
        ]
        await edit_bot_message(user.telegram_id, message_id, b_msg, reply_markup={"inline_keyboard": b_buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_broadcast_all_start":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        session["state"] = "admin_waiting_broadcast_all_text"
        cancel_markup = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "📢 <b>Broadcast Message to All Users</b>\n\nPlease type the broadcast message below to send to ALL registered users:\n\n<i>Supports HTML (e.g. <b>bold</b>, <i>italic</i>, <code>code</code>).</i>",
            reply_markup=cancel_markup
        )
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_broadcast_all_confirm":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        broadcast_text = session.get("temp_broadcast_text")
        if not broadcast_text:
            await answer_callback_query(callback_query_id, "No message found!")
            return
            
        all_user_tg_ids = [u.telegram_id for u in db.query(DbUser).filter(DbUser.telegram_id.isnot(None)).all() if u.telegram_id]
        
        async def run_broadcast(tg_ids, text, admin_id, m_id):
            sent_count = 0
            fail_count = 0
            for target_tg_id in tg_ids:
                ok = await send_bot_message(target_tg_id, f"📢 <b>Announcement from Admin:</b>\n\n{text}")
                if ok:
                    sent_count += 1
                else:
                    fail_count += 1
                await asyncio.sleep(0.05) # Rate limit safety
            report_msg = (
                f"✅ <b>Broadcast Completed!</b>\n\n"
                f"• <b>Sent Successfully:</b> {sent_count} users\n"
                f"• <b>Failed / Blocked:</b> {fail_count} users"
            )
            await edit_bot_message(admin_id, m_id, report_msg, reply_markup={"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "admin_refresh_stats"}]]})
            
        asyncio.create_task(run_broadcast(all_user_tg_ids, broadcast_text, user.telegram_id, message_id))
        
        session["state"] = None
        session["temp_broadcast_text"] = None
        
        await edit_bot_message(user.telegram_id, message_id, "⏳ <b>Broadcasting...</b>\n\nSending messages to all users. This may take a moment. You will be notified here when finished.", reply_markup={"inline_keyboard": [[{"text": "🔙 Back (Running in background)", "callback_data": "admin_refresh_stats"}]]})
        await answer_callback_query(callback_query_id, "Broadcast started!")
        return

    elif data == "admin_direct_msg_start":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        session["state"] = "admin_waiting_direct_user"
        cancel_markup = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "💬 <b>Direct Message User</b>\n\nPlease enter the Username (e.g. <code>@name</code>), Display Name, or Telegram ID of the user you want to message:",
            reply_markup=cancel_markup
        )
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_toggle_maintenance":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        cfg = db.query(SystemConfig).filter(SystemConfig.key == "maintenance_mode").first()
        old_val = cfg.value if cfg else "false"
        new_val = "false" if old_val == "true" else "true"
        if not cfg:
            cfg = SystemConfig(key="maintenance_mode", value=new_val)
            db.add(cfg)
        else:
            cfg.value = new_val
        db.commit()
        
        # Broadcast config change alert to other admins
        asyncio.create_task(broadcast_config_change_to_admins(user.telegram_id, "Maintenance Mode", old_val.upper(), new_val.upper(), db))

        # Update sse
        try:

            if sse_broadcast_callback:
                asyncio.create_task(sse_broadcast_callback({"type": "config_update", "maintenance_mode": cfg.value}))
        except Exception:
            pass

        func = sql_func

        total_users = db.query(DbUser).count()
        today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        total_orders = db.query(Order).count()
        today_orders = db.query(Order).filter(Order.created_at >= today_start).count()
        today_completed_orders = db.query(Order).filter(
            Order.status == "Completed",
            Order.created_at >= today_start
        ).all()
        today_revenue = sum(o.total_payable for o in today_completed_orders)
        total_wallets = db.query(func.sum(DbUser.wallet_balance)).scalar() or 0.0
        pending_orders_count = db.query(Order).filter(Order.status.in_(["Paid", "Pending Payment", "Order Processing"])).count()
        
        
        
        

        maint_status = "⚠️ MAINTENANCE ON" if cfg.value == "true" else "🟢 ONLINE"

        admin_dashboard_text = (
            f"🤖 <b>Platform Admin Command Center</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🛠️ <b>Platform Status:</b> <code>{maint_status}</code>\n"
            f"👥 <b>Total Registered Users:</b> <code>{total_users}</code>\n"
            f"💳 <b>Total Wallet Holdings:</b> <code>₹{total_wallets:.2f}</code>\n\n"
            f"📊 <b>Orders Overview:</b>\n"
            f"• Total Orders placed: <code>{total_orders}</code>\n"
            f"• Orders Today: <code>{today_orders}</code>\n"
            f"• Revenue Today: <b>₹{today_revenue:.2f}</b>\n\n"
            f"⚠️ <b>Action Needed:</b>\n"
            f"• Pending Orders: <b>{pending_orders_count}</b>\n\n"
            f"<i>Use the control panel options below to approve actions manually:</i>"
        )
        admin_inline_markup = {
            "inline_keyboard": [
                [
                    {"text": "📊 Refresh Stats", "callback_data": "admin_refresh_stats"},
                    {"text": "📦 Pending Orders", "callback_data": "admin_view_pending_orders"}
                ],
                [
                    {"text": "🎟️ Manage Promo Codes", "callback_data": "admin_promo_menu"},
                    {"text": "👥 Manage Users", "callback_data": "admin_manage_users"}
                ],
                [
                    {"text": "⚙️ System Config", "callback_data": "admin_sys_config"},
                    {"text": "📊 Reports & Backup", "callback_data": "admin_reports_menu"}
                ],
                [
                    {"text": "💬 Support Tickets (24h)", "callback_data": "admin_view_support_tickets"},
                    {"text": "⚠️ View Error Logs", "callback_data": "admin_view_error_logs"}
                ],
                [
                    {"text": "🔙 Exit to User Menu", "callback_data": "start_menu"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, admin_dashboard_text, reply_markup=admin_inline_markup)
        await answer_callback_query(callback_query_id, f"Platform Status set to {maint_status}!")
        return



    elif data.startswith("admin_reply_support_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        target_tg_id = data.replace("admin_reply_support_", "").strip()
        session["state"] = f"admin_replying_to_{target_tg_id}"
        await send_bot_message(
            user.telegram_id,
            f"💬 <b>Replying to Support Ticket</b>\n\n"
            f"Please type and send your reply message for customer (TG ID: <code>{target_tg_id}</code>). "
            f"It will be forwarded to them instantly."
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_tmpl_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        
        parts = data.replace("admin_tmpl_", "").split("_", 1)
        if len(parts) < 2:
            await answer_callback_query(callback_query_id, "Invalid template data!")
            return
        
        tmpl_code = parts[0]
        target_tg_id = parts[1]
        
        templates = {
            "placed": "🍕 <b>Update from Support:</b> Your order has been successfully placed manually by our agent. You can track its status inside the Track Orders page!",
            "refund": "💸 <b>Update from Support:</b> A refund has been successfully credited back to your wallet balance. Please check your wallet!",
            "utr": "❌ <b>Update from Support:</b> The UPI/UTR transaction ID you provided is invalid or has already been used. Please re-check the receipt and upload a valid UTR.",
            "delay": "🕒 <b>Update from Support:</b> There is a slight delay in manual order placement due to high volume. We are processing it as quickly as possible. Thanks for your patience!"
        }
        
        reply_text = templates.get(tmpl_code)
        if not reply_text:
            await answer_callback_query(callback_query_id, "Template not found!")
            return
            
        target_user = db.query(DbUser).filter(DbUser.telegram_id == target_tg_id).first()
        if target_user:
            try:
                sup = SupportMessage(
                    user_id=target_user.id,
                    sender_type="admin",
                    message=reply_text
                )
                db.add(sup)
                db.commit()
            except Exception as e:
                logger.warning(f"Could not save support template reply: {e}")
                
            await send_bot_message(target_tg_id, reply_text)
            await send_bot_message(
                user.telegram_id,
                f"✅ <b>Template reply sent successfully!</b> (TG ID: <code>{target_tg_id}</code>)."
            )
            await answer_callback_query(callback_query_id, "Reply sent!")
        else:
            await answer_callback_query(callback_query_id, "Target user not found!")
        return

    elif data == "admin_manage_orders_menu" or data.startswith("admin_orders_page_") or data.startswith("admin_orders_filter_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        page = 1
        filter_type = "all"
        
        if data.startswith("admin_orders_page_"):
            try:
                page = int(data.split("_")[-1])
            except Exception:
                page = 1
        elif data.startswith("admin_orders_filter_"):
            parts = data.replace("admin_orders_filter_", "").split("_page_")
            filter_type = parts[0]
            try:
                page = int(parts[1]) if len(parts) > 1 else 1
            except Exception:
                page = 1
                
        # Build query (excluding top-up deposit orders)
        query = db.query(Order).filter(~Order.id.like("TOPUP-%"))
        if filter_type == "active":
            query = query.filter(Order.status.in_(["Paid", "Order Processing", "Pending Payment", "Preparing", "Out for Delivery", "Delivered"]))
        elif filter_type == "completed":
            query = query.filter(Order.status == "Completed")
        elif filter_type == "cancelled":
            query = query.filter(Order.status == "Cancelled")
            
        limit = 5
        offset = (page - 1) * limit
        total_orders = query.count()
        total_pages = (total_orders + limit - 1) // limit if total_orders > 0 else 1
        page = max(1, min(page, total_pages))
        
        orders_list = query.order_by(Order.created_at.desc()).offset(offset).limit(limit).all()
        
        filter_labels = {
            "all": "All Orders",
            "active": "Active/Processing Orders",
            "completed": "Completed Orders",
            "cancelled": "Cancelled Orders"
        }
        filter_label = filter_labels.get(filter_type, "All Orders")
        
        msg = f"🛒 <b>Order Management Panel — {filter_label} (Page {page}/{total_pages}):</b>\n\n"
        buttons = []
        buttons.append([{"text": "🔍 Search by Order ID", "callback_data": "admin_search_order_id"}])
        
        for o in orders_list:
            short_id = o.id
            if len(short_id) > 12:
                short_id = short_id[:12] + "..."
            status_emoji = "🟢" if o.status == "Completed" else "🟡" if o.status in ["Paid", "Order Processing", "Preparing", "Out for Delivery", "Delivered"] else "🔴"
            msg += f"{status_emoji} <code>{o.id}</code> — ₹{o.total_payable:.2f} ({o.status})\n"
            buttons.append([{"text": f"⚙️ Manage {short_id}", "callback_data": f"admin_view_order_{o.id}"}])
            
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"admin_orders_filter_{filter_type}_page_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"admin_orders_filter_{filter_type}_page_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
            
        filter_buttons = []
        for key, label in [("all", "📂 All"), ("active", "🟡 Active"), ("completed", "🟢 Done"), ("cancelled", "🔴 Cancelled")]:
            if key == filter_type:
                filter_buttons.append({"text": f"• {label} •", "callback_data": f"admin_orders_filter_{key}_page_1"})
            else:
                filter_buttons.append({"text": label, "callback_data": f"admin_orders_filter_{key}_page_1"})
        buttons.append(filter_buttons[:2])
        buttons.append(filter_buttons[2:])
        
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_search_order_id":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        session["state"] = "admin_waiting_search_order_id"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "🔍 <b>Search Order:</b>\n\nPlease type the <b>Order ID</b> (e.g. <code>PIZZA-XXXXXX</code>) to search and edit:",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_view_order_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_view_order_", "").strip()
        await send_admin_order_details(user.telegram_id, order_id, db, message_id)
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_order_attach_sc_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_order_attach_sc_", "").strip()
        session["state"] = f"admin_waiting_order_screenshot_{order_id}"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            f"🖼️ <b>Attach Order Screenshot/Receipt:</b>\n\nPlease upload/send a photo receipt for order <code>{order_id}</code>:",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_order_view_sc_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_order_view_sc_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order or not order.screenshot_url:
            await answer_callback_query(callback_query_id, "No screenshot attached!", show_alert=True)
            return
            
        file_id = order.screenshot_url.replace("telegram_file:", "")
        await answer_callback_query(callback_query_id)
        # Send the photo directly to the admin using file_id
        await send_bot_photo(
            user.telegram_id,
            file_id,
            caption=f"🖼️ Receipt for Order <code>{order_id}</code>",
            reply_markup={"inline_keyboard": [[{"text": "🔙 Back to Order Details", "callback_data": f"admin_view_order_{order_id}"}]]}
        )
        return

    elif data.startswith("admin_order_del_sc_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_order_del_sc_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if order:
            order.screenshot_url = None
            db.commit()
            await answer_callback_query(callback_query_id, "Screenshot detached successfully!")
            await send_admin_order_details(user.telegram_id, order_id, db, message_id)
        else:
            await answer_callback_query(callback_query_id, "Order not found!")
        return

    elif data.startswith("admin_edit_ref_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_edit_ref_", "").strip()
        session["state"] = f"admin_waiting_edit_ref_{order_id}"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            f"✏️ <b>Edit Domino's Reference:</b>\n\nPlease type the reference number for order <code>{order_id}</code> (or type <code>None</code> to clear):",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_edit_store_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_edit_store_", "").strip()
        session["state"] = f"admin_waiting_store_{order_id}"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            f"✏️ <b>Edit Sector Store:</b>\n\nPlease type the sector store name for order <code>{order_id}</code> (or type <code>None</code> to clear):",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_edit_rider_name_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_edit_rider_name_", "").strip()
        session["state"] = f"admin_waiting_rider_name_{order_id}"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            f"✏️ <b>Edit Rider Name:</b>\n\nPlease type the rider name for order <code>{order_id}</code> (or type <code>None</code> to clear):",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_edit_rider_phone_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_edit_rider_phone_", "").strip()
        session["state"] = f"admin_waiting_rider_phone_{order_id}"
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            f"✏️ <b>Edit Rider Phone:</b>\n\nPlease type the rider phone number for order <code>{order_id}</code> (or type <code>None</code> to clear):",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return
    elif data.startswith("admin_tpl_delay_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_tpl_delay_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order or not order.user:
            await answer_callback_query(callback_query_id, "Order/User not found!")
            return
        
        msg = f"⏳ <b>Order Status Update</b>\n\nDear {escape_html(order.user.display_name or order.user.username or 'Customer')},\nThere is a slight delay with your order <code>{order.id}</code> due to high volume. We are working on it and will update you shortly! Thank you for your patience."
        await send_bot_message(order.user.telegram_id, msg)
        await answer_callback_query(callback_query_id, "Delay template sent to user!", show_alert=True)
        return

    elif data.startswith("admin_tpl_nopay_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_tpl_nopay_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order or not order.user:
            await answer_callback_query(callback_query_id, "Order/User not found!")
            return
        
        is_topup = order.id.startswith("TOPUP-")
        msg = f"⚠️ <b>Payment Verification Pending</b>\n\nDear {escape_html(order.user.display_name or order.user.username or 'Customer')},\nWe have not yet received or verified the payment for your {'deposit request' if is_topup else 'order'} <code>{order.id}</code>. Please ensure you have completed the payment and submitted the correct UTR/Reference number."
        await send_bot_message(order.user.telegram_id, msg)
        await answer_callback_query(callback_query_id, "Payment pending template sent to user!", show_alert=True)
        return

    elif data.startswith("admin_change_status_menu_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_change_status_menu_", "").strip()
        
        msg = f"🔄 <b>Change Status for Order: {order_id}</b>\n\nSelect the new status below:"
        statuses = ["Accepted", "Order Processing", "Placed", "Preparing", "Out for Delivery", "Delivered", "Completed", "Out of Stock (Cancel & Refund)", "Cancelled"]
        buttons = []
        row = []
        for s in statuses:
            cb_val = "admin_set_status_" + order_id + "_" + ("out_of_stock" if s.startswith("Out of Stock") else s)
            row.append({"text": s, "callback_data": cb_val})
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([{"text": "🔙 Back to Order Editor", "callback_data": f"admin_view_order_{order_id}"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_set_status_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        parts = data.replace("admin_set_status_", "").split("_")
        order_id = parts[0].strip()
        status_key = "_".join(parts[1:]).strip()
        
        is_out_of_stock = (status_key == "out_of_stock")
        new_status = "Cancelled" if is_out_of_stock else status_key
        
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        if new_status == "Out for Delivery":
            r_name = order.rider.rider_name if order.rider else None
            r_phone = order.rider.rider_phone if order.rider else None
            if not r_name or not r_phone or r_name.strip().lower() in ("", "none") or r_phone.strip().lower() in ("", "none"):
                await answer_callback_query(callback_query_id, "❌ Validation Failed: Please enter Rider Name and Rider Phone first!", show_alert=True)
                return
                
        old_status = order.status
        order.status = new_status
        note_str = "Cancelled by admin: Items Out of Stock" if is_out_of_stock else f"Status set manually by admin: {user.username or 'admin'}"
        h = OrderStatusHistory(
            order_id=order.id,
            status=new_status,
            note=note_str
        )
        db.add(h)
        
        # Process refund if transitioning to Cancelled or Refunded
        if (new_status in ("Cancelled", "Refunded") or is_out_of_stock) and old_status not in ("Cancelled", "Refunded"):
            customer = db.query(User).filter(User.id == order.user_id).first()
            if customer and order.payment_method in ("wallet", "upi"):
                customer.wallet_balance += order.total_payable
                desc_str = f"Wallet Refund for Out of Stock order #{order.id[:8]}" if is_out_of_stock else f"Refund for {new_status.lower()} order #{order.id[:8]}"
                refund_tx = WalletTransaction(
                    user_id=customer.id,
                    type="refund",
                    amount=order.total_payable,
                    description=desc_str
                )
                db.add(refund_tx)
        
        db.commit()
        
        # Save persistence snapshot
        auto_save_persistent_db_state(db)
        
        disp_status = "Out of Stock (Cancelled & Refunded)" if is_out_of_stock else new_status
        await answer_callback_query(callback_query_id, f"Status updated to {disp_status}!")
        
        # Notify the user via the bot
        try:
            status_bar = get_order_progress_bar(new_status)
            user_notify_text = (
                f"🔔 <b>Order Status Updated!</b>\n\n"
                f"• <b>Order ID:</b> <code>{order.id}</code>\n"
                f"• <b>New Status:</b> <b>{new_status}</b>\n\n"
                f"<b>Progress:</b>\n{status_bar}"
            )
            if new_status in ("Cancelled", "Refunded") and order.payment_method in ("wallet", "upi"):
                user_notify_text += f"\n\n💰 <b>₹{order.total_payable:.2f}</b> has been refunded to your wallet."
            # Send with screenshot if available, otherwise plain text
            if order.screenshot_url:
                await send_bot_photo(order.user.telegram_id, order.screenshot_url, caption=user_notify_text)
            else:
                await send_bot_message(order.user.telegram_id, user_notify_text)
        except Exception:
            pass
            
        # Re-render using modular helper function
        await send_admin_order_details(user.telegram_id, order.id, db, message_id)
        return

    elif data == "admin_view_pending_orders":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        pending_orders = db.query(Order).filter(
            Order.status.in_(["Paid", "Pending Payment", "Pending Verification", "Order Processing"]),
            ~Order.id.like("TOPUP-%")
        ).order_by(Order.created_at.desc()).limit(15).all()
        
        if not pending_orders:
            await edit_bot_message(
                user.telegram_id,
                message_id,
                "📦 <b>No pending pizza orders found!</b>\n\nAll customer orders have been completed.",
                reply_markup={"inline_keyboard": [[{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]]}
            )
            await answer_callback_query(callback_query_id)
            return

        msg = "📦 <b>Pending Pizza Orders Control Panel</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        buttons = []
        for o in pending_orders:
            u_name = o.user.display_name if o.user else f"User_{o.user_id}"
            dt_str = o.created_at.strftime("%d %b %I:%M %p") if o.created_at else "Recently"
            msg += f"• <code>{o.id[-6:]}</code> - <b>{escape_html(u_name)}</b> (₹{o.total_payable:.0f}) - <i>{o.status}</i>\n"
            buttons.append([{"text": f"⚙️ Manage Order {o.id[-6:]}", "callback_data": f"admin_view_order_{o.id}"}])
            
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_act_complete_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_act_complete_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        session["state"] = f"admin_waiting_ref_{order.id}"
        await edit_bot_message(
            user.telegram_id,
            message_id,
            f"✅ <b>Complete Order: {order.id}</b>\n\n"
            f"Please enter/type the <b>Domino's Reference Number</b> (e.g. <code>DOM-123456</code>) or type <code>None</code> if no reference:",
            reply_markup={"inline_keyboard": [[{"text": "❌ Cancel", "callback_data": "admin_view_pending_orders"}]]}
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_act_approve_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_act_approve_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
        if order.status in ["Completed", "Approved", "Paid", "Order Processing"]:
            await answer_callback_query(callback_query_id, "Already approved!", show_alert=True)
            return
            
        order.status = "Paid"
        h = OrderStatusHistory(order_id=order.id, status="Paid", note="Approved manually via Telegram Bot admin panel")
        db.add(h)
        db.commit()
        
        success_text = (
            f"💳 <b>Payment Confirmed (Manual Admin Approval)!</b>\n"
            f"We verified your payment for Order ID: <code>{order.id}</code>.\n\n"
            f"⏳ <b>Order Status: Paid / Review</b>\n"
            f"The administrator is currently placing your order manually on Domino's. You will receive updates shortly!"
        )
        await send_bot_message(order.user.telegram_id, success_text)
        await answer_callback_query(callback_query_id, "Order Approved!")
        
        # Refresh pending list
        pending_orders = db.query(Order).filter(
            Order.status.in_(["Paid", "Pending Payment", "Pending Verification", "Order Processing"]),
            ~Order.id.like("TOPUP-%")
        ).order_by(Order.created_at.desc()).limit(10).all()
        msg = "📦 <b>Pending Pizza Orders Control Panel</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        buttons = []
        for o in pending_orders:
            u_name = o.user.display_name if o.user else f"User_{o.user_id}"
            u_tg_id = o.user.telegram_id if o.user else o.user_id
            phone_num = o.phone or "Not provided"
            full_addr = (o.address or "Address pending").strip()
            items_str = "  • " + ", ".join([f"{it.quantity}x {it.item_name or (it.product.name if it.product else 'Pizza Item')}" for it in o.items]) if o.items else "  • Pizza Order Items"
            
            msg += (
                f"🍕 <b>Order ID:</b> <code>{o.id}</code>\n"
                f"👤 <b>Customer:</b> <b>{u_name}</b> (ID: <code>{u_tg_id}</code>)\n"
                f"📱 <b>Phone:</b> <code>{phone_num}</code>\n"
                f"🏡 <b>Location:</b> <code>{full_addr}</code>\n"
                f"📦 <b>Items:</b>\n{items_str}\n"
                f"💰 <b>Total Bill:</b> <b>₹{o.total_payable:.2f}</b> (Paid via: <b>{(o.payment_method or 'wallet').upper()}</b>)\n"
                f"🏷️ <b>Status:</b> <code>{o.status}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            )
            action_buttons = []
            if o.status == "Pending Verification":
                action_buttons.append([
                    {"text": f"✅ Approve Deposit/Order", "callback_data": f"admin_dep_approve_{o.id}" if o.id.startswith("TOPUP-") else f"admin_act_approve_{o.id}"},
                    {"text": f"❌ Reject", "callback_data": f"admin_dep_reject_{o.id}" if o.id.startswith("TOPUP-") else f"admin_act_reject_{o.id}"}
                ])
            else:
                action_buttons.append([
                    {"text": f"✅ Complete ({o.id[-6:]})", "callback_data": f"admin_act_complete_{o.id}"},
                    {"text": f"❌ Reject ({o.id[-6:]})", "callback_data": f"admin_act_reject_{o.id}"}
                ])
            buttons.extend(action_buttons)
            
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif data.startswith("admin_act_reject_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_act_reject_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return

        existing_refund = db.query(WalletTransaction).filter(
            WalletTransaction.user_id == order.user.id,
            WalletTransaction.type == "refund",
            WalletTransaction.description.like(f"%{order.id}%")
        ).first()

        if existing_refund or order.status in ["Cancelled", "Cancelled & Refunded"]:
            await answer_callback_query(callback_query_id, "⚠️ Order is already cancelled/refunded!", show_alert=True)
            return

        order.status = "Cancelled"
        h = OrderStatusHistory(order_id=order.id, status="Cancelled", note="Rejected manually via Telegram Bot admin panel")
        db.add(h)
        
        refunded = False
        refund_amt = getattr(order, "wallet_applied", 0.0) or (order.total_payable if order.payment_method == "wallet" else 0.0)
        if refund_amt > 0:
            order.user.wallet_balance += refund_amt
            tx = WalletTransaction(
                user_id=order.user.id,
                type="refund",
                amount=refund_amt,
                description=f"Order Rejected/Cancelled Refund: {order.id}"
            )
            db.add(tx)
            refunded = True
            
        db.commit()
        
        customer_msg = f"❌ <b>Order Rejected/Cancelled:</b>\n\nYour order <code>{order.id}</code> has been rejected/cancelled by the admin."
        if refunded:
            customer_msg += f"\n\n💸 <b>Refund Credited!</b>\n<b>₹{order.total_payable:.2f}</b> has been credited back to your wallet balance. New Balance: <b>₹{order.user.wallet_balance:.2f}</b>"
        else:
            customer_msg += f"\n\nℹ️ Support will verify and process your refund manually."
            
        await send_bot_message(order.user.telegram_id, customer_msg)
        await answer_callback_query(callback_query_id, "Order Rejected & Refunded!")
        
        # Refresh pending list
        pending_orders = db.query(Order).filter(
            Order.status.in_(["Paid", "Pending Payment", "Pending Verification", "Order Processing"]),
            ~Order.id.like("TOPUP-%")
        ).order_by(Order.created_at.desc()).limit(10).all()
        msg = "📦 <b>Pending Pizza Orders Control Panel</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        buttons = []
        for o in pending_orders:
            u_name = o.user.display_name if o.user else f"User_{o.user_id}"
            u_tg_id = o.user.telegram_id if o.user else o.user_id
            phone_num = o.phone or "Not provided"
            full_addr = (o.address or "Address pending").strip()
            items_str = "  • " + ", ".join([f"{it.quantity}x {it.item_name or (it.product.name if it.product else 'Pizza Item')}" for it in o.items]) if o.items else "  • Pizza Order Items"
            
            msg += (
                f"🍕 <b>Order ID:</b> <code>{o.id}</code>\n"
                f"👤 <b>Customer:</b> <b>{u_name}</b> (ID: <code>{u_tg_id}</code>)\n"
                f"📱 <b>Phone:</b> <code>{phone_num}</code>\n"
                f"🏡 <b>Location:</b> <code>{full_addr}</code>\n"
                f"📦 <b>Items:</b>\n{items_str}\n"
                f"💰 <b>Total Bill:</b> <b>₹{o.total_payable:.2f}</b> (Paid via: <b>{(o.payment_method or 'wallet').upper()}</b>)\n"
                f"🏷️ <b>Status:</b> <code>{o.status}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            )
            buttons.append([
                {"text": f"✅ Complete ({o.id[-6:]})", "callback_data": f"admin_act_complete_{o.id}"},
                {"text": f"❌ Reject ({o.id[-6:]})", "callback_data": f"admin_act_reject_{o.id}"}
            ])
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif data == "admin_view_pending_deposits" or data.startswith("admin_deposits_filter_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        filter_mode = data.replace("admin_deposits_filter_", "").strip() if data.startswith("admin_deposits_filter_") else "pending"
        
        query = db.query(Order).filter(Order.id.like("TOPUP-%"))
        if filter_mode == "pending":
            query = query.filter(Order.status.in_(["Pending Verification", "Pending Payment"]))
            title_tag = "⏳ Pending Verification"
        elif filter_mode == "verified":
            query = query.filter(Order.status.in_(["Completed", "Approved", "Paid"]))
            title_tag = "✅ Verified / Completed"
        elif filter_mode == "rejected":
            query = query.filter(Order.status.in_(["Cancelled", "Rejected", "Failed"]))
            title_tag = "❌ Rejected / Cancelled"
        else:
            title_tag = "📜 All Deposit History"
            
        deposits = query.order_by(Order.created_at.desc()).limit(15).all()

        filter_buttons = [
            [
                {"text": f"{'▶ ' if filter_mode=='pending' else ''}⏳ Pending", "callback_data": "admin_deposits_filter_pending"},
                {"text": f"{'▶ ' if filter_mode=='verified' else ''}✅ Verified", "callback_data": "admin_deposits_filter_verified"},
            ],
            [
                {"text": f"{'▶ ' if filter_mode=='rejected' else ''}❌ Rejected", "callback_data": "admin_deposits_filter_rejected"},
                {"text": f"{'▶ ' if filter_mode=='all' else ''}📜 All", "callback_data": "admin_deposits_filter_all"},
            ]
        ]
        
        if not deposits:
            msg = f"🏦 <b>Deposit Management ({title_tag})</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n<i>No deposits found under this filter.</i>"
            buttons = filter_buttons + [[{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]]
            await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
            await answer_callback_query(callback_query_id)
            return

        msg = f"🏦 <b>Deposit Management ({title_tag})</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        action_buttons = []
        for o in deposits:
            u_name = o.user.display_name if o.user else f"User_{o.user_id}"
            dt_str = o.created_at.strftime("%d %b %I:%M %p") if o.created_at else "Recently"
            msg += f"• <code>{o.id[-6:]}</code> - <b>{escape_html(u_name)}</b> (₹{o.total_payable:.0f}) - <i>{o.status}</i>\n"
            action_buttons.append([{"text": f"⚙️ Manage Deposit {o.id[-6:]}", "callback_data": f"admin_view_order_{o.id}"}])
                
        buttons = filter_buttons + action_buttons + [[{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]]
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_dep_approve_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_dep_approve_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Deposit request not found!")
            return
        if order.status in ["Completed", "Approved", "Paid"]:
            await answer_callback_query(callback_query_id, "Already approved!", show_alert=True)
            return
            
        target_user = order.user
        target_user.wallet_balance += order.total_payable
        order.status = "Completed"
        
        attempt = db.query(UTRAttempt).filter(UTRAttempt.order_id == order.id).first()
        if attempt:
            attempt.is_successful = True
            
        # Create WalletTransaction record
        tx = WalletTransaction(
            user_id=target_user.id,
            type="deposit",
            amount=order.total_payable,
            description=f"UPI Deposit Approved (Ref: {order.id})"
        )
        db.add(tx)
        
        h1 = OrderStatusHistory(order_id=order.id, status="Manual Payment Approved")
        db.add(h1)
        h2 = OrderStatusHistory(order_id=order.id, status="Completed")
        db.add(h2)
        
        # Log to OrderNote & AuditLog
        admin_info = f"{user.display_name} (@{user.username or 'admin'} - ID: {user.telegram_id})"
        note = OrderNote(
            order_id=order.id,
            admin_username=user.username or user.display_name or "admin",
            note=f"Deposit approved by: {admin_info}"
        )
        db.add(note)
        
        audit = AuditLog(admin_id=user.id, action="WALLET_TOPUP_APPROVED", details=json.dumps({
            "order_id": order.id,
            "utr": order.transaction_id,
            "amount": order.total_payable,
            "user_id": target_user.id,
            "admin": admin_info
        }))
        db.add(audit)
        db.commit()
        
        # Save persistence state snapshot
        auto_save_persistent_db_state(db)

        # Automatically check and pay any pending orders for target_user
        await process_auto_pay_for_user(db, target_user)
        
        # Notify user via bot
        success_text = (
            f"🎉 <b>Wallet Deposit Approved!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Your deposit of <b>₹{order.total_payable:.2f}</b> (Ref: <code>{order.id}</code>) has been verified & approved!\n\n"
            f"💰 <b>Your New Wallet Balance:</b> <b>₹{target_user.wallet_balance:.2f}</b>\n\n"
            f"You can now order delicious pizzas! 🍕"
        )
        user_markup = {
            "inline_keyboard": [
                [{"text": "🛒 View Cart", "callback_data": "cart_view"}, {"text": "🍕 Order Now", "callback_data": "menu_view"}],
                [{"text": "💰 My Wallet", "callback_data": "wallet_view"}]
            ]
        }
        await send_bot_message(target_user.telegram_id, success_text, reply_markup=user_markup)
        await answer_callback_query(callback_query_id, "Deposit Approved!")
        
        # Update admin message
        approved_text = (
            f"✅ <b>Deposit Request Approved!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 <b>User:</b> {target_user.display_name} (ID: <code>{target_user.telegram_id}</code>)\n"
            f"💰 <b>Amount:</b> ₹{order.total_payable:.2f}\n"
            f"🆔 <b>Ref ID:</b> <code>{order.id}</code>\n"
            f"🔢 <b>UTR:</b> <code>{order.transaction_id or 'None'}</code>\n\n"
            f"👮 <b>Approved By Admin:</b> {admin_info}"
        )
        await edit_bot_message(user.telegram_id, message_id, approved_text, reply_markup={"inline_keyboard": [[{"text": "🔙 Back to Payment Management", "callback_data": "admin_payment_management"}]]})
        
        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "wallet_update", "user_id": target_user.id, "balance": target_user.wallet_balance})
            except Exception:
                pass
        return

    elif data.startswith("admin_dep_reject_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_dep_reject_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Deposit not found!")
            return
        if order.status in ["Completed", "Approved", "Paid"]:
            await answer_callback_query(callback_query_id, "⚠️ Deposit already approved! An approved deposit cannot be rejected.", show_alert=True)
            return
        if order.status in ["Rejected", "Cancelled"]:
            await answer_callback_query(callback_query_id, "Already rejected/cancelled!")
            return
            
        order.status = "Cancelled"
        h = OrderStatusHistory(order_id=order.id, status="Cancelled", note="Rejected manually by admin via bot")
        db.add(h)
        
        # Log to OrderNote
        admin_info = f"@{user.username} ({user.telegram_id})" if user.username else f"{user.display_name} ({user.telegram_id})"
        note = OrderNote(
            order_id=order.id,
            admin_username=user.username or user.display_name or "admin",
            note=f"Deposit rejected by admin: {admin_info}"
        )
        db.add(note)
        db.commit()
        
        admin_username = username or first_name or "Admin"
        
        # Notify user via bot
        reject_text = (
            f"❌ <b>Deposit Request Rejected</b>\n\n"
            f"Your deposit request of <b>₹{order.total_payable:.2f}</b> (Ref: <code>{order.id}</code>) has been rejected by the admin team.\n\n"
            f"Please verify your payment details or contact support."
        )
        await send_bot_message(order.user.telegram_id, reject_text)
        await answer_callback_query(callback_query_id, "Deposit Rejected!")
        
        # Update admin message
        rejected_text = (
            f"❌ <b>Deposit Request Rejected</b>\n\n"
            f"👤 <b>User:</b> {order.user.display_name} (ID: {order.user.telegram_id})\n"
            f"💰 <b>Amount:</b> ₹{order.total_payable:.2f}\n"
            f"🆔 <b>Ref ID:</b> <code>{order.id}</code>\n\n"
            f"Processed by: <b>@{admin_username}</b>"
        )
        await edit_bot_message(user.telegram_id, message_id, rejected_text, reply_markup={"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "admin_refresh_stats"}]]})
        return

    elif data == "admin_manage_users" or data.startswith("admin_users_page_") or data.startswith("admin_users_filter_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        page = 1
        filter_type = "all"
        
        if data.startswith("admin_users_page_"):
            try:
                page = int(data.split("_")[-1])
            except Exception:
                page = 1
        elif data.startswith("admin_users_filter_"):
            parts = data.replace("admin_users_filter_", "").split("_page_")
            filter_type = parts[0]
            try:
                page = int(parts[1]) if len(parts) > 1 else 1
            except Exception:
                page = 1
                
        query = db.query(DbUser)
        if filter_type == "admins":
            query = query.filter(DbUser.role == "admin")
        elif filter_type == "blocked":
            query = query.filter(DbUser.is_blocked == True)
        elif filter_type == "balance":
            query = query.filter(DbUser.wallet_balance > 0.0)
            
        limit = 5
        offset = (page - 1) * limit
        total_users = query.count()
        total_pages = (total_users + limit - 1) // limit if total_users > 0 else 1
        page = max(1, min(page, total_pages))
        
        users_list = query.order_by(DbUser.created_at.desc()).offset(offset).limit(limit).all()
        
        filter_labels = {
            "all": "All Registered Users",
            "admins": "Admins Only",
            "blocked": "Blocked Users",
            "balance": "Users with Balance > ₹0"
        }
        filter_label = filter_labels.get(filter_type, "All Users")
        
        msg = f"👥 <b>User Browser — {filter_label} (Page {page}/{total_pages}):</b>\n\n"
        buttons = []
        buttons.append([{"text": "🔍 Search User by Username/ID/Name", "callback_data": "admin_search_user"}])
        
        for u in users_list:
            status_emoji = "🚫" if u.is_blocked else "🟢"
            role_badge = "👑" if u.role == "admin" else "👤"
            disp = u.display_name or u.username or "Unknown"
            msg += f"{role_badge} {status_emoji} <b>{disp}</b>\n• Balance: ₹{u.wallet_balance:.2f} • ID: <code>{u.id}</code>\n\n"
            buttons.append([{"text": f"⚙️ Manage {(u.display_name or u.username or 'Unknown')[:15]}", "callback_data": f"admin_user_detail_{u.id}"}])
            
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"admin_users_filter_{filter_type}_page_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"admin_users_filter_{filter_type}_page_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
            
        filter_buttons = []
        for key, label in [("all", "📂 All"), ("admins", "👑 Admins"), ("blocked", "🚫 Blocked"), ("balance", "💰 Bal > 0")]:
            if key == filter_type:
                filter_buttons.append({"text": f"• {label} •", "callback_data": f"admin_users_filter_{key}_page_1"})
            else:
                filter_buttons.append({"text": label, "callback_data": f"admin_users_filter_{key}_page_1"})
        buttons.append(filter_buttons[:2])
        buttons.append(filter_buttons[2:])
        
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_search_user":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        session["state"] = "admin_waiting_search_user"
        cancel_markup = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "🔍 <b>Search User Database:</b>\n\nPlease enter the Username (e.g. <code>@name</code>), Display Name, or Telegram ID of the user you want to find:",
            reply_markup=cancel_markup
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_msg_user_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        target_id = data.replace("admin_msg_user_", "").strip()
        session["state"] = f"admin_sending_user_msg_{target_id}"
        cancel_markup = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "💬 <b>Send Personal Message:</b>\n\nPlease type the message you want to send to this user directly (text only):",
            reply_markup=cancel_markup
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_user_detail_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        target_id = data.replace("admin_user_detail_", "").strip()
        target_user = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        await send_admin_user_details(user.telegram_id, target_user.id, db, message_id)
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_promo_menu" or data.startswith("admin_promo_page_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        page = 1
        if data.startswith("admin_promo_page_"):
            try:
                page = int(data.split("_")[-1])
            except ValueError:
                page = 1
                
        limit = 5
        offset = (page - 1) * limit
        total_coupons = db.query(Coupon).count()
        import math
        total_pages = max(1, math.ceil(total_coupons / limit))
        
        coupons = db.query(Coupon).order_by(Coupon.created_at.desc()).offset(offset).limit(limit).all()
        
        msg = f"🎟️ <b>Promo Codes Management (Page {page}/{total_pages}):</b>\n\n"
        buttons = []
        
        if not coupons:
            msg += "<i>No promo codes created yet.</i>\n"
        else:
            for c in coupons:
                status = "🟢 Active" if (c.is_active and c.redeemed_count < c.usage_limit) else "🔴 Inactive"
                msg += f"• <b>Code:</b> <code>{c.code}</code>\n  Value: ₹{c.value:.2f} | Limit: {c.redeemed_count}/{c.usage_limit} | Status: {status}\n\n"
                buttons.append([
                    {"text": f"❌ Delete {c.code}", "callback_data": f"admin_promo_delete_{c.id}"}
                ])
                
        # Navigation
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"admin_promo_page_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"admin_promo_page_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
            
        buttons.append([
            {"text": "➕ Create Promo Code", "callback_data": "admin_promo_create"},
            {"text": "🔙 Back", "callback_data": "admin_refresh_stats"}
        ])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data == "admin_promo_create":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        session["state"] = "admin_waiting_promo_code"
        cancel_markup = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            "🎟️ <b>Create Promo Code</b>\n\n"
            "Please enter the promo code string (e.g. <code>FREE200</code>):",
            reply_markup=cancel_markup
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_promo_delete_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        cid = data.replace("admin_promo_delete_", "").strip()
        coupon = db.query(Coupon).filter(Coupon.id == cid).first()
        if coupon:
            # Delete redemptions first to avoid ForeignKey constraint violation
            db.query(CouponRedemption).filter(CouponRedemption.coupon_id == coupon.id).delete()
            db.delete(coupon)
            db.commit()
            await answer_callback_query(callback_query_id, "Promo code deleted successfully!")
        else:
            await answer_callback_query(callback_query_id, "Promo code not found!")
            
        # Redirect back to promo menu page 1
        page = 1
        limit = 5
        offset = 0
        total_coupons = db.query(Coupon).count()
        import math
        total_pages = max(1, math.ceil(total_coupons / limit))
        
        coupons = db.query(Coupon).order_by(Coupon.created_at.desc()).offset(offset).limit(limit).all()
        
        msg = f"🎟️ <b>Promo Codes Management (Page {page}/{total_pages}):</b>\n\n"
        buttons = []
        if not coupons:
            msg += "<i>No promo codes created yet.</i>\n"
        else:
            for c in coupons:
                status = "🟢 Active" if (c.is_active and c.redeemed_count < c.usage_limit) else "🔴 Inactive"
                msg += f"• <b>Code:</b> <code>{c.code}</code>\n  Value: ₹{c.value:.2f} | Limit: {c.redeemed_count}/{c.usage_limit} | Status: {status}\n\n"
                buttons.append([
                    {"text": f"❌ Delete {c.code}", "callback_data": f"admin_promo_delete_{c.id}"}
                ])
        nav_row = []
        if total_pages > 1:
            nav_row.append({"text": "Next ➡️", "callback_data": "admin_promo_page_2"})
        if nav_row:
            buttons.append(nav_row)
        buttons.append([
            {"text": "➕ Create Promo Code", "callback_data": "admin_promo_create"},
            {"text": "🔙 Back", "callback_data": "admin_refresh_stats"}
        ])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif data.startswith("admin_user_block_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        target_id = data.replace("admin_user_block_", "").strip()
        target_user = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        if str(target_user.telegram_id) == str(admin_tg_id):
            await answer_callback_query(callback_query_id, "Security Restriction: You cannot block the Super Admin!", show_alert=True)
            return
            
        target_user.is_blocked = not target_user.is_blocked
        if target_user.is_blocked:
            if UserSession:
                db.query(UserSession).filter(UserSession.user_id == target_id).update({"is_active": False})
        db.commit()
        
        action = "Blocked" if target_user.is_blocked else "Unblocked"
        await answer_callback_query(callback_query_id, f"User {action} successfully!")
        
        # Reload target_user details and render using helper
        await send_admin_user_details(user.telegram_id, target_user.id, db, message_id)
    elif data.startswith("admin_user_wallet_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        target_id = data.replace("admin_user_wallet_", "").strip()
        target_user = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        session["state"] = f"admin_waiting_wallet_adj_{target_user.id}"
        
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            f"💰 <b>Adjust Wallet Balance:</b>\n\n"
            f"👤 User: <b>{target_user.display_name}</b>\n"
            f"• Current Balance: <b>₹{target_user.wallet_balance:.2f}</b>\n\n"
            f"Please enter the amount to adjust (e.g. <code>+500</code> to credit or <code>-250</code> to debit):",
            reply_markup=cancel_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_user_role_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        target_id = data.replace("admin_user_role_", "").strip()
        target_user = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        if str(target_user.telegram_id) == str(admin_tg_id):
            await answer_callback_query(callback_query_id, "Security Restriction: You cannot modify the Super Admin role!", show_alert=True)
            return
            
        msg = (
            f"👑 <b>Manage Role: {target_user.display_name}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Current Role: <code>{target_user.role.upper()}</code>\n"
            f"Current Expiry: <code>{target_user.admin_expires_at.strftime('%d-%m-%Y %I:%M %p UTC') if target_user.admin_expires_at else 'Permanent / N/A'}</code>\n\n"
            f"Select one of the actions below to promote or demote this user:"
        )
        
        buttons = []
        if target_user.role == "admin":
            buttons.append([{"text": "🚫 Demote to Regular User", "callback_data": f"admin_user_demote_{target_user.id}"}])
        else:
            buttons.append([{"text": "👑 Promote: Permanent", "callback_data": f"admin_user_promote_perm_{target_user.id}"}])
            buttons.append([
                {"text": "⏳ Promote: 1 Hour", "callback_data": f"admin_user_promote_1h_{target_user.id}"},
                {"text": "⏳ Promote: 1 Day", "callback_data": f"admin_user_promote_1d_{target_user.id}"}
            ])
            buttons.append([{"text": "⏳ Promote: 7 Days", "callback_data": f"admin_user_promote_7d_{target_user.id}"}])
            
        buttons.append([{"text": "🔙 Back to User details", "callback_data": f"admin_user_detail_{target_user.id}"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_user_demote_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        target_id = data.replace("admin_user_demote_", "").strip()
        target_user = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        if str(target_user.telegram_id) == str(admin_tg_id):
            await answer_callback_query(callback_query_id, "Security Restriction: You cannot demote the Super Admin!", show_alert=True)
            return
            
        if target_user.telegram_id == user.telegram_id:
            await answer_callback_query(callback_query_id, "You cannot demote yourself!", show_alert=True)
            return
            
        target_user.role = "user"
        target_user.admin_expires_at = None
        db.commit()
        
        await answer_callback_query(callback_query_id, "Demoted to regular user!")
        await send_admin_user_details(user.telegram_id, target_user.id, db, message_id)
        return

    elif data.startswith("admin_user_promote_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        parts = data.replace("admin_user_promote_", "").split("_", 1)
        duration_key = parts[0]
        target_id = parts[1]
        
        target_user = db.query(DbUser).filter(DbUser.id == target_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        if str(target_user.telegram_id) == str(admin_tg_id):
            await answer_callback_query(callback_query_id, "Security Restriction: You cannot modify the Super Admin role!", show_alert=True)
            return
            
        target_user.role = "admin"
        if duration_key == "perm":
            target_user.admin_expires_at = None
        elif duration_key == "1h":
            target_user.admin_expires_at = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        elif duration_key == "1d":
            target_user.admin_expires_at = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        elif duration_key == "7d":
            target_user.admin_expires_at = datetime.datetime.utcnow() + datetime.timedelta(days=7)
            
        db.commit()
        await answer_callback_query(callback_query_id, f"Promoted to Admin ({duration_key})!")
        await send_admin_user_details(user.telegram_id, target_user.id, db, message_id)
        return

    elif data.startswith("adm_u_txs_") or data.startswith("admin_user_txs_page_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        raw = data.replace("adm_u_txs_", "").replace("admin_user_txs_page_", "")
        parts = raw.split("_")
        user_id = parts[0]
        page = int(parts[1]) if len(parts) > 1 else 1
        
        limit = 5
        offset = (page - 1) * limit
        
        target_user = db.query(User).filter(User.id == user_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        total_txs = db.query(WalletTransaction).filter(WalletTransaction.user_id == user_id).count()
        total_pages = (total_txs + limit - 1) // limit if total_txs > 0 else 1
        page = max(1, min(page, total_pages))
        
        txs = db.query(WalletTransaction).filter(WalletTransaction.user_id == user_id).order_by(WalletTransaction.created_at.desc()).offset(offset).limit(limit).all()
        
        msg = f"📜 <b>Wallet Transactions for {target_user.display_name} (Page {page}/{total_pages}):</b>\n\n"
        for t in txs:
            t_sign = "+" if t.amount >= 0 else ""
            desc = f" ({t.description})" if t.description else ""
            date_str = t.created_at.strftime("%d-%m-%Y %I:%M %p") if t.created_at else "—"
            msg += f"• [{date_str}] [Type: <b>{t.type.upper()}</b>]\n  Amount: <b>{t_sign}₹{t.amount:.2f}</b>{desc}\n\n"
            
        if not txs:
            msg += "No transactions found for this user.\n"
            
        buttons = []
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"adm_u_txs_{user_id}_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"adm_u_txs_{user_id}_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
        buttons.append([{"text": "🔙 Back to User details", "callback_data": f"admin_user_detail_{user_id}"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("adm_u_ord_") or data.startswith("admin_user_orders_page_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        raw = data.replace("adm_u_ord_", "").replace("admin_user_orders_page_", "")
        parts = raw.split("_")
        user_id = parts[0]
        page = int(parts[1]) if len(parts) > 1 else 1
        
        limit = 5
        offset = (page - 1) * limit
        
        target_user = db.query(User).filter(User.id == user_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        total_orders = db.query(Order).filter(Order.user_id == user_id).count()
        total_pages = (total_orders + limit - 1) // limit if total_orders > 0 else 1
        page = max(1, min(page, total_pages))
        
        orders = db.query(Order).filter(Order.user_id == user_id).order_by(Order.created_at.desc()).offset(offset).limit(limit).all()
        
        msg = f"📦 <b>Order History for {target_user.display_name} (Page {page}/{total_pages}):</b>\n\n"
        buttons = []
        for o in orders:
            status_emoji = "🟢" if o.status == "Completed" else "🟡" if o.status in ["Paid", "Order Processing"] else "🔴"
            date_str = o.created_at.strftime("%d-%m-%Y %I:%M %p") if o.created_at else "—"
            msg += f"{status_emoji} Order: <code>{o.id}</code>\n  Amount: <b>₹{o.total_payable:.2f}</b> • Status: <code>{o.status}</code> • [{date_str}]\n\n"
            buttons.append([{"text": f"⚙️ Manage {o.id[:12]}...", "callback_data": f"admin_view_order_{o.id}"}])
            
        if not orders:
            msg += "No orders found for this user.\n"
            
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"adm_u_ord_{user_id}_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"adm_u_ord_{user_id}_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
        buttons.append([{"text": "🔙 Back to User details", "callback_data": f"admin_user_detail_{user_id}"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("adm_u_addrs_") or data.startswith("admin_user_addresses_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        user_id = data.replace("adm_u_addrs_", "").replace("admin_user_addresses_", "").strip()
        target_user = db.query(User).filter(User.id == user_id).first()
        if not target_user:
            await answer_callback_query(callback_query_id, "User not found!")
            return
            
        addresses = db.query(SavedAddress).filter(SavedAddress.user_id == user_id).all()
        
        msg = f"📍 <b>Saved Delivery Addresses for {target_user.display_name}:</b>\n\n"
        buttons = []
        for addr in addresses:
            def_badge = " [DEFAULT]" if addr.is_default else ""
            msg += f"🏠 <b>{addr.label.upper()}{def_badge}</b>\n  Address: <i>{addr.full_address}</i>\n  Landmark: <code>{addr.landmark or '—'}</code>\n\n"
            buttons.append([{"text": f"🗑️ Delete {addr.label}", "callback_data": f"adm_u_adel_{addr.id}"}])
            
        if not addresses:
            msg += "No saved addresses found for this user.\n"
            
        buttons.append([{"text": "🔙 Back to User details", "callback_data": f"admin_user_detail_{user_id}"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("adm_u_adel_") or data.startswith("admin_user_addr_del_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        raw = data.replace("adm_u_adel_", "").replace("admin_user_addr_del_", "").strip()
        address_id = raw.split("_")[0]
        
        addr = db.query(SavedAddress).filter(SavedAddress.id == address_id).first()
        user_id = addr.user_id if addr else None
        if addr:
            db.delete(addr)
            db.commit()
            await answer_callback_query(callback_query_id, "Address deleted successfully!")
        else:
            await answer_callback_query(callback_query_id, "Address not found!")
            
        if not user_id:
            await answer_callback_query(callback_query_id, "User not found!")
            return

        # Re-render list
        addresses = db.query(SavedAddress).filter(SavedAddress.user_id == user_id).all()
        target_user = db.query(User).filter(User.id == user_id).first()
        msg = f"📍 <b>Saved Delivery Addresses for {target_user.display_name}:</b>\n\n"
        buttons = []
        for a in addresses:
            def_badge = " [DEFAULT]" if a.is_default else ""
            msg += f"🏠 <b>{a.label.upper()}{def_badge}</b>\n  Address: <i>{a.full_address}</i>\n  Landmark: <code>{a.landmark or '—'}</code>\n\n"
            buttons.append([{"text": f"🗑️ Delete {a.label}", "callback_data": f"adm_u_adel_{a.id}"}])
            
        if not addresses:
            msg += "No saved addresses found for this user.\n"
            
        buttons.append([{"text": "🔙 Back to User details", "callback_data": f"admin_user_detail_{user_id}"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif data == "wallet_add":
        session["state"] = "waiting_for_topup_amount"
        add_funds_prompt = (
            "💳 <b>Add Funds to Wallet</b>\n\n"
            "Select a deposit amount from the options below or choose 'Custom Amount' to type a different value:"
        )
        amount_inline_markup = {
            "inline_keyboard": [
                [
                    {"text": "₹50", "callback_data": "wallet_deposit_50"},
                    {"text": "₹100", "callback_data": "wallet_deposit_100"},
                    {"text": "₹200", "callback_data": "wallet_deposit_200"}
                ],
                [
                    {"text": "₹500", "callback_data": "wallet_deposit_500"},
                    {"text": "₹1000", "callback_data": "wallet_deposit_1000"},
                    {"text": "⌨️ Custom Amount", "callback_data": "wallet_deposit_custom"}
                ],
                [
                    {"text": "❌ Cancel", "callback_data": "wallet_view"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, add_funds_prompt, reply_markup=amount_inline_markup)
        await answer_callback_query(callback_query_id)

    elif data.startswith("wallet_deposit_"):
        amount_str = data.split("_")[-1]
        if amount_str == "custom":
            session["state"] = "waiting_for_topup_amount"
            cancel_keyboard = {
                "keyboard": [[{"text": "❌ Cancel"}]],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            await delete_bot_message(user.telegram_id, message_id)
            res = await send_bot_message(
                user.telegram_id,
                "💳 <b>Enter Custom Amount</b>\n\nPlease type the amount in Rupees you would like to add (e.g. 150):",
                reply_markup=cancel_keyboard
            )
            if isinstance(res, int):
                session["last_bot_msg_id"] = res
            await answer_callback_query(callback_query_id)
        else:
            amount = float(amount_str)
            session["topup_amount"] = amount
            confirm_text = (
                f"📋 <b>Confirm Deposit Request</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💰 Amount to Deposit: <b>₹{amount:.2f}</b>\n\n"
                f"Are you sure you want to proceed with this deposit?"
            )
            confirm_markup = {
                "inline_keyboard": [
                    [
                        {"text": "✅ Yes, Confirm", "callback_data": f"wallet_confirm_deposit_{amount}"},
                        {"text": "❌ Cancel", "callback_data": "wallet_cancel_deposit_unconfirmed"}
                    ]
                ]
            }
            await edit_bot_message(user.telegram_id, message_id, confirm_text, reply_markup=confirm_markup)
            await answer_callback_query(callback_query_id)

    elif data.startswith("wallet_confirm_deposit_"):
        amount = float(data.split("_")[-1])

        # --- Enforce max 2 pending deposit requests ---
        pending_count = db.query(Order).filter(
            Order.user_id == user.id,
            Order.id.like("TOPUP-%"),
            Order.status.in_(["Pending Payment", "Pending Verification"])
        ).count()
        if pending_count >= 2:
            limit_text = (
                "⚠️ <b>Deposit Limit Reached</b>\n\n"
                "You already have <b>2 pending deposit requests</b>. "
                "Please wait for your existing requests to be verified by admin before submitting a new one.\n\n"
                "Go to <b>💰 My Wallet → 🕐 History</b> to see your pending requests."
            )
            await edit_bot_message(user.telegram_id, message_id, limit_text, reply_markup={
                "inline_keyboard": [[{"text": "💰 View Wallet", "callback_data": "wallet_view"}]]
            })
            await answer_callback_query(callback_query_id, "Max 2 pending deposits allowed!")
            return

        # Check max 2 pending payments limit
        pending_count = db.query(Order).filter(
            Order.user_id == user.id,
            Order.status.in_(["Pending Payment", "Pending Verification"])
        ).count()
        if pending_count >= 2:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Pending Limit Reached</b>\n\n"
                "You currently have <b>2 pending orders or wallet deposits</b> awaiting payment/verification.\n"
                "Please complete or resolve your pending requests before creating a new deposit.",
                reply_markup={"inline_keyboard": [[{"text": "📦 My Orders", "callback_data": "menu_my_orders"}, {"text": "📞 Contact Support", "callback_data": "menu_support"}]]}
            )
            return

        # Create a Pending Payment order
        import random
        order_id = f"TOPUP-{random.randint(100000, 999999)}"
        topup_order = Order(
            id=order_id,
            user_id=user.id,
            original_total=amount,
            discount=0.0,
            delivery_charge=0.0,
            total_payable=amount,
            status="Pending Payment",
            payment_method="upi",
            transaction_id=f"TEMP-{uuid.uuid4().hex[:8].upper()}",
            city=user.city or "Mumbai"
        )
        db.add(topup_order)
        db.commit()
        
        # Construct merchant UPI Payment URI
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        upi_id = upi_id_cfg.value if upi_id_cfg else "pranjalottery@fam"
        upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        
        upi_details = generate_upi_qr_details(upi_id, upi_name, amount, order_id, f"Deposit {order_id}")
        upi_uri = upi_details["upi_uri"]
        qr_url = upi_details["qr_code_url"]
        qr_data_url = upi_details.get("qr_data_url", "")
        
        payment_text = (
            f"💳 <b>Deposit Payment Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Ref ID:</b> <code>{order_id}</code>\n"
            f"• <b>Amount:</b> <b>₹{amount:.2f}</b>\n\n"
            f"👉 <a href=\"{upi_uri}\"><b>📱 Click to Pay via UPI App</b></a>\n\n"
            f"<i>After transferring the amount, tap the <b>✅ I Have Paid</b> button below for admin verification.</i>"
        )
        
        base_domain = get_mini_app_url(db).rstrip('/')
        pay_link = f"{base_domain}/api/pay_upi/{order_id}"
        payment_markup = {
            "inline_keyboard": [
                [{"text": f"⚡ Click to Pay ₹{amount:.2f} via UPI App", "url": pay_link}],
                [{"text": "✅ I Have Paid / Verify Payment", "callback_data": f"wallet_marked_paid_{order_id}"}],
                [{"text": "❌ Cancel Request", "callback_data": f"wallet_cancel_deposit_{order_id}"}]
            ]
        }
        
        # Delete previous confirmation message
        await delete_bot_message(user.telegram_id, message_id)
        # Send locally-generated QR PNG bytes — no external URL fetch needed
        if qr_data_url and qr_data_url.startswith("data:image/png;base64,"):
            import base64 as _b64
            qr_png_bytes = _b64.b64decode(qr_data_url.split(",", 1)[1])
            new_msg_res = await send_bot_photo_bytes(user.telegram_id, qr_png_bytes, "upi_qr.png", payment_text, reply_markup=payment_markup)
        else:
            new_msg_res = await send_bot_photo(user.telegram_id, qr_url, payment_text, reply_markup=payment_markup)
        await answer_callback_query(callback_query_id)
        
        new_msg_id = message_id  # fallback message ID since we deleted the old one
        
        # Schedule the 5 minute automatic cancellation task
        async def expire_deposit_task(tg_id: str, msg_id: int, oid: str):
            await asyncio.sleep(300) # 5 minutes
            from .database import SessionLocal as _SL
            bg_db = _SL()
            try:
                ord_obj = bg_db.query(Order).filter(Order.id == oid).first()
                if ord_obj and ord_obj.status == "Pending Payment":
                    ord_obj.status = "Cancelled"
                    bg_db.commit()
                    
                    expired_text = (
                        f"❌ <b>Payment Link Expired</b>\n\n"
                        f"The payment request for <b>₹{ord_obj.total_payable:.2f}</b> (Ref: <code>{oid}</code>) has expired.\n"
                        f"Please request a new deposit if you still wish to add funds."
                    )
                    await edit_bot_message(tg_id, msg_id, expired_text, reply_markup=None)
            except Exception as e:
                logger.error(f"Error in automatic payment expiry: {e}")
            finally:
                bg_db.close()
                
        asyncio.create_task(expire_deposit_task(user.telegram_id, new_msg_id, order_id))

    elif data.startswith("reenter_utr_"):
        order_id = data.replace("reenter_utr_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
        order.status = "Pending Verification"
        db.commit()
        await edit_bot_message(
            user.telegram_id,
            message_id,
            f"✅ <b>Payment Submitted!</b>\n\n"
            f"Ref ID: <code>{order_id}</code>\n"
            f"Your payment has been submitted for admin verification."
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("wallet_marked_paid_"):
        order_id = data.replace("wallet_marked_paid_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        if order.status in ["Pending Verification", "Completed", "Approved", "Paid", "Order Processing"]:
            await answer_callback_query(callback_query_id, "⚠️ Payment verification already submitted & awaiting approval!", show_alert=True)
            conf_text = (
                f"⏳ <b>Payment Submitted for Verification!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 <b>{'Topup ID:' if order.id.startswith('TOPUP-') else 'Order ID:'}</b> <code>{order.id}</code>\n"
                f"💵 <b>Amount:</b> <b>₹{order.total_payable:.2f}</b>\n\n"
                f"<i>Our support team will verify your payment and process your request shortly! 🍕</i>"
            )
            conf_markup = {
                "inline_keyboard": [
                    [{"text": "📞 Contact Support", "callback_data": "support_menu"}]
                ]
            }
            await edit_bot_message(user.telegram_id, message_id, conf_text, reply_markup=conf_markup)
            return

        if order.status in ["Cancelled", "Rejected", "Failed"]:
            await answer_callback_query(callback_query_id, f"⚠️ Request #{order.id} was already {order.status.lower()}!", show_alert=True)
            return

        # Check 10-minute QR validity window
        age_sec = (datetime.datetime.utcnow() - order.created_at).total_seconds()
        if age_sec > 600 and order.status == "Pending Payment":
            expired_text = (
                f"⚠️ <b>Payment QR Code Expired</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Your payment QR session for Order <code>{order.id}</code> has expired (10-minute validity window).\n\n"
                f"• <b>Order Reference:</b> <code>{order.id}</code>\n"
                f"• <b>Total Amount:</b> <b>₹{order.total_payable:.2f}</b>\n\n"
                f"<i>If you have ALREADY transferred funds, tap <b>📞 Contact Support</b> below and share Reference ID <code>{order.id}</code>.\n"
                f"Otherwise, tap <b>🔄 Generate New Payment QR</b> to restart your payment session.</i>"
            )
            expired_markup = {
                "inline_keyboard": [
                    [{"text": "🔄 Generate New Payment QR", "callback_data": f"regen_qr_{order.id}"}],
                    [{"text": "📞 Contact Support", "callback_data": "support_menu"}],
                    [{"text": "❌ Cancel Order", "callback_data": f"cancel_order_{order.id}"}]
                ]
            }
            await send_bot_message(user.telegram_id, expired_text, reply_markup=expired_markup)
            await answer_callback_query(callback_query_id, "⚠️ Payment QR session expired!", show_alert=True)
            return

        order.status = "Pending Verification"
        ref_code = f"{'TOPUP-REF' if order.id.startswith('TOPUP-') else 'BOT-TXN'}-{uuid.uuid4().hex[:6].upper()}"
        order.transaction_id = ref_code
        
        h = OrderStatusHistory(order_id=order.id, status="Pending Verification", note="Customer marked payment as completed")
        db.add(h)
        db.commit()
        auto_save_persistent_db_state(db)
        
        session["state"] = None
        sync_user_db_session(db, user, session)

        # Notify Admin
        admin_alert = (
            f"📥 <b>New Payment Verification Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>Order ID:</b> <code>{order.id}</code>\n"
            f"👤 <b>Customer:</b> <b>{escape_html(user.display_name)}</b> (ID: <code>{user.telegram_id}</code>)\n"
            f"💵 <b>Amount:</b> <b>₹{order.total_payable:.2f}</b>\n"
            f"🔢 <b>Reference:</b> <code>{ref_code}</code>\n\n"
            f"<i>Please verify receipt in UPI merchant app and approve/reject below.</i>"
        )
        action_buttons = [
            [
                {"text": "✅ Approve Deposit/Order", "callback_data": f"admin_dep_approve_{order.id}" if order.id.startswith("TOPUP-") else f"admin_act_approve_{order.id}"},
                {"text": "❌ Reject", "callback_data": f"admin_dep_reject_{order.id}" if order.id.startswith("TOPUP-") else f"admin_act_reject_{order.id}"}
            ]
        ]
        await notify_admins(db, admin_alert, reply_markup={"inline_keyboard": action_buttons})

        conf_text = (
            f"⏳ <b>Payment Submitted for Verification!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>{'Topup ID:' if order.id.startswith('TOPUP-') else 'Order ID:'}</b> <code>{order.id}</code>\n"
            f"💵 <b>Amount:</b> <b>₹{order.total_payable:.2f}</b>\n"
            f"🔢 <b>Transaction Ref:</b> <code>{ref_code}</code>\n\n"
            f"<i>Our support team will verify your payment and process your request shortly! 🍕</i>"
        )
        conf_markup = {
            "inline_keyboard": [
                [{"text": "📦 Track Status", "callback_data": f"track_refresh_{order.id}"}],
                [{"text": "📞 Contact Support", "callback_data": "support_menu"}]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, conf_text, reply_markup=conf_markup)
        await answer_callback_query(callback_query_id, "Payment submitted for verification!")
        return


    elif data.startswith("regen_qr_"):
        order_id = data.replace("regen_qr_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
        order.created_at = datetime.datetime.utcnow()
        db.commit()
        auto_save_persistent_db_state(db)
        await answer_callback_query(callback_query_id, "🔄 Generated new payment QR code!")
        
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        upi_id = upi_id_cfg.value if upi_id_cfg else "pranjalottery@fam"
        upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        pay_amount = order.total_payable
        upi_details = generate_upi_qr_details(upi_id, upi_name, pay_amount, order.id, f"Payment for Order {order.id}")
        upi_uri = upi_details["upi_uri"]
        qr_url = upi_details["qr_code_url"]
        qr_data_url = upi_details.get("qr_data_url", "")
        
        pending_text = (
            f"📱 <b>Fresh QR Code Ready</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Order ID:</b> <code>{order.id}</code>\n"
            f"• <b>Amount:</b> <b>₹{pay_amount:.2f}</b>\n\n"
            f"👉 <a href=\"{upi_uri}\"><b>📱 Click to Pay via UPI App</b></a>\n\n"
            f"<i>After transferring the amount, tap the <b>✅ I Have Paid</b> button below for verification.</i>"
        )
        pending_markup = {
            "inline_keyboard": [
                [{"text": "✅ I Have Paid / Verify Payment", "callback_data": f"wallet_marked_paid_{order.id}"}],
                [{"text": "❌ Cancel Order", "callback_data": f"cancel_order_{order.id}"}]
            ]
        }
        if qr_data_url and qr_data_url.startswith("data:image/png;base64,"):
            import base64 as _b64
            qr_png_bytes = _b64.b64decode(qr_data_url.split(",", 1)[1])
            await send_bot_photo_bytes(user.telegram_id, qr_png_bytes, "upi_qr.png", pending_text, reply_markup=pending_markup)
        else:
            await send_bot_photo(user.telegram_id, qr_url, pending_text, reply_markup=pending_markup)
        return
        
        is_topup = order.id.startswith("TOPUP-") or order.payment_method == "upi"
        
        if is_topup:
            pending_text = (
                f"⏳ <b>Deposit Submitted for Admin Approval</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Your deposit request for <b>₹{order.total_payable:.2f}</b> (Ref: <code>{order_id}</code>) has been submitted for admin verification.\n\n"
                f"We are verifying your transaction. Your wallet balance will be credited automatically upon approval by an admin! 💰"
            )
            pending_markup = {
                "inline_keyboard": [
                    [
                        {"text": "🍕 View Menu", "callback_data": "menu_view"},
                        {"text": "💰 Wallet Menu", "callback_data": "wallet_view"}
                    ]
                ]
            }
            await send_bot_message(user.telegram_id, pending_text, reply_markup=pending_markup)
            await answer_callback_query(callback_query_id, "Submitted for admin approval!")
            
            # Notify admins of the deposit request needing approval
            admin_text = (
                "🔔 <b>New Deposit Marked as Paid (Requires Admin Approval)</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>User:</b> {user.display_name} (ID: <code>{user.telegram_id}</code>)\n"
                f"💰 <b>Amount:</b> ₹{order.total_payable:.2f}\n"
                f"🆔 <b>Ref ID:</b> <code>{order_id}</code>"
            )
            admin_markup = {
                "inline_keyboard": [
                    [
                        {"text": "✅ Approve Deposit", "callback_data": f"admin_dep_approve_{order.id}"},
                        {"text": "❌ Reject Deposit", "callback_data": f"admin_dep_reject_{order.id}"}
                    ]
                ]
            }
            await notify_admins(db, admin_text, reply_markup=admin_markup)
        else:
            # DIRECT UPI PIZZA ORDER
            item_lines = []
            for item in order.items:
                p_name = item.item_name or (item.product.name if item.product else "Pizza Item")
                item_lines.append(f"  • <b>{escape_html(p_name)}</b> ×{item.quantity} — ₹{item.price * item.quantity:.0f}")
                if item.item_details:
                    for sub in item.item_details.split("\n"):
                        if sub.strip():
                            item_lines.append(f"     {escape_html(sub.strip())}")
            items_summary = "\n".join(item_lines) if item_lines else "  • Pizza Items"
            
            pending_text = (
                f"🍕 <b>Direct Order Payment Submitted</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Your payment of <b>₹{order.total_payable:.2f}</b> for Order <code>{order_id}</code> has been submitted for admin verification!\n\n"
                f"🛒 <b>Items Ordered:</b>\n{items_summary}\n\n"
                f"🏠 <b>Delivery Address:</b> <code>{order.address or 'Saved Address'}</code>\n"
                f"📱 <b>Phone:</b> {order.phone or 'Saved Phone'}\n\n"
                f"Our admin team is verifying your payment and will prepare & dispatch your order shortly! You can track status in <b>📦 Track Orders</b>! 🍕"
            )
            pending_markup = {
                "inline_keyboard": [
                    [
                        {"text": "📦 Track Order", "callback_data": f"track_order_{order_id}"},
                        {"text": "🍕 View Menu", "callback_data": "menu_view"}
                    ]
                ]
            }
            await send_bot_message(user.telegram_id, pending_text, reply_markup=pending_markup)
            await answer_callback_query(callback_query_id, "Direct order payment submitted for admin verification!")

            # Build full admin notification using centralized card renderer
            admin_order_text, admin_markup = render_admin_order_notification_card(db, order, action_mode="pending")
            await notify_admins(db, admin_order_text, reply_markup=admin_markup)

        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "order_update", "order_id": order_id, "status": "Pending Verification"})
            except Exception:
                pass

    elif data.startswith("admin_approve_direct_order_"):
        order_id = data.replace("admin_approve_direct_order_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return

        if order.status in ("Order Processing", "Completed", "Delivered", "Cancelled", "Refunded"):
            await answer_callback_query(
                callback_query_id,
                f"ℹ️ Order #{order.id} is already in status '{order.status}'!",
                show_alert=True
            )
            mode = "approved" if order.status in ("Order Processing", "Completed", "Delivered") else "rejected"
            updated_admin_text, updated_admin_markup = render_admin_order_notification_card(db, order, action_mode=mode)
            try:
                await edit_bot_message(user.telegram_id, message_id, updated_admin_text, reply_markup=updated_admin_markup)
            except Exception:
                pass
            return

        order_user = order.user
        payment_method = (order.payment_method or "upi").lower()
        wallet_required = getattr(order, "wallet_applied", 0.0) or (order.total_payable if payment_method == "wallet" else 0.0)

        # Check Insufficient Wallet Balance Safety Rule
        if wallet_required > 0 and order_user:
            if order_user.wallet_balance < wallet_required:
                shortfall = wallet_required - order_user.wallet_balance
                await answer_callback_query(
                    callback_query_id,
                    f"❌ Insufficient Wallet Balance! Customer has ₹{order_user.wallet_balance:.2f}, but ₹{wallet_required:.2f} is required.",
                    show_alert=True
                )
                
                # Notify admin in chat
                admin_warn_text = (
                    f"❌ <b>APPROVAL BLOCKED: INSUFFICIENT WALLET BALANCE!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🆔 <b>Order ID:</b> <code>{order.id}</code>\n"
                    f"👤 <b>Customer:</b> <b>{escape_html(order_user.display_name)}</b> (ID: <code>{order_user.telegram_id}</code>)\n"
                    f"💰 <b>Current Wallet Balance:</b> <b>₹{order_user.wallet_balance:.2f}</b>\n"
                    f"💵 <b>Required Wallet Amount:</b> <b>₹{wallet_required:.2f}</b>\n"
                    f"⚠️ <b>Shortfall:</b> <b>₹{shortfall:.2f}</b>\n\n"
                    f"<i>Please request customer to top-up their wallet or pay via UPI.</i>"
                )
                await send_bot_message(user.telegram_id, admin_warn_text)

                # Notify customer
                customer_warn_text = (
                    f"⚠️ <b>Order Approval Alert — Insufficient Wallet Balance</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Your order <code>{order.id}</code> requires <b>₹{wallet_required:.2f}</b> from your wallet.\n"
                    f"Your current wallet balance is <b>₹{order_user.wallet_balance:.2f}</b>.\n\n"
                    f"Please top-up your wallet using the button below so our admin team can approve and dispatch your order! 🍕"
                )
                markup = {
                    "inline_keyboard": [
                        [{"text": "💳 Top-Up Wallet Now", "callback_data": "menu_wallet"}, {"text": "💬 Contact Support", "callback_data": "menu_support"}]
                    ]
                }
                await send_bot_message(order_user.telegram_id, customer_warn_text, reply_markup=markup)
                return

            # Deduct wallet balance if wallet payment
            existing_wallet_tx = db.query(WalletTransaction).filter(
                WalletTransaction.user_id == order_user.id,
                WalletTransaction.description.like(f"%{order.id}%"),
                WalletTransaction.type == "payment"
            ).first()
            if not existing_wallet_tx:
                order_user.wallet_balance -= wallet_required
                tx = WalletTransaction(
                    user_id=order_user.id,
                    type="payment",
                    amount=-wallet_required,
                    description=f"Wallet Payment for Order: {order.id}"
                )
                db.add(tx)

        # For UPI direct orders, log transaction if not exists
        if payment_method != "wallet":
            existing_tx = db.query(WalletTransaction).filter(
                WalletTransaction.user_id == order.user_id,
                WalletTransaction.description.like(f"%{order.id}%")
            ).first()
            if not existing_tx:
                tx = WalletTransaction(
                    user_id=order.user_id,
                    type="payment",
                    amount=-order.total_payable,
                    description=f"Direct UPI Payment for Order: {order.id}"
                )
                db.add(tx)

        order.status = "Order Processing"
        admin_info = f"@{user.username}" if user.username else user.display_name
        h1 = OrderStatusHistory(order_id=order.id, status="Order Processing", note=f"Approved & verified by Admin ({admin_info})")
        db.add(h1)
        db.commit()
        auto_save_persistent_db_state(db)

        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Order Processing"})
            except Exception:
                pass

        # Notify customer
        if order_user:
            success_text = (
                f"🎉 <b>Payment Verified! Order #{order.id} Approved!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Your order of <b>₹{order.total_payable:.2f}</b> has been verified and approved by our admin team ({admin_info})!\n\n"
                f"Domino's is now preparing your pizzas. Track your order status in <b>📦 Track Orders</b>! 🍕"
            )
            markup = {
                "inline_keyboard": [
                    [{"text": "📦 Track Order", "callback_data": f"track_order_{order.id}"}, {"text": "🍕 View Menu", "callback_data": "menu_view"}]
                ]
            }
            await send_bot_message(order_user.telegram_id, success_text, reply_markup=markup)

        # Render rich updated admin notification card
        updated_admin_text, updated_admin_markup = render_admin_order_notification_card(db, order, action_mode="approved")
        await edit_bot_message(user.telegram_id, message_id, updated_admin_text, reply_markup=updated_admin_markup)
        await answer_callback_query(callback_query_id, "Order approved & dispatched successfully!")

    elif data.startswith("admin_reject_direct_order_"):
        order_id = data.replace("admin_reject_direct_order_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return

        if order.status in ("Cancelled", "Refunded", "Completed", "Delivered"):
            await answer_callback_query(
                callback_query_id,
                f"ℹ️ Order #{order.id} is already in status '{order.status}'!",
                show_alert=True
            )
            mode = "approved" if order.status in ("Order Processing", "Completed", "Delivered") else "rejected"
            updated_admin_text, updated_admin_markup = render_admin_order_notification_card(db, order, action_mode=mode)
            try:
                await edit_bot_message(user.telegram_id, message_id, updated_admin_text, reply_markup=updated_admin_markup)
            except Exception:
                pass
            return

        order.status = "Cancelled"
        refund_amt = getattr(order, "wallet_applied", 0.0) or (order.total_payable if order.payment_method == "wallet" else 0.0)
        refunded = False
        if refund_amt > 0 and order.user:
            order.user.wallet_balance += refund_amt
            tx = WalletTransaction(
                user_id=order.user.id,
                type="refund",
                amount=refund_amt,
                description=f"Refund for Cancelled Order: {order.id}"
            )
            db.add(tx)
            refunded = True

        h1 = OrderStatusHistory(
            order_id=order.id,
            status="Cancelled",
            note=f"Payment rejected by admin. Refunded ₹{refund_amt:.2f} to wallet" if refunded else "Payment rejected by admin"
        )
        db.add(h1)
        db.commit()
        auto_save_persistent_db_state(db)

        # Notify customer
        if order.user:
            reject_text = (
                f"❌ <b>Payment Verification Failed for Order #{order.id}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Your order of <b>₹{order.total_payable:.2f}</b> could not be verified or was rejected by our admin.\n\n"
                + (f"💸 <b>Refund Credited!</b> <b>₹{refund_amt:.2f}</b> has been credited back to your wallet balance. New Balance: <b>₹{order.user.wallet_balance:.2f}</b>\n\n" if refunded else "") +
                f"If you believe this is an error, please contact support or retry."
            )
            markup = {
                "inline_keyboard": [
                    [{"text": "💬 Contact Support", "callback_data": "menu_support"}, {"text": "🍕 View Menu", "callback_data": "menu_view"}]
                ]
            }
            await send_bot_message(order.user.telegram_id, reject_text, reply_markup=markup)

        updated_admin_text, updated_admin_markup = render_admin_order_notification_card(db, order, action_mode="rejected")
        await edit_bot_message(user.telegram_id, message_id, updated_admin_text, reply_markup=updated_admin_markup)
        await answer_callback_query(callback_query_id, "Order rejected & refunded.")

    elif data.startswith("wallet_cancel_deposit_"):
        order_id = data.replace("wallet_cancel_deposit_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if order:
            order.status = "Cancelled"
            db.commit()
            
        await delete_bot_message(user.telegram_id, message_id)
        
        cancel_text = (
            f"❌ <b>Deposit Cancelled</b>\n\n"
            f"Deposit reference <code>#{order_id}</code> was cancelled."
        )
        await send_bot_message(user.telegram_id, cancel_text, reply_markup=main_keyboard)
        await answer_callback_query(callback_query_id, "Deposit reference cancelled!")

    elif data == "wallet_cancel_deposit_unconfirmed":
        session["state"] = None
        session["topup_amount"] = None
        await delete_bot_message(user.telegram_id, message_id)
        cancel_text = (
            "❌ <b>Deposit Request Cancelled</b>\n\n"
            "The deposit request has been cancelled successfully. No funds have been added."
        )
        await send_bot_message(user.telegram_id, cancel_text, reply_markup=main_keyboard)
        await answer_callback_query(callback_query_id, "Deposit Cancelled")

        
    elif data.startswith("user_cancel_order_"):
        order_id = data.replace("user_cancel_order_", "").strip()
        order = db.query(Order).filter(Order.id == order_id, Order.user_id == user.id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
        
        import datetime as _cdt
        age_seconds = (_cdt.datetime.utcnow() - order.created_at).total_seconds()
        history = db.query(OrderStatusHistory).filter(OrderStatusHistory.order_id == order_id).order_by(OrderStatusHistory.created_at.desc()).first()
        current_status = history.status if history else order.status
        
        if age_seconds > 120:
            await answer_callback_query(callback_query_id, "Cancellation window expired (2 min)!")
            await edit_bot_message(user.telegram_id, message_id,
                f"❌ <b>Cancellation Failed</b>\n\nThe 2-minute cancellation window for order <code>{order_id}</code> has expired.",
                {"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "wallet_view"}]]}
            )
            return
        
        if current_status not in ("Order Processing", "Placed"):
            await answer_callback_query(callback_query_id, "Order can no longer be cancelled!")
            return
        
        # Refund wallet if paid by wallet (full or partial)
        wallet_refund_amt = 0.0
        if order.payment_method == "wallet":
            wallet_refund_amt = order.total_payable
        elif order.payment_method in ("partial_wallet_upi", "partial_wallet"):
            # Refund only the wallet portion that was charged
            wallet_refund_amt = getattr(order, "wallet_applied", 0.0) or 0.0
        if wallet_refund_amt > 0:
            order.user.wallet_balance += wallet_refund_amt
            tx = WalletTransaction(
                user_id=user.id,
                type="refund",
                amount=wallet_refund_amt,
                description=f"User-cancelled order wallet refund: {order_id}"
            )
            db.add(tx)
        
        order.status = "Cancelled"
        hist = OrderStatusHistory(order_id=order.id, status="Cancelled", note="Cancelled by user within 2-minute window")
        db.add(hist)
        db.commit()
        
        # Notify admin
        cancel_admin_text = (
            "🚨 <b>Order Cancelled by User</b>\n\n"
            f"👤 <b>User:</b> {user.display_name} (ID: {user.telegram_id})\n"
            f"🆔 <b>Order ID:</b> <code>{order_id}</code>\n"
            f"💰 <b>Amount:</b> ₹{order.total_payable:.2f}\n"
            f"💳 <b>Payment:</b> {order.payment_method.upper()}\n\n"
            f"<i>Cancelled within 2-minute window. {'Wallet portion refunded automatically.' if wallet_refund_amt > 0 else 'No wallet charge.'}</i>"
        )
        asyncio.create_task(notify_admins(db, cancel_admin_text))
        
        refund_note = f"\n💰 <b>Wallet Refunded:</b> ₹{order.total_payable:.2f}" if order.payment_method == "wallet" else ""
        cancel_conf_text = (
            f"✅ <b>Order Cancelled Successfully</b>\n\n"
            f"🆔 <b>Order:</b> <code>{order_id}</code>{refund_note}\n\n"
            f"Your order has been cancelled. The admin team has been notified."
        )
        await edit_bot_message(user.telegram_id, message_id, cancel_conf_text, {
            "inline_keyboard": [
                [{"text": "🚒 Re-order", "callback_data": "menu_view"}],
                [{"text": "💰 My Wallet", "callback_data": "wallet_view"}]
            ]
        })
        await answer_callback_query(callback_query_id, "Order cancelled and refunded!")
        
        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "order_update", "order_id": order_id, "status": "Cancelled"})
            except Exception:
                pass

    elif data.startswith("track_info_rider_"):
        order_id = data.replace("track_info_rider_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if order and order.rider and order.rider.rider_phone and order.rider.rider_phone != "None":
            r_phone = order.rider.rider_phone.strip()
            r_name = order.rider.rider_name.strip() if order.rider.rider_name else "Rider"
            await answer_callback_query(callback_query_id, f"📞 {r_name}\nMobile: {r_phone}\n\n(Dial: {r_phone})", show_alert=True)
        else:
            await answer_callback_query(callback_query_id, "📞 Rider phone number is not available yet.", show_alert=True)
        return

    elif data.startswith("track_refresh_") or data.startswith("track_order_"):
        order_id = data.replace("track_refresh_", "").replace("track_order_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        import datetime as _tdt
        _ist_off = _tdt.timedelta(hours=5, minutes=30)
        history = db.query(OrderStatusHistory).filter(OrderStatusHistory.order_id == order_id).order_by(OrderStatusHistory.created_at.desc()).first()
        current_status = history.status if history else order.status
        _ist_placed = (order.created_at + _ist_off).strftime("%d %b %Y, %I:%M %p IST") if order.created_at else "Recently"
        _ist_now = (_tdt.datetime.utcnow() + _ist_off).strftime("%d %b %Y, %I:%M %p IST")
        
        progress_bar = get_order_progress_bar(current_status)
        
        item_lines = []
        for item in (order.items or []):
            p_name = item.item_name or (item.product.name if item.product else "Pizza Item")
            item_lines.append(f"  • <b>{escape_html(p_name)}</b> ×{item.quantity} — ₹{item.price * item.quantity:.0f}")
            if getattr(item, "item_details", None):
                for sub in item.item_details.split("\n"):
                    if sub.strip():
                        item_lines.append(f"     └ <i>{escape_html(sub.strip())}</i>")
        items_summary = "\n".join(item_lines) if item_lines else "  • Pizza Items"
        
        wallet_applied = getattr(order, "wallet_applied", 0.0) or 0.0
        p_meth = (order.payment_method or "upi").upper()
        if p_meth == "WALLET" and wallet_applied == 0.0:
            wallet_applied = order.total_payable
        upi_paid = max(0.0, order.total_payable - wallet_applied)
        utr_val = order.transaction_id or None

        financial_lines = [f"💰 <b>Total Payable:</b> <b>₹{order.total_payable:.2f}</b>"]
        if wallet_applied > 0:
            financial_lines.append(f"💳 <b>Wallet Paid:</b> ₹{wallet_applied:.2f}")
        if upi_paid > 0:
            financial_lines.append(f"📱 <b>UPI Paid:</b> ₹{upi_paid:.2f}")
        if utr_val:
            financial_lines.append(f"🔢 <b>UTR Ref:</b> <code>{utr_val}</code>")
        financial_summary_text = "\n".join(financial_lines)

        extra_info = ""
        if order.dominos_reference:
            extra_info += f"• <b>Domino's Ref ID:</b> <code>{order.dominos_reference}</code>\n"
        if order.rider:
            extra_info += f"🛵 <b>Rider Name:</b> <b>{order.rider.rider_name}</b>\n"
            if order.rider.rider_phone and order.rider.rider_phone != "None":
                extra_info += f"📞 <b>Rider Mobile:</b> <code>{order.rider.rider_phone}</code>\n"
        if order.sector_store:
            extra_info += f"🏬 <b>Fulfilling Store:</b> {order.sector_store}\n"
        
        highlight_box = ""
        if current_status.lower() in ("out for delivery", "delivery"):
            highlight_box = "🚨 <b>OUT FOR DELIVERY!</b> 🛵\n<i>Your rider is on the way with your hot pizza!</i>\n\n"

        track_text = (
            f"📦 <b>LIVE ORDER TRACKING</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{highlight_box}"
            f"🆔 <b>Order ID:</b> <code>{order.id}</code>\n"
            f"📊 <b>Status:</b> {progress_bar}\n"
            f"🕒 <b>Placed At:</b> {_ist_placed}\n\n"
            f"🛒 <b>Items Ordered:</b>\n{items_summary}\n\n"
            f"{financial_summary_text}\n"
            f"🏠 <b>Address:</b> <code>{order.address or 'Saved Address'}</code>\n"
            f"📱 <b>Phone:</b> {order.phone or 'Saved Phone'}\n"
            + (f"📝 <b>Note:</b> <i>{order.delivery_instructions}</i>\n" if order.delivery_instructions else "")
            + (f"\n{extra_info}" if extra_info else "")
            + f"\n🕒 <i>Refreshed live at {_ist_now}</i>"
        )
        
        buttons = []
        if order.rider and order.rider.rider_phone and order.rider.rider_phone != "None":
            r_phone = order.rider.rider_phone.strip()
            buttons.append([{"text": f"📞 Call Rider ({order.rider.rider_name}): {r_phone}", "callback_data": f"track_info_rider_{order_id}"}])

        if getattr(order, "screenshot_url", None):
            buttons.append([{"text": "📸 View Order Receipt", "callback_data": f"view_receipt_{order_id}"}])

        buttons.append([{"text": "💬 Contact Support for this Order", "callback_data": f"support_order_{order_id}"}])
        buttons.append([{"text": "🔄 Refresh Status", "callback_data": f"track_refresh_{order_id}"}])
        buttons.append([{"text": "🍕 View Menu", "callback_data": "menu_view"}, {"text": "📦 My Orders", "callback_data": "menu_my_orders"}])
        
        if current_status in ("Order Processing", "Pending", "Pending Payment") and order.created_at:
            age_sec = (_tdt.datetime.utcnow() - order.created_at.replace(tzinfo=None)).total_seconds()
            if age_sec <= 120:
                buttons.insert(1, [{"text": "❌ Cancel Order (2m window)", "callback_data": f"cancel_order_{order_id}"}])
                
        refresh_markup = {"inline_keyboard": buttons}
        
        try:
            await edit_bot_message(user.telegram_id, message_id, track_text, refresh_markup)
        except Exception:
            await send_bot_message(user.telegram_id, track_text, reply_markup=refresh_markup)
            
        await answer_callback_query(callback_query_id, "Order tracking updated!")
        return

    elif data.startswith("view_receipt_"):
        order_id = data.replace("view_receipt_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order or not getattr(order, "screenshot_url", None) or not order.screenshot_url.startswith("telegram_file:"):
            await answer_callback_query(callback_query_id, "Receipt not available!", show_alert=True)
            return
            
        await send_bot_photo(user.telegram_id, order.screenshot_url, f"🧾 <b>Receipt for Order: {order_id}</b>")
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("support_order_"):
        order_id = data.replace("support_order_", "").strip()
        session["state"] = "waiting_for_support_message"
        session["support_relation"] = f"Order: {order_id}"
        await send_bot_message(
            user.telegram_id,
            f"💬 <b>Contact Support for Order #{order_id}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Please type your message or upload a screenshot regarding Order <code>{order_id}</code>.\n\n"
            f"<i>Our support team will review your message and respond directly in this chat shortly.</i>",
            reply_markup=main_keyboard
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("download_qr_"):
        order_id = data.replace("download_qr_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order/Deposit not found!")
            return
            
        sys_upi = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        sys_name = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        upi_id = sys_upi.value if sys_upi and sys_upi.value else "dominos@upi"
        upi_name = sys_name.value if sys_name and sys_name.value else "Domino's Pizza"
        
        pay_amt = order.total_payable if not getattr(order, "upi_paid", 0.0) else order.upi_paid
        if pay_amt <= 0: pay_amt = order.total_payable

        upi_details = generate_upi_qr_details(upi_id, upi_name, pay_amt, order.id, f"Payment for {order.id}")
        qr_data_url = upi_details.get("qr_data_url", "")
        
        caption = (
            f"📥 <b>UPI Payment QR Image</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>Reference:</b> <code>{order.id}</code>\n"
            f"💵 <b>Amount:</b> <b>₹{pay_amt:.2f}</b>\n"
            f"📱 <b>UPI ID:</b> <code>{upi_id}</code>\n\n"
            f"<i>Save this image to your gallery to pay via GPay, PhonePe, or Paytm!</i>"
        )
        import base64 as _b64
        if qr_data_url and qr_data_url.startswith("data:image/png;base64,"):
            qr_bytes = _b64.b64decode(qr_data_url.split(",", 1)[1])
            await send_bot_document(user.telegram_id, qr_bytes, f"UPI_Payment_QR_{order.id}.png", caption)
        else:
            await send_bot_photo(user.telegram_id, upi_details["qr_code_url"], caption)

        # Update parent message inline keyboard to hide the Download QR Image button
        clean_markup = {
            "inline_keyboard": [
                [{"text": "✅ I Have Paid / Verify Payment", "callback_data": f"wallet_marked_paid_{order.id}"}],
                [{"text": "❌ Cancel Request", "callback_data": f"wallet_cancel_deposit_{order.id}" if order.id.startswith("TOPUP-") else f"cancel_order_{order.id}"}]
            ]
        }
        await edit_bot_message_reply_markup(user.telegram_id, message_id, clean_markup)
            
        await answer_callback_query(
            callback_query_id,
            f"📱 QR Image sent to chat!\n\nTap photo → Save to Gallery to pay via Google Pay, PhonePe, or Paytm.",
            show_alert=True
        )
        return

    elif data.startswith("admin_item_cancel_menu_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_item_cancel_menu_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        if not order.items:
            await answer_callback_query(callback_query_id, "No items in order!", show_alert=True)
            return
            
        cancel_msg = (
            f"✂️ <b>Partial Item Cancellation</b> for Order <code>{order.id}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Select an item below to cancel it from this order and refund its value to the customer's wallet balance:"
        )
        
        buttons = []
        for itm in order.items:
            i_name = itm.item_name or (itm.product.name if itm.product else "Pizza Item")
            i_cost = itm.price * itm.quantity
            buttons.append([{"text": f"❌ Cancel {i_name[:20]} (₹{i_cost:.0f})", "callback_data": f"adm_icanc_{itm.id}"}])
        buttons.append([{"text": "🔙 Back to Order Details", "callback_data": f"admin_view_order_{order.id}"}])
        
        await edit_bot_message(user.telegram_id, message_id, cancel_msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("adm_icanc_") or data.startswith("admin_item_cancel_confirm_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
            
        if data.startswith("adm_icanc_"):
            item_id = data.replace("adm_icanc_", "").strip()
            item = db.query(OrderItem).filter(OrderItem.id == item_id).first()
            order = item.order if item else None
        else:
            parts = data.replace("admin_item_cancel_confirm_", "").split("_", 1)
            order_id, item_id = (parts[0], parts[1]) if len(parts) >= 2 else (None, None)
            order = db.query(Order).filter(Order.id == order_id).first() if order_id else None
            item = db.query(OrderItem).filter(OrderItem.id == item_id).first() if item_id else None

        if not order or not item:
            await answer_callback_query(callback_query_id, "Item or Order not found!", show_alert=True)
            return
            
        item_name = item.item_name or (item.product.name if item.product else "Pizza Item")
        refund_amt = float(item.price * item.quantity)
        
        order.total_payable = max(0.0, float(order.total_payable or 0.0) - refund_amt)
        
        if order.user:
            order.user.wallet_balance = float(order.user.wallet_balance or 0.0) + refund_amt
            tx = WalletTransaction(
                user_id=order.user.id,
                type="refund",
                amount=refund_amt,
                description=f"Partial Item Cancel Refund: {item_name} (Order #{order.id})"
            )
            db.add(tx)
            
        h = OrderStatusHistory(order_id=order.id, status=order.status, note=f"Admin cancelled item {item_name} (Refunded ₹{refund_amt:.2f})")
        db.add(h)
        
        db.delete(item)
        db.commit()
        auto_save_persistent_db_state(db)
        
        cust_msg = (
            f"⚠️ <b>Item Cancelled from Order #{order.id}</b>\n\n"
            f"Item <b>{escape_html(item_name)}</b> has been cancelled by store support.\n"
            f"💸 <b>₹{refund_amt:.2f}</b> has been credited to your wallet balance."
        )
        if order.user:
            try:
                await send_bot_message(order.user.telegram_id, cust_msg)
            except Exception:
                pass
                
        await answer_callback_query(callback_query_id, f"Cancelled {item_name} & refunded ₹{refund_amt:.2f}!", show_alert=True)
        await send_admin_order_details(user.telegram_id, order.id, db, message_id)
        return
        
    elif data in ("order_confirm_place", "order_confirm_place_wallet", "order_confirm_place_direct_qr"):
        if session.get("placing_order"):
            await answer_callback_query(callback_query_id, "⚠️ Order is already processing. Please wait.")
            return
            
        # Check Pending Order Limit (Max 2 Pending Orders allowed per user)
        pending_count = db.query(Order).filter(
            Order.user_id == user.id,
            Order.status.in_(["Pending", "Pending Payment", "Pending Verification", "Placed"])
        ).count()
        if pending_count >= 2:
            await answer_callback_query(callback_query_id, "Order Limit Reached! Max 2 pending orders allowed.", show_alert=True)
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Pending Order Limit Reached!</b>\n\n"
                "You currently have <b>2 orders pending verification</b>. Please wait for your previous orders to be processed or completed before placing a new one.",
                reply_markup=main_keyboard
            )
            return
            
        session["placing_order"] = True
        
        order_id = f"BOT-{uuid.uuid4().hex[:8].upper()}"
        ref_id = f"REF-{uuid.uuid4().hex[:8].upper()}"
        txn_id = ref_id
        
        cart = session.get("cart", {})
        address = html_escape(session.get("temp_address"))
        phone = html_escape(session.get("temp_phone"))
        
        if not cart or not address or not phone:
            session["placing_order"] = False
            await answer_callback_query(callback_query_id, "Error: Session expired or invalid order details.")
            session["state"] = None
            return
            
        multiplier = 1.0
        delivery_charge = 30.0

        subtotal = 0.0
        for key_str, raw_qty in cart.items():
            qty = parse_cart_quantity(raw_qty)
            if qty <= 0:
                continue
            item_type, obj, item_name, unit_price, items_breakdown = resolve_cart_item(db, key_str)
            subtotal += (unit_price * qty)
                
        # Fetch bot service fee
        bot_fee = get_bot_fee(db)
        total_payable = subtotal + bot_fee
        discount = 0.0
        coupon = ""
        delivery_charge = bot_fee
        
        wallet_bal = user.wallet_balance or 0.0
        is_direct_qr = (data == "order_confirm_place_direct_qr")
        
        if is_direct_qr:
            wallet_usable = 0.0
            remaining_upi = total_payable
        else:
            wallet_usable = min(wallet_bal, total_payable)
            remaining_upi = total_payable - wallet_usable

        is_insufficient = (remaining_upi > 0)

        if wallet_usable > 0:
            user.wallet_balance -= wallet_usable
            tx_desc = f"💳 Paid ₹{wallet_usable:.2f} for Order #{order_id}" if remaining_upi == 0 else f"🌗 Paid ₹{wallet_usable:.2f} from wallet for Order #{order_id} (₹{remaining_upi:.2f} UPI remaining)"
            tx = WalletTransaction(
                user_id=user.id,
                type="payment",
                amount=-wallet_usable,
                description=tx_desc
            )
            db.add(tx)

        if remaining_upi <= 0:
            initial_status = "Payment Received"
            payment_method_lbl = "wallet"
        elif wallet_usable > 0:
            initial_status = "Pending Payment"
            payment_method_lbl = "partial_wallet_upi"
        else:
            initial_status = "Pending Payment"
            payment_method_lbl = "direct_upi"
        
        # Place order in DB with dynamic values
        order_note = session.get("order_note", "") or ""
        order = Order(
            id=order_id,
            user_id=user.id,
            transaction_id=txn_id,
            original_total=subtotal,
            discount=discount,
            delivery_charge=delivery_charge,
            total_payable=total_payable,
            wallet_applied=wallet_usable,
            upi_paid=remaining_upi,
            payment_method=payment_method_lbl,
            status=initial_status,
            address=address,
            phone=phone,
            coupon_applied=coupon,
            delivery_instructions=order_note if order_note else None,
            latitude=user.latitude,
            longitude=user.longitude,
            created_at=datetime.datetime.now(datetime.timezone.utc),
            updated_at=datetime.datetime.now(datetime.timezone.utc)
        )

        db.add(order)
        db.flush()
        
        # Save OrderItems to DB for both regular products and Active Offers
        for key_str, raw_qty in cart.items():
            qty = parse_cart_quantity(raw_qty)
            if qty <= 0:
                continue
            item_type, obj, item_name, unit_price, items_breakdown = resolve_cart_item(db, key_str)
            breakdown_str = "\n".join([f"└ {b}" for b in items_breakdown]) if items_breakdown else None
            p_id = obj.id if (item_type == "product" and obj) else None
            if not p_id:
                fallback_p = db.query(Product).first()
                if fallback_p:
                    p_id = fallback_p.id
            item = OrderItem(
                order_id=order_id,
                product_id=p_id,
                item_key=key_str,
                item_name=item_name,
                item_details=breakdown_str,
                quantity=qty,
                price=unit_price
            )
            db.add(item)

        user.phone = phone
        clean_addr = address or user.address or "Saved Address"
        exists_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id, SavedAddress.full_address == clean_addr).first()
        if not exists_addr:
            db.query(SavedAddress).filter(SavedAddress.user_id == user.id).update({SavedAddress.is_default: False})
            new_addr = SavedAddress(
                user_id=user.id,
                label="Last Used",
                full_address=clean_addr,
                city=user.city,
                state=user.state,
                latitude=user.latitude or 19.0760,
                longitude=user.longitude or 72.8777,
                is_default=True
            )
            db.add(new_addr)

        if is_direct_qr or is_insufficient:
            h1 = OrderStatusHistory(order_id=order_id, status="Pending Payment", note="Direct UPI QR / Pending payment at checkout")
            db.add(h1)
            db.commit()
            auto_save_persistent_db_state(db)
            
            pay_amount = total_payable if is_direct_qr else remaining_upi
            
            upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
            upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
            upi_id = upi_id_cfg.value if upi_id_cfg else "pranjalottery@fam"
            upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
            
            upi_details = generate_upi_qr_details(upi_id, upi_name, pay_amount, order_id, f"Payment for Order {order_id}")
            upi_uri = upi_details["upi_uri"]
            qr_url = upi_details["qr_code_url"]
            qr_data_url = upi_details.get("qr_data_url", "")

            breakdown_text = f"• <b>Total Payable:</b> ₹{total_payable:.2f}\n"
            if not is_direct_qr and wallet_usable > 0:
                breakdown_text += f"• <b>Wallet Applied:</b> -₹{wallet_usable:.2f}\n"
                breakdown_text += f"• <b>Amount to Pay (UPI):</b> <b>₹{pay_amount:.2f}</b>\n"
            else:
                breakdown_text += f"• <b>Amount to Pay:</b> <b>₹{pay_amount:.2f}</b>\n"

            pending_text = (
                f"🍕 <b>Order Payment Pending</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"• <b>Order ID:</b> <code>{order_id}</code>\n"
                f"{breakdown_text}\n"
                f"👉 <a href=\"{upi_uri}\"><b>📱 Click to Pay via UPI App</b></a>\n\n"
                f"<i>After transferring the amount, tap the <b>✅ I Have Paid</b> button below for verification.</i>"
            )
            base_domain = get_mini_app_url(db).rstrip('/')
            pay_link = f"{base_domain}/api/pay_upi/{order_id}"
            pending_markup = {
                "inline_keyboard": [
                    [{"text": f"⚡ Click to Pay ₹{pay_amount:.2f} via UPI App", "url": pay_link}],
                    [{"text": "✅ I Have Paid / Verify Payment", "callback_data": f"wallet_marked_paid_{order_id}"}],
                    [{"text": "❌ Cancel Order", "callback_data": f"cancel_order_{order_id}"}]
                ]
            }
            
            if qr_data_url and qr_data_url.startswith("data:image/png;base64,"):
                import base64 as _b64
                qr_bytes = _b64.b64decode(qr_data_url.split(",", 1)[1])
                await send_bot_photo_bytes(user.telegram_id, qr_bytes, "order_upi_qr.png", pending_text, reply_markup=pending_markup)
            else:
                await send_bot_photo(user.telegram_id, qr_url, pending_text, reply_markup=pending_markup)
                
            session["cart"] = {}
            session["state"] = None
            session["placing_order"] = False
            session["temp_address"] = None
            session["temp_phone"] = None
            session["order_note"] = ""
            await answer_callback_query(callback_query_id)
            return

        h1 = OrderStatusHistory(order_id=order_id, status="Payment Received")
        db.add(h1)
        db.commit()
        
        order.status = "Order Processing"
        h3 = OrderStatusHistory(order_id=order_id, status="Order Processing")
        db.add(h3)
        db.commit()
        
        # Notify admins via Telegram Bot
        item_names = []
        for key_str, qty in list(cart.items()):
            item_type, obj, item_name, unit_price, items_breakdown = resolve_cart_item(db, key_str)
            base_str = f"• {item_name} x{qty}"
            if items_breakdown:
                breakdown_str = "\n".join([f"  ↳ {b}" for b in items_breakdown])
                base_str += f"\n{breakdown_str}"
            item_names.append(base_str)
        items_summary = "\n".join(item_names)
        
        discount_text = f"₹{discount:.2f}" if discount > 0 else "None"
        
        admin_order_text = (
            "🔔 <b>New Order Placed (Manual Admin Action Required):</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 <b>Order ID:</b> <code>{order_id}</code>\n"
            f"👤 <b>User:</b> {user.display_name} (ID: <code>{user.telegram_id}</code>)\n"
            f"🛒 <b>Items:</b>\n{items_summary}\n\n"
            f"💰 <b>Total Paid:</b> ₹{total_payable:.2f} (Discount: {discount_text})\n"
            f"🏡 <b>Address:</b> <code>{address}</code>\n"
            f"📱 <b>Phone:</b> <code>{phone}</code>\n"
            f"📍 <b>GPS:</b> <code>{user.latitude or 'None'}, {user.longitude or 'None'}</code>\n"
            + (f"📝 <b>Order Note:</b> <i>{order_note}</i>\n" if order_note else "")
            + "\n👩‍🍳 <b>Actions:</b>"
        )

        
        import urllib.parse
        lat = user.latitude
        lon = user.longitude
        if lat and lon:
            maps_url = f"https://www.google.com/maps?q={lat},{lon}"
        else:
            clean_addr = urllib.parse.quote(address or "India")
            maps_url = f"https://www.google.com/maps/search/?api=1&query={clean_addr}"

        action_markup = {
            "inline_keyboard": [
                [
                    {"text": "📍 Open Google Maps Location", "url": maps_url}
                ],
                [
                    {"text": "✅ Accept & Complete", "callback_data": f"admin_act_complete_{order_id}"},
                    {"text": "❌ Reject & Refund", "callback_data": f"admin_act_reject_{order_id}"}
                ],
                [
                    {"text": "💬 Reply to Customer", "callback_data": f"admin_reply_support_{user.telegram_id}"}
                ]
            ]
        }
        await notify_admins(db, admin_order_text, reply_markup=action_markup)
        
        # Clear cart, state, AND the placing_order lock so user can place future orders
        session["cart"] = {}
        session["state"] = None
        session["placing_order"] = False
        session["temp_address"] = None
        session["temp_phone"] = None
        session["order_note"] = ""

        
        # Build item list for the confirmation message
        item_lines = []
        for key_str, qty in list(cart.items()):
            item_type, obj, item_name, unit_price, items_breakdown = resolve_cart_item(db, key_str)
            item_lines.append(f"  • {item_name} x{qty} — ₹{unit_price * qty:.0f}")
        items_text = "\n".join(item_lines) if item_lines else "  • (items unavailable)"

        success_text = (
            f"✅ <b>Payment Confirmed!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 Order ID: <code>{order_id}</code>\n\n"
            f"🛒 <b>Items Ordered:</b>\n{items_text}\n\n"
            f"💰 <b>Price Breakdown:</b>\n"
            f"  Subtotal:       ₹{subtotal:.2f}\n"
            f"  Discount ({coupon}): -₹{discount:.2f}\n"
            f"  Delivery:       +₹{delivery_charge:.2f}\n"
            f"  ─────────────────\n"
            f"  <b>Total Paid:    ₹{total_payable:.2f}</b>\n\n"
            f"👨‍🍳 <b>Status:</b> Order Processing\n"
            f"Your pizza is being submitted to Domino's now!\n"
            f"Estimated delivery in <b>~30 minutes</b>."
        )
        
        await answer_callback_query(callback_query_id, "Order placed!")
        await edit_bot_message(
            user.telegram_id,
            message_id,
            success_text,
            reply_markup={
                "inline_keyboard": [
                    [{"text": "🔄 Track Live Status", "callback_data": f"track_refresh_{order_id}"}],
                    [{"text": "📞 Contact Support", "callback_data": "support_menu"}]
                ]
            }
        )
        
        if sse_broadcast_callback:
            await sse_broadcast_callback({
                "type": "new_order",
                "order_id": order_id,
                "total": total_payable,
                "subtotal": subtotal,
                "discount": discount,
                "delivery_charge": delivery_charge,
                "user": user.display_name,
                "user_id": user.id,
                "items_count": len(item_lines)
            })
            await sse_broadcast_callback({"type": "order_update"})
        
        # Launch Domino's browser ordering in background
        async def run_dominos_in_background(oid: str):
            from .database import SessionLocal as _SL
            from .services.dominos_service import submit_dominos_order as _submit
            from .database import OrderStatusHistory as _OSH, WalletTransaction as _WT
            bg_db = _SL()
            tg_id = None
            try:
                bg_order = bg_db.query(Order).filter(Order.id == oid).first()
                if bg_order and bg_order.user:
                    tg_id = bg_order.user.telegram_id
                
                if bg_order:
                    try:
                        result = await _submit(bg_order, bg_db)
                        if result and result.get("success"):
                            bg_order.status = "Preparing"
                            bg_db.add(_OSH(order_id=oid, status="Preparing", note="Domino's order placed successfully"))
                            bg_db.commit()
                            if sse_broadcast_callback:
                                await sse_broadcast_callback({"type": "order_update", "order_id": oid, "status": "Preparing"})
                            if tg_id:
                                await send_bot_message(
                                    tg_id,
                                    f"🍕 <b>Great news!</b> Your order <code>{oid}</code> has been successfully placed on Domino's!\n"
                                    f"Ref: <code>{result.get('reference', 'N/A')}</code>\n"
                                    f"The kitchen is now preparing your pizza! 👨‍🍳"
                                )
                        else:
                            error_msg = result.get("error") if result else "Unknown error"
                            raise Exception(error_msg)
                    except Exception as order_exc:
                        # Order placement failed - refund & update status to Failed
                        error_msg = str(order_exc)
                        bg_order.status = "Failed"
                        bg_db.add(_OSH(order_id=oid, status="Failed", note=error_msg))
                        
                        user = bg_order.user
                        if user:
                            user.wallet_balance += bg_order.total_payable
                            tx = _WT(
                                user_id=user.id,
                                type="refund",
                                amount=bg_order.total_payable,
                                description=f"Refund for failed order: {bg_order.id}"
                            )
                            bg_db.add(tx)
                            
                        bg_db.commit()
                        
                        if sse_broadcast_callback:
                            await sse_broadcast_callback({"type": "order_update", "order_id": oid, "status": "Failed"})
                            
                        if tg_id:
                            # Format a nice, clear error notification with action buttons
                            error_text = (
                                f"⚠️ <b>We encountered an issue placing your order on Domino's.</b>\n\n"
                                f"• <b>Order ID:</b> <code>{oid}</code>\n"
                                f"• <b>Reason:</b> <code>{error_msg}</code>\n"
                                f"• <b>Wallet Refund:</b> +₹{bg_order.total_payable:.2f} (Refunded to your balance)\n\n"
                                f"💡 <i>You can modify your address or try again using the buttons below.</i>"
                            )
                            await send_bot_message(
                                tg_id,
                                error_text,
                                reply_markup={
                                    "inline_keyboard": [
                                        [{"text": "📍 Update Address / Phone", "callback_data": "checkout_enter_new"}],
                                        [{"text": "🛒 View Cart / Checkout", "callback_data": "cart_view"}],
                                        [{"text": "📞 Contact Support", "callback_data": "support_menu"}]
                                    ]
                                }
                            )
            except Exception as e:
                import traceback as _tb
                from .database import ErrorLog as _EL
                err = _EL(
                    type="integration",
                    message=f"Background Dominos task failed for {oid}: {e}",
                    stack_trace=_tb.format_exc()
                )
                try:
                    bg_db.add(err)
                    bg_db.commit()
                except Exception:
                    pass
            finally:
                bg_db.close()
        
        asyncio.create_task(run_dominos_in_background(order_id))
        
    elif data == "order_cancel_place":
        session["state"] = None
        session["temp_address"] = None
        session["temp_phone"] = None
        await answer_callback_query(callback_query_id, "Order cancelled!")
        await edit_bot_message(
            user.telegram_id,
            message_id,
            "❌ <b>Order Cancelled.</b> Your shopping cart is still intact. You can view it or continue browsing the menu.",
            reply_markup={
                "inline_keyboard": [
                    [{"text": "🛒 View Shopping Cart", "callback_data": "cart_view"}],
                    [{"text": "🍕 View Menu", "callback_data": "menu_view"}]
                ]
            }
        )
        return

    else:
        # Fallback for unmatched inline callback data: answer callback query immediately to stop Telegram loading spinner
        try:
            await answer_callback_query(callback_query_id)
        except Exception:
            pass
        return

async def process_bot_callback_task(telegram_id: str, first_name: str, last_name: str, username: str, data: str, message_id: int, callback_query_id: str):
    """Processes callback query button clicks in a concurrent background task."""

    if check_rate_limit(telegram_id, is_callback=True):
        return

    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    user_key = str(telegram_id)
    
    # 0.01s callback cooldown to allow responsive fast multi-clicks (+/- cart items)
    last_time = USER_LAST_CB_TIME.get(user_key, 0)
    if now - last_time < 0.01:
        return
    USER_LAST_CB_TIME[user_key] = now

    if user_key not in USER_PROCESSING_LOCKS:
        USER_PROCESSING_LOCKS[user_key] = asyncio.Lock()
        
    async with USER_PROCESSING_LOCKS[user_key]:
        db = SessionLocal()
        try:
            user_check = db.query(User).filter(User.telegram_id == str(telegram_id)).first()
            if user_check and user_check.is_blocked and not (data.startswith("menu_support") or data.startswith("support_") or data.startswith("faq_") or data.startswith("admin_")):
                await answer_callback_query(callback_query_id, "❌ Your account is suspended. Please contact support.", show_alert=True)
                await send_bot_message(
                    telegram_id,
                    "❌ <b>Account Suspended</b>\n\nYour account is currently suspended by administration. Please contact support if you believe this is an error.",
                    reply_markup={"inline_keyboard": [[{"text": "📞 Contact Support", "callback_data": "menu_support"}]]}
                )
                return

            await handle_bot_callback(db, telegram_id, first_name, last_name, username, data, message_id, callback_query_id)
            user = db.query(User).filter(User.telegram_id == str(telegram_id)).first()
            if user and str(telegram_id) in USER_BOT_SESSION:
                import json
                session = USER_BOT_SESSION[str(telegram_id)]
                user.bot_state = session.get("state")
                dump_data = {
                    "cart": session.get("cart", {}),
                    "temp_address": session.get("temp_address"),
                    "temp_phone": session.get("temp_phone")
                }
                user.bot_cart = json.dumps(dump_data)
                db.commit()
                
            # Acknowledge callback at the end to stop spinner (fails silently if already answered)
            if callback_query_id:
                try:
                    await answer_callback_query(callback_query_id)
                except Exception:
                    pass
        except Exception as e:
            tb = traceback.format_exc()
            # 1. Always print to terminal so it shows in uvicorn log
            logger.error(f"\n[BOT CALLBACK ERROR] user={telegram_id} ({first_name}) data={data}\n{tb}")
            # 2. Save to DB ErrorLog
            try:
                err = ErrorLog(
                     type="bot_callback",
                     message=f"Callback error for {telegram_id} ({first_name}) data={data}: {str(e)}",
                     stack_trace=tb
                )
                db.add(err)
                db.commit()
            except Exception:
                db.rollback()
            # 3. Broadcast to admin SSE Live Feed & Telegram Admin Alert
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({
                        "type": "error_alert",
                        "message": f"[Bot Callback] {first_name} ({telegram_id}) / {data}: {str(e)}",
                    })
                except Exception:
                    pass
            try:
                admin_alert_text = (
                    "⚠️ <b>Bot Exception Alert</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"<b>Type:</b> Callback Query Error\n"
                    f"<b>User:</b> {first_name} (ID: <code>{telegram_id}</code>)\n"
                    f"<b>Data:</b> <code>{data}</code>\n"
                    f"<b>Error:</b> <code>{html_escape(str(e))}</code>\n\n"
                    "<i>Log saved to DB ErrorLog.</i>"
                )
                asyncio.create_task(notify_admins(db, admin_alert_text))
            except Exception:
                pass
            # Always answer the callback so the button doesn't freeze
            try:
                await answer_callback_query(callback_query_id, "Action completed. Please refresh or try again if needed.")
            except Exception:
                pass
        finally:
            db.close()

async def process_incoming_message_task(telegram_id: str, first_name: str, last_name: str, username: str, text: str, location: dict = None, message_id: int = None, photo: list = None, document: dict = None):
    """Processes an incoming message in a non-blocking background task with a clean DB session."""
    import time
    now_ts = time.time()
    user_key = str(telegram_id)
    
    if check_rate_limit(telegram_id, is_callback=False):
        return

    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    
    # 0.01s message cooldown to allow smooth keyboard navigation
    last_time = USER_LAST_MSG_TIME.get(user_key, 0)
    if now - last_time < 0.01:
        return
    USER_LAST_MSG_TIME[user_key] = now

    if user_key not in USER_PROCESSING_LOCKS:
        USER_PROCESSING_LOCKS[user_key] = asyncio.Lock()
        
    async with USER_PROCESSING_LOCKS[user_key]:
        db = SessionLocal()
        try:
            user_check = db.query(User).filter(User.telegram_id == str(telegram_id)).first()
            session = USER_BOT_SESSION.get(str(telegram_id), {})
            if user_check and user_check.is_blocked and session.get("state") != "waiting_for_support_message":
                await send_bot_message(
                    telegram_id,
                    "❌ <b>Account Suspended</b>\n\nYour account is currently suspended by administration. Please contact support if you believe this is an error.",
                    reply_markup={"inline_keyboard": [[{"text": "📞 Contact Support", "callback_data": "menu_support"}]]}
                )
                return

            await handle_bot_message(db, telegram_id, first_name, last_name, username, text, location, message_id, photo=photo, document=document)
            user = db.query(User).filter(User.telegram_id == str(telegram_id)).first()
            if user and str(telegram_id) in USER_BOT_SESSION:
                import json
                session = USER_BOT_SESSION[str(telegram_id)]
                user.bot_state = session.get("state")
                user.bot_cart = json.dumps(session.get("cart", {}))
                db.commit()
        except Exception as e:
            tb = traceback.format_exc()
            # 1. Always print to terminal so it shows in uvicorn log
            logger.error(f"\n[BOT MESSAGE ERROR] user={telegram_id} ({first_name}) text={repr(text)} loc={location is not None}\n{tb}")
            # 2. Save to DB ErrorLog
            try:
                err = ErrorLog(
                    type="bot_message",
                    message=f"Message error for {telegram_id} ({first_name}) text={repr(text[:100])}: {str(e)}",
                    stack_trace=tb
                )
                db.add(err)
                db.commit()
            except Exception:
                db.rollback()
            # 3. Broadcast to admin SSE Live Feed & Telegram Admin Alert
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({
                        "type": "error_alert",
                        "message": f"[Bot Message] {first_name} ({telegram_id}) / {repr(text[:60])}: {str(e)}",
                    })
                except Exception:
                    pass
            try:
                admin_alert_text = (
                    "⚠️ <b>Bot Exception Alert</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"<b>Type:</b> Message Handler Error\n"
                    f"<b>User:</b> {first_name} (ID: <code>{telegram_id}</code>)\n"
                    f"<b>Text:</b> <code>{html_escape(repr(text[:100]))}</code>\n"
                    f"<b>Error:</b> <code>{html_escape(str(e))}</code>\n\n"
                    "<i>Log saved to DB ErrorLog.</i>"
                )
                asyncio.create_task(notify_admins(db, admin_alert_text))
            except Exception:
                pass
        finally:
            db.close()

async def run_bot_polling():
    """
    Background polling loop or Webhook manager for Telegram Bot.
    """
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.info("Telegram Bot Token is missing or MOCK_TOKEN. Running in MOCK Mode (notifications logged to terminal).")
        while True:
            await asyncio.sleep(3600) # Sleep indefinitely in mock mode
            
    webhook_url = os.getenv("TELEGRAM_WEBHOOK_URL")
    if webhook_url:
        logger.info(f"Setting Telegram Bot Webhook to: {webhook_url}")
        try:
            # Set webhook on Telegram API
            setup_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
            setup_payload = {
                "url": webhook_url,
                "allowed_updates": ["message", "edited_message", "callback_query"],
                "drop_pending_updates": True
            }
            resp = await _http_client.post(setup_url, json=setup_payload, timeout=10.0)
            if resp.status_code == 200:
                logger.info(f"Successfully registered Telegram webhook: {resp.text}")
            else:
                logger.error(f"Failed to register webhook: Code {resp.status_code}, Response: {resp.text}")
        except Exception as e:
            logger.error(f"Error registering Telegram Webhook on startup: {e}")
            
        while True:
            await asyncio.sleep(3600)
            
    logger.info("Starting Telegram Bot Polling Loop...")
    
    # Delete any active webhooks and drop pending updates to prevent conflicts
    try:
        await _http_client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", json={"drop_pending_updates": True}, timeout=10.0)
        logger.info("Successfully cleared active webhooks and dropped pending updates.")
    except Exception as e:
        logger.error(f"Warning: Failed to clear webhook: {e}")

    offset = 0
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    
    while True:
        try:
            resp = await _http_client.post(
                url, 
                json={"offset": offset, "timeout": 30, "allowed_updates": ["message", "edited_message", "callback_query"]},
                timeout=35.0
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("ok") and data.get("result"):
                    for update in data["result"]:
                        offset = update["update_id"] + 1
                        message = update.get("message") or update.get("edited_message")
                        callback_query = update.get("callback_query")
                        
                        if message:
                            if "text" not in message and "location" not in message and "photo" not in message and "document" not in message:
                                continue
                            chat = message.get("chat", {})
                            if chat.get("type") != "private":
                                continue
                            from_user = message.get("from", {})
                            telegram_id = from_user.get("id")
                            first_name = from_user.get("first_name", "")
                            last_name = from_user.get("last_name", "")
                            username = from_user.get("username", "")
                            text = message.get("text", "").strip() if "text" in message else message.get("caption", "").strip()
                            location = message.get("location")
                            photo = message.get("photo")
                            document = message.get("document")
                            
                            logger.debug(f"[BOT TRACE] Received message/location from {first_name} [ID: {telegram_id}]")
                            asyncio.create_task(process_incoming_message_task(telegram_id, first_name, last_name, username, text, location, message.get("message_id"), photo=photo, document=document))
                            
                        elif callback_query:
                            from_user = callback_query.get("from", {})
                            telegram_id = from_user.get("id")
                            first_name = from_user.get("first_name", "")
                            last_name = from_user.get("last_name", "")
                            username = from_user.get("username", "")
                            cb_data = callback_query.get("data", "")
                            cb_message = callback_query.get("message", {})
                            message_id = cb_message.get("message_id")
                            callback_query_id = callback_query.get("id")
                            
                            logger.debug(f"[BOT CB TRACE] Received callback from {first_name} [ID: {telegram_id}]: {cb_data}")
                            user_key = str(telegram_id)
                            task = asyncio.create_task(process_bot_callback_task(telegram_id, first_name, last_name, username, cb_data, message_id, callback_query_id))
                            USER_CALLBACK_TASKS[user_key] = task
            elif resp.status_code == 409:
                # Another instance/old container is shutting down — clear webhook & wait 5s to acquire slot
                try:
                    await _http_client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", json={"drop_pending_updates": True}, timeout=5.0)
                except Exception:
                    pass
                await asyncio.sleep(5)
            elif resp.status_code == 429:
                # Rate limited — respect Retry-After header
                retry_after = int(resp.headers.get("Retry-After", "10"))
                logger.warning(f"[BOT] 429 Too Many Requests. Retrying after {retry_after}s...")
                await asyncio.sleep(retry_after)
            else:
                logger.warning(f"[BOT] Unexpected status from getUpdates: {resp.status_code}")
                await asyncio.sleep(5)
        except httpx.RequestError as e:
            # Silently wait on connection issues
            await asyncio.sleep(5)
        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"\n[BOT POLLING ERROR] {str(e)}\n{tb}")
            db = SessionLocal()
            err = ErrorLog(
                type="bot_polling",
                message=f"Bot polling crash: {str(e)}",
                stack_trace=tb
            )
            db.add(err)
            db.commit()
            db.close()
            # Broadcast to SSE
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({
                        "type": "error_alert",
                        "message": f"[Bot Polling Crash] {str(e)}",
                    })
                except Exception:
                    pass
            await asyncio.sleep(10)

async def handle_incoming_update(update: dict):
    """
    Processes an incoming Telegram Update dictionary (received via Webhook).
    """
    message = update.get("message") or update.get("edited_message")
    callback_query = update.get("callback_query")
    
    if message:
        if "text" not in message and "location" not in message and "photo" not in message and "document" not in message:
            return
        chat = message.get("chat", {})
        if chat.get("type") != "private":
            return
        from_user = message.get("from", {})
        telegram_id = from_user.get("id")
        first_name = from_user.get("first_name", "")
        last_name = from_user.get("last_name", "")
        username = from_user.get("username", "")
        text = message.get("text", "").strip() if "text" in message else message.get("caption", "").strip()
        location = message.get("location")
        photo = message.get("photo")
        document = message.get("document")
        
        logger.debug(f"[BOT WEBHOOK] Received message/location from {first_name} [ID: {telegram_id}]")
        asyncio.create_task(process_incoming_message_task(telegram_id, first_name, last_name, username, text, location, message.get("message_id"), photo=photo, document=document))
        
    elif callback_query:
        from_user = callback_query.get("from", {})
        telegram_id = from_user.get("id")
        first_name = from_user.get("first_name", "")
        last_name = from_user.get("last_name", "")
        username = from_user.get("username", "")
        cb_data = callback_query.get("data", "")
        cb_message = callback_query.get("message", {})
        message_id = cb_message.get("message_id")
        callback_query_id = callback_query.get("id")
        
        logger.debug(f"[BOT WEBHOOK CB] Received callback from {first_name} [ID: {telegram_id}]: {cb_data}")
        user_key = str(telegram_id)
        prev_task = USER_CALLBACK_TASKS.get(user_key)
        if prev_task and not prev_task.done():
            prev_task.cancel()
        task = asyncio.create_task(process_bot_callback_task(telegram_id, first_name, last_name, username, cb_data, message_id, callback_query_id))
        USER_CALLBACK_TASKS[user_key] = task
