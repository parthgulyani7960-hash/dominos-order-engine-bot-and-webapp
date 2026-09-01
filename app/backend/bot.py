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
from .database import SessionLocal, User, SupportMessage, ErrorLog, SystemConfig, Product, Order, OrderItem, OrderStatusHistory, GiftCard, AuditLog, LocationPricing, Notification, UTRAttempt, SavedAddress, Coupon, CouponRedemption, WalletTransaction, WithdrawalRequest, OrderNote, RiderAssignment, Proxy, ProxyLog, DominosSession
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

def check_rate_limit(telegram_id: str, is_callback: bool = False) -> bool:
    """Returns True if rate limit is exceeded, False otherwise.
    
    Rule: Max 5 requests in a rolling 5-second window.
    """
    import time
    now = time.time()
    user_key = str(telegram_id)
    timestamps_dict = USER_CB_TIMESTAMPS if is_callback else USER_MSG_TIMESTAMPS
    
    if user_key not in timestamps_dict:
        timestamps_dict[user_key] = []
        
    # Filter out timestamps older than 5 seconds
    timestamps_dict[user_key] = [t for t in timestamps_dict[user_key] if now - t < 5.0]
    
    if len(timestamps_dict[user_key]) >= 5:
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
MINI_APP_URL = os.getenv("MINI_APP_URL", "http://localhost:8000")

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
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def send_bot_typing(telegram_id: str):
    """Sends a 'typing...' status to Telegram — makes bot feel human while processing."""
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendChatAction"
        await _fast_client.post(url, json={"chat_id": str(telegram_id), "action": "typing"}, timeout=2.0)
    except Exception:
        pass


async def edit_bot_message(telegram_id: str, message_id: int, text: str, reply_markup: dict = None) -> bool:
    """Edits an existing text message on the user's screen (in-place text updates).
    Falls back to editMessageCaption if the target message contains media (photo, animation).
    """
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT EDIT] Chat: {telegram_id}, Msg: {message_id}, Text: {text}")
        return True
        
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
        cap_payload = {
            "chat_id": telegram_id,
            "message_id": message_id,
            "caption": text,
            "parse_mode": "HTML"
        }
        if reply_markup:
            cap_payload["reply_markup"] = reply_markup
            
        cap_resp = await _fast_client.post(cap_url, json=cap_payload)
        return cap_resp.status_code == 200
    except Exception:
        return False


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


async def answer_callback_query(callback_query_id: str, text: str = None, show_alert: bool = False, url: str = None) -> bool:
    """Dismisses the loading spinner icon on the Telegram client button. If show_alert=True, shows a popup alert instead of a toast."""
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
        await _fast_client.post(tg_url, json=payload)
        return True
    except Exception:
        return False

async def send_bot_message(telegram_id: str, text: str, reply_markup: dict = None) -> bool:
    """
    Sends a direct message to a Telegram user.
    Returns True if successful, False otherwise.
    Splits long messages (> 4000 characters) into smaller chunks safely.
    """
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
            err = ErrorLog(
                type="notification",
                message=f"Failed to send bot message to {telegram_id}. Code: {resp.status_code}, Body: {resp.text}"
            )
            db.add(err)
            db.commit()
            db.close()
            return False
    except Exception as e:
        db = SessionLocal()
        err = ErrorLog(
            type="notification",
            message=f"Error sending bot message: {str(e)}",
            stack_trace=traceback.format_exc()
        )
        db.add(err)
        db.commit()
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
    expiry_text = target_user.admin_expires_at.strftime("%Y-%m-%d %H:%M UTC") if target_user.admin_expires_at else "Permanent / N/A"
    
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
        txs_lines.append(f"  • {t.type.upper()}: {t_sign}₹{t.amount:.2f} ({t.created_at.strftime('%Y-%m-%d')})")
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
            {"text": "📜 Wallet Logs", "callback_data": f"admin_user_txs_page_{target_user.id}_1"},
            {"text": "📦 Order History", "callback_data": f"admin_user_orders_page_{target_user.id}_1"}
        ],
        [
            {"text": "📍 Saved Addresses", "callback_data": f"admin_user_addresses_{target_user.id}"}
        ],
        [
            {"text": "🔙 Back to Users List", "callback_data": "admin_manage_users"}
        ]
    ]
    
    if message_id:
        await edit_bot_message(telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
    else:
        await send_bot_message(telegram_id, msg, reply_markup={"inline_keyboard": buttons})


async def send_admin_order_details(telegram_id: str, order_id: str, db: Session, message_id: int = None):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        await send_bot_message(telegram_id, "❌ Order not found!")
        return
        
    rider_name = order.rider.rider_name if order.rider else "None"
    rider_phone = order.rider.rider_phone if order.rider else "None"
    
    rider_phone_link = f"<a href='tel:{rider_phone}'>{rider_phone}</a>" if (rider_phone and rider_phone != "None") else "None"
    customer_phone_link = f"<a href='tel:{order.phone}'>{order.phone}</a>" if order.phone else "None"
    
    screenshot_disp = "—"
    if order.screenshot_url:
        screenshot_disp = "🖼️ Attached"
    
    detail_msg = (
        f"🛒 <b>Order Editor: {order.id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"• <b>Status:</b> <code>{order.status}</code>\n"
        f"• <b>User:</b> {order.user.display_name} (ID: <code>{order.user.telegram_id}</code>)\n"
        f"• <b>Customer Phone:</b> {customer_phone_link}\n"
        f"• <b>Total Payable:</b> ₹{order.total_payable:.2f} ({order.payment_method.upper()})\n"
        f"• <b>Domino's Ref:</b> <code>{order.dominos_reference or 'None'}</code>\n"
        f"• <b>Sector Store:</b> <code>{order.sector_store or 'None'}</code>\n"
        f"• <b>Rider Name:</b> <code>{rider_name}</code>\n"
        f"• <b>Rider Phone:</b> {rider_phone_link}\n"
        f"• <b>Screenshot/Receipt:</b> {screenshot_disp}\n"
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
            {"text": "🖼️ Attach Screenshot", "callback_data": f"admin_order_attach_sc_{order.id}"},
            {"text": "🔄 Change Status", "callback_data": f"admin_change_status_menu_{order.id}"}
        ],
        [
            {"text": "🔙 Back to Orders List", "callback_data": "admin_manage_orders_menu"}
        ]
    ]
    
    if order.screenshot_url:
        buttons.insert(3, [
            {"text": "👁️ View Screenshot", "callback_data": f"admin_order_view_sc_{order.id}"},
            {"text": "🗑️ Delete Screenshot", "callback_data": f"admin_order_del_sc_{order.id}"}
        ])
        
    if message_id:
        await edit_bot_message(telegram_id, message_id, detail_msg, reply_markup={"inline_keyboard": buttons})
    else:
        await send_bot_message(telegram_id, detail_msg, reply_markup={"inline_keyboard": buttons})


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
    """Renders the My Wallet overview and recent transaction history combining WalletTransactions and Orders."""
    import re
    txs = db.query(WalletTransaction).filter(WalletTransaction.user_id == user.id).order_by(WalletTransaction.created_at.desc()).all()
    orders = db.query(Order).filter(Order.user_id == user.id).order_by(Order.created_at.desc()).all()
    
    tx_lines = []
    # Combine WalletTransaction entries showing ONLY requested simplified copy-paste trace details
    for t in txs:
        sign = "+" if t.amount >= 0 else ""
        t_type = t.type.lower()
        
        # Extract order ID if present in the description
        order_match = re.search(r'(BOT-[A-Z0-9]+|TOPUP-[A-Z0-9]+)', t.description or "")
        order_info = f" | Order: <code>{order_match.group(1)}</code>" if order_match else ""
        
        tx_lines.append(
            f"💰 <b>{sign}₹{abs(t.amount):.2f}</b> — {t_type.upper()}\n"
            f"└ Tx ID: <code>{t.id[:8].upper()}</code>{order_info}"
        )

    # Combine Top-up orders if completed or verified
    for o in orders:
        if o.id.startswith("TOPUP-") and o.status in ["Completed", "Verified"]:
            tx_lines.append(
                f"💰 <b>+₹{o.total_payable:.2f}</b> — DEPOSIT\n"
                f"└ Order: <code>{o.id}</code>"
            )

    total_count = len(tx_lines)
    history_text = "\n\n".join(tx_lines[offset:offset+limit]) if tx_lines else "<i>No wallet transactions recorded yet.</i>"
    
    wallet_text = (
        f"💰 <b>My Wallet Overview</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 Account: <b>{user.display_name}</b>\n"
        f"💵 Available Balance: <b>₹{user.wallet_balance:.2f}</b>\n\n"
        f"📜 <b>Recent Transaction History:</b>\n\n"
        f"{history_text}\n\n"
        f"💡 <i>Select an option below to top-up balance or redeem a promo code.</i>"
    )

    inline_buttons = [
        [
            {"text": "💳 Add Funds", "callback_data": "wallet_add"},
            {"text": "🎫 Add Promo Code", "callback_data": "wallet_promo"}
        ]
    ]

    nav_row = []
    if offset > 0:
        nav_row.append({"text": "⬅️ Prev", "callback_data": f"wallet_tx_more_{max(0, offset - limit)}"})
    if offset + limit < total_count:
        nav_row.append({"text": "Next ➡️", "callback_data": f"wallet_tx_more_{offset + limit}"})
    if nav_row:
        inline_buttons.append(nav_row)

    inline_buttons.append([{"text": "🛒 Back to Cart", "callback_data": "cart_view"}])
    inline_buttons.append([{"text": "🍕 View Menu & Order", "callback_data": "menu_view"}])

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
            db = SessionLocal()
            err = ErrorLog(
                type="notification",
                message=f"Failed to send bot photo to {telegram_id}. Code: {resp.status_code}, Body: {resp.text}"
            )
            db.add(err)
            db.commit()
            db.close()
            
            # Fallback to plain text message
            logger.error(f"[BOT] Photo failed (Code {resp.status_code}). Falling back to text...")
            fallback_text = caption or "Domino's Order Engine Photo"
            if photo_url and photo_url.startswith("http"):
                fallback_text = f"{fallback_text}\n\n🔗 Image Link: {photo_url}"
            return await send_bot_message(telegram_id, fallback_text, reply_markup)
    except Exception as e:
        db = SessionLocal()
        err = ErrorLog(
            type="notification",
            message=f"Error sending bot photo: {str(e)}",
            stack_trace=traceback.format_exc()
        )
        db.add(err)
        db.commit()
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
    """
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT ANIMATION] To {telegram_id}: Animation: {animation_url}, Caption: {caption}")
        return True
        
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendAnimation"
    payload = {
        "chat_id": telegram_id,
        "animation": animation_url
    }
    if caption:
        payload["caption"] = caption
        payload["parse_mode"] = "HTML"
    import json
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
        
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
                resp = await _http_client.post(url, data={"chat_id": telegram_id, "caption": caption, "parse_mode": "HTML"}, files={"animation": f}, timeout=25.0)
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
    if _MINI_APP_URL_CACHE is None or now - _MINI_APP_URL_LAST_UPDATE > 10.0:
        close_db = False
        if db is None:
            db = SessionLocal()
            close_db = True
        try:
            cfg = db.query(SystemConfig).filter(SystemConfig.key == "mini_app_url").first()
            if cfg and cfg.value:
                val = cfg.value.strip()
                if "testserver" not in val.lower():
                    _MINI_APP_URL_CACHE = val
                _MINI_APP_URL_LAST_UPDATE = now
        except Exception:
            pass
        finally:
            if close_db:
                db.close()
    return _MINI_APP_URL_CACHE if _MINI_APP_URL_CACHE is not None else os.getenv("MINI_APP_URL", "http://localhost:8000")

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
    coord_line = ""
    if user.latitude is not None and user.longitude is not None:
        coord_line = f"\n📡 Coords: <code>{user.latitude:.4f}, {user.longitude:.4f}</code>"
    else:
        coord_line = f"\n📡 Coords: <i>No coordinates saved by you</i>"
        
    saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
    if saved_addr and saved_addr.full_address and saved_addr.full_address != "GPS Location":
        addr_line = f"\n🏡 Address: <code>{saved_addr.full_address}</code>"
    else:
        addr_line = f"\n🏡 Address: <i>No delivery address saved by you</i>"
        
    if user.phone:
        phone_line = f"\n📱 Phone: <code>{user.phone}</code>"
    else:
        phone_line = f"\n📱 Phone: <i>No phone number saved by you</i>"

    loc_msg = (
        f"📍 <b>Delivery Location & Details</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Current city: <b>{user.city or '—'}</b>"
        f"{coord_line}"
        f"{addr_line}"
        f"{phone_line}\n\n"
        "Choose an option below:"
    )
    
    loc_options_keyboard = {
        "keyboard": [
            [{"text": "📍 Share My GPS Location", "request_location": True}],
            [{"text": "🏠 Update Delivery Address"}],
            [{"text": "📱 Update Phone Number"}],
            [{"text": "🔙 Back"}]
        ],
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
    # Determine pricing multiplier and delivery charge based on user's location
    multiplier = 1.0
    delivery_charge = 30.0
    loc = None
    if user.city:
        loc = db.query(LocationPricing).filter(LocationPricing.city.ilike(user.city)).first()
    if loc:
        multiplier = loc.price_multiplier
        delivery_charge = loc.delivery_charge

    # Fetch all active products
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
            f"Try a different category or tap <b>⭐ All</b> to see everything."
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

    # Generate mappings to get 1-based display codes
    code_to_id, id_to_code = get_product_mappings(db)

    # Pagination: 5 items per page
    items_per_page = 5
    total_pages = (len(products) + items_per_page - 1) // items_per_page
    page = max(1, min(page, total_pages))

    start_idx = (page - 1) * items_per_page
    page_products = products[start_idx:start_idx + items_per_page]

    # Category emoji map
    category_emoji = {
        "veg": "🟢", "non-veg": "🔴", "sides": "🍟",
        "drinks": "🥤", "desserts": "🍰", "all": "🍕"
    }
    cat_icon = category_emoji.get(category.lower(), "🍽️")

    # Compose header
    city_display = f"📍 <b>{user.city}</b>" if user.city else "📍 <i>Location not set</i>"
    price_note = f" · {multiplier:.1f}x pricing" if multiplier != 1.0 else ""
    
    menu_lines = [
        f"🍕 <b>Domino's Menu</b>  —  {cat_icon} <b>{category}</b>",
        f"{city_display}{price_note}",
        f"📄 Page {page} of {total_pages}  ·  {len(products)} items",
        "━━━━━━━━━━━━━━━━━━━━━━\n"
    ]

    for p in page_products:
        original = float(round(p.original_price * multiplier))
        if p.discounted_price is not None:
            effective = float(round(p.discounted_price * multiplier))
        else:
            effective = original

        veg_dot = "🟢" if p.is_veg else "🔴"
        display_code = id_to_code.get(p.id, "—")
        
        # Price display: show strikethrough original if discounted
        if p.discounted_price is not None and p.discounted_price < p.original_price:
            price_str = f"<s>₹{original:.0f}</s>  <b>₹{effective:.0f}</b>"
            savings = original - effective
            price_str += f"  <i>(Save ₹{savings:.0f}!)</i>"
        else:
            price_str = f"<b>₹{effective:.0f}</b>"

        badges = []
        if p.is_popular:
            badges.append("🔥 Popular")
        if p.is_recommended:
            badges.append("⭐ Chef's Pick")
        badge_str = "  " + "  ".join(badges) if badges else ""

        menu_lines.append(
            f"{veg_dot} <b>[{display_code}] {p.name}</b>{badge_str}\n"
            f"     📁 {p.category}  ·  {price_str}\n"
            f"     <i>{(p.description or 'Freshly made at your nearest Domino\'s store.')[:90]}</i>"
        )

    # Delivery info footer
    menu_lines.append(
        f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🚚 Delivery via Domino's  ·  🤖 Bot fee applies"
    )

    menu_text = "\n\n".join(menu_lines)

    # Build inline keyboard: add-to-cart buttons in 2-column layout
    grid = []
    row = []
    for p in page_products:
        original = float(round(p.original_price * multiplier))
        effective = float(round(p.discounted_price * multiplier)) if p.discounted_price is not None else original
        name_limit = p.name[:16] + "…" if len(p.name) > 18 else p.name
        veg_icon = "🟢" if p.is_veg else "🔴"
        row.append({"text": f"➕ {veg_icon} {name_limit} ₹{effective:.0f}", "callback_data": f"cart_add_{p.id}"})
        if len(row) == 2:
            grid.append(row)
            row = []
    if row:
        grid.append(row)

    # Pagination row
    nav_row = []
    if page > 1:
        nav_row.append({"text": "⬅️ Prev", "callback_data": f"menu_page_{page-1}_{category}"})
    nav_row.append({"text": f"📄 {page}/{total_pages}", "callback_data": "menu_page_noop"})
    if page < total_pages:
        nav_row.append({"text": "Next ➡️", "callback_data": f"menu_page_{page+1}_{category}"})
    grid.append(nav_row)

    # Category filter row
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
    grid.append([{"text": "🛒 View Shopping Cart", "callback_data": "cart_view"}])

    markup = {"inline_keyboard": grid}

    if edit_message_id:
        edited = await edit_bot_message(user.telegram_id, edit_message_id, menu_text, reply_markup=markup)
        if not edited:
            # If edit fails (content unchanged / message too old), send fresh
            await send_bot_message(user.telegram_id, menu_text, reply_markup=markup)
    else:
        # Send animated intro GIF only on first open (not re-opens/edits)
        gif_url = "https://media.giphy.com/media/10kxE34bJPaDPy/giphy.gif"
        res = await send_bot_animation(user.telegram_id, gif_url, caption=menu_text, reply_markup=markup)
        if str(user.telegram_id) not in USER_BOT_SESSION:
            USER_BOT_SESSION[str(user.telegram_id)] = {"state": None, "cart": {}}
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


def render_order_confirmation_screen(db: Session, user: User, session: dict) -> tuple[str, dict]:
    address = session.get("temp_address")
    phone   = session.get("temp_phone")
    multiplier = 1.0
    delivery_charge = 30.0
    if user.city:
        loc = db.query(LocationPricing).filter(LocationPricing.city.ilike(user.city)).first()
        if loc:
            multiplier = loc.price_multiplier
            delivery_charge = loc.delivery_charge

    cart = session.get("cart", {})
    active_deal = session.get("active_deal")
    if active_deal:
        subtotal = session.get("deal_price", 0.0)
    else:
        subtotal = 0.0
        for pid_str, qty in cart.items():
            p = db.query(Product).filter(Product.id == pid_str).first()
            if p:
                price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
                subtotal += price * qty

    bot_fee = get_bot_fee(db)
    total_payable = subtotal + bot_fee

    item_lines = []
    for pid_str, qty in list(cart.items()):
        p = db.query(Product).filter(Product.id == pid_str).first()
        if p:
            price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
            veg_dot = "🟢" if p.is_veg else "🔴"
            item_lines.append(f"  {veg_dot} {p.name} ×{qty}" if active_deal else f"  {veg_dot} {p.name} ×{qty}  —  ₹{price * qty:.0f}")
    items_text = "\n".join(item_lines) if item_lines else "  • (items unavailable)"

    # Order note (delivery_instructions)
    order_note = session.get("order_note", "")
    note_line = f"\n✏️ <b>Order Note:</b> <i>{order_note}</i>" if order_note else ""
    note_btn_label = "✏️ Edit Note" if order_note else "📝 Add Note to Order"

    confirm_text = (
        "📋 <b>Review Your Order</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🛒 <b>Items:</b>\n{items_text}\n\n"
        f"📍 <b>City:</b> {user.city or '—'}\n"
        f"🏠 <b>Delivery Address:</b> {address}\n"
        f"📱 <b>Phone:</b> {phone}"
        f"{note_line}\n\n"
        "💰 <b>Price Breakdown:</b>\n"
        f"  Pizza Total:      ₹{subtotal:.2f}\n"
        f"  Bot Service Fee:  +₹{bot_fee:.2f}\n"
        "  ─────────────────────\n"
        f"  <b>Total Payable:  ₹{total_payable:.2f}</b>\n\n"
        f"💳 <i>Wallet Balance: ₹{user.wallet_balance:.2f}</i>\n\n"
        "✅ Tap <b>Confirm &amp; Place</b> to place your order!"
    )
    confirm_markup = {
        "inline_keyboard": [
            [
                {"text": "✅ Confirm & Place", "callback_data": "order_confirm_place"},
                {"text": note_btn_label,       "callback_data": "checkout_add_note"}
            ],
            [
                {"text": "✏️ Edit Details",  "callback_data": "checkout_edit_details"},
                {"text": "❌ Cancel Order",  "callback_data": "order_cancel_place"}
            ]
        ]
    }
    return confirm_text, confirm_markup


async def initiate_checkout(db: Session, user: User, session: dict, edit_message_id: int = None):
    """Starts or resumes the checkout flow using a unified message layout."""
    cart = session.get("cart", {})
    if not cart:
        if edit_message_id:
            await edit_bot_message(user.telegram_id, edit_message_id, "🛒 <b>Your cart is empty!</b>")
        else:
            await send_bot_message(user.telegram_id, "🛒 <b>Your cart is empty!</b>")
        return

    saved_addr = db.query(SavedAddress).filter(
        SavedAddress.user_id == user.id, SavedAddress.is_default == True
    ).first()
    if not saved_addr:
        saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()

    if (user.latitude is not None and user.longitude is not None):
        if not saved_addr:
            saved_addr = SavedAddress(user_id=user.id, label="Home", is_default=True, latitude=user.latitude, longitude=user.longitude, city=user.city)
            db.add(saved_addr)
            db.commit()
        elif saved_addr.latitude is None or saved_addr.longitude is None:
            saved_addr.latitude = user.latitude
            saved_addr.longitude = user.longitude
            db.commit()
    elif saved_addr and saved_addr.latitude is not None and saved_addr.longitude is not None:
        user.latitude = saved_addr.latitude
        user.longitude = saved_addr.longitude
        if not user.city and saved_addr.city:
            user.city = saved_addr.city
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

    if (city and city != "Not Shared" or has_coords):
        if has_doorstep_address and saved_phone and has_coords and not session.get("force_address_entry"):
            # AUTO-SKIP: If we already have their location, address, and phone, go straight to order confirmation!
            session["temp_address"] = saved_address
            session["temp_phone"]   = saved_phone
            session["state"] = "waiting_for_confirm"
            prompt, confirm_markup = render_order_confirmation_screen(db, user, session)
            if edit_message_id:
                await edit_bot_message(user.telegram_id, edit_message_id, prompt, reply_markup=confirm_markup)
            else:
                await send_bot_message(user.telegram_id, prompt, reply_markup=confirm_markup)
            return

        # City known but missing details or coordinates — show status of 3 fields
        addr_line  = f"\n✅ <b>Saved Address:</b> <code>{saved_address}</code>" if has_doorstep_address else "\n⚠️ <b>Saved Address:</b> <i>Missing / Required</i>"
        phone_line = f"\n✅ <b>Phone:</b> <code>{saved_phone}</code>" if saved_phone else "\n⚠️ <b>Phone:</b> <i>Missing / Required</i>"
        coords_line = f"\n✅ <b>GPS Coordinates:</b> <code>{user.latitude:.5f}, {user.longitude:.5f}</code>" if has_coords else "\n⚠️ <b>GPS Coordinates:</b> <i>Missing / Required</i>"
        
        prompt = (
            "📍 <b>Delivery Details Required</b>\n\n"
            f"Your delivery city is: <b>{city}</b>\n"
            f"{addr_line}"
            f"{phone_line}"
            f"{coords_line}\n\n"
            "Please enter/update the missing details below to proceed."
        )
        inline = []
        if has_doorstep_address and saved_phone and has_coords:
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
        user.bot_cart = json.dumps(session.get("cart", {}))
        db.commit()
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
    import json
    saved_cart = {}
    if user.bot_cart:
        try:
            saved_cart = json.loads(user.bot_cart)
        except Exception:
            pass

    if str(telegram_id) not in USER_BOT_SESSION:
        USER_BOT_SESSION[str(telegram_id)] = {
            "state": user.bot_state,
            "cart": saved_cart
        }
    else:
        session = USER_BOT_SESSION[str(telegram_id)]
        if user.bot_state is not None:
            session["state"] = user.bot_state
        if saved_cart:
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
            upi_id = upi_id_cfg.value if upi_id_cfg else "dominos@upi"
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
            
        elif prev_state and prev_state.startswith("waiting_for_utr_"):
            await send_bot_message(user.telegram_id, "❌ Action cancelled.", reply_markup=main_keyboard)
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
        if not saved_addr:
            saved_addr = SavedAddress(user_id=user.id, label="Home", is_default=True)
            db.add(saved_addr)
            
        if not has_doorstep:
            saved_addr.full_address = "GPS Location"
            
        saved_addr.latitude = lat
        saved_addr.longitude = lon
        saved_addr.city = city
        db.commit()
        
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
        
        if session.get("checkout_pending"):
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

    elif text_clean == "❌ Cancel" or text_lower.startswith("/cancel"):
        session["state"] = None
        if session.get("checkout_pending"):
            session["checkout_pending"] = False
            await initiate_checkout(db, user, session)
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

    # --- UTR Auto-detection ---
    if text_clean.isdigit() and len(text_clean) == 12:
        pending_order = db.query(Order).filter(
            Order.user_id == user.id,
            Order.status.in_(["Pending Payment", "Payment Pending"])
        ).order_by(Order.created_at.desc()).first()
        if pending_order:
            pending_order.transaction_id = text_clean
            pending_order.status = "Pending Verification"
            
            attempt = UTRAttempt(order_id=pending_order.id, utr=text_clean, is_successful=False)
            db.add(attempt)
            db.commit()
            
            await send_bot_message(
                user.telegram_id,
                f"✅ <b>UTR Associated!</b>\n\nYour UTR <code>{text_clean}</code> has been linked to order <code>{pending_order.id}</code> for verification."
            )
            
            # Notify admins of the payment verification request
            if pending_order.id.startswith("TOPUP-"):
                admin_text = (
                    "🔔 <b>New Deposit Marked as Paid (Via UTR)</b>\n\n"
                    f"👤 <b>User:</b> {user.display_name} (ID: {user.telegram_id})\n"
                    f"💰 <b>Amount:</b> ₹{pending_order.total_payable:.2f}\n"
                    f"🆔 <b>Ref ID:</b> <code>{pending_order.id}</code>\n"
                    f"🔢 <b>UTR:</b> <code>{text_clean}</code>"
                )
                admin_markup = {
                    "inline_keyboard": [
                        [
                            {"text": "✅ Approve", "callback_data": f"admin_dep_approve_{pending_order.id}"},
                            {"text": "❌ Reject", "callback_data": f"admin_dep_reject_{pending_order.id}"}
                        ]
                    ]
                }
                await notify_admins(db, admin_text, reply_markup=admin_markup)
            else:
                admin_text = (
                    "⚠️ <b>New Order Placed (Manual Admin Action Required):</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🆔 <b>Order ID:</b> <code>{pending_order.id}</code>\n"
                    f"👤 <b>User:</b> {user.display_name} (ID: <code>{user.telegram_id}</code>)\n"
                    f"💰 <b>Total Paid:</b> ₹{pending_order.total_payable:.2f}\n"
                    f"🔢 <b>UTR Number:</b> <code>{text_clean}</code>\n"
                    f"🏡 <b>Address:</b> <code>{pending_order.address}</code>\n"
                    f"📱 <b>Phone:</b> <code>{pending_order.phone}</code>\n"
                    f"📍 <b>GPS Coordinates:</b> <a href='https://www.google.com/maps?q={user.latitude},{user.longitude}'>🗺️ View on Google Maps ({user.latitude:.6f}, {user.longitude:.6f})</a>\n\n"
                    "👩‍🍳 <b>Actions:</b>"
                )
                action_markup = {
                    "inline_keyboard": [
                        [
                            {"text": "✅ Accept & Complete", "callback_data": f"admin_act_complete_{pending_order.id}"},
                            {"text": "❌ Reject & Refund", "callback_data": f"admin_act_reject_{pending_order.id}"}
                        ],
                        [
                            {"text": "💬 Reply to Customer", "callback_data": f"admin_reply_support_{user.telegram_id}"}
                        ]
                    ]
                }
                await notify_admins(db, admin_text, reply_markup=action_markup)
            return

    if session.get("state") in ("waiting_for_city", "in_location_menu"):
        city_buttons = [
            "❌ skip location", "🍕 view menu", "💰 my wallet", "📦 track orders",
            "📍 change location", "❌ cancel", "🏠 update delivery address", "📱 update phone number", "🔙 back"
        ]
        if text and text.strip().lower() in city_buttons:
            pass  # Fall through to main button routing
        elif text and text.strip():
            city_input = text.strip().title()
            
            # Geocode the typed city/area to resolve coordinates
            try:
                from .routes import geocode_address
                lat, lon = await geocode_address(city_input)
                if lat is not None and lon is not None:
                    user.city = city_input
                    user.latitude = lat
                    user.longitude = lon
                    db.commit()
                    
                    saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
                    if not saved_addr:
                        saved_addr = SavedAddress(user_id=user.id, label="Home", is_default=True)
                        db.add(saved_addr)
                    # Note: We keep the full_address if it is already a doorstep address,
                    # but if it was missing or GPS Location, we set it to city_input.
                    if not saved_addr.full_address or saved_addr.full_address == "GPS Location":
                        saved_addr.full_address = city_input
                    saved_addr.latitude = lat
                    saved_addr.longitude = lon
                    saved_addr.city = city_input
                    db.commit()
                    session["state"] = None
                    session["force_address_entry"] = True
                else:
                    await send_bot_message(
                        user.telegram_id,
                        f"⚠️ <b>City/Area Resolution Failed:</b>\n\n"
                        f"We could not resolve coordinates for '{city_input}'. Please enter a more specific city/area name or share your GPS location using the button below.",
                        reply_markup={
                            "keyboard": [
                                [{"text": "📍 Share My GPS Location", "request_location": True}],
                                [{"text": "❌ Cancel"}]
                            ],
                            "resize_keyboard": True,
                            "one_time_keyboard": True
                        }
                    )
                    return
            except Exception as e:
                logger.error(f"Error resolving coordinates for city '{city_input}': {e}")
                await send_bot_message(user.telegram_id, "⚠️ Error resolving location. Please try again or share your GPS location.")
                return
            
            try:
                from .services.dominos_service import sync_realtime_menu, sync_realtime_menu_bg
                if db.query(Product).count() > 5:
                    asyncio.create_task(sync_realtime_menu_bg(city_input))
                else:
                    await sync_realtime_menu(city_input, db)
            except Exception as e:
                logger.error(f"Error syncing menu for typed city '{city_input}': {e}")
            
            if session.get("checkout_pending"):
                session["checkout_pending"] = False
                await initiate_checkout(db, user, session)
            else:
                await send_bot_message(
                    user.telegram_id,
                    f"✅ <b>City updated to: {city_input}</b>\n\nMenu prices and store settings have been updated for your city!"
                )
                await display_delivery_location_menu(db, user)
            return

    if session.get("state") == "waiting_for_phone_update":
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
        session["state"] = None
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Phone updated to <code>{phone_formatted}</code></b>\n\nThis number will be used for all future orders."
        )
        await display_delivery_location_menu(db, user)
        return

    state = session.get("state") or ""
    if state.startswith("admin_replying_to_"):
        target_tg_id = state.replace("admin_replying_to_", "").strip()
        reply_text = text.strip() if text else ""
        if not reply_text:
            await send_bot_message(user.telegram_id, "⚠️ Please send a valid message text.")
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
                logger.warning(f"Could not save support message reply: {e}")
                
            cust_msg = (
                f"💬 <b>Support Agent Reply:</b>\n"
                f"{reply_text}"
            )
            await send_bot_message(target_tg_id, cust_msg)
            await send_bot_message(
                user.telegram_id,
                f"✅ <b>Reply sent to customer</b> (TG ID: <code>{target_tg_id}</code>)."
            )
            session["state"] = None
        else:
            await send_bot_message(user.telegram_id, f"❌ Target user (TG ID: {target_tg_id}) not found.")
            session["state"] = None
        return

    if session.get("state") == "waiting_for_support_message":
        msg_text = text.strip() if text else ""
        if not photo and not document and len(msg_text) < 10:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Message too short.</b>\n\nPlease describe your issue in at least 10 characters so we can help you.",
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
            "<i>Your message:</i>\n" + f"<blockquote>{msg_text[:300]}</blockquote>",
            reply_markup=main_keyboard
        )
        
        # Forward to admin
        admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
        relation = session.get("support_relation", "General Query")
        
        # Build relation detail for admin
        relation_detail = f"<b>{relation}</b>"
        if relation.startswith("Order: "):
            oid = relation.replace("Order: ", "").strip()
            bg_order = db.query(Order).filter(Order.id == oid).first()
            if bg_order:
                relation_detail += f" (Status: {bg_order.status}, Paid: ₹{bg_order.total_payable:.2f}, Phone: {bg_order.phone})"
                
        admin_ticket_text = (
            f"💬 <b>Support Ticket from {user.display_name}</b>\n"
            f"• User ID: <code>{user.id}</code>\n"
            f"• Telegram ID: <code>{user.telegram_id}</code>\n"
            f"• Username: @{user.username or '—'}\n"
            f"• Phone Number: <code>{user.phone or '—'}</code>\n"
            f"• Relates to: {relation_detail}\n\n"
            f"✉️ <b>Message:</b>\n"
            f"<blockquote>{msg_text}</blockquote>"
        )
        admin_ticket_markup = {
            "inline_keyboard": [
                [{"text": "💬 Custom Reply", "callback_data": f"admin_reply_support_{user.telegram_id}"}],
                [
                    {"text": "📋 Order Placed", "callback_data": f"admin_tmpl_placed_{user.telegram_id}"},
                    {"text": "💸 Refund Done", "callback_data": f"admin_tmpl_refund_{user.telegram_id}"}
                ],
                [
                    {"text": "❌ UTR Invalid", "callback_data": f"admin_tmpl_utr_{user.telegram_id}"},
                    {"text": "🕒 Delay Alert", "callback_data": f"admin_tmpl_delay_{user.telegram_id}"}
                ]
            ]
        }
        if file_id:
            await send_bot_photo(admin_tg_id, file_id, caption=admin_ticket_text, reply_markup=admin_ticket_markup)
        else:
            await send_bot_message(admin_tg_id, admin_ticket_text, reply_markup=admin_ticket_markup)
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



    if session.get("state") == "waiting_for_address":
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
            saved_addr = SavedAddress(user_id=user.id, label="Home", is_default=True)
            db.add(saved_addr)
        saved_addr.full_address = addr_stripped
        db.commit()
        
        logger.info(f"[Bot Checkout] Saved doorstep address without geocoding: {addr_stripped}")
            
        if session.get("checkout_pending"):
            if user.phone:
                session["state"] = None
                session["checkout_pending"] = False
                await initiate_checkout(db, user, session)
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

    elif session.get("state") == "waiting_for_topup_amount":
        try:
            amount = float(text_clean)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            await send_bot_message(user.telegram_id, "❌ Please enter a valid positive number for the amount (e.g. 200, 500).")
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
        
        upi_id_val = upi_id_cfg.value if upi_id_cfg else "dominos@upi"
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
        
        upi_id_val = upi_id_cfg.value if upi_id_cfg else "dominos@upi"
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

    elif session.get("state") == "waiting_for_topup_utr":
        utr = text_clean
        if not (utr.isdigit() and len(utr) == 12):
            await send_bot_message(user.telegram_id, "❌ Invalid format. Please reply with your <b>12-digit UPI UTR number</b> (digits only).")
            return
            
        # Check if UTR is already used
        dup = db.query(Order).filter(Order.transaction_id == utr).first()
        if dup:
            await send_bot_message(user.telegram_id, "❌ This UPI Transaction UTR has already been submitted. Please enter a new UTR.")
            return
            
        amount = session.get("topup_amount", 100.0)
        session["state"] = None
        
        # Create a TOPUP order with Payment Pending status
        import random
        topup_order = Order(
            id=f"TOPUP-{random.randint(100000, 999999)}",
            user_id=user.id,
            original_total=amount,
            discount=0.0,
            delivery_charge=0.0,
            total_payable=amount,
            status="Payment Pending",
            payment_method="upi",
            transaction_id=utr,
            city=user.city or "Mumbai"
        )
        db.add(topup_order)
        db.flush()
        
        # Log as an unverified payment attempt (is_successful=False)
        attempt = UTRAttempt(order_id=topup_order.id, utr=utr, is_successful=False)
        db.add(attempt)
        db.commit()
        
        # Broadcast SSE update for admin dashboard
        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "order_update", "order_id": topup_order.id, "status": "Payment Pending"})
            except Exception:
                pass
                
        pending_text = (
            f"📥 <b>Top-up Request Submitted!</b>\n\n"
            f"• Amount: <b>₹{amount:.2f}</b>\n"
            f"• UTR: <code>{utr}</code>\n"
            f"• Ref ID: <code>{topup_order.id}</code>\n\n"
            f"⏳ Your payment is pending verification by our admin. Your wallet balance will update automatically once verified."
        )
        await send_bot_message(user.telegram_id, pending_text, reply_markup=main_keyboard)
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
            
        session["state"] = "waiting_for_confirm"
        session["force_address_entry"] = False
        
        cart = session.get("cart", {})
        if not cart:
            session["state"] = None
            await send_bot_message(user.telegram_id, "❌ Your cart is empty! Order cancelled.", reply_markup=main_keyboard)
            return
            
        address = session.get("temp_address", "Default Address")
        phone = phone_formatted
        
        # Calculate pricing based on location multiplier
        multiplier = 1.0
        delivery_charge = 30.0
        if user.city:
            loc = db.query(LocationPricing).filter(LocationPricing.city.ilike(user.city)).first()
            if loc:
                multiplier = loc.price_multiplier
                delivery_charge = loc.delivery_charge

        active_deal = session.get("active_deal")
        if active_deal:
            subtotal = session.get("deal_price", 0.0)
        else:
            subtotal = 0.0
            for product_id_str, qty in cart.items():
                product_id = product_id_str  # Product.id is a UUID string
                p = db.query(Product).filter(Product.id == product_id).first()
                if p:
                    price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
                    subtotal += (price * qty)
                
        # Fetch bot service fee
        bot_fee = get_bot_fee(db)
        total_payable = subtotal + bot_fee
        
        # Build item list for the confirmation message
        item_lines = []
        for product_id_str, qty in list(cart.items()):
            p = db.query(Product).filter(Product.id == product_id_str).first()
            if p:
                price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
                if active_deal:
                    item_lines.append(f"  • {p.name} x{qty}")
                else:
                    item_lines.append(f"  • {p.name} x{qty} — ₹{price * qty:.0f}")
        items_text = "\n".join(item_lines) if item_lines else "  • (items unavailable)"

        confirm_text = (
            f"📋 <b>Please Confirm Your Order</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🛒 <b>Items:</b>\n{items_text}\n\n"
            f"🏡 <b>Delivery Address:</b> {address}\n"
            f"📱 <b>Phone Number:</b> {phone}\n\n"
            f"💰 <b>Price Breakdown:</b>\n"
            f"  Pizza Total:     ₹{subtotal:.2f}\n"
            f"  Bot Service Fee: +₹{bot_fee:.2f}\n"
            f"  ─────────────────\n"
            f"  <b>Total Payable: ₹{total_payable:.2f}</b>\n\n"
            f"💡 <i>Wallet Balance: ₹{user.wallet_balance:.2f}</i>\n\n"
            f"Are you sure you want to place this order?"
        )
        
        confirm_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ Yes, Confirm & Place", "callback_data": "order_confirm_place"},
                    {"text": "❌ Cancel Order", "callback_data": "order_cancel_place"}
                ]
            ]
        }
        await send_bot_message(
            user.telegram_id,
            confirm_text,
            reply_markup=confirm_markup
        )
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
        pending_orders_count = db.query(Order).filter(Order.status.in_(["Paid", "Pending Payment", "Order Processing"]), ~Order.id.like("TOPUP-%")).count()
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
                    {"text": "🎟️ Manage Promo Codes", "callback_data": "admin_promo_menu"},
                    {"text": "👥 Manage Users", "callback_data": "admin_manage_users"}
                ],
                [
                    {"text": "⚙️ System Config", "callback_data": "admin_sys_config"},
                    {"text": "📊 Reports & Backup", "callback_data": "admin_reports_menu"}
                ],
                [
                    {"text": "🏦 Pending Deposits", "callback_data": "admin_view_pending_deposits"}
                ],
                [
                    {"text": "⚠️ View Error Logs", "callback_data": "admin_view_error_logs"}
                ]
            ]
        }

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
        # Automatically display the menu
        await display_pizza_menu(db, user, main_keyboard)
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

    elif text_lower == "💰 my wallet" or text.startswith("/wallet") or text.startswith("/balance"):
        wallet_text, wallet_markup = render_wallet_view(db, user, offset=0, limit=5)
        res = await send_bot_message(user.telegram_id, wallet_text, reply_markup=wallet_markup)
        # Also edit in place if we have a last message to avoid spam
        last_msg_id = session.get("last_bot_msg_id")
        if last_msg_id:
            await edit_bot_message(user.telegram_id, last_msg_id, wallet_text, reply_markup=wallet_markup)
        else:
            if isinstance(res, int):
                session["last_bot_msg_id"] = res
        return

    elif text_lower == "wallet_add" or text_lower == "💳 add funds" or text_clean.startswith("/addfunds"):
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
        _now_utc = _dt.datetime.utcnow()
        _cutoff_utc = _now_utc - _dt.timedelta(hours=24)
        orders = db.query(Order).filter(
            Order.user_id == user.id,
            ~Order.id.like("TOPUP-%"),
            Order.created_at >= _cutoff_utc
        ).order_by(Order.created_at.desc()).limit(5).all()
        if not orders:
            track_text = (
                "📦 <b>Track Orders:</b>\n\n"
                "No orders placed in the last 24 hours!\n\n"
                "👉 Click <b>Order App</b> or type /menu to order delicious pizzas!"
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

    elif text_lower == "🎉 active offers" or "/offers" in text_lower or "offer" in text_lower or "coupon" in text_lower:
        bot_fee = get_bot_fee(db)
        
        offers_text = (
            "🎉 <b>Special Active Deals:</b>\n\n"
            "Select one of our exclusive deals below to add items to your cart:\n\n"
            "🔥 <b>Deal 1: Medium Cheese Burst Margherita</b>\n"
            "• 1x Cheese Burst Margherita (Medium)\n"
            f"• <b>Price:</b> ₹220.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "🔥 <b>Deal 2: Paneer & Corn</b>\n"
            "• 1x Paneer & Corn Pizza (Regular)\n"
            f"• <b>Price:</b> ₹90.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "🔥 <b>Deal 3: Double Cheese Burst Margherita</b>\n"
            "• 1x Cheese Burst Margherita (Large / Double)\n"
            f"• <b>Price:</b> ₹240.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "🔥 <b>Deal 4: Classic Duo</b>\n"
            "• 1x Paneer + 1x Capsicum & Paprika, or 2x Paneer / 2x Capsicum\n"
            f"• <b>Price:</b> ₹105.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "🔥 <b>Deal 5A: 3x Onion Pizzas</b>\n"
            "• 3x Onion Pizzas (Regular)\n"
            f"• <b>Price:</b> ₹100.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "🔥 <b>Deal 5B: 4x Classic Pizzas</b>\n"
            "• 4x Classic Pizzas (Regular)\n"
            f"• <b>Price:</b> ₹90.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "🔥 <b>Deal 6: 2x Chicken Sausage Pizzas</b>\n"
            "• 2x Chicken Sausage Pizzas (Regular)\n"
            f"• <b>Price:</b> ₹105.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "💡 <i>Tap a deal below to load it into your cart instantly. Custom combinations available via Support!</i>"
        )
        offers_markup = {
            "inline_keyboard": [
                [{"text": "🔥 Deal 1: Cheese Burst Margherita (₹220)", "callback_data": "apply_deal_1"}],
                [{"text": "🔥 Deal 2: Paneer & Corn (₹90)", "callback_data": "apply_deal_2"}],
                [{"text": "🔥 Deal 3: Double Cheese Burst (₹240)", "callback_data": "apply_deal_3"}],
                [{"text": "🔥 Deal 4: Classic Duo (₹105)", "callback_data": "apply_deal_4"}],
                [{"text": "🔥 Deal 5A: 3x Onion Pizzas (₹100)", "callback_data": "apply_deal_5a"}],
                [{"text": "🔥 Deal 5B: 4x Classic Pizzas (₹90)", "callback_data": "apply_deal_5b"}],
                [{"text": "🔥 Deal 6: 2x Chicken Sausage (₹105)", "callback_data": "apply_deal_6"}],
                [{"text": "🛒 View Cart", "callback_data": "cart_view"}]
            ]
        }
        
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
                [{"text": "📦 FAQ: Where is my Order?", "callback_data": "faq_where_order"}]
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
        # Filter out short noise / greetings / single-word typos from creating support tickets
        ignored_words = ("hi", "hello", "hey", "test", "ok", "menu", "pizza", "help", "start", "admin", "back", "cancel")
        if len(msg_text) < 8 or msg_text.lower() in ignored_words:
            session["state"] = None
            help_reply = (
                f"❓ <b>I have not recognized your message.</b>\n\n"
                f"Kindly click one of the menu buttons below to navigate, view pizzas, or manage your order.\n\n"
                f"<i>If you need assistance or want a custom order, tap <b>💬 Contact Support</b> in the menu or type /support.</i>"
            )
            await send_bot_message(user.telegram_id, help_reply, reply_markup=main_keyboard)
            return

        # Descriptive text: save as support message
        try:
            sup = SupportMessage(
                user_id=user.id,
                sender_type="user",
                message=msg_text
            )
            db.add(sup)
            db.commit()
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({
                        "type": "support_message",
                        "user_id": user.id,
                        "message": msg_text,
                        "display_name": user.display_name
                    })
                except Exception:
                    pass
        except Exception as db_err:
            logger.warning(f"Could not save fallback support message: {db_err}")

        await send_bot_message(
            user.telegram_id,
            "✅ <b>Support message sent!</b>\n\n"
            "Our team has received your message and will reply directly in this chat shortly.\n\n"
            "<i>Your message:</i>\n" + f"<blockquote>{escape_html(msg_text[:300])}</blockquote>",
            reply_markup=main_keyboard
        )
        return

def render_cart_message(db: Session, user: User, cart: dict, session: dict = None):
    """Generates the shopping cart item lines and dynamic checkout keyboard."""
    if not cart:
        return "🛒 <b>Your Cart is empty!</b>\n\nClick below to view our menu and start ordering pizzas.", {
            "inline_keyboard": [[{"text": "🍕 View Menu", "callback_data": "menu_view"}]]
        }
        
    lines = ["🛒 <b>Your Current Shopping Cart:</b>\n"]
    subtotal = 0.0
    inline_keyboard = []

    # Fetch price multiplier and delivery charge based on user city
    multiplier = 1.0
    delivery_charge = 30.0
    if user.city:
        loc = db.query(LocationPricing).filter(LocationPricing.city.ilike(user.city)).first()
        if loc:
            multiplier = loc.price_multiplier
            delivery_charge = loc.delivery_charge
            
    active_deal = session.get("active_deal") if session else None
    if active_deal:
        deal_price = session.get("deal_price", 0.0)
        if active_deal == "deal_1":
            lines.append("🔥 <b>Active Deal: Double Cheeseburst Feast</b> (₹410.00)")
        elif active_deal == "deal_2":
            lines.append("🔥 <b>Active Deal: Veggie Duo Deal</b> (₹90.00)")
        elif active_deal == "deal_3":
            lines.append("🔥 <b>Active Deal: Classic Pizza Duo</b> (₹150.00)")
    
    for product_id_str, qty in list(cart.items()):
        product_id = product_id_str  # Product.id is a UUID string
        p = db.query(Product).filter(Product.id == product_id).first()
        if not p:
            continue
        price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
        item_total = price * qty
        if not active_deal:
            subtotal += item_total
        
        lines.append(f"• <b>{p.name}</b> (x{qty}) — ₹{price:.0f} ea. = <b>₹{item_total:.0f}</b>")
        
        # Incrementor/decrementor/delete row
        inline_keyboard.append([
            {"text": "➖", "callback_data": f"cart_sub_{product_id}"},
            {"text": f"🍕 {p.name} (x{qty})", "callback_data": "cart_view"},
            {"text": "➕", "callback_data": f"cart_add_{product_id}"},
            {"text": "🗑️", "callback_data": f"cart_del_{product_id}"}
        ])
        
    if active_deal:
        subtotal = deal_price
        
    # Fetch bot service fee
    bot_fee = get_bot_fee(db)
    total_payable = subtotal + bot_fee
    
    # Proactive Deal suggestions
    suggestions = []
    if not active_deal:
        cart_product_names = {}
        for p_id_str, qty in cart.items():
            p = db.query(Product).filter(Product.id == p_id_str).first()
            if p:
                cart_product_names[p.name] = qty
                
        has_paneer = "Paneer & Capsicum" in cart_product_names
        has_corn = any("corn" in name.lower() for name in cart_product_names.keys())
        
        has_margherita = any("margherita" in name.lower() for name in cart_product_names.keys())
        has_tomato = any("tomato" in name.lower() or "onion" in name.lower() for name in cart_product_names.keys())
        
        if (has_paneer or has_corn) and not (has_paneer and has_corn):
            missing = "Golden Corn Pizza" if has_paneer else "Paneer & Capsicum Pizza"
            suggestions.append(f"💡 <i>Tip: Add a <b>{missing}</b> to unlock the Veggie Duo Deal (2 pizzas for ₹90)!</i>")
        elif has_paneer and has_corn:
            suggestions.append("🎉 <i>You qualify for the <b>Veggie Duo Deal</b>! Go to 'Active Offers' to apply it for ₹90.</i>")
            
        if (has_margherita or has_tomato) and not (has_margherita and has_tomato):
            missing = "Tomato Onion Pizza" if has_margherita else "Margherita Classic Pizza"
            suggestions.append(f"💡 <i>Tip: Add a <b>{missing}</b> to unlock the Classic Pizza Duo Deal (2 pizzas for ₹150)!</i>")
        elif has_margherita and has_tomato:
            suggestions.append("🎉 <i>You qualify for the <b>Classic Pizza Duo Deal</b>! Go to 'Active Offers' to apply it for ₹150.</i>")
            
    if suggestions:
        lines.append("\n" + "\n".join(suggestions))
    
    lines.append(f"\n💵 <b>Pizza Total:</b> ₹{subtotal:.2f}")
    lines.append(f"🤖 <b>Bot Service Fee:</b> ₹{bot_fee:.2f}")
    lines.append(f"💳 <b>Total Payable:</b> <b>₹{total_payable:.2f}</b>")
    lines.append(f"\n💰 <b>Your Wallet Balance:</b> ₹{user.wallet_balance:.2f}")
    
    action_row = [
        {"text": "❌ Clear Cart", "callback_data": "cart_empty"},
        {"text": "🍕 View Menu", "callback_data": "menu_view"}
    ]
    inline_keyboard.append(action_row)
    
    if user.wallet_balance >= total_payable:
        inline_keyboard.append([{"text": "🛍️ Checkout & Place Order", "callback_data": "cart_checkout"}])
    else:
        inline_keyboard.append([{"text": "⚠️ Insufficient Wallet Balance", "callback_data": "wallet_view"}])
        
    return "\n".join(lines), {"inline_keyboard": inline_keyboard}

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
    if user.bot_cart:
        try:
            saved_cart = json.loads(user.bot_cart)
        except Exception:
            pass

    if str(telegram_id) not in USER_BOT_SESSION:
        USER_BOT_SESSION[str(telegram_id)] = {
            "state": user.bot_state,
            "cart": saved_cart
        }
    else:
        session = USER_BOT_SESSION[str(telegram_id)]
        if user.bot_state is not None:
            session["state"] = user.bot_state
        if saved_cart:
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
        
    elif data == "menu_page_noop":
        await answer_callback_query(callback_query_id)
        return
        
    elif data.startswith("menu_page_"):
        parts = data.split("_")
        page = int(parts[2])
        category = parts[3]
        await display_pizza_menu(db, user, main_keyboard, page=page, category=category, edit_message_id=message_id)
        await answer_callback_query(callback_query_id)
        
    elif data == "apply_deal_1":
        # Deal 1: 1x Cheese Burst Margherita (Medium) - ₹220
        p = (db.query(Product).filter(Product.name.like("%Cheese Burst%"), Product.name.like("%Margherita%")).first()
             or db.query(Product).filter(Product.name.like("%Margherita%")).first()
             or db.query(Product).first())
        if not p:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p.id): 1}
        session["active_deal"] = "deal_1"
        session["deal_price"] = 220.0
        await answer_callback_query(callback_query_id, "Deal 1 applied! ₹220")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "apply_deal_2":
        # Deal 2: 1x Paneer & Corn Pizza - ₹90
        p = (db.query(Product).filter(Product.name.like("%Paneer%"), Product.name.like("%Corn%")).first()
             or db.query(Product).filter(Product.name.like("%Paneer%")).first()
             or db.query(Product).first())
        if not p:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p.id): 1}
        session["active_deal"] = "deal_2"
        session["deal_price"] = 90.0
        await answer_callback_query(callback_query_id, "Deal 2 applied! ₹90")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "apply_deal_3":
        # Deal 3: 1x Double Cheese Burst Margherita (Large) - ₹240
        p = (db.query(Product).filter(Product.name.like("%Cheese Burst%")).first()
             or db.query(Product).filter(Product.name.like("%Margherita%")).first()
             or db.query(Product).first())
        if not p:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p.id): 1}
        session["active_deal"] = "deal_3"
        session["deal_price"] = 240.0
        await answer_callback_query(callback_query_id, "Deal 3 applied! ₹240")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "apply_deal_4":
        # Deal 4: Classic Duo - 1x Paneer + 1x Capsicum - ₹105
        p_paneer = (db.query(Product).filter(Product.name.like("%Paneer%")).first() or db.query(Product).first())
        p_cap = (db.query(Product).filter(Product.name.like("%Capsicum%")).first() or p_paneer)
        if not p_paneer:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p_paneer.id): 1, str(p_cap.id): 1} if str(p_paneer.id) != str(p_cap.id) else {str(p_paneer.id): 2}
        session["active_deal"] = "deal_4"
        session["deal_price"] = 105.0
        await answer_callback_query(callback_query_id, "Deal 4 applied! ₹105")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "apply_deal_5a":
        # Deal 5A: 3x Onion Pizzas - ₹100
        p = (db.query(Product).filter(Product.name.like("%Onion%")).first() or db.query(Product).first())
        if not p:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p.id): 3}
        session["active_deal"] = "deal_5a"
        session["deal_price"] = 100.0
        await answer_callback_query(callback_query_id, "Deal 5A applied! ₹100")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "apply_deal_5b":
        # Deal 5B: 4x Classic Pizzas - ₹90
        p = (db.query(Product).filter(Product.name.like("%Classic%")).first() or db.query(Product).first())
        if not p:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p.id): 4}
        session["active_deal"] = "deal_5b"
        session["deal_price"] = 90.0
        await answer_callback_query(callback_query_id, "Deal 5B applied! ₹90")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "apply_deal_6":
        # Deal 6: 2x Chicken Sausage Pizzas - ₹105
        p = (db.query(Product).filter(Product.name.like("%Chicken Sausage%")).first()
             or db.query(Product).filter(Product.name.like("%Chicken%")).first()
             or db.query(Product).first())
        if not p:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p.id): 2}
        session["active_deal"] = "deal_6"
        session["deal_price"] = 105.0
        await answer_callback_query(callback_query_id, "Deal 6 applied! ₹105")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "support_menu":
        support_help = (
            "\ud83d\udcac <b>Contact Support & FAQs</b>\n\n"
            "Need help with your order, wallet, or deals?\n\n"
            "\u2022 <b>Send Message:</b> Contact our support team directly\n"
            "\u2022 <b>FAQs:</b> Tap a question below for instant answers"
        )
        support_markup = {
            "inline_keyboard": [
                [{"text": "\ud83d\udcac Send a Message to Support", "callback_data": "support_send_message"}],
                [{"text": "\ud83d\udcd6 FAQ: How to Order?", "callback_data": "faq_how_to_order"}],
                [{"text": "\ud83d\udcb3 FAQ: Wallet & Deposits?", "callback_data": "faq_wallet_upi"}],
                [{"text": "\ud83d\udce6 FAQ: Where is my Order?", "callback_data": "faq_where_order"}],
                [{"text": "\ud83e\udd1d FAQ: Custom Deals?", "callback_data": "faq_custom_deals"}]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, support_help, reply_markup=support_markup)
        await answer_callback_query(callback_query_id)

    elif data == "support_send_message":
        session["state"] = "waiting_for_support_message"
        await send_bot_message(
            user.telegram_id,
            "💬 <b>Send a Support Message</b>\n\n"
            "Please type your message below and press send.\n"
            "Our support team will review it and reply directly in this chat.\n\n"
            "<i>Describe your issue clearly, e.g.:\n"
            "\"My order #PIZZA-12345 has not been delivered after 1 hour.\"</i>",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        await answer_callback_query(callback_query_id)

    elif data == "faq_how_to_order":
        faq_text = (
            "\ud83d\udcd6 <b>FAQ: How to Order?</b>\n\n"
            "1\ufe0f\u20e3 Tap <b>\ud83d\ude92 View Menu</b> to browse Domino's pizzas\n"
            "2\ufe0f\u20e3 Tap <b>+ Add to Cart</b> on any item you want\n"
            "3\ufe0f\u20e3 Tap <b>\ud83d\uded2 View Cart</b> and then <b>Checkout</b>\n"
            "4\ufe0f\u20e3 Confirm your delivery address and phone number\n"
            "5\ufe0f\u20e3 Choose to pay with <b>\ud83d\udcb3 Wallet</b> or <b>\ud83d\udcf2 UPI QR</b>\n"
            "6\ufe0f\u20e3 Confirm your order — done! \ud83c\udf55\n\n"
            "<b>Quick Tip:</b> Top-up your wallet first (\ud83d\udcb0 My Wallet \u2192 Add Funds) for the fastest checkout!\n\n"
            "<b>Active Deals:</b> Tap \ud83c\udf89 Active Offers in the menu for special deal prices!"
        )
        back_markup = {"inline_keyboard": [[{"text": "\ud83d\udd19 Support Menu", "callback_data": "support_menu"}]]}
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
            "• At checkout, choose <b>💳 Pay with Wallet</b> (if balance is enough) or <b>📲 Pay via UPI QR</b>\n"
            "• Your balance is shown at checkout so you always know\n\n"
            "<b>Top-Up ID:</b> Every deposit gets a unique <code>TOPUP-XXXXXX</code> ID. Admins verify by this ID — no UTR needed!"
        )
        back_markup = {"inline_keyboard": [[{"text": "🔙 Support Menu", "callback_data": "support_menu"}]]}
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data == "faq_where_order":
        faq_text = (
            "📦 <b>FAQ: Where is my Order?</b>\n\n"
            "• Tap <b>📦 Track Orders</b> in the bot to see all your orders from the last 24 hours, including status, rider details, and store info.\n"
            "• Order tracking shows Domino's reference ID, rider name, and your delivery store once your order is dispatched.\n"
            "• You can cancel your order within <b>2 minutes</b> of placing it if it's still in 'Order Processing' status."
        )
        back_markup = {"inline_keyboard": [[{"text": "🔙 Support Menu", "callback_data": "support_menu"}]]}
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data == "faq_custom_deals":
        faq_text = (
            "🤝 <b>FAQ: Custom Deals?</b>\n\n"
            "We offer custom pizza combinations at special prices not listed in the public menu!\n\n"
            "To get a custom deal:\n"
            "1️⃣ Contact Support via the <b>💬 Send Message</b> option\n"
            "2️⃣ Tell us which pizzas and quantities you want\n"
            "3️⃣ Our team will quote you a discounted bundle price\n\n"
            "Custom deals are great for large orders or events! 🎉"
        )
        back_markup = {"inline_keyboard": [[{"text": "🔙 Support Menu", "callback_data": "support_menu"}]]}
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data.startswith("menu_category_"):
        category = data.split("_")[-1]
        await display_pizza_menu(db, user, main_keyboard, page=1, category=category, edit_message_id=message_id)
        await answer_callback_query(callback_query_id)
        
    elif data.startswith("cart_add_"):
        product_id = data[len("cart_add_"):]  # Full UUID string
        p = db.query(Product).filter(Product.id == product_id).first()
        if not p:
            await answer_callback_query(callback_query_id, "Product not found!")
            return
            
        cart = session["cart"]
        cart[product_id] = cart.get(product_id, 0) + 1
        session["active_deal"] = None
        
        await answer_callback_query(callback_query_id, f"Added 1x {p.name}")
        cart_text, cart_markup = render_cart_message(db, user, cart, session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)
        
    elif data.startswith("cart_sub_"):
        product_id = data[len("cart_sub_"):]  # Full UUID string
        p = db.query(Product).filter(Product.id == product_id).first()
        if not p:
            await answer_callback_query(callback_query_id, "Product not found!")
            return
            
        cart = session["cart"]
        if product_id in cart:
            cart[product_id] -= 1
            if cart[product_id] <= 0:
                del cart[product_id]
        session["active_deal"] = None
                
        await answer_callback_query(callback_query_id, f"Removed 1x {p.name}")
        cart_text, cart_markup = render_cart_message(db, user, cart, session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)
        
    elif data.startswith("cart_del_"):
        product_id = data[len("cart_del_"):]  # Full UUID string
        p = db.query(Product).filter(Product.id == product_id).first()
        if not p:
            await answer_callback_query(callback_query_id, "Product not found!")
            return
            
        cart = session["cart"]
        if product_id in cart:
            del cart[product_id]
        session["active_deal"] = None
            
        await answer_callback_query(callback_query_id, f"Removed {p.name} from cart")
        cart_text, cart_markup = render_cart_message(db, user, cart, session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)
        
    elif data == "cart_view":
        cart = session["cart"]
        cart_text, cart_markup = render_cart_message(db, user, cart, session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)
        await answer_callback_query(callback_query_id)
        
    elif data == "cart_empty":
        session["cart"] = {}
        session["active_deal"] = None
        await edit_bot_message(user.telegram_id, message_id, "🛒 <b>Your Cart is empty!</b>", {
            "inline_keyboard": [[{"text": "🍕 View Menu", "callback_data": "menu_view"}]]
        })
        await answer_callback_query(callback_query_id, "Cart cleared!")
        
    elif data == "cart_checkout":
        await initiate_checkout(db, user, session, edit_message_id=message_id)
        await answer_callback_query(callback_query_id)

    elif data == "checkout_confirm_location":
        # User confirmed existing city + saved details
        address = session.get("temp_address")
        phone   = session.get("temp_phone")
        if address and phone:
            session["state"] = "waiting_for_confirm"
            confirm_text, confirm_markup = render_order_confirmation_screen(db, user, session)
            await edit_bot_message(user.telegram_id, message_id, confirm_text, reply_markup=confirm_markup)
            await answer_callback_query(callback_query_id)
        else:
            # Have city but no saved address — ask for address
            session["state"] = "waiting_for_address"
            city_line = f"\U0001f4cd <b>City: {user.city}</b>\n\n" if user.city else ""
            prompt = (
                f"🏡 <b>Delivery Checkout:</b>\n\n{city_line}"
                "Please type your <b>full delivery address</b> in this chat and press enter."
            )
            # Send clean keyboard containing only Cancel option
            address_keyboard = {
                "keyboard": [[{"text": "❌ Cancel"}]],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            # Remove inline message first
            await delete_bot_message(user.telegram_id, message_id)
            
            # Send prompt with typing animation
            await send_bot_animation(
                user.telegram_id,
                "https://i.giphy.com/3o7iMClCoYV72aXf6o.gif",
                caption=prompt,
                reply_markup=address_keyboard
            )
            await answer_callback_query(callback_query_id)

    elif data == "checkout_change_location":
        session["checkout_pending"] = True
        session["state"] = "waiting_for_city"
        session["force_address_entry"] = True
        loc_keyboard = {
            "keyboard": [
                [{"text": "📍 Share Current Location", "request_location": True}],
                [{"text": "🔙 Back"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        current_city = f"Current city: <b>{user.city}</b>\n\n" if user.city else ""
        await send_bot_message(
            user.telegram_id,
            f"📍 <b>Change Delivery Location</b>\n\n{current_city}"
            "Tap the <b>📍 Share Current Location</b> button below or type your <b>City / Area Name</b> in this chat to update location.",
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
        session["temp_address"] = None
        session["temp_phone"]   = None
        city_line = f"\U0001f4cd <b>City:</b> {user.city}\n\n" if user.city else ""
        prompt = (
            f"🏡 <b>Delivery Checkout:</b>\n\n{city_line}"
            "Please type your <b>full delivery address</b> in this chat and press enter."
        )
        address_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_animation(
            user.telegram_id,
            "https://i.giphy.com/3o7iMClCoYV72aXf6o.gif",
            caption=prompt,
            reply_markup=address_keyboard
        )
        await answer_callback_query(callback_query_id)

    elif data == "checkout_enter_phone":
        session["state"] = "waiting_for_phone"
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
        address = html_escape(session.get("temp_address"))
        phone = html_escape(session.get("temp_phone"))
        
        if not address or not phone:
            await answer_callback_query(callback_query_id, "Error: Saved details missing.")
            return
            
        session["state"] = "waiting_for_confirm"
        
        multiplier = 1.0
        delivery_charge = 30.0
        if user.city:
            loc = db.query(LocationPricing).filter(LocationPricing.city.ilike(user.city)).first()
            if loc:
                multiplier = loc.price_multiplier
                delivery_charge = loc.delivery_charge

        cart = session.get("cart", {})
        active_deal = session.get("active_deal")
        if active_deal:
            subtotal = session.get("deal_price", 0.0)
        else:
            subtotal = 0.0
            for product_id_str, qty in cart.items():
                product_id = product_id_str  # Product.id is a UUID string
                p = db.query(Product).filter(Product.id == product_id).first()
                if p:
                    price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
                    subtotal += (price * qty)
                
        bot_fee = get_bot_fee(db)
        total_payable = subtotal + bot_fee
        
        item_lines = []
        for product_id_str, qty in list(cart.items()):
            p = db.query(Product).filter(Product.id == product_id_str).first()
            if p:
                price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
                if active_deal:
                    item_lines.append(f"  • {p.name} x{qty}")
                else:
                    item_lines.append(f"  • {p.name} x{qty} — ₹{price * qty:.0f}")
        items_text = "\n".join(item_lines) if item_lines else "  • (items unavailable)"

        confirm_text = (
            f"📋 <b>Please Confirm Your Order</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🛒 <b>Items:</b>\n{items_text}\n\n"
            f"🏡 <b>Delivery Address:</b> {address}\n"
            f"📱 <b>Phone Number:</b> {phone}\n\n"
            f"💰 <b>Price Breakdown:</b>\n"
            f"  Pizza Total:     ₹{subtotal:.2f}\n"
            f"  Bot Service Fee: +₹{bot_fee:.2f}\n"
            f"  ─────────────────\n"
            f"  <b>Total Payable: ₹{total_payable:.2f}</b>\n\n"
            f"💡 <i>Wallet Balance: ₹{user.wallet_balance:.2f}</i>\n\n"
            f"Click the confirmation button below to finalize your order."
        )
        
        confirm_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ Place Order", "callback_data": "order_confirm_place"},
                    {"text": "❌ Cancel Order", "callback_data": "cart_view"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, confirm_text, reply_markup=confirm_markup)
        await answer_callback_query(callback_query_id)
        
    elif data == "wallet_view" or data.startswith("wallet_tx_more_"):
        offset = 0
        if data.startswith("wallet_tx_more_"):
            offset = int(data.replace("wallet_tx_more_", ""))
            
        wallet_text, wallet_markup = render_wallet_view(db, user, offset=offset, limit=5)
        await edit_bot_message(user.telegram_id, message_id, wallet_text, wallet_markup)
        await answer_callback_query(callback_query_id)

    elif data.startswith("pay_now_"):
        order_id = data.replace("pay_now_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        # Construct merchant UPI Payment URI
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        upi_id = upi_id_cfg.value if upi_id_cfg else "dominos@upi"
        upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        
        upi_details = generate_upi_qr_details(upi_id, upi_name, order.total_payable, order.id, f"Order {order.id}")
        upi_uri = upi_details["upi_uri"]
        qr_url = upi_details["qr_code_url"]
        qr_data_url = upi_details.get("qr_data_url", "")
        
        payment_text = (
            f"💳 <b>UPI Payment Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>Order ID:</b> <code>{order.id}</code>\n"
            f"• <b>Amount:</b> <b>₹{order.total_payable:.2f}</b>\n\n"
            f"👉 <a href=\"{upi_uri}\"><b>📱 Click Here to Pay via UPI App</b></a> (mobile) or scan the QR code above.\n\n"
            f"After completing the UPI payment, tap <b>✅ I Have Paid / Submit Order</b> below to submit your order!"
        )
        
        payment_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ I Have Paid / Submit Order", "callback_data": f"pay_skip_utr_{order.id}"},
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
            
        order.status = "Pending Verification"
        order.transaction_id = f"NO-UTR-{uuid.uuid4().hex[:6].upper()}"
        db.commit()
        
        await edit_bot_message(
            user.telegram_id,
            message_id,
            f"⏳ <b>Order Submitted without UTR!</b>\n\n"
            f"Ref ID: <code>{order.id}</code>\n\n"
            f"We have queued your order for manual verification by the admin. This may take slightly longer than automatic UTR verification."
        )
        
        # Notify admin
        admin_text = (
            "⚠️ <b>New Order Placed (Manual Admin Action Required):</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 <b>Order ID:</b> <code>{order.id}</code>\n"
            f"👤 <b>User:</b> {user.display_name} (ID: <code>{user.telegram_id}</code>)\n"
            f"💰 <b>Total Paid:</b> ₹{order.total_payable:.2f}\n"
            f"🔢 <b>UTR Number:</b> <i>Not provided (Skipped)</i>\n"
            f"🏡 <b>Address:</b> <code>{order.address}</code>\n"
            f"📱 <b>Phone:</b> <code>{order.phone}</code>\n"
            f"📍 <b>GPS Coordinates:</b> <a href='https://www.google.com/maps?q={user.latitude},{user.longitude}'>🗺️ View on Google Maps ({user.latitude:.6f}, {user.longitude:.6f})</a>\n\n"
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

    elif data.startswith("cancel_order_"):
        order_id = data.replace("cancel_order_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        cancellable = ["Pending Payment", "Payment Pending", "Payment Received", "Order Processing"]
        if order.status not in cancellable:
            await answer_callback_query(callback_query_id, f"Cannot cancel order in status: {order.status}")
            return
            
        # Refund if wallet
        if order.payment_method == "wallet":
            user.wallet_balance += order.total_payable
            # Log WalletTransaction
            tx = WalletTransaction(
                user_id=user.id,
                type="refund",
                amount=order.total_payable,
                description=f"Refund for cancelled order: {order.id}"
            )
            db.add(tx)
            
        order.status = "Cancelled"
        
        # Add to status history  (OrderStatusHistory already imported at top of file)
        h = OrderStatusHistory(order_id=order.id, status="Cancelled")
        db.add(h)
        db.commit()
        
        # Broadcast SSE updates for admin dashboard real-time updates
        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Cancelled"})
                await sse_broadcast_callback({"type": "wallet_update", "user_id": user.id, "balance": user.wallet_balance})
            except Exception:
                pass
                
        await send_bot_message(user.telegram_id, f"❌ <b>Order Cancelled</b>\n\nOrder <code>{order.id}</code> has been cancelled successfully. Any wallet funds used have been refunded.")
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
            "Select an option below to process deposits, view history, or issue manual credits:"
        )
        
        buttons = [
            [
                {"text": "📥 Pending Deposits", "callback_data": "admin_view_pending_deposits"},
                {"text": "📜 Deposit History", "callback_data": "admin_deposit_history_page_1"}
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

    elif data.startswith("admin_deposit_history_page_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        page = int(data.replace("admin_deposit_history_page_", "").strip())
        limit = 5
        offset = (page - 1) * limit
        
        query = db.query(Order).filter(Order.id.like("TOPUP-%"))
        total_count = query.count()
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        page = max(1, min(page, total_pages))
        
        deposits = query.order_by(Order.created_at.desc()).offset(offset).limit(limit).all()
        
        msg = f"📜 <b>Deposit Transaction History (Page {page}/{total_pages}):</b>\n\n"
        for d in deposits:
            status_emoji = "🟢" if d.status == "Completed" else "🟡" if d.status == "Pending Verification" else "🔴"
            utr_lbl = f"UTR: {d.transaction_id}" if d.transaction_id else "No UTR"
            date_str = d.created_at.strftime("%Y-%m-%d %H:%M") if d.created_at else "—"
            msg += f"{status_emoji} <code>{d.id}</code> — <b>₹{d.total_payable:.2f}</b> ({d.status})\n  {utr_lbl} | {date_str}\n\n"
            
        if not deposits:
            msg += "No deposit requests found.\n"
            
        buttons = []
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"admin_deposit_history_page_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"admin_deposit_history_page_{page+1}"})
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

    elif data == "admin_refresh_stats":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
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
        pending_orders_count = db.query(Order).filter(Order.status.in_(["Paid", "Pending Payment", "Order Processing"]), ~Order.id.like("TOPUP-%")).count()
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
                    {"text": "👥 Manage Users", "callback_data": "admin_manage_users"}
                ],
                [
                    {"text": "🎟️ Manage Promo Codes", "callback_data": "admin_promo_menu"},
                    {"text": "⚙️ System Config", "callback_data": "admin_sys_config"}
                ],
                [
                    {"text": "📊 Reports & Backup", "callback_data": "admin_reports_menu"},
                    {"text": "🏦 Payment Management", "callback_data": "admin_payment_management"}
                ],
                [
                    {"text": "⚠️ View Error Logs", "callback_data": "admin_view_error_logs"}
                ]
            ]
        }
        await edit_bot_message(user.telegram_id, message_id, admin_dashboard_text, reply_markup=admin_inline_markup)
        await answer_callback_query(callback_query_id, "Stats Refreshed!")
        return

    elif data == "admin_sys_config":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        maint_cfg = db.query(SystemConfig).filter(SystemConfig.key == "maintenance_mode").first()
        
        upi_id = upi_id_cfg.value if upi_id_cfg else "dominos@upi"
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
                self.cell(0, 5, f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Page {self.page_no()}", align="C", ln=True)

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
        
        for u in users[:50]:
            pdf.add_page()
            
            pdf.set_font("Helvetica", "B", 13)
            pdf.set_text_color(24, 38, 86)
            pdf.cell(0, 8, f"User Profile: {(u.display_name or 'N/A').encode('latin-1', 'replace').decode('latin-1')}", ln=True)
            pdf.set_font("Helvetica", "", 10)
            pdf.set_text_color(50, 50, 50)
            pdf.cell(0, 6, f"Telegram ID: {u.telegram_id}  |  Username: @{u.username or 'N/A'}", ln=True)
            pdf.cell(0, 6, f"Phone: {u.phone or 'N/A'}  |  Role: {u.role.upper()}", ln=True)
            pdf.cell(0, 6, f"Current Wallet Balance: INR {u.wallet_balance:.2f}  |  Status: {'Blocked' if u.is_blocked else 'Active'}", ln=True)
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
                    pdf.cell(40, 6, str(o.id), 1)
                    pdf.cell(45, 6, o.created_at.strftime('%Y-%m-%d %H:%M') if o.created_at else 'N/A', 1)
                    pdf.cell(30, 6, f"INR {o.total_payable:.2f}", 1, 0, "R")
                    pdf.cell(30, 6, str(o.payment_method).upper(), 1, 0, "C")
                    pdf.cell(35, 6, str(o.status), 1, 1, "C")
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
                    pdf.cell(45, 6, tx.created_at.strftime('%Y-%m-%d %H:%M') if tx.created_at else 'N/A', 1)
                    pdf.cell(30, 6, str(tx.type).upper(), 1, 0, "C")
                    pdf.cell(35, 6, f"INR {tx.amount:.2f}", 1, 0, "R")
                    desc_safe = (tx.description or 'N/A')[:40].encode('latin-1', 'replace').decode('latin-1')
                    pdf.cell(70, 6, desc_safe, 1, 1, "L")
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
        logs = db.query(ErrorLog).order_by(ErrorLog.created_at.desc()).limit(5).all()
        if not logs:
            await edit_bot_message(user.telegram_id, message_id, "🟢 <b>No system error logs found!</b>", reply_markup={"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "admin_refresh_stats"}]]})
            await answer_callback_query(callback_query_id)
            return
            
        msg = "⚠️ <b>Recent System Error Logs:</b>\n\n"
        for l in logs:
            date_str = l.created_at.strftime("%Y-%m-%d %H:%M") if l.created_at else "—"
            msg += f"• [{date_str}] [Type: <b>{l.type}</b>]\n<code>{l.message[:200]}</code>\n\n"
            
        buttons = [[{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}]]
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
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
                    {"text": "⚠️ View Error Logs", "callback_data": "admin_view_error_logs"}
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

    elif data.startswith("admin_change_status_menu_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        order_id = data.replace("admin_change_status_menu_", "").strip()
        
        msg = f"🔄 <b>Change Status for Order: {order_id}</b>\n\nSelect the new status below:"
        statuses = ["Accepted", "Order Processing", "Placed", "Preparing", "Out for Delivery", "Delivered", "Completed", "Cancelled"]
        buttons = []
        row = []
        for s in statuses:
            row.append({"text": s, "callback_data": f"admin_set_status_{order_id}_{s}"})
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
        new_status = "_".join(parts[1:]).strip()
        
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
        h = OrderStatusHistory(
            order_id=order.id,
            status=new_status,
            note=f"Status set manually by admin: {user.username or 'admin'}"
        )
        db.add(h)
        
        # Process refund if transitioning to Cancelled or Refunded
        if new_status in ("Cancelled", "Refunded") and old_status not in ("Cancelled", "Refunded"):
            customer = db.query(User).filter(User.id == order.user_id).first()
            if customer and order.payment_method in ("wallet", "upi"):
                customer.wallet_balance += order.total_payable
                refund_tx = WalletTransaction(
                    user_id=customer.id,
                    type="refund",
                    amount=order.total_payable,
                    description=f"Refund for {new_status.lower()} order #{order.id[:8]}"
                )
                db.add(refund_tx)
        
        db.commit()
        
        await answer_callback_query(callback_query_id, f"Status updated to {new_status}!")
        
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
        pending_orders = db.query(Order).filter(Order.status.in_(["Paid", "Pending Payment", "Order Processing"])).order_by(Order.created_at.desc()).limit(5).all()
        if not pending_orders:
            await edit_bot_message(user.telegram_id, message_id, "📦 <b>No pending orders found!</b>", reply_markup={"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "admin_refresh_stats"}]]})
            await answer_callback_query(callback_query_id)
            return

        msg = "📦 <b>Pending Orders Control Panel:</b>\n\n"
        buttons = []
        for o in pending_orders:
            msg += f"• <code>{o.id}</code> — ₹{o.total_payable:.2f} ({o.status})\n"
            buttons.append([
                {"text": f"✅ Complete {o.id[:6]}", "callback_data": f"admin_act_complete_{o.id}"},
                {"text": f"❌ Reject {o.id[:6]}", "callback_data": f"admin_act_reject_{o.id}"}
            ])
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
        
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await delete_bot_message(user.telegram_id, message_id)
        await send_bot_message(
            user.telegram_id,
            f"✅ <b>Complete Order: {order.id}</b>\n\n"
            f"Please enter/type the <b>Domino's Reference Number</b> (e.g. <code>DOM-123456</code>) or type <code>None</code> if no reference:",
            reply_markup=cancel_keyboard
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
        pending_orders = db.query(Order).filter(Order.status.in_(["Paid", "Pending Payment", "Order Processing"])).order_by(Order.created_at.desc()).limit(5).all()
        msg = "📦 <b>Pending Orders Control Panel:</b>\n\n"
        buttons = []
        for o in pending_orders:
            msg += f"• <code>{o.id}</code> — ₹{o.total_payable:.2f} ({o.status})\n"
            buttons.append([
                {"text": f"✅ Complete {o.id[:6]}", "callback_data": f"admin_act_complete_{o.id}"},
                {"text": f"❌ Reject {o.id[:6]}", "callback_data": f"admin_act_reject_{o.id}"}
            ])
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
        order.status = "Cancelled"
        h = OrderStatusHistory(order_id=order.id, status="Cancelled", note="Rejected manually via Telegram Bot admin panel")
        db.add(h)
        
        refunded = False
        if order.payment_method == "wallet":
            order.user.wallet_balance += order.total_payable
            tx = WalletTransaction(
                user_id=order.user.id,
                type="refund",
                amount=order.total_payable,
                description=f"Order Rejected/Cancelled Refund: {order.id}"
            )
            db.add(tx)
            refunded = True
            
        db.commit()
        
        customer_msg = f"❌ <b>Order Rejected/Cancelled:</b>\n\nYour order <code>{order.id}</code> has been rejected/cancelled by the admin."
        if refunded:
            customer_msg += f"\n\n💸 <b>Refund Credited!</b>\n<b>₹{order.total_payable:.2f}</b> has been credited back to your wallet balance. New Balance: <b>₹{order.user.wallet_balance:.2f}</b>"
        else:
            customer_msg += f"\n\nℹ️ Since you paid via UPI, support will verify and process your refund manually."
            
        await send_bot_message(order.user.telegram_id, customer_msg)
        await answer_callback_query(callback_query_id, "Order Rejected & Refunded!")
        
        # Refresh pending list
        pending_orders = db.query(Order).filter(Order.status.in_(["Paid", "Pending Payment", "Order Processing"])).order_by(Order.created_at.desc()).limit(5).all()
        msg = "📦 <b>Pending Orders Control Panel:</b>\n\n"
        buttons = []
        for o in pending_orders:
            msg += f"• <code>{o.id}</code> — ₹{o.total_payable:.2f} ({o.status})\n"
            buttons.append([
                {"text": f"✅ Complete {o.id[:6]}", "callback_data": f"admin_act_complete_{o.id}"},
                {"text": f"❌ Reject {o.id[:6]}", "callback_data": f"admin_act_reject_{o.id}"}
            ])
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        return

    elif data == "admin_view_pending_deposits":
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        pending_deps = db.query(Order).filter(Order.id.like("TOPUP-%"), Order.status == "Pending Verification").order_by(Order.created_at.desc()).limit(5).all()
        if not pending_deps:
            await edit_bot_message(user.telegram_id, message_id, "🏦 <b>No pending deposit requests found!</b>", reply_markup={"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "admin_refresh_stats"}]]})
            await answer_callback_query(callback_query_id)
            return

        msg = "🏦 <b>Pending Deposit Requests Control Panel:</b>\n\n"
        buttons = []
        for o in pending_deps:
            utr_lbl = f"UTR: {o.transaction_id}" if o.transaction_id else "No UTR"
            msg += f"• <code>{o.id}</code> — ₹{o.total_payable:.2f} ({utr_lbl})\n"
            buttons.append([
                {"text": f"✅ Approve {o.id.replace('TOPUP-', '')}", "callback_data": f"admin_dep_approve_{o.id}"},
                {"text": f"❌ Reject {o.id.replace('TOPUP-', '')}", "callback_data": f"admin_dep_reject_{o.id}"}
            ])
        buttons.append([{"text": "🔙 Back to Control Center", "callback_data": "admin_refresh_stats"}])
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
            await answer_callback_query(callback_query_id, "Deposit not found!")
            return
        if order.status == "Completed":
            await answer_callback_query(callback_query_id, "Already approved!")
            return
            
        target_user = order.user
        target_user.wallet_balance += order.total_payable
        order.status = "Completed"
        
        attempt = db.query(UTRAttempt).filter(UTRAttempt.order_id == order.id).first()
        if attempt:
            attempt.is_successful = True
            
        # Create WalletTransaction
        tx = WalletTransaction(
            user_id=target_user.id,
            type="deposit",
            amount=order.total_payable,
            description=f"Deposit via UTR: {order.transaction_id or 'None'}"
        )
        db.add(tx)
        
        h1 = OrderStatusHistory(order_id=order.id, status="Manual Payment Approved")
        db.add(h1)
        h2 = OrderStatusHistory(order_id=order.id, status="Completed")
        db.add(h2)
        
        # Log to OrderNote
        admin_info = f"@{user.username} ({user.telegram_id})" if user.username else f"{user.display_name} ({user.telegram_id})"
        note = OrderNote(
            order_id=order.id,
            admin_username=user.username or user.display_name or "admin",
            note=f"Deposit approved by admin: {admin_info}"
        )
        db.add(note)
        
        admin_username = username or first_name or "Admin"
        audit = AuditLog(admin_id=user.id, action="WALLET_TOPUP_APPROVED", details=json.dumps({
            "order_id": order.id,
            "utr": order.transaction_id,
            "amount": order.total_payable,
            "user_id": target_user.id,
            "admin": admin_username
        }))
        db.add(audit)
        db.commit()
        
        # Notify user via bot
        success_text = (
            f"💳 <b>Wallet Top-up Approved!</b>\n\n"
            f"We verified your payment of <b>₹{order.total_payable:.2f}</b> (Ref: <code>{order.id}</code>).\n\n"
            f"💰 <b>Your New Wallet Balance:</b> <b>₹{target_user.wallet_balance:.2f}</b>"
        )
        await send_bot_message(target_user.telegram_id, success_text)
        await answer_callback_query(callback_query_id, "Deposit Approved!")
        
        # Update admin message
        approved_text = (
            f"✅ <b>Deposit Request Approved!</b>\n\n"
            f"👤 <b>User:</b> {target_user.display_name} (ID: {target_user.telegram_id})\n"
            f"💰 <b>Amount:</b> ₹{order.total_payable:.2f}\n"
            f"🆔 <b>Ref ID:</b> <code>{order.id}</code>\n"
            f"🔢 <b>UTR:</b> <code>{order.transaction_id or 'None'}</code>\n\n"
            f"Processed by: <b>@{admin_username}</b>"
        )
        # Render back to control center
        await edit_bot_message(user.telegram_id, message_id, approved_text, reply_markup={"inline_keyboard": [[{"text": "🔙 Back", "callback_data": "admin_refresh_stats"}]]})
        
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
        if order.status == "Rejected" or order.status == "Cancelled":
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
            f"Current Expiry: <code>{target_user.admin_expires_at.strftime('%Y-%m-%d %H:%M UTC') if target_user.admin_expires_at else 'Permanent / N/A'}</code>\n\n"
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

    elif data.startswith("admin_user_txs_page_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        parts = data.replace("admin_user_txs_page_", "").split("_")
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
            date_str = t.created_at.strftime("%Y-%m-%d %H:%M") if t.created_at else "—"
            msg += f"• [{date_str}] [Type: <b>{t.type.upper()}</b>]\n  Amount: <b>{t_sign}₹{t.amount:.2f}</b>{desc}\n\n"
            
        if not txs:
            msg += "No transactions found for this user.\n"
            
        buttons = []
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"admin_user_txs_page_{user_id}_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"admin_user_txs_page_{user_id}_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
        buttons.append([{"text": "🔙 Back to User details", "callback_data": f"admin_user_detail_{user_id}"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_user_orders_page_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        parts = data.replace("admin_user_orders_page_", "").split("_")
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
            date_str = o.created_at.strftime("%Y-%m-%d %H:%M") if o.created_at else "—"
            msg += f"{status_emoji} Order: <code>{o.id}</code>\n  Amount: <b>₹{o.total_payable:.2f}</b> • Status: <code>{o.status}</code> • [{date_str}]\n\n"
            buttons.append([{"text": f"⚙️ Manage {o.id[:12]}...", "callback_data": f"admin_view_order_{o.id}"}])
            
        if not orders:
            msg += "No orders found for this user.\n"
            
        nav_row = []
        if page > 1:
            nav_row.append({"text": "⬅️ Prev", "callback_data": f"admin_user_orders_page_{user_id}_{page-1}"})
        if page < total_pages:
            nav_row.append({"text": "Next ➡️", "callback_data": f"admin_user_orders_page_{user_id}_{page+1}"})
        if nav_row:
            buttons.append(nav_row)
        buttons.append([{"text": "🔙 Back to User details", "callback_data": f"admin_user_detail_{user_id}"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_user_addresses_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        user_id = data.replace("admin_user_addresses_", "").strip()
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
            buttons.append([{"text": f"🗑️ Delete {addr.label}", "callback_data": f"admin_user_addr_del_{addr.id}_{user_id}"}])
            
        if not addresses:
            msg += "No saved addresses found for this user.\n"
            
        buttons.append([{"text": "🔙 Back to User details", "callback_data": f"admin_user_detail_{user_id}"}])
        
        await edit_bot_message(user.telegram_id, message_id, msg, reply_markup={"inline_keyboard": buttons})
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("admin_user_addr_del_"):
        if not is_admin:
            await answer_callback_query(callback_query_id, "Unauthorized!")
            return
        parts = data.replace("admin_user_addr_del_", "").split("_")
        address_id = parts[0]
        user_id = parts[1]
        
        addr = db.query(SavedAddress).filter(SavedAddress.id == address_id).first()
        if addr:
            db.delete(addr)
            db.commit()
            await answer_callback_query(callback_query_id, "Address deleted successfully!")
        else:
            await answer_callback_query(callback_query_id, "Address not found!")
            
        # Re-render list
        addresses = db.query(SavedAddress).filter(SavedAddress.user_id == user_id).all()
        target_user = db.query(User).filter(User.id == user_id).first()
        msg = f"📍 <b>Saved Delivery Addresses for {target_user.display_name}:</b>\n\n"
        buttons = []
        for a in addresses:
            def_badge = " [DEFAULT]" if a.is_default else ""
            msg += f"🏠 <b>{a.label.upper()}{def_badge}</b>\n  Address: <i>{a.full_address}</i>\n  Landmark: <code>{a.landmark or '—'}</code>\n\n"
            buttons.append([{"text": f"🗑️ Delete {a.label}", "callback_data": f"admin_user_addr_del_{a.id}_{user_id}"}])
            
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
        upi_id = upi_id_cfg.value if upi_id_cfg else "dominos@upi"
        upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        
        upi_details = generate_upi_qr_details(upi_id, upi_name, amount, order_id, f"Deposit {order_id}")
        upi_uri = upi_details["upi_uri"]
        qr_url = upi_details["qr_code_url"]
        qr_data_url = upi_details.get("qr_data_url", "")
        
        payment_text = (
            f"💳 <b>Deposit Payment Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>Ref ID:</b> <code>{order_id}</code>\n"
            f"• <b>Amount:</b> <b>₹{amount:.2f}</b>\n\n"
            f"👉 <a href=\"{upi_uri}\"><b>📱 Click Here to Pay via UPI App</b></a> (mobile) or scan the QR code above.\n\n"
            f"After completing the UPI payment, tap <b>✅ I Have Paid</b> below to submit your request for admin verification."
        )
        
        payment_markup = {
            "inline_keyboard": [
                [{"text": "✅ I Have Paid", "callback_data": f"wallet_marked_paid_{order_id}"}],
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
            
        session["state"] = f"waiting_for_utr_{order_id}"
        await edit_bot_message(
            user.telegram_id,
            message_id,
            f"✍️ <b>Payment Re-submission</b>\n\n"
            f"Please type your reference number for Ref ID: <code>{order_id}</code> now:"
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("wallet_marked_paid_"):
        order_id = data.replace("wallet_marked_paid_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        order.status = "Pending Verification"
        db.commit()
        
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
                    {"text": "✅ Approve", "callback_data": f"admin_dep_approve_{order.id}"},
                    {"text": "❌ Reject", "callback_data": f"admin_dep_reject_{order.id}"}
                ]
            ]
        }
        await notify_admins(db, admin_text, reply_markup=admin_markup)
        
        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "order_update", "order_id": order_id, "status": "Pending Verification"})
            except Exception:
                pass

    elif data.startswith("wallet_cancel_deposit_"):
        order_id = data.replace("wallet_cancel_deposit_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if order:
            order.status = "Cancelled"
            db.commit()
            
        wallet_text = (
            "❌ <b>Deposit Cancelled</b>\n\n"
            "Your deposit request was cancelled successfully."
        )
        await edit_bot_message(user.telegram_id, message_id, wallet_text, {
            "inline_keyboard": [[{"text": "💰 Wallet Menu", "callback_data": "wallet_view"}]]
        })
        await answer_callback_query(callback_query_id, "Cancelled!")

    elif data.startswith("reenter_utr_"):
        order_id = data.replace("reenter_utr_", "").strip()
        session["state"] = f"waiting_for_utr_{order_id}"
        await send_bot_message(
            user.telegram_id,
            f"✍️ <b>Re-enter Payment UTR Number</b>\n\n"
            f"Order Ref ID: <code>{order_id}</code>\n"
            f"Please reply to this chat with your <b>12-digit UPI UTR number</b> from your UPI app statement.",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        await answer_callback_query(callback_query_id)

    elif data.startswith("cancel_rejected_order_"):
        order_id = data.replace("cancel_rejected_order_", "").strip()
        order = db.query(Order).filter(Order.id == order_id).first()
        if order:
            order.status = "Cancelled"
            h = OrderStatusHistory(order_id=order.id, status="Cancelled by User")
            db.add(h)
            db.commit()
            
        session["state"] = None
        session["checkout_pending"] = False
        session["temp_address"] = None
        session["temp_phone"] = None

        cancel_text = (
            f"❌ <b>Order Cancelled</b>\n\n"
            f"Order <code>{order_id}</code> has been cancelled. No charges were made to your wallet."
        )
        await edit_bot_message(user.telegram_id, message_id, cancel_text, {
            "inline_keyboard": [
                [{"text": "🍕 View Menu", "callback_data": "menu_view"}],
                [{"text": "💰 My Wallet", "callback_data": "wallet_view"}]
            ]
        })
        await answer_callback_query(callback_query_id, "Order cancelled!")

    elif data == "wallet_cancel_deposit_unconfirmed":
        cancel_text = (
            "❌ <b>Deposit Request Cancelled</b>\n\n"
            "The deposit request has been cancelled successfully. No funds have been added."
        )
        await edit_bot_message(user.telegram_id, message_id, cancel_text, {
            "inline_keyboard": [[{"text": "💰 Wallet Menu", "callback_data": "wallet_view"}]]
        })
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
        
        # Refund wallet if paid by wallet
        if order.payment_method == "wallet":
            order.user.wallet_balance += order.total_payable
            tx = WalletTransaction(
                user_id=user.id,
                type="refund",
                amount=order.total_payable,
                description=f"User-cancelled order refund: {order_id}"
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
            f"<i>Cancelled within 2-minute window. {'Wallet refunded automatically.' if order.payment_method == 'wallet' else 'No wallet charge.'}</i>"
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

    elif data.startswith("track_refresh_"):
        order_id = data[len("track_refresh_"):]
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        import datetime as _tdt
        _ist_off = _tdt.timedelta(hours=5, minutes=30)
        history = db.query(OrderStatusHistory).filter(OrderStatusHistory.order_id == order_id).order_by(OrderStatusHistory.created_at.desc()).first()
        current_status = history.status if history else order.status
        _ist_placed = (order.created_at + _ist_off).strftime("%d %b %Y, %I:%M %p IST")
        _ist_now = (_tdt.datetime.utcnow() + _ist_off).strftime("%d %b %Y, %I:%M %p IST")
        
        # Build rider/store info if available
        extra_info = ""
        if order.dominos_reference:
            extra_info += f"• <b>Domino's Ref ID:</b> <code>{order.dominos_reference}</code>\n"
        if order.rider:
            extra_info += f"• <b>Rider:</b> {order.rider.rider_name}"
            if order.rider.rider_phone:
                extra_info += f" \u00b7 {order.rider.rider_phone}"
            extra_info += "\n"
        if order.sector_store:
            extra_info += f"• <b>Store:</b> {order.sector_store}\n"
        
        track_text = (
            f"📦 <b>Order Status:</b>\n\n"
            f"• <b>Order ID:</b> <code>{order.id}</code>\n"
            f"• <b>Status:</b> <b>{current_status}</b>\n"
            f"• <b>Total:</b> ₹{order.total_payable:.2f}\n"
            f"• <b>Placed At:</b> {_ist_placed}\n"
            + extra_info +
            f"\n🕒 <i>Refreshed at {_ist_now}</i>"
        )
        
        refresh_markup = {"inline_keyboard": [[{"text": "🔄 Refresh Status", "callback_data": f"track_refresh_{order_id}"}]]}
        await edit_bot_message(user.telegram_id, message_id, track_text, refresh_markup)
        await answer_callback_query(callback_query_id, "Status updated!")
        
    elif data == "order_confirm_place":
        if session.get("placing_order"):
            await answer_callback_query(callback_query_id, "⚠️ Order is already processing. Please wait.")
            return
        session["placing_order"] = True
        
        order_id = f"BOT-{uuid.uuid4().hex[:8].upper()}"
        txn_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
        
        cart = session.get("cart", {})
        address = html_escape(session.get("temp_address"))
        phone = html_escape(session.get("temp_phone"))
        
        if not cart or not address or not phone:
            session["placing_order"] = False
            await answer_callback_query(callback_query_id, "Error: Session expired or invalid order details.")
            session["state"] = None
            return
            
        # Calculate pricing based on location multiplier
        multiplier = 1.0
        delivery_charge = 30.0
        if user.city:
            loc = db.query(LocationPricing).filter(LocationPricing.city.ilike(user.city)).first()
            if loc:
                multiplier = loc.price_multiplier
                delivery_charge = loc.delivery_charge

        active_deal = session.get("active_deal")
        if active_deal:
            subtotal = session.get("deal_price", 0.0)
        else:
            subtotal = 0.0
            for product_id_str, qty in cart.items():
                product_id = product_id_str  # Product.id is a UUID string
                p = db.query(Product).filter(Product.id == product_id).first()
                if p:
                    price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
                    subtotal += (price * qty)
                
        # Fetch bot service fee
        bot_fee = get_bot_fee(db)
        total_payable = subtotal + bot_fee
        discount = 0.0
        coupon = ""
        delivery_charge = bot_fee
        
        if user.wallet_balance < total_payable:
            await answer_callback_query(callback_query_id, "Insufficient Wallet Balance!")
            await edit_bot_message(
                user.telegram_id,
                message_id,
                f"❌ <b>Insufficient Wallet Balance!</b>\n\nTotal payable is <b>₹{total_payable:.2f}</b>, but you only have <b>₹{user.wallet_balance:.2f}</b>.\n\nPlease top-up in the Order App.",
                reply_markup={
                    "inline_keyboard": [[{"text": "🛒 Back to Cart", "callback_data": "cart_view"}]]
                }
            )
            session["state"] = None
            return
            
        # Deduct balance
        user.wallet_balance -= total_payable
        # Log WalletTransaction
        tx = WalletTransaction(
            user_id=user.id,
            type="payment",
            amount=-total_payable,
            description=f"Paid for order: {order_id}"
        )
        db.add(tx)
        
        # Find gift card
        gift_card = db.query(GiftCard).filter(GiftCard.status == "available").first()
        if not gift_card:
            err = ErrorLog(
                type="giftcard",
                message=f"Gift Card Exhausted! Proceeding with Bot Order {order_id} without pre-allocated card."
            )
            db.add(err)
            db.commit()
            
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
            payment_method="wallet",
            status="Payment Pending",
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
        db.flush() # Flush to get binding for OrderItems
        
        # Save OrderItems to DB
        for product_id_str, qty in cart.items():
            product_id = product_id_str  # Product.id is a UUID string
            p = db.query(Product).filter(Product.id == product_id).first()
            if p:
                price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
                item = OrderItem(
                    order_id=order_id,
                    product_id=product_id,
                    quantity=qty,
                    price=price
                )
                db.add(item)

        # Update user profile with latest details
        user.phone = phone
        
        # Check if address already exists in saved addresses
        exists_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id, SavedAddress.full_address == address).first()
        if not exists_addr:
            # Set other addresses to non-default
            db.query(SavedAddress).filter(SavedAddress.user_id == user.id).update({SavedAddress.is_default: False})
            new_addr = SavedAddress(
                user_id=user.id,
                label="Last Used",
                full_address=address,
                city=user.city,
                state=user.state,
                latitude=user.latitude or 19.0760,
                longitude=user.longitude or 72.8777,
                is_default=True
            )
            db.add(new_addr)

        h1 = OrderStatusHistory(order_id=order_id, status="Payment Received")
        db.add(h1)
        db.commit()
        
        order.status = "Order Processing"
        h3 = OrderStatusHistory(order_id=order_id, status="Order Processing")
        db.add(h3)
        db.commit()
        
        # Notify admins via Telegram Bot
        item_names = []
        for product_id_str, qty in list(cart.items()):
            p = db.query(Product).filter(Product.id == product_id_str).first()
            if p:
                item_names.append(f"• {p.name} x{qty}")
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

        
        action_markup = {
            "inline_keyboard": [
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
        for product_id_str, qty in list(cart.items()):
            p = db.query(Product).filter(Product.id == product_id_str).first()
            if p:
                price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
                item_lines.append(f"  • {p.name} x{qty} — ₹{price * qty:.0f}")
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

async def process_bot_callback_task(telegram_id: str, first_name: str, last_name: str, username: str, data: str, message_id: int, callback_query_id: str):
    """Processes callback query button clicks in a concurrent background task."""
    if check_rate_limit(telegram_id, is_callback=True):
        try:
            await answer_callback_query(callback_query_id, "⚠️ Slow down! Please wait a moment.")
        except Exception:
            pass
        return

    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    user_key = str(telegram_id)
    
    # 0.5s callback cooldown to avoid double clicks
    last_time = USER_LAST_CB_TIME.get(user_key, 0)
    if now - last_time < 0.5:
        await answer_callback_query(callback_query_id, "Please wait...")
        return
    USER_LAST_CB_TIME[user_key] = now

    if user_key not in USER_PROCESSING_LOCKS:
        USER_PROCESSING_LOCKS[user_key] = asyncio.Lock()
        
    async with USER_PROCESSING_LOCKS[user_key]:
        db = SessionLocal()
        try:
            await handle_bot_callback(db, telegram_id, first_name, last_name, username, data, message_id, callback_query_id)
            # Sync session changes to DB (persists bot brain)
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
            # 3. Broadcast to admin SSE Live Feed
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({
                        "type": "error_alert",
                        "message": f"[Bot Callback] {first_name} ({telegram_id}) / {data}: {str(e)}",
                    })
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
        last_warn = USER_LAST_WARNING_TIME.get(user_key, 0)
        if now_ts - last_warn > 5.0:
            USER_LAST_WARNING_TIME[user_key] = now_ts
            try:
                await send_bot_message(
                    telegram_id,
                    "⚠️ <b>Spam Warning</b>\n\nYou are sending messages too fast! Please wait a few seconds before trying again."
                )
            except Exception:
                pass
        return

    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    
    # 1.0s message cooldown to avoid message spam
    last_time = USER_LAST_MSG_TIME.get(user_key, 0)
    if now - last_time < 1.0:
        logger.warning(f"[RATE LIMIT] Ignoring message from {telegram_id} (too fast)")
        return
    USER_LAST_MSG_TIME[user_key] = now

    if user_key not in USER_PROCESSING_LOCKS:
        USER_PROCESSING_LOCKS[user_key] = asyncio.Lock()
        
    async with USER_PROCESSING_LOCKS[user_key]:
        db = SessionLocal()
        try:
            await handle_bot_message(db, telegram_id, first_name, last_name, username, text, location, message_id, photo=photo, document=document)
            # Sync session changes to DB (persists bot brain)
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
            # 3. Broadcast to admin SSE Live Feed
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({
                        "type": "error_alert",
                        "message": f"[Bot Message] {first_name} ({telegram_id}) / {repr(text[:60])}: {str(e)}",
                    })
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
                            prev_task = USER_CALLBACK_TASKS.get(user_key)
                            if prev_task and not prev_task.done():
                                prev_task.cancel()
                                logger.info(f"[Bot Callback] Cancelled obsolete callback task for user {user_key}")
                            task = asyncio.create_task(process_bot_callback_task(telegram_id, first_name, last_name, username, cb_data, message_id, callback_query_id))
                            USER_CALLBACK_TASKS[user_key] = task
            elif resp.status_code == 409:
                # Another bot instance is already polling — wait longer and retry
                logger.warning("[BOT] 409 Conflict: another instance is polling. Waiting 15s before retry...")
                await asyncio.sleep(15)
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
