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
from sqlalchemy.orm import Session
from .database import SessionLocal, User, SupportMessage, ErrorLog, SystemConfig, Product, Order, OrderItem, OrderStatusHistory, GiftCard, AuditLog, LocationPricing, Notification, UTRAttempt, SavedAddress, Coupon, CouponRedemption, WalletTransaction
from .utils import encrypt_data, decrypt_data, generate_upi_qr_details
import logging

logger = logging.getLogger(__name__)

# Rate limit and concurrency protection maps
USER_LAST_MSG_TIME = {}
USER_LAST_CB_TIME = {}
USER_PROCESSING_LOCKS = {}
USER_CALLBACK_TASKS = {}


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


async def answer_callback_query(callback_query_id: str, text: str = None) -> bool:
    """Dismisses the loading spinner icon on the Telegram client button."""
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        return True
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    try:
        await _fast_client.post(url, json=payload)
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
    return success


def render_wallet_view(db: Session, user: User, offset: int = 0, limit: int = 5):
    """Renders the My Wallet overview and recent transaction history combining WalletTransactions and Orders."""
    txs = db.query(WalletTransaction).filter(WalletTransaction.user_id == user.id).order_by(WalletTransaction.created_at.desc()).all()
    orders = db.query(Order).filter(Order.user_id == user.id).order_by(Order.created_at.desc()).all()
    
    tx_lines = []
    # Combine WalletTransaction entries
    for t in txs:
        sign = "+" if t.amount >= 0 else ""
        icon = "✅" if t.amount >= 0 else "🛒"
        date_str = t.created_at.strftime("%d %b, %H:%M")
        tx_lines.append(f"{icon} <b>{sign}₹{abs(t.amount):.2f}</b> — {t.type.upper().replace('_', ' ')}\n  └ <i>{date_str} | {t.description or ''}</i>")

    # Also combine Top-up orders if any
    for o in orders:
        if o.id.startswith("TOPUP-"):
            status_icon = "✅" if o.status in ["Completed", "Verified"] else ("⏳" if "Pending" in o.status else "❌")
            date_str = o.created_at.strftime("%d %b, %H:%M")
            tx_lines.append(f"{status_icon} <b>+₹{o.total_payable:.2f}</b> — Top-Up Order (<code>{o.id}</code>)\n  └ <i>{date_str} | Status: {o.status}</i>")

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
    Supports either a public URL or a local file path.
    Falls back to send_bot_message on failure.
    """
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.debug(f"[MOCK BOT PHOTO] To {telegram_id}: Photo: {photo_url}, Caption: {caption}, ReplyMarkup: {reply_markup}")
        return True
        
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    
    import os
    is_local_file = os.path.exists(photo_url)
    
    try:
        if is_local_file:
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
                
            with open(photo_url, "rb") as f:
                files = {
                    "photo": f
                }
                resp = await _http_client.post(url, data=data, files=files, timeout=20.0)
        else:
            # Send as json url link
            payload = {
                "chat_id": telegram_id,
                "photo": photo_url
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
    """Reverse geocode latitude and longitude to get city name using OpenStreetMap Nominatim."""
    url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}"
    headers = {"User-Agent": "DominosOrderEngineBot/2.0"}
    try:
        resp = await _http_client.get(url, headers=headers, timeout=8.0)
        if resp.status_code == 200:
            data = resp.json()
            address = data.get("address", {})
            # Prioritize local city details - do NOT fall back to state name (e.g. Punjab)
            city_val = (
                address.get("city") 
                or address.get("town") 
                or address.get("village") 
                or address.get("hamlet") 
                or address.get("suburb") 
                or address.get("municipality") 
                or address.get("city_district") 
                or address.get("county") 
                or address.get("state_district")
            )
            if city_val:
                city_name = str(city_val).strip()
                norm = city_name.lower()
                if "bengaluru" in norm or "bangalore" in norm:
                    return "Bangalore"
                if "mumbai" in norm or "bombay" in norm:
                    return "Mumbai"
                if "delhi" in norm:
                    return "Delhi"
                if "kolkata" in norm or "calcutta" in norm:
                    return "Kolkata"
                if "chennai" in norm or "madras" in norm:
                    return "Chennai"
                if "hyderabad" in norm:
                    return "Hyderabad"
                if "pune" in norm or "poona" in norm:
                    return "Pune"
                if "ahmedabad" in norm:
                    return "Ahmedabad"
                if "jaipur" in norm:
                    return "Jaipur"
                if "surat" in norm:
                    return "Surat"
                return city_name
    except Exception as e:
        logger.error(f"Error reverse geocoding ({lat}, {lon}): {e}")
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
        return "⏳ <code>[▒░░░░░░░░░] 10%</code> — Payment Verification"
    elif "received" in status_lower or "placed" in status_lower:
        return "✅ <code>[▓░░░░░░░░░] 20%</code> — Payment Confirmed"
    elif "processing" in status_lower or "baking" in status_lower:
        return "🍕 <code>[▓▓▓▓░░░░░░] 40%</code> — Baking your Pizza in Oven"
    elif "kitchen" in status_lower or "preparing" in status_lower:
        return "👨‍🍳 <code>[▓▓▓▓▓▓░░░░] 60%</code> — Packaging & Quality Checks"
    elif "delivery" in status_lower or "out" in status_lower or "route" in status_lower:
        return "🛵 <code>[▓▓▓▓▓▓▓▓░░] 80%</code> — Out for Delivery with Rider"
    elif "delivered" in status_lower or "complete" in status_lower or "success" in status_lower:
        return "🎉 <code>[▓▓▓▓▓▓▓▓▓▓] 100%</code> — Delivered! Enjoy! 🍕"
    elif "cancel" in status_lower or "fail" in status_lower:
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


async def display_pizza_menu(db: Session, user: User, reply_markup: dict, page: int = 1, category: str = "All", edit_message_id: int = None):
    """Displays the menu containing all pizzas as text (complying with no images in chat) with pagination and category support."""
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
        query = query.filter(Product.category.ilike(category))
    products = query.order_by(Product.original_price.asc()).all()

    if not products:
        text = f"🍽️ No active items found in category '{category}'."
        if edit_message_id:
            await edit_bot_message(user.telegram_id, edit_message_id, text, reply_markup={"inline_keyboard": [
                [{"text": "⭐ All Categories", "callback_data": "menu_category_All"}],
                [{"text": "🍕 View Menu", "callback_data": "menu_view"}]
            ]})
        else:
            await send_bot_message(user.telegram_id, text, reply_markup=reply_markup)
        return

    # Generate mappings to get 1-based display codes
    code_to_id, id_to_code = get_product_mappings(db)

    # Pagination: 4 items per page
    items_per_page = 4
    total_pages = (len(products) + items_per_page - 1) // items_per_page
    page = max(1, min(page, total_pages))
    
    start_idx = (page - 1) * items_per_page
    page_products = products[start_idx:start_idx + items_per_page]

    # Compose unified menu text
    menu_lines = [
        f"🍕 <b>Domino's Order Engine Menu (Page {page}/{total_pages})</b>",
        f"📍 City: <b>{user.city}</b> (Multiplier: {multiplier:.2f}x)",
        f"📁 Category: <b>{category}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━\n"
    ]
    for p in page_products:
        price = float(round((p.discounted_price if p.discounted_price is not None else p.original_price) * multiplier))
        veg_indicator = "🟢 Veg" if p.is_veg else "🔴 Non-Veg"
        display_code = id_to_code.get(p.id, p.id)
        menu_lines.append(
            f"🍕 <b>[{display_code}] {p.name}</b> ({veg_indicator}) — <b>₹{price:.2f}</b>\n"
            f"<i>{p.description or 'No description available'}</i>\n"
        )
    
    menu_text = "\n".join(menu_lines)

    # Build inline keyboard buttons in 2-column layout
    grid = []
    row = []
    for p in page_products:
        name_limit = p.name[:20] + "..." if len(p.name) > 23 else p.name
        row.append({"text": f"➕ {name_limit}", "callback_data": f"cart_add_{p.id}"})
        if len(row) == 2:
            grid.append(row)
            row = []
    if row:
        grid.append(row)

    # Pagination row
    nav_row = []
    if page > 1:
        nav_row.append({"text": "⬅️ Prev Page", "callback_data": f"menu_page_{page-1}_{category}"})
    if page < total_pages:
        nav_row.append({"text": "Next Page ➡️", "callback_data": f"menu_page_{page+1}_{category}"})
    if nav_row:
        grid.append(nav_row)

    # Category selection rows
    grid.append([
        {"text": "⭐ All", "callback_data": "menu_category_All"},
        {"text": "🟢 Veg", "callback_data": "menu_category_Veg"},
        {"text": "🔴 Non-Veg", "callback_data": "menu_category_Non-Veg"}
    ])
    grid.append([
        {"text": "🍟 Sides", "callback_data": "menu_category_Sides"},
        {"text": "🥤 Drinks", "callback_data": "menu_category_Drinks"},
        {"text": "🍰 Desserts", "callback_data": "menu_category_Desserts"}
    ])

    app_url = get_mini_app_url(db)
    is_https = app_url.startswith("https://")
    grid.append([{"text": "🛒 View Shopping Cart", "callback_data": "cart_view"}])
    grid.append([{"text": "🍕 Launch Order App (Visual)", "web_app": {"url": app_url}}] if is_https else [{"text": "🍕 View Cart (Inline)", "callback_data": "cart_view"}])

    markup = {"inline_keyboard": grid}

    if edit_message_id:
        await edit_bot_message(user.telegram_id, edit_message_id, menu_text, reply_markup=markup)
    else:
        res = await send_bot_message(user.telegram_id, menu_text, reply_markup=markup)
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
            item_lines.append(f"  \u2022 {p.name} x{qty}" if active_deal else f"  \u2022 {p.name} x{qty} \u2014 \u20b9{price * qty:.0f}")
    items_text = "\n".join(item_lines) if item_lines else "  \u2022 (items unavailable)"

    confirm_text = (
        f"\U0001f4cb <b>Please Confirm Your Order</b>\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"
        f"\U0001f6d2 <b>Items:</b>\n{items_text}\n\n"
        f"\U0001f4cd <b>City:</b> {user.city}\n"
        f"\U0001f3e1 <b>Delivery Address:</b> {address}\n"
        f"\U0001f4f1 <b>Phone:</b> {phone}\n\n"
        f"\U0001f4b0 <b>Price Breakdown:</b>\n"
        f"  Pizza Total:     \u20b9{subtotal:.2f}\n"
        f"  Bot Service Fee: +\u20b9{bot_fee:.2f}\n"
        f"  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n"
        f"  <b>Total Payable: \u20b9{total_payable:.2f}</b>\n\n"
        f"\U0001f4a1 <i>Wallet Balance: \u20b9{user.wallet_balance:.2f}</i>\n\nAre you sure you want to place this order?"
    )
    confirm_markup = {
        "inline_keyboard": [
            [
                {"text": "\u2705 Yes, Confirm & Place", "callback_data": "order_confirm_place"},
                {"text": "\u274c Cancel Order",         "callback_data": "order_cancel_place"}
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

    city = user.city
    saved_addr = db.query(SavedAddress).filter(
        SavedAddress.user_id == user.id, SavedAddress.is_default == True
    ).first()
    if not saved_addr:
        saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()

    latest_order = db.query(Order).filter(
        Order.user_id == user.id
    ).order_by(Order.created_at.desc()).first()

    saved_address = saved_addr.full_address if saved_addr else (latest_order.address if latest_order else None)
    saved_phone   = user.phone if user.phone else (latest_order.phone if latest_order else None)

    has_coords = (user.latitude is not None and user.longitude is not None)
    has_doorstep_address = (saved_address is not None and saved_address != "GPS Location" and len(saved_address.strip()) > 3)
    if city and city != "Not Shared" and not session.get("force_address_entry"):
        if has_doorstep_address and saved_phone and has_coords:
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
            inline.append([{"text": "📝 Update Address / Phone",   "callback_data": "checkout_enter_new"}])
        else:
            if not has_doorstep_address:
                inline.append([{"text": "🏠 Enter Delivery Address",   "callback_data": "checkout_enter_new"}])
            if not has_coords:
                inline.append([{"text": "📍 Share GPS Coordinates",   "callback_data": "checkout_change_location"}])
            if not saved_phone:
                inline.append([{"text": "📱 Enter Phone Number",       "callback_data": "checkout_enter_phone"}])
            
        inline.append([{"text": "📍 Change City / GPS",                "callback_data": "checkout_change_location"}])
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
        # No city at all — prompt to share GPS or type manually
        session["checkout_pending"] = True
        prompt = (
            "\U0001f4cd <b>Location Required for Checkout</b>\n\n"
            "We need your GPS coordinates to place your Domino's order.\n"
            "Please share your GPS location using the button below."
        )
        inline = [
            [{"text": "\U0001f6d2 Back to Cart",          "callback_data": "cart_view"}],
        ]
        loc_keyboard = {
            "keyboard": [
                [{"text": "\U0001f4cd Share Current Location", "request_location": True}],
                [{"text": "\U0001f519 Back"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        await send_bot_message(
            user.telegram_id,
            "\U0001f4cd <b>Share your GPS to auto-detect city for checkout:</b>\n"
            "Or use the button below to enter your address manually.",
            reply_markup=loc_keyboard
        )

    markup = {"inline_keyboard": inline}
    if edit_message_id:
        await edit_bot_message(user.telegram_id, edit_message_id, prompt, reply_markup=markup)
    else:
        await send_bot_message(user.telegram_id, prompt, reply_markup=markup)


async def handle_bot_message(db: Session, telegram_id: str, first_name: str, last_name: str, username: str, text: str, location: dict = None, message_id: int = None):
    """
    Handles an incoming message sent to the Telegram bot with custom keyboards, commands, and looping GIFs.
    """
    # Show typing indicator immediately — makes the bot feel human & responsive
    await send_bot_typing(str(telegram_id))

    global MINI_APP_URL
    MINI_APP_URL = get_mini_app_url(db)

    user = db.query(User).filter(User.telegram_id == str(telegram_id)).first()
    display_name = f"{first_name or ''} {last_name or ''}".strip() or username or f"User_{telegram_id}"
    
    if not user:
        user = User(
            telegram_id=str(telegram_id),
            username=username,
            display_name=display_name,
            wallet_balance=100.0,
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

    # Look up session state (restores from database to preserve "bot brain" on server restarts)
    if str(telegram_id) not in USER_BOT_SESSION:
        import json
        saved_cart = {}
        if user.bot_cart:
            try:
                saved_cart = json.loads(user.bot_cart)
            except Exception:
                pass
        USER_BOT_SESSION[str(telegram_id)] = {
            "state": user.bot_state,
            "cart": saved_cart
        }
    session = USER_BOT_SESSION[str(telegram_id)]

    app_url = get_mini_app_url(db)
    is_https = app_url.startswith("https://")
    order_app_btn = {"text": "🍕 Order App", "web_app": {"url": app_url}} if is_https else {"text": "🍕 Order App (Link)", "url": app_url}
    admin_portal_btn = {"text": "🌐 Open Admin Portal", "web_app": {"url": f"{app_url}/admin/"}} if is_https else {"text": "🌐 Open Admin Portal (Link)", "url": f"{app_url}/admin/"}

    admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
    is_admin = str(telegram_id) == str(admin_tg_id) or (user and user.role == "admin")

    main_keyboard = {
        "keyboard": [
            [{"text": "🍕 View Menu"}, {"text": "💰 My Wallet"}],
            [{"text": "📍 Change Location"}, {"text": "📦 Track Orders"}],
            [{"text": "🎉 Active Offers"}, {"text": "💬 Contact Support"}],
            [order_app_btn]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    if is_admin:
        main_keyboard["keyboard"].append([
            {"text": "🔑 Get Admin Secret Key"},
            admin_portal_btn
        ])

    # Handle Cancel / Back command — clears ALL session state
    if text and text.strip().lower() in ("❌ cancel", "cancel", "🔙 back", "back", "🏠 main menu"):
        session["state"] = None
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

    # Intercept general messages if location is required but not yet shared
    is_testing = (os.getenv("TELEGRAM_BOT_TOKEN") == "MOCK_TOKEN")
    if not is_testing and not user.city and not location:
        if text and (text.startswith("/start") or text.strip().lower() == "❌ skip location"):
            pass
        else:
            location_keyboard = {
                "keyboard": [
                    [{"text": "📍 Share Current Location", "request_location": True}],
                    [{"text": "🔙 Back"}]
                ],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            await send_bot_message(
                user.telegram_id,
                "📍 <b>Location Required:</b> Please click the button below to share your <b>Current Location</b> directly to browse menu pricing for your area.",
                reply_markup=location_keyboard
            )
            return

    # Handle shared Telegram location message
    if location:
        lat = location.get("latitude")
        lon = location.get("longitude")
        city = await reverse_geocode(lat, lon) or "GPS Location"
        
        user.city = city
        user.latitude = lat
        user.longitude = lon
        db.commit()
        
        # Save/update Default SavedAddress in database
        from .database import SavedAddress
        saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
        if not saved_addr:
            saved_addr = SavedAddress(user_id=user.id, label="Home", is_default=True)
            db.add(saved_addr)
        saved_addr.full_address = "GPS Location"
        saved_addr.latitude = lat
        saved_addr.longitude = lon
        saved_addr.city = city
        db.commit()
        session["state"] = None
        session["force_address_entry"] = True
        try:
            from .services.dominos_service import sync_realtime_menu, sync_realtime_menu_bg
            # Fetch store-specific menu and dynamic pricing based on exact GPS coordinates
            if db.query(Product).count() > 5:
                asyncio.create_task(sync_realtime_menu_bg(city, lat=lat, lon=lon))
            else:
                await sync_realtime_menu(city, db, lat=lat, lon=lon)
        except Exception as e:
            logger.error(f"Error syncing menu in bot location handler: {e}")
        
        if session.get("checkout_pending"):
            session["checkout_pending"] = False
            await initiate_checkout(db, user, session)
        else:
            city_disp = f" ({city})" if city and city != "GPS Location" else ""
            await send_bot_message(
                user.telegram_id, 
                f"📍 <b>Location coordinates saved!</b>{city_disp}\nStore pricing and delivery parameters have been updated automatically!", 
                reply_markup=main_keyboard
            )
            await display_pizza_menu(db, user, main_keyboard)
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
        cancel_keyboard = {
            "keyboard": [[{"text": "❌ Cancel"}]],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }
        res = await send_bot_message(
            user.telegram_id,
            "💳 <b>Enter Custom Amount</b>\n\nPlease type the amount in Rupees you would like to add (e.g. 150):",
            reply_markup=cancel_keyboard
        )
        if isinstance(res, int):
            session["last_bot_msg_id"] = res
        return

    elif text_clean == "❌ Cancel" or text_lower.startswith("/cancel"):
        session["state"] = None
        session["temp_address"] = None
        session["temp_phone"] = None
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

    # --- 1.5 Handle waiting_for_utr state ---
    if session.get("state") and session.get("state").startswith("waiting_for_utr_"):
        order_id = session.get("state").replace("waiting_for_utr_", "")
        utr = text_clean
        if not (utr.isdigit() and len(utr) == 12):
            await send_bot_message(user.telegram_id, "❌ Invalid format. Please reply with your <b>12-digit UPI UTR number</b> (digits only).")
            return
            
        # Check duplicate
        dup = db.query(Order).filter(Order.transaction_id == utr).first()
        if dup:
            await send_bot_message(user.telegram_id, "❌ This UTR has already been submitted. Please enter a new UTR.")
            return
            
        order = db.query(Order).filter(Order.id == order_id).first()
        if order:
            order.transaction_id = utr
            order.status = "Pending Verification"
            
            attempt = UTRAttempt(order_id=order.id, utr=utr, is_successful=False)
            db.add(attempt)
            db.commit()
            
            session["state"] = None
            
            await send_bot_message(
                user.telegram_id,
                f"✅ <b>New UTR Received!</b>\n\n"
                f"• Ref ID: <code>{order.id}</code>\n"
                f"• New UTR: <code>{utr}</code>\n\n"
                f"The admin team has been notified and will verify the transaction shortly.",
                reply_markup=main_keyboard
            )
            
            # Notify admin
            admin_text = (
                "🔔 <b>New UTR Submitted (Resubmission)</b>\n\n"
                f"👤 <b>User:</b> {user.display_name} (ID: {user.telegram_id})\n"
                f"💰 <b>Amount:</b> ₹{order.total_payable:.2f}\n"
                f"🔢 <b>UTR Number:</b> <code>{utr}</code>\n"
                f"🆔 <b>Ref ID:</b> <code>{order.id}</code>"
            )
            await notify_admins(db, admin_text)
            
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Pending Verification"})
                except Exception:
                    pass
            return
        else:
            session["state"] = None
            await send_bot_message(user.telegram_id, "❌ Order not found.", reply_markup=main_keyboard)
            return

    # --- 2. Auto-detect 12-digit UTR Code ---
    if text_clean.isdigit() and len(text_clean) == 12:
        utr = text_clean
        
        # Check if UTR is already used
        dup = db.query(Order).filter(Order.transaction_id == utr).first()
        if dup:
            await send_bot_message(user.telegram_id, "❌ This UPI Transaction UTR has already been submitted. Please enter a new UTR.")
            return
            
        # Find the most recent pending UPI topup order for this user
        pending_order = db.query(Order).filter(
            Order.user_id == user.id,
            Order.status == "Pending Payment",
            Order.payment_method == "upi"
        ).order_by(Order.created_at.desc()).first()
        
        if pending_order:
            pending_order.transaction_id = utr
            pending_order.status = "Pending Verification"
            
            attempt = UTRAttempt(order_id=pending_order.id, utr=utr, is_successful=False)
            db.add(attempt)
            db.commit()
            
            success_text = (
                f"✅ <b>UTR Number Received!</b>\n\n"
                f"• Amount: <b>₹{pending_order.total_payable:.2f}</b>\n"
                f"• UTR: <code>{utr}</code>\n"
                f"• Ref ID: <code>{pending_order.id}</code>\n\n"
                f"The admin team will verify and approve the payment shortly."
            )
            await send_bot_message(user.telegram_id, success_text, reply_markup=main_keyboard)
            
            # Send notification to admin
            admin_text = (
                "🔔 <b>UTR Submitted for Verification</b>\n\n"
                f"👤 <b>User:</b> {user.display_name} (ID: {user.telegram_id})\n"
                f"💰 <b>Amount:</b> ₹{pending_order.total_payable:.2f}\n"
                f"🔢 <b>UTR Number:</b> <code>{utr}</code>\n"
                f"🆔 <b>Ref ID:</b> <code>{pending_order.id}</code>\n\n"
                f"Please verify this payment in your admin portal!"
            )
            await notify_admins(db, admin_text)
            
            if sse_broadcast_callback:
                try:
                    await sse_broadcast_callback({"type": "order_update", "order_id": pending_order.id, "status": "Pending Verification"})
                except Exception:
                    pass
            return
        else:
            if session.get("state") == "waiting_for_topup_utr":
                amount = session.get("topup_amount", 100.0)
                session["state"] = None
                
                # Generate unique order id
                import random
                topup_order = Order(
                    id=f"TOPUP-{random.randint(100000, 999999)}",
                    user_id=user.id,
                    original_total=amount,
                    discount=0.0,
                    delivery_charge=0.0,
                    total_payable=amount,
                    status="Pending Verification",
                    payment_method="upi",
                    transaction_id=utr,
                    city=user.city or "Mumbai"
                )
                db.add(topup_order)
                db.flush()
                
                attempt = UTRAttempt(order_id=topup_order.id, utr=utr, is_successful=False)
                db.add(attempt)
                db.commit()
                
                success_text = (
                    f"✅ <b>Top-up Request Submitted!</b>\n\n"
                    f"• Amount: <b>₹{amount:.2f}</b>\n"
                    f"• UTR: <code>{utr}</code>\n"
                    f"• Ref ID: <code>{topup_order.id}</code>\n\n"
                    f"The admin team will verify and approve the payment shortly."
                )
                await send_bot_message(user.telegram_id, success_text, reply_markup=main_keyboard)
                
                # Notify admin
                admin_text = (
                    "🔔 <b>New Deposit Request Raised</b>\n\n"
                    f"👤 <b>User:</b> {user.display_name} (ID: {user.telegram_id})\n"
                    f"💰 <b>Amount:</b> ₹{amount:.2f}\n"
                    f"🔢 <b>UTR Number:</b> <code>{utr}</code>\n"
                    f"🆔 <b>Ref ID:</b> <code>{topup_order.id}</code>\n\n"
                    f"Please verify this payment in your admin portal!"
                )
                await notify_admins(db, admin_text)
                
                if sse_broadcast_callback:
                    try:
                        await sse_broadcast_callback({"type": "order_update"})
                    except Exception:
                        pass
                return

    if session.get("state") == "waiting_for_city":
        city_buttons = [
            "❌ skip location", "🍕 view menu", "💰 my wallet", "📦 track orders",
            "📍 change location", "❌ cancel", "🏠 update delivery address", "📱 update phone number"
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
                    
                    # Save/update Default SavedAddress in database
                    from .database import SavedAddress
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
                    f"📍 <b>City updated to: {city_input}</b>\n\nMenu prices and store settings have been updated for your city!",
                    reply_markup=main_keyboard
                )
                await display_pizza_menu(db, user, main_keyboard)
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
            f"✅ <b>Phone updated to <code>{phone_formatted}</code></b>\n\nThis number will be used for all future orders.",
            reply_markup=main_keyboard
        )
        return

    if session.get("state") == "waiting_for_support_message":
        msg_text = text.strip() if text else ""
        if len(msg_text) < 10:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Message too short.</b>\n\nPlease describe your issue in at least 10 characters so we can help you.",
            )
            return
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
            logger.warning(f"Could not save support message: {db_err}")
        session["state"] = None
        await send_bot_message(
            user.telegram_id,
            "✅ <b>Support message sent!</b>\n\n"
            "Our team has received your message and will reply directly in this chat shortly.\n\n"
            "<i>Your message:</i>\n" + f"<blockquote>{msg_text[:300]}</blockquote>",
            reply_markup=main_keyboard
        )
        return

    if session.get("state") == "waiting_for_promo_code":
        # Code was entered but not valid — the actual validation happens in promo_candidate block above
        await send_bot_message(user.telegram_id, "❌ Invalid or expired promo code. Please double-check the code and try again, or send /start to cancel.")
        return

    if session.get("state") == "waiting_for_address":
        # Validate: address must be at least 8 characters
        addr_stripped = text.strip() if text else ""
        if len(addr_stripped) < 8:
            await send_bot_message(
                user.telegram_id,
                "⚠️ <b>Address too short.</b>\n\n"
                "Please enter your <b>full delivery address</b> including house/flat number, street, and area.\n"
                "<i>Example: Flat 4B, Sunrise Apartments, MG Road, Bengaluru</i>",
                reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
            )
            return
        session["temp_address"] = addr_stripped
        try:
            from .routes import geocode_address
            lat, lon = await geocode_address(addr_stripped)
            if lat is not None and lon is not None:
                user.latitude = lat
                user.longitude = lon
                db.commit()
                
                # Save/update Default SavedAddress in database
                from .database import SavedAddress
                saved_addr = db.query(SavedAddress).filter(SavedAddress.user_id == user.id).first()
                if not saved_addr:
                    saved_addr = SavedAddress(user_id=user.id, label="Home", is_default=True)
                    db.add(saved_addr)
                saved_addr.full_address = addr_stripped
                saved_addr.latitude = lat
                saved_addr.longitude = lon
                saved_addr.city = user.city
                db.commit()
                
                logger.info(f"[Bot Checkout] Geocoded typed address '{addr_stripped}' to: lat={lat}, lon={lon}")
            else:
                await send_bot_message(
                    user.telegram_id,
                    "⚠️ <b>Address Location Resolution Failed:</b>\n\n"
                    "We could not resolve coordinates for your address. Please verify the address text (include city name) and enter it again, or click cancel to share your GPS location instead.",
                    reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
                )
                return
        except Exception as e:
            logger.error(f"[Bot Checkout] Failed to geocode address: {e}")
            await send_bot_message(user.telegram_id, "⚠️ Error resolving location. Please try again.")
            return
        if user.phone:
            session["state"] = None
            await initiate_checkout(db, user, session)
        else:
            session["state"] = "waiting_for_phone"
            phone_keyboard = {
                "keyboard": [[{"text": "❌ Cancel"}]],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            phone_hint = ""
            await send_bot_message(
                user.telegram_id,
                f"📱 <b>Phone Number Required:</b>\n\n"
                f"Please enter your contact number with country code.\n"
                f"Format: <code>+91XXXXXXXXXX</code>{phone_hint}",
                reply_markup=phone_keyboard
            )
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
        # Validate phone format (10 to 15 digits)
        digits_only = "".join(c for c in text if c.isdigit())
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
        session["state"] = "waiting_for_confirm"
        session["force_address_entry"] = False
        
        cart = session.get("cart", {})
        if not cart:
            session["state"] = None
            await send_bot_message(user.telegram_id, "❌ Your cart is empty! Order cancelled.", reply_markup=main_keyboard)
            return
            
        address = session.get("temp_address", "Default Address")
        phone = text
        
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
    admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
    is_admin = str(telegram_id) == str(admin_tg_id) or (user and user.role == "admin")

    # Dynamic keyboard configuration based on HTTPS protocol support
    app_url = get_mini_app_url(db)
    is_https = app_url.startswith("https://")
    order_app_btn = {"text": "🍕 Order App", "web_app": {"url": app_url}} if is_https else {"text": "🍕 Order App (Link)", "url": app_url}

    main_keyboard = {
        "keyboard": [
            [{"text": "🍕 View Menu"}, {"text": "💰 My Wallet"}],
            [{"text": "📍 Change Location"}, {"text": "📦 Track Orders"}],
            [{"text": "🎉 Active Offers"}, {"text": "💬 Contact Support"}],
            [order_app_btn]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    
    if is_admin:
        admin_portal_btn = {"text": "🌐 Open Admin Portal", "web_app": {"url": f"{app_url}/admin/"}} if is_https else {"text": "🌐 Open Admin Portal (Link)", "url": f"{app_url}/admin/"}
        main_keyboard["keyboard"].append([
            {"text": "🔑 Get Admin Secret Key"},
            admin_portal_btn
        ])

    text_lower = text.strip().lower()

    if text_clean.startswith("/admin_msg "):
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
            
        from .database import SupportMessage
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
            from .routes import sse_broadcast_callback
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

    if text_lower.startswith("/admin_dashboard") or text_lower.startswith("/admin"):
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return
        
        admin_portal_btn = {"text": "🌐 Open Admin Portal", "web_app": {"url": f"{app_url}/admin/"}} if is_https else {"text": "🌐 Open Admin Portal (Link)", "url": f"{app_url}/admin/"}
        admin_keyboard = {
            "keyboard": [
                [{"text": "🔑 Get Admin Secret Key"}],
                [admin_portal_btn]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }
        welcome_text = (
            "👑 <b>Admin Dashboard Controls Unlocked!</b>\n\n"
            "Use the keyboard options below to generate a one-time session key or access the Admin Portal directly."
        )
        await send_bot_animation(
            user.telegram_id,
            "https://i.giphy.com/3o7TKSjRrfIPjei1fG.gif",
            caption=welcome_text,
            reply_markup=admin_keyboard
        )
        return

    elif text == "🔑 Get Admin Secret Key" or text.startswith("/secret_key") or text_lower == "🔑 get admin secret key":
        if not is_admin:
            await send_bot_message(user.telegram_id, "❌ <b>Unauthorized!</b> This command is restricted to the administrator.")
            return

        if message_id:
            try:
                await delete_bot_message(user.telegram_id, message_id)
            except Exception as e:
                logger.error(f"[Warning] Failed to delete incoming secret key request: {e}")

        import secrets
        import string
        alphabet = string.ascii_letters + string.digits
        key = "".join(secrets.choice(alphabet) for _ in range(50))

        cfg = db.query(SystemConfig).filter(SystemConfig.key == "admin_session_key").first()
        if not cfg:
            cfg = SystemConfig(key="admin_session_key", value=key)
            db.add(cfg)
        else:
            cfg.value = key
        
        audit = AuditLog(
            admin_id=user.id,
            admin_username=user.username or f"admin_{user.telegram_id}",
            action="GENERATE_ADMIN_KEY",
            details=f"Admin session key generated via Telegram bot.",
            ip_address="telegram"
        )
        db.add(audit)
        db.commit()
        logger.debug(f"[AUTH TRACE] Admin session key generated via Telegram bot for admin user ID: {user.id}")

        msg = (
            "🔑 <b>Your New Admin Session Key:</b>\n\n"
            f"<code>{key}</code>\n\n"
            "Copy and paste this key into the <b>Password</b> field of the Admin Portal login screen (Username: <code>admin</code>).\n\n"
            "⚠️ <i>Note: This key is only valid for ONE login session and will expire instantly once used or when you exit the browser.</i>"
        )
        
        last_msg_id = session.get("last_bot_msg_id")
        edited = False
        if last_msg_id:
            try:
                edited = await edit_bot_message(user.telegram_id, last_msg_id, msg)
            except Exception:
                edited = False
                
        if not edited:
            res = await send_bot_animation(
                user.telegram_id,
                "https://i.giphy.com/3o7TKSjRrfIPjei1fG.gif",
                caption=msg,
                reply_markup=main_keyboard
            )
            if isinstance(res, int):
                session["last_bot_msg_id"] = res
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
            location_keyboard = {
                "keyboard": [
                    [{"text": "📍 Share Current Location", "request_location": True}],
                    [{"text": "❌ Skip Location"}],
                    [{"text": "🍕 View Menu"}, {"text": "💰 My Wallet"}]
                ],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            welcome_text = (
                f"Hello {user.display_name}! 🍕 Welcome to <b>Domino's Order Engine</b> - The ultimate pizza shop.\n\n"
                f"📍 <b>Location Required:</b> Please click the button below to share your <b>Current Location</b> directly or choose '❌ Skip Location' to browse without it."
            )
            await send_bot_animation(
                user.telegram_id,
                "https://i.giphy.com/3o7iMClCoYV72aXf6o.gif", # Welcome Pizza Spinning
                caption=welcome_text,
                reply_markup=location_keyboard
            )
            return

        # If city IS set, welcome them and show the menu immediately!
        welcome_text = (
            f"Hello {user.display_name}! 🍕 Welcome back to <b>Domino's Order Engine</b>.\n\n"
            f"💰 Current Wallet Balance: <b>₹{user.wallet_balance:.2f}</b>\n"
            f"📍 City: <b>{user.city}</b>"
        )
        await send_bot_animation(
            user.telegram_id,
            "https://i.giphy.com/3o7iMClCoYV72aXf6o.gif", # Welcome Pizza Spinning
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
        session["state"] = "waiting_for_city"  # waiting for GPS only now
        coord_line = ""
        if user.latitude and user.longitude:
            coord_line = f"\n📡 Coords: <code>{user.latitude:.4f}, {user.longitude:.4f}</code>"
        addr_line = ""
        if user.phone:
            phone_line = f"\n📱 Phone: <code>{user.phone}</code>"
        else:
            phone_line = ""

        loc_msg = (
            f"📍 <b>Delivery Location & Details</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + (f"Current city: <b>{user.city}</b>{coord_line}{phone_line}\n\n" if user.city else "No location set yet.\n\n") +
            "Choose an option below:"
        )
        loc_options_keyboard = {
            "keyboard": [
                [{"text": "📍 Share My GPS Location", "request_location": True}],
                [{"text": "🏠 Update Delivery Address"}],
                [{"text": "📱 Update Phone Number"}],
                [{"text": "❌ Cancel"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }
        await send_bot_message(
            user.telegram_id,
            loc_msg,
            reply_markup=loc_options_keyboard
        )
        return

    elif text_clean == "🏠 Update Delivery Address":
        session["state"] = "waiting_for_address"
        session["temp_address"] = None
        coord_info = ""
        if user.latitude and user.longitude:
            coord_info = f"\n📡 GPS: <code>{user.latitude:.4f}, {user.longitude:.4f}</code> (will be updated from address)"
        await send_bot_message(
            user.telegram_id,
            f"🏠 <b>Enter Your Delivery Address</b>\n\n"
            f"Please type your full delivery address and press send.{coord_info}\n\n"
            f"<i>Example: Flat 4B, Sunrise Apartments, MG Road, Bengaluru 560001</i>",
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
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
            reply_markup={"keyboard": [[{"text": "❌ Cancel"}]], "resize_keyboard": True, "one_time_keyboard": True}
        )
        return

    elif text_lower == "🍕 view menu" or text.startswith("/menu"):
        # Ensure we have location for price adjustments
        if not user.city:
            location_keyboard = {
                "keyboard": [
                    [{"text": "📍 Share Current Location", "request_location": True}],
                    [{"text": "🔙 Back"}]
                ],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            await send_bot_message(
                user.telegram_id,
                "📍 <b>Location Required</b>\n\nPlease share your <b>Current GPS Location</b> to view incorrect pricing menu for your area.",
                reply_markup=location_keyboard
            )
            return
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
        orders = db.query(Order).filter(Order.user_id == user.id, ~Order.id.like("TOPUP-%")).order_by(Order.created_at.desc()).limit(5).all()
        if not orders:
            track_text = (
                "📦 <b>Track Orders:</b>\n\n"
                "You haven't placed any orders yet!\n\n"
                "👉 Click <b>Order App</b> or type /menu to order delicious pizzas!"
            )
            await send_bot_animation(
                user.telegram_id,
                "https://i.giphy.com/26FL34o80tNnJjS24.gif", # Scooter Delivery
                caption=track_text,
                reply_markup=main_keyboard
            )
            return
            
        track_lines = ["📦 <b>Your Recent Orders (Max 5):</b>\n"]
        inline_keyboard = []
        for o in orders:
            # Query latest status from history
            history = db.query(OrderStatusHistory).filter(OrderStatusHistory.order_id == o.id).order_by(OrderStatusHistory.created_at.desc()).first()
            current_status = history.status if history else o.status
            
            # Format order items summary
            items = db.query(OrderItem).filter(OrderItem.order_id == o.id).all()
            items_desc = ", ".join([f"{item.product.name} x{item.quantity}" for item in items if item.product])
            
            # Formatted placed date (local-friendly UTC string)
            date_str = o.created_at.strftime("%Y-%m-%d %H:%M UTC")
            
            track_lines.append(
                f"• <b>Order ID:</b> <code>{o.id}</code>\n"
                f"  <b>Items:</b> {items_desc or 'Pizza Order'}\n"
                f"  <b>Total:</b> ₹{o.total_payable:.2f} ({o.payment_method.upper()})\n"
                f"  <b>Progress:</b>\n  {get_order_progress_bar(current_status)}\n"
                f"  <b>Placed At:</b> {date_str}\n"
            )
            
            # Add interactive row buttons for each order
            row = []
            short_id = o.id.split("-")[-1]
            app_url = get_mini_app_url(db)
            is_https = app_url.startswith("https://")
            
            # 1. Live Web App tracker button
            if is_https:
                row.append({"text": f"📍 Track {short_id}", "web_app": {"url": f"{app_url}/?track={o.id}"}})
            else:
                row.append({"text": f"📍 Track {short_id} (Link)", "url": f"{app_url}/?track={o.id}"})
                
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
            "🎉 <b>Special Active Offers & Deals:</b>\n\n"
            "Select one of our exclusive deals below to automatically add the pizzas to your cart:\n\n"
            "🔥 <b>Deal 1: Double Cheeseburst Feast</b>\n"
            "• 2x Cheeseburst Margherita Pizzas\n"
            f"• <b>Price:</b> ₹410.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "🔥 <b>Deal 2: Veggie Duo Deal</b>\n"
            "• 1x Paneer & Capsicum Pizza + 1x Golden Corn Pizza\n"
            f"• <b>Price:</b> ₹90.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "🔥 <b>Deal 3: Classic Pizza Duo</b>\n"
            "• 1x Margherita Classic + 1x Tomato Onion Pizza\n"
            f"• <b>Price:</b> ₹150.00 + ₹{bot_fee:.2f} Service Fee\n\n"
            "💡 <i>Bot Service Fee is mandatory for all orders. Select a deal to add to cart instantly!</i>"
        )
        offers_markup = {
            "inline_keyboard": [
                [{"text": "🔥 Deal 1: 2x Cheeseburst (₹410)", "callback_data": "apply_deal_1"}],
                [{"text": "🔥 Deal 2: Paneer & Corn (₹90)", "callback_data": "apply_deal_2"}],
                [{"text": "🔥 Deal 3: Classic Duo (₹150)", "callback_data": "apply_deal_3"}],
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
                "https://i.giphy.com/3o7iMClCoYV72aXf6o.gif", # Pizza Spinning
                caption=offers_text,
                reply_markup=offers_markup
            )
            if isinstance(res, int):
                session["last_bot_msg_id"] = res
        return

    elif text_lower == "💬 contact support" or text.startswith("/support"):
        session["state"] = None  # Clear any pending state
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
        
        # Make API call to verify-payment endpoint
        api_url = f"http://localhost:8000/api/orders/{order_id}/verify-payment"
        try:
            resp = await _http_client.post(api_url, json={"utr": utr}, headers=headers, timeout=10.0)
            if resp.status_code == 200:
                # Success notification is sent by the verify_payment endpoint, so we do nothing here
                pass
            else:
                err_data = resp.json()
                detail = err_data.get("detail", "Payment verification failed.")
                await send_bot_message(user.telegram_id, f"❌ <b>Verification Failed:</b> {detail}")
        except Exception as e:
            await send_bot_message(user.telegram_id, f"❌ <b>Error:</b> Could not connect to verification server: {str(e)}")
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
        # Unknown message — show a helpful guide rather than silently logging as support.
        # Support is only triggered via the explicit "💬 Contact Support" button.
        await send_bot_message(
            user.telegram_id,
            f"🤖 <b>I didn't understand that.</b>\n\n"
            f"Use the menu buttons below to navigate:\n\n"
            f"• 🍕 <b>View Menu</b> — Browse & add items\n"
            f"• 🛒 <b>Cart</b> — View your cart & checkout\n"
            f"• 💰 <b>My Wallet</b> — Check balance, add funds\n"
            f"• 📍 <b>Change Location</b> — Update GPS & delivery address\n"
            f"• 📦 <b>Track Orders</b> — Check order status\n"
            f"• 💬 <b>Contact Support</b> — Send us a support message\n\n"
            f"<i>Type /start to restart the bot.</i>",
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
    if not user:
        await answer_callback_query(callback_query_id, "Error: Start session with /start first")
        return
        
    # Look up session state (restores from database to preserve "bot brain" on server restarts)
    if str(telegram_id) not in USER_BOT_SESSION:
        import json
        saved_cart = {}
        if user.bot_cart:
            try:
                saved_cart = json.loads(user.bot_cart)
            except Exception:
                pass
        USER_BOT_SESSION[str(telegram_id)] = {
            "state": user.bot_state,
            "cart": saved_cart
        }
    session = USER_BOT_SESSION[str(telegram_id)]
    
    # If the user clicks any inline button, clear any active text input states
    # to prevent them from being stuck in a waiting state if they navigate away.
    if session.get("state") in ("waiting_for_address", "waiting_for_phone", "waiting_for_promo_code", "waiting_for_topup_amount"):
        session["state"] = None
        session["checkout_pending"] = False

    app_url = get_mini_app_url(db)
    is_https = app_url.startswith("https://")
    order_app_btn = {"text": "🍕 Order App", "web_app": {"url": app_url}} if is_https else {"text": "🍕 Order App (Link)", "url": app_url}
    admin_portal_btn = {"text": "🌐 Open Admin Portal", "web_app": {"url": f"{app_url}/admin/"}} if is_https else {"text": "🌐 Open Admin Portal (Link)", "url": f"{app_url}/admin/"}
    
    admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
    is_admin = str(telegram_id) == str(admin_tg_id) or (user and user.role == "admin")
    
    main_keyboard = {
        "keyboard": [
            [{"text": "🍕 View Menu"}, {"text": "💰 My Wallet"}],
            [{"text": "📍 Change Location"}, {"text": "📦 Track Orders"}],
            [{"text": "🎉 Active Offers"}, {"text": "💬 Contact Support"}],
            [order_app_btn]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    if is_admin:
        main_keyboard["keyboard"].append([
            {"text": "🔑 Get Admin Secret Key"},
            admin_portal_btn
        ])

    if data == "menu_view":
        # Ensure we have location for price adjustments
        if not user.city:
            location_keyboard = {
                "keyboard": [
                    [{"text": "📍 Share Current Location", "request_location": True}],
                    [{"text": "🔙 Back"}]
                ],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
            await send_bot_message(
                user.telegram_id,
                "📍 <b>Location Required:</b> Please click the button below to share your <b>Current Location</b> directly to browse menu pricing for your area.",
                reply_markup=location_keyboard
            )
            await answer_callback_query(callback_query_id)
            return
        await display_pizza_menu(db, user, main_keyboard, page=1, category="All", edit_message_id=message_id)
        await answer_callback_query(callback_query_id)
        
    elif data.startswith("menu_page_"):
        parts = data.split("_")
        page = int(parts[2])
        category = parts[3]
        await display_pizza_menu(db, user, main_keyboard, page=page, category=category, edit_message_id=message_id)
        await answer_callback_query(callback_query_id)
        
    elif data == "apply_deal_1":
        p = db.query(Product).filter(Product.name.like("%Margherita%")).first()
        if not p:
            p = db.query(Product).first()
        if not p:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p.id): 2}
        session["active_deal"] = "deal_1"
        session["deal_price"] = 410.0
        await answer_callback_query(callback_query_id, "Deal 1 applied!")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "apply_deal_2":
        p_paneer = db.query(Product).filter(Product.name.like("%Paneer%")).first()
        if not p_paneer:
            p_paneer = db.query(Product).first()
        p_corn = db.query(Product).filter(Product.name.like("%Corn%")).first()
        if not p_corn:
            p_corn = db.query(Product).first()
        if not p_paneer or not p_corn:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p_paneer.id): 1, str(p_corn.id): 1}
        session["active_deal"] = "deal_2"
        session["deal_price"] = 90.0
        await answer_callback_query(callback_query_id, "Deal 2 applied!")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "apply_deal_3":
        p_margherita = db.query(Product).filter(Product.name.like("%Margherita%")).first()
        if not p_margherita:
            p_margherita = db.query(Product).first()
        p_onion = db.query(Product).filter(Product.name.like("%Tomato%")).first() or db.query(Product).filter(Product.name.like("%Onion%")).first()
        if not p_onion:
            p_onion = db.query(Product).first()
        if not p_margherita or not p_onion:
            await answer_callback_query(callback_query_id, "Product database is empty!")
            return
        session["cart"] = {str(p_margherita.id): 1, str(p_onion.id): 1}
        session["active_deal"] = "deal_3"
        session["deal_price"] = 150.0
        await answer_callback_query(callback_query_id, "Deal 3 applied!")
        cart_text, cart_markup = render_cart_message(db, user, session["cart"], session)
        await edit_bot_message(user.telegram_id, message_id, cart_text, cart_markup)

    elif data == "support_menu":
        support_help = (
            "💬 <b>Contact Support & FAQs</b>\n\n"
            "Need help with your order, payment, or wallet?\n\n"
            "• <b>Dedicated Help Bot:</b> Click the button below to message our support representative bot directly.\n"
            "• <b>Common Queries:</b> Tap one of the support FAQ options below to get instant answers."
        )
        support_markup = {
            "inline_keyboard": [
                [{"text": "💬 Go to Help Bot", "url": "https://t.me/dominosordersHELP_bot"}],
                [{"text": "📖 FAQ: How to Order?", "callback_data": "faq_how_to_order"}],
                [{"text": "💳 FAQ: Wallet & UPI?", "callback_data": "faq_wallet_upi"}],
                [{"text": "📦 FAQ: Where is my Order?", "callback_data": "faq_where_order"}]
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
            "📖 <b>FAQ: How to Order?</b>\n\n"
            "1. Click the 'Order Delicious Pizza 🍕' button or start the app to open the menu.\n"
            "2. Select your favorite pizzas and add them to the cart.\n"
            "3. Go to the Checkout tab, enter your address and phone number, and drop a pin on the map.\n"
            "4. Choose your payment method (Wallet or Card) and click 'Place Order'.\n"
            "5. You can view your order status in the My Orders tab."
        )
        back_markup = {"inline_keyboard": [[{"text": "🔙 Support Menu", "callback_data": "support_menu"}]]}
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data == "faq_wallet_upi":
        faq_text = (
            "💳 <b>FAQ: Wallet & UPI?</b>\n\n"
            "• <b>UPI Payment:</b> Choose 'UPI / Scan QR' at checkout. Scan the QR code, pay the exact amount, and reply in this chat with the 12-digit UPI UTR number.\n"
            "• <b>Wallet Top-up:</b> Go to '💰 My Wallet' -> '💳 Add Funds', choose or enter the amount, scan the QR code to pay, and reply with the 12-digit UTR to credit your wallet instantly."
        )
        back_markup = {"inline_keyboard": [[{"text": "🔙 Support Menu", "callback_data": "support_menu"}]]}
        await edit_bot_message(user.telegram_id, message_id, faq_text, reply_markup=back_markup)
        await answer_callback_query(callback_query_id)

    elif data == "faq_where_order":
        faq_text = (
            "📦 <b>FAQ: Where is my Order?</b>\n\n"
            "• You can check order progress in the Web App under the 'My Orders' tab.\n"
            "• In the bot, tap '📦 Track Orders' to see the status of all your active orders, refreshed in real-time."
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
        address = session.get("temp_address")
        phone = session.get("temp_phone")
        
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
        order_id = data.replace("pay_now_", "")
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
        
        payment_text = (
            f"💳 <b>UPI Payment Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>Order ID:</b> <code>{order.id}</code>\n"
            f"• <b>Amount:</b> <b>₹{order.total_payable:.2f}</b>\n\n"
            f"👉 <a href=\"{upi_uri}\"><b>📱 Click Here to Pay via UPI App</b></a> (mobile) or scan the QR code above.\n\n"
            f"Once paid, send your 12-digit UTR number in this chat to complete verification!"
        )
        
        payment_markup = {
            "inline_keyboard": [
                [{"text": "❌ Cancel Order", "callback_data": f"cancel_order_{order.id}"}]
            ]
        }
        
        await send_bot_photo(user.telegram_id, qr_url, payment_text, reply_markup=payment_markup)
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("cancel_order_"):
        order_id = data.replace("cancel_order_", "")
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
        
        payment_text = (
            f"💳 <b>Deposit Payment Request</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"• <b>Ref ID / Order ID:</b> <code>{order_id}</code>\n"
            f"• <b>Amount:</b> <b>₹{amount:.2f}</b>\n\n"
            f"👉 <a href=\"{upi_uri}\"><b>📱 Click Here to Pay via UPI App</b></a> (mobile) or scan the QR code above.\n\n"
            f"⏳ <b>Expiry notice:</b> This payment request expires automatically in <b>5 minutes</b>.\n\n"
            f"Once paid, send the 12-digit UTR number here, or tap <b>✅ I Have Paid</b> below."
        )
        
        payment_markup = {
            "inline_keyboard": [
                [{"text": "✅ I Have Paid", "callback_data": f"wallet_marked_paid_{order_id}"}],
                [{"text": "❌ Cancel Request", "callback_data": f"wallet_cancel_deposit_{order_id}"}]
            ]
        }
        
        # Delete previous confirmation message
        await delete_bot_message(user.telegram_id, message_id)
        # Send actual QR photo for instant loading
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
        order_id = data.replace("reenter_utr_", "")
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        session["state"] = f"waiting_for_utr_{order_id}"
        await edit_bot_message(
            user.telegram_id,
            message_id,
            f"✍️ <b>Payment Re-submission</b>\n\n"
            f"Please type your new 12-digit UPI UTR number for Ref ID: <code>{order_id}</code> now:"
        )
        await answer_callback_query(callback_query_id)
        return

    elif data.startswith("wallet_marked_paid_"):
        order_id = data.replace("wallet_marked_paid_", "")
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        order.status = "Pending Verification"
        db.commit()
        
        success_text = (
            f"✅ <b>Payment Submitted for Verification</b>\n\n"
            f"Your deposit request for <b>₹{order.total_payable:.2f}</b> (Ref: <code>{order_id}</code>) has been submitted to the admin team.\n\n"
            f"We are verifying your transaction now. Your balance will update automatically upon approval."
        )
        await edit_bot_message(user.telegram_id, message_id, success_text, reply_markup=None)
        await answer_callback_query(callback_query_id, "Status updated!")
        
        # Notify admin via Telegram
        admin_text = (
            "🔔 <b>New Deposit Marked as Paid (No UTR)</b>\n\n"
            f"👤 <b>User:</b> {user.display_name} (ID: {user.telegram_id})\n"
            f"💰 <b>Amount:</b> ₹{order.total_payable:.2f}\n"
            f"🆔 <b>Ref ID:</b> <code>{order_id}</code>\n\n"
            f"Please verify this payment against your UPI/bank statement!"
        )
        await notify_admins(db, admin_text)
        
        if sse_broadcast_callback:
            try:
                await sse_broadcast_callback({"type": "order_update", "order_id": order_id, "status": "Pending Verification"})
            except Exception:
                pass

    elif data.startswith("wallet_cancel_deposit_"):
        order_id = data.replace("wallet_cancel_deposit_", "")
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
        order_id = data.replace("reenter_utr_", "")
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
        order_id = data.replace("cancel_rejected_order_", "")
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

        
    elif data.startswith("track_refresh_"):
        order_id = data.split("_")[-1]
        order = db.query(Order).filter(Order.id == order_id).first()
        if not order:
            await answer_callback_query(callback_query_id, "Order not found!")
            return
            
        history = db.query(OrderStatusHistory).filter(OrderStatusHistory.order_id == order_id).order_by(OrderStatusHistory.created_at.desc()).first()
        current_status = history.status if history else order.status
        date_str = order.created_at.strftime("%Y-%m-%d %H:%M UTC")
        
        track_text = (
            f"📦 <b>Order Status Tracking:</b>\n\n"
            f"• <b>Order ID:</b> <code>{order.id}</code>\n"
            f"• <b>Status:</b> <b>{current_status}</b>\n"
            f"• <b>Total:</b> ₹{order.total_payable:.2f}\n"
            f"• <b>Updated At:</b> {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
            "🕒 <i>Refreshed in-place. Click below to check again.</i>"
        )
        await edit_bot_message(user.telegram_id, message_id, track_text, {
            "inline_keyboard": [[{"text": "🔄 Refresh Status", "callback_data": f"track_refresh_{order_id}"}]]
        })
        await answer_callback_query(callback_query_id, "Status updated!")
        
    elif data == "order_confirm_place":
        if session.get("placing_order"):
            await answer_callback_query(callback_query_id, "⚠️ Order is already processing. Please wait.")
            return
        session["placing_order"] = True
        
        order_id = f"BOT-{uuid.uuid4().hex[:8].upper()}"
        txn_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
        
        cart = session.get("cart", {})
        address = session.get("temp_address")
        phone = session.get("temp_phone")
        
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
        
        # Allocate gift card if available
        if gift_card:
            gift_card.status = "used"
            gift_card.used_by_user_id = user.id
            gift_card.used_in_order_id = order_id
            gift_card.used_at = datetime.datetime.now(datetime.timezone.utc)
            order.gift_card_id = gift_card.id
            h2 = OrderStatusHistory(order_id=order_id, status="Gift Card Applied")
            db.add(h2)
            
        order.status = "Order Processing"
        h3 = OrderStatusHistory(order_id=order_id, status="Order Processing")
        db.add(h3)
        db.commit()
        
        # Notify admins via Telegram Bot
        admin_order_text = (
            "🔔 <b>New Order Placed!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 <b>Order ID:</b> <code>{order_id}</code>\n"
            f"👤 <b>User:</b> {user.display_name} (ID: <code>{user.telegram_id}</code>)\n"
            f"💰 <b>Total Paid:</b> ₹{total_payable:.2f}\n"
            f"🏡 <b>Address:</b> <code>{address}</code>\n"
            f"📱 <b>Phone:</b> <code>{phone}</code>\n"
            f"📍 <b>GPS:</b> <code>{user.latitude or 'None'}, {user.longitude or 'None'}</code>\n\n"
            "👩‍🍳 <i>The background robot is attempting to place this order automatically on Domino's. Check your admin panel for live logs!</i>"
        )
        await notify_admins(db, admin_order_text)
        
        # Clear cart & state
        session["cart"] = {}
        session["state"] = None
        session["temp_address"] = None
        session["temp_phone"] = None
        
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
                    [{"text": "📞 Contact Support", "url": "https://t.me/dominosordersHELP_bot"}]
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
                                        [{"text": "📞 Contact Support", "url": "https://t.me/dominosordersHELP_bot"}]
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
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    user_key = str(telegram_id)
    
    # 0.05s callback cooldown to avoid double clicks
    last_time = USER_LAST_CB_TIME.get(user_key, 0)
    if now - last_time < 0.05:
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
                await answer_callback_query(callback_query_id, "An error occurred. Please try again.")
            except Exception:
                pass
        finally:
            db.close()

async def process_incoming_message_task(telegram_id: str, first_name: str, last_name: str, username: str, text: str, location: dict = None, message_id: int = None):
    """Processes an incoming message in a non-blocking background task with a clean DB session."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    user_key = str(telegram_id)
    
    # 0.05s message cooldown to avoid message spam
    last_time = USER_LAST_MSG_TIME.get(user_key, 0)
    if now - last_time < 0.05:
        logger.warning(f"[RATE LIMIT] Ignoring message from {telegram_id} (too fast)")
        return
    USER_LAST_MSG_TIME[user_key] = now

    if user_key not in USER_PROCESSING_LOCKS:
        USER_PROCESSING_LOCKS[user_key] = asyncio.Lock()
        
    async with USER_PROCESSING_LOCKS[user_key]:
        db = SessionLocal()
        try:
            await handle_bot_message(db, telegram_id, first_name, last_name, username, text, location, message_id)
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
    Background polling loop for Telegram Bot.
    """
    if not BOT_TOKEN or BOT_TOKEN == "MOCK_TOKEN":
        logger.info("Telegram Bot Token is missing or MOCK_TOKEN. Running in MOCK Mode (notifications logged to terminal).")
        while True:
            await asyncio.sleep(3600) # Sleep indefinitely in mock mode
            
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
                            if "text" not in message and "location" not in message:
                                continue
                            chat = message.get("chat", {})
                            if chat.get("type") != "private":
                                continue
                            from_user = message.get("from", {})
                            telegram_id = from_user.get("id")
                            first_name = from_user.get("first_name", "")
                            last_name = from_user.get("last_name", "")
                            username = from_user.get("username", "")
                            text = message.get("text", "").strip() if "text" in message else ""
                            location = message.get("location")
                            
                            logger.debug(f"[BOT TRACE] Received message/location from {first_name} [ID: {telegram_id}]")
                            asyncio.create_task(process_incoming_message_task(telegram_id, first_name, last_name, username, text, location, message.get("message_id")))
                            
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
