import os
import json
import uuid
import datetime
import sys
import asyncio
import traceback
import hashlib
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Cookie, Response, Request, UploadFile, File, Form
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from pydantic import BaseModel, Field

from .database import get_db, User, UserSession, Product, Order, OrderItem, OrderStatusHistory, GiftCard, SupportMessage, AuditLog, ErrorLog, SystemConfig, LoginAttempt, SavedAddress, LocationPricing, RiderAssignment, OrderNote, Notification, Proxy, ProxyLog, DominosSession, DominosOTPRequest, QRGenerationHistory, VerifiedUTR, UTRAttempt, RobotLog, Coupon, CouponRedemption, WalletTransaction
import logging
logger = logging.getLogger(__name__)
from .services import dominos_service
from .services.dominos_browser import DominosBrowser
from .services.order_sync import OrderSyncer
from .auth import (
    verify_telegram_init_data, create_access_token, create_refresh_token,
    verify_token, hash_password, verify_password, ACCESS_TOKEN_EXPIRE_MINUTES
)
from .bot import send_bot_message, send_bot_photo, get_order_progress_bar, reverse_geocode
from .utils import encrypt_data, decrypt_data, parse_gift_card_file, api_rate_limiter, strict_rate_limiter, generate_upi_qr_details

router = APIRouter()
UPLOAD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "uploads"))
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Admin Credentials configuration (defaults for testing, can be overridden)
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_HASH = hash_password(os.getenv("ADMIN_PASSWORD", "pizza123"))

# Callback for broadcasting events via SSE (injected by main.py)
sse_broadcast_callback = None
ws_broadcast_callback = None

# --- Helper Dependencies ---

def get_client_ip(request: Request) -> str:
    """Helper to detect real client IP, respecting standard reverse proxy headers."""
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    x_real_ip = request.headers.get("x-real-ip")
    if x_real_ip:
        return x_real_ip.strip()
    if request.client:
        return request.client.host
    return "127.0.0.1"

def get_current_user(request: Request, db: Session = Depends(get_db)):
    """Dependency to validate JWT Access Token and fetch user."""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid token")
    
    token = auth_header.split(" ")[1]
    payload = verify_token(token)
    if not payload or "sub" not in payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token session")
    
    sub = str(payload["sub"])
    if sub == "0":
        user = db.query(User).filter(User.role == "admin").first()
    else:
        user = db.query(User).filter(or_(User.id == sub, User.telegram_id == sub)).first()
        if not user and sub.isdigit():
            user = db.query(User).filter(User.id == int(sub)).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    if user.is_blocked:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User account is blocked")
        
    return user

def get_current_user_optional(request: Request, db: Session = Depends(get_db)) -> Optional[User]:
    """Optional dependency to fetch user if valid token is provided."""
    try:
        return get_current_user(request, db)
    except Exception:
        return None

def get_current_admin(request: Request, db: Session = Depends(get_db)):
    """Dependency to validate JWT Access Token and ensure the user is an admin."""
    user = get_current_user(request, db)
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required")
    return user

async def log_admin_action(db: Session, admin_id: str, username: str, action: str, details: dict, request: Request):
    """Utility to log administrative actions to the audit trail."""
    log = AuditLog(
        admin_id=admin_id,
        admin_username=username,
        action=action,
        details=json.dumps(details),
        ip_address=get_client_ip(request)
    )
    db.add(log)
    db.commit()

# --- Schemas ---

class TelegramLoginRequest(BaseModel):
    initData: str

class AdminLoginRequest(BaseModel):
    username: str
    password: str

class CartItemSchema(BaseModel):
    product_id: str
    quantity: int

class CheckoutRequest(BaseModel):
    items: List[CartItemSchema]
    payment_method: str # 'wallet', 'direct'
    address: str
    landmark: Optional[str] = None
    latitude: float
    longitude: float
    phone: str
    coupon_code: Optional[str] = None
    device_id: Optional[str] = None
    device_details: Optional[str] = None

class OrderStatusUpdateSchema(BaseModel):
    status: str
    estimated_delivery_minutes: Optional[int] = None

class SupportMessageSend(BaseModel):
    message: str
    recipient_id: Optional[int] = None # Admin uses this to specify user

class UserCreateRequest(BaseModel):
    telegram_id: str
    username: Optional[str] = None
    display_name: str
    wallet_balance: float = 100.0
    role: str = "user"

class SystemConfigSchema(BaseModel):
    newbie_coupon: str
    welcome_coupon: str
    cart_promo_min: float
    cart_promo_max: float
    cart_promo_fixed: float

class SingleConfigSchema(BaseModel):
    key: str
    value: str

class PaymentVerifyRequest(BaseModel):
    utr: str

# --- AUTH ROUTES ---

@router.post("/auth/login")
async def telegram_login(payload: TelegramLoginRequest, request: Request, response: Response, db: Session = Depends(get_db)):
    client_ip = get_client_ip(request)
    if api_rate_limiter.is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too many login attempts")

    # 1. Verify Telegram signature
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "MOCK_TOKEN")
    tg_user = verify_telegram_init_data(payload.initData, bot_token)
    if not tg_user:
        raise HTTPException(status_code=400, detail="Invalid Telegram authentication signature")
    
    tg_id = str(tg_user["id"])
    username = tg_user.get("username", "")
    display_name = f"{tg_user.get('first_name', '')} {tg_user.get('last_name', '')}".strip() or username or f"User_{tg_id}"
    photo_url = tg_user.get("photo_url", "")
    
    # 2. Find or create user
    user = db.query(User).filter(User.telegram_id == tg_id).first()
    if not user:
        user = User(
            telegram_id=tg_id,
            username=username,
            display_name=display_name,
            photo_url=photo_url,
            wallet_balance=100.0, # Default sign-up balance for testing/marketing
            role="user"
        )
        # Check if first user is admin (or if configured in environment)
        if tg_id == os.getenv("ADMIN_TELEGRAM_ID"):
            user.role = "admin"
        db.add(user)
        db.flush()
    else:
        # Update user profile information dynamically
        user.username = username
        user.display_name = display_name
        if photo_url:
            user.photo_url = photo_url
        if user.is_blocked:
            raise HTTPException(status_code=403, detail="User account is blocked")
    
    # 3. Create tokens
    access_token = create_access_token({"sub": str(user.id), "role": user.role})
    refresh_token = create_refresh_token()
    
    # 4. Save Session (Deactivate older active sessions first)
    db.query(UserSession).filter(UserSession.user_id == user.id, UserSession.is_active == True).update({"is_active": False})
    session_id = str(uuid.uuid4())
    db_session = UserSession(
        id=session_id,
        user_id=user.id,
        refresh_token=refresh_token,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("user-agent", "unknown")
    )
    db.add(db_session)
    db.commit()
    
    # 5. Set HttpOnly Refresh Cookie
    response.set_cookie(
        key="refreshToken",
        value=refresh_token,
        httponly=True,
        max_age=30 * 24 * 3600, # 30 days
        samesite="lax",
        secure=False # Set to True in HTTPS production
    )
    
    # Broadcast user login activity
    if sse_broadcast_callback:
        await sse_broadcast_callback({
            "type": "user_login",
            "user_id": user.id,
            "display_name": user.display_name,
            "ip": db_session.ip_address
        })

    return {
        "access_token": access_token,
        "user": {
            "id": user.id,
            "telegram_id": user.telegram_id,
            "username": user.username,
            "display_name": user.display_name,
            "photo_url": user.photo_url,
            "wallet_balance": user.wallet_balance,
            "role": user.role,
            "city": user.city,
            "phone": user.phone
        }
    }

@router.post("/auth/refresh")
def auth_refresh(request: Request, response: Response, refreshToken: Optional[str] = Cookie(None), db: Session = Depends(get_db)):
    if not refreshToken:
        raise HTTPException(status_code=401, detail="Refresh token missing")
        
    db_session = db.query(UserSession).filter(
        UserSession.refresh_token == refreshToken,
        UserSession.is_active == True
    ).first()
    
    if not db_session:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
        
    user = db.query(User).filter(User.id == db_session.user_id).first()
    if not user or user.is_blocked:
        raise HTTPException(status_code=401, detail="User blocked or deactivated")
        
    # Refresh token and generate new access token
    new_access_token = create_access_token({"sub": str(user.id), "role": user.role})
    db_session.last_active = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    db.commit()
    
    return {"access_token": new_access_token}

@router.post("/auth/logout")
def auth_logout(response: Response, refreshToken: Optional[str] = Cookie(None), db: Session = Depends(get_db)):
    if refreshToken:
        db_session = db.query(UserSession).filter(UserSession.refresh_token == refreshToken).first()
        if db_session:
            db_session.is_active = False
            db.commit()
    response.delete_cookie("refreshToken")
    return {"status": "success"}

@router.get("/users/sessions")
def get_user_sessions(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Fetches all active sessions for the current logged-in user."""
    sessions = db.query(UserSession).filter(
        UserSession.user_id == user.id,
        UserSession.is_active == True
    ).order_by(UserSession.created_at.desc()).all()
    
    result = []
    for s in sessions:
        result.append({
            "id": s.id,
            "ip_address": s.ip_address or "unknown",
            "user_agent": s.user_agent or "unknown",
            "created_at": s.created_at.isoformat(),
            "last_active": s.last_active.isoformat()
        })
    return result

# --- ADMIN AUTH ROUTE ---

@router.post("/admin/login")
def admin_login(payload: AdminLoginRequest, request: Request, response: Response, db: Session = Depends(get_db)):
    client_ip = get_client_ip(request)
    if strict_rate_limiter.is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too many login attempts")

    # 1. Brute force lockout check
    ip_addr = get_client_ip(request)
    fifteen_mins_ago = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=15)
    
    failed_attempts = db.query(LoginAttempt).filter(
        or_(LoginAttempt.username == payload.username, LoginAttempt.ip_address == ip_addr),
        LoginAttempt.status == "failed",
        LoginAttempt.attempted_at >= fifteen_mins_ago
    ).count()
    
    if failed_attempts >= 5:
        # Record failed lockout attempt
        attempt = LoginAttempt(username=payload.username, ip_address=ip_addr, status="failed")
        db.add(attempt)
        db.commit()
        raise HTTPException(status_code=403, detail="Too many failed login attempts. Locked out for 15 minutes.")

    # 2. Check credentials
    login_success = False
    
    # Check if this is the admin user attempting to log in with the bot-generated session key
    session_key_cfg = db.query(SystemConfig).filter(SystemConfig.key == "admin_session_key").first()
    bot_key_matched = False
    
    trace_details = {
        "username": payload.username,
        "password_len": len(payload.password) if payload.password else 0,
        "ip_address": ip_addr,
        "error_reason": None
    }
    
    if payload.username == ADMIN_USERNAME:
        # Check static password first (stripped)
        password_stripped = payload.password.strip() if payload.password else ""
        if verify_password(ADMIN_PASSWORD_HASH, password_stripped):
            admin_db_user = db.query(User).filter(User.role == "admin").first()
            user_id = admin_db_user.id if admin_db_user else 1
            role = "admin"
            display_name = admin_db_user.display_name if admin_db_user else "Super Admin"
            login_success = True
            logger.info(f"[AUTH TRACE] Static password matched. Session created for admin from IP: {ip_addr}")
        # Otherwise, check if this is the admin user attempting to log in with the bot-generated session key
        elif session_key_cfg and session_key_cfg.value and len(session_key_cfg.value.strip()) == 50 and password_stripped == session_key_cfg.value.strip():
            admin_db_user = db.query(User).filter(User.role == "admin").first()
            user_id = admin_db_user.id if admin_db_user else 1
            role = "admin"
            display_name = admin_db_user.display_name if admin_db_user else "Super Admin"
            login_success = True
            # Invalidate/consume the key immediately
            session_key_cfg.value = ""
            db.commit()
            logger.info(f"[AUTH TRACE] Bot session key matched and invalidated. Session created for admin from IP: {ip_addr}")
        else:
            if not session_key_cfg:
                trace_details["error_reason"] = "No admin session key initialized in database"
            elif not session_key_cfg.value:
                trace_details["error_reason"] = "Admin session key is empty (already consumed or not set)"
            else:
                trace_details["error_reason"] = "Submitted key/password does not match"
    else:
        trace_details["error_reason"] = f"Invalid admin username: '{payload.username}'"
    
    if not login_success:
        # Log failure in database
        attempt = LoginAttempt(username=payload.username, ip_address=ip_addr, status="failed")
        db.add(attempt)
        
        # Detailed trace log
        msg = f"Failed admin login attempt. Details: {json.dumps(trace_details)}"
        err = ErrorLog(type="auth_failure", message=msg)
        db.add(err)
        
        # Audit log for failed attempt
        audit = AuditLog(
            admin_id=None,
            admin_username=payload.username,
            action="LOGIN_FAILED",
            details=json.dumps(trace_details),
            ip_address=ip_addr
        )
        db.add(audit)
        db.commit()
        logger.warning(f"[AUTH TRACE] Failed admin login attempt. Reason: {trace_details['error_reason'] or 'Unknown'}")
        raise HTTPException(status_code=401, detail="Invalid admin credentials")

    # Log success in database
    attempt = LoginAttempt(username=payload.username, ip_address=ip_addr, status="success")
    db.add(attempt)
    
    # Audit log for success
    audit = AuditLog(
        admin_id=user_id,
        admin_username=payload.username,
        action="LOGIN_SUCCESS",
        details=json.dumps({"ip": ip_addr, "auth_method": "one_time_session_key"}),
        ip_address=ip_addr
    )
    db.add(audit)

    access_token = create_access_token({"sub": str(user_id), "role": role})
    refresh_token = create_refresh_token()
    
    # Save Session (Deactivate older active sessions first)
    db.query(UserSession).filter(UserSession.user_id == user_id, UserSession.is_active == True).update({"is_active": False})
    session_id = str(uuid.uuid4())
    db_session = UserSession(
        id=session_id,
        user_id=user_id,
        refresh_token=refresh_token,
        ip_address=ip_addr,
        user_agent=request.headers.get("user-agent", "unknown")
    )
    db.add(db_session)
    
    # Audit log
    log = AuditLog(
        admin_id=user_id,
        admin_username=payload.username,
        action="LOGIN",
        details=json.dumps({"ip": db_session.ip_address}),
        ip_address=db_session.ip_address
    )
    db.add(log)
    db.commit()

    response.set_cookie(
        key="refreshToken",
        value=refresh_token,
        httponly=True,
        max_age=30 * 24 * 3600,
        samesite="lax"
    )

    return {
        "access_token": access_token,
        "user": {
            "id": user_id,
            "username": payload.username,
            "display_name": display_name,
            "role": role
        }
    }

# --- PRODUCT ROUTES (MENU) ---

@router.get("/products")
def get_products(db: Session = Depends(get_db)):
    products = db.query(Product).order_by(Product.original_price.asc(), Product.id.asc()).all()
    return products

@router.post("/products")
async def create_product(
    request: Request,
    name: str = Form(...),
    description: str = Form(None),
    category: str = Form(...),
    is_veg: bool = Form(True),
    original_price: float = Form(...),
    discounted_price: Optional[float] = Form(None),
    availability: bool = Form(True),
    sort_order: int = Form(0),
    is_popular: bool = Form(False),
    is_recommended: bool = Form(False),
    image: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    image_url = ""
    if image:
        file_ext = os.path.splitext(image.filename)[1]
        filename = f"{uuid.uuid4()}{file_ext}"
        filepath = os.path.join(UPLOAD_DIR, filename)
        with open(filepath, "wb") as f:
            f.write(await image.read())
        image_url = f"/uploads/{filename}"
        
    product = Product(
        name=name,
        description=description,
        category=category,
        is_veg=is_veg,
        original_price=original_price,
        discounted_price=discounted_price,
        image_url=image_url,
        availability=availability,
        sort_order=sort_order,
        is_popular=is_popular,
        is_recommended=is_recommended
    )
    db.add(product)
    db.commit()
    db.refresh(product)
    
    await log_admin_action(db, admin.id, admin.username, "PRODUCT_CREATED", {"id": product.id, "name": product.name}, request)
    
    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "menu_update"})
        
    return product

@router.put("/products/{product_id}")
async def update_product(
    product_id: str,
    request: Request,
    name: str = Form(...),
    description: str = Form(None),
    category: str = Form(...),
    is_veg: bool = Form(True),
    original_price: float = Form(...),
    discounted_price: Optional[float] = Form(None),
    availability: bool = Form(True),
    sort_order: int = Form(0),
    is_popular: bool = Form(False),
    is_recommended: bool = Form(False),
    image: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
        
    # Check if price changed
    price_change = {}
    if product.original_price != original_price:
        price_change["original_price"] = {"old": product.original_price, "new": original_price}
    if product.discounted_price != discounted_price:
        price_change["discounted_price"] = {"old": product.discounted_price, "new": discounted_price}

    product.name = name
    product.description = description
    product.category = category
    product.is_veg = is_veg
    product.original_price = original_price
    product.discounted_price = discounted_price
    product.availability = availability
    product.sort_order = sort_order
    product.is_popular = is_popular
    product.is_recommended = is_recommended
    
    if image:
        # Delete old image if it exists
        if product.image_url and product.image_url.startswith("/uploads/"):
            old_path = os.path.join(UPLOAD_DIR, os.path.basename(product.image_url))
            if os.path.exists(old_path):
                os.remove(old_path)
                
        file_ext = os.path.splitext(image.filename)[1]
        filename = f"{uuid.uuid4()}{file_ext}"
        filepath = os.path.join(UPLOAD_DIR, filename)
        with open(filepath, "wb") as f:
            f.write(await image.read())
        product.image_url = f"/uploads/{filename}"
        
    db.commit()
    
    details = {"id": product.id, "name": product.name}
    if price_change:
        details["price_changes"] = price_change
        await log_admin_action(db, admin.id, admin.username, "PRICE_CHANGED", details, request)
    else:
        await log_admin_action(db, admin.id, admin.username, "PRODUCT_EDITED", details, request)
        
    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "menu_update"})
        
    return product

@router.delete("/products/{product_id}")
async def delete_product(product_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
        
    product_name = product.name
    # Delete image from filesystem
    if product.image_url and product.image_url.startswith("/uploads/"):
        img_path = os.path.join(UPLOAD_DIR, os.path.basename(product.image_url))
        if os.path.exists(img_path):
            os.remove(img_path)
            
    db.delete(product)
    db.commit()
    
    await log_admin_action(db, admin.id, admin.username, "PRODUCT_DELETED", {"id": product_id, "name": product_name}, request)
    
    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "menu_update"})
        
    return {"status": "success"}

@router.get("/dominos/menu")
async def get_dominos_menu(lat: float, lon: float, page: int = 1, limit: int = 10, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Fetch nearby Domino's store and menu items (paginated)."""
    browser = DominosBrowser()
    store = await browser.find_nearest_store(lat, lon, db)
    menu = await browser.fetch_menu(store["store_id"], page=page, limit=limit, db=db)
    # Broadcast menu update (optional)
    try:
        from .main import ws_manager
        await ws_manager.broadcast_all({"type": "menu_update", "store_id": store["store_id"], "menu": menu, "page": page})
    except Exception:
        pass
    return {"store": store, "menu": menu, "page": page, "limit": limit}

@router.get("/orders/my-orders")
def get_my_orders(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    from .services.order_processor import serialize_order
    orders = db.query(Order).filter(Order.user_id == user.id).order_by(Order.created_at.desc()).all()
    result = []
    for o in orders:
        d = serialize_order(o)
        d["gift_card"] = {"value": o.gift_card.value} if o.gift_card else None
        result.append(d)
    return result

@router.get("/orders")
def get_orders_list(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Alias for /orders/my-orders — used by the web app."""
    from .services.order_processor import serialize_order
    orders = db.query(Order).filter(Order.user_id == user.id).order_by(Order.created_at.desc()).all()
    result = []
    for o in orders:
        d = serialize_order(o)
        d["gift_card"] = {"value": o.gift_card.value} if o.gift_card else None
        result.append(d)
    return result

@router.get("/orders/{order_id}")
def get_order_details(order_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    order = db.query(Order).filter(Order.id == order_id, Order.user_id == user.id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    from .services.order_processor import serialize_order
    data = serialize_order(order)
    if order.gift_card:
        data["gift_card"] = {
            "code": "REDACTED",
            "pin": "REDACTED",
            "value": order.gift_card.value
        }
    else:
        data["gift_card"] = None
    return data

# --- CART & ORDER ROUTES (CUSTOMER) ---

@router.post("/orders")
async def checkout_order(payload: CheckoutRequest, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """
    Submits a cart and processes the order transaction.
    Uses database transactions to guarantee safety.
    """
    if not payload.items:
        raise HTTPException(status_code=400, detail="Cart is empty")
        
    # Automatically capture, sync, and save the user's coordinates and resolved city during checkout
    if payload.latitude and payload.longitude:
        user.latitude = payload.latitude
        user.longitude = payload.longitude
        if not user.city:
            try:
                city = await reverse_geocode(payload.latitude, payload.longitude)
                if city:
                    user.city = city
            except Exception:
                pass
        db.commit()
        
    original_total = 0.0
    discount_total = 0.0
    items_to_create = []
    
    ketchup_product = db.query(Product).filter(Product.name == "Tomato Ketchup (Auto-Added)").first()
    has_ketchup = False
    ketchup_qty = 0

    # Apply location-based price adjustment
    multiplier = 1.0
    if user.city:
        pricing = db.query(LocationPricing).filter(LocationPricing.city.ilike(f"%{user.city}%")).first()
        if pricing:
            multiplier = pricing.price_multiplier
        else:
            multiplier = 1.0
    else:
        multiplier = 1.0

    # 1. Calculate prices and validate stock
    for item in payload.items:
        if ketchup_product and item.product_id == ketchup_product.id:
            has_ketchup = True
            ketchup_qty = item.quantity
            continue
        product = db.query(Product).filter(Product.id == item.product_id).first()
        if not product or not product.availability:
            raise HTTPException(
                status_code=400, 
                detail=f"Product {item.product_id} is out of stock or unavailable"
            )
            
        base_price = product.discounted_price if product.discounted_price is not None else product.original_price
        unit_price = float(round(base_price * multiplier))
        orig_price = float(round(product.original_price * multiplier))
        
        item_total = unit_price * item.quantity
        original_total += orig_price * item.quantity
        
        # Calculate discount
        if product.discounted_price is not None:
            disc_price = float(round(product.discounted_price * multiplier))
            discount_total += (orig_price - disc_price) * item.quantity
            
        items_to_create.append(
            OrderItem(
                product_id=product.id,
                quantity=item.quantity,
                price=unit_price
            )
        )
        
    subtotal = round(original_total - discount_total, 2)
    
    # Retrieve system configs for pricing caps and coupons
    newbie_coupon_cfg = db.query(SystemConfig).filter(SystemConfig.key == "newbie_coupon").first()
    welcome_coupon_cfg = db.query(SystemConfig).filter(SystemConfig.key == "welcome_coupon").first()
    cart_promo_min_cfg = db.query(SystemConfig).filter(SystemConfig.key == "cart_promo_min").first()
    cart_promo_max_cfg = db.query(SystemConfig).filter(SystemConfig.key == "cart_promo_max").first()
    cart_promo_fixed_cfg = db.query(SystemConfig).filter(SystemConfig.key == "cart_promo_fixed").first()
    
    val_min = float(cart_promo_min_cfg.value) if cart_promo_min_cfg else 180.0
    val_max = float(cart_promo_max_cfg.value) if cart_promo_max_cfg else 220.0
    val_fixed = float(cart_promo_fixed_cfg.value) if cart_promo_fixed_cfg else 100.0

    diff = round(val_min - subtotal, 2)
    if 10.0 <= diff <= 20.0:
        # Auto-add Tomato Ketchup
        if ketchup_product:
            items_to_create.append(
                OrderItem(
                    product_id=ketchup_product.id,
                    quantity=1,
                    price=diff
                )
            )
            subtotal = val_min
            original_total = round(original_total + diff, 2)
    elif has_ketchup and ketchup_product:
        items_to_create.append(
            OrderItem(
                product_id=ketchup_product.id,
                quantity=ketchup_qty,
                price=diff
            )
        )
        subtotal = val_min
        original_total = round(original_total + diff, 2)
        
    coupon_applied = None
    # Use centralized total calculation
    from .services.total_calculator import calculate_total_payable
    try:
        total_payable, service_charge, coupon_applied = calculate_total_payable(
            subtotal=subtotal,
            val_min=val_min,
            val_max=val_max,
            val_fixed=val_fixed,
            discount_total=discount_total,
            user=user,
            db=db,
            newbie_coupon_cfg=newbie_coupon_cfg,
            welcome_coupon_cfg=welcome_coupon_cfg,
            coupon_code=payload.coupon_code
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # 2. Process Payments
    txn_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
    order_id = f"PIZZA-{uuid.uuid4().hex[:8].upper()}"
    
    if payload.payment_method == "wallet":
        if user.wallet_balance < total_payable:
            # Register payment failure log
            err = ErrorLog(
                type="payment",
                message=f"Insufficient wallet balance. User: {user.display_name}, Order: {order_id}, Cost: {total_payable}, Balance: {user.wallet_balance}"
            )
            db.add(err)
            db.commit()
            raise HTTPException(status_code=400, detail="Insufficient wallet balance")
            
        # Deduct wallet balance
        user.wallet_balance -= total_payable
    elif payload.payment_method == "direct":
        # Simulates UPI / Card Payment Pending
        pass
    else:
        raise HTTPException(status_code=400, detail="Invalid payment method")
        
    # 3. Create Order
    initial_status = "Payment Received" if payload.payment_method == "wallet" else "Payment Pending"
    order = Order(
        id=order_id,
        user_id=user.id,
        transaction_id=txn_id,
        payment_method=payload.payment_method,
        original_total=original_total,
        discount=discount_total,
        service_charge=service_charge,
        total_payable=total_payable,
        coupon_applied=coupon_applied,
        status=initial_status,
        address=payload.address,
        landmark=payload.landmark,
        latitude=payload.latitude,
        longitude=payload.longitude,
        phone=payload.phone,
        device_id=payload.device_id,
        device_details=payload.device_details,
        estimated_delivery=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(minutes=30)
    )
    db.add(order)
    db.flush() # Establish order structure to populate foreign keys
    
    for order_item in items_to_create:
        order_item.order_id = order.id
        db.add(order_item)
        
    # Initial status history
    h1 = OrderStatusHistory(order_id=order.id, status=initial_status)
    db.add(h1)
    db.flush()
    
    if payload.payment_method == "direct":
        # Return payment QR details for direct checkout
        db.commit()
        upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
        upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
        upi_id = upi_id_cfg.value if upi_id_cfg else "dominos@upi"
        upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
        
        upi_details = generate_upi_qr_details(upi_id, upi_name, total_payable, order.id, f"Order {order.id}")
        upi_data = upi_details["upi_uri"]
        qr_code_url = upi_details["qr_data_url"]  # High-res instant base64 data URL for frontend
        bot_qr_url = upi_details["qr_code_url"]   # High-res HTTP URL for Telegram bot photo
        
        # Log generated QR to history
        qr_hist = QRGenerationHistory(
            order_id=order.id,
            user_id=user.id,
            upi_uri=upi_data,
            amount=total_payable,
            qr_code_url=qr_code_url,
            created_at=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        )
        db.add(qr_hist)
        db.commit()
        
        caption_text = (
            f"💳 <b>Domino's Order Engine UPI Payment QR Code</b>\n"
            f"Order ID: <code>{order.id}</code>\n"
            f"Payable Amount: <b>₹{total_payable:.2f}</b>\n\n"
            f"Scan the QR code to pay, then submit your 12-digit UPI UTR number in the Mini App to verify and start processing your order!"
        )
        await send_bot_photo(user.telegram_id, bot_qr_url, caption_text)
        
        if sse_broadcast_callback:
            await sse_broadcast_callback({
                "type": "new_order",
                "order_id": order.id,
                "total": total_payable,
                "user": user.display_name,
                "status": order.status
            })
            await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": order.status})
            
        return {
            "order_id": order.id,
            "transaction_id": txn_id,
            "total": total_payable,
            "status": order.status,
            "coupon_applied": order.coupon_applied,
            "upi_uri": upi_data,
            "qr_code_url": qr_code_url,
            "upi_id": upi_id,
            "invoice": {
                "order_id": order.id,
                "date": order.created_at.isoformat(),
                "items": [{"name": i.product.name, "qty": i.quantity, "price": i.price} for i in order.items],
                "subtotal": original_total,
                "discount": discount_total,
                "service_charge": service_charge,
                "total": total_payable
            }
        }
    
    # 4. Gift Card Allocation Logic (Wallet Flow)
    # Pull the oldest available gift card that matches or exceeds order payable, or simply any available gift card.
    gift_card = db.query(GiftCard).filter(
        GiftCard.status == "available",
        GiftCard.value >= total_payable
    ).order_by(GiftCard.created_at.asc()).first()
    
    # Fallback to oldest available gift card if no card covers the entire amount
    if not gift_card:
        gift_card = db.query(GiftCard).filter(
            GiftCard.status == "available"
        ).order_by(GiftCard.created_at.asc()).first()
        
    if not gift_card:
        # Halt Order Status, Notify User & Admin!
        order.status = "Payment Received" # Retain status as payment received, but cannot progress
        
        # Log Gift Card Failure
        err = ErrorLog(
            type="giftcard",
            message=f"Gift Card Exhausted! Cannot allocate card for Order: {order.id}. Order value: {total_payable}."
        )
        db.add(err)
        db.commit()
        
        # Inform customer
        await send_bot_message(
            user.telegram_id,
            f"⚠️ <b>Order Status Notification</b>\n"
            f"Your order <code>{order.id}</code> has been accepted and is currently being processed. "
            f"Our dispatch team has been notified and we will update you shortly!"
        )
        
        # Broadcast to Admin Dashboard
        if sse_broadcast_callback:
            await sse_broadcast_callback({
                "type": "error_alert",
                "message": f"Critical: Gift card inventory is empty! Order {order.id} requires card of value ₹{total_payable:.2f}."
            })
            await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": order.status})
            

        return {
            "order_id": order.id,
            "status": order.status,
            "message": "Payment verified but gift card inventory empty. Order paused."
        }
        
    # Allocate Gift Card successfully
    gift_card.status = "used"
    gift_card.used_by_user_id = user.id
    gift_card.used_in_order_id = order.id
    gift_card.used_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    
    order.gift_card_id = gift_card.id
    
    # Move Status to Gift Card Applied
    order.status = "Gift Card Applied"
    h2 = OrderStatusHistory(order_id=order.id, status="Gift Card Applied")
    db.add(h2)
    
    # Instantly progress to Order Processing
    order.status = "Order Processing"
    h3 = OrderStatusHistory(order_id=order.id, status="Order Processing")
    db.add(h3)
    
    # Store admin audit log
    admin_log = AuditLog(
        admin_id=None,
        action="GIFT_CARD_APPLIED",
        details=json.dumps({
            "order_id": order.id,
            "user_id": user.id,
            "card_id": gift_card.id,
            "card_value": gift_card.value
        })
    )
    db.add(admin_log)
    db.commit()
    
    try:
        from .services.dominos_service import submit_dominos_order
        await submit_dominos_order(order, db)
    except Exception as e:
        # Rollback database changes and refund
        if payload.payment_method == "wallet":
            user.wallet_balance += total_payable
            
        gift_card.status = "available"
        gift_card.used_by_user_id = None
        gift_card.used_in_order_id = None
        gift_card.used_at = None
        
        order.status = "Failed"
        h_fail = OrderStatusHistory(order_id=order.id, status="Failed", note=f"Auto-submission failed: {str(e)}")
        db.add(h_fail)
        
        err = ErrorLog(
            type="integration",
            message=f"Failed to submit order {order.id} to Domino's automatically during checkout: {e}",
            stack_trace=traceback.format_exc()
        )
        db.add(err)
        db.commit()
        
        # Broadcast failure to SSE
        if sse_broadcast_callback:
            await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Failed"})
            
        await send_bot_message(
            user.telegram_id, 
            f"❌ <b>Order Submission Failed</b>\n"
            f"We were unable to place your order <code>{order.id}</code> on Domino's.\n"
            f"Your payment of <b>₹{total_payable:.2f}</b> has been refunded to your wallet."
        )
        raise HTTPException(status_code=500, detail=f"Domino's submission failed: {str(e)}")
    
    # Send user success notification (Redacted coupon code/pin per privacy policy)
    await send_bot_message(
        user.telegram_id, 
        f"💳 <b>Payment Confirmed!</b>\n"
        f"We received your payment of <b>₹{total_payable:.2f}</b> for Order ID: <code>{order.id}</code> (Transaction: <code>{txn_id}</code>).\n\n"
        f"👩‍🍳 <b>Order Status: Processing</b>\n"
        f"Your order is now being dispatched to the kitchen. Estimated delivery in 30 minutes!"
    )
    
    # Real-time SSE updates for admin
    if sse_broadcast_callback:
        await sse_broadcast_callback({
            "type": "new_order",
            "order_id": order.id,
            "total": total_payable,
            "user": user.display_name
        })
        await sse_broadcast_callback({"type": "order_update"})
        
    return {
        "order_id": order.id,
        "transaction_id": txn_id,
        "total": total_payable,
        "status": order.status,
        "coupon_applied": order.coupon_applied,
        "invoice": {
            "order_id": order.id,
            "date": order.created_at.isoformat(),
            "items": [{"name": i.product.name, "qty": i.quantity, "price": i.price} for i in order.items],
            "subtotal": original_total,
            "discount": discount_total,
            "service_charge": service_charge,
            "total": total_payable,
            "gift_card": {"value": gift_card.value}
        }
    }


async def geocode_address(address: str) -> tuple:
    """Geocode address string to get (lat, lon) using OpenStreetMap Nominatim."""
    import urllib.parse
    import httpx
    url = f"https://nominatim.openstreetmap.org/search?q={urllib.parse.quote(address)}&format=json&limit=1"
    headers = {"User-Agent": "DominosOrderEngineApp/2.0"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, timeout=8.0)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        logger.error(f"Error geocoding address ({address}): {e}")
    # Return None, None if geocoding fails (prevent fake mock location fallback)
    return None, None


@router.post("/dominos/order")
async def place_dominos_order(payload: dict, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Place an order via Domino's browser automation.
    Expected payload keys: lat, lon, items [{"dominos_id": str, "quantity": int}], address {"pin": str, "text": str}, receiver {"name": str, "mobile": str}, payment_method ("cod" or "online").
    """
    required = ["lat", "lon", "items", "address", "receiver", "payment_method"]
    for key in required:
        if not payload.get(key):
            raise HTTPException(status_code=400, detail=f"Missing {key}")
    # Find store based on location
    browser = DominosBrowser()
    store = await browser.find_nearest_store(payload["lat"], payload["lon"], db)
    # Add items to cart
    await browser.add_to_cart(store["store_id"], payload["items"], db)
    # Place order
    order_result = await browser.place_order({
        "store_id": store["store_id"],
        "items": payload["items"],
        "address": payload["address"],
        "receiver": payload["receiver"],
        "payment_method": payload["payment_method"]
    }, db)
    # Notify user via WebSocket
    try:
        from .main import ws_manager
        await ws_manager.send_to_user(user.id, {"type": "order_placed", "result": order_result})
    except Exception as e:
        logger.error(f"WebSocket order notify failed: {e}")
    return order_result


@router.post("/order/auto")
async def auto_order(payload: dict, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """
    High‑level wrapper that runs the whole flow:
    1️⃣ Find nearest store from address text.
    2️⃣ Pull the menu (first 10 items for demo).
    3️⃣ Add a couple of items to the cart.
    4️⃣ Checkout (solves reCAPTCHA via 2Captcha).
    Returns the order confirmation JSON.
    """
    address_text = payload.get("address", {}).get("text", "")
    if not address_text:
        raise HTTPException(status_code=400, detail="Address text is required")
        
    # Geocode address → coordinates.
    lat, lng = await geocode_address(address_text)
    if lat is None or lng is None:
        raise HTTPException(status_code=400, detail="Could not resolve coordinates for address")

    # 1️⃣ Nearest store
    browser = DominosBrowser()
    store = await browser.find_nearest_store(lat, lng, db)

    # 2️⃣ Get menu (first 10 items)
    menu = await browser.fetch_menu(store["store_id"], page=1, limit=10, db=db)

    # 3️⃣ Pick first two items for a demo order
    items = [{"dominos_id": m["dominos_id"], "quantity": 1} for m in menu[:2]]

    # 4️⃣ Checkout
    checkout_payload = {
        "store_id": store["store_id"],
        "address": payload["address"],
        "receiver": payload.get("receiver", {"name": "Default User", "mobile": "9876543210"}),
        "payment_method": payload.get("payment_method", "cod"),
        "items": items,
    }
    result = await browser.place_order(checkout_payload, db)
    return result


@router.post("/cart/calculate")
@router.post("/api/v1/cart/calculate")
async def calculate_cart_pricing(payload: dict, db: Session = Depends(get_db), current_user: Optional[User] = Depends(get_current_user_optional)):
    """
    Real-time cart pricing calculation endpoint.
    Accepts items list, city/location, and optional coupon code.
    Returns subtotal, discount total, location multiplier, bot service fee, applied coupon, and total payable amount.
    """
    items_raw = payload.get("items", [])
    city = payload.get("city") or payload.get("location")
    coupon_code = payload.get("coupon_code")
    
    # Get location pricing multiplier if city provided
    multiplier = 1.0
    if city:
        loc = db.query(LocationPricing).filter(LocationPricing.city.ilike(city)).first()
        if loc:
            multiplier = loc.price_multiplier

    subtotal = 0.0
    discount_total = 0.0
    item_details = []

    for item in items_raw:
        product_id = item.get("product_id") or item.get("id")
        qty = int(item.get("quantity", 1))
        
        product = None
        if product_id:
            product = db.query(Product).filter(Product.id == str(product_id)).first()
        if not product and item.get("name"):
            product = db.query(Product).filter(Product.name.ilike(item["name"])).first()
            
        if product:
            orig_price = float(round(product.original_price * multiplier))
            disc_price = float(round(product.discounted_price * multiplier)) if product.discounted_price else orig_price
            
            unit_price = disc_price
            line_subtotal = round(unit_price * qty, 2)
            subtotal += line_subtotal
            if product.discounted_price:
                discount_total += round((orig_price - disc_price) * qty, 2)
                
            item_details.append({
                "product_id": product.id,
                "name": product.name,
                "quantity": qty,
                "unit_price": unit_price,
                "original_price": orig_price,
                "subtotal": line_subtotal
            })

    from .services.total_calculator import calculate_total_payable
    newbie_cfg = db.query(SystemConfig).filter(SystemConfig.key == "newbie_coupon").first()
    welcome_cfg = db.query(SystemConfig).filter(SystemConfig.key == "welcome_coupon").first()
    min_cfg = db.query(SystemConfig).filter(SystemConfig.key == "cart_promo_min").first()
    max_cfg = db.query(SystemConfig).filter(SystemConfig.key == "cart_promo_max").first()
    fixed_cfg = db.query(SystemConfig).filter(SystemConfig.key == "cart_promo_fixed").first()

    val_min = float(min_cfg.value) if min_cfg else 180.0
    val_max = float(max_cfg.value) if max_cfg else 220.0
    val_fixed = float(fixed_cfg.value) if fixed_cfg else 100.0

    total_payable, service_charge, coupon_applied = calculate_total_payable(
        subtotal=subtotal,
        val_min=val_min,
        val_max=val_max,
        val_fixed=val_fixed,
        discount_total=discount_total,
        user=current_user,
        db=db,
        newbie_coupon_cfg=newbie_cfg,
        welcome_coupon_cfg=welcome_cfg,
        coupon_code=coupon_code
    )

    return {
        "items": item_details,
        "subtotal": round(subtotal, 2),
        "discount_total": round(discount_total, 2),
        "location_multiplier": multiplier,
        "service_charge": service_charge,
        "coupon_applied": coupon_applied,
        "total_payable": total_payable
    }




@router.get("/orders/active")
def get_active_orders(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Fetches in-progress orders for tracking."""
    orders = db.query(Order).filter(
        Order.user_id == user.id,
        Order.status.notin_(["Completed", "Cancelled", "Refunded"])
    ).order_by(Order.created_at.desc()).all()
    
    result = []
    for o in orders:
        items = [{"name": i.product.name, "quantity": i.quantity, "price": i.price, "image": i.product.image_url} for i in o.items]
        timeline = [{"status": h.status, "time": h.created_at.isoformat()} for h in o.status_history]
        
        result.append({
            "id": o.id,
            "status": o.status,
            "payable": o.total_payable,
            "payment_method": o.payment_method,
            "created_at": o.created_at.isoformat(),
            "estimated_delivery": o.estimated_delivery.isoformat() if o.estimated_delivery else None,
            "items": items,
            "timeline": timeline,
            "has_gift_card": o.gift_card_id is not None,
            "gift_card_value": o.gift_card.value if o.gift_card else 0.0,
            "coupon_applied": o.coupon_applied
        })
    return result

@router.get("/orders/{order_id}")
def get_order_details(order_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Gets all tracking details for a specific order."""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
        
    # Check permissions
    if order.user_id != user.id and user.role != "admin":
        raise HTTPException(status_code=403, detail="Access denied")
        
    items = []
    for i in order.items:
        items.append({
            "name": i.product.name,
            "quantity": i.quantity,
            "price": i.price,
            "image": i.product.image_url
        })
        
    timeline = []
    for h in sorted(order.status_history, key=lambda x: x.created_at):
        timeline.append({
            "status": h.status,
            "time": h.created_at.isoformat()
        })
        
    # Decrypt gift card details if admin or user
    gift_card_info = None
    if order.gift_card:
        gift_card_info = {
            "code": "REDACTED",
            "pin": "REDACTED",
            "value": order.gift_card.value
        }

    return {
        "id": order.id,
        "transaction_id": order.transaction_id,
        "payment_method": order.payment_method,
        "status": order.status,
        "original_total": order.original_total,
        "discount": order.discount,
        "service_charge": order.service_charge,
        "total_payable": order.total_payable,
        "address": order.address,
        "landmark": order.landmark,
        "latitude": order.latitude,
        "longitude": order.longitude,
        "phone": order.phone,
        "created_at": order.created_at.isoformat(),
        "estimated_delivery": order.estimated_delivery.isoformat() if order.estimated_delivery else None,
        "coupon_applied": order.coupon_applied,
        "items": items,
        "timeline": timeline,
        "gift_card": gift_card_info
    }

# --- SUPPORT LIVE CHAT ---

@router.get("/support/messages")
def get_support_messages(user_id: Optional[int] = None, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """
    Fetches chat messages. Customers only fetch their own messages.
    Admins can supply a user_id query parameter to load history for that user.
    """
    target_user_id = current_user.id
    if current_user.role == "admin" and user_id is not None:
        target_user_id = user_id
        
    messages = db.query(SupportMessage).filter(
        SupportMessage.user_id == target_user_id
    ).order_by(SupportMessage.created_at.asc()).all()
    
    # Mark messages as read
    unread_messages = [m for m in messages if not m.is_read and (
        (current_user.role == "admin" and m.sender_type == "user") or
        (current_user.role != "admin" and m.sender_type == "admin")
    )]
    for m in unread_messages:
        m.is_read = True
    if unread_messages:
        db.commit()

    return messages

@router.post("/support/messages")
async def send_support_message(payload: SupportMessageSend, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """Sends a chat message, pushing notifications via Telegram and SSE."""
    if current_user.role == "admin":
        if not payload.recipient_id:
            raise HTTPException(status_code=400, detail="recipient_id required for admin replies")
        target_user_id = payload.recipient_id
        sender_type = "admin"
    else:
        target_user_id = current_user.id
        sender_type = "user"
        
    target_user = db.query(User).filter(User.id == target_user_id).first()
    if not target_user:
        raise HTTPException(status_code=404, detail="Recipient user not found")
        
    msg = SupportMessage(
        user_id=target_user_id,
        sender_type=sender_type,
        message=payload.message
    )
    db.add(msg)
    db.commit()
    
    # If admin reply, send it via Telegram Bot
    if sender_type == "admin":
        await send_bot_message(
            target_user.telegram_id,
            f"💬 <b>Support Agent Reply:</b>\n{payload.message}"
        )
        
    # Trigger SSE update
    if sse_broadcast_callback:
        await sse_broadcast_callback({
            "type": "support_message",
            "user_id": target_user_id,
            "sender_type": sender_type,
            "message": payload.message,
            "created_at": msg.created_at.isoformat()
        })
        
    return msg

# --- ADMIN PANEL ROUTES ---

@router.get("/admin/support-messages")
def get_all_support_messages(user_id: Optional[str] = None, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Admin view of all support messages. Optionally filter by user_id."""
    from .database import SupportMessage
    q = db.query(SupportMessage)
    if user_id:
        q = q.filter(SupportMessage.user_id == user_id)
    messages = q.order_by(SupportMessage.created_at.desc()).limit(200).all()
    result = []
    for m in messages:
        u = db.query(User).filter(User.id == m.user_id).first()
        result.append({
            "id": m.id,
            "user_id": m.user_id,
            "user_display_name": u.display_name if u else "Unknown",
            "user_telegram_id": u.telegram_id if u else None,
            "sender_type": m.sender_type,
            "message": m.message,
            "is_read": m.is_read,
            "created_at": m.created_at.isoformat(),
        })
    return result


class AdminSupportReplyPayload(BaseModel):
    user_id: str
    message: str

@router.post("/admin/support-reply")
async def admin_support_reply(payload: AdminSupportReplyPayload, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .database import SupportMessage
    from .bot import send_bot_message
    
    target_user = db.query(User).filter(User.id == payload.user_id).first()
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")
        
    msg = SupportMessage(
        user_id=payload.user_id,
        sender_type="admin",
        message=payload.message
    )
    db.add(msg)
    db.commit()
    
    # Send it to the user via Telegram Bot
    await send_bot_message(
        target_user.telegram_id,
        f"💬 <b>Support Agent Reply:</b>\n{payload.message}"
    )
    
    # Broadcast SSE update
    from .routes import sse_broadcast_callback
    if sse_broadcast_callback:
        try:
            await sse_broadcast_callback({
                "type": "support_message",
                "user_id": payload.user_id,
                "sender_type": "admin",
                "message": payload.message,
                "created_at": msg.created_at.isoformat()
            })
        except Exception:
            pass
            
    return {"status": "success", "message": "Reply sent successfully"}


@router.get("/admin/stats")
def get_admin_stats(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Computes aggregated live metrics for the dashboard."""
    # Sales Revenue
    revenue = db.query(func.sum(Order.total_payable)).filter(Order.status != "Cancelled", Order.status != "Refunded").scalar() or 0.0
    
    # Active Users (logged in)
    active_users = db.query(User).count()
    
    # Online Users (active in the last 5 minutes)
    five_mins_ago = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=5)
    online_users = db.query(func.count(func.distinct(UserSession.user_id))).filter(
        UserSession.last_active >= five_mins_ago,
        UserSession.is_active == True
    ).scalar() or 0
    
    # Active Orders
    active_orders = db.query(Order).filter(Order.status.notin_(["Completed", "Cancelled", "Refunded"])).count()
    
    # Completed Orders
    completed_orders = db.query(Order).filter(Order.status == "Completed").count()
    
    # Failed Payments
    failed_payments = db.query(ErrorLog).filter(ErrorLog.type == "payment").count()
    
    # Refunds
    refunds_count = db.query(Order).filter(Order.status == "Refunded").count()
    refund_amount = db.query(func.sum(Order.total_payable)).filter(Order.status == "Refunded").scalar() or 0.0
    
    # Wallet balances total
    wallet_total = db.query(func.sum(User.wallet_balance)).scalar() or 0.0
    
    # Gift Cards Count
    avail_gc = db.query(GiftCard).filter(GiftCard.status == "available").count()
    used_gc = db.query(GiftCard).filter(GiftCard.status == "used").count()

    return {
        "revenue": round(revenue, 2),
        "active_users": active_users,
        "online_users": online_users,
        "active_orders": active_orders,
        "completed_orders": completed_orders,
        "failed_payments": failed_payments,
        "refunds": {
            "count": refunds_count,
            "total_amount": round(refund_amount, 2)
        },
        "wallet_balances_total": round(wallet_total, 2),
        "gift_cards": {
            "available": avail_gc,
            "used": used_gc
        }
    }

@router.get("/admin/orders")
def get_all_orders(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    orders = db.query(Order).join(User, Order.user_id == User.id).filter(
        ~User.username.like("buyer%"),
        ~User.username.like("pizzabuyer%"),
        ~User.telegram_id.like("9999%"),
        ~Order.id.like("TOPUP-%")
    ).order_by(Order.created_at.desc()).all()
    result = []
    for o in orders:
        customer = db.query(User).filter(User.id == o.user_id).first()
        result.append({
            "id": o.id,
            "customer_name": customer.display_name if customer else "Unknown",
            "customer_id": o.user_id,
            "total": o.total_payable,
            "status": o.status,
            "payment_method": o.payment_method,
            "created_at": o.created_at.isoformat(),
            "estimated_delivery": o.estimated_delivery.isoformat() if o.estimated_delivery else None
        })
    return result

@router.get("/admin/dashboard")
def get_admin_dashboard(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    orders = db.query(Order).join(User, Order.user_id == User.id).filter(
        ~User.username.like("buyer%"),
        ~User.username.like("pizzabuyer%"),
        ~User.telegram_id.like("9999%"),
        ~Order.id.like("TOPUP-%")
    ).order_by(Order.created_at.desc()).all()
    orders_result = []
    for o in orders:
        customer = db.query(User).filter(User.id == o.user_id).first()
        from .services.order_processor import serialize_order
        order_dict = serialize_order(o)
        order_dict["user_display_name"] = customer.display_name if customer else "Unknown"
        order_dict["user"] = {
            "id": customer.id if customer else None,
            "display_name": customer.display_name if customer else "Unknown",
            "telegram_id": customer.telegram_id if customer else None,
            "username": customer.username if customer else None,
        }
        orders_result.append(order_dict)
        
    users = db.query(User).filter(
        ~User.username.like("buyer%"),
        ~User.username.like("pizzabuyer%"),
        ~User.telegram_id.like("9999%")
    ).all()
    users_result = []
    for u in users:
        active_sessions = db.query(UserSession).filter(UserSession.user_id == u.id, UserSession.is_active == True).count()
        users_result.append({
            "id": u.id,
            "telegram_id": u.telegram_id,
            "username": u.username,
            "display_name": u.display_name,
            "photo_url": u.photo_url,
            "wallet_balance": u.wallet_balance,
            "role": u.role,
            "is_blocked": u.is_blocked,
            "created_at": u.created_at.isoformat(),
            "active_sessions": active_sessions
        })
        
    return {
        "orders": orders_result,
        "users": users_result
    }

@router.put("/admin/orders/{order_id}/status")
async def update_order_status(
    order_id: str,
    payload: OrderStatusUpdateSchema,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """Updates order status, handles refunds, and broadcasts real-time changes."""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
        
    old_status = order.status
    new_status = payload.status
    
    order.status = new_status
    
    # Process Refund if status changed to Refunded
    if new_status == "Refunded" and old_status != "Refunded":
        # Return funds to customer wallet
        customer = db.query(User).filter(User.id == order.user_id).first()
        if customer:
            customer.wallet_balance += order.total_payable
            # Audit log
            await log_admin_action(
                db, admin.id, admin.username, "REFUND_APPROVED",
                {"order_id": order.id, "amount": order.total_payable, "user": customer.display_name},
                request
            )
            
    if payload.estimated_delivery_minutes is not None:
        order.estimated_delivery = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(minutes=payload.estimated_delivery_minutes)
        
    h = OrderStatusHistory(order_id=order.id, status=new_status)
    db.add(h)
    
    if new_status == "Order Processing":
        try:
            from .services.dominos_service import submit_dominos_order
            await submit_dominos_order(order, db)
        except Exception as e:
            err = ErrorLog(
                type="integration",
                message=f"Failed to submit order {order.id} to Domino's automatically: {e}",
                stack_trace=traceback.format_exc()
            )
            db.add(err)
            
    db.commit()
    
    # Log administrative audit trail
    await log_admin_action(
        db, admin.id, admin.username, "ORDER_UPDATED",
        {"order_id": order.id, "old_status": old_status, "new_status": new_status},
        request
    )
    
    # Notify customer via Telegram Bot
    customer_user = db.query(User).filter(User.id == order.user_id).first()
    if customer_user:
        status_emojis = {
            "Order Accepted": "✅",
            "Preparing": "👩‍🍳",
            "Out for Delivery": "🛵",
            "Delivered": "📦",
            "Completed": "🎉",
            "Cancelled": "❌",
            "Refunded": "💰"
        }
        emoji = status_emojis.get(new_status, "🔔")
        msg_text = (
            f"{emoji} <b>Order Status Update: {new_status}</b>\n\n"
            f"Your order <code>{order.id}</code> has transitioned to: <b>{new_status}</b>.\n\n"
            f"<b>Progress:</b>\n{get_order_progress_bar(new_status)}"
        )
        if new_status == "Out for Delivery":
            msg_text += f"\n\nEstimated delivery: <code>{order.estimated_delivery.strftime('%I:%M %p') if order.estimated_delivery else 'soon'}</code>."
            
        elif new_status == "Refunded":
            msg_text += f"\n\nAn amount of <b>₹{order.total_payable:.2f}</b> has been credited back to your wallet."
            
        markup = {
            "inline_keyboard": [
                [{"text": "💬 Contact Support", "url": "https://t.me/dominosordersHELP_bot"}]
            ]
        }
        await send_bot_message(customer_user.telegram_id, msg_text, reply_markup=markup)
        
    # Broadcast to app clients via SSE
    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": new_status})
        
    return {"status": "success"}

@router.get("/admin/gift-cards")
@router.get("/admin/giftcards")
def get_gift_cards(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    cards = db.query(GiftCard).all()
    result = []
    for c in cards:
        result.append({
            "id": c.id,
            "code": decrypt_data(c.code_encrypted),
            "pin": decrypt_data(c.pin_encrypted),
            "value": c.value,
            "status": c.status,
            "used_by_user_id": c.used_by_user_id,
            "used_in_order_id": c.used_in_order_id,
            "used_at": c.used_at.isoformat() if c.used_at else None,
            "uploaded_at": c.created_at.isoformat()
        })
    return result

@router.post("/admin/gift-cards/upload")
@router.post("/admin/giftcards/upload")
async def upload_gift_cards(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """Uploads and parses a PDF or Spreadsheet spreadsheet file containing gift cards."""
    file_ext = os.path.splitext(file.filename)[1]
    temp_filename = f"gc_temp_{uuid.uuid4()}{file_ext}"
    temp_filepath = os.path.join(UPLOAD_DIR, temp_filename)
    
    with open(temp_filepath, "wb") as f:
        f.write(await file.read())
        
    try:
        parsed_cards = parse_gift_card_file(temp_filepath, file.filename)
        os.remove(temp_filepath)
    except Exception as e:
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
        # Log failure
        err = ErrorLog(
            type="giftcard",
            message=f"Gift Card File Parsing Failed: {str(e)}",
            stack_trace=traceback.format_exc()
        )
        db.add(err)
        db.commit()
        raise HTTPException(status_code=400, detail=f"File parsing error: {str(e)}")

    added_count = 0
    duplicate_count = 0
    
    # Store with SHA256 hashed code lookup for uniqueness
    for card in parsed_cards:
        code_hash = hashlib.sha256(card["code"].encode("utf-8")).hexdigest()
        
        # Check uniqueness
        exists = db.query(GiftCard).filter(GiftCard.code_hash == code_hash).first()
        if exists:
            duplicate_count += 1
            continue
            
        gc = GiftCard(
            code_encrypted=encrypt_data(card["code"]),
            code_hash=code_hash,
            pin_encrypted=encrypt_data(card["pin"]),
            value=card["value"],
            status="available"
        )
        db.add(gc)
        added_count += 1
        
    db.commit()
    
    # Admin audit trail log
    await log_admin_action(
        db, admin.id, admin.username, "GIFT_CARD_UPLOADED",
        {"filename": file.filename, "extracted": len(parsed_cards), "added": added_count, "duplicates": duplicate_count},
        request
    )
    
    return {
        "status": "success",
        "extracted": len(parsed_cards),
        "added": added_count,
        "duplicates": duplicate_count
    }

class GiftCardManualAddPayload(BaseModel):
    code: str
    pin: str
    value: float

class GiftCardsBulkAddPayload(BaseModel):
    text_data: str

@router.post("/admin/gift-cards/add-manual")
@router.post("/admin/giftcards/add-manual")
async def add_gift_card_manual(payload: GiftCardManualAddPayload, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    code = payload.code.strip()
    pin = payload.pin.strip()
    value = payload.value
    
    if not (code.isdigit() and len(code) == 5):
        raise HTTPException(status_code=400, detail="Gift card code must be exactly 5 digits.")
    if not (pin.isdigit() and len(pin) == 6):
        raise HTTPException(status_code=400, detail="PIN must be exactly 6 digits.")
    if value <= 0:
        raise HTTPException(status_code=400, detail="Value must be a positive number.")
        
    code_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
    exists = db.query(GiftCard).filter(GiftCard.code_hash == code_hash).first()
    if exists:
        raise HTTPException(status_code=400, detail="This gift card code already exists in the inventory.")
        
    gc = GiftCard(
        code_encrypted=encrypt_data(code),
        code_hash=code_hash,
        pin_encrypted=encrypt_data(pin),
        value=value,
        status="available"
    )
    db.add(gc)
    db.commit()
    
    await log_admin_action(
        db, admin.id, admin.username, "GIFT_CARD_ADDED_MANUALLY",
        {"code_hash": code_hash[:8], "value": value},
        request
    )
    return {"status": "success", "message": f"Gift card code {code[:4]}... added successfully."}

@router.post("/admin/gift-cards/add-bulk")
@router.post("/admin/giftcards/add-bulk")
async def add_gift_cards_bulk(payload: GiftCardsBulkAddPayload, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    lines = payload.text_data.strip().split("\n")
    added_count = 0
    duplicate_count = 0
    errors = []
    
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) == 2:
            code, pin = parts[0].strip(), parts[1].strip()
            val = 100.0
        elif len(parts) == 3:
            code, pin, val_str = parts[0].strip(), parts[1].strip(), parts[2].strip()
            try:
                val = float(val_str)
                if val <= 0:
                    raise ValueError()
            except ValueError:
                errors.append(f"Line {line_num}: Value must be a positive number.")
                continue
        else:
            errors.append(f"Line {line_num}: Must contain 'code,pin' or 'code,pin,value'")
            continue
        if not (code.isdigit() and len(code) == 5):
            errors.append(f"Line {line_num}: Code must be 5 digits.")
            continue
        if not (pin.isdigit() and len(pin) == 6):
            errors.append(f"Line {line_num}: PIN must be 6 digits.")
            continue
            
        code_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()
        exists = db.query(GiftCard).filter(GiftCard.code_hash == code_hash).first()
        if exists:
            duplicate_count += 1
            continue
            
        gc = GiftCard(
            code_encrypted=encrypt_data(code),
            code_hash=code_hash,
            pin_encrypted=encrypt_data(pin),
            value=val,
            status="available"
        )
        db.add(gc)
        added_count += 1
        
    db.commit()
    
    await log_admin_action(
        db, admin.id, admin.username, "GIFT_CARD_BULK_ADDED",
        {"added": added_count, "duplicates": duplicate_count, "errors_count": len(errors)},
        request
    )
    
    return {
        "status": "success",
        "added": added_count,
        "duplicates": duplicate_count,
        "errors": errors,
        "message": f"Successfully added {added_count} cards. Duplicates: {duplicate_count}. Errors: {len(errors)}"
    }

@router.get("/admin/users")
def get_all_users(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    users = db.query(User).filter(
        ~User.username.like("buyer%"),
        ~User.username.like("pizzabuyer%"),
        ~User.telegram_id.like("9999%")
    ).all()
    result = []
    for u in users:
        active_sessions = db.query(UserSession).filter(UserSession.user_id == u.id, UserSession.is_active == True).count()
        result.append({
            "id": u.id,
            "telegram_id": u.telegram_id,
            "username": u.username,
            "display_name": u.display_name,
            "photo_url": u.photo_url,
            "wallet_balance": u.wallet_balance,
            "role": u.role,
            "is_blocked": u.is_blocked,
            "created_at": u.created_at.isoformat(),
            "active_sessions": active_sessions,
            "telegram_verified": getattr(u, "telegram_verified", False)
        })
    return result

@router.get("/admin/users/{user_id}/detail")
def get_user_detail(user_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    u = db.query(User).filter(User.id == user_id).first()
    if not u:
        raise HTTPException(status_code=404, detail="User not found")

    # Orders (filter out TOPUP- deposits)
    orders = db.query(Order).filter(Order.user_id == user_id, ~Order.id.like("TOPUP-%")).order_by(Order.created_at.desc()).all()
    orders_data = []
    for o in orders:
        items = db.query(OrderItem).filter(OrderItem.order_id == o.id).all()
        orders_data.append({
            "id": o.id,
            "status": o.status,
            "payment_method": o.payment_method,
            "total_payable": o.total_payable,
            "city": o.city,
            "address": o.address,
            "phone": o.phone,
            "created_at": o.created_at.isoformat(),
            "items": [{"name": i.product.name if i.product else "?", "qty": i.quantity, "price": i.price} for i in items],
        })

    # Saved addresses
    saved_addrs = db.query(SavedAddress).filter(SavedAddress.user_id == user_id).all()
    addresses_data = [
        {"id": a.id, "label": a.label, "full_address": a.full_address, "city": a.city,
         "pincode": a.pincode, "is_default": a.is_default, "created_at": a.created_at.isoformat()}
        for a in saved_addrs
    ]

    # Active sessions
    sessions = db.query(UserSession).filter(UserSession.user_id == user_id).order_by(UserSession.last_active.desc()).all()
    sessions_data = [
        {"id": s.id, "is_active": s.is_active, "ip_address": s.ip_address,
         "created_at": s.created_at.isoformat(), "last_active": s.last_active.isoformat()}
        for s in sessions
    ]

    # Wallet Transactions
    from .database import WalletTransaction
    txs = db.query(WalletTransaction).filter(WalletTransaction.user_id == user_id).order_by(WalletTransaction.created_at.desc()).all()
    txs_data = [{
        "id": t.id,
        "type": t.type,
        "amount": t.amount,
        "description": t.description,
        "created_at": t.created_at.isoformat()
    } for t in txs]

    return {
        "id": u.id,
        "telegram_id": u.telegram_id,
        "username": u.username,
        "display_name": u.display_name,
        "photo_url": u.photo_url,
        "phone": u.phone,
        "wallet_balance": u.wallet_balance,
        "role": u.role,
        "is_blocked": u.is_blocked,
        "city": u.city,
        "state": u.state,
        "latitude": u.latitude,
        "longitude": u.longitude,
        "created_at": u.created_at.isoformat(),
        "total_orders": len(orders_data),
        "total_spent": sum(o["total_payable"] for o in orders_data if o["status"] not in ("Cancelled",)),
        "orders": orders_data,
        "saved_addresses": addresses_data,
        "sessions": sessions_data,
        "wallet_transactions": txs_data
    }

@router.get("/admin/robot-logs")
@router.get("/api/v1/admin/robot-logs")
def get_robot_logs(
    limit: int = 200,
    mobile: str = None,
    level: str = None,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    q = db.query(RobotLog)
    if mobile:
        q = q.filter(RobotLog.mobile_number.ilike(f"%{mobile}%"))
    if level and level.upper() != "ALL":
        q = q.filter(RobotLog.level == level.upper())
    logs = q.order_by(RobotLog.created_at.desc()).limit(limit).all()
    return [
        {
            "id": l.id,
            "session_id": l.session_id,
            "mobile_number": l.mobile_number,
            "level": l.level,
            "stage": l.stage,
            "message": l.message,
            "details": l.details,
            "created_at": l.created_at.isoformat(),
        }
        for l in logs
    ]

@router.delete("/admin/robot-logs")
@router.delete("/api/v1/admin/robot-logs")
async def clear_robot_logs(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    db.query(RobotLog).delete()
    db.commit()
    return {"status": "success", "message": "All robot logs cleared"}


@router.post("/admin/users")
async def create_user(
    payload: UserCreateRequest,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    # Check if telegram_id exists
    exists = db.query(User).filter(User.telegram_id == payload.telegram_id).first()
    if exists:
        raise HTTPException(status_code=400, detail="Telegram ID already exists")
    
    # Check if username exists (if provided)
    if payload.username:
        username_exists = db.query(User).filter(User.username == payload.username).first()
        if username_exists:
            raise HTTPException(status_code=400, detail="Username already exists")
            
    if payload.role not in ["user", "admin"]:
        raise HTTPException(status_code=400, detail="Invalid role. Must be 'user' or 'admin'")
        
    user = User(
        telegram_id=payload.telegram_id,
        username=payload.username,
        display_name=payload.display_name,
        wallet_balance=payload.wallet_balance,
        role=payload.role
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    
    # Log administrative action
    await log_admin_action(
        db, admin.id, admin.username, "USER_CREATED",
        {"id": user.id, "telegram_id": user.telegram_id, "username": user.username, "role": user.role},
        request
    )
    
    return {
        "id": user.id,
        "telegram_id": user.telegram_id,
        "username": user.username,
        "display_name": user.display_name,
        "wallet_balance": user.wallet_balance,
        "role": user.role,
        "is_blocked": user.is_blocked,
        "created_at": user.created_at.isoformat()
    }

@router.get("/admin/config")
def get_system_config(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    configs = {c.key: c.value for c in db.query(SystemConfig).all()}
    return {
        "newbie_coupon": configs.get("newbie_coupon", "NEWBIE100"),
        "welcome_coupon": configs.get("welcome_coupon", "WELCOME90"),
        "cart_promo_min": float(configs.get("cart_promo_min", 180.0)),
        "cart_promo_max": float(configs.get("cart_promo_max", 220.0)),
        "cart_promo_fixed": float(configs.get("cart_promo_fixed", 100.0)),
        "bot_fee": float(configs.get("bot_fee", 10.0)),
        "upi_id": configs.get("upi_id", "dominos@upi"),
        "upi_name": configs.get("upi_name", "Domino's Order Engine"),
        "platform_name": configs.get("platform_name", "Domino's Order Engine"),

        "mini_app_url": configs.get("mini_app_url", ""),
        "captcha_api_key": configs.get("captcha_api_key", ""),
        "playwright_headless": configs.get("playwright_headless", "true"),
        "activity_timeout": int(configs.get("activity_timeout", 30)),
    }

@router.put("/admin/config")
async def update_system_config(
    payload: SingleConfigSchema,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    key = payload.key.strip()
    val = payload.value.strip()
    
    cfg = db.query(SystemConfig).filter(SystemConfig.key == key).first()
    if cfg:
        cfg.value = val
    else:
        cfg = SystemConfig(key=key, value=val)
        db.add(cfg)
        
    db.commit()
    
    # Audit log
    await log_admin_action(
        db, admin.id, admin.username, "CONFIG_UPDATED",
        {key: val},
        request
    )
    
    return {"status": "success"}

class ProfileChangeRequest(BaseModel):
    new_username: Optional[str] = None
    new_password: Optional[str] = None

@router.put("/admin/change-password")
async def change_admin_profile(
    payload: ProfileChangeRequest,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    global ADMIN_USERNAME, ADMIN_PASSWORD_HASH
    
    if payload.new_username:
        new_un = payload.new_username.strip()
        if len(new_un) < 3:
            raise HTTPException(status_code=400, detail="Username must be at least 3 characters")
        ADMIN_USERNAME = new_un
        admin_db = db.query(User).filter(User.role == "admin").first()
        if admin_db:
            admin_db.username = new_un
            db.commit()
            
    if payload.new_password:
        if len(payload.new_password) < 6:
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
        ADMIN_PASSWORD_HASH = hash_password(payload.new_password)
        
    await log_admin_action(db, admin.id, admin.username, "ADMIN_PROFILE_CHANGED", {
        "new_username": payload.new_username
    }, request)
    return {"status": "success"}


@router.get("/admin/login-attempts")
def get_login_attempts(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    attempts = db.query(LoginAttempt).order_by(LoginAttempt.attempted_at.desc()).limit(50).all()
    result = []
    for a in attempts:
        result.append({
            "id": a.id,
            "username": a.username,
            "ip_address": a.ip_address,
            "status": a.status,
            "attempted_at": a.attempted_at.isoformat()
        })
    return result

class BlockUserRequest(BaseModel):
    blocked: Optional[bool] = None
    is_blocked: Optional[bool] = None

@router.put("/admin/users/{user_id}/block")
async def block_user(user_id: str, request: Request, payload: BlockUserRequest, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    blocked = payload.blocked if payload.blocked is not None else payload.is_blocked
    if blocked is None:
        raise HTTPException(status_code=400, detail="blocked or is_blocked field required")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
        
    if blocked:
        if user.id == admin.id:
            raise HTTPException(status_code=400, detail="Admins cannot block themselves")
        if user.role == "admin":
            raise HTTPException(status_code=400, detail="Cannot block an admin user")
            
    user.is_blocked = blocked
    if blocked:
        # Deactivate all sessions
        db.query(UserSession).filter(UserSession.user_id == user_id).update({"is_active": False})
        
    db.commit()
    
    action = "USER_BLOCKED" if blocked else "USER_UNBLOCKED"
    await log_admin_action(db, admin.id, admin.username, action, {"user_id": user_id, "username": user.username}, request)
    return {"status": "success"}

@router.post("/admin/users/{user_id}/sessions/terminate")
async def terminate_sessions(user_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    admin_password = request.headers.get("X-Admin-Password")
    if not admin_password or not verify_password(ADMIN_PASSWORD_HASH, admin_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin password")
        
    db.query(UserSession).filter(UserSession.user_id == user_id).update({"is_active": False})
    db.commit()
    
    await log_admin_action(db, admin.id, admin.username, "USER_SESSIONS_TERMINATED", {"user_id": user_id}, request)
    return {"status": "success"}

class WalletAdjustmentPayload(BaseModel):
    amount: float
    reason: str = "Admin adjustment"

@router.put("/admin/users/{user_id}/wallet")
def adjust_user_wallet(user_id: str, payload: WalletAdjustmentPayload, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    old_balance = user.wallet_balance
    user.wallet_balance += payload.amount
    
    # Create WalletTransaction
    from .database import WalletTransaction
    tx = WalletTransaction(
        user_id=user.id,
        type="admin_adjustment",
        amount=payload.amount,
        description=payload.reason
    )
    db.add(tx)
    
    db.commit()
    db.refresh(user)
    
    # Log audit
    audit = AuditLog(
        admin_id=admin.id,
        action="WALLET_ADJUSTED",
        details=f"User {user.display_name} ({user_id}) wallet adjusted by {payload.amount:.2f}. Old: {old_balance:.2f}, New: {user.wallet_balance:.2f}. Reason: {payload.reason}"
    )
    db.add(audit)
    db.commit()
    
    # Notify customer via Telegram if callback exists
    try:
        from .bot import send_bot_message
        direction = "credited to" if payload.amount > 0 else "deducted from"
        abs_amount = abs(payload.amount)
        asyncio.create_task(send_bot_message(
            user.telegram_id,
            f"💰 <b>Wallet Balance Adjusted</b>\n\n"
            f"An amount of <b>₹{abs_amount:.2f}</b> has been {direction} your wallet by the administrator.\n"
            f"<b>New Wallet Balance:</b> <b>₹{user.wallet_balance:.2f}</b>"
        ))
    except Exception:
        pass
        
    return {"status": "success", "new_balance": user.wallet_balance}

@router.get("/admin/logs")
def get_system_logs(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    audit_logs = db.query(AuditLog).order_by(AuditLog.created_at.desc()).limit(100).all()
    error_logs = db.query(ErrorLog).order_by(ErrorLog.created_at.desc()).limit(100).all()
    return {
        "audit_logs": audit_logs,
        "error_logs": error_logs
    }

@router.get("/admin/search")
def run_admin_search(q: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Unified search over order IDs, txn IDs, users, statuses, and dates."""
    # Find orders matching q directly or as part of transaction
    orders = db.query(Order).join(User).filter(
        or_(
            Order.id.like(f"%{q}%"),
            Order.transaction_id.like(f"%{q}%"),
            Order.status.like(f"%{q}%"),
            User.username.like(f"%{q}%"),
            User.display_name.like(f"%{q}%")
        )
    ).order_by(Order.created_at.desc()).all()
    
    # Find users matching telegram_id, display_name or username
    users = db.query(User).filter(
        or_(
            User.telegram_id.like(f"%{q}%"),
            User.username.like(f"%{q}%"),
            User.display_name.like(f"%{q}%")
        )
    ).all()
    
    order_results = []
    for o in orders:
        customer = db.query(User).filter(User.id == o.user_id).first()
        order_results.append({
            "id": o.id,
            "customer_name": customer.display_name if customer else "Unknown",
            "total": o.total_payable,
            "status": o.status,
            "payment_method": o.payment_method,
            "created_at": o.created_at.isoformat()
        })
        
    user_results = []
    for u in users:
        user_results.append({
            "id": u.id,
            "telegram_id": u.telegram_id,
            "username": u.username,
            "display_name": u.display_name,
            "wallet_balance": u.wallet_balance,
            "role": u.role,
            "is_blocked": u.is_blocked
        })
        
    return {
        "orders": order_results,
        "users": user_results
    }

@router.get("/config")
def get_public_config(db: Session = Depends(get_db)):
    configs = {c.key: c.value for c in db.query(SystemConfig).all()}
    return {
        "newbie_coupon": configs.get("newbie_coupon", "NEWBIE100"),
        "welcome_coupon": configs.get("welcome_coupon", "WELCOME90"),
        "cart_promo_min": float(configs.get("cart_promo_min", 180.0)),
        "cart_promo_max": float(configs.get("cart_promo_max", 220.0)),
        "cart_promo_fixed": float(configs.get("cart_promo_fixed", 100.0)),
        "bot_fee": float(configs.get("bot_fee", 10.0)),
        "upi_id": configs.get("upi_id", "dominos@upi"),
        "upi_name": configs.get("upi_name", "Domino's Order Engine"),
        "platform_name": configs.get("platform_name", "Domino's Order Engine"),
        "mini_app_url": configs.get("mini_app_url", ""),
    }


# ============================================================
# SAVED ADDRESSES
# ============================================================

class SavedAddressSchema(BaseModel):
    label: str
    full_address: str
    landmark: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_default: bool = False


@router.get("/addresses")
def list_addresses(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """List all saved addresses for the current user."""
    addresses = db.query(SavedAddress).filter(
        SavedAddress.user_id == user.id
    ).order_by(SavedAddress.is_default.desc(), SavedAddress.created_at.desc()).all()
    return [
        {
            "id": a.id,
            "label": a.label,
            "full_address": a.full_address,
            "landmark": a.landmark,
            "city": a.city,
            "state": a.state,
            "pincode": a.pincode,
            "latitude": a.latitude,
            "longitude": a.longitude,
            "is_default": a.is_default,
        }
        for a in addresses
    ]


@router.post("/addresses")
def save_address(payload: SavedAddressSchema, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Save a new delivery address."""
    if payload.is_default:
        # Unset all existing defaults
        db.query(SavedAddress).filter(SavedAddress.user_id == user.id).update({"is_default": False})
    addr = SavedAddress(
        user_id=user.id,
        label=payload.label,
        full_address=payload.full_address,
        landmark=payload.landmark,
        city=payload.city,
        state=payload.state,
        pincode=payload.pincode,
        latitude=payload.latitude,
        longitude=payload.longitude,
        is_default=payload.is_default,
    )
    db.add(addr)
    db.commit()
    db.refresh(addr)
    return {"id": addr.id, "label": addr.label, "full_address": addr.full_address}


@router.delete("/addresses/{address_id}")
def delete_address(address_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    addr = db.query(SavedAddress).filter(SavedAddress.id == address_id, SavedAddress.user_id == user.id).first()
    if not addr:
        raise HTTPException(status_code=404, detail="Address not found")
    db.delete(addr)
    db.commit()
    return {"status": "deleted"}


@router.put("/addresses/{address_id}/default")
def set_default_address(address_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    db.query(SavedAddress).filter(SavedAddress.user_id == user.id).update({"is_default": False})
    addr = db.query(SavedAddress).filter(SavedAddress.id == address_id, SavedAddress.user_id == user.id).first()
    if not addr:
        raise HTTPException(status_code=404, detail="Address not found")
    addr.is_default = True
    db.commit()
    return {"status": "updated"}


# ============================================================
# LOCATION-BASED PRICING
# ============================================================

@router.get("/location/pricing")
async def get_location_pricing(city: str = None, db: Session = Depends(get_db)):
    """Get pricing for a specific city or all cities."""
    if city:
        pricing = db.query(LocationPricing).filter(
            LocationPricing.city.ilike(f"%{city}%")
        ).first()
        if not pricing:
            # Create a default serviceable location pricing record dynamically
            pricing = LocationPricing(
                city=city,
                price_multiplier=1.0,
                delivery_charge=30.0,
                min_order_value=149.0,
                is_serviceable=True
            )
            db.add(pricing)
            db.commit()
            db.refresh(pricing)
            
        # Trigger real-time menu scraping and syncing to database
        try:
            from .services.dominos_service import sync_realtime_menu, sync_realtime_menu_bg
            from .database import Product
            # If we already have products, sync in the background so we respond instantly!
            if db.query(Product).count() > 5:
                asyncio.create_task(sync_realtime_menu_bg(city))
            else:
                await sync_realtime_menu(city, db)
        except Exception as e:
            logger.error(f"Error syncing menu for city {city}: {e}")

        return {
            "city": pricing.city,
            "state": pricing.state,
            "price_multiplier": pricing.price_multiplier,
            "delivery_charge": pricing.delivery_charge,
            "min_order_value": pricing.min_order_value,
            "is_serviceable": pricing.is_serviceable,
        }
    # Return all
    all_pricing = db.query(LocationPricing).all()
    return [
        {
            "id": p.id,
            "city": p.city,
            "state": p.state,
            "price_multiplier": p.price_multiplier,
            "delivery_charge": p.delivery_charge,
            "min_order_value": p.min_order_value,
            "is_serviceable": p.is_serviceable,
        }
        for p in all_pricing
    ]


class LocationPricingSchema(BaseModel):
    city: str
    state: Optional[str] = None
    price_multiplier: float = 1.0
    delivery_charge: float = 30.0
    min_order_value: float = 149.0
    is_serviceable: bool = True


@router.post("/api/v1/admin/location-pricing")
def create_location_pricing(
    payload: LocationPricingSchema,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    existing = db.query(LocationPricing).filter(LocationPricing.city.ilike(payload.city)).first()
    if existing:
        existing.price_multiplier = payload.price_multiplier
        existing.delivery_charge = payload.delivery_charge
        existing.min_order_value = payload.min_order_value
        existing.is_serviceable = payload.is_serviceable
        db.commit()
        return {"status": "updated", "city": payload.city}
    pricing = LocationPricing(**payload.dict())
    db.add(pricing)
    db.commit()
    return {"status": "created", "city": payload.city}


@router.delete("/admin/location-pricing/{pricing_id}")
def delete_location_pricing(
    pricing_id: str,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    pricing = db.query(LocationPricing).filter(LocationPricing.id == pricing_id).first()
    if not pricing:
        raise HTTPException(status_code=404, detail="Pricing rule not found")
    db.delete(pricing)
    db.commit()
    return {"status": "deleted"}



# ============================================================
# RIDER MANAGEMENT
# ============================================================

class RiderAssignSchema(BaseModel):
    rider_name: str
    rider_phone: str
    vehicle_number: Optional[str] = None
    rider_lat: Optional[float] = None
    rider_lng: Optional[float] = None


class RiderLocationSchema(BaseModel):
    rider_lat: float
    rider_lng: float


@router.post("/admin/orders/{order_id}/assign-rider")
async def assign_rider(
    order_id: str,
    payload: RiderAssignSchema,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """Assign a delivery rider to an order."""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    # Remove existing assignment if any
    existing = db.query(RiderAssignment).filter(RiderAssignment.order_id == order_id).first()
    if existing:
        db.delete(existing)
        db.flush()

    rider = RiderAssignment(
        order_id=order_id,
        rider_name=payload.rider_name,
        rider_phone=payload.rider_phone,
        vehicle_number=payload.vehicle_number,
        rider_lat=payload.rider_lat,
        rider_lng=payload.rider_lng,
    )
    db.add(rider)
    db.commit()

    # Notify customer via WebSocket
    if ws_broadcast_callback and order.user_id:
        await ws_broadcast_callback(order.user_id, {
            "type": "rider_assigned",
            "order_id": order_id,
            "rider": {
                "name": payload.rider_name,
                "phone": payload.rider_phone,
                "vehicle": payload.vehicle_number,
                "lat": payload.rider_lat,
                "lng": payload.rider_lng,
            }
        })

    await log_admin_action(db, admin.id, admin.username or "admin", "RIDER_ASSIGNED",
                           {"order_id": order_id, "rider": payload.rider_name}, request)
    return {"status": "assigned", "rider": payload.rider_name}


@router.put("/admin/orders/{order_id}/rider-location")
async def update_rider_location(
    order_id: str,
    payload: RiderLocationSchema,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """Update rider GPS location — called frequently from admin dashboard."""
    rider = db.query(RiderAssignment).filter(RiderAssignment.order_id == order_id).first()
    if not rider:
        raise HTTPException(status_code=404, detail="Rider not assigned to this order")

    rider.rider_lat = payload.rider_lat
    rider.rider_lng = payload.rider_lng
    rider.updated_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    db.commit()

    # Push live location to customer
    order = db.query(Order).filter(Order.id == order_id).first()
    if order and ws_broadcast_callback:
        await ws_broadcast_callback(order.user_id, {
            "type": "rider_location",
            "order_id": order_id,
            "lat": payload.rider_lat,
            "lng": payload.rider_lng,
        })

    return {"status": "updated"}


@router.get("/orders/{order_id}/rider")
def get_rider_info(order_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Get rider details for a specific order (customer view)."""
    order = db.query(Order).filter(Order.id == order_id, Order.user_id == user.id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    rider = db.query(RiderAssignment).filter(RiderAssignment.order_id == order_id).first()
    if not rider:
        return {"assigned": False}
    return {
        "assigned": True,
        "name": rider.rider_name,
        "phone": rider.rider_phone,
        "vehicle": rider.vehicle_number,
        "lat": rider.rider_lat,
        "lng": rider.rider_lng,
    }


# ============================================================
# ORDER CANCELLATION & NOTES
# ============================================================

class OrderCancelSchema(BaseModel):
    reason: Optional[str] = None


class OrderNoteSchema(BaseModel):
    note: str


@router.post("/orders/{order_id}/cancel")
async def cancel_order(
    order_id: str,
    payload: OrderCancelSchema,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    """User-initiated order cancellation (only allowed in certain statuses)."""
    order = db.query(Order).filter(Order.id == order_id, Order.user_id == user.id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    cancellable_statuses = ["Payment Pending", "Payment Received", "Order Processing"]
    if order.status not in cancellable_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Order cannot be cancelled at status '{order.status}'"
        )

    order.status = "Cancelled"
    order.cancellation_reason = payload.reason or "Cancelled by customer"
    order.updated_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    # Refund wallet balance if wallet payment
    if order.payment_method == "wallet":
        order.user.wallet_balance += order.total_payable

    h = OrderStatusHistory(
        order_id=order_id,
        status="Cancelled",
        note=payload.reason or "Cancelled by customer"
    )
    db.add(h)
    db.commit()

    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "order_update", "order_id": order_id, "status": "Cancelled"})

    return {"status": "cancelled", "order_id": order_id}


@router.post("/admin/orders/{order_id}/note")
async def add_order_note(
    order_id: str,
    payload: OrderNoteSchema,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    note = OrderNote(
        order_id=order_id,
        admin_username=admin.username or "admin",
        note=payload.note
    )
    db.add(note)
    db.commit()
    return {"status": "added"}


# ============================================================
# NOTIFICATIONS
# ============================================================

@router.get("/notifications")
def list_notifications(
    limit: int = 20,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user)
):
    notifs = db.query(Notification).filter(
        Notification.user_id == user.id
    ).order_by(Notification.created_at.desc()).limit(limit).all()
    return [
        {
            "id": n.id,
            "title": n.title,
            "body": n.body,
            "type": n.type,
            "reference_id": n.reference_id,
            "is_read": n.is_read,
            "created_at": n.created_at.isoformat(),
        }
        for n in notifs
    ]


@router.put("/notifications/{notif_id}/read")
def mark_notification_read(notif_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    notif = db.query(Notification).filter(Notification.id == notif_id, Notification.user_id == user.id).first()
    if notif:
        notif.is_read = True
        db.commit()
    return {"status": "ok"}


@router.put("/notifications/read-all")
def mark_all_notifications_read(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    db.query(Notification).filter(
        Notification.user_id == user.id,
        Notification.is_read == False
    ).update({"is_read": True})
    db.commit()
    return {"status": "ok"}


# ============================================================
# ANALYTICS (ADMIN)
# ============================================================

@router.get("/admin/analytics/summary")
def analytics_summary(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .services.analytics_service import get_dashboard_summary, get_daily_revenue, get_top_products, get_order_status_distribution, get_new_users_trend
    return {
        "summary": get_dashboard_summary(db),
        "daily_revenue": get_daily_revenue(db, days=14),
        "top_products": get_top_products(db, limit=5),
        "status_distribution": get_order_status_distribution(db),
        "user_trend": get_new_users_trend(db, days=14),
    }


@router.get("/admin/analytics/revenue")
def analytics_revenue(
    days: int = 30,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    from .services.analytics_service import get_revenue_summary, get_daily_revenue
    return {
        "summary": get_revenue_summary(db, days=days),
        "daily": get_daily_revenue(db, days=days),
    }


# ============================================================
# ORDER DETAIL ENRICHED (CUSTOMER + ADMIN)
# ============================================================

@router.get("/orders/{order_id}")
def get_order_detail(order_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Get full order detail including rider info and status history."""
    order = db.query(Order).filter(Order.id == order_id, Order.user_id == user.id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    from .services.order_processor import serialize_order
    data = serialize_order(order)
    if order.gift_card:
        data["gift_card"] = {
            "code": "REDACTED",
            "pin": "REDACTED",
            "value": order.gift_card.value
        }
    else:
        data["gift_card"] = None
    return data


@router.get("/admin/orders/{order_id}/detail")
def get_order_detail_admin(order_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Get full order detail for admin."""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    from .services.order_processor import serialize_order
    data = serialize_order(order)
    data["notes"] = [{"note": n.note, "admin": n.admin_username, "at": n.created_at.isoformat()} for n in order.notes]
    if order.gift_card:
        data["gift_card"] = {
            "code": decrypt_data(order.gift_card.code_encrypted),
            "pin": decrypt_data(order.gift_card.pin_encrypted),
            "value": order.gift_card.value
        }
    else:
        data["gift_card"] = None
    data["user"] = {
        "id": order.user.id,
        "display_name": order.user.display_name,
        "telegram_id": order.user.telegram_id,
        "username": order.user.username,
    }
    return data


@router.get("/admin/orders/{order_id}/pdf")
def get_order_pdf(order_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Generates and downloads a beautifully formatted PDF receipt for an order."""
    import datetime
    from fpdf import FPDF
    
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
        
    class ReceiptPDF(FPDF):
        def header(self):
            # Header banner
            self.set_fill_color(255, 71, 87)
            self.rect(0, 0, 210, 25, "F")
            self.set_y(5)
            self.set_font("Helvetica", "B", 14)
            self.set_text_color(255, 255, 255)
            self.cell(0, 10, "DOMINO'S ORDER ENGINE", align="C", ln=True)
            self.ln(5)

        def footer(self):
            self.set_y(-18)
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(128, 128, 128)
            self.cell(0, 5, "Domino's Order Engine Platform — Official Digital Receipt", align="C", ln=True)
            self.cell(0, 5, "If you need support, please contact the administrator via the Telegram Bot.", align="C", ln=True)

    pdf = ReceiptPDF()
    pdf.add_page()
    pdf.set_margins(15, 30, 15)
    pdf.set_y(30)
    
    # Title
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(30, 30, 45)
    pdf.cell(0, 10, f"INVOICE & RECEIPT (Order ID: {order.id})", ln=True)
    pdf.ln(2)
    
    # Metadata columns
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(80, 80, 90)
    
    # Date & Time (IST offset)
    created_at_ist = order.created_at + datetime.timedelta(hours=5, minutes=30)
    date_str = created_at_ist.strftime("%d %b %Y, %I:%M %p")
    
    col_w = 90
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(col_w, 6, "Order Information", ln=0)
    pdf.cell(col_w, 6, "Customer Details", ln=1)
    
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(col_w, 5, f"Date: {date_str}", ln=0)
    pdf.cell(col_w, 5, f"Name: {order.user.display_name}", ln=1)
    
    pdf.cell(col_w, 5, f"Transaction ID: {order.transaction_id}", ln=0)
    pdf.cell(col_w, 5, f"Telegram ID: {order.user.telegram_id}", ln=1)
    
    pdf.cell(col_w, 5, f"Payment Method: {order.payment_method.upper()}", ln=0)
    pdf.cell(col_w, 5, f"Phone: {order.phone or 'N/A'}", ln=1)
    
    if order.dominos_reference:
        pdf.cell(col_w, 5, f"Domino's Ref: {order.dominos_reference}", ln=0)
    else:
        pdf.cell(col_w, 5, "Domino's Ref: Pending Dispatch", ln=0)
    pdf.cell(col_w, 5, f"City: {order.city or 'N/A'}", ln=1)
    
    pdf.ln(5)
    
    # Address block
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Delivery Address", ln=True)
    pdf.set_font("Helvetica", "", 9)
    pdf.multi_cell(0, 5, f"{order.address or 'No address provided'}\nCoordinates: Lat {order.latitude}, Lng {order.longitude}")
    pdf.ln(5)
    
    # Items Table Header
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_fill_color(240, 240, 245)
    pdf.cell(90, 8, "Item Description", border=1, fill=True)
    pdf.cell(30, 8, "Unit Price", border=1, align="C", fill=True)
    pdf.cell(20, 8, "Qty", border=1, align="C", fill=True)
    pdf.cell(40, 8, "Subtotal", border=1, align="R", fill=True, ln=True)
    
    # Items Table Rows
    pdf.set_font("Helvetica", "", 9)
    for item in order.items:
        desc = item.product.name
        if item.size or item.crust:
            extras = []
            if item.size: extras.append(item.size)
            if item.crust: extras.append(item.crust)
            desc += f" ({', '.join(extras)})"
            
        sub_total_item = item.price * item.quantity
        pdf.cell(90, 8, desc, border=1)
        pdf.cell(30, 8, f"INR {item.price:.2f}", border=1, align="C")
        pdf.cell(20, 8, str(item.quantity), border=1, align="C")
        pdf.cell(40, 8, f"INR {sub_total_item:.2f}", border=1, align="R", ln=True)
        
    pdf.ln(5)
    
    # Pricing Summary
    pdf.set_x(120)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(40, 7, "Base Price (Fixed):", ln=0)
    pdf.set_font("Helvetica", "", 10)
    base_fixed_price = order.total_payable - order.service_charge
    pdf.cell(40, 7, f"INR {base_fixed_price:.2f}", ln=1, align="R")
    
    pdf.set_x(120)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(40, 7, "Bot Service Fee:", ln=0)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(40, 7, f"INR {order.service_charge:.2f}", ln=1, align="R")
    
    pdf.set_x(120)
    pdf.line(120, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(1)
    
    pdf.set_x(120)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(255, 71, 87)
    pdf.cell(40, 8, "Total Payable:", ln=0)
    pdf.cell(40, 8, f"INR {order.total_payable:.2f}", ln=1, align="R")
    
    pdf_bytes = bytes(pdf.output())
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=receipt-{order.id}.pdf"
        }
    )


# ============================================================
# ENHANCED ADMIN ORDER STATUS UPDATE (uses state machine)
# ============================================================

@router.put("/admin/orders/{order_id}/status-v2")
async def update_order_status_v2(
    order_id: str,
    payload: OrderStatusUpdateSchema,
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """State-machine-enforced order status transition."""
    from .services.order_processor import transition_order_status
    from .services import notification_service

    result = await transition_order_status(
        db=db,
        order_id=order_id,
        new_status=payload.status,
        admin_username=admin.username or "admin",
        notify_callback=notification_service.notify_order_update,
    )
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])

    await log_admin_action(db, admin.id, admin.username or "admin", "ORDER_STATUS_CHANGED",
                           {"order_id": order_id, "new_status": payload.status}, request)

    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "order_update", "order_id": order_id, "status": payload.status})

    return {"status": "updated", "new_status": payload.status}


# ============================================================
# USER PROFILE UPDATE
# ============================================================

class ProfileUpdateSchema(BaseModel):
    phone: Optional[str] = None
    display_name: Optional[str] = None
    city: Optional[str] = None


@router.put("/users/profile")
def update_user_profile(payload: ProfileUpdateSchema, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    if payload.phone is not None:
        user.phone = payload.phone
    if payload.display_name is not None:
        user.display_name = payload.display_name
    if payload.city is not None:
        user.city = payload.city
    db.commit()
    return {"status": "updated", "phone": user.phone, "display_name": user.display_name, "city": user.city}


class LinkTelegramRequest(BaseModel):
    telegram_id: str


@router.post("/users/link-telegram")
def link_telegram_account(payload: LinkTelegramRequest, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    # Verify no other verified user has this telegram_id
    existing = db.query(User).filter(User.telegram_id == payload.telegram_id, User.telegram_verified == True).first()
    if existing:
        raise HTTPException(status_code=400, detail="This Telegram ID is already linked to another verified account.")
        
    # Generate 6-digit random code
    import random
    code = "".join([str(random.randint(0, 9)) for _ in range(6)])
    user.telegram_verification_code = code
    db.commit()
    
    bot_username = os.getenv("TELEGRAM_BOT_USERNAME", "dominosordersHELP_bot")
    deep_link = f"https://t.me/{bot_username}?start=verify_{code}"
    
    return {
        "status": "success",
        "code": code,
        "deep_link": deep_link,
        "instructions": f"Start our Telegram Bot using the link or send /verify {code} to complete linking."
    }


@router.get("/users/link-telegram/status")
def get_telegram_link_status(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    return {
        "telegram_verified": getattr(user, "telegram_verified", False),
        "telegram_id": user.telegram_id if getattr(user, "telegram_verified", False) else None
    }


# ============================================================
# PROXY MANAGEMENT ROUTES (ADMIN)
# ============================================================

class ProxyCreateSchema(BaseModel):
    ip: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: Optional[str] = "http"
    is_active: Optional[bool] = True

class ProxyUpdateSchema(BaseModel):
    ip: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: Optional[str] = None
    is_active: Optional[bool] = None

@router.get("/admin/proxies")
def get_proxies(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    proxies = db.query(Proxy).order_by(Proxy.id.desc()).all()
    return [{
        "id": p.id,
        "ip": p.ip,
        "port": p.port,
        "username": p.username,
        "password": p.password,
        "protocol": p.protocol,
        "is_active": p.is_active,
        "fail_count": p.fail_count,
        "last_used": p.last_used.isoformat() if p.last_used else None,
        "created_at": p.created_at.isoformat()
    } for p in proxies]

@router.post("/admin/proxies")
async def create_proxy(payload: ProxyCreateSchema, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    proxy = Proxy(
        ip=payload.ip.strip(),
        port=payload.port,
        username=payload.username.strip() if payload.username else None,
        password=payload.password.strip() if payload.password else None,
        protocol=payload.protocol.strip().lower() if payload.protocol else "http",
        is_active=payload.is_active
    )
    db.add(proxy)
    db.commit()
    db.refresh(proxy)
    await log_admin_action(db, admin.id, admin.username, "PROXY_CREATED", {"id": proxy.id, "ip": proxy.ip, "port": proxy.port}, request)
    return {
        "id": proxy.id,
        "ip": proxy.ip,
        "port": proxy.port,
        "username": proxy.username,
        "password": proxy.password,
        "protocol": proxy.protocol,
        "is_active": proxy.is_active,
        "fail_count": proxy.fail_count,
        "last_used": proxy.last_used.isoformat() if proxy.last_used else None,
        "created_at": proxy.created_at.isoformat()
    }

@router.put("/admin/proxies/{proxy_id}")
async def update_proxy(proxy_id: str, payload: ProxyUpdateSchema, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found")
        
    if payload.ip is not None:
        proxy.ip = payload.ip.strip()
    if payload.port is not None:
        proxy.port = payload.port
    if payload.username is not None:
        proxy.username = payload.username.strip() if payload.username else None
    if payload.password is not None:
        proxy.password = payload.password.strip() if payload.password else None
    if payload.protocol is not None:
        proxy.protocol = payload.protocol.strip().lower()
    if payload.is_active is not None:
        proxy.is_active = payload.is_active
        if payload.is_active:
            proxy.fail_count = 0 # Reset fails if manually re-enabling
            
    db.commit()
    db.refresh(proxy)
    await log_admin_action(db, admin.id, admin.username, "PROXY_UPDATED", {"id": proxy.id, "ip": proxy.ip, "port": proxy.port, "is_active": proxy.is_active}, request)
    return {
        "id": proxy.id,
        "ip": proxy.ip,
        "port": proxy.port,
        "username": proxy.username,
        "password": proxy.password,
        "protocol": proxy.protocol,
        "is_active": proxy.is_active,
        "fail_count": proxy.fail_count,
        "last_used": proxy.last_used.isoformat() if proxy.last_used else None,
        "created_at": proxy.created_at.isoformat()
    }

@router.delete("/admin/proxies/{proxy_id}")
async def delete_proxy(proxy_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found")
    
    db.delete(proxy)
    db.commit()
    await log_admin_action(db, admin.id, admin.username, "PROXY_DELETED", {"id": proxy_id, "ip": proxy.ip}, request)
    return {"status": "success"}

@router.post("/admin/proxies/{proxy_id}/test")
async def trigger_proxy_test(proxy_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found")
    
    from .services.dominos_service import test_proxy_connection
    res = await test_proxy_connection(proxy, db)
    return res

@router.get("/admin/proxies/logs")
def get_proxy_logs(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    logs = db.query(ProxyLog).order_by(ProxyLog.created_at.desc()).limit(100).all()
    return [{
        "id": l.id,
        "proxy_id": l.proxy_id,
        "order_id": l.order_id,
        "action": l.action,
        "status": l.status,
        "details": l.details,
    } for l in logs]


# ============================================================
# DOMINOS SESSION MANAGEMENT ROUTES (ADMIN)
# ============================================================

class DominosOTPRequestPayload(BaseModel):
    mobile_number: str
    manual_mode: bool = False

class DominosOTPVerifyPayload(BaseModel):
    request_token: str
    otp: str

class DominosRawSessionPayload(BaseModel):
    mobile_number: str
    cookies_json: str

class DominosOTPCancelPayload(BaseModel):
    request_token: str

class DominosOTPActionPayload(BaseModel):
    request_token: str
    action: str
    text: Optional[str] = None

class DominosSessionUpdatePayload(BaseModel):
    max_orders_per_day: Optional[int] = None
    allowed_stores: Optional[str] = None
    assigned_admins: Optional[str] = None
    terms_accepted: Optional[bool] = None
    is_active: Optional[bool] = None

@router.put("/admin/dominos/sessions/{session_id}")
async def update_dominos_session(
    session_id: str,
    payload: DominosSessionUpdatePayload,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    session = db.query(DominosSession).filter(DominosSession.id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="DominosSession not found")
    
    if payload.max_orders_per_day is not None:
        session.max_orders_per_day = payload.max_orders_per_day
    if payload.allowed_stores is not None:
        session.allowed_stores = payload.allowed_stores
    if payload.assigned_admins is not None:
        session.assigned_admins = payload.assigned_admins
    if payload.terms_accepted is not None:
        session.terms_accepted = payload.terms_accepted
    if payload.is_active is not None:
        session.is_active = payload.is_active
        
    db.commit()
    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "session_update"})
    return {"status": "success", "message": "Dominos session updated"}

@router.get("/admin/dominos/sessions")
def get_dominos_sessions(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    sessions = db.query(DominosSession).order_by(DominosSession.created_at.desc()).all()
    from .services.dominos_session_manager import LOGIN_COOKIES
    import datetime as _dt

    result = []
    for s in sessions:
        cookies_list = s.cookies or []
        auth_cookies = [c["name"] for c in cookies_list if c.get("name") in LOGIN_COOKIES]

        # ── Compute health_status based on age and last verify result ──
        now = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        created = s.created_at or now
        age_days = (now - created).days

        verify_status = getattr(s, 'verify_status', None)
        last_verified_at = getattr(s, 'last_verified_at', None)

        if verify_status == "expired":
            health = "expired"
        elif verify_status == "valid" and last_verified_at and (now - last_verified_at).days < 3:
            health = "fresh"
        elif age_days > 14:
            health = "expired"
        elif age_days > 7:
            health = "expiring"
        elif auth_cookies:
            health = "fresh"
        else:
            health = "unknown"

        # ── Count orders that used this session's mobile number ──
        try:
            order_count = db.query(Order).filter(Order.phone == s.mobile_number).count()
        except Exception:
            order_count = 0

        updated_at = getattr(s, 'updated_at', None)

        result.append({
            "id": s.id,
            "mobile_number": s.mobile_number,
            "is_active": s.is_active,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
            "expires_at": s.expires_at.isoformat() if s.expires_at else None,
            "admin_id": s.admin_id,
            "cookie_count": len(cookies_list),
            "auth_cookie_names": auth_cookies,
            "has_auth_cookie": bool(auth_cookies),
            "last_verified_at": last_verified_at.isoformat() if last_verified_at else None,
            "verify_status": verify_status,
            "health_status": health,       # fresh / expiring / expired / unknown
            "age_days": age_days,
            "order_count": order_count,
            "max_orders_per_day": getattr(s, 'max_orders_per_day', 0),
            "today_orders_count": getattr(s, 'today_orders_count', 0),
            "last_order_placed_at": s.last_order_placed_at.isoformat() if getattr(s, 'last_order_placed_at', None) else None,
            "allowed_stores": getattr(s, 'allowed_stores', ""),
            "assigned_admins": getattr(s, 'assigned_admins', ""),
            "terms_accepted": getattr(s, 'terms_accepted', False),
            "total_orders_placed": getattr(s, 'total_orders_placed', 0),
        })
    return result

@router.get("/admin/dominos/sessions/status")
async def get_dominos_otp_status(token: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Polling endpoint: returns browser_ready / browser_error state for a given request token.
    Used by the admin UI as a fallback when SSE events may have been missed (e.g. HTTP/2 proxy)."""
    from .services import dominos_session_manager
    req_data = dominos_session_manager.ACTIVE_OTP_REQUESTS.get(token)
    if not req_data:
        # Check if the session was successfully created in the DB
        import datetime
        from .database import DominosOTPRequest, DominosSession
        otp_req = db.query(DominosOTPRequest).filter(DominosOTPRequest.request_token == token).first()
        if otp_req:
            time_limit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=3)
            session = db.query(DominosSession).filter(
                DominosSession.mobile_number == otp_req.mobile_number,
                DominosSession.is_active == True,
                DominosSession.created_at >= time_limit
            ).first()
            if session:
                return {
                    "active": False,
                    "browser_ready": True,
                    "browser_error": None,
                    "last_status": "🎉 Login successful! Session stored.",
                    "login_success": True
                }
        return {"active": False, "browser_ready": False, "browser_error": "No active session found for this token"}
    return {
        "active": True,
        "browser_ready": req_data.get("browser_ready", False),
        "browser_error": req_data.get("browser_error"),
        "last_status": req_data.get("last_status", ""),
        "login_success": req_data.get("login_success", False),
    }

@router.post("/admin/dominos/sessions/request")

async def trigger_dominos_otp_request(payload: DominosOTPRequestPayload, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .services import dominos_session_manager
    try:
        res = await dominos_session_manager.request_otp(db, admin, payload.mobile_number, payload.manual_mode)
        await log_admin_action(db, admin.id, admin.username, "DOMINOS_OTP_REQUESTED", {"mobile_number": payload.mobile_number, "status": res.get("status"), "manual_mode": payload.manual_mode}, request)
        return res
    except Exception as e:
        import traceback as _tb
        logger.error(f"[SESSION REQUEST ERROR] {str(e)}\n{_tb.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to start OTP session: {str(e)}")


@router.post("/admin/dominos/sessions/cancel")
async def cancel_dominos_otp_request(payload: DominosOTPCancelPayload, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .services import dominos_session_manager
    req_data = dominos_session_manager.ACTIVE_OTP_REQUESTS.get(payload.request_token)
    if req_data:
        mob = req_data.get("mobile_number")
        if mob:
            await dominos_session_manager.delete_session_resources(db, "", mob)
        else:
            # Fallback if mobile is missing
            context = req_data.get("context")
            if context:
                try: await context.close()
                except Exception: pass
            browser = req_data.get("browser")
            if browser:
                try: await browser.close()
                except Exception: pass
            pw_ctx = req_data.get("playwright_ctx")
            if pw_ctx:
                try: await pw_ctx.stop()
                except Exception: pass
            dominos_session_manager.ACTIVE_OTP_REQUESTS.pop(payload.request_token, None)
        await dominos_session_manager.broadcast_status(payload.request_token, "❌ Session request cancelled by admin.")
    await log_admin_action(db, admin.id, admin.username, "DOMINOS_OTP_CANCELLED", {"request_token": payload.request_token}, request)
    return {"status": "success"}

@router.post("/admin/dominos/sessions/screenshot")
async def refresh_dominos_otp_screenshot(payload: DominosOTPCancelPayload, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .services import dominos_session_manager
    req_data = dominos_session_manager.ACTIVE_OTP_REQUESTS.get(payload.request_token)
    if not req_data:
        return {"status": "error", "message": "No active session found for this token"}
    
    page = req_data.get("page")
    # Check if page is alive — do NOT auto-recover (that creates extra tabs)
    if not page or not await dominos_session_manager.is_page_alive(page):
        return {"status": "error", "message": "Browser page is closed. Please start a new OTP session."}
    
    await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
    return {"status": "success", "screenshot": req_data.get("last_screenshot")}

@router.post("/admin/dominos/sessions/action")
async def trigger_dominos_otp_action(payload: DominosOTPActionPayload, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .services import dominos_session_manager
    req_data = dominos_session_manager.ACTIVE_OTP_REQUESTS.get(payload.request_token)
    if not req_data:
        return {"status": "error", "message": "No active session found for this token"}
    
    # Check if page is alive — do NOT auto-recover (that creates extra tabs)
    page = req_data.get("page")
    if not page or not await dominos_session_manager.is_page_alive(page):
        return {"status": "error", "message": "Browser page is closed. Please start a new OTP session."}
    
    action = payload.action
    
    try:
        if action == "click_login":
            for sel in ['button:has-text("Login")', 'a:has-text("Login")', '.btn-login', 'text="Login"', 'button:has-text("Sign In")', 'a:has-text("Sign In")', '[data-testid="login-btn"]']:
                if await page.is_visible(sel, timeout=1000):
                    await page.click(sel)
                    break
            else:
                await page.goto("https://m.dominos.co.in/login", wait_until="domcontentloaded", timeout=10000)
            await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
            return {"status": "success", "message": "Login clicked"}
            
        elif action == "click_send_otp":
            for sel in ['button.btn--red', 'span:has-text("Send OTP")', 'button:has-text("Send OTP")', 'button:has-text("GET OTP")', 'button:has-text("Get OTP")', 'button:has-text("SEND OTP")', 'button:has-text("Submit")', 'button:has-text("SUBMIT")', 'button:has-text("Continue")', 'button:has-text("CONTINUE")', 'button:has-text("Proceed")', 'input[type="submit"]', '.login-btn', '[data-testid="submit-btn"]', 'button[type="submit"]', '.otp-btn', '.send-otp-btn', 'button.btn-primary']:
                if await page.is_visible(sel, timeout=1000):
                    await page.evaluate(f"() => {{ const el = document.querySelector('{sel}'); if (el) el.removeAttribute('disabled'); }}")
                    await page.click(sel)
                    break
            else:
                await page.evaluate("""() => {
                    const allBtns = Array.from(document.querySelectorAll('button,input[type="submit"]'));
                    const btn = allBtns.find(b => {
                        const t = (b.textContent || b.value || '').toLowerCase();
                        return t.includes('otp') || t.includes('send') || t.includes('submit') || t.includes('continue') || t.includes('get');
                    });
                    if (btn) { btn.removeAttribute('disabled'); btn.click(); }
                }""")
            await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
            return {"status": "success", "message": "Send OTP clicked"}

        elif action == "click_resend_otp":
            for sel in ['button:has-text("Resend")', 'span:has-text("Resend")', 'a:has-text("Resend")', 'button:has-text("Send OTP")', 'span:has-text("Send OTP")', '.resend-otp-btn', 'text="Resend"']:
                if await page.is_visible(sel, timeout=1000):
                    await page.click(sel)
                    break
            else:
                await page.evaluate("""() => {
                    const el = Array.from(document.querySelectorAll('button,span,a,input'))
                        .find(e => (e.textContent || e.value || '').toLowerCase().includes('resend'));
                    if (el) el.click();
                }""")
            await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
            return {"status": "success", "message": "Resend OTP clicked"}

            
        elif action == "type_text" and payload.text:
            await page.keyboard.insert_text(payload.text)
            await page.evaluate("""() => {
                const el = document.activeElement;
                if (el) {
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true }));
                }
            }""")
            await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
            return {"status": "success", "message": f"Typed '{payload.text}'"}
            
        elif action == "focus_input":
            await page.evaluate("""() => {
                const inp = document.querySelector('input[type="tel"],input[type="number"],input[type="text"]');
                if (inp) inp.focus();
            }""")
            return {"status": "success"}

        elif action == "dismiss_overlays":
            from .services.dominos_session_manager import dismiss_overlays
            await dismiss_overlays(page)
            await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
            return {"status": "success", "message": "Overlays dismissed"}

        elif action == "complete_profile":
            from .services.dominos_session_manager import handle_post_login_navigation
            await handle_post_login_navigation(page, payload.request_token)
            await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
            return {"status": "success", "message": "Profile navigation handler triggered"}

        elif action == "refresh_screenshot":
            await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
            return {"status": "success", "message": "Screenshot refreshed"}

        elif action == "click_selector" and payload.text:
            if await page.is_visible(payload.text, timeout=2000):
                await page.click(payload.text)
                await dominos_session_manager.capture_and_broadcast_screenshot(payload.request_token, page)
                return {"status": "success", "message": f"Clicked selector '{payload.text}'"}
            else:
                return {"status": "error", "message": f"Selector '{payload.text}' is not visible."}

        elif action == "force_save":
            context = req_data["context"]
            cookies = await context.cookies()
            
            from .services.dominos_session_manager import detect_page_state, LOGIN_COOKIES, verify_logged_in_mobile, sanitize_cookies
            
            # 1. Verify page is actually logged in
            state = await detect_page_state(page)
            has_cookie = any(c.get("name") in LOGIN_COOKIES for c in cookies)
            if state != "logged_in" and not has_cookie:
                return {"status": "error", "message": "Cannot save session: Browser page is not logged in. Please complete login first."}
                
            # 2. Verify logged-in mobile matches requested mobile
            is_match = await verify_logged_in_mobile(page, req_data["mobile_number"])
            if not is_match:
                return {"status": "error", "message": f"Cannot save session: Logged-in account does not match requested mobile +91{req_data['mobile_number']}."}
                
            ls_str = await page.evaluate("() => JSON.stringify(localStorage)")
            local_storage_data = json.loads(ls_str) if ls_str else None
            db.query(DominosSession).filter(
                DominosSession.mobile_number == req_data["mobile_number"]
            ).update({"is_active": False})
            
            import datetime
            session = DominosSession(
                mobile_number=req_data["mobile_number"],
                cookies=sanitize_cookies(cookies),
                local_storage=local_storage_data,
                is_active=True,
                admin_id=admin.id,
                created_at=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
            )
            db.add(session)
            db.commit()
            
            # Broadcast final status
            dominos_session_manager.ACTIVE_OTP_REQUESTS.pop(payload.request_token, None)
            await dominos_session_manager.broadcast_status(
                payload.request_token, f"🎉 Session for +91{req_data['mobile_number']} saved manually by admin!"
            )
            
            # Close browser context in background
            try:
                await context.close()
            except Exception:
                pass
            return {"status": "success", "message": f"Successfully force-saved session for +91{req_data['mobile_number']}."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
        
    return {"status": "error", "message": "Unknown action"}

@router.post("/admin/dominos/sessions/verify")
async def verify_dominos_otp(payload: DominosOTPVerifyPayload, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .services import dominos_session_manager
    try:
        # Save OTP into the database request record for traceability
        otp_req = db.query(DominosOTPRequest).filter(DominosOTPRequest.request_token == payload.request_token).first()
        if otp_req:
            otp_req.otp = payload.otp
            db.commit()

        session = await dominos_session_manager.verify_otp(db, admin, payload.request_token, payload.otp)
        await log_admin_action(db, admin.id, admin.username, "DOMINOS_SESSION_CREATED", {"mobile_number": session.mobile_number, "session_id": session.id}, request)
        return {
            "status": "success",
            "session_id": session.id,
            "mobile_number": session.mobile_number,
            "message": "Session created successfully"
        }
    except Exception as e:
        err_msg = str(e)

        # ── Manual fallback: OTP could not be auto-filled ─────────────────────
        if "manual fallback" in err_msg.lower():
            # Check if a session was already saved in the DB (background monitor may have got it)
            if otp_req:
                tl = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
                saved = db.query(DominosSession).filter(
                    DominosSession.mobile_number == otp_req.mobile_number,
                    DominosSession.is_active == True,
                    DominosSession.created_at >= tl
                ).order_by(DominosSession.created_at.desc()).first()
                if saved:
                    return {
                        "status": "success",
                        "session_id": saved.id,
                        "mobile_number": saved.mobile_number,
                        "message": "Session created (auto-saved by background monitor)"
                    }
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="manual fallback: Could not auto-fill OTP boxes. Please enter OTP manually in the browser."
            )

        # ── Session expired (server restart / missing token) ──────────────────
        if "session expired" in err_msg.lower() or "click 'request otp'" in err_msg.lower():
            raise HTTPException(
                status_code=400,
                detail=err_msg  # already has clear user-facing message
            )

        # ── Browser / automation error ─────────────────────────────────────────
        import traceback as _tb
        tb_str = _tb.format_exc()
        logger.error(f"[VERIFY OTP ERROR] {err_msg}\n{tb_str}")
        try:
            err = ErrorLog(type="verify_otp_error", message=f"Verify OTP Error: {err_msg}\n{tb_str}")
            db.add(err)
            db.commit()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=err_msg)


@router.post("/admin/dominos/sessions/manual_otp")
async def verify_dominos_manual_otp(payload: DominosOTPVerifyPayload, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .services import dominos_session_manager
    try:
        otp_req = db.query(DominosOTPRequest).filter(DominosOTPRequest.request_token == payload.request_token).first()
        if otp_req:
            otp_req.otp = payload.otp
            db.commit()

        session = await dominos_session_manager.verify_otp(db, admin, payload.request_token, payload.otp)
        await log_admin_action(db, admin.id, admin.username, "DOMINOS_SESSION_CREATED_MANUAL", {"mobile_number": session.mobile_number, "session_id": session.id}, request)
        return {
            "status": "success",
            "session_id": session.id,
            "mobile_number": session.mobile_number,
            "message": "Session created successfully via manual OTP"
        }
    except Exception as e:
        err_msg = str(e)
        import traceback as _tb
        tb_str = _tb.format_exc()
        logger.error(f"[MANUAL OTP ERROR] {err_msg}\n{tb_str}")
        try:
            err = ErrorLog(type="manual_otp_error", message=f"Manual OTP Error: {err_msg}\n{tb_str}")
            db.add(err)
            db.commit()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=err_msg)


@router.post("/admin/dominos/sessions/raw")
async def add_dominos_raw_session(payload: DominosRawSessionPayload, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .services import dominos_session_manager
    try:
        session = dominos_session_manager.add_raw_session(db, admin, payload.mobile_number, payload.cookies_json)
        await log_admin_action(db, admin.id, admin.username, "DOMINOS_RAW_SESSION_IMPORTED", {"mobile_number": session.mobile_number, "session_id": session.id}, request)
        if sse_broadcast_callback:
            await sse_broadcast_callback({"type": "session_update"})
        return {
            "status": "success",
            "session_id": session.id,
            "mobile_number": session.mobile_number,
            "message": "Raw session cookies imported successfully"
        }
    except Exception as e:
        import traceback as _tb
        logger.error(f"[RAW SESSION IMPORT ERROR] {str(e)}\n{_tb.format_exc()}")
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/admin/dominos/sessions/{session_id}")
async def delete_dominos_session(session_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    session = db.query(DominosSession).filter(DominosSession.id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    mob = session.mobile_number
    from .services.dominos_session_manager import delete_session_resources
    await delete_session_resources(db, session_id, mob)
    
    db.delete(session)
    db.commit()
    await log_admin_action(db, admin.id, admin.username, "DOMINOS_SESSION_DELETED", {"session_id": session_id, "mobile_number": mob}, request)
    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "session_update"})
    return {"status": "success"}

@router.get("/admin/dominos/sessions/{session_id}/cookies")
async def get_dominos_session_cookies(session_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    session = db.query(DominosSession).filter(DominosSession.id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    cookies_list = session.cookies or []
    from .services.dominos_session_manager import LOGIN_COOKIES
    return {
        "cookies_json": json.dumps(cookies_list, indent=2),
        "cookie_count": len(cookies_list),
        "auth_cookies": [c["name"] for c in cookies_list if c.get("name") in LOGIN_COOKIES],
        "mobile_number": session.mobile_number,
        "is_active": session.is_active,
    }

@router.post("/admin/dominos/sessions/{session_id}/verify")
async def verify_dominos_session(session_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Test if stored cookies are still valid by making a real HTTP request to Domino's API,
    falling back to Playwright stealth browser verification if Cloudflare blocks the HTTP client."""
    session = db.query(DominosSession).filter(DominosSession.id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    import httpx
    cookies_list = session.cookies or []
    cookie_jar = {}
    if isinstance(cookies_list, list):
        for c in cookies_list:
            if isinstance(c, dict):
                name = c.get("name") or c.get("Name")
                value = c.get("value") if c.get("value") is not None else c.get("Value")
                if name and value is not None:
                    cookie_jar[str(name)] = str(value)
    
    proxy_url = os.getenv("STATIC_PROXY")
    normalized_proxy_url = None
    if proxy_url:
        try:
            from .services.proxy_manager import parse_proxy_string
            parsed = parse_proxy_string(proxy_url)
            normalized_proxy_url = parsed["normalized_url"]
        except Exception as pe:
            logger.warning(f"Failed to parse STATIC_PROXY: {pe}")
            
    verify_status = "unknown"
    message = ""
    
    # Attempt 1: Fast HTTP API call (takes < 1 second under normal circumstances)
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, proxy=normalized_proxy_url) as client:
            resp = await client.get(
                "https://m.dominos.co.in/api/en/v2/customer",
                headers={
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                    "Accept": "application/json",
                    "Referer": "https://m.dominos.co.in/",
                },
                cookies=cookie_jar
            )
        if resp.status_code == 200:
            verify_status = "valid"
            message = "✅ Session is VALID — cookies are authenticated!"
        elif resp.status_code == 401:
            verify_status = "expired"
            message = "❌ Session EXPIRED — cookies no longer authenticate."
        else:
            # 403 or other codes can mean Cloudflare block, fallback to Playwright below
            logger.info(f"Domino's HTTP verification returned HTTP {resp.status_code}. Falling back to Playwright check...")
    except Exception as e:
        logger.warning(f"Domino's HTTP verification failed: {e}. Falling back to Playwright check...")
        
    # Attempt 2: Playwright fallback (100% accurate stealth check bypasses Cloudflare)
    if verify_status not in ("valid", "expired"):
        from .services.browser_pool import browser_pool
        from .services.dominos_session_manager import detect_page_state, sanitize_cookies
        
        logger.info(f"Running Playwright session verification fallback for session {session_id}")
        context = None
        try:
            context = await browser_pool.create_context(
                user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                locale="en-IN",
            )
            if session.cookies:
                await context.add_cookies(sanitize_cookies(session.cookies))
            
            page = await context.new_page()
            
            # Block unnecessary resources to reduce page load time
            async def handle_route(route):
                req = route.request
                url = req.url.lower()
                r_type = req.resource_type
                if r_type in ("image", "media", "font") or any(
                    x in url for x in ("google-analytics", "analytics", "facebook", "doubleclick", "hotjar", "amplitude", "clarity", "clevertap", "wizrocket", "mixpanel", "sentry")
                ):
                    try: await route.abort()
                    except Exception: pass
                else:
                    try: await route.continue_()
                    except Exception: pass

            await page.route("**/*", handle_route)
            
            # Inject localStorage keys if available (Domino's React PWA requires this to detect login state)
            if session.local_storage:
                try:
                    # We must be on the domain to write to localStorage, so navigate with commit
                    await page.goto("https://m.dominos.co.in/login", wait_until="commit", timeout=10000)
                    ls_data = session.local_storage
                    if isinstance(ls_data, str):
                        ls_data = json.loads(ls_data)
                    ls_json = json.dumps(ls_data)
                    await page.evaluate("""(lsData) => {
                        for (const [k, v] of Object.entries(lsData)) {
                            try { localStorage.setItem(k, typeof v === 'object' ? JSON.stringify(v) : v); } catch(e) {}
                        }
                    }""", ls_json)
                    logger.info(f"[Verification] Injected {len(ls_data)} localStorage keys for verification")
                except Exception as ls_err:
                    logger.warning(f"[Verification] localStorage injection failed (non-fatal): {ls_err}")

            # Navigate to discovery home page to trigger cookie validation
            await page.goto("https://m.dominos.co.in/jfl-discovery-ui/en/pwa/home", wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)
            
            state = await detect_page_state(page)
            if state == "logged_in":
                verify_status = "valid"
                message = "✅ Session is VALID — authenticated via Playwright browser check."
            else:
                verify_status = "expired"
                message = "❌ Session EXPIRED — browser check redirected to login page."
        except Exception as e:
            verify_status = "error"
            message = f"⚠️ Verification failed: {str(e)[:150]}"
        finally:
            if context:
                try: await context.close()
                except Exception: pass
    
    # Persist verification result if model has the fields
    try:
        if hasattr(session, 'last_verified_at'):
            session.last_verified_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if hasattr(session, 'verify_status'):
            session.verify_status = verify_status
        if hasattr(session, 'is_active'):
            session.is_active = (verify_status == "valid")
        db.commit()
    except Exception:
        db.rollback()
    
    await log_admin_action(db, admin.id, admin.username, "DOMINOS_SESSION_VERIFIED", {"session_id": session_id, "verify_status": verify_status}, request)
    return {"status": verify_status, "message": message, "session_id": session_id, "is_valid": (verify_status == "valid")}

@router.put("/admin/dominos/sessions/{session_id}/toggle")
async def toggle_dominos_session(session_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    session = db.query(DominosSession).filter(DominosSession.id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    session.is_active = not session.is_active
    db.commit()
    await log_admin_action(db, admin.id, admin.username, "DOMINOS_SESSION_TOGGLED", {"session_id": session_id, "mobile_number": session.mobile_number, "is_active": session.is_active}, request)
    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "session_update"})
    return {"status": "success", "is_active": session.is_active}

@router.post("/admin/dominos/sessions/{session_id}/save")
async def save_dominos_session_browser(session_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from app.backend import routes as _routes_mod
    import json
    from .services.dominos_session_manager import sanitize_cookies
    
    browser_entry = None
    if hasattr(_routes_mod, "OPENED_ADMIN_BROWSERS"):
        for entry in _routes_mod.OPENED_ADMIN_BROWSERS:
            if entry.get("session_id") == session_id:
                browser_entry = entry
                break
                
    if not browser_entry:
        raise HTTPException(status_code=400, detail="No active browser session found for this account. Make sure you click 'Browser' first to open it.")
        
    page = browser_entry.get("page")
    context = browser_entry.get("context")
    
    if not page or page.is_closed():
        raise HTTPException(status_code=400, detail="The browser session window is closed.")
        
    try:
        cookies = await context.cookies()
        
        # 1. Verify page is actually logged in
        from .services.dominos_session_manager import detect_page_state, LOGIN_COOKIES, verify_logged_in_mobile
        state = await detect_page_state(page)
        has_cookie = any(c.get("name") in LOGIN_COOKIES for c in cookies)
        if state != "logged_in" and not has_cookie:
            raise HTTPException(status_code=400, detail="Cannot save session: Browser page is not logged in. Please complete login first.")
            
        session = db.query(DominosSession).filter(DominosSession.id == session_id).first()
        if not session:
            raise HTTPException(status_code=404, detail="Session record not found")

        # 2. Verify logged-in mobile matches session's mobile
        is_match = await verify_logged_in_mobile(page, session.mobile_number)
        if not is_match:
            raise HTTPException(status_code=400, detail=f"Cannot save session: Logged-in account does not match this record's mobile +91{session.mobile_number}.")
            
        ls_str = await page.evaluate("() => JSON.stringify(localStorage)")
        local_storage_data = json.loads(ls_str) if ls_str else None
        
        # 3. Fast HTTP validation of extracted cookies before saving
        import httpx
        cookie_jar = {}
        if isinstance(cookies, list):
            for c in cookies:
                if isinstance(c, dict):
                    name = c.get("name") or c.get("Name")
                    value = c.get("value") if c.get("value") is not None else c.get("Value")
                    if name and value is not None:
                        cookie_jar[str(name)] = str(value)
        verify_http_ok = False
        resp_status = None
        proxy_url = os.getenv("STATIC_PROXY")
        normalized_proxy_url = None
        if proxy_url:
            try:
                from .services.proxy_manager import parse_proxy_string
                parsed = parse_proxy_string(proxy_url)
                normalized_proxy_url = parsed["normalized_url"]
            except Exception:
                pass
        try:
            async with httpx.AsyncClient(timeout=5.0, proxy=normalized_proxy_url) as client:
                resp = await client.get(
                    "https://m.dominos.co.in/api/en/v2/customer",
                    headers={
                        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                        "Accept": "application/json",
                        "Referer": "https://m.dominos.co.in/",
                    },
                    cookies=cookie_jar
                )
                resp_status = resp.status_code
            if resp.status_code == 200:
                verify_http_ok = True
                logger.info(f"[BROWSER SAVE] Fast HTTP verification succeeded for +91{session.mobile_number}")
            else:
                logger.warning(f"[BROWSER SAVE] HTTP verification returned {resp.status_code} for +91{session.mobile_number}")
        except Exception as ve:
            logger.warning(f"[BROWSER SAVE] HTTP verification failed: {ve}")
        
        if resp_status == 401:
            raise HTTPException(status_code=400, detail="Cannot save session: Domino's rejected the cookies as unauthorized (session is not authenticated).")

        if cookies:
            session.cookies = sanitize_cookies(cookies)
        if local_storage_data:
            session.local_storage = local_storage_data
            
        import datetime as _dt
        now_utc = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        session.is_active = True
        session.verify_status = "valid" if (verify_http_ok or resp_status != 401) else "error"
        session.last_verified_at = now_utc
        session.expires_at = now_utc + _dt.timedelta(days=14)
        db.commit()
        
        await log_admin_action(db, admin.id, admin.username, "DOMINOS_SESSION_MANUALLY_SAVED",
            {"session_id": session_id, "mobile_number": session.mobile_number}, request)
            
        if sse_broadcast_callback:
            await sse_broadcast_callback({"type": "session_update"})
            
        return {"status": "success", "message": f"Successfully extracted and saved active session for +91{session.mobile_number}!"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to extract session: {str(e)}")

@router.post("/admin/dominos/sessions/{session_id}/open")
@router.post("/api/v1/admin/dominos/sessions/{session_id}/open")
async def open_dominos_session_browser(session_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    session = db.query(DominosSession).filter(DominosSession.id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
        
    try:
        import sys, asyncio
        from playwright.async_api import async_playwright
        from .services.dominos_session_manager import sanitize_cookies
        from app.backend import routes as _routes_mod
        
        if sys.platform == "win32":
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            except Exception:
                pass
        
        # Start dedicated playwright instance for interactive inspection window
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=False,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--window-size=1280,900",
                "--window-position=50,50",
                "--start-maximized",
            ]
        )
        
        # Desktop PC environment with real cookies injected
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-IN",
            viewport={"width": 1280, "height": 900}
        )
        
        if session.cookies:
            await context.add_cookies(sanitize_cookies(session.cookies))
            
        # Inject saved LocalStorage variables (tokens, customerId, client settings) via init script
        # to ensure the PWA React app reads them on initialization rather than booting as a guest
        if getattr(session, 'local_storage', None):
            ls_data = session.local_storage
            if isinstance(ls_data, str):
                import json as _json
                ls_data = _json.loads(ls_data)
            if ls_data:
                try:
                    import json
                    ls_json = json.dumps(ls_data)
                    await context.add_init_script(f"""
                        try {{
                            const lsData = {ls_json};
                            for (const [k, v] of Object.entries(lsData)) {{
                                localStorage.setItem(k, typeof v === 'object' ? JSON.stringify(v) : String(v));
                            }}
                        }} catch (e) {{}}
                    """)
                    logger.info(f"[OPEN BROWSER] Pre-injected {len(ls_data)} localStorage keys via init script")
                except Exception as lse_init:
                    logger.error(f"[OPEN BROWSER] Failed to add localStorage init script: {lse_init}")
            
        page = await context.new_page()
        # Navigate directly to logged-in user home / menu screen
        await page.goto("https://m.dominos.co.in/jfl-discovery-ui/en/pwa/home", wait_until="domcontentloaded", timeout=20000)

        
        # Keep global reference so browser stays alive until manually closed
        if not hasattr(_routes_mod, "OPENED_ADMIN_BROWSERS"):
            _routes_mod.OPENED_ADMIN_BROWSERS = []
        
        # Close any existing open browser window for this session_id to avoid duplicate windows
        for b in list(_routes_mod.OPENED_ADMIN_BROWSERS):
            if b.get("session_id") == session_id:
                try:
                    await b["browser"].close()
                except Exception:
                    pass
                try:
                    await b["playwright"].stop()
                except Exception:
                    pass
                if b in _routes_mod.OPENED_ADMIN_BROWSERS:
                    _routes_mod.OPENED_ADMIN_BROWSERS.remove(b)

        # Clean up closed browsers first
        active_list = []
        for b in _routes_mod.OPENED_ADMIN_BROWSERS:
            try:
                if not b["page"].is_closed():
                    active_list.append(b)
            except Exception:
                pass
        _routes_mod.OPENED_ADMIN_BROWSERS = active_list
        
        _routes_mod.OPENED_ADMIN_BROWSERS.append({
            "playwright": pw, "browser": browser, "context": context, "page": page,
            "session_id": session_id, "mobile": session.mobile_number
        })

        # Inactivity auto-close task (15 minutes idle timeout)
        async def monitor_inactivity(sess_id, browser_obj, context_obj, page_obj):
            import time, asyncio, json
            from .database import SessionLocal, DominosSession
            from .services.dominos_session_manager import is_page_alive
            try:
                # Initialize lastActivity on page loading/navigation
                await page_obj.evaluate("""() => {
                    window.lastActivity = Date.now();
                    const logActivity = () => { window.lastActivity = Date.now(); };
                    window.addEventListener('mousemove', logActivity);
                    window.addEventListener('keydown', logActivity);
                    window.addEventListener('click', logActivity);
                    window.addEventListener('scroll', logActivity);
                }""")
                
                # Retrieve the target mobile number for this session
                target_mobile = None
                db_session = SessionLocal()
                try:
                    sess = db_session.query(DominosSession).filter(DominosSession.id == sess_id).first()
                    if sess:
                        target_mobile = sess.mobile_number
                except Exception:
                    pass
                finally:
                    db_session.close()
                
                last_cookies = []
                last_local_storage = None
                
                while True:
                    if not await is_page_alive(page_obj):
                        break
                        
                    try:
                        # Extract cookies and localStorage periodically while open
                        cookies = await context_obj.cookies()
                        if cookies:
                            from .services.dominos_session_manager import verify_logged_in_mobile, LOGIN_COOKIES
                            has_cookie = any(c.get("name") in LOGIN_COOKIES for c in cookies)
                            is_match = True
                            if has_cookie and target_mobile:
                                is_match = await verify_logged_in_mobile(page_obj, target_mobile)
                                
                            if is_match:
                                last_cookies = cookies
                                ls_str = await page_obj.evaluate("() => JSON.stringify(localStorage)")
                                if ls_str:
                                    last_local_storage = json.loads(ls_str)
                                    
                                # Auto-save every 5 seconds to database and clear duplicates
                                db_session = SessionLocal()
                                try:
                                    if target_mobile:
                                        db_session.query(DominosSession).filter(
                                            DominosSession.mobile_number == target_mobile,
                                            DominosSession.id != sess_id
                                        ).delete()
                                    
                                    sess = db_session.query(DominosSession).filter(DominosSession.id == sess_id).first()
                                    if sess:
                                        sess.cookies = sanitize_cookies(cookies)
                                        if last_local_storage:
                                            sess.local_storage = last_local_storage
                                        sess.verify_status = "valid"
                                        sess.is_active = True
                                        sess.last_verified_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                                        db_session.commit()
                                        logger.debug(f"[SESSION MONITOR] Auto-saved session {sess_id} to database (cleared duplicates).")
                                except Exception as save_err:
                                    logger.error(f"[SESSION MONITOR] Auto-save database error: {save_err}")
                                finally:
                                    db_session.close()
                            else:
                                logger.warning(f"[SESSION MONITOR] Mismatched account detected in browser for +91{target_mobile}. Skipping cookie capture.")
                    except Exception:
                        pass
                        
                    # Sleep 5 seconds between captures
                    await asyncio.sleep(5)
                    
                    if not await is_page_alive(page_obj):
                        break
                        
                    try:
                        last_act = await page_obj.evaluate("window.lastActivity")
                        if not last_act:
                            # Re-bind activity listener if page was navigated or refreshed
                            await page_obj.evaluate("""() => {
                                window.lastActivity = Date.now();
                                const logActivity = () => { window.lastActivity = Date.now(); };
                                window.addEventListener('mousemove', logActivity);
                                window.addEventListener('keydown', logActivity);
                                window.addEventListener('click', logActivity);
                                window.addEventListener('scroll', logActivity);
                            }""")
                            last_act = time.time() * 1000

                        elapsed = (time.time() * 1000) - last_act
                        if elapsed > 900000: # 15 minutes (900 seconds)
                            logger.info(f"[SESSION AUTO-CLOSE] Browser idle for {elapsed/1000:.1f}s. Auto-closing...")
                            break
                    except Exception:
                        pass
                
                # Final save upon closing/exit just to make sure latest state is preserved
                if last_cookies:
                    db_session = SessionLocal()
                    try:
                        if target_mobile:
                            db_session.query(DominosSession).filter(
                                DominosSession.mobile_number == target_mobile,
                                DominosSession.id != sess_id
                            ).delete()
                        sess = db_session.query(DominosSession).filter(DominosSession.id == sess_id).first()
                        if sess:
                            sess.cookies = sanitize_cookies(last_cookies)
                            if last_local_storage:
                                sess.local_storage = last_local_storage
                            sess.verify_status = "valid"
                            sess.is_active = True
                            sess.last_verified_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                            db_session.commit()
                            logger.info(f"[SESSION AUTO-CLOSE] Saved session {sess_id} updates to database on exit.")
                    except Exception as commit_err:
                        logger.error(f"[SESSION AUTO-CLOSE] Failed to commit session on exit: {commit_err}")
                    finally:
                        db_session.close()
                
                try:
                    await browser_obj.close()
                except Exception:
                    pass
                try:
                    await pw.stop()
                except Exception:
                    pass
            except Exception as se:
                err_str = str(se)
                benign = ("Target page, context or browser has been closed" in err_str
                          or "Target closed" in err_str
                          or "Execution context was destroyed" in err_str
                          or "most likely because of a navigation" in err_str)
                if not benign:
                    logger.error(f"[SESSION MONITOR ERR] {se}")

        import asyncio
        asyncio.create_task(monitor_inactivity(session_id, browser, context, page))
        
        await log_admin_action(db, admin.id, admin.username, "DOMINOS_SESSION_BROWSER_OPENED",
            {"session_id": session_id, "mobile_number": session.mobile_number}, request)
        return {"status": "success", "message": f"🌐 Browser opened for +91{session.mobile_number} with saved cookies loaded!"}
    except Exception as e:
        import traceback as _tb
        logger.error(f"[OPEN BROWSER ERROR] {e}\n{_tb.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to open browser: {str(e)[:200]}")


OPENED_ADMIN_BROWSERS = []


# --- Additional Schemas for new endpoints ---
class CouponValidateRequest(BaseModel):
    coupon_code: str

class ManualQRRequest(BaseModel):
    amount: float
    label: Optional[str] = None
    user_id: Optional[int] = None

# --- Additional Routes ---

@router.post("/coupons/validate")
def validate_coupon_endpoint(payload: CouponValidateRequest, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    from .database import SystemConfig
    from .services.total_calculator import calculate_total_payable
    
    newbie_coupon_cfg = db.query(SystemConfig).filter(SystemConfig.key == "newbie_coupon").first()
    welcome_coupon_cfg = db.query(SystemConfig).filter(SystemConfig.key == "welcome_coupon").first()
    cart_promo_fixed_cfg = db.query(SystemConfig).filter(SystemConfig.key == "cart_promo_fixed").first()
    
    val_fixed = float(cart_promo_fixed_cfg.value) if cart_promo_fixed_cfg else 100.0
    
    try:
        total_payable, service_charge, coupon_applied = calculate_total_payable(
            subtotal=0.0,
            val_min=0.0,
            val_max=0.0,
            val_fixed=val_fixed,
            discount_total=0.0,
            user=user,
            db=db,
            newbie_coupon_cfg=newbie_coupon_cfg,
            welcome_coupon_cfg=welcome_coupon_cfg,
            coupon_code=payload.coupon_code
        )
        return {
            "valid": True,
            "coupon_code": coupon_applied,
            "total_payable": total_payable,
            "service_charge": service_charge,
            "message": f"Coupon '{coupon_applied}' applied successfully!"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/admin/qr-history")
def get_qr_history(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    history = db.query(QRGenerationHistory).order_by(QRGenerationHistory.created_at.desc()).all()
    import urllib.parse
    result = []
    for item in history:
        url = item.qr_code_url
        if url.startswith("data:image/png;base64,"):
            encoded_uri = urllib.parse.quote(item.upi_uri, safe='')
            url = f"https://api.qrserver.com/v1/create-qr-code/?size=400x400&ecc=M&margin=4&data={encoded_uri}"
        result.append({
            "id": item.id,
            "order_id": item.order_id,
            "user_id": item.user_id,
            "upi_uri": item.upi_uri,
            "amount": item.amount,
            "qr_code_url": url,
            "created_at": item.created_at
        })
    return result

@router.post("/admin/qr-generate")
def generate_manual_qr(payload: ManualQRRequest, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    upi_id_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_id").first()
    upi_name_cfg = db.query(SystemConfig).filter(SystemConfig.key == "upi_name").first()
    
    upi_id = upi_id_cfg.value if upi_id_cfg else "dominos@upi"
    upi_name = upi_name_cfg.value if upi_name_cfg else "Domino's Order Engine"
    
    label = payload.label or "Manual Payment"
    tx_ref = f"MANUAL-{uuid.uuid4().hex[:8].upper()}"
    upi_details = generate_upi_qr_details(upi_id, upi_name, payload.amount, tx_ref, label)
    upi_data = upi_details["upi_uri"]
    qr_code_url = upi_details["qr_data_url"]
    
    qr_hist = QRGenerationHistory(
        order_id=tx_ref,
        user_id=payload.user_id,
        upi_uri=upi_data,
        amount=payload.amount,
        qr_code_url=qr_code_url,
        created_at=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    )
    db.add(qr_hist)
    db.commit()
    db.refresh(qr_hist)
    return qr_hist

@router.get("/admin/payments")
def get_admin_payments(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .database import UTRAttempt, Order
    attempts = db.query(UTRAttempt).order_by(UTRAttempt.attempted_at.desc()).all()
    existing_attempt_order_ids = {att.order_id for att in attempts}
    
    result = []
    for att in attempts:
        order = db.query(Order).filter(Order.id == att.order_id).first()
        result.append({
            "id": att.id,
            "order_id": att.order_id,
            "utr": att.utr,
            "is_successful": att.is_successful,
            "created_at": att.attempted_at.isoformat(),
            "order_total": order.total_payable if order else 0.0,
            "order_status": order.status if order else "Unknown"
        })
        
    # Also include pending orders that don't have a UTRAttempt record yet
    pending_orders = db.query(Order).filter(
        Order.status.in_(["Pending Verification", "Payment Pending"]),
        ~Order.id.in_(existing_attempt_order_ids)
    ).order_by(Order.created_at.desc()).all()
    
    for p_order in pending_orders:
        result.append({
            "id": f"ORDER_{p_order.id}",  # synthetic ID for orders missing UTR attempt
            "order_id": p_order.id,
            "utr": p_order.transaction_id or "NO-UTR",
            "is_successful": False,
            "created_at": p_order.created_at.isoformat(),
            "order_total": p_order.total_payable,
            "order_status": p_order.status
        })
        
    return result

from fastapi import BackgroundTasks

async def run_order_placement_task(order_id: str):
    from .database import SessionLocal
    db = SessionLocal()
    try:
        from .database import Order
        order = db.query(Order).filter(Order.id == order_id).first()
        if order:
            from .services.dominos_service import submit_dominos_order
            await submit_dominos_order(order, db)
    except Exception as e:
        logger.error(f"Background order placement task error: {e}")
    finally:
        db.close()

@router.post("/admin/payments/{attempt_id}/approve")
async def approve_payment_manually(attempt_id: str, request: Request = None, db: Session = Depends(get_db), admin: User = Depends(get_current_admin), background_tasks: BackgroundTasks = None):
    from .database import UTRAttempt, Order, OrderStatusHistory, GiftCard, AuditLog
    
    order_id_target = None
    utr_val = "MANUAL-APPROVE"
    attempt = db.query(UTRAttempt).filter(UTRAttempt.id == attempt_id).first()
    if attempt:
        order_id_target = attempt.order_id
        utr_val = attempt.utr
    elif attempt_id.startswith("ORDER_"):
        order_id_target = attempt_id.replace("ORDER_", "")
        
    order = db.query(Order).filter(Order.id == order_id_target).first() if order_id_target else None
    if not order:
        raise HTTPException(status_code=404, detail="Order or payment attempt not found")
        
    # Check if this is a wallet top-up order
    if order.id.startswith("TOPUP-"):
        user = order.user
        user.wallet_balance += order.total_payable
        order.status = "Completed"
        if attempt:
            attempt.is_successful = True
        
        # Create WalletTransaction
        from .database import WalletTransaction
        tx = WalletTransaction(
            user_id=user.id,
            type="deposit",
            amount=order.total_payable,
            description=f"Deposit via UTR: {utr_val}"
        )
        db.add(tx)
        
        h1 = OrderStatusHistory(order_id=order.id, status="Manual Payment Approved")
        db.add(h1)
        h2 = OrderStatusHistory(order_id=order.id, status="Completed")
        db.add(h2)
        
        audit = AuditLog(admin_id=admin.id, action="WALLET_TOPUP_APPROVED", details=json.dumps({
            "order_id": order.id,
            "utr": utr_val,
            "amount": order.total_payable,
            "user_id": user.id,
            "admin": admin.username
        }))
        db.add(audit)
        db.commit()
        
        success_text = (
            f"💳 <b>Wallet Top-up Approved!</b>\n"
            f"We verified your payment of <b>₹{order.total_payable:.2f}</b> (UTR: <code>{utr_val}</code>).\n\n"
            f"💰 <b>Your New Wallet Balance:</b> <b>₹{user.wallet_balance:.2f}</b>"
        )
        await send_bot_message(user.telegram_id, success_text)
        
        if sse_broadcast_callback:
            await sse_broadcast_callback({"type": "wallet_update", "user_id": user.id, "balance": user.wallet_balance})
            
        return {"status": "success", "message": "Wallet top-up manually approved successfully"}


    # Allocate a gift card manually if available
    gift_card = db.query(GiftCard).filter(GiftCard.status == "available").first()
    if gift_card:
        gift_card.status = "used"
        gift_card.used_by_user_id = order.user_id
        gift_card.used_in_order_id = order.id
        gift_card.used_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        order.gift_card_id = gift_card.id
    else:
        logger.warning(f"No available gift cards in inventory when manually approving order {order.id}. Proceeding with order placement directly.")
        
    order.status = "Order Processing"
    if attempt:
        attempt.is_successful = True
    
    h1 = OrderStatusHistory(order_id=order.id, status="Manual Payment Approved")
    db.add(h1)
    h2 = OrderStatusHistory(order_id=order.id, status="Order Processing")
    db.add(h2)
    
    audit = AuditLog(admin_id=admin.id, action="MANUAL_PAYMENT_APPROVED", details=json.dumps({
        "order_id": order.id,
        "utr": utr_val,
        "admin": admin.username
    }))
    db.add(audit)
    db.commit()
    
    if background_tasks:
        background_tasks.add_task(run_order_placement_task, order.id)
        
    success_text = (
        f"💳 <b>Payment Confirmed (Manual Admin Approval)!</b>\n"
        f"We verified your payment for Order ID: <code>{order.id}</code>.\n\n"
        f"👩‍🍳 <b>Order Status: Processing</b>\n"
        f"Your order is now being dispatched to the kitchen!"
    )
    await send_bot_message(order.user.telegram_id, success_text)
    
    if sse_broadcast_callback:
        await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Order Processing"})
        
    return {"status": "success", "message": "Payment manually approved successfully"}

# --- Phase 2 Enhancements ---

@router.get("/admin/users/{user_id}/sessions")
def get_admin_user_sessions(user_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Lists active and inactive sessions for a specific user."""
    sessions = db.query(UserSession).filter(UserSession.user_id == user_id).order_by(UserSession.created_at.desc()).all()
    result = []
    for s in sessions:
        result.append({
            "id": s.id,
            "ip_address": s.ip_address or "unknown",
            "user_agent": s.user_agent or "unknown",
            "created_at": s.created_at.isoformat(),
            "last_active": s.last_active.isoformat(),
            "is_active": s.is_active
        })
    return result

@router.delete("/admin/users/{user_id}/sessions/{session_id}")
async def delete_user_session(user_id: str, session_id: str, request: Request, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Deactivates/terminates a specific session after checking X-Admin-Password."""
    admin_password = request.headers.get("X-Admin-Password")
    if not admin_password or not verify_password(ADMIN_PASSWORD_HASH, admin_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin password")
        
    session = db.query(UserSession).filter(UserSession.user_id == user_id, UserSession.id == session_id).first()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
        
    session.is_active = False
    db.commit()
    
    await log_admin_action(db, admin.id, admin.username, "USER_SESSION_TERMINATED", {"user_id": user_id, "session_id": session_id}, request)
    return {"status": "success"}

@router.get("/coupons/eligible")
def get_eligible_coupon(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    """Get the eligible coupon for the current logged-in user."""
    orders_count = db.query(Order).filter(Order.user_id == user.id).count()
    
    newbie_coupon_cfg = db.query(SystemConfig).filter(SystemConfig.key == "newbie_coupon").first()
    welcome_coupon_cfg = db.query(SystemConfig).filter(SystemConfig.key == "welcome_coupon").first()
    
    coupon = newbie_coupon_cfg.value if newbie_coupon_cfg else "NEWBIE100"
    if orders_count > 0:
        coupon = welcome_coupon_cfg.value if welcome_coupon_cfg else "WELCOME90"
        
    return {"coupon": coupon}

@router.post("/orders/{order_id}/verify-payment")
async def verify_payment(order_id: str, payload: PaymentVerifyRequest, request: Request = None, db: Session = Depends(get_db), user: User = Depends(get_current_user), background_tasks: BackgroundTasks = None):
    """Validates 12-digit UTR, rate-limits failed attempts, and transitions payment pending to processing."""
    from .database import VerifiedUTR, UTRAttempt
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
        
    # Rate limit: 3 failed attempts per order ID
    failed_attempts = db.query(UTRAttempt).filter(
        UTRAttempt.order_id == order_id,
        UTRAttempt.is_successful == False
    ).count()
    
    if failed_attempts >= 3:
        raise HTTPException(status_code=403, detail="UTR verification lockout: Maximum failed attempts exceeded for this order")
        
    utr = payload.utr.strip()
    
    # 1. Must be exactly 12 digits (numeric)
    if not (len(utr) == 12 and utr.isdigit()):
        # Log failed attempt
        attempt = UTRAttempt(order_id=order_id, utr=utr, is_successful=False)
        db.add(attempt)
        db.commit()
        raise HTTPException(status_code=400, detail="Invalid UTR format. Must be exactly 12 digits.")
        
    # 2. Must be unique (not already successfully verified)
    existing_attempt = db.query(UTRAttempt).filter(UTRAttempt.utr == utr, UTRAttempt.is_successful == True).first()
    if existing_attempt:
        # Log failed attempt
        attempt = UTRAttempt(order_id=order_id, utr=utr, is_successful=False)
        db.add(attempt)
        db.commit()
        raise HTTPException(status_code=400, detail="This UTR has already been used.")
        
    verified = db.query(VerifiedUTR).filter(VerifiedUTR.utr == utr).first()
    
    is_success = False
    if verified:
        # Check if amount matches total_payable
        if abs(verified.amount - order.total_payable) < 0.1:
            is_success = True
            
    # Record the attempt
    attempt = UTRAttempt(
        order_id=order_id,
        utr=utr,
        is_successful=is_success
    )
    db.add(attempt)
    
    if not is_success:
        db.commit()
        raise HTTPException(
            status_code=400,
            detail="Payment verification failed: UTR not found in bank confirmation record or amount mismatch."
        )
        
    # Transition status: Payment Received
    order.status = "Payment Received"
    h_recv = OrderStatusHistory(order_id=order_id, status="Payment Received")
    db.add(h_recv)
    
    # Process Gift Card Allocation
    gift_card = db.query(GiftCard).filter(
        GiftCard.status == "available",
        GiftCard.value >= order.total_payable
    ).order_by(GiftCard.created_at.asc()).first()
    
    # Fallback to oldest available gift card if no card covers the entire amount
    if not gift_card:
        gift_card = db.query(GiftCard).filter(
            GiftCard.status == "available"
        ).order_by(GiftCard.created_at.asc()).first()
        
    if not gift_card:
        # Paused state due to inventory empty
        # Log Gift Card Failure
        err = ErrorLog(
            type="giftcard",
            message=f"Gift Card Exhausted! Cannot allocate card for Order: {order.id}. Order value: {order.total_payable}."
        )
        db.add(err)
        db.commit()
        
        await send_bot_message(
            user.telegram_id,
            f"💳 <b>Payment Confirmed!</b>\n\n"
            f"📦 <b>Order ID:</b> <code>{order.id}</code>\n"
            f"Your payment has been successfully verified. Your order is currently in queue and will be processed shortly. We'll update you as soon as the store prepares it!"
        )
        
        if sse_broadcast_callback:
            await sse_broadcast_callback({
                "type": "error_alert",
                "message": f"Critical: Gift card inventory is empty! Order {order.id} requires card of value ₹{order.total_payable:.2f}."
            })
            await sse_broadcast_callback({"type": "order_update"})
            
        return {
            "order_id": order.id,
            "status": order.status,
            "message": "Payment verified but gift card inventory empty. Order paused."
        }
        
    # Allocate Gift Card successfully
    gift_card.status = "used"
    gift_card.used_by_user_id = user.id
    gift_card.used_in_order_id = order.id
    gift_card.used_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    
    order.gift_card_id = gift_card.id
    
    # Move Status to Gift Card Applied
    order.status = "Gift Card Applied"
    h_app = OrderStatusHistory(order_id=order.id, status="Gift Card Applied")
    db.add(h_app)
    
    # Progress to Order Processing
    order.status = "Order Processing"
    h_proc = OrderStatusHistory(order_id=order.id, status="Order Processing")
    db.add(h_proc)
    
    db.commit()
    
    # Store admin audit log
    admin_log = AuditLog(
        admin_id=None,
        action="GIFT_CARD_APPLIED",
        details=json.dumps({
            "order_id": order.id,
            "user_id": user.id,
            "card_id": gift_card.id,
            "card_value": gift_card.value
        })
    )
    db.add(admin_log)
    db.commit()
    
    if background_tasks:
        background_tasks.add_task(run_order_placement_task, order.id)
    
    # Send user success notification (Redacted code/pin per privacy policy)
    await send_bot_message(
        user.telegram_id,
        f"💳 <b>Payment Confirmed!</b>\n"
        f"We verified your payment of <b>₹{order.total_payable:.2f}</b> for Order ID: <code>{order.id}</code> (UTR: <code>{utr}</code>).\n\n"
        f"👩‍🍳 <b>Order Status: Processing</b>\n"
        f"Your order is now being dispatched to the kitchen. Estimated delivery in 30 minutes!"
    )
    
    if sse_broadcast_callback:
        await sse_broadcast_callback({
            "type": "new_order",
            "order_id": order.id,
            "total": order.total_payable,
            "user": user.display_name,
            "status": order.status
        })
        await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": order.status})
        
    return {
        "order_id": order.id,
        "status": order.status,
        "message": "Payment verified and order is processing"
    }

@router.get("/admin/orders/{order_id}")
def get_admin_order_details(order_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Retrieves order details for admins with unredacted gift card codes/PINs."""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
        
    from .services.order_processor import serialize_order
    data = serialize_order(order)
    
    if order.gift_card:
        data["gift_card"] = {
            "code": decrypt_data(order.gift_card.code_encrypted),
            "pin": decrypt_data(order.gift_card.pin_encrypted),
            "value": order.gift_card.value
        }
    else:
        data["gift_card"] = None
        
    data["user"] = {
        "id": order.user.id,
        "display_name": order.user.display_name,
        "telegram_id": order.user.telegram_id,
        "username": order.user.username,
    }
    return data






# ─────────────────────────────────────────────────────────────────────────────
# Coupon CRUD & Manual Payment Reject Endpoints
# ─────────────────────────────────────────────────────────────────────────────

class CouponCreate(BaseModel):
    code: str
    value: float
    usage_limit: Optional[int] = 1
    is_active: Optional[bool] = True

@router.get("/admin/coupons")
def get_admin_coupons(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .database import Coupon, CouponRedemption, User
    coupons = db.query(Coupon).all()
    res = []
    for c in coupons:
        redemptions = db.query(CouponRedemption).filter(CouponRedemption.coupon_id == c.id).all()
        redeemer_names = []
        for r in redemptions:
            u = db.query(User).filter(User.id == r.user_id).first()
            if u:
                redeemer_names.append(f"{u.display_name} ({u.telegram_id})")
        res.append({
            "id": c.id,
            "code": c.code,
            "value": c.value,
            "usage_limit": c.usage_limit,
            "redeemed_count": c.redeemed_count,
            "is_active": c.is_active,
            "created_at": c.created_at.isoformat(),
            "redeemers": redeemer_names
        })
    return res

@router.post("/admin/coupons")
def create_admin_coupon(payload: CouponCreate, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .database import Coupon
    dup = db.query(Coupon).filter(Coupon.code == payload.code.strip().upper()).first()
    if dup:
        raise HTTPException(status_code=400, detail="Coupon code already exists")
    c = Coupon(
        code=payload.code.strip().upper(),
        value=payload.value,
        usage_limit=payload.usage_limit,
        is_active=payload.is_active
    )
    db.add(c)
    db.commit()
    db.refresh(c)
    return {"status": "success", "coupon": {"id": c.id, "code": c.code, "value": c.value}}

@router.put("/admin/coupons/{coupon_id}")
def update_admin_coupon(coupon_id: str, payload: CouponCreate, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .database import Coupon
    c = db.query(Coupon).filter(Coupon.id == coupon_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Coupon not found")
    c.code = payload.code.strip().upper()
    c.value = payload.value
    c.usage_limit = payload.usage_limit
    c.is_active = payload.is_active
    db.commit()
    return {"status": "success"}

@router.delete("/admin/coupons/{coupon_id}")
def delete_admin_coupon(coupon_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .database import Coupon, CouponRedemption, AuditLog
    c = db.query(Coupon).filter(Coupon.id == coupon_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Coupon not found")
    # Delete associated redemptions first
    db.query(CouponRedemption).filter(CouponRedemption.coupon_id == coupon_id).delete()
    db.delete(c)
    
    audit = AuditLog(admin_id=admin.id, action="COUPON_DELETED", details=f"Code: {c.code}")
    db.add(audit)
    db.commit()
    return {"status": "success"}

@router.post("/admin/payments/{attempt_id}/reject")
async def reject_payment_manually(attempt_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    from .database import UTRAttempt, Order, OrderStatusHistory, AuditLog
    from .bot import send_bot_message
    import json
    
    order_id_target = None
    utr_val = "NO-UTR"
    attempt = db.query(UTRAttempt).filter(UTRAttempt.id == attempt_id).first()
    if attempt:
        order_id_target = attempt.order_id
        utr_val = attempt.utr
    elif attempt_id.startswith("ORDER_"):
        order_id_target = attempt_id.replace("ORDER_", "")
        
    order = db.query(Order).filter(Order.id == order_id_target).first() if order_id_target else None
    if not order:
        raise HTTPException(status_code=404, detail="Order or payment attempt not found")
        
    order.status = "Payment Rejected"
    if attempt:
        attempt.is_successful = False
    
    h1 = OrderStatusHistory(order_id=order.id, status="Manual Payment Rejected")
    db.add(h1)
    
    audit = AuditLog(admin_id=admin.id, action="PAYMENT_REJECTED", details=json.dumps({
        "order_id": order.id,
        "utr": utr_val,
        "amount": order.total_payable,
        "user_id": order.user_id,
        "admin": admin.username
    }))
    db.add(audit)
    db.commit()
    
    user = order.user
    rejection_text = (
        f"❌ <b>Payment Verification Failed</b>\n\n"
        f"Your payment of <b>₹{order.total_payable:.2f}</b> (Ref ID: <code>{order.id}</code>) with UTR <code>{utr_val}</code> was marked as invalid by the administrator.\n\n"
        f"You can re-enter a valid 12-digit UTR, cancel this order request, or contact support:"
    )
    markup = {
        "inline_keyboard": [
            [{"text": "✍️ Re-enter UTR", "callback_data": f"reenter_utr_{order.id}"}],
            [{"text": "❌ Cancel Order", "callback_data": f"cancel_rejected_order_{order.id}"}],
            [{"text": "💬 Contact Support", "callback_data": "support_message"}]
        ]
    }
    await send_bot_message(user.telegram_id, rejection_text, reply_markup=markup)
    
    # Broadcast SSE update
    from .routes import sse_broadcast_callback
    if sse_broadcast_callback:
        try:
            await sse_broadcast_callback({"type": "order_update", "order_id": order.id, "status": "Payment Rejected"})
        except Exception:
            pass
            
    return {"status": "success", "message": "Payment rejected manually"}


# ========================
# Coupon / Promo Code CRUD
# ========================

class CouponCreatePayload(BaseModel):
    code: str
    value: float
    usage_limit: int = 100

@router.get("/admin/coupons")
def get_coupons(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    coupons = db.query(Coupon).order_by(Coupon.created_at.desc()).all()
    return [{
        "id": c.id,
        "code": c.code,
        "value": c.value,
        "usage_limit": c.usage_limit,
        "redeemed_count": c.redeemed_count,
        "is_active": c.is_active,
        "created_at": c.created_at.isoformat() if c.created_at else None
    } for c in coupons]

@router.post("/admin/coupons")
def create_coupon(payload: CouponCreatePayload, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    # Check for duplicate code
    existing = db.query(Coupon).filter(Coupon.code == payload.code.upper()).first()
    if existing:
        raise HTTPException(status_code=400, detail="Coupon code already exists")
    
    coupon = Coupon(
        code=payload.code.upper(),
        value=payload.value,
        usage_limit=payload.usage_limit,
        is_active=True,
        redeemed_count=0
    )
    db.add(coupon)
    
    audit = AuditLog(admin_id=admin.id, action="COUPON_CREATED", details=f"Code: {coupon.code}, Value: {coupon.value}, Limit: {coupon.usage_limit}")
    db.add(audit)
    db.commit()
    
    # Broadcast coupon_update SSE event
    from .routes import sse_broadcast_callback
    if sse_broadcast_callback:
        try:
            import asyncio
            asyncio.create_task(sse_broadcast_callback({"type": "coupon_update"}))
        except Exception:
            pass
            
    return {"status": "success", "id": coupon.id, "code": coupon.code}

@router.post("/admin/coupons/{coupon_id}/toggle")
def toggle_coupon(coupon_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    coupon = db.query(Coupon).filter(Coupon.id == coupon_id).first()
    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")
    
    coupon.is_active = not coupon.is_active
    db.commit()
    
    # Broadcast coupon_update SSE event
    from .routes import sse_broadcast_callback
    if sse_broadcast_callback:
        try:
            import asyncio
            asyncio.create_task(sse_broadcast_callback({"type": "coupon_update"}))
        except Exception:
            pass
            
    return {"status": "success", "is_active": coupon.is_active}

@router.delete("/admin/coupons/{coupon_id}")
def delete_coupon(coupon_id: str, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    coupon = db.query(Coupon).filter(Coupon.id == coupon_id).first()
    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")
    
    # Delete associated redemptions first
    db.query(CouponRedemption).filter(CouponRedemption.coupon_id == coupon_id).delete()
    db.delete(coupon)
    
    audit = AuditLog(admin_id=admin.id, action="COUPON_DELETED", details=f"Code: {coupon.code}")
    db.add(audit)
    db.commit()
    
    # Broadcast coupon_update SSE event
    from .routes import sse_broadcast_callback
    if sse_broadcast_callback:
        try:
            import asyncio
            asyncio.create_task(sse_broadcast_callback({"type": "coupon_update"}))
        except Exception:
            pass
            
    return {"status": "success"}


# ========================
# User Wallet Ledger
# ========================

@router.get("/admin/users/{user_id}/wallet-ledger")
def get_wallet_ledger(user_id: str, offset: int = 0, limit: int = 20, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    total = db.query(WalletTransaction).filter(WalletTransaction.user_id == user_id).count()
    txs = db.query(WalletTransaction).filter(
        WalletTransaction.user_id == user_id
    ).order_by(WalletTransaction.created_at.desc()).offset(offset).limit(limit).all()
    
    return {
        "user_id": user_id,
        "balance": user.wallet_balance,
        "total": total,
        "offset": offset,
        "limit": limit,
        "transactions": [{
            "id": t.id,
            "type": t.type,
            "amount": t.amount,
            "description": t.description,
            "created_at": t.created_at.isoformat() if t.created_at else None
        } for t in txs]
    }
