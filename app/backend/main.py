import asyncio
import os
import datetime
import traceback
import json
import sys
from typing import Set, Dict, List

# Event loop policy setup for Windows platform compatibility
if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass


# Load environment variables from .env file at application start
def load_env():
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    key = k.strip()
                    if key not in os.environ:
                        os.environ[key] = v.strip()


load_env()

# ---------------------------------------------------------------------------
# Framework & third-party imports
# ---------------------------------------------------------------------------
from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
import httpx

# ---------------------------------------------------------------------------
# Local infrastructure – logging must be configured BEFORE any other local
# import that might emit log records.
# ---------------------------------------------------------------------------
from .logging_config import configure_logging, logger as struct_logger

configure_logging()
logger = struct_logger

import contextlib
from .health import router as health_router
from .middleware import CorrelationIdMiddleware

from .database import init_db, SessionLocal, Product, User, SystemConfig, LocationPricing, DominosSession
from .routes import router as api_router, get_current_admin
from . import routes, bot
from .bot import run_bot_polling
from .utils import run_backup
from .services import notification_service


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages application startup and graceful shutdown."""
    # ── Startup ──────────────────────────────────────────────────────────────
    init_db()
    seed_database()
    _bg_tasks: list[asyncio.Task] = [
        asyncio.create_task(run_bot_polling(), name="bot_polling"),
        asyncio.create_task(schedule_daily_backup(), name="daily_backup"),
    ]
    logger.info("Domino's Order Engine Platform v2.0 started successfully!")

    yield  # ── Application is running ────────────────────────────────────────

    # ── Shutdown ─────────────────────────────────────────────────────────────
    logger.info("[Shutdown] Cancelling background tasks...")
    for task in _bg_tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*_bg_tasks, return_exceptions=True)

    logger.info("[Shutdown] Closing HTTP client...")
    try:
        await bot._http_client.aclose()
    except Exception as exc:
        logger.warning(f"[Shutdown] _http_client.aclose error: {exc}")

    logger.info("[Shutdown] Clean shutdown complete.")


app = FastAPI(
    title="Domino's Order Engine Platform API",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Middleware stack (applied in reverse registration order by Starlette).
# The last registered middleware runs first.
# Desired order (outermost → innermost):
#   CorrelationId → CORS → GZip → DomainDetection → Security
# Registration order must therefore be reversed:
# ---------------------------------------------------------------------------

# 1. Security / error-logging (innermost – runs last, catches all exceptions)
# registered below after the class is defined.

# 2. GZip compression — reduces JSON response sizes by 60-80%
app.add_middleware(GZipMiddleware, minimum_size=500)

# 3. CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 4. Correlation ID (outermost – runs first, sets ID for all downstream logs)
app.add_middleware(CorrelationIdMiddleware)

# Health & readiness probes (registered early so they bypass auth middleware)
app.include_router(health_router)


# --- Security & Error Logging Middleware ---

class SecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)
            return response
        except Exception as e:
            from fastapi import HTTPException
            from starlette.exceptions import HTTPException as StarletteHTTPException
            if isinstance(e, (HTTPException, StarletteHTTPException)):
                return Response(
                    content=json.dumps({"detail": getattr(e, "detail", str(e))}),
                    status_code=getattr(e, "status_code", 400),
                    media_type="application/json"
                )
            tb = traceback.format_exc()
            def _log_error_sync():
                db = SessionLocal()
                try:
                    from .database import ErrorLog
                    err = ErrorLog(
                        type="backend",
                        message=f"Unhandled Backend Exception: {str(e)}",
                        stack_trace=tb
                    )
                    db.add(err)
                    db.commit()
                except Exception as db_err:
                    logger.error(f"Error logging failed: {db_err}")
                finally:
                    db.close()
            await asyncio.to_thread(_log_error_sync)
            return Response(
                content='{"detail": "Internal server error. Logged and reported."}',
                status_code=500,
                media_type="application/json"
            )


app.add_middleware(SecurityMiddleware)


from fastapi.exceptions import RequestValidationError

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # Format details into a readable list of errors or string
    errors = []
    for err in exc.errors():
        loc = " -> ".join(str(item) for item in err.get("loc", []))
        msg = err.get("msg", "invalid value")
        errors.append(f"{loc}: {msg}")
    
    detail_str = "; ".join(errors)
    logger.warning(f"Validation error on {request.method} {request.url.path}: {detail_str}")
    
    return JSONResponse(
        status_code=422,
        content={"detail": f"Validation Error: {detail_str}", "errors": exc.errors()}
    )


# In-memory cache: only write to DB when domain actually changes (avoids per-request DB hit)
# Web app features (SSE, WebSockets, Domain Detection) have been removed to focus exclusively on Telegram Bot.
routes.sse_broadcast_callback = None
routes.ws_broadcast_callback = None
bot.sse_broadcast_callback = None
notification_service.send_bot_message_func = bot.send_bot_message
notification_service.send_bot_photo_func = bot.send_bot_photo
notification_service.sse_broadcast_func = None
notification_service.ws_broadcast_func = None



# --- Database Seeding ---

def seed_database():
    db = SessionLocal()
    try:
        # Clean up any previously seeded mock pizzas to ensure database has only 100% real menu items from Domino's site
        db.query(Product).filter(
            Product.name.in_([
                "Margherita Classic",
                "Pepperoni Feast",
                "Garden Veggie Supreme",
                "BBQ Smoked Chicken",
                "Double Cheese Romano",
                "Cheeseburst Margherita",
                "Tomato Onion Pizza Mania",
                "Golden Corn Pizza Mania",
                "Truffle Mushroom Artisan",
                "Cheeseburst Margherita (Medium)"
            ])
        ).delete(synchronize_session=False)
        db.commit()

        DOMINOS_MENU_CATALOG = [
            # --- PIZZA MANIA ---
            {
                "name": "Classic Pizza Mania (Tomato)",
                "price": 49.0,
                "description": "Tangy tomato sauce with 100% mozzarella cheese",
                "is_veg": True,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Onion Pizza Mania",
                "price": 69.0,
                "description": "Crunchy onion topping with mozzarella cheese",
                "is_veg": True,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Golden Corn Pizza Mania",
                "price": 79.0,
                "description": "Juicy sweet corn with mozzarella cheese",
                "is_veg": True,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Capsicum & Red Paprika Pizza Mania",
                "price": 89.0,
                "description": "Fresh green capsicum and spicy red paprika",
                "is_veg": True,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Capsicum & Masala Paneer Pizza Mania",
                "price": 109.0,
                "description": "Flavorsome masala paneer with crunchy capsicum",
                "is_veg": True,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Paneer, Onion & Capsicum Pizza Mania",
                "price": 123.0,
                "description": "Chunky paneer, crunchy onion & capsicum",
                "is_veg": True,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Veg Loaded Pizza Mania",
                "price": 159.0,
                "description": "Tomato, jalapeno, corn & grilled mushroom",
                "is_veg": True,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Chicken Sausage Pizza Mania",
                "price": 109.0,
                "description": "Spicy chicken sausage with mozzarella cheese",
                "is_veg": False,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Pepper Barbecue Chicken Pizza Mania",
                "price": 109.0,
                "description": "Pepper barbecue chicken on classic crust",
                "is_veg": False,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Chicken Keema & Onion Pizza Mania",
                "price": 129.0,
                "description": "Spicy chicken keema with crunchy onion",
                "is_veg": False,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },
            {
                "name": "Non-Veg Loaded Pizza Mania",
                "price": 159.0,
                "description": "Loaded with pepper barbecue chicken & chicken sausage",
                "is_veg": False,
                "category": "Mania",
                "crust_options": ["Classic Hand Tossed"],
                "size_options": ["Regular"]
            },

            # --- CLASSIC PIZZAS (VEG) ---
            {
                "name": "Margherita",
                "price": 109.0,
                "description": "Classic delight with 100% real mozzarella cheese",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Achari Do Pyaza",
                "price": 189.0,
                "description": "Tangy achari flavors with crunchy onions and cheese",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Corn & Cheese Paratha Pizza",
                "price": 189.0,
                "description": "Delicious paratha crust topped with sweet corn and cheese",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["Paratha Crust"],
                "size_options": ["Regular"]
            },
            {
                "name": "Fresh Veggie",
                "price": 209.0,
                "description": "Delectable combination of onion & capsicum",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Cheese n Corn",
                "price": 209.0,
                "description": "Sweet & juicy golden corn with 100% real mozzarella cheese",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Double Cheese Margherita",
                "price": 209.0,
                "description": "The classic Margherita scaled up with double cheese!",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Spiced Double Cheese",
                "price": 209.0,
                "description": "Loaded with extra cheese and spicy seasoning",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Peppy Paneer",
                "price": 279.0,
                "description": "Chunky paneer with spicy red paprika, capsicum & mozzarella",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Veggie Paradise",
                "price": 279.0,
                "description": "Gold corn, black olives, capsicum & red paprika",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Farmhouse",
                "price": 279.0,
                "description": "Delightful combination of onion, capsicum, tomato & grilled mushroom",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Mexican Green Wave",
                "price": 279.0,
                "description": "Loaded with crunchy onions, juicy tomatoes, capsicum & jalapenos",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Veg Extravaganza",
                "price": 349.0,
                "description": "Black olives, capsicum, onion, grilled mushroom, corn, tomato, jalapeno & extra cheese",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Indi Tandoori Paneer",
                "price": 349.0,
                "description": "Tandoori paneer with capsicum, red paprika & mint mayo",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Cheese Overload",
                "price": 359.0,
                "description": "Ultra creamy pizza loaded with 35% more liquid cheese",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },

            # --- CLASSIC PIZZAS (NON-VEG) ---
            {
                "name": "Chicken Sausage",
                "price": 209.0,
                "description": "American classic with spicy chicken sausage",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Pepper Barbecue Chicken",
                "price": 259.0,
                "description": "Pepper barbecue chicken for that extra spicy kick",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Chicken Keema Paratha Pizza",
                "price": 259.0,
                "description": "Paratha crust topped with spicy chicken keema & cheese",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["Paratha Crust"],
                "size_options": ["Regular"]
            },
            {
                "name": "Chicken Fiesta",
                "price": 319.0,
                "description": "Grilled chicken rashers, peri-peri chicken, onion & capsicum",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Spiced Double Chicken",
                "price": 319.0,
                "description": "Pepper barbecue chicken & spicy chicken sausage",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Chicken Golden Delight",
                "price": 319.0,
                "description": "Mouth-watering chicken kebab, sweet corn, double cheese",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Non-Veg Supreme",
                "price": 369.0,
                "description": "Bite into hot n spicy chicken, chicken meatballs, onion & herby chicken sausage",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Indi Chicken Tikka",
                "price": 369.0,
                "description": "Tandoori masala with chicken tikka, onion, red paprika & mint mayo",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Chicken Pepperoni",
                "price": 369.0,
                "description": "Classic chicken pepperoni with mozzarella cheese",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "Chicken Dominator",
                "price": 409.0,
                "description": "Loaded with 4 different chicken toppings: barbecue, tikka, meatballs & sausage",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },
            {
                "name": "The 5 Chicken Feast Pizza",
                "price": 409.0,
                "description": "Ultimate chicken feast loaded with 5 supreme chicken toppings",
                "is_veg": False,
                "category": "Non-Veg",
                "crust_options": ["New Hand Tossed", "Cheese Burst", "Fresh Pan"],
                "size_options": ["Regular", "Medium", "Large"]
            },

            # --- CHEF'S SPECIAL & PREMIUM PIZZAS ---
            {
                "name": "Cheese Volcano Farmhouse",
                "price": 289.0,
                "description": "Center molten cheese volcano surrounded by Farmhouse toppings",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["Cheese Volcano Crust"],
                "size_options": ["Medium"]
            },
            {
                "name": "Cheese Volcano Peppy Paneer",
                "price": 289.0,
                "description": "Molten cheese volcano pool with spicy paneer tikka",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["Cheese Volcano Crust"],
                "size_options": ["Medium"]
            },
            {
                "name": "Cheese Volcano Veggie Paradise",
                "price": 289.0,
                "description": "Cheesy volcano center with golden corn & paprika",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["Cheese Volcano Crust"],
                "size_options": ["Medium"]
            },
            {
                "name": "Sourdough Classic Veg",
                "price": 349.0,
                "description": "Artisanal sourdough crust topped with fresh mozzarella & herbs",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["Artisanal Sourdough"],
                "size_options": ["Medium"]
            },
            {
                "name": "Sourdough Creamy Truffle Mushroom",
                "price": 389.0,
                "description": "Sourdough crust infused with truffle mushroom cream sauce",
                "is_veg": True,
                "category": "Veg",
                "crust_options": ["Artisanal Sourdough"],
                "size_options": ["Medium"]
            },
            {
                "name": "Burger Pizza Classic Veg",
                "price": 129.0,
                "description": "Looks like a burger, tastes like a pizza!",
                "is_veg": True,
                "category": "Sides",
                "crust_options": ["Burger Crust"],
                "size_options": ["Regular"]
            },
            {
                "name": "Burger Pizza Premium Veg",
                "price": 159.0,
                "description": "Burger pizza loaded with paneer & extra cheese",
                "is_veg": True,
                "category": "Sides",
                "crust_options": ["Burger Crust"],
                "size_options": ["Regular"]
            },
            {
                "name": "Burger Pizza Classic Non-Veg",
                "price": 161.0,
                "description": "Burger pizza stuffed with chicken patty & cheese",
                "is_veg": False,
                "category": "Sides",
                "crust_options": ["Burger Crust"],
                "size_options": ["Regular"]
            },

            # --- SIDES & SNACKS ---
            {
                "name": "Garlic Breadsticks",
                "price": 119.0,
                "description": "Baked to perfection, garlic-buttered breadsticks served with seasoning",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Stuffed Garlic Bread",
                "price": 159.0,
                "description": "Freshly baked garlic bread stuffed with mozzarella cheese, sweet corn, and jalapenos",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Zingy Parcel Veg",
                "price": 60.0,
                "description": "Golden brown pastry filled with creamy harissa veg filling",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Zingy Parcel Non-Veg",
                "price": 70.0,
                "description": "Golden brown pastry filled with spicy chicken filling",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Loaded Saucy Paneer Parcel - Tandoori",
                "price": 80.0,
                "description": "Paneer parcel with rich tandoori sauce",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Loaded Saucy Chicken Parcel - Tandoori",
                "price": 90.0,
                "description": "Chicken parcel with rich tandoori sauce",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Taco Mexicana Veg",
                "price": 139.0,
                "description": "Crispy taco shell filled with spicy veg patty & creamy sauce",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Taco Mexicana Non-Veg",
                "price": 169.0,
                "description": "Crispy taco shell filled with hot chicken patty & sauce",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Single Taco Veg",
                "price": 80.0,
                "description": "Single crispy Mexican taco shell filled with veg patty",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Double Taco Veg",
                "price": 150.0,
                "description": "Double pair of crispy Mexican veg tacos",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Single Taco Non-Veg",
                "price": 90.0,
                "description": "Single crispy Mexican taco shell with spicy chicken",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Double Taco Non-Veg",
                "price": 170.0,
                "description": "Double pair of crispy Mexican chicken tacos",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Cheesy Dip",
                "price": 30.0,
                "description": "Creamy melted cheese dip",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Cheesy Jalapeno Dip",
                "price": 30.0,
                "description": "Spicy jalapeno infused cheese dip",
                "is_veg": True,
                "category": "Sides"
            },
            {
                "name": "Peri Peri Dip",
                "price": 30.0,
                "description": "Hot & tangy peri-peri seasoning dip",
                "is_veg": True,
                "category": "Sides"
            },

            # --- CHICKEN FEAST ---
            {
                "name": "Spicy Chicken Pops",
                "price": 99.0,
                "description": "Bite-sized crunchy spicy chicken pops",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Spicy Chicken Bombs",
                "price": 109.0,
                "description": "Crispy fried chicken bombs with liquid cheese core",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Mini Garlic Cheese Chicken Rice",
                "price": 139.0,
                "description": "Garlic butter chicken rice topped with melted cheese",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Garlic Cheese Chicken",
                "price": 169.0,
                "description": "Tender chicken breasts smothered in garlic cheese sauce",
                "is_veg": False,
                "category": "Sides"
            },
            {
                "name": "Grilled Wings - Southern Spice",
                "price": 169.0,
                "description": "Juicy grilled chicken wings marinated in Southern spices",
                "is_veg": False,
                "category": "Sides"
            },

            # --- DESSERTS ---
            {
                "name": "Choco Lava Cake",
                "price": 119.0,
                "description": "Gooey molten chocolate lava inside a soft cocoa cake",
                "is_veg": True,
                "category": "Desserts"
            },
            {
                "name": "Butterscotch Mousse Cup",
                "price": 109.0,
                "description": "Sweet butterscotch mousse whipped to creamy perfection",
                "is_veg": True,
                "category": "Desserts"
            },
            {
                "name": "Red Velvet Lava Cake",
                "price": 149.0,
                "description": "Rich red velvet cake with melted white chocolate center",
                "is_veg": True,
                "category": "Desserts"
            },

            # --- BEVERAGES ---
            {
                "name": "Packaged Drinking Water",
                "price": 20.0,
                "description": "Pure packaged mineral water",
                "is_veg": True,
                "category": "Drinks"
            },
            {
                "name": "Fountain Drinks (350ml)",
                "price": 65.0,
                "description": "Refreshing Coca-Cola / Sprite / Fanta",
                "is_veg": True,
                "category": "Drinks"
            },
            {
                "name": "Drinks (475ml)",
                "price": 70.0,
                "description": "Bottle of Coca-Cola / Sprite / Fanta",
                "is_veg": True,
                "category": "Drinks"
            },
            {
                "name": "Coca Cola Zero Sugar (330ml Can)",
                "price": 70.0,
                "description": "Chilled zero-sugar Coca-Cola can",
                "is_veg": True,
                "category": "Drinks"
            },
            {
                "name": "B Natural Juice",
                "price": 70.0,
                "description": "Natural Mango / Orange / Mixed Fruit juice",
                "is_veg": True,
                "category": "Drinks"
            }
        ]
        import json

        # Delete Unlimited Coke if present in database as requested
        db.query(Product).filter(Product.name == "Unlimited Coke").delete(synchronize_session=False)

        for it in DOMINOS_MENU_CATALOG:
            name = it["name"]
            product = db.query(Product).filter(Product.name == name).first()
            
            crusts = json.dumps(it.get("crust_options", ["New Hand Tossed", "Cheese Burst", "Fresh Pan"]))
            sizes = json.dumps(it.get("size_options", ["Regular", "Medium", "Large"]))
            category = it["category"]
            
            if product:
                product.original_price = it["price"]
                product.description = it["description"]
                product.availability = True
                product.is_veg = it["is_veg"]
                product.crust_options = crusts
                product.size_options = sizes
                product.category = category
            else:
                new_prod = Product(
                    name=name,
                    description=it["description"],
                    category=category,
                    is_veg=it["is_veg"],
                    original_price=it["price"],
                    image_url="",
                    availability=True,
                    crust_options=crusts,
                    size_options=sizes,
                    sort_order=10,
                )
                db.add(new_prod)
        db.commit()

        # Remove Tomato Pizza Mania if present in database as requested by user
        db.query(Product).filter(Product.name == "Tomato Pizza Mania").delete(synchronize_session=False)

        # Remove obsolete SystemConfig keys requested by user
        obsolete_keys = ["newbie_coupon", "welcome_coupon", "cart_promo_min", "cart_promo_max", "cart_promo_fixed", "captcha_api_key", "mini_app_url"]
        db.query(SystemConfig).filter(SystemConfig.key.in_(obsolete_keys)).delete(synchronize_session=False)
        db.commit()

        # Seed default admin (only if explicitly set in environment to avoid mock user seeding)
        admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID")
        if admin_tg_id and admin_tg_id.strip():
            admin_user = db.query(User).filter(User.telegram_id == admin_tg_id).first()
            if not admin_user:
                admin_user = User(
                    telegram_id=admin_tg_id,
                    username="admin",
                    display_name="Super Admin",
                    wallet_balance=0.0,
                    role="admin"
                )
                db.add(admin_user)
                db.commit()
                logger.info(f"Admin user seeded with Telegram ID {admin_tg_id}.")

        # Seed essential default system configurations only
        default_configs = {
            "bot_fee": "10.0",
            "upi_id": "pranjalnautry@fam",
            "upi_name": "Domino's Order Engine",
            "platform_name": "Domino's Order Engine",
        }
        for k, v in default_configs.items():
            cfg = db.query(SystemConfig).filter(SystemConfig.key == k).first()
            if not cfg:
                cfg = SystemConfig(key=k, value=str(v))
                db.add(cfg)
            elif k == "upi_id" and cfg.value in ("dominos@upi", ""):
                cfg.value = "pranjalnautry@fam"
        db.commit()

        # Seed default location pricing for major Indian cities
        if db.query(LocationPricing).count() == 0:
            cities = [
                {"city": "Mumbai", "state": "Maharashtra", "price_multiplier": 1.0, "delivery_charge": 30.0},
                {"city": "Delhi", "state": "Delhi", "price_multiplier": 1.0, "delivery_charge": 30.0},
                {"city": "Bangalore", "state": "Karnataka", "price_multiplier": 1.0, "delivery_charge": 30.0},
                {"city": "Chennai", "state": "Tamil Nadu", "price_multiplier": 1.0, "delivery_charge": 30.0},
                {"city": "Hyderabad", "state": "Telangana", "price_multiplier": 1.0, "delivery_charge": 30.0},
                {"city": "Kolkata", "state": "West Bengal", "price_multiplier": 0.95, "delivery_charge": 25.0},
                {"city": "Pune", "state": "Maharashtra", "price_multiplier": 1.0, "delivery_charge": 30.0},
                {"city": "Ahmedabad", "state": "Gujarat", "price_multiplier": 0.95, "delivery_charge": 25.0},
                {"city": "Jaipur", "state": "Rajasthan", "price_multiplier": 0.9, "delivery_charge": 25.0},
                {"city": "Surat", "state": "Gujarat", "price_multiplier": 0.9, "delivery_charge": 25.0},
            ]
            for c in cities:
                db.add(LocationPricing(**c))
            db.commit()
            logger.info("Seeded location pricing for major cities.")

        logger.info("Database initialization complete.")
    except Exception as e:
        logger.error(f"Database seeding failed: {e}")
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


# --- Background Schedulers ---

async def schedule_daily_backup():
    """Triggers a SQLite backup every 24 hours."""
    while True:
        await asyncio.sleep(86400)
        try:
            backup_file = run_backup()
            if backup_file:
                logger.info(f"Daily backup success: {backup_file}")
        except Exception as e:
            logger.error(f"Backup failed: {e}")


# --- API and Static Routes ---

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {
        "status": "online",
        "service": "Domino's Order Engine Platform v2.0",
        "health": "/health",
        "docs": "/api/docs"
    }

app.include_router(api_router, prefix="/api")

UPLOAD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "uploads"))
os.makedirs(UPLOAD_DIR, exist_ok=True)

app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")
