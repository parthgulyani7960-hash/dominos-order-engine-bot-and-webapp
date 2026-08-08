"""
Notification Service - Centralized dispatch for all user notifications.
Handles Telegram Bot messages, SSE broadcasts, and WebSocket pushes.
"""
import asyncio
import traceback
from typing import Optional, Callable

from ..database import SessionLocal, Order, User, RiderAssignment, ErrorLog
from ..services.order_processor import get_status_icon

# These are injected by main.py
send_bot_message_func: Optional[Callable] = None
send_bot_photo_func: Optional[Callable] = None
sse_broadcast_func: Optional[Callable] = None
ws_broadcast_func: Optional[Callable] = None


async def notify_order_update(order: Order, user: User, new_status: str):
    """
    Full notification dispatch when an order status changes.
    Sends Telegram message + SSE/WebSocket broadcast.
    """
    icon = get_status_icon(new_status)

    # Build the status message
    status_lines = {
        "Payment Received": (
            f"{icon} <b>Payment Confirmed!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"💰 Amount: ₹{order.total_payable:.0f}\n\n"
            f"Your order has been received and is being reviewed. "
            f"We'll notify you when the kitchen picks it up!"
        ),
        "Order Processing": (
            f"{icon} <b>Order Accepted!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order has been confirmed and the kitchen is preparing it. "
            f"Estimated delivery in ~{30} minutes."
        ),
        "Preparing": (
            f"{icon} <b>Pizza in the Oven!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your pizza is being freshly baked. It'll be ready for delivery soon!"
        ),
        "Out for Delivery": None,  # Handled separately with rider info
        "Delivered": (
            f"{icon} <b>Order Delivered!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order has been delivered. Enjoy your meal! 🍕\n\n"
            f"Thank you for ordering with us!"
        ),
        "Cancelled": (
            f"{icon} <b>Order Cancelled</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order has been cancelled."
            + (f"\nReason: {order.cancellation_reason}" if order.cancellation_reason else "")
        ),
    }

    # Special handling for "Out for Delivery"
    if new_status == "Out for Delivery":
        rider_info = ""
        rider = getattr(order, "rider", None)
        if rider:
            vehicle_name = rider.vehicle_number if rider.vehicle_number else "Domino's Delivery Bike"
            rider_info = (
                f"\n\n🛵 <b>Delivery Rider Details:</b>\n"
                f"👤 <b>Name:</b> {rider.rider_name}\n"
                f"📞 <b>Phone:</b> {rider.rider_phone}\n"
                f"🚲 <b>Vehicle:</b> {vehicle_name}"
            )
        text = (
            f"{icon} <b>Rider is on the way!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order is out for delivery. Estimated arrival in ~15 minutes.{rider_info}"
        )
    else:
        text = status_lines.get(new_status)

    # Append delay warning if order is delayed
    import datetime as _dt
    if text and order.estimated_delivery and _dt.datetime.now(datetime.timezone.utc).replace(tzinfo=None) > order.estimated_delivery:
        text += "\n\n⏳ <b>Slight Delay Notice:</b> Your order is taking slightly longer than expected due to store traffic. We apologize for the delay and are working to get it delivered as fast as possible!"

    if text and send_bot_message_func and user.telegram_id:
        try:
            markup = {
                "inline_keyboard": [
                    [{"text": "💬 Contact Support", "url": "https://t.me/dominosordersHELP_bot"}]
                ]
            }
            await send_bot_message_func(user.telegram_id, text, reply_markup=markup)
        except Exception as e:
            _log_error("notification", f"Bot message failed for {user.telegram_id}: {e}")

    # SSE broadcast to all connected clients (admin panel live monitoring)
    if sse_broadcast_func:
        try:
            await sse_broadcast_func({
                "type": "order_update",
                "order_id": order.id,
                "user_id": user.id,
                "user_name": user.display_name,
                "status": new_status,
                "status_icon": get_status_icon(new_status),
                "total_payable": float(order.total_payable or 0),
                "delivery_charge": float(getattr(order, "delivery_charge", 0) or 0),
                "items_count": len(order.items) if hasattr(order, "items") else 0,
                "address": order.address,
                "timestamp": order.updated_at.isoformat() if order.updated_at else None,
            })
        except Exception as e:
            _log_error("notification", f"SSE broadcast failed: {e}")

    # WebSocket push to specific user connection
    if ws_broadcast_func:
        try:
            await ws_broadcast_func(user.id, {
                "type": "order_update",
                "order_id": order.id,
                "status": new_status,
                "status_icon": get_status_icon(new_status),
                "estimated_delivery": order.estimated_delivery.isoformat() if order.estimated_delivery else None,
            })
        except Exception as e:
            _log_error("notification", f"WebSocket broadcast failed: {e}")


async def notify_rider_location_update(order_id: str, user_id: int, lat: float, lng: float):
    """Pushes live rider location update to the customer via WebSocket."""
    if ws_broadcast_func:
        try:
            await ws_broadcast_func(user_id, {
                "type": "rider_location",
                "order_id": order_id,
                "lat": lat,
                "lng": lng,
            })
        except Exception as e:
            _log_error("notification", f"Rider location WS push failed: {e}")


def _log_error(error_type: str, message: str):
    """Logs an error to the database without raising."""
    try:
        db = SessionLocal()
        err = ErrorLog(type=error_type, message=message, stack_trace=traceback.format_exc())
        db.add(err)
        db.commit()
        db.close()
    except Exception:
        pass
