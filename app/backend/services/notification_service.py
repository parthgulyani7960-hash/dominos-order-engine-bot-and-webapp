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
    ref_str = f"\n🔖 Domino's Ref: <code>{order.dominos_reference}</code>" if getattr(order, "dominos_reference", None) else ""
    status_lines = {
        "Pending Approval": (
            f"{icon} <b>Order Received & Pending Review!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"💰 Amount: ₹{order.total_payable:.2f}\n\n"
            f"Your order has been placed and is currently pending admin review. We will notify you as soon as it is accepted!"
        ),
        "Pending Payment": (
            f"{icon} <b>Payment Pending!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"💰 Amount: ₹{order.total_payable:.2f}\n\n"
            f"Please complete payment using UTR/QR to process your order."
        ),
        "Payment Received": (
            f"{icon} <b>Payment Confirmed!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"💰 Amount: ₹{order.total_payable:.2f}\n\n"
            f"Your order payment has been confirmed and is waiting for admin acceptance!"
        ),
        "Paid": (
            f"{icon} <b>Payment Verified!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"💰 Amount: ₹{order.total_payable:.2f}\n\n"
            f"Your payment is verified. Admin is now reviewing and configuring your order."
        ),
        "Accepted": (
            f"{icon} <b>Order Accepted by Admin!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order has been accepted by the admin and is now processing on Domino's."
        ),
        "Order Processing": (
            f"{icon} <b>Order Processing on Domino's!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order is processing on Domino's system. The kitchen will prepare it shortly!"
        ),
        "Placed": (
            f"{icon} <b>Order Placed on Domino's!</b>\n\n"
            f"📦 Order <b>{order.id}</b>{ref_str}\n"
            f"Your order has been successfully placed on Domino's!"
        ),
        "Preparing": (
            f"{icon} <b>Pizza in the Oven!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your pizza is being freshly baked in the kitchen. It'll be ready for delivery soon!"
        ),
        "Out for Delivery": None,  # Handled separately with rider info
        "Delivered": (
            f"{icon} <b>Order Delivered!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order has been delivered. Enjoy your meal! 🍕\n\n"
            f"Thank you for ordering with us!"
        ),
        "Completed": (
            f"{icon} <b>Order Completed!</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order is completed. Thank you for choosing Domino's Order Engine! 🍕"
        ),
        "Cancelled": (
            f"{icon} <b>Order Cancelled</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order has been cancelled."
            + (f"\nReason: {order.cancellation_reason}" if getattr(order, "cancellation_reason", None) else "")
        ),
        "Refunded": (
            f"{icon} <b>Order Refunded</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order has been cancelled and ₹{order.total_payable:.2f} refunded to your wallet."
        )
    }

    # Fallback status text if status is not explicitly in dictionary
    if text is None and new_status != "Out for Delivery":
        text = status_lines.get(new_status)
    if text is None and new_status != "Out for Delivery":
        text = (
            f"{icon} <b>Order Status Update: {new_status}</b>\n\n"
            f"📦 Order <b>{order.id}</b>\n"
            f"Your order status is now: <b>{new_status}</b>"
        )

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
    if text and order.estimated_delivery and _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None) > order.estimated_delivery:
        text += "\n\n⏳ <b>Slight Delay Notice:</b> Your order is taking slightly longer than expected due to store traffic. We apologize for the delay and are working to get it delivered as fast as possible!"

    if text and send_bot_message_func and user.telegram_id:
        try:
            markup = {
                "inline_keyboard": [
                    [{"text": "💬 Contact Support", "url": "https://t.me/dominosordersHELP_bot"}]
                ]
            }
            # If there's a screenshot attached, send it as a photo with caption
            screenshot_url = getattr(order, "screenshot_url", None)
            if screenshot_url and send_bot_photo_func:
                await send_bot_photo_func(user.telegram_id, screenshot_url, caption=text, reply_markup=markup)
            else:
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
