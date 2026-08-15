"""
Order Processing Service - State Machine for Order Lifecycle
Handles all order state transitions with validation and notifications.
"""
import datetime
import traceback
from typing import Optional, Callable
from sqlalchemy.orm import Session

from ..database import Order, OrderStatusHistory, User, RiderAssignment, Notification, ErrorLog

# Valid order status transitions
STATUS_TRANSITIONS = {
    "Payment Pending":    ["Payment Received", "Cancelled"],
    "Payment Received":   ["Order Processing", "Cancelled"],
    "Order Processing":   ["Preparing", "Cancelled"],
    "Preparing":          ["Out for Delivery", "Cancelled"],
    "Out for Delivery":   ["Delivered"],
    "Delivered":          ["Completed"],
    "Completed":          [],
    "Cancelled":          [],
    "Refunded":           [],
}

# Estimated time for each status (minutes from now)
STATUS_ETA = {
    "Order Processing": 5,
    "Preparing": 20,
    "Out for Delivery": 35,
    "Delivered": 45,
}

# Human-readable status messages for notifications
STATUS_MESSAGES = {
    "Payment Received": "✅ Payment confirmed! Your order is being reviewed.",
    "Order Processing": "👨‍🍳 Your order has been accepted and is being prepared!",
    "Preparing": "🍕 Your pizza is in the oven! Getting ready...",
    "Out for Delivery": "🛵 Your order is out for delivery! Rider is on the way.",
    "Delivered": "🎉 Your order has been delivered! Enjoy your meal!",
    "Completed": "⭐ Order completed. Thank you for ordering with us!",
    "Cancelled": "❌ Your order has been cancelled.",
    "Refunded": "💰 Your refund has been processed.",
}


async def transition_order_status(
    db: Session,
    order_id: str,
    new_status: str,
    note: Optional[str] = None,
    admin_username: Optional[str] = None,
    notify_callback: Optional[Callable] = None,
) -> dict:
    """
    Transitions an order to a new status.
    Returns {"success": bool, "error": str or None, "order": Order}
    """
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        return {"success": False, "error": f"Order {order_id} not found"}

    current_status = order.status
    allowed = STATUS_TRANSITIONS.get(current_status, [])

    if new_status not in allowed:
        return {
            "success": False,
            "error": f"Cannot transition from '{current_status}' to '{new_status}'. Allowed: {allowed}"
        }

    # Update order
    order.status = new_status
    order.updated_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    # Process Refund if status changed to Cancelled or Refunded
    if new_status in ["Cancelled", "Refunded"] and current_status not in ["Cancelled", "Refunded"]:
        user = db.query(User).filter(User.id == order.user_id).first()
        if user:
            user.wallet_balance += order.total_payable
            from ..database import WalletTransaction
            refund_tx = WalletTransaction(
                user_id=user.id,
                amount=order.total_payable,
                type="refund",
                description=f"Refund for cancelled order #{order.id}"
            )
            db.add(refund_tx)

    # Set estimated delivery time
    if new_status in STATUS_ETA:
        order.estimated_delivery = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(
            minutes=STATUS_ETA[new_status]
        )

    # Automatically populate RiderAssignment on transition to "Out for Delivery"
    if new_status == "Out for Delivery":
        import random
        rider_names = ["Ramesh", "Suresh", "Vikram", "Rahul", "Amit", "Karan"]
        rider_name = random.choice(rider_names)
        rider_phone = f"+91 {random.randint(70000, 99999)} {random.randint(10000, 99999)}"
        vehicle_num = f"MH-12-{chr(random.randint(65, 90))}{chr(random.randint(65, 90))}-{random.randint(1000, 9999)}"
        
        rider_assign = db.query(RiderAssignment).filter(RiderAssignment.order_id == order_id).first()
        if not rider_assign:
            rider_assign = RiderAssignment(
                order_id=order_id,
                rider_name=rider_name,
                rider_phone=rider_phone,
                vehicle_number=vehicle_num,
                rider_lat=19.0760,
                rider_lng=72.8777,
                assigned_at=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            )
            db.add(rider_assign)

    # Record status history
    history_entry = OrderStatusHistory(
        order_id=order_id,
        status=new_status,
        note=note or (f"Updated by admin: {admin_username}" if admin_username else None)
    )
    db.add(history_entry)

    # Create persistent notification for user
    user = db.query(User).filter(User.id == order.user_id).first()
    if user:
        notification_body = STATUS_MESSAGES.get(new_status, f"Order status updated to {new_status}")
        # Map status to an image URL (default placeholders)
        status_image_map = {
            "Payment Received": "https://images.unsplash.com/photo-1604068549290-dea0e4a305ca?q=80&w=400&auto=format&fit=crop",
            "Order Processing": "https://images.unsplash.com/photo-1628840042765-356cda07504e?q=80&w=400&auto=format&fit=crop",
            "Preparing": "https://images.unsplash.com/photo-1571066811602-71683a3f680d?q=80&w=400&auto=format&fit=crop",
            "Out for Delivery": "https://images.unsplash.com/photo-1593560708920-61dd98c46a4e?q=80&w=400&auto=format&fit=crop",
            "Delivered": "https://images.unsplash.com/photo-1513104890138-7c749659a591?q=80&w=400&auto=format&fit=crop",
            "Completed": "https://images.unsplash.com/photo-1544982503-9f984c14501a?q=80&w=400&auto=format&fit=crop",
            "Cancelled": "https://images.unsplash.com/photo-1604068549290-dea0e4a305ca?q=80&w=400&auto=format&fit=crop",
            "Refunded": "https://images.unsplash.com/photo-1628840042765-356cda07504e?q=80&w=400&auto=format&fit=crop",
        }
        image_url = status_image_map.get(new_status)
        notif = Notification(
            user_id=user.id,
            title=f"Order {order_id}",
            body=notification_body,
            type="order_update",
            reference_id=order_id,
            image_url=image_url
        )
        db.add(notif)

    # If transitioning to Order Processing, trigger Domino's order submission bot
    if new_status == "Order Processing":
        try:
            from .dominos_service import submit_dominos_order
            await submit_dominos_order(order, db)
        except Exception as e:
            err = ErrorLog(
                type="integration",
                message=f"Failed to submit order {order_id} to Domino's automatically: {e}",
                stack_trace=traceback.format_exc()
            )
            db.add(err)

    db.commit()
    db.refresh(order)

    # Trigger external notifications (bot + SSE)
    if notify_callback and user:
        try:
            await notify_callback(order, user, new_status)
        except Exception as e:
            err = ErrorLog(
                type="notification",
                message=f"Notification callback failed for order {order_id}: {e}",
                stack_trace=traceback.format_exc()
            )
            db.add(err)
            db.commit()

    return {"success": True, "error": None, "order": order}


def get_order_progress_percent(status: str) -> int:
    """Returns a 0-100 progress percentage for the order status."""
    progress_map = {
        "Payment Pending": 5,
        "Payment Received": 15,
        "Order Processing": 30,
        "Preparing": 55,
        "Out for Delivery": 80,
        "Delivered": 100,
        "Completed": 100,
        "Cancelled": 0,
        "Refunded": 0,
    }
    return progress_map.get(status, 0)


def get_status_icon(status: str) -> str:
    """Returns an emoji icon for a given status."""
    icons = {
        "Payment Pending": "⏳",
        "Payment Received": "✅",
        "Order Processing": "📋",
        "Preparing": "👨‍🍳",
        "Out for Delivery": "🛵",
        "Delivered": "🎉",
        "Completed": "⭐",
        "Cancelled": "❌",
        "Refunded": "💰",
    }
    return icons.get(status, "📦")


def serialize_order(order: Order) -> dict:
    """Serialize an order to a JSON-safe dict for API responses."""
    rider_data = None
    if order.rider:
        rider_data = {
            "name": order.rider.rider_name,
            "phone": order.rider.rider_phone,
            "vehicle": order.rider.vehicle_number,
            "lat": order.rider.rider_lat,
            "lng": order.rider.rider_lng,
        }

    return {
        "id": order.id,
        "transaction_id": order.transaction_id,
        "status": order.status,
        "status_icon": get_status_icon(order.status),
        "progress": get_order_progress_percent(order.status),
        "payment_method": order.payment_method,
        "original_total": order.original_total,
        "discount": order.discount,
        "delivery_charge": getattr(order, "delivery_charge", 0.0) or 0.0,
        "service_charge": getattr(order, "service_charge", 0.0) or 0.0,
        "total_payable": order.total_payable,
        "address": order.address,
        "landmark": order.landmark,
        "city": getattr(order, "city", None),
        "latitude": getattr(order, "latitude", None),
        "longitude": getattr(order, "longitude", None),
        "phone": order.phone,
        "delivery_instructions": getattr(order, "delivery_instructions", None),
        "estimated_delivery": order.estimated_delivery.isoformat() if order.estimated_delivery else None,
        "coupon_applied": getattr(order, "coupon_applied", None),
        "rider": rider_data,
        "items": [
            {
                "product_id": item.product_id,
                "product_name": item.product.name if item.product else "Unknown",
                "product_image": item.product.image_url if item.product else None,
                "image_url": item.product.image_url if item.product else None,
                "quantity": item.quantity,
                "price": item.price,
                "crust": getattr(item, "crust", None),
                "size": getattr(item, "size", None),
            }
            for item in order.items
        ],
        "status_history": [
            {
                "status": h.status,
                "note": h.note,
                "timestamp": h.created_at.isoformat()
            }
            for h in sorted(order.status_history, key=lambda x: x.created_at)
        ],
        "created_at": order.created_at.isoformat(),
        "updated_at": order.updated_at.isoformat() if order.updated_at else None,
    }
