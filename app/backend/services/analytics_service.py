"""
Analytics Service - Computes platform metrics for the Admin Dashboard.
"""
import datetime
from typing import Dict, List
from sqlalchemy.orm import Session
from sqlalchemy import func

from ..database import Order, OrderItem, Product, User


def get_revenue_summary(db: Session, days: int = 30) -> Dict:
    """Revenue totals for the last N days."""
    since = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=days)
    orders = db.query(Order).filter(
        Order.created_at >= since,
        Order.status.notin_(["Cancelled", "Refunded", "Payment Pending"])
    ).all()

    total_revenue = sum(o.total_payable for o in orders)
    total_orders = len(orders)
    avg_order_value = total_revenue / total_orders if total_orders else 0

    return {
        "total_revenue": round(total_revenue, 2),
        "total_orders": total_orders,
        "avg_order_value": round(avg_order_value, 2),
        "period_days": days,
    }


def get_daily_revenue(db: Session, days: int = 14) -> List[Dict]:
    """Day-by-day revenue for chart rendering."""
    result = []
    today = datetime.datetime.now(datetime.timezone.utc).date()
    for i in range(days - 1, -1, -1):
        day = today - datetime.timedelta(days=i)
        start = datetime.datetime.combine(day, datetime.time.min)
        end = datetime.datetime.combine(day, datetime.time.max)
        orders = db.query(Order).filter(
            Order.created_at >= start,
            Order.created_at <= end,
            Order.status.notin_(["Cancelled", "Refunded", "Payment Pending"])
        ).all()
        revenue = sum(o.total_payable for o in orders)
        result.append({
            "date": day.strftime("%d %b"),
            "revenue": round(revenue, 2),
            "orders": len(orders),
        })
    return result


def get_top_products(db: Session, limit: int = 10) -> List[Dict]:
    """Most ordered products by total quantity sold."""
    rows = (
        db.query(
            Product.id,
            Product.name,
            Product.category,
            Product.image_url,
            func.sum(OrderItem.quantity).label("total_qty"),
            func.sum(OrderItem.quantity * OrderItem.price).label("total_revenue"),
        )
        .join(OrderItem, OrderItem.product_id == Product.id)
        .join(Order, Order.id == OrderItem.order_id)
        .filter(Order.status.notin_(["Cancelled", "Refunded"]))
        .group_by(Product.id)
        .order_by(func.sum(OrderItem.quantity).desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "product_id": r.id,
            "name": r.name,
            "category": r.category,
            "image_url": r.image_url,
            "total_qty": int(r.total_qty or 0),
            "total_revenue": round(float(r.total_revenue or 0), 2),
        }
        for r in rows
    ]


def get_order_status_distribution(db: Session) -> dict:
    """Count of orders per status for donut chart (returned as dict)."""
    rows = (
        db.query(Order.status, func.count(Order.id).label("count"))
        .group_by(Order.status)
        .all()
    )
    return {r.status: r.count for r in rows}


def get_new_users_trend(db: Session, days: int = 14) -> List[Dict]:
    """New user registrations per day."""
    result = []
    today = datetime.datetime.now(datetime.timezone.utc).date()
    for i in range(days - 1, -1, -1):
        day = today - datetime.timedelta(days=i)
        start = datetime.datetime.combine(day, datetime.time.min)
        end = datetime.datetime.combine(day, datetime.time.max)
        count = db.query(User).filter(
            User.created_at >= start,
            User.created_at <= end
        ).count()
        result.append({"date": day.strftime("%Y-%m-%d"), "count": count})

    return result


def get_failed_orders(db: Session) -> int:
    """Count of orders that failed/were cancelled today."""
    today_start = datetime.datetime.combine(
        datetime.datetime.now(datetime.timezone.utc).date(), datetime.time.min
    )
    return db.query(Order).filter(
        Order.status.in_(["Cancelled"]),
        Order.created_at >= today_start
    ).count()


def get_dashboard_summary(db: Session) -> Dict:
    """Full dashboard metrics in one call."""
    today_start = datetime.datetime.combine(
        datetime.datetime.now(datetime.timezone.utc).date(), datetime.time.min
    )
    total_users = db.query(User).filter(User.role == "user").count()
    active_orders = db.query(Order).filter(
        Order.status.notin_(["Delivered", "Completed", "Cancelled", "Refunded"])
    ).count()
    today_orders = db.query(Order).filter(Order.created_at >= today_start).count()
    today_revenue = db.query(func.sum(Order.total_payable)).filter(
        Order.created_at >= today_start,
        Order.status.notin_(["Cancelled", "Refunded", "Payment Pending"])
    ).scalar() or 0.0

    return {
        "total_users": total_users,
        "active_orders": active_orders,
        "today_orders": today_orders,
        "today_revenue": round(float(today_revenue), 2),
        "failed_today": get_failed_orders(db),
    }
