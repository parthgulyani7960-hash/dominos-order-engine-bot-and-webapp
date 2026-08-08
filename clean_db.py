import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "app")))

from backend.database import (
    SessionLocal, User, Order, OrderItem, OrderStatusHistory,
    SavedAddress, DominosSession, GiftCard, Proxy, ProxyLog,
    SupportMessage, AuditLog, Notification, UTRAttempt, DominosOTPRequest
)

def clean_database():
    db = SessionLocal()
    try:
        print("[CLEAN] Starting database cleanup of all test/fake data...")
        
        # 1. Delete all audit logs, notifications, support messages
        db.query(AuditLog).delete()
        db.query(Notification).delete()
        db.query(SupportMessage).delete()
        print("[CLEAN] Cleared audit logs, notifications, and support messages.")
        
        # 2. Delete all orders, order items, status histories, UTR attempts
        db.query(OrderStatusHistory).delete()
        db.query(UTRAttempt).delete()
        db.query(OrderItem).delete()
        db.query(Order).delete()
        print("[CLEAN] Cleared all orders, order items, history, and payment attempts.")
        
        # 3. Delete sessions and address records
        db.query(SavedAddress).delete()
        db.query(DominosSession).delete()
        db.query(DominosOTPRequest).delete()
        print("[CLEAN] Cleared active logins, saved addresses, and OTP request queues.")
        
        # 4. Delete proxies and logs
        db.query(ProxyLog).delete()
        db.query(Proxy).delete()
        print("[CLEAN] Cleared proxies and proxy performance logs.")
        
        # 5. Delete gift cards
        db.query(GiftCard).delete()
        print("[CLEAN] Cleared all gift cards.")
        
        # 6. Delete all users except the super admin
        admin_tg_id = os.getenv("ADMIN_TELEGRAM_ID", "7958236048")
        deleted_users = db.query(User).filter(User.telegram_id != admin_tg_id).delete(synchronize_session=False)
        print(f"[CLEAN] Cleared {deleted_users} non-admin users.")
        
        # Reset admin wallet balance to 0
        admin = db.query(User).filter(User.telegram_id == admin_tg_id).first()
        if admin:
            admin.wallet_balance = 0.0
            print("[CLEAN] Reset admin wallet balance to 0.00.")
            
        db.commit()
        print("[CLEAN] Database successfully cleared of all testing data!")
    except Exception as e:
        db.rollback()
        print(f"[ERROR] Cleanup failed: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    clean_database()
