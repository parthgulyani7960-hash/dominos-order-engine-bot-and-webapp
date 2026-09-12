import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "app")))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from backend.database import (
    SessionLocal, User, Order, OrderItem, OrderStatusHistory,
    SavedAddress, DominosSession, GiftCard, Proxy, ProxyLog,
    SupportMessage, AuditLog, Notification, UTRAttempt, DominosOTPRequest,
    VerifiedUTR, WalletTransaction, WithdrawalRequest, OrderNote, RiderAssignment,
    ActiveOffer, seed_default_active_offers, auto_save_persistent_db_state
)

def clean_database():
    db = SessionLocal()
    try:
        print("[CLEAN] Starting comprehensive database cleanup...")

        # 1. Clear audit logs, notifications, support messages
        db.query(AuditLog).delete()
        db.query(Notification).delete()
        db.query(SupportMessage).delete()
        print("[CLEAN] Cleared audit logs, notifications, and support messages.")

        # 2. Clear orders, items, history, notes, rider assignments
        db.query(OrderStatusHistory).delete()
        db.query(OrderNote).delete()
        db.query(RiderAssignment).delete()
        db.query(OrderItem).delete()
        db.query(Order).delete()
        print("[CLEAN] Cleared all orders, order items, history, notes, and rider assignments.")

        # 3. Clear wallet transactions, UTR attempts, verified UTRs, withdrawal requests
        db.query(WalletTransaction).delete()
        db.query(UTRAttempt).delete()
        db.query(VerifiedUTR).delete()
        db.query(WithdrawalRequest).delete()
        print("[CLEAN] Cleared all wallet transactions, UTR attempts, and withdrawal requests.")

        # 4. Clear sessions, addresses, gift cards, proxy logs
        db.query(SavedAddress).delete()
        db.query(DominosSession).delete()
        db.query(DominosOTPRequest).delete()
        db.query(ProxyLog).delete()
        db.query(Proxy).delete()
        db.query(GiftCard).delete()
        print("[CLEAN] Cleared sessions, addresses, OTP requests, proxies, and gift cards.")

        # 5. Reset wallet balances and state for ALL users while PRESERVING user accounts
        users = db.query(User).all()
        for u in users:
            u.wallet_balance = 0.0
            u.bot_state = None
            u.bot_cart = None
        print(f"[CLEAN] Reset wallet balances to ₹0.00 for all {len(users)} registered users (user accounts preserved).")

        # 6. Clear active offers table completely
        db.query(ActiveOffer).delete()
        db.commit()
        print("[CLEAN] Cleared all Active Offers.")

        # 7. Commit changes and update persistent snapshot
        db.commit()
        try:
            auto_save_persistent_db_state(db)
            print("[CLEAN] Persistent JSON backup state updated successfully.")
        except Exception as p_err:
            print(f"[CLEAN] Persistence update notice: {p_err}")

        print("[CLEAN] Database successfully cleaned and reset!")
    except Exception as e:
        db.rollback()
        print(f"[ERROR] Cleanup failed: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    clean_database()
