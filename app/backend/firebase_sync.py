import os
import json
import logging
import datetime
from sqlalchemy import event, inspect
from sqlalchemy.orm import Session
import firebase_admin
from firebase_admin import credentials, firestore

logger = logging.getLogger("app.backend.firebase_sync")

# Initialize Firestore Client
firestore_db = None
is_firebase_active = False

KEY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "firebase-key.json"))

def init_firebase():
    global firestore_db, is_firebase_active
    if not os.path.exists(KEY_PATH):
        logger.warning(f"Firebase service key not found at {KEY_PATH}. Running in offline database mode.")
        return False
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(KEY_PATH)
            firebase_admin.initialize_app(cred)
        firestore_db = firestore.client()
        is_firebase_active = True
        logger.info("Firebase Firestore client initialized successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Firebase admin SDK: {e}", exc_info=True)
        return False

# Reusable serializer for SQLAlchemy models to dict
def model_to_dict(obj):
    try:
        mapper = inspect(obj).mapper
        d = {}
        for c in mapper.column_attrs:
            val = getattr(obj, c.key)
            if isinstance(val, (datetime.datetime, datetime.date)):
                d[c.key] = val.isoformat()
            else:
                d[c.key] = val
        return d
    except Exception as e:
        logger.error(f"Error serializing model {obj}: {e}")
        return {}

# Automatic listeners for SQLAlchemy Session events
@event.listens_for(Session, "after_flush")
def capture_session_changes(session, flush_context):
    if not is_firebase_active:
        return
    if "to_sync" not in session.info:
        session.info["to_sync"] = set()
    if "to_delete" not in session.info:
        session.info["to_delete"] = set()
        
    for obj in list(session.new) + list(session.dirty):
        # Ignore non-Base model instances if any
        if not hasattr(obj, "__tablename__") or not hasattr(obj, "id"):
            continue
        try:
            session.info["to_sync"].add((obj.__tablename__, str(obj.id), json.dumps(model_to_dict(obj))))
        except Exception:
            pass
        
    for obj in list(session.deleted):
        if not hasattr(obj, "__tablename__") or not hasattr(obj, "id"):
            continue
        session.info["to_delete"].add((obj.__tablename__, str(obj.id)))

@event.listens_for(Session, "after_commit")
def sync_changes_to_firebase(session):
    global firestore_db, is_firebase_active
    if not is_firebase_active or not firestore_db:
        return
        
    to_sync = session.info.get("to_sync", set())
    to_delete = session.info.get("to_delete", set())
    
    # Reset session sync logs
    session.info["to_sync"] = set()
    session.info["to_delete"] = set()
    
    try:
        # Bulk-write changes using Firestore batches
        batch = firestore_db.batch()
        count = 0
        
        for table, doc_id, data_json in to_sync:
            try:
                data = json.loads(data_json) if isinstance(data_json, str) else data_json
            except Exception:
                continue
            doc_ref = firestore_db.collection(table).document(doc_id)
            batch.set(doc_ref, data)
            count += 1
            if count >= 400:  # Firestore batch limit is 500
                batch.commit()
                batch = firestore_db.batch()
                count = 0
                
        for table, doc_id in to_delete:
            doc_ref = firestore_db.collection(table).document(doc_id)
            batch.delete(doc_ref)
            count += 1
            if count >= 400:
                batch.commit()
                batch = firestore_db.batch()
                count = 0
                
        if count > 0:
            batch.commit()
    except Exception as e:
        logger.error(f"Error syncing SQLite changes to Firebase Firestore: {e}", exc_info=True)

def restore_database_from_firebase(db: Session):
    """Downloads all documents from Firestore collections and populates SQLite to recover state on startup."""
    global firestore_db, is_firebase_active
    if not is_firebase_active or not firestore_db:
        return
        
    logger.info("Starting local SQLite database restore/synchronization from Firebase Firestore...")
    from .database import (
        User, Order, OrderItem, LocationPricing, Coupon,
        SavedAddress, WalletTransaction, WithdrawalRequest,
        OrderStatusHistory, UTRAttempt, AuditLog, SupportMessage
    )
    
    models_mapping = {
        "users": User,
        "orders": Order,
        "order_items": OrderItem,
        "location_pricings": LocationPricing,
        "coupons": Coupon,
        "saved_addresses": SavedAddress,
        "wallet_transactions": WalletTransaction,
        "withdrawal_requests": WithdrawalRequest,
        "order_status_history": OrderStatusHistory,
        "utr_attempts": UTRAttempt,
        "audit_logs": AuditLog,
        "support_messages": SupportMessage
    }
    
    for collection_name, model_class in models_mapping.items():
        try:
            docs = firestore_db.collection(collection_name).stream()
            count = 0
            for doc in docs:
                data = doc.to_dict()
                # Parse ISO datetime strings back to datetime objects
                mapper = inspect(model_class).mapper
                parsed_data = {}
                for key, val in data.items():
                    if key in mapper.columns:
                        col_type = mapper.columns[key].type
                        if col_type.python_type == datetime.datetime and isinstance(val, str):
                            try:
                                parsed_data[key] = datetime.datetime.fromisoformat(val)
                            except ValueError:
                                parsed_data[key] = None
                        elif col_type.python_type == datetime.date and isinstance(val, str):
                            try:
                                parsed_data[key] = datetime.date.fromisoformat(val)
                            except ValueError:
                                parsed_data[key] = None
                        else:
                            parsed_data[key] = val
                            
                # Check duplicate and merge
                existing = db.query(model_class).filter(model_class.id == doc.id).first()
                if existing:
                    for k, v in parsed_data.items():
                        setattr(existing, k, v)
                else:
                    new_obj = model_class(**parsed_data)
                    db.add(new_obj)
                count += 1
            db.commit()
            if count > 0:
                logger.info(f"Synchronized {count} records from Firestore collection '{collection_name}' to SQLite.")
        except Exception as e:
            logger.error(f"Error restoring collection '{collection_name}' from Firebase: {e}", exc_info=True)
