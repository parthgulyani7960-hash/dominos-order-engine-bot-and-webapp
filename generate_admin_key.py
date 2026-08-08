import secrets
import string
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "app")))

from backend.database import SessionLocal, SystemConfig

db = SessionLocal()
alphabet = string.ascii_letters + string.digits
key = "".join(secrets.choice(alphabet) for _ in range(50))

cfg = db.query(SystemConfig).filter(SystemConfig.key == "admin_session_key").first()
if not cfg:
    cfg = SystemConfig(key="admin_session_key", value=key)
    db.add(cfg)
else:
    cfg.value = key

db.commit()
db.close()
print("GENERATED_ADMIN_KEY=" + key)
