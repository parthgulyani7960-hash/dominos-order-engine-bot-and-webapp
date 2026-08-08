from app.backend.database import SessionLocal, SystemConfig

db = SessionLocal()
cfg = db.query(SystemConfig).filter(SystemConfig.key == 'mini_app_url').first()
url = 'https://d372030830c58548-122-173-30-142.serveousercontent.com'

if not cfg:
    cfg = SystemConfig(key='mini_app_url', value=url)
    db.add(cfg)
else:
    cfg.value = url

db.commit()
db.close()
print("DB SystemConfig mini_app_url updated successfully to:", url)
