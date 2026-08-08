import asyncio
import sys
import traceback
from sqlalchemy.orm import Session
from app.backend.database import SessionLocal, User
from app.backend.services.dominos_session_manager import request_otp

async def main():
    db = SessionLocal()
    admin = db.query(User).filter(User.role == 'admin').first()
    if not admin:
        print("No admin user found")
        return
    print(f"Using admin: {admin.username}")
    
    # Try requesting OTP
    mobile = "9999999999"
    print(f"Triggering request_otp for {mobile}...")
    try:
        res = await request_otp(db, admin, mobile)
        print("Result:", res)
    except Exception as e:
        print("Error calling request_otp:")
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())
