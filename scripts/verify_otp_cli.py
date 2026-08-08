import asyncio
import sys
import os
import traceback

if sys.platform == 'win32':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass

# Add project path to sys.path so we can import app modules correctly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.backend.database import SessionLocal, User, DominosSession
from app.backend.services.dominos_session_manager import (
    request_otp, verify_otp, ACTIVE_OTP_REQUESTS
)

async def print_status_loop(token: str):
    """Prints the background browser status logs to console in real-time."""
    last_msg = ""
    while True:
        req = ACTIVE_OTP_REQUESTS.get(token)
        if not req:
            break
        msg = req.get("last_status", "")
        if msg and msg != last_msg:
            print(f"[Status] {msg}")
            last_msg = msg
        if req.get("browser_ready") or req.get("browser_error"):
            break
        await asyncio.sleep(0.5)

async def main():
    print("=" * 60)
    print("      DOMINOS INDIA OTP LOGIN & SESSION SAVER CLI")
    print("=" * 60)
    
    db = SessionLocal()
    try:
        # 1. Fetch or create admin user
        admin = db.query(User).filter(User.role == 'admin').first()
        if not admin:
            # Create a temporary admin user if database is clean
            print("No admin user found in database. Creating a temporary 'admin' user...")
            admin = User(telegram_id="999999999", username="admin", display_name="Admin", role="admin", is_active=True)
            db.add(admin)
            db.commit()
            db.refresh(admin)
            print(f"Temporary admin user '{admin.username}' created.")
        else:
            print(f"Associated Admin User: '{admin.username}'")

        # 2. Get mobile number
        mobile_number = input("\nEnter your 10-digit Domino's Mobile Number (e.g. 9999999999): ").strip()
        if not mobile_number or len(mobile_number) != 10 or not mobile_number.isdigit():
            print("Error: Invalid mobile number! Must be exactly 10 digits.")
            return

        # 3. Request OTP
        print(f"\nInitiating OTP login browser for +91{mobile_number}...")
        try:
            res = await request_otp(db, admin, mobile_number, manual_mode=False)
            request_token = res.get("request_token")
            if not request_token:
                print("Error: Failed to start login browser context: No request token returned.")
                return
        except Exception as e:
            print(f"Error requesting OTP: {e}")
            traceback.print_exc()
            return

        # 4. Monitor browser setup status
        print("\nStarting background browser context. Please wait...")
        await print_status_loop(request_token)

        # 5. Check if browser successfully requested OTP
        req_data = ACTIVE_OTP_REQUESTS.get(request_token)
        if not req_data:
            print("Error: Browser session terminated unexpectedly.")
            return

        if req_data.get("browser_error"):
            print(f"Error: Browser Automation Error: {req_data['browser_error']}")
            return

        # 6. Ask for OTP input
        print("\n" + "-" * 50)
        print("The browser has successfully navigated and requested the OTP.")
        print("-" * 50)
        
        while True:
            otp = input("\nEnter the 6-digit OTP code you received (or type 'cancel' to exit): ").strip()
            if otp.lower() == 'cancel':
                print("Exiting login flow...")
                break
            if len(otp) != 6 or not otp.isdigit():
                print("Error: Invalid OTP format. The OTP must be a 6-digit numeric code.")
                continue

            print(f"\nSubmitting OTP '{otp}' for verification...")
            try:
                # Start status loop in background while we verify
                status_task = asyncio.create_task(print_status_loop(request_token))
                
                # Perform verification
                session = await verify_otp(db, admin, request_token, otp)
                
                # Cancel status monitoring task
                status_task.cancel()
                
                print("\n" + "=" * 60)
                print("SUCCESS! SESSION VERIFIED & SAVED SUCCESSFULLY")
                print("=" * 60)
                print(f"Session ID: {session.id}")
                print(f"Mobile Number: +91{session.mobile_number}")
                print(f"Active Status: {'Active' if session.is_active else 'Inactive'}")
                print(f"Created At: {session.created_at}")
                print(f"Cookies Count: {len(session.cookies or [])}")
                print("=" * 60)
                break
            except Exception as e:
                print(f"\nVerification Failed: {e}")
                # Check if fatal
                req = ACTIVE_OTP_REQUESTS.get(request_token)
                if not req or req.get("browser_error"):
                    print("Error: Fatal error: Browser context closed/destroyed. You must request a new OTP.")
                    break
                else:
                    print("Warning: Non-fatal error. The browser is still open. You can retry entering the OTP.")
                    print("Please check the OTP code and enter it again.")

    finally:
        db.close()

if __name__ == '__main__':
    # On Windows, set proactor loop policy
    if sys.platform == 'win32':
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        except Exception:
            pass
    asyncio.run(main())
