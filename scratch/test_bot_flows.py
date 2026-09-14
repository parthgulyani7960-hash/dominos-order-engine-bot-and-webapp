import sys
sys.stdout.reconfigure(encoding='utf-8')
import asyncio
import os
import logging

logging.basicConfig(level=logging.INFO)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.backend.database import SessionLocal, User, Product, ActiveOffer, Order, auto_restore_persistent_db_state
import app.backend.bot as bot

captured_calls = []

async def mock_send_bot_message(chat_id, text, reply_markup=None, **kwargs):
    captured_calls.append(("send_message", text, reply_markup))
    return True

async def mock_edit_bot_message(chat_id, message_id, text, reply_markup=None, **kwargs):
    captured_calls.append(("edit_message", text, reply_markup))
    return True

async def mock_answer_callback_query(callback_query_id, text=None, show_alert=False):
    captured_calls.append(("answer_cb", text, show_alert))
    return True

async def mock_send_bot_photo(*args, **kwargs):
    captured_calls.append(("send_photo", args, kwargs))
    return True

async def mock_send_bot_animation(*args, **kwargs):
    captured_calls.append(("send_animation", args, kwargs))
    return True

async def mock_send_bot_typing(chat_id):
    return True

bot.send_bot_message = mock_send_bot_message
bot.edit_bot_message = mock_edit_bot_message
bot.answer_callback_query = mock_answer_callback_query
bot.send_bot_photo = mock_send_bot_photo
bot.send_bot_animation = mock_send_bot_animation
bot.send_bot_typing = mock_send_bot_typing

async def run_bot_tests():
    db = SessionLocal()
    auto_restore_persistent_db_state(db)

    telegram_id = "999888777"
    first_name = "Test"
    last_name = "User"
    username = "testuser"

    print("=== STARTING COMPREHENSIVE BOT FLOW TESTING (ORDERED SEQUENCES) ===")
    errors = []

    async def test_msg(text, label, location=None):
        try:
            print(f"\n[TEST MSG] {label}: text='{text}'")
            await bot.handle_bot_message(db, telegram_id, first_name, last_name, username, text, location=location)
            last_call = captured_calls[-1] if captured_calls else None
            msg_snippet = str(last_call[1])[:100] if last_call else "No output"
            print(f"  └ OUTGOING: {msg_snippet.replace('\n', ' ')}")
        except Exception as e:
            print(f"  └ ERROR: {e}")
            import traceback
            traceback.print_exc()
            errors.append((label, f"Message '{text}'", str(e)))

    async def test_cb(data, label, message_id=1001):
        try:
            print(f"\n[TEST CB] {label}: data='{data}'")
            await bot.handle_bot_callback(db, telegram_id, first_name, last_name, username, data, message_id, "query_123")
            last_call = captured_calls[-1] if captured_calls else None
            msg_snippet = str(last_call[1])[:100] if last_call else "No output"
            print(f"  └ OUTGOING: {msg_snippet.replace('\n', ' ')}")
        except Exception as e:
            print(f"  └ ERROR: {e}")
            import traceback
            traceback.print_exc()
            errors.append((label, f"Callback '{data}'", str(e)))

    # Phase 1: Core Navigation Commands
    await test_msg("/start", "1. Command /start")
    await test_msg("🍕 View Menu", "2. Text Menu")
    await test_msg("🎉 Active Offers", "3. Text Active Offers")
    await test_msg("💰 My Wallet", "4. Text Wallet")
    await test_msg("📦 Track Orders", "5. Text Orders")
    await test_msg("📍 Change Location", "6. Text Location")
    await test_msg("💬 Contact Support", "7. Text Support")

    # Cancel support mode to reset state
    await test_msg("❌ Cancel", "8. Cancel Support Mode")
    await test_msg("🛒 View Cart", "9. Text View Cart")

    # Phase 2: Add Product & Active Offer to Cart
    await test_cb("menu_view", "10. CB Menu View")
    await test_cb("menu_cat_Veg Pizza", "11. CB Menu Veg Pizza")

    prod = db.query(Product).first()
    if prod:
        await test_cb(f"cart_add_{prod.id}", f"12. CB Add Product {prod.id}")

    offer = db.query(ActiveOffer).first()
    if offer:
        await test_cb(f"apply_offer_{offer.offer_key}", f"13. CB Apply Offer {offer.offer_key}")

    # Phase 3: Cart & Checkout Steps
    await test_cb("cart_view", "14. CB View Cart")
    await test_cb("checkout_initiate", "15. CB Initiate Checkout")

    # Address step
    await test_cb("checkout_enter_new", "16. CB Enter Address Prompt")
    await test_msg("123 Pizza Street, Suite 4, New York", "17. State Address Input")

    # Phone step
    await test_msg("+919876543210", "18. State Phone Input")

    # Checkout confirmation step & Note
    await test_cb("checkout_add_note", "19. CB Add Note Prompt")
    await test_msg("Extra cheese and crispy crust please!", "20. State Note Input")

    # Order Placement
    await test_cb("order_confirm_place_wallet", "21. CB Place Order via Wallet")
    await test_cb("order_confirm_place_direct_qr", "22. CB Place Order via Direct QR")

    # Phase 4: Topup & Wallet
    await test_cb("topup_menu", "23. CB Topup Menu")
    await test_cb("topup_amount_500", "24. CB Topup 500")
    await test_cb("wallet_history", "25. CB Wallet History")

    # Phase 5: Admin Panel
    user = db.query(User).filter(User.telegram_id == telegram_id).first()
    if user:
        user.role = "admin"
        db.commit()
    await test_msg("🔑 Admin Center", "26. Text Admin Center")
    await test_cb("admin_panel", "27. CB Admin Panel")
    await test_cb("admin_orders", "28. CB Admin Orders")
    await test_cb("admin_users", "29. CB Admin Users")

    db.close()

    print("\n==========================================")
    if errors:
        print(f"FAILED: Found {len(errors)} error(s):")
        for label, target, err in errors:
            print(f" - [{label}] {target}: {err}")
    else:
        print("SUCCESS: All tested bot flows executed cleanly with 0 errors!")
    print("==========================================")

if __name__ == "__main__":
    asyncio.run(run_bot_tests())
