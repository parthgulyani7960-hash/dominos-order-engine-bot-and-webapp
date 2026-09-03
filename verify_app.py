import os
import sys
import unittest

# Standard stream configuration

from fastapi.testclient import TestClient

# Add application backend directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "app")))

# Force mock environment settings for unittest validation
os.environ["TELEGRAM_BOT_TOKEN"] = "MOCK_TOKEN"
os.environ["ADMIN_TELEGRAM_ID"] = "123456789"
os.environ["MINI_APP_URL"] = "http://localhost:8000"
os.environ["JWT_SECRET"] = "verification_secret_key_11223344"
os.environ["ADMIN_USERNAME"] = "admin"
os.environ["ADMIN_PASSWORD"] = "pizza123"

# Set temporary database URL for testing to protect production pizza.db
os.environ["DATABASE_URL"] = f"sqlite:///{os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'pizza_test.db'))}"

from backend.database import init_db, SessionLocal, User, Product, GiftCard, Order, ErrorLog, LoginAttempt, SystemConfig, Proxy, ProxyLog, UserSession, VerifiedUTR, UTRAttempt, QRGenerationHistory, Coupon, SupportMessage, CouponRedemption, WalletTransaction, WithdrawalRequest, OrderStatusHistory, OrderItem, DominosSession
from backend.main import app
from backend.auth import hash_password, verify_password, create_access_token
from backend.utils import encrypt_data, decrypt_data, run_backup

client = TestClient(app)

class TestPizzaPlatform(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # 1. Initialize SQLite schema
        init_db()
        
    def setUp(self):
        self.db = SessionLocal()
        # Clean all tables for isolated assertions
        self.db.query(ProxyLog).delete()
        self.db.query(Proxy).delete()
        self.db.query(OrderItem).delete()
        self.db.query(Order).delete()
        self.db.query(GiftCard).delete()
        self.db.query(User).delete()
        self.db.query(ErrorLog).delete()
        self.db.query(LoginAttempt).delete()
        self.db.query(Product).delete()
        self.db.query(SystemConfig).delete()
        self.db.query(UserSession).delete()
        self.db.query(VerifiedUTR).delete()
        self.db.query(UTRAttempt).delete()
        self.db.query(QRGenerationHistory).delete()
        self.db.query(SupportMessage).delete()
        self.db.query(CouponRedemption).delete()
        self.db.query(WalletTransaction).delete()
        self.db.query(WithdrawalRequest).delete()
        self.db.query(OrderStatusHistory).delete()
        self.db.query(Coupon).delete()
        self.db.query(DominosSession).delete()
        self.db.commit()

        
        from backend.main import seed_database
        seed_database()
        
        # Seed default test pizzas only inside verification suite
        default_pizzas = [
            {
                "name": "Margherita Classic",
                "description": "Simple perfection: Fresh basil, premium mozzarella cheese, and rich organic tomato sauce over a thin crispy crust.",
                "category": "Veg",
                "is_veg": True,
                "original_price": 199.00,
                "discounted_price": 159.00,
                "image_url": "https://images.unsplash.com/photo-1604068549290-dea0e4a305ca?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 1,
                "is_popular": True,
                "is_recommended": True,
                "crust_options": '["Thin Crust","Wheat Thin Crust","Hand Tossed"]',
                "size_options": '["Regular (10\\")", "Medium (12\\")", "Large (14\\")"]',
            },
            {
                "name": "Pepperoni Feast",
                "description": "Double portion of spicy, crispy pepperoni over strings of melted mozzarella and seasoned marinara sauce.",
                "category": "Non-Veg",
                "is_veg": False,
                "original_price": 299.00,
                "discounted_price": 249.00,
                "image_url": "https://images.unsplash.com/photo-1628840042765-356cda07504e?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 2,
                "is_popular": True,
                "is_recommended": False,
                "crust_options": '["Thin Crust","Hand Tossed","Cheese Burst"]',
                "size_options": '["Regular (10\\")", "Medium (12\\")", "Large (14\\")"]',
            },
            {
                "name": "Garden Veggie Supreme",
                "description": "Healthy feast of green bell peppers, red onions, mushrooms, black olives, sweet corn, and cherry tomatoes.",
                "category": "Veg",
                "is_veg": True,
                "original_price": 249.00,
                "discounted_price": None,
                "image_url": "https://images.unsplash.com/photo-1571066811602-71683a3f680d?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 3,
                "is_popular": False,
                "is_recommended": True,
                "crust_options": '["Thin Crust","Wheat Thin Crust","Hand Tossed"]',
                "size_options": '["Regular (10\\")", "Medium (12\\")", "Large (14\\")"]',
            },
            {
                "name": "BBQ Smoked Chicken",
                "description": "Tender grilled chicken breast, hickory-smoked BBQ sauce, red onions, and fresh cilantro leaves.",
                "category": "Non-Veg",
                "is_veg": False,
                "original_price": 349.00,
                "discounted_price": 299.00,
                "image_url": "https://images.unsplash.com/photo-1513104890138-7c749659a591?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 4,
                "is_popular": True,
                "is_recommended": False,
                "crust_options": '["Thin Crust","Hand Tossed","Cheese Burst"]',
                "size_options": '["Regular (10\\")", "Medium (12\\")", "Large (14\\")"]',
            },
            {
                "name": "Double Cheese Romano",
                "description": "Extra rich blend of creamy mozzarella, sharp cheddar, Parmesan, and a touch of blue cheese.",
                "category": "Veg",
                "is_veg": True,
                "original_price": 229.00,
                "discounted_price": 199.00,
                "image_url": "https://images.unsplash.com/photo-1593560708920-61dd98c46a4e?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 5,
                "is_popular": False,
                "is_recommended": False,
                "crust_options": '["Thin Crust","Cheese Burst","Hand Tossed"]',
                "size_options": '["Regular (10\\")", "Medium (12\\")", "Large (14\\")"]',
            },
            {
                "name": "Cheeseburst Margherita",
                "description": "Double layers of liquid cheese between two crusts, topped with premium mozzarella cheese and classic Italian Margherita sauce.",
                "category": "Cheese Burst",
                "is_veg": True,
                "original_price": 399.00,
                "discounted_price": 349.00,
                "image_url": "https://images.unsplash.com/photo-1604068549290-dea0e4a305ca?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 6,
                "is_popular": True,
                "is_recommended": True,
                "crust_options": '["Cheese Burst"]',
                "size_options": '["Medium (12\\")", "Large (14\\")"]',
            },
            {
                "name": "Tomato Onion Pizza Mania",
                "description": "Tangy tomatoes and crunchy onions on a classic hand-tossed base. Great value!",
                "category": "Pizza Mania",
                "is_veg": True,
                "original_price": 99.00,
                "discounted_price": 89.00,
                "image_url": "https://images.unsplash.com/photo-1571066811602-71683a3f680d?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 7,
                "is_popular": True,
                "is_recommended": True,
                "crust_options": '["Hand Tossed"]',
                "size_options": '["Regular (10\\")"]',
            },
            {
                "name": "Golden Corn Pizza Mania",
                "description": "Sweet golden corn with melted mozzarella cheese on a hand-tossed base.",
                "category": "Pizza Mania",
                "is_veg": True,
                "original_price": 119.00,
                "discounted_price": 99.00,
                "image_url": "https://images.unsplash.com/photo-1593560708920-61dd98c46a4e?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 8,
                "is_popular": False,
                "is_recommended": True,
                "crust_options": '["Hand Tossed"]',
                "size_options": '["Regular (10\\")"]',
            },
            {
                "name": "Truffle Mushroom Artisan",
                "description": "Earthy portobello and white button mushrooms finished with truffle oil, garlic confit, and shaved Parmesan.",
                "category": "Veg",
                "is_veg": True,
                "original_price": 349.00,
                "discounted_price": 299.00,
                "image_url": "https://images.unsplash.com/photo-1544982503-9f984c14501a?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 9,
                "is_popular": False,
                "is_recommended": True,
                "crust_options": '["Thin Crust","Hand Tossed"]',
                "size_options": '["Regular (10\\")", "Medium (12\\")", "Large (14\\")"]',
            },
            {
                "name": "Cheeseburst Margherita (Medium)",
                "description": "Premium liquid cheese filled crust with classic Margherita topping.",
                "category": "Veg",
                "is_veg": True,
                "original_price": 205.00,
                "discounted_price": None,
                "image_url": "https://images.unsplash.com/photo-1604068549290-dea0e4a305ca?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 10,
                "crust_options": '["Cheese Burst"]',
                "size_options": '["Medium (12\\")"]',
            },
            {
                "name": "Tomato Ketchup (Auto-Added)",
                "description": "Auto-added tomato ketchup for cart promos.",
                "category": "Veg",
                "is_veg": True,
                "original_price": 0.00,
                "discounted_price": None,
                "image_url": "https://images.unsplash.com/photo-1604068549290-dea0e4a305ca?q=80&w=400&auto=format&fit=crop",
                "availability": True,
                "sort_order": 11,
            },
        ]
        for p in default_pizzas:
            self.db.add(Product(**p))
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_01_security_utils(self):
        """Validates encryption helpers and password hashing functions."""
        # Encryption
        plain_text = "VOUCHER-ABC-1234"
        enc = encrypt_data(plain_text)
        self.assertNotEqual(plain_text, enc)
        
        dec = decrypt_data(enc)
        self.assertEqual(plain_text, dec)
        
        # Admin credentials PBKDF2
        raw_pw = "pass123"
        hashed = hash_password(raw_pw)
        self.assertTrue(verify_password(hashed, raw_pw))
        self.assertFalse(verify_password(hashed, "wrong_pw"))

    def test_02_database_seeding(self):
        """Verifies default products are successfully seeded by startup routines."""
        products_count = self.db.query(Product).count()
        self.assertGreater(products_count, 0, "Default products should have been seeded on app start.")

    def test_03_telegram_mock_login(self):
        """Simulates Telegram user login and token generation."""
        mock_init_data = "user=%7B%22id%22%3A7958236048%2C%22first_name%22%3A%22Test%22%2C%22username%22%3A%22testuser%22%7D&hash=mock_hash"
        resp = client.post("/api/auth/login", json={"initData": mock_init_data})
        self.assertEqual(resp.status_code, 200)
        
        data = resp.json()
        self.assertIn("access_token", data)
        self.assertEqual(data["user"]["telegram_id"], "7958236048")
        
        # Verify user created in DB
        db_user = self.db.query(User).filter(User.telegram_id == "7958236048").first()
        self.assertIsNotNone(db_user)
        self.assertEqual(db_user.wallet_balance, 100.0) # check default test balance

    def test_04_order_processing_checkout(self):
        """Validates cart checkout and wallet deduction."""
        # 1. Create a customer
        customer = User(
            telegram_id="999999",
            username="buyer",
            display_name="Buyer Doe",
            wallet_balance=1000.0,
            role="user"
        )
        self.db.add(customer)
        self.db.flush()
        
        # 2. Get a seeded pizza
        pizza = self.db.query(Product).filter(Product.availability == True).first()
        self.assertIsNotNone(pizza)
        self.db.commit()
        
        # 4. Generate user access token
        token = create_access_token({"sub": str(customer.id), "role": "user"})
        headers = {"Authorization": f"Bearer {token}"}
        
        # 5. Place order
        checkout_payload = {
            "items": [{"product_id": pizza.id, "quantity": 2}],
            "payment_method": "wallet",
            "address": "123 Main St, New York",
            "landmark": "Near park",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "phone": "+15550199"
        }
        
        resp = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp.status_code, 200, f"Checkout failed: {resp.text}")
        
        data = resp.json()
        self.assertEqual(data["status"], "Order Processing")
        self.assertIn("invoice", data)
        
        # 6. Verify wallet balance deducted
        self.db.refresh(customer)
        self.assertAlmostEqual(customer.wallet_balance, 1000.0 - data["total"], places=2)

    def test_05_checkout_proceeds_on_empty_giftcards(self):
        """Tests that ordering proceeds successfully even if gift card inventory is empty."""
        # 1. Create a customer
        customer = User(
            telegram_id="888888",
            username="buyer2",
            display_name="Buyer Two",
            wallet_balance=1000.0,
            role="user"
        )
        self.db.add(customer)
        self.db.flush()
        
        pizza = self.db.query(Product).filter(Product.availability == True).first()
        self.db.commit()
        
        token = create_access_token({"sub": str(customer.id), "role": "user"})
        headers = {"Authorization": f"Bearer {token}"}
        
        checkout_payload = {
            "items": [{"product_id": pizza.id, "quantity": 1}],
            "payment_method": "wallet",
            "address": "456 Side St",
            "latitude": 40.7,
            "longitude": -74.0,
            "phone": "+15550299"
        }
        
        resp = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp.status_code, 200, f"Checkout failed: {resp.text}")
        
        data = resp.json()
        self.assertEqual(data["status"], "Order Processing")

    def test_06_admin_dashboard_metrics(self):
        """Verifies stats aggregation and administrative actions."""
        # Generate admin access token
        admin_token = create_access_token({"sub": "0", "role": "admin"})
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        resp = client.get("/api/admin/stats", headers=headers)
        self.assertEqual(resp.status_code, 200)
        
        stats = resp.json()
        self.assertIn("revenue", stats)
        self.assertIn("active_users", stats)
        self.assertIn("online_users", stats)
        self.assertIn("gift_cards", stats)

    def test_07_admin_creates_user(self):
        """Verifies that an admin can manually create a customer or admin account."""
        # Generate admin access token
        admin_token = create_access_token({"sub": "0", "role": "admin"})
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        # 1. Create a regular user
        create_payload = {
            "telegram_id": "88776655",
            "username": "new_customer",
            "display_name": "New Customer",
            "wallet_balance": 150.0,
            "role": "user"
        }
        
        resp = client.post("/api/admin/users", json=create_payload, headers=headers)
        self.assertEqual(resp.status_code, 200)
        
        data = resp.json()
        self.assertEqual(data["telegram_id"], "88776655")
        self.assertEqual(data["username"], "new_customer")
        self.assertEqual(data["display_name"], "New Customer")
        self.assertEqual(data["wallet_balance"], 150.0)
        self.assertEqual(data["role"], "user")
        
        # Verify created in DB
        db_user = self.db.query(User).filter(User.telegram_id == "88776655").first()
        self.assertIsNotNone(db_user)
        self.assertEqual(db_user.display_name, "New Customer")
        
        # 2. Attempt to create with duplicate Telegram ID
        resp_dup = client.post("/api/admin/users", json=create_payload, headers=headers)
        self.assertEqual(resp_dup.status_code, 400)
        
        # 3. Create an admin user
        admin_payload = {
            "telegram_id": "11223344",
            "username": "new_admin",
            "display_name": "New Admin Assistant",
            "wallet_balance": 0.0,
            "role": "admin"
        }
        
        resp_admin = client.post("/api/admin/users", json=admin_payload, headers=headers)
        self.assertEqual(resp_admin.status_code, 200)
        self.assertEqual(resp_admin.json()["role"], "admin")

    def test_08_admin_login_lockout(self):
        """Verifies admin login brute force locking and attempt logging."""
        # 1. Trigger 5 failed login attempts
        payload = {"username": "admin", "password": "wrongpassword"}
        for i in range(5):
            resp = client.post("/api/admin/login", json=payload)
            self.assertEqual(resp.status_code, 401)
        
        # 2. The 6th attempt should be blocked with 403 Forbidden due to lockout
        resp_lockout = client.post("/api/admin/login", json=payload)
        self.assertEqual(resp_lockout.status_code, 403)
        self.assertIn("locked out", resp_lockout.json()["detail"].lower())

        # 3. Seed a valid 50-char admin session key for testing
        test_key = "TESTSECRETKEY1234567890TESTSECRETKEY12345678901234"
        cfg = self.db.query(SystemConfig).filter(SystemConfig.key == "admin_session_key").first()
        if not cfg:
            cfg = SystemConfig(key="admin_session_key", value=test_key)
            self.db.add(cfg)
        else:
            cfg.value = test_key
        self.db.commit()

        # 4. Verify that a correct login is also rejected with 403 while locked out
        correct_payload = {"username": "admin", "password": test_key}
        resp_correct_blocked = client.post("/api/admin/login", json=correct_payload)
        self.assertEqual(resp_correct_blocked.status_code, 403)
        self.assertIn("locked out", resp_correct_blocked.json()["detail"].lower())

        # 4. Check that login attempts are correctly logged in the database and accessible to admin
        admin_token = create_access_token({"sub": "0", "role": "admin"})
        headers = {"Authorization": f"Bearer {admin_token}"}
        resp_attempts = client.get("/api/admin/login-attempts", headers=headers)
        self.assertEqual(resp_attempts.status_code, 200)
        attempts = resp_attempts.json()
        self.assertGreaterEqual(len(attempts), 6) # At least our 6 attempts

    def test_09_dominos_cart_pricing(self):
        """Verifies the Domino's cart pricing cap (subtotal ₹180-₹220 capped at ₹100) and auto-coupon application."""
        # 1. Create a customer
        customer = User(
            telegram_id="87654321",
            username="pizza_lover",
            display_name="Pizza Lover",
            wallet_balance=1000.0,
            role="user"
        )
        self.db.add(customer)
        # Ensure a product is available and has a price that can hit ₹180-₹220
        pizza = Product(
            name="Test Capping Pizza",
            description="Tastes like code",
            category="Veg",
            is_veg=True,
            original_price=100.0,
            discounted_price=100.0,
            availability=True,
            sort_order=1
        )
        self.db.add(pizza)
        # Seed some gift cards so order doesn't pause/fail on gift card extraction
        for code in ["GIFT-1122", "GIFT-2233", "GIFT-3344", "GIFT-4455"]:
            gift_card = GiftCard(
                code_encrypted=encrypt_data(code),
                code_hash=hashlib_sha256(code),
                pin_encrypted=encrypt_data("1234"),
                value=250.0,
                status="available"
            )
            self.db.add(gift_card)
        self.db.commit()

        # Generate access token for the customer
        cust_token = create_access_token({"sub": str(customer.id), "role": "user"})
        headers = {"Authorization": f"Bearer {cust_token}"}

        # 2. Test first order (Newbie) with subtotal = 200.0 (within 180-220 range)
        checkout_payload = {
            "items": [{"product_id": pizza.id, "quantity": 2}],
            "payment_method": "wallet",
            "address": "123 Coding Street",
            "latitude": 12.9716,
            "longitude": 77.5946,
            "phone": "9876543210"
        }
        
        resp = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["coupon_applied"], None)
        self.assertEqual(data["total"], 210.0) # subtotal 200.0 + ₹10 bot fee!

        # 3. Test second order with subtotal = 200.0
        resp_2 = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp_2.status_code, 200)
        data_2 = resp_2.json()
        self.assertEqual(data_2["coupon_applied"], None)
        self.assertEqual(data_2["total"], 210.0)

        # 4. Verify admin can change config, and the changes are respected in subsequent checkouts
        admin_token = create_access_token({"sub": "0", "role": "admin"})
        admin_headers = {"Authorization": f"Bearer {admin_token}"}
        
        new_config = {
            "newbie_coupon": "NEWBIE200",
            "welcome_coupon": "WELCOME150",
            "cart_promo_min": "300.0",
            "cart_promo_max": "400.0",
            "cart_promo_fixed": "150.0"
        }
        for k, v in new_config.items():
            resp_config = client.put("/api/admin/config", json={"key": k, "value": v}, headers=admin_headers)
            self.assertEqual(resp_config.status_code, 200)

        # Subtotal + bot fee is calculated
        resp_not_capped = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp_not_capped.status_code, 200)
        data_not_capped = resp_not_capped.json()
        self.assertEqual(data_not_capped["coupon_applied"], None)
        self.assertEqual(data_not_capped["total"], 210.0)

        # Subtotal = 300.0 (3 pizzas of price 100.0) should be capped at ₹150 + ₹10 bot fee with WELCOME150 applied
        checkout_payload_capped = {
            "items": [{"product_id": pizza.id, "quantity": 3}],
            "payment_method": "wallet",
            "address": "123 Coding Street",
            "latitude": 12.9716,
            "longitude": 77.5946,
            "phone": "9876543210"
        }
        
        # Add another gift card to cover value
        self.db.add(GiftCard(
            code_encrypted=encrypt_data("GIFT-5566"),
            code_hash=hashlib_sha256("GIFT-5566"),
            pin_encrypted=encrypt_data("1234"),
            value=250.0,
            status="available"
        ))
        resp_capped = client.post("/api/orders", json=checkout_payload_capped, headers=headers)
        self.assertEqual(resp_capped.status_code, 200)
        data_capped = resp_capped.json()
        self.assertEqual(data_capped["coupon_applied"], None)
        self.assertEqual(data_capped["total"], 310.0)

    def test_10_proxy_crud_and_status(self):
        """Verifies proxy CRUD endpoints and log tracing."""
        # Generate admin access token
        admin_token = create_access_token({"sub": "0", "role": "admin"})
        headers = {"Authorization": f"Bearer {admin_token}"}

        # 1. Create a proxy
        payload = {
            "ip": "127.0.0.1",
            "port": 8888,
            "username": "user",
            "password": "pass",
            "protocol": "http",
            "is_active": True
        }
        resp = client.post("/api/admin/proxies", json=payload, headers=headers)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("id", data)
        self.assertEqual(data["ip"], "127.0.0.1")

        # 2. Get proxies list
        resp_list = client.get("/api/admin/proxies", headers=headers)
        self.assertEqual(resp_list.status_code, 200)
        self.assertEqual(len(resp_list.json()), 1)

        # 3. Update proxy (make inactive)
        proxy_id = data["id"]
        update_payload = {"is_active": False}
        resp_update = client.put(f"/api/admin/proxies/{proxy_id}", json=update_payload, headers=headers)
        self.assertEqual(resp_update.status_code, 200)
        self.assertFalse(resp_update.json()["is_active"])

        # 4. Trigger test connection (should fail since 127.0.0.1:8888 is not a real proxy)
        resp_test = client.post(f"/api/admin/proxies/{proxy_id}/test", headers=headers)
        self.assertEqual(resp_test.status_code, 200)
        self.assertFalse(resp_test.json()["success"])

        # 5. Retrieve proxy logs
        resp_logs = client.get("/api/admin/proxies/logs", headers=headers)
        self.assertEqual(resp_logs.status_code, 200)
        self.assertGreaterEqual(len(resp_logs.json()), 1)

        # 6. Delete proxy
        resp_delete = client.delete(f"/api/admin/proxies/{proxy_id}", headers=headers)
        self.assertEqual(resp_delete.status_code, 200)
        self.assertEqual(resp_delete.json()["status"], "success")

    def test_11_dominos_order_submission_with_proxy(self):
        """Tests order processing submits order using rotated proxies."""
        # 1. Add an active proxy to the database
        proxy = Proxy(
            ip="127.0.0.1",
            port=9090,
            protocol="http",
            is_active=True
        )
        self.db.add(proxy)
        
        # 2. Create customer & seed gift card
        customer = User(
            telegram_id="111222",
            username="pizzabuyer",
            display_name="Pizza Buyer",
            wallet_balance=1000.0,
            role="user"
        )
        self.db.add(customer)
        
        pizza = self.db.query(Product).filter(Product.availability == True).first()
        gc = GiftCard(
            code_encrypted=encrypt_data("GIFT-999"),
            code_hash=hashlib_sha256("GIFT-999"),
            pin_encrypted=encrypt_data("4321"),
            value=500.0,
            status="available"
        )
        self.db.add(gc)
        self.db.commit()

        # 3. Place order
        token = create_access_token({"sub": str(customer.id), "role": "user"})
        headers = {"Authorization": f"Bearer {token}"}
        
        checkout_payload = {
            "items": [{"product_id": pizza.id, "quantity": 1}],
            "payment_method": "wallet",
            "address": "123 Coding Street",
            "latitude": 12.9716,
            "longitude": 77.5946,
            "phone": "9876543210"
        }
        resp = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp.status_code, 200)
        
        # 4. As an admin, change order status to 'Order Processing'
        admin_token = create_access_token({"sub": "0", "role": "admin"})
        admin_headers = {"Authorization": f"Bearer {admin_token}"}
        order_id = resp.json()["order_id"]
        
        # Transitioning to Order Processing triggers submit_dominos_order
        resp_transition = client.put(f"/api/admin/orders/{order_id}/status", json={"status": "Order Processing"}, headers=admin_headers)
        self.assertEqual(resp_transition.status_code, 200)
        
        # 5. Verify the order has a dominos_reference assigned by the integration service
        temp_db = SessionLocal()
        try:
            order = temp_db.query(Order).filter(Order.id == order_id).first()
            self.assertIsNotNone(order.dominos_reference)
            self.assertTrue(order.dominos_reference.startswith("DOM-REF-"))
        finally:
            temp_db.close()

    def test_12_telegram_bot_handlers(self):
        """Simulates Telegram user commands and verifies bot handler responses and state updates."""
        import asyncio
        from backend.database import SupportMessage, SystemConfig
        from backend.bot import handle_bot_message
        
        # 1. Start command
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "/start"))
        user = self.db.query(User).filter(User.telegram_id == "111222").first()
        self.assertIsNotNone(user)
        
        # 2. View menu
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "/menu"))
        
        # 3. View wallet
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "/wallet"))
        
        # 4. View offers
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "/offers"))
        
        # 5. Track orders (empty)
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "/track"))
        
        # 6. Support command
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "/support"))
        
        # 7. Help command
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "/help"))
        
        # 8. Send support message (fallback)
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "Please deliver to the backyard"))
        
        temp_db = SessionLocal()
        try:
            user_in_db = temp_db.query(User).filter(User.telegram_id == "111222").first()
            sup_msg = temp_db.query(SupportMessage).filter(SupportMessage.user_id == user_in_db.id).first()
            self.assertIsNotNone(sup_msg)
            self.assertEqual(sup_msg.message, "Please deliver to the backyard")
        finally:
            temp_db.close()

            
        # 9. Admin keys generation
        os.environ["ADMIN_TELEGRAM_ID"] = "111222"
        asyncio.run(handle_bot_message(self.db, "111222", "Test", "User", "testuser", "/secret_key"))
        
        temp_db2 = SessionLocal()
        try:
            cfg = temp_db2.query(SystemConfig).filter(SystemConfig.key == "admin_session_key").first()
            self.assertIsNotNone(cfg)
            self.assertTrue(len(cfg.value) > 40)
        finally:
            temp_db2.close()

    def test_13_admin_wallet_and_session_management(self):
        """Tests the new admin wallet adjustment and Domino's session management endpoints."""
        from backend.database import SystemConfig, DominosSession
        # 1. Setup admin session and auth headers
        from backend.routes import create_access_token
        admin_token = create_access_token({"sub": "0", "role": "admin"})
        admin_headers = {"Authorization": f"Bearer {admin_token}"}

        # Create a test user
        user = User(
            telegram_id="999888",
            display_name="Test Wallet User",
            wallet_balance=100.0
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)

        # 2. Test Wallet Adjustment endpoint
        resp_wallet = client.put(
            f"/api/admin/users/{user.id}/wallet",
            json={"amount": 50.0, "reason": "Test adjustment"},
            headers=admin_headers
        )
        self.assertEqual(resp_wallet.status_code, 200)
        self.assertEqual(resp_wallet.json()["new_balance"], 150.0)

        # Verify balance updated in DB
        self.db.refresh(user)
        self.assertEqual(user.wallet_balance, 150.0)

        # 3. Create a Domino's Session manually
        admin_user = self.db.query(User).filter(User.role == "admin").first()
        admin_id = admin_user.id if admin_user else 1
        
        session = DominosSession(
            mobile_number="9999999999",
            cookies=[{"name": "test_cookie", "value": "xyz"}],
            is_active=True,
            admin_id=admin_id
        )
        self.db.add(session)
        self.db.commit()
        self.db.refresh(session)

        # Test GET sessions list
        resp_sessions = client.get("/api/admin/dominos/sessions", headers=admin_headers)
        self.assertEqual(resp_sessions.status_code, 200)
        self.assertTrue(any(s["mobile_number"] == "9999999999" for s in resp_sessions.json()))

        # Test GET session cookies JSON
        resp_cookies = client.get(f"/api/admin/dominos/sessions/{session.id}/cookies", headers=admin_headers)
        self.assertEqual(resp_cookies.status_code, 200)
        self.assertIn("test_cookie", resp_cookies.json()["cookies_json"])

        # Test PUT toggle active status
        resp_toggle = client.put(f"/api/admin/dominos/sessions/{session.id}/toggle", headers=admin_headers)
        self.assertEqual(resp_toggle.status_code, 200)
        self.assertFalse(resp_toggle.json()["is_active"])

        self.assertFalse(resp_toggle.json()["is_active"])

        # Verify status toggled in DB
        self.db.refresh(session)
        self.assertFalse(session.is_active)

    def test_14_flat_service_charge(self):
        """Verifies that non-capped orders have a flat ₹5.00 service charge applied."""
        # Configure bot_fee to 5.0 for this test
        bot_fee_cfg = self.db.query(SystemConfig).filter(SystemConfig.key == "bot_fee").first()
        if bot_fee_cfg:
            bot_fee_cfg.value = "5.0"
        else:
            self.db.add(SystemConfig(key="bot_fee", value="5.0"))
        self.db.commit()

        customer = User(
            telegram_id="999914",
            username="buyer14",
            display_name="Buyer 14",
            wallet_balance=1000.0,
            role="user"
        )
        self.db.add(customer)
        self.db.commit()

        # Seed gift cards
        for code in ["GIFT-14-1", "GIFT-14-2"]:
            self.db.add(GiftCard(
                code_encrypted=encrypt_data(code),
                code_hash=hashlib_sha256(code),
                pin_encrypted=encrypt_data("1234"),
                value=250.0,
                status="available"
            ))
        self.db.commit()

        cheap_pizza = Product(
            name="Cheap Pizza",
            description="Tastes cheap",
            category="Veg",
            original_price=9.99,
            availability=True,
            sort_order=0
        )
        self.db.add(cheap_pizza)
        self.db.commit()

        pizza = cheap_pizza
        token = create_access_token({"sub": str(customer.id), "role": "user"})
        headers = {"Authorization": f"Bearer {token}"}

        checkout_payload = {
            "items": [{"product_id": pizza.id, "quantity": 1}],
            "payment_method": "wallet",
            "address": "Store Area",
            "latitude": 12.9716,
            "longitude": 77.5946,
            "phone": "+9199991414"
        }
        resp = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        
        self.assertEqual(data["invoice"]["service_charge"], 5.0)
        self.assertEqual(data["total"], 15.0)

    def test_15_ketchup_auto_addition(self):
        """Verifies that Tomato Ketchup is auto-added when subtotal is slightly below the promo minimum."""
        customer = User(
            telegram_id="999915",
            username="buyer15",
            display_name="Buyer 15",
            wallet_balance=1000.0,
            role="user"
        )
        self.db.add(customer)
        
        for code in ["GIFT-15-1", "GIFT-15-2"]:
            self.db.add(GiftCard(
                code_encrypted=encrypt_data(code),
                code_hash=hashlib_sha256(code),
                pin_encrypted=encrypt_data("1234"),
                value=250.0,
                status="available"
            ))
        self.db.commit()

        pizza = Product(
            name="Near Promo Pizza",
            description="Tastes like promo",
            category="Veg",
            original_price=165.0,
            availability=True,
            sort_order=12
        )
        self.db.add(pizza)
        self.db.commit()

        token = create_access_token({"sub": str(customer.id), "role": "user"})
        headers = {"Authorization": f"Bearer {token}"}

        checkout_payload = {
            "items": [{"product_id": pizza.id, "quantity": 1}],
            "payment_method": "wallet",
            "address": "Store Area",
            "latitude": 12.9716,
            "longitude": 77.5946,
            "phone": "+9199991515"
        }
        resp = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        
        # Capping is disabled, so subtotal + bot fee is calculated
        # Subtotal with auto-ketchup is 180.0, bot fee is 10.0 -> total 190.0
        self.assertEqual(data["total"], 190.0)
        self.assertEqual(data["coupon_applied"], None)
        
        items = data["invoice"]["items"]
        self.assertTrue(any("Ketchup" in item["name"] for item in items))

    def test_16_upi_payment_utr_verification(self):
        """Verifies direct UPI checkout returns QR code, verify-payment endpoint checks UTR and enforces failed attempt rate limit."""
        customer = User(
            telegram_id="999916",
            username="buyer16",
            display_name="Buyer 16",
            wallet_balance=1000.0,
            role="user"
        )
        self.db.add(customer)
        
        self.db.add(GiftCard(
            code_encrypted=encrypt_data("GIFT-16"),
            code_hash=hashlib_sha256("GIFT-16"),
            pin_encrypted=encrypt_data("1234"),
            value=250.0,
            status="available"
        ))
        self.db.commit()

        pizza = self.db.query(Product).filter(Product.availability == True).first()
        token = create_access_token({"sub": str(customer.id), "role": "user"})
        headers = {"Authorization": f"Bearer {token}"}

        checkout_payload = {
            "items": [{"product_id": pizza.id, "quantity": 2}],
            "payment_method": "direct",
            "address": "Store Area",
            "latitude": 12.9716,
            "longitude": 77.5946,
            "phone": "+9199991616"
        }
        resp = client.post("/api/orders", json=checkout_payload, headers=headers)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        
        order_id = data["order_id"]
        self.assertEqual(data["status"], "Payment Pending")
        self.assertIn("qr_code_url", data)

        # Try invalid UTR format
        resp_verify = client.post(f"/api/orders/{order_id}/verify-payment", json={"utr": "12345"}, headers=headers)
        self.assertEqual(resp_verify.status_code, 400)

        # Try invalid UTR format 2
        resp_verify2 = client.post(f"/api/orders/{order_id}/verify-payment", json={"utr": "12345678901a"}, headers=headers)
        self.assertEqual(resp_verify2.status_code, 400)

        # Try invalid UTR format 3
        resp_verify3 = client.post(f"/api/orders/{order_id}/verify-payment", json={"utr": "999999999999"}, headers=headers)
        self.assertEqual(resp_verify3.status_code, 400)

        # 4th attempt should be blocked due to lockout
        resp_verify4 = client.post(f"/api/orders/{order_id}/verify-payment", json={"utr": "123456789012"}, headers=headers)
        self.assertEqual(resp_verify4.status_code, 403)
        self.assertIn("lockout", resp_verify4.json()["detail"].lower())

    def test_17_admin_sessions_management_and_privacy(self):
        """Verifies fetching and deleting user sessions by admin, and privacy filters on gift cards."""
        customer = User(
            telegram_id="999917",
            username="buyer17",
            display_name="Buyer 17",
            role="user"
        )
        self.db.add(customer)
        self.db.commit()

        gc = GiftCard(
            code_encrypted=encrypt_data("GIFT-17-SECRET"),
            code_hash=hashlib_sha256("GIFT-17-SECRET"),
            pin_encrypted=encrypt_data("4321"),
            value=250.0,
            status="available"
        )
        self.db.add(gc)
        self.db.commit()

        session_id = "test-session-17"
        session = UserSession(
            id=session_id,
            user_id=customer.id,
            refresh_token="ref-17",
            is_active=True
        )
        self.db.add(session)
        self.db.commit()

        user_token = create_access_token({"sub": str(customer.id), "role": "user"})
        user_headers = {"Authorization": f"Bearer {user_token}"}
        
        admin_token = create_access_token({"sub": "0", "role": "admin"})
        admin_headers = {"Authorization": f"Bearer {admin_token}", "X-Admin-Password": "pizza123"}

        # Admin fetches sessions for customer
        resp_sessions = client.get(f"/api/admin/users/{customer.id}/sessions", headers=admin_headers)
        self.assertEqual(resp_sessions.status_code, 200)
        self.assertEqual(len(resp_sessions.json()), 1)
        self.assertEqual(resp_sessions.json()[0]["id"], session_id)

        # Admin deletes session without password -> 401
        headers_no_pass = {"Authorization": f"Bearer {admin_token}"}
        resp_del_fail = client.delete(f"/api/admin/users/{customer.id}/sessions/{session_id}", headers=headers_no_pass)
        self.assertEqual(resp_del_fail.status_code, 401)

        # Admin deletes session with password -> 200
        resp_del = client.delete(f"/api/admin/users/{customer.id}/sessions/{session_id}", headers=admin_headers)
        self.assertEqual(resp_del.status_code, 200)
        self.assertEqual(resp_del.json()["status"], "success")

        # Verify session deactivated
        db_sess = self.db.query(UserSession).filter(UserSession.id == session_id).first()
        self.assertFalse(db_sess.is_active)

        # Test gift card privacy filter: Place order
        pizza = self.db.query(Product).filter(Product.availability == True).first()
        checkout_payload = {
            "items": [{"product_id": pizza.id, "quantity": 1}],
            "payment_method": "wallet",
            "address": "Store Area",
            "latitude": 12.9716,
            "longitude": 77.5946,
            "phone": "+9199991717"
        }
        customer.wallet_balance = 500.0
        self.db.commit()
        
        resp_order = client.post("/api/orders", json=checkout_payload, headers=user_headers)
        self.assertEqual(resp_order.status_code, 200)
        order_id = resp_order.json()["order_id"]
        
        # Manually link the gift card to the order for testing privacy visibility
        db_order = self.db.query(Order).filter(Order.id == order_id).first()
        db_order.gift_card_id = gc.id
        self.db.commit()

        # Fetch via user-facing endpoint -> gift card code & pin redacted
        resp_user_get = client.get(f"/api/orders/{order_id}", headers=user_headers)
        self.assertEqual(resp_user_get.status_code, 200)
        gift_card_user = resp_user_get.json()["gift_card"]
        self.assertEqual(gift_card_user["code"], "REDACTED")
        self.assertEqual(gift_card_user["pin"], "REDACTED")

        # Fetch via admin-facing endpoint -> gift card code & pin visible
        resp_admin_get = client.get(f"/api/admin/orders/{order_id}", headers=admin_headers)
        self.assertEqual(resp_admin_get.status_code, 200)
        gift_card_admin = resp_admin_get.json()["gift_card"]
        self.assertEqual(gift_card_admin["code"], "GIFT-17-SECRET")
        self.assertEqual(gift_card_admin["pin"], "4321")

    def test_18_bot_wallet_and_promo_codes(self):
        """Verifies promo code auto-redemption and UTR auto-submission in handle_bot_message."""
        from backend.bot import handle_bot_message, USER_BOT_SESSION
        
        # 1. Create a user
        customer = User(
            telegram_id="999918",
            username="buyer18",
            display_name="Buyer 18",
            wallet_balance=10.0,
            role="user"
        )
        self.db.add(customer)
        self.db.commit()
        
        # 2. Seed a Coupon internal promo code
        coupon_code = "PROMO150"
        cp = Coupon(
            code=coupon_code,
            value=150.0,
            is_active=True
        )
        self.db.add(cp)
        self.db.commit()
        
        # 3. Simulate message with internal promo code after setting session state
        USER_BOT_SESSION["999918"] = {"state": "waiting_for_promo_code"}
        import asyncio
        asyncio.run(handle_bot_message(self.db, "999918", "Test", "User", "buyer18", coupon_code))
        
        # Verify wallet balance increased and session state is cleared
        self.db.refresh(customer)
        self.assertEqual(customer.wallet_balance, 160.0) # 10.0 + 150.0
        self.assertIsNone(USER_BOT_SESSION["999918"].get("state"))
        
        # 4. Seed a pending TOPUP order
        topup_order = Order(
            id="TOPUP-181818",
            user_id=customer.id,
            original_total=100.0,
            discount=0.0,
            delivery_charge=0.0,
            total_payable=100.0,
            status="Pending Payment",
            payment_method="upi",
            transaction_id="TEMP-REF",
            city="Delhi"
        )
        self.db.add(topup_order)
        self.db.commit()
        
        # 5. Simulate sending a 12-digit UTR
        utr_code = "987654321012"
        asyncio.run(handle_bot_message(self.db, "999918", "Test", "User", "buyer18", utr_code))
        
        # Verify order status updated to Pending Verification and UTR is associated
        self.db.refresh(topup_order)
        self.assertEqual(topup_order.status, "Pending Verification")
        self.assertEqual(topup_order.transaction_id, utr_code)

    def test_19_admin_command_center_and_broadcast(self):
        """Verifies render_admin_command_center output, deposit history formatting, and broadcast handlers."""
        from app.backend.bot import render_admin_command_center, render_wallet_view
        
        # 1. Test Admin Command Center rendering
        admin_text, admin_markup = render_admin_command_center(self.db)
        self.assertIn("Platform Admin Command Center", admin_text)
        self.assertIn("📢 Broadcast Message", str(admin_markup))
        
        # 2. Test Wallet View rendering with Transaction History button
        user = User(telegram_id="99988811", username="tx_user", display_name="Tx User", wallet_balance=500.0)
        self.db.add(user)
        self.db.commit()
        
        wallet_text, wallet_markup = render_wallet_view(self.db, user)
        self.assertIn("My Wallet", wallet_text)
        self.assertIn("wallet_tx_history_page_1", str(wallet_markup))

    def test_20_user_suspension_and_support_faqs(self):
        """Verifies user blocked enforcement and support FAQ menu additions."""
        import asyncio
        from unittest.mock import patch, AsyncMock
        from app.backend.bot import USER_BOT_SESSION, process_incoming_message_task, process_bot_callback_task
        
        # 1. Create a blocked user
        blocked_user = User(
            telegram_id="88877711",
            username="blocked_guy",
            display_name="Blocked Guy",
            is_blocked=True
        )
        self.db.add(blocked_user)
        self.db.commit()
        
        # 2. Test callback query blocked check via wrapper
        with patch("app.backend.bot.send_bot_message", new_callable=AsyncMock) as mock_send, \
             patch("app.backend.bot.answer_callback_query", new_callable=AsyncMock) as mock_answer:
            asyncio.run(process_bot_callback_task(
                telegram_id=88877711,
                first_name="Blocked",
                last_name="Guy",
                username="blocked_guy",
                data="menu_view",
                message_id=100,
                callback_query_id="cb_1"
            ))
            mock_answer.assert_called_with("cb_1", "❌ Your account is suspended. Please contact support.", show_alert=True)
        
        # 3. Test message handler blocked check via wrapper
        with patch("app.backend.bot.send_bot_message", new_callable=AsyncMock) as mock_send:
            asyncio.run(process_incoming_message_task(
                telegram_id=88877711,
                first_name="Blocked",
                last_name="Guy",
                username="blocked_guy",
                text="Hello bot"
            ))
            mock_send.assert_called_once()
            call_args = mock_send.call_args[0]
            self.assertIn("Account Suspended", call_args[1])

    def test_21_direct_qr_order_and_cart_item_rendering(self):
        """Verifies direct UPI QR order workflow and robust cart item text rendering."""
        import asyncio
        from unittest.mock import patch, AsyncMock
        from app.backend.bot import (
            resolve_cart_item_product, parse_cart_quantity, render_cart_message,
            process_bot_callback_task
        )
        
        test_user = self.db.query(User).filter(User.telegram_id == "111222").first()
        if not test_user:
            test_user = User(telegram_id="111222", username="testuser", display_name="Test User", city="Mumbai")
            self.db.add(test_user)
            self.db.commit()
            
        p1 = self.db.query(Product).first()

        # 1. Verify helper functions
        prod = resolve_cart_item_product(self.db, "Margherita Classic")
        self.assertIsNotNone(prod)
        self.assertIn("Margherita", prod.name)
        
        self.assertEqual(parse_cart_quantity({"quantity": 3}), 3)
        self.assertEqual(parse_cart_quantity(2), 2)

        # 2. Verify render_cart_message with dict & name-based keys
        cart_data = {"Margherita Classic": {"quantity": 2}}
        msg, markup = render_cart_message(self.db, test_user, cart_data, {})
        self.assertIn("Margherita", msg)
        self.assertNotIn("items unavailable", msg)

        # 3. Test direct QR order placement & payment verification
        order = Order(
            id="BOT-TESTQR1",
            user_id=test_user.id,
            transaction_id="REF-TESTQR1",
            original_total=300.0,
            total_payable=330.0,
            payment_method="direct_upi",
            status="Pending Payment",
            address="123 Pizza Street, Mumbai",
            phone="9876543210",
            latitude=19.0760,
            longitude=72.8777
        )
        self.db.add(order)
        self.db.flush()

        item = OrderItem(order_id=order.id, product_id=p1.id, quantity=2, price=150.0)
        self.db.add(item)
        self.db.commit()

        with patch("app.backend.bot.send_bot_message", new_callable=AsyncMock) as mock_send, \
             patch("app.backend.bot.notify_admins", new_callable=AsyncMock) as mock_notify, \
             patch("app.backend.bot.answer_callback_query", new_callable=AsyncMock) as mock_answer:
            asyncio.run(process_bot_callback_task(
                telegram_id=test_user.telegram_id,
                first_name="Test",
                last_name="User",
                username="testuser",
                data=f"wallet_marked_paid_{order.id}",
                message_id=200,
                callback_query_id="cb_qr"
            ))
            
            # Verify customer notification contains Direct Order Payment header (NOT Deposit)
            mock_send.assert_called_once()
            cust_msg = mock_send.call_args[0][1]
            self.assertIn("Direct Order Payment Submitted", cust_msg)
            self.assertNotIn("Deposit Submitted for Admin Approval", cust_msg)

            # Verify admin notification contains Direct UPI Pizza Order details
            mock_notify.assert_called_once()
            admin_msg = mock_notify.call_args[0][1]
            self.assertIn("New Direct UPI Pizza Order", admin_msg)
            admin_markup = mock_notify.call_args[1]["reply_markup"]
            self.assertIn("admin_approve_direct_order_", admin_markup["inline_keyboard"][0][0]["callback_data"])

    def test_22_location_separation_and_address_preservation(self):
        """Verifies location separation: GPS coordinates, written doorstep address, and phone updating."""
        import asyncio
        from unittest.mock import patch, AsyncMock
        from app.backend.bot import (
            display_delivery_location_menu, handle_bot_message, SavedAddress
        )

        test_user = self.db.query(User).filter(User.telegram_id == "999000111").first()
        if not test_user:
            test_user = User(telegram_id="999000111", username="loc_user", display_name="Location User")
            self.db.add(test_user)
            self.db.commit()

        # 1. Test display_delivery_location_menu renders 3 distinct sections & location buttons
        with patch("app.backend.bot.send_bot_message", new_callable=AsyncMock) as mock_send:
            asyncio.run(display_delivery_location_menu(self.db, test_user))
            mock_send.assert_called_once()
            msg = mock_send.call_args[0][1]
            markup = mock_send.call_args[1]["reply_markup"]
            self.assertIn("GPS Location:", msg)
            self.assertIn("Doorstep Address:", msg)
            self.assertIn("Phone Number:", msg)
            self.assertNotIn("Delivery City:", msg)
            
            keyboard_texts = [btn["text"] for row in markup["keyboard"] for btn in row]
            self.assertIn("📍 Share My GPS Location", keyboard_texts)
            self.assertIn("🏠 Update Delivery Address", keyboard_texts)
            self.assertNotIn("🏙️ Change City / Area", keyboard_texts)

        # 2. Test manual doorstep address entry (preserves existing coords)
        test_user.latitude = 19.0760
        test_user.longitude = 72.8777
        self.db.commit()

        with patch("app.backend.bot.send_bot_message", new_callable=AsyncMock) as mock_send:
            asyncio.run(handle_bot_message(
                self.db, "999000111", "Loc", "User", "loc_user",
                "🏠 Update Delivery Address"
            ))
            # Send written doorstep address
            asyncio.run(handle_bot_message(
                self.db, "999000111", "Loc", "User", "loc_user",
                "Flat 402, Sunshine Apartments, MG Road, Andheri West"
            ))
            
            saved_addr = self.db.query(SavedAddress).filter(SavedAddress.user_id == test_user.id).first()
            self.assertIsNotNone(saved_addr)
            self.assertEqual(saved_addr.full_address, "Flat 402, Sunshine Apartments, MG Road, Andheri West")
            # Verify coordinates were preserved
            self.assertEqual(test_user.latitude, 19.0760)
            self.assertEqual(test_user.longitude, 72.8777)

        # 3. Test sharing GPS location preserves written doorstep address (does not overwrite with "GPS Location")
        with patch("app.backend.bot.send_bot_message", new_callable=AsyncMock) as mock_send, \
             patch("app.backend.bot.reverse_geocode", new_callable=AsyncMock, return_value="Mumbai"):
            asyncio.run(handle_bot_message(
                self.db, "999000111", "Loc", "User", "loc_user",
                text=None, location={"latitude": 19.1197, "longitude": 72.8464}
            ))
            
            self.assertEqual(test_user.latitude, 19.1197)
            self.assertEqual(test_user.longitude, 72.8464)
            saved_addr = self.db.query(SavedAddress).filter(SavedAddress.user_id == test_user.id).first()
            # Must NOT be overwritten with literal string "GPS Location"
            self.assertEqual(saved_addr.full_address, "Flat 402, Sunshine Apartments, MG Road, Andheri West")

def hashlib_sha256(text: str) -> str:


    import hashlib
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

if __name__ == "__main__":
    try:
        unittest.main()
    finally:
        # Clean up the test database file automatically after tests finish
        db_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "pizza_test.db"))
        if os.path.exists(db_file):
            try:
                # Force close all database connections first by disposing the engine
                from backend.database import engine
                engine.dispose()
            except Exception:
                pass
            for attempt in range(5):
                try:
                    os.remove(db_file)
                    print(f"\n[CLEANUP] Deleted temporary test database: {db_file}")
                    break
                except Exception:
                    import time
                    time.sleep(0.5)
