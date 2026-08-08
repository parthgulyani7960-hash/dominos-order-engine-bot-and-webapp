# 🍕 Antigravity Pizza Ordering Platform (v2.0)

A production-grade, real-time Pizza Ordering Platform featuring a Telegram Mini App (customer interface), a connected Telegram Bot (chat orders, notifications, and support), a FastAPI Backend API with SQLite database, and an Admin Dashboard.

## 🏗️ Architecture

```
       Telegram Bot (python-telegram-bot)
               ↕ HTTPS
       FastAPI Backend (REST + WebSocket / SSE)
               ↕
       SQLite Database (SQLAlchemy ORM)
         ↕                  ↕
  Admin Dashboard     Customer Mini App
(Vanilla HTML/CSS/JS) (Telegram WebApp UI)
```

## 🌟 Key Features

### 1. Customer Mini App (Web App)
- **Modern UI**: Dark glassmorphic design system using Google Fonts (Inter & Outfit).
- **Interactive Ordering**: Product category filters, search, custom crust/size selector, and animated cart stepper.
- **Location-Based Pricing**: Geolocation distance pricing multiplier and serviceable zone checks.
- **Live Tracker**: Real-time status update progress timeline with simulated delivery rider location using Leaflet maps.
- **Wallet & Mock Payment**: Built-in wallet balance system and interactive QR-based Mock UPI QR code payment workflow.

### 2. Telegram Bot
- **Automatic Registration**: New users are created instantly upon first message with custom onboarding.
- **Chat Ordering**: Full keyboard or text `/order <product_id> <quantity> <address>` flow.
- **Direct Support Chat**: Auto-routes messages directly to the Admin Dashboard's real-time support chat.
- **Discount & Promotions**: Active promo details, including Domino's cart pricing cap (subtotal ₹180-₹220 capped at ₹100).

### 3. Admin Dashboard
- **Analytics Metrics**: Real-time order distribution charts, user trend charts, revenue indicators, and online user counts.
- **Rider Management**: Live map assignment with mock rider simulation.
- **Gift Cards Management**: Cryptographically secure encryption of Domino's Gift Cards (PBKDF2/AES) for backing orders.
- **Audit Logs & Security**: Complete history of administrative actions, rate limiting, and brute-force protection with login lockouts.

---

## 🛠️ Setup & Installation

### 1. Prerequisites
- Python 3.10+
- A Telegram Bot Token (obtained from `@BotFather`)

### 2. Clone and Install Dependencies
```bash
cd telegram-pizza-app
pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the project root:
```env
TELEGRAM_BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
ADMIN_TELEGRAM_ID=YOUR_TELEGRAM_ID
MINI_APP_URL=http://localhost:8000
JWT_SECRET=super_secret_jwt_key_change_me_in_production
ADMIN_USERNAME=admin
ADMIN_PASSWORD=pizza123
```

### 4. Running the Backend Server
Start the Uvicorn web server:
```bash
py -m uvicorn app.backend.main:app --host 0.0.0.0 --port 8000 --reload
```
Once started:
- **Customer Mini App**: Available at `http://localhost:8000/`
- **Admin Dashboard**: Available at `http://localhost:8000/admin/`

---

## 🧪 Testing

The platform includes a comprehensive automated integration and unit test suite:

```bash
py verify_app.py
```
This suite tests:
- Cryptographic security utilities.
- Database seeding.
- Telegram session mock logins.
- Gift card encryption and auto-allocation.
- Order processing & balance checks.
- Domino's Capping / Promotion rules.
- Admin dashboard metrics and user creation.
- Security lockouts for brute force login attempts.
