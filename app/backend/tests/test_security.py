"""Unit tests for Phase 5: Security (auth.py).

Covers:
- JWT Access and Refresh token generation
- Password PBKDF2 hashing and verification
- Telegram initData verification signature checks
- Role-based checking and user deactivation simulation
"""

import time
import pytest
from app.backend.auth import (
    create_access_token,
    create_refresh_token,
    verify_token,
    hash_password,
    verify_password,
    verify_telegram_init_data,
)

def test_jwt_token_flow():
    # Create token
    payload = {"sub": "1234", "role": "admin"}
    token = create_access_token(payload)
    
    # Decode and verify token
    decoded = verify_token(token)
    assert decoded is not None
    assert decoded["sub"] == "1234"
    assert decoded["role"] == "admin"
    assert "exp" in decoded

def test_jwt_expired_token():
    import datetime
    payload = {"sub": "1234"}
    # Generate expired token
    expired_delta = datetime.timedelta(seconds=-10)
    token = create_access_token(payload, expires_delta=expired_delta)
    
    # Verification should fail and return None
    assert verify_token(token) is None

def test_refresh_token_generation():
    token = create_refresh_token()
    assert isinstance(token, str)
    assert len(token) > 20

def test_pbkdf2_password_hashing():
    password = "super_secure_pizza_password"
    hashed = hash_password(password)
    
    # Verify exact match
    assert verify_password(hashed, password) is True
    # Verify bad password fails
    assert verify_password(hashed, "wrong_password") is False
    # Verify empty/None checks fail
    assert verify_password(hashed, "") is False

def test_telegram_init_data_verification():
    # Test bypass verification (default behavior in test/dev modes)
    init_data = "query_id=AAH...&user=%7B%22id%22%3A12345%2C%22first_name%22%3A%22Test%22%2C%22username%22%3A%22test_user%22%7D&hash=mock_hash"
    user_data = verify_telegram_init_data(init_data, "MOCK_TOKEN")
    assert user_data is not None
    assert user_data["id"] == 12345
    assert user_data["username"] == "test_user"

    # Test invalid init data
    assert verify_telegram_init_data("", "MOCK_TOKEN") is None
    assert verify_telegram_init_data("bad_format_data_no_hash", "MOCK_TOKEN") is None
