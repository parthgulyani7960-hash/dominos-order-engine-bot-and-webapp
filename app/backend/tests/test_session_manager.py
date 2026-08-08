"""Unit and integration tests for Phase 3: SessionManager (dominos_session_manager.py).

Covers:
- Session persistence in database
- In-memory session tracking
- Sanitize and normalization of cookies
- Session cleanup and recovery simulation
"""

import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.backend.database import Base, User, DominosSession
from app.backend.services.dominos_session_manager import (
    sanitize_cookies,
    add_raw_session,
    ACTIVE_OTP_REQUESTS,
)

# Use in-memory SQLite for testing
TEST_DATABASE_URL = "sqlite:///:memory:"

@pytest.fixture(name="db_session")
def fixture_db_session():
    engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    # Create tables
    Base.metadata.create_all(bind=engine)
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)

@pytest.fixture(name="test_user")
def fixture_test_user(db_session):
    user = User(
        telegram_id="12345678",
        username="test_admin",
        role="admin"
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user

def test_sanitize_cookies():
    # Test valid cookie sanitization
    raw_cookies = [
        {
            "name": "customerId",
            "value": "12345",
            "domain": "dominos.co.in",
            "path": "/",
            "httpOnly": True,
            "secure": True,
            "sameSite": "Lax"
        },
        {
            "Name": "token",
            "Value": "token_val",
            "Domain": ".dominos.co.in:443"
        }
    ]
    
    sanitized = sanitize_cookies(raw_cookies)
    assert len(sanitized) == 2
    assert sanitized[0]["name"] == "customerId"
    assert sanitized[0]["value"] == "12345"
    assert sanitized[0]["domain"] == "dominos.co.in"
    assert sanitized[0]["httpOnly"] is True
    assert sanitized[0]["secure"] is True
    assert sanitized[0]["sameSite"] == "Lax"

    assert sanitized[1]["name"] == "token"
    assert sanitized[1]["value"] == "token_val"
    assert sanitized[1]["domain"] == ".dominos.co.in"

def test_add_raw_session_json(db_session, test_user):
    # Standard JSON cookie list
    cookies_json = """
    [
        {"name": "customerId", "value": "999", "domain": ".dominos.co.in"},
        {"name": "token", "value": "session_tok", "domain": ".dominos.co.in"}
    ]
    """
    
    session = add_raw_session(db_session, test_user, "9876543210", cookies_json)
    assert session.is_active is True
    assert session.mobile_number == "9876543210"
    assert len(session.cookies) == 2
    assert session.admin_id == test_user.id

def test_add_raw_session_header_string(db_session, test_user):
    # Cookie header format string
    cookie_str = "customerId=111; token=abc_token; ACCESS_TOKEN=jwt_secret"
    
    session = add_raw_session(db_session, test_user, "9876543210", cookie_str)
    assert session.is_active is True
    assert len(session.cookies) == 3
    names = {c["name"] for c in session.cookies}
    assert "customerId" in names
    assert "token" in names
    assert "ACCESS_TOKEN" in names

def test_add_raw_session_invalid_format(db_session, test_user):
    # Missing required authentication cookie
    invalid_cookies = '[{"name": "non_auth_cookie", "value": "some_value"}]'
    with pytest.raises(ValueError, match="Authentication cookies not found"):
        add_raw_session(db_session, test_user, "9876543210", invalid_cookies)

def test_add_raw_session_deletes_duplicates(db_session, test_user):
    # Standard JSON cookie list
    cookies_json = """
    [
        {"name": "customerId", "value": "999", "domain": ".dominos.co.in"},
        {"name": "token", "value": "session_tok", "domain": ".dominos.co.in"}
    ]
    """
    
    # Add first session
    session1 = add_raw_session(db_session, test_user, "9876543210", cookies_json)
    
    # Check it exists
    all_sessions_1 = db_session.query(DominosSession).filter(DominosSession.mobile_number == "9876543210").all()
    assert len(all_sessions_1) == 1
    
    # Add second session for same number
    session2 = add_raw_session(db_session, test_user, "9876543210", cookies_json)
    
    # Check first session was DELETED (not just deactivated) and only second one exists
    all_sessions_2 = db_session.query(DominosSession).filter(DominosSession.mobile_number == "9876543210").all()
    assert len(all_sessions_2) == 1
    assert all_sessions_2[0].id == session2.id
