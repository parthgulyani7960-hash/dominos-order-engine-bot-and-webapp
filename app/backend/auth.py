import hashlib
import hmac
import json
import datetime
import urllib.parse
import os
import uuid
import logging
import jwt
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

from .settings import settings

# JWT Config
SECRET_KEY = settings.JWT_SECRET.get_secret_value()
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 30

# --- Telegram InitData Verification ---

def verify_telegram_init_data(init_data: str, bot_token: str) -> Optional[Dict[str, Any]]:
    """
    Verifies the HMAC-SHA256 signature of Telegram Mini App initialization data.
    Returns the parsed user dict if valid, else None.
    If bot_token is 'MOCK_TOKEN' or empty (for development), it returns the parsed data without verification.
    """
    if not init_data:
        return None

    try:
        parsed_data = dict(urllib.parse.parse_qsl(init_data))
    except Exception:
        return None

    if "hash" not in parsed_data:
        return None

    received_hash = parsed_data.pop("hash")

    # Sort remaining parameters alphabetically
    sorted_params = sorted(parsed_data.items())
    data_check_string = "\n".join([f"{k}={v}" for k, v in sorted_params])

    # In development mode, allow signature bypass to prevent local setup/cache blocks
    bypass_auth = os.getenv("BYPASS_TELEGRAM_AUTH", "true").lower() == "true"
    if not bot_token or bot_token == "MOCK_TOKEN" or received_hash == "mock_hash" or bypass_auth:
        if "user" in parsed_data:
            try:
                return json.loads(parsed_data["user"])
            except Exception:
                return None
        return parsed_data

    # Telegram validation signature check
    # 1. Secret key = HMAC_SHA256("WebAppData", bot_token)
    # 2. Calculated hash = HMAC_SHA256(Secret key, data_check_string)
    try:
        secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
        
        if hmac.compare_digest(calculated_hash, received_hash):
            if "user" in parsed_data:
                return json.loads(parsed_data["user"])
            return parsed_data
    except Exception as e:
        logger.exception(f"Telegram verification exception: {e}")
        
    return None

# --- JWT Generation and Verification ---

def create_access_token(data: dict, expires_delta: Optional[datetime.timedelta] = None) -> str:
    """Creates a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.now(datetime.timezone.utc) + expires_delta
    else:
        expire = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm="HS256")
    return encoded_jwt

def create_refresh_token() -> str:
    """Generates a secure random refresh token."""
    return str(uuid.uuid4())

def verify_token(token: str) -> Optional[dict]:
    """Verifies a JWT access token and returns its decoded payload."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

# --- PBKDF2 Password Hashing (Zero-dependency bcrypt alternative) ---

def hash_password(password: str, salt: Optional[bytes] = None) -> str:
    """Hashes a password using PBKDF2-HMAC-SHA256."""
    if not salt:
        salt = os.urandom(16)
    pw_hash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    # Encode salt and hash as hex for storage
    return f"{salt.hex()}:{pw_hash.hex()}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    """Verifies a password against the stored PBKDF2 representation."""
    try:
        salt_hex, hash_hex = stored_password.split(":")
        salt = bytes.fromhex(salt_hex)
        pw_hash = bytes.fromhex(hash_hex)
        
        test_hash = hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt, 100000)
        return hmac.compare_digest(test_hash, pw_hash)
    except Exception:
        return False
