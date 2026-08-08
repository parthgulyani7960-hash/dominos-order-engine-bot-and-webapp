import os
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from ..database import Proxy, ProxyLog

def parse_proxy_string(proxy_url: str) -> dict:
    """Parse a proxy string of format:
      - socks5://username:password@host:port
      - socks5://host:port:username:password
      - host:port:username:password
      - host:port
    Returns a dict with parsed fields, normalized_url, and playwright_config.
    """
    if not proxy_url:
        raise ValueError("Empty proxy URL")
        
    proxy_str = proxy_url.strip()
    
    # Extract scheme
    scheme = "http"
    for s in ("socks5://", "socks4://", "http://", "https://"):
        if proxy_str.lower().startswith(s):
            scheme = s[:-3]
            proxy_str = proxy_str[len(s):]
            break
            
    # Check if it has @ (standard format)
    if "@" in proxy_str:
        auth_part, host_part = proxy_str.split("@", 1)
        if ":" in auth_part:
            username, password = auth_part.split(":", 1)
        else:
            username = auth_part
            password = None
            
        if ":" in host_part:
            host, port_str = host_part.split(":", 1)
            port = int(port_str)
        else:
            host = host_part
            port = 1080 if "socks" in scheme else 80
    else:
        # Check custom host:port:username:password (or any colon-separated combo)
        parts = proxy_str.split(":")
        
        # Try to determine which part is the port (first purely numeric part at index 0 or 1)
        port = None
        host = None
        username = None
        password = None
        
        if len(parts) == 1:
            # Just a host
            host = parts[0]
        elif len(parts) == 2:
            # host:port  OR  port:something (malformed)
            try:
                port = int(parts[1])
                host = parts[0]
            except ValueError:
                try:
                    port = int(parts[0])
                    host = "127.0.0.1"  # port-only, assume localhost
                    password = parts[1]
                except ValueError:
                    host = parts[0]
                    username = parts[1]
        elif len(parts) == 3:
            # host:port:username  OR  port:password:extra (malformed)
            try:
                port = int(parts[1])
                host = parts[0]
                username = parts[2]
            except ValueError:
                # parts[0] might itself be a port if numeric
                try:
                    port = int(parts[0])
                    host = "127.0.0.1"
                    username = parts[1]
                    password = parts[2]
                except ValueError:
                    host = parts[0]
                    username = parts[1]
                    password = parts[2]
        else:
            # 4+ parts: host:port:username:password[:...rest joined as password]
            try:
                port = int(parts[1])
                host = parts[0]
                username = parts[2]
                password = ":".join(parts[3:])  # rejoin remainder as password
            except (ValueError, IndexError):
                # Fallback: try treating first numeric token as port
                host = None
                for i, p in enumerate(parts):
                    try:
                        port = int(p)
                        host = ":".join(parts[:i]) if i > 0 else "127.0.0.1"
                        rem = parts[i+1:]
                        username = rem[0] if rem else None
                        password = ":".join(rem[1:]) if len(rem) > 1 else None
                        break
                    except ValueError:
                        continue
                if host is None:
                    host = parts[0]
                    
        if port is None:
            port = 1080 if "socks" in scheme else 8080
            
    if username and password:
        normalized_url = f"{scheme}://{username}:{password}@{host}:{port}"
    elif username:
        normalized_url = f"{scheme}://{username}@{host}:{port}"
    else:
        normalized_url = f"{scheme}://{host}:{port}"
        
    playwright_scheme = "http" if ("socks" in scheme and username) else scheme
    playwright_config = {
        "server": f"{playwright_scheme}://{host}:{port}"
    }
    if username:
        playwright_config["username"] = username
    if password:
        playwright_config["password"] = password
        
    return {
        "scheme": scheme,
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "normalized_url": normalized_url,
        "playwright_config": playwright_config
    }

class ProxyManager:
    """Simple static proxy manager.
    Returns the proxy URL defined in the environment variable STATIC_PROXY.
    Records usage in the ProxyLog table for audit.
    """

    def __init__(self, db: Session):
        self.db = db
        self.proxy_url = os.getenv("STATIC_PROXY")
        if not self.proxy_url:
            raise ValueError("STATIC_PROXY environment variable not set")

    def get_proxy(self) -> dict:
        """Return Playwright-compatible proxy dict and log usage.
        """
        parsed = parse_proxy_string(self.proxy_url)
        
        # Log usage
        proxy_entry = self.db.query(Proxy).filter(Proxy.ip == parsed["host"], Proxy.port == parsed["port"]).first()
        if not proxy_entry:
            proxy_entry = Proxy(
                ip=parsed["host"],
                port=parsed["port"],
                username=parsed["username"],
                password=parsed["password"],
                protocol=parsed["scheme"],
                is_active=True,
                created_at=datetime.now(timezone.utc).replace(tzinfo=None)
            )
            self.db.add(proxy_entry)
            self.db.flush()
        log = ProxyLog(
            proxy_id=proxy_entry.id,
            action="use",
            status="success",
            details="Static proxy used for order sync",
            created_at=datetime.now(timezone.utc).replace(tzinfo=None)
        )
        self.db.add(log)
        self.db.commit()
        return parsed["playwright_config"]
