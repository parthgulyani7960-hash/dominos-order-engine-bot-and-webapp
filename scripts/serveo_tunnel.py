import subprocess
import re
import time
import sys
import os

# Adjust path to find database
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.backend.database import SessionLocal, SystemConfig

def update_db_url(url):
    db = SessionLocal()
    try:
        cfg = db.query(SystemConfig).filter(SystemConfig.key == 'mini_app_url').first()
        if not cfg:
            cfg = SystemConfig(key='mini_app_url', value=url)
            db.add(cfg)
        else:
            cfg.value = url
        db.commit()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Updated mini_app_url in database to: {url}", flush=True)
    except Exception as e:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Database update error: {e}", flush=True)
    finally:
        db.close()

def run_tunnel():
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting Serveo SSH tunnel...", flush=True)
    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-R", "80:127.0.0.1:8000",
        "serveo.net"
    ]
    
    # Run the SSH command
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    
    url_pattern = re.compile(r'https?://[a-zA-Z0-9.-]+\.serveo(?:usercontent)?\.com')
    
    # Read output line by line
    while True:
        line = proc.stdout.readline()
        if not line:
            break
            
        print(f"[Serveo Output] {line.strip()}", flush=True)
        
        # Look for the URL in the output
        match = url_pattern.search(line)
        if match:
            url = match.group(0)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Found Serveo URL: {url}", flush=True)
            update_db_url(url)
            
    # If we exited the loop, the process terminated
    rc = proc.wait()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SSH process terminated with exit code {rc}", flush=True)
    return rc

if __name__ == "__main__":
    while True:
        try:
            run_tunnel()
        except Exception as e:
            print(f"Tunnel runner exception: {e}", flush=True)
        print("Waiting 5 seconds before reconnecting...", flush=True)
        time.sleep(5)
