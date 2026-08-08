import httpx
try:
    r = httpx.get("http://localhost:8000/api/admin/dominos/sessions")
    print(f"Status: {r.status_code}")
except Exception as e:
    print(f"Error: {e}")
