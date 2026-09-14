import re

with open('app/backend/bot.py', 'r', encoding='utf-8') as f:
    content = f.read()

setters = set(re.findall(r'session\[["\']state["\']\]\s*=\s*["\']([^"\']+)["\']', content))
getters = set(re.findall(r'session\.get\(["\']state["\']\)\s*==?\s*["\']([^"\']+)["\']', content))
in_getters = set(re.findall(r'session\.get\(["\']state["\']\)\s*in\s*\(([^)]+)\)', content))

print("=== STATES SET IN BOT.PY ===")
for s in sorted(setters):
    print(f"SET: {s}")

print("\n=== STATES HANDLED (==) IN BOT.PY ===")
for g in sorted(getters):
    print(f"GET ==: {g}")

print("\n=== STATES HANDLED (in tuple) IN BOT.PY ===")
for ig in sorted(in_getters):
    print(f"GET IN: {ig}")
