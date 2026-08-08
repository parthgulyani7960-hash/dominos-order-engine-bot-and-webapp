import os

filepath = r"C:\Users\parth\.gemini\antigravity-ide\scratch\telegram-pizza-app\app\backend\services\dominos_session_manager.py"
with open(filepath, "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines, 1):
    if line.strip().startswith("def ") or line.strip().startswith("async def "):
        print(f"{i:4d}: {line.strip()}")
