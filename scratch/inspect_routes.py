import os

filepath = r"C:\Users\parth\.gemini\antigravity-ide\scratch\telegram-pizza-app\app\backend\routes.py"
with open(filepath, "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines, 1):
    if "@router." in line or "def " in line or "async def " in line:
        print(f"{i:4d}: {line.strip()}")
