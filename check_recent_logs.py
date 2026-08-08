import sqlite3
import os
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

db_path = "data/pizza.db"
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("--- LATEST ERROR LOGS ---")
    try:
        cursor.execute("SELECT id, type, message, created_at FROM error_logs ORDER BY id DESC LIMIT 15")
        for row in cursor.fetchall():
            print(f"ID: {row[0]} | Type: {row[1]} | Msg: {row[2]} | At: {row[3]}")
    except Exception as e:
        print("Error fetching error_logs:", e)
        
    print("\n--- LATEST ROBOT LOGS ---")
    try:
        cursor.execute("SELECT id, mobile_number, level, stage, message, created_at FROM robot_logs ORDER BY id DESC LIMIT 15")
        for row in cursor.fetchall():
            print(f"ID: {row[0]} | Mobile: {row[1]} | Level: {row[2]} | Stage: {row[3]} | Msg: {row[4]} | At: {row[5]}")
    except Exception as e:
        print("Error fetching robot_logs:", e)
        
    print("\n--- LATEST AUDIT LOGS ---")
    try:
        cursor.execute("SELECT id, admin_username, action, details, created_at FROM audit_logs ORDER BY id DESC LIMIT 15")
        for row in cursor.fetchall():
            print(f"ID: {row[0]} | Admin: {row[1]} | Action: {row[2]} | Details: {row[3]} | At: {row[4]}")
    except Exception as e:
        print("Error fetching audit_logs:", e)
        
    conn.close()
else:
    print("Database path not found")
