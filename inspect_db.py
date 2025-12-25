
import sqlite3
import os

db_path = "event_category/selectors.db"

if not os.path.exists(db_path):
    print("Database not found at", db_path)
else:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT url_pattern, container_selector, item_selectors_json FROM selector_configs")
        rows = cursor.fetchall()
        print(f"Found {len(rows)} entries:")
        for row in rows:
            print(f"Pattern: {row[0]}")
            print(f"Container: {row[1]}")
            print(f"Items: {row[2]}")
            print("-" * 20)
        conn.close()
    except Exception as e:
        print(f"Error reading DB: {e}")
