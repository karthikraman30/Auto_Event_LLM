
import sqlite3
import os

DB_PATH = 'selectors.db'

def check_selectors():
    if not os.path.exists(DB_PATH):
        print("DB not found")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT url, container_selector, item_selectors FROM selectors WHERE url LIKE '%skansen.se%'")
    rows = cursor.fetchall()
    
    print(f"Found {len(rows)} entries for Skansen:")
    for row in rows:
        print(f"URL: {row[0]}")
        print(f"Container: {row[1]}")
        print(f"Items: {row[2]}")
        print("-" * 20)
        
    conn.close()

if __name__ == "__main__":
    check_selectors()
