
import sqlite3
import os

DB_PATH = "event_category/selectors.db"

def clear_selectors():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    domain = "biblioteket.stockholm.se"
    cursor.execute("DELETE FROM selector_configs WHERE domain = ?", (domain,))
    
    if cursor.rowcount > 0:
        print(f"Deleted {cursor.rowcount} rows for {domain}")
    else:
        print(f"No selectors found for {domain}")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    clear_selectors()
