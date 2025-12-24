import sqlite3
import json
import re
import os
from urllib.parse import urlparse

class DatabaseManager:
    def __init__(self, db_path="selectors.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        # Add timeout=30 (seconds) to wait for locks to clear
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS selector_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                url_pattern TEXT NOT NULL,
                container_selector TEXT,
                item_selectors_json TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(domain, url_pattern)
            )
        ''')
        conn.commit()
        conn.close()

    def get_selectors(self, url):
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        path = parsed_url.path or "/"

        # Add timeout for parallel access
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT url_pattern, container_selector, item_selectors_json 
            FROM selector_configs 
            WHERE domain = ?
        ''', (domain,))
        
        rows = cursor.fetchall()
        conn.close()

        # Sort by pattern length descending to get more specific matches first
        rows.sort(key=lambda x: len(x[0]), reverse=True)

        for pattern, container, item_selectors_json in rows:
            # Simple regex matching for now
            regex_pattern = pattern.replace('*', '.*')
            if re.match(f"^{regex_pattern}$", path):
                return {
                    "container": container,
                    "items": json.loads(item_selectors_json)
                }
        
        return None

    def save_selectors(self, url, container, item_selectors):
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        path = parsed_url.path or "/"
        
        # For saving, we use the specific path as the pattern unless provided otherwise
        pattern = path

        # Add timeout for parallel access
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO selector_configs (domain, url_pattern, container_selector, item_selectors_json, last_updated)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(domain, url_pattern) DO UPDATE SET
                container_selector = excluded.container_selector,
                item_selectors_json = excluded.item_selectors_json,
                last_updated = CURRENT_TIMESTAMP
        ''', (domain, pattern, container, json.dumps(item_selectors)))
        conn.commit()
        conn.close()
