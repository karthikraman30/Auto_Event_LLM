import sqlite3
import os
import json
from datetime import datetime, timedelta

class DatabaseManager:
    def __init__(self, db_path="selectors.db"):
        # Correct pathing for different execution contexts
        if not os.path.exists(db_path) and os.path.exists("event_category/selectors.db"):
            self.db_path = "event_category/selectors.db"
        else:
            self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Existing Selector Table
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

        # Events Table with Upsert constraint
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_name TEXT NOT NULL,
                date_iso TEXT NOT NULL,
                event_url TEXT NOT NULL,
                end_date_iso TEXT,
                time TEXT,
                location TEXT,
                target_group TEXT, 
                status TEXT,
                booking_info TEXT,
                description TEXT,
                last_scraped TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(event_name, date_iso, event_url)
            )
        ''')
        
        # Settings Table (key-value store)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        
        # Scraping URLs Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                enabled INTEGER DEFAULT 1
            )
        ''')
        
        # Scraping Logs Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                type TEXT NOT NULL,
                status TEXT NOT NULL,
                events_found INTEGER DEFAULT 0,
                failures INTEGER DEFAULT 0,
                warnings TEXT
            )
        ''')
        
        # Seed default URLs if table is empty
        cursor.execute("SELECT COUNT(*) FROM scraping_urls")
        if cursor.fetchone()[0] == 0:
            default_urls = [
                ("https://biblioteket.stockholm.se/evenemang", "Biblioteket Stockholm", 1),
                ("https://biblioteket.stockholm.se/forskolor", "Forskolor", 1),
                ("https://www.skansen.se/en/calendar/", "Skansen", 1),
                ("https://www.modernamuseet.se/stockholm/sv/kalender/", "Moderna museet", 1),
                ("https://armemuseum.se/kalender/", "Armémuseum", 1),
                ("https://www.tekniskamuseet.se/pa-gang/", "Tekniska museet", 1),
            ]
            cursor.executemany("INSERT INTO scraping_urls (url, name, enabled) VALUES (?, ?, ?)", default_urls)
        
        # Seed default settings if empty
        cursor.execute("SELECT COUNT(*) FROM settings")
        if cursor.fetchone()[0] == 0:
            defaults = [
                ("schedule_frequency", "weekly"),
                ("schedule_day", "monday"),
                ("schedule_time", "06:00"),
                ("date_range_days", "45"),
                ("auto_delete_enabled", "false"),
                ("auto_delete_days", "90"),
                ("email_enabled", "false"),
                ("email_address", ""),
                ("notify_on_complete", "true"),
                ("notify_on_failure", "true"),
                ("notify_weekly_summary", "false"),
            ]
            cursor.executemany("INSERT INTO settings (key, value) VALUES (?, ?)", defaults)
        
        conn.commit()
        conn.close()

    # ==================== EVENTS ====================
    
    def upsert_event(self, event_data):
        """Insert new event or update existing info (Deduplication)."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO events (
                event_name, date_iso, event_url, end_date_iso, time, location, 
                target_group, status, booking_info, description, last_scraped
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(event_name, date_iso, event_url) DO UPDATE SET
                end_date_iso = excluded.end_date_iso,
                time = excluded.time,
                location = excluded.location,
                target_group = excluded.target_group,
                status = excluded.status,
                booking_info = excluded.booking_info,
                description = excluded.description,
                last_scraped = CURRENT_TIMESTAMP
        ''', (
            event_data.get('event_name'),
            event_data.get('date_iso'),
            event_data.get('event_url'),
            event_data.get('end_date_iso'),
            event_data.get('time'),
            event_data.get('location'),
            event_data.get('target_group_normalized'),
            event_data.get('status'),
            event_data.get('booking_info'),
            event_data.get('description')
        ))
        conn.commit()
        conn.close()

    def get_all_events(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM events ORDER BY date_iso ASC")
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        events = [dict(zip(column_names, row)) for row in rows]
        conn.close()
        return events

    def get_events_filtered(self, search="", venue="All Venues", date_range="All Time", 
                            target_groups=None, source="All Sources", page=1, per_page=20):
        """Get filtered and paginated events."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        query = "SELECT * FROM events WHERE 1=1"
        params = []
        
        if search:
            query += " AND event_name LIKE ?"
            params.append(f"%{search}%")
        
        if venue and venue != "All Venues":
            query += " AND location = ?"
            params.append(venue)
        
        # Source filter - match by domain from scraping_urls
        if source and source != "All Sources":
            # Get the URL for this source name
            cursor.execute("SELECT url FROM scraping_urls WHERE name = ?", (source,))
            row = cursor.fetchone()
            if row:
                from urllib.parse import urlparse
                domain = urlparse(row[0]).netloc.replace("www.", "")
                query += " AND event_url LIKE ?"
                params.append(f"%{domain}%")
        
        today = datetime.now().strftime("%Y-%m-%d")
        if date_range == "This Week":
            week_end = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
            query += " AND date_iso >= ? AND date_iso <= ?"
            params.extend([today, week_end])
        elif date_range == "Next 30 Days":
            month_end = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
            query += " AND date_iso >= ? AND date_iso <= ?"
            params.extend([today, month_end])
        
        if target_groups and len(target_groups) > 0 and "All" not in target_groups:
            placeholders = ",".join("?" * len(target_groups))
            # Map display names to DB values
            group_map = {"Children": "children", "Adults": "adults", "Families": "families"}
            db_groups = [group_map.get(g, g.lower()) for g in target_groups]
            query += f" AND target_group IN ({placeholders})"
            params.extend(db_groups)
        
        # Get total count
        count_query = query.replace("SELECT *", "SELECT COUNT(*)")
        cursor.execute(count_query, params)
        total = cursor.fetchone()[0]
        
        # Add pagination
        query += " ORDER BY date_iso ASC LIMIT ? OFFSET ?"
        params.extend([per_page, (page - 1) * per_page])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        events = [dict(zip(column_names, row)) for row in rows]
        conn.close()
        
        return events, total

    def delete_old_events(self, days):
        """Delete events older than specified days."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        cursor.execute("DELETE FROM events WHERE date_iso < ?", (cutoff,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted

    # ==================== DASHBOARD STATS ====================
    
    def get_stats(self):
        """Fetch summary statistics for the dashboard."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM events")
        total_events = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT location) FROM events")
        total_venues = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(last_scraped) FROM events")
        last_sync = cursor.fetchone()[0]
        conn.close()
        return total_events, total_venues, last_sync

    def get_events_this_week(self):
        """Count events happening this week."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        week_end = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        cursor.execute("SELECT COUNT(*) FROM events WHERE date_iso >= ? AND date_iso <= ?", 
                       (today, week_end))
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def get_events_next_month(self):
        """Count events in the next 30 days."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        month_end = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        cursor.execute("SELECT COUNT(*) FROM events WHERE date_iso >= ? AND date_iso <= ?", 
                       (today, month_end))
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def get_active_venues_count(self):
        """Get count of enabled scraping URLs."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM scraping_urls WHERE enabled = 1")
        enabled = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM scraping_urls")
        total = cursor.fetchone()[0]
        conn.close()
        return enabled, total

    def get_unique_venues(self):
        """Get list of unique venue names from events."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT location FROM events WHERE location IS NOT NULL ORDER BY location")
        venues = [row[0] for row in cursor.fetchall()]
        conn.close()
        return venues

    def get_unique_sources(self):
        """Get list of unique source websites with friendly names."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Get all scraping URLs with their names
        cursor.execute("SELECT url, name FROM scraping_urls ORDER BY name")
        url_name_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Get unique domains from event URLs and map to source names
        cursor.execute("SELECT DISTINCT event_url FROM events WHERE event_url IS NOT NULL")
        event_urls = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        # Match event URLs to their source name
        sources = set()
        for event_url in event_urls:
            for scrape_url, name in url_name_map.items():
                # Check if the event URL domain matches the scraping URL domain
                from urllib.parse import urlparse
                event_domain = urlparse(event_url).netloc.replace("www.", "")
                scrape_domain = urlparse(scrape_url).netloc.replace("www.", "")
                if event_domain == scrape_domain:
                    sources.add(name)
                    break
        
        return sorted(list(sources))

    # ==================== SETTINGS ====================
    
    def get_setting(self, key, default=None):
        """Get a single setting value."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else default

    def get_all_settings(self):
        """Get all settings as a dictionary."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT key, value FROM settings")
        settings = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return settings

    def save_setting(self, key, value):
        """Save a setting value."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()
        conn.close()

    def save_settings(self, settings_dict):
        """Save multiple settings at once."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        for key, value in settings_dict.items():
            cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()
        conn.close()

    # ==================== SCRAPING URLS ====================
    
    def get_scraping_urls(self):
        """Get all scraping URLs with their status."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT id, url, name, enabled FROM scraping_urls ORDER BY name")
        urls = [{"id": row[0], "url": row[1], "name": row[2], "enabled": bool(row[3])} 
                for row in cursor.fetchall()]
        conn.close()
        return urls

    def get_enabled_urls(self):
        """Get only enabled scraping URLs."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM scraping_urls WHERE enabled = 1")
        urls = [row[0] for row in cursor.fetchall()]
        conn.close()
        return urls

    def toggle_url(self, url_id, enabled):
        """Enable or disable a scraping URL."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("UPDATE scraping_urls SET enabled = ? WHERE id = ?", (1 if enabled else 0, url_id))
        conn.commit()
        conn.close()

    def add_scraping_url(self, url, name):
        """Add a new scraping URL."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO scraping_urls (url, name, enabled) VALUES (?, ?, 1)", (url, name))
            conn.commit()
            success = True
        except sqlite3.IntegrityError:
            success = False
        conn.close()
        return success

    def delete_scraping_url(self, url_id):
        """Delete a scraping URL."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM scraping_urls WHERE id = ?", (url_id,))
        conn.commit()
        conn.close()

    # ==================== SCRAPING LOGS ====================
    
    def add_log(self, run_type, status, events_found=0, failures=0, warnings=None):
        """Add a scraping log entry."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        warnings_json = json.dumps(warnings) if warnings else None
        cursor.execute('''
            INSERT INTO scraping_logs (type, status, events_found, failures, warnings)
            VALUES (?, ?, ?, ?, ?)
        ''', (run_type, status, events_found, failures, warnings_json))
        conn.commit()
        conn.close()

    def get_logs(self, days=30, status_filter="All"):
        """Get scraping logs optionally filtered by status."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        query = "SELECT * FROM scraping_logs WHERE timestamp >= ?"
        params = [cutoff]
        
        if status_filter and status_filter != "All":
            query += " AND status = ?"
            params.append(status_filter)
        
        query += " ORDER BY timestamp DESC"
        cursor.execute(query, params)
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        logs = [dict(zip(column_names, row)) for row in rows]
        conn.close()
        
        # Parse warnings JSON
        for log in logs:
            if log.get("warnings"):
                log["warnings"] = json.loads(log["warnings"])
        
        return logs

    def clear_old_logs(self, days=90):
        """Clear logs older than specified days."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("DELETE FROM scraping_logs WHERE timestamp < ?", (cutoff,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted

    # ==================== ANALYTICS ====================
    
    def get_events_by_venue(self):
        """Get event counts grouped by venue/location."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT location, COUNT(*) as count 
            FROM events 
            WHERE location IS NOT NULL 
            GROUP BY location 
            ORDER BY count DESC
        ''')
        result = [{"venue": row[0], "count": row[1]} for row in cursor.fetchall()]
        conn.close()
        return result

    def get_events_by_target_group(self):
        """Get event counts grouped by target group."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT target_group, COUNT(*) as count 
            FROM events 
            WHERE target_group IS NOT NULL 
            GROUP BY target_group
        ''')
        result = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return result

    def get_events_timeline(self, weeks=4):
        """Get event counts per week for timeline chart."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        timeline = []
        today = datetime.now()
        
        for week in range(weeks):
            week_start = today + timedelta(weeks=week)
            week_end = week_start + timedelta(days=6)
            
            cursor.execute('''
                SELECT COUNT(*) FROM events 
                WHERE date_iso >= ? AND date_iso <= ?
            ''', (week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")))
            
            count = cursor.fetchone()[0]
            timeline.append({"week": f"Week {week + 1}", "count": count})
        
        conn.close()
        return timeline

    # ==================== SELECTORS (for spider) ====================
    
    def get_selectors(self, url):
        """Get saved selectors for a URL pattern."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Extract domain and path from URL
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        url_path = parsed.path.rstrip('/')  # Normalize trailing slash
        
        # First try exact match on domain + path
        cursor.execute('''
            SELECT container_selector, item_selectors_json 
            FROM selector_configs 
            WHERE domain = ? AND (url_pattern = ? OR url_pattern = ?)
        ''', (domain, url_path, url_path + '/'))
        
        row = cursor.fetchone()
        
        # Fallback: match by domain only (for backwards compatibility)
        if not row:
            cursor.execute('''
                SELECT container_selector, item_selectors_json 
                FROM selector_configs 
                WHERE domain = ?
                LIMIT 1
            ''', (domain,))
            row = cursor.fetchone()
        
        conn.close()
        
        if row and row[1]:
            return {
                "container": row[0],
                "items": json.loads(row[1])
            }
        return None

    def save_selectors(self, url, container_selector, item_selectors):
        """Save selectors for a URL pattern."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Extract domain from URL
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        url_pattern = parsed.path
        
        cursor.execute('''
            INSERT INTO selector_configs (domain, url_pattern, container_selector, item_selectors_json, last_updated)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(domain, url_pattern) DO UPDATE SET
                container_selector = excluded.container_selector,
                item_selectors_json = excluded.item_selectors_json,
                last_updated = CURRENT_TIMESTAMP
        ''', (domain, url_pattern, container_selector, json.dumps(item_selectors)))
        
        conn.commit()
        conn.close()