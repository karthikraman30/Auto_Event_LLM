import sqlite3
import os
import json
from datetime import datetime, timedelta

class DatabaseManager:
    def __init__(self, db_path="selectors.db"):
        # If an absolute path is provided (e.g., for testing), use it directly
        if os.path.isabs(db_path):
            self.db_path = db_path
            self._init_db()
            return
        
        # Compute absolute path to project root's selectors.db
        # db_manager.py is at: event_category/event_category/utils/db_manager.py
        # Project root is 3 levels up
        this_file_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.normpath(os.path.join(this_file_dir, "..", "..", ".."))
        project_root_db = os.path.join(project_root, "selectors.db")
        
        # Try multiple locations for production compatibility (Streamlit Cloud etc.)
        possible_paths = [
            project_root_db,  # Always prefer project root first
            db_path,
            "event_category/selectors.db",
            os.path.join(this_file_dir, "..", "selectors.db"),  # Relative to this file
            os.path.join(this_file_dir, "..", "..", "selectors.db"),
            "/mount/src/auto_event_llm/selectors.db",  # Streamlit Cloud absolute
            "/mount/src/auto_event_llm/event_category/selectors.db",
        ]
        
        self.db_path = project_root_db  # Default to project root
        for path in possible_paths:
            if path and os.path.exists(path):
                self.db_path = os.path.abspath(path)  # Always use absolute path
                break
        
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

        # Events Table with Upsert constraint (simplified schema - no soft-delete columns)
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
                target_group_normalized TEXT,
                status TEXT,
                booking_info TEXT,
                description TEXT,
                image_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_scraped TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                unique_key TEXT,
                age_limit TEXT,
                UNIQUE(event_name, date_iso, event_url, location)
            )
        ''')
        
        # Migration: Add new columns if they don't exist
        cursor.execute("PRAGMA table_info(events)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'image_url' not in columns:
            cursor.execute('ALTER TABLE events ADD COLUMN image_url TEXT')
        
        if 'created_at' not in columns:
            # SQLite doesn't allow DEFAULT CURRENT_TIMESTAMP in ALTER TABLE
            # Add column without default, then backfill with last_scraped
            cursor.execute('ALTER TABLE events ADD COLUMN created_at TIMESTAMP')
            cursor.execute('UPDATE events SET created_at = last_scraped WHERE created_at IS NULL')
        
        if 'unique_key' not in columns:
            cursor.execute('ALTER TABLE events ADD COLUMN unique_key TEXT')
        
        if 'age_limit' not in columns:
            cursor.execute('ALTER TABLE events ADD COLUMN age_limit TEXT')
        
        if 'target_group_normalized' not in columns:
            cursor.execute('ALTER TABLE events ADD COLUMN target_group_normalized TEXT')
        
        # Migration: Drop old soft-delete columns if they exist (one-time cleanup)
        # SQLite doesn't support DROP COLUMN directly, so we skip this for existing DBs
        # New databases won't have these columns at all
        
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
        
        # Scrape Runs Table - tracks metadata for each scheduled scrape run
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scrape_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                run_type TEXT NOT NULL,
                window_days INTEGER NOT NULL,
                events_scraped INTEGER DEFAULT 0,
                events_marked_deleted INTEGER DEFAULT 0
            )
        ''')
        
        # Pending Changes Table (staging for incremental scraper)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                change_type TEXT NOT NULL,
                event_data TEXT NOT NULL,
                unique_key TEXT NOT NULL,
                event_name TEXT,
                date_iso TEXT,
                location TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                run_id TEXT
            )
        ''')
        
        # Scrape Failures Table - tracks URL-specific scraping failures
        # This table persists failures across scrape runs for user notification
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scrape_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                url_name TEXT,
                error_message TEXT NOT NULL,
                error_type TEXT,
                occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                run_type TEXT,
                resolved_at TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
        ''')
        
        # Scheduler Settings Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduler_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Seed default scheduler settings if empty
        cursor.execute("SELECT COUNT(*) FROM scheduler_settings")
        if cursor.fetchone()[0] == 0:
            scheduler_defaults = [
                ("baseline_start_date", "", datetime.now().isoformat()),
                ("baseline_interval_months", "1", datetime.now().isoformat()),
                ("incremental_interval_days", "3", datetime.now().isoformat()),
                ("last_baseline_run", "", datetime.now().isoformat()),
                ("last_incremental_run", "", datetime.now().isoformat()),
            ]
            cursor.executemany("INSERT INTO scheduler_settings (key, value, updated_at) VALUES (?, ?, ?)", scheduler_defaults)
        
        # Create indexes for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_dates ON events(date_iso, end_date_iso)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_last_scraped ON events(last_scraped)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_unique_key ON events(unique_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pending_unique_key ON pending_changes(unique_key)')
        
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
        """Insert new event or update existing info (Deduplication).
        
        For Stockholm library (biblioteket.stockholm.se) events, deduplication is disabled
        to allow multiple events with the same name/date at different library branches.
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Check if this is a Stockholm library event (skip deduplication for this site)
        event_url = event_data.get('event_url', '')
        is_stockholm_library = 'biblioteket.stockholm.se' in event_url
        
        try:
            if is_stockholm_library:
                # For Stockholm library: Use deterministic hash as suffix to ensure uniqueness
                # but allow updates if the SAME event is scraped again.
                import hashlib
                
                # Create a unique signature for this event instance
                # We use name + date + time + location to distinguish events at different branches/times
                unique_str = f"{event_data.get('event_name')}_{event_data.get('date_iso')}_{event_data.get('time')}_{event_data.get('location')}"
                unique_hash = hashlib.md5(unique_str.encode('utf-8')).hexdigest()[:8]
                
                # Append hash to URL to make it unique per time/location, but consistent across scrapes
                unique_url = f"{event_url}#{unique_hash}"
                
                # Generate unique_key for this event
                unique_key = self.generate_unique_key(event_data)
                
                # Use UPSERT logic just like regular events
                cursor.execute('''
                    INSERT INTO events (
                            event_name, date_iso, event_url, end_date_iso, time, location, 
                            target_group, target_group_normalized, status, booking_info, description, image_url, 
                            unique_key, age_limit, created_at, last_scraped
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT(event_name, date_iso, event_url, location) DO UPDATE SET
                            end_date_iso = excluded.end_date_iso,
                            time = excluded.time,
                            location = excluded.location,
                            target_group = excluded.target_group,
                            target_group_normalized = excluded.target_group_normalized,
                            status = excluded.status,
                            booking_info = excluded.booking_info,
                            description = excluded.description,
                            image_url = excluded.image_url,
                            unique_key = excluded.unique_key,
                            age_limit = excluded.age_limit,
                            last_scraped = CURRENT_TIMESTAMP
                ''', (
                    event_data.get('event_name'),
                    event_data.get('date_iso'),
                    unique_url,  # Use the deterministic hashed URL
                    event_data.get('end_date_iso'),
                    event_data.get('time'),
                    event_data.get('location'),
                    event_data.get('target_group'),
                    event_data.get('target_group_normalized'),
                    event_data.get('status'),
                    event_data.get('booking_info'),
                    event_data.get('description'),
                    event_data.get('image_url'),
                    unique_key,
                    event_data.get('age_limit', 'N/A'),
                ))
            else:
                # Generate unique_key for this event
                unique_key = self.generate_unique_key(event_data)
                
                # For all other sites: Use normal upsert with deduplication
                cursor.execute('''
                    INSERT INTO events (
                            event_name, date_iso, event_url, end_date_iso, time, location, 
                            target_group, target_group_normalized, status, booking_info, description, image_url, 
                            unique_key, age_limit, created_at, last_scraped
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT(event_name, date_iso, event_url, location) DO UPDATE SET
                            end_date_iso = excluded.end_date_iso,
                            time = excluded.time,
                            location = excluded.location,
                            target_group = excluded.target_group,
                            target_group_normalized = excluded.target_group_normalized,
                            status = excluded.status,
                            booking_info = excluded.booking_info,
                            description = excluded.description,
                            image_url = excluded.image_url,
                            unique_key = excluded.unique_key,
                            age_limit = excluded.age_limit,
                            last_scraped = CURRENT_TIMESTAMP
                ''', (
                    event_data.get('event_name'),
                    event_data.get('date_iso'),
                    event_data.get('event_url'),
                    event_data.get('end_date_iso'),
                    event_data.get('time'),
                    event_data.get('location'),
                    event_data.get('target_group'),
                    event_data.get('target_group_normalized'),
                    event_data.get('status'),
                    event_data.get('booking_info'),
                    event_data.get('description'),
                    event_data.get('image_url'),
                    unique_key,
                    event_data.get('age_limit', 'N/A'),
                ))
        except Exception as e:
            conn.close()
            raise
        
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
                            target_groups=None, source="All Sources", page=1, per_page=20, filter_date=None):
        """Get filtered and paginated events. If filter_date is provided, show only events on that date."""
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
        
        # If filter_date is provided, show only events on that specific date
        if filter_date:
            # Match events where:
            # 1. date_iso equals filter_date (single-day events on that date), OR
            # 2. filter_date falls within the event's date range (multi-day events)
            query += """ AND (
                date_iso = ? 
                OR (date_iso <= ? AND end_date_iso >= ? AND end_date_iso != 'N/A')
            )"""
            params.extend([filter_date, filter_date, filter_date])
        else:
            # For multi-day events, include events where:
            # 1. date_iso is today or later, OR
            # 2. end_date_iso is today or later (event spans into today/future)
            query += """ AND (
                date_iso >= ?
                OR (end_date_iso >= ? AND end_date_iso != 'N/A')
            )"""
            params.extend([today, today])
            
            if date_range == "This Week":
                week_end = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
                query += " AND date_iso <= ?"
                params.append(week_end)
            elif date_range == "Next 30 Days":
                month_end = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
                query += " AND date_iso <= ?"
                params.append(month_end)
        
        if target_groups and len(target_groups) > 0 and "All" not in target_groups:
            placeholders = ",".join("?" * len(target_groups))
            # Map display names to DB values
            group_map = {"Children": "children", "Adults": "adults", "Families": "families"}
            db_groups = [group_map.get(g, g.lower()) for g in target_groups]
            query += f" AND target_group IN ({placeholders})"
            params.extend(db_groups)
        
        # Fetch ALL matching events (no pagination at DB level)
        query += " ORDER BY date_iso ASC"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        events = [dict(zip(column_names, row)) for row in rows]
        
        conn.close()
        
        # Expand multi-day events
        expanded_events = []
        for event in events:
            # If filtering by specific date, only expand to that date
            expanded_events.extend(self._expand_event_across_days(event, filter_date=filter_date))
        
        # Filter expanded events to only show today onwards (for multi-day events that started in past)
        if not filter_date:
            expanded_events = [e for e in expanded_events if e['date_iso'] >= today]
        
        # Sort by date
        expanded_events.sort(key=lambda x: x['date_iso'])
        
        # Get total count of EXPANDED events
        total = len(expanded_events)
        
        # Apply pagination on expanded events
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_events = expanded_events[start_idx:end_idx]
        
        return paginated_events, total
    
    def _expand_event_across_days(self, event, filter_date=None):
        """Expand an event that spans multiple days into separate entries for each day.
        If filter_date is provided, only return the event instance for that specific date.
        Expansion is capped at 30 days from today to prevent long-running events from flooding the list."""
        events = []
        start_date_str = event.get('date_iso')
        end_date_str = event.get('end_date_iso')
        
        if not start_date_str:
            return [event]
        
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            today = datetime.now().date()
            
            # If no end date or end date is 'N/A', just return single event
            if not end_date_str or end_date_str == 'N/A':
                return [event]
            
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
            
            # If start and end are the same, return single event
            if start_date == end_date:
                return [event]
            
            # Cap the expansion at 30 days from today
            max_expansion_date = today + timedelta(days=30)
            effective_end_date = min(end_date, max_expansion_date)
            
            # Start from today if the event started in the past
            effective_start_date = max(start_date, today)
            
            # Generate event for each day in the capped range
            current_date = effective_start_date
            while current_date <= effective_end_date:
                # If filter_date is specified, only return event for that date
                if filter_date:
                    filter_date_obj = datetime.strptime(filter_date, "%Y-%m-%d").date()
                    if current_date == filter_date_obj:
                        event_copy = event.copy()
                        event_copy['date_iso'] = current_date.strftime("%Y-%m-%d")
                        event_copy['end_date_iso'] = 'N/A'
                        events.append(event_copy)
                        break
                else:
                    # Return all days in the capped range
                    event_copy = event.copy()
                    event_copy['date_iso'] = current_date.strftime("%Y-%m-%d")
                    event_copy['end_date_iso'] = 'N/A'
                    events.append(event_copy)
                
                current_date += timedelta(days=1)
            
            return events
            
        except Exception as e:
            print(f"Error expanding event: {e}")
            return [event]

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

    def delete_event(self, event_name, date_iso, event_url):
        """Delete a specific event by its unique identifiers."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM events WHERE event_name = ? AND date_iso = ? AND event_url = ?",
            (event_name, date_iso, event_url)
        )
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted > 0

    def delete_all_events(self):
        """Delete ALL events from the database."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM events")
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
        """Count events in the next calendar month (not including current month)."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Get first day of next month
        today = datetime.now()
        if today.month == 12:
            next_month_start = datetime(today.year + 1, 1, 1)
            next_month_end = datetime(today.year + 1, 1, 31)
        else:
            next_month_start = datetime(today.year, today.month + 1, 1)
            # Get last day of next month
            if today.month + 1 == 12:
                next_month_end = datetime(today.year, 12, 31)
            else:
                next_month_end = datetime(today.year, today.month + 2, 1) - timedelta(days=1)
        
        cursor.execute("SELECT COUNT(*) FROM events WHERE date_iso >= ? AND date_iso <= ?", 
                       (next_month_start.strftime("%Y-%m-%d"), next_month_end.strftime("%Y-%m-%d")))
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
                # Check if the event URL matches the scraping URL
                from urllib.parse import urlparse
                event_parsed = urlparse(event_url)
                scrape_parsed = urlparse(scrape_url)
                
                # Normalize domains (remove www.)
                event_domain = event_parsed.netloc.replace("www.", "")
                scrape_domain = scrape_parsed.netloc.replace("www.", "")
                
                # Primary match: same domain
                if event_domain == scrape_domain:
                    # For sites with very specific paths (like calendar pages), 
                    # we still want to match events from other parts of the same site
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

    # ==================== SCRAPE FAILURES ====================
    
    def log_scrape_failure(self, url, url_name, error_message, error_type='UNKNOWN', run_type='manual'):
        """Log a scraping failure for a specific URL.
        
        If there's already an active failure for this URL, update it instead of creating a new one.
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Check if there's already an active failure for this URL
        cursor.execute(
            "SELECT id FROM scrape_failures WHERE url = ? AND is_active = 1",
            (url,)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Update existing failure with latest error
            cursor.execute('''
                UPDATE scrape_failures 
                SET error_message = ?, error_type = ?, occurred_at = CURRENT_TIMESTAMP, run_type = ?
                WHERE id = ?
            ''', (error_message, error_type, run_type, existing[0]))
        else:
            # Insert new failure
            cursor.execute('''
                INSERT INTO scrape_failures (url, url_name, error_message, error_type, run_type)
                VALUES (?, ?, ?, ?, ?)
            ''', (url, url_name, error_message, error_type, run_type))
        
        conn.commit()
        conn.close()
    
    def get_active_failures(self):
        """Get all active (unresolved) scraping failures."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, url, url_name, error_message, error_type, occurred_at, run_type
            FROM scrape_failures 
            WHERE is_active = 1
            ORDER BY occurred_at DESC
        ''')
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        failures = [dict(zip(column_names, row)) for row in rows]
        conn.close()
        return failures
    
    def resolve_failure(self, url):
        """Mark a failure as resolved when the URL scrapes successfully."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE scrape_failures 
            SET is_active = 0, resolved_at = CURRENT_TIMESTAMP
            WHERE url = ? AND is_active = 1
        ''', (url,))
        resolved = cursor.rowcount
        conn.commit()
        conn.close()
        return resolved > 0
    
    def get_failure_history(self, days=7):
        """Get failure history including resolved failures from the last N days."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            SELECT id, url, url_name, error_message, error_type, occurred_at, run_type, resolved_at, is_active
            FROM scrape_failures 
            WHERE occurred_at >= ?
            ORDER BY occurred_at DESC
        ''', (cutoff,))
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        failures = [dict(zip(column_names, row)) for row in rows]
        conn.close()
        return failures
    
    def cleanup_old_failures(self, days=30):
        """Remove old resolved failures to keep database clean."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            DELETE FROM scrape_failures 
            WHERE is_active = 0 AND resolved_at < ?
        ''', (cutoff,))
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

    def get_all_selector_configs(self):
        """Get all selector configurations from the database."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT domain, url_pattern, container_selector, item_selectors_json, last_updated
            FROM selector_configs
            ORDER BY last_updated DESC
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        result = []
        for row in rows:
            result.append({
                'domain': row['domain'],
                'url_pattern': row['url_pattern'],
                'container_selector': row['container_selector'],
                'item_selectors_json': row['item_selectors_json'],
                'last_updated': row['last_updated']
            })
        
        return result

    def delete_selector_config(self, url):
        """Delete selector configuration for a URL."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Extract domain and path from URL
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        url_pattern = parsed.path
        
        cursor.execute('''
            DELETE FROM selector_configs
            WHERE domain = ? AND url_pattern = ?
        ''', (domain, url_pattern))
        
        conn.commit()
        conn.close()

    # ==================== UPDATES TAB (Sunday vs Wednesday Comparison) ====================
    
    def get_added_events_today(self):
        """Get events where created_at matches today's date (newly added events)."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        cursor.execute('''
            SELECT * FROM events 
            WHERE DATE(created_at) = ?
            ORDER BY event_url, date_iso
        ''', (today,))
        
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        events = [dict(zip(column_names, row)) for row in rows]
        conn.close()
        return events
    
    def get_deleted_events(self, days_ahead=10):
        """
        Get events in the next N days that were present in the previous Sunday scrape
        but NOT updated in the latest (Wednesday) run.
        
        Logic:
        - Find the last Sunday scrape timestamp from logs
        - Find the last Wednesday scrape timestamp from logs  
        - Events with last_scraped matching Sunday but NOT Wednesday are considered "deleted"
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        today = datetime.now()
        future_date = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        today_str = today.strftime("%Y-%m-%d")
        
        # Get the last two scrape timestamps from logs
        cursor.execute('''
            SELECT timestamp FROM scraping_logs 
            WHERE status IN ('OK', 'Warn')
            ORDER BY timestamp DESC 
            LIMIT 2
        ''')
        recent_logs = cursor.fetchall()
        
        if len(recent_logs) < 2:
            conn.close()
            return []  # Not enough scrape history to compare
        
        latest_scrape = recent_logs[0][0]  # Most recent (e.g., Wednesday)
        previous_scrape = recent_logs[1][0]  # Previous (e.g., Sunday)
        
        # Events in next N days that have last_scraped from previous run but NOT latest run
        # This means they existed before but weren't found in the latest scrape
        cursor.execute('''
            SELECT * FROM events 
            WHERE date_iso >= ? AND date_iso <= ?
            AND DATE(last_scraped) = DATE(?)
            AND DATE(last_scraped) < DATE(?)
            ORDER BY event_url, date_iso
        ''', (today_str, future_date, previous_scrape, latest_scrape))
        
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        events = [dict(zip(column_names, row)) for row in rows]
        conn.close()
        return events
    
    def get_last_scrape_info(self):
        """Get information about the last two scrape runs for comparison."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT timestamp, type, status, events_found 
            FROM scraping_logs 
            WHERE status IN ('OK', 'Warn')
            ORDER BY timestamp DESC 
            LIMIT 2
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return None, None
        
        latest = {
            'timestamp': rows[0][0],
            'type': rows[0][1],
            'status': rows[0][2],
            'events_found': rows[0][3]
        } if len(rows) > 0 else None
        
        previous = {
            'timestamp': rows[1][0],
            'type': rows[1][1],
            'status': rows[1][2],
            'events_found': rows[1][3]
        } if len(rows) > 1 else None
        
        return latest, previous
    
    # ==================== SCHEDULED SCRAPING & COMPARATOR ====================
    
    @staticmethod
    def generate_unique_key(event_data):
        """Generate a stable unique key for an event.
        
        Uses event_url as base, or creates canonical key from name+date+location.
        For events with URL fragments (#), strips them to ensure consistency.
        """
        import hashlib
        
        event_url = event_data.get('event_url', '')
        event_name = event_data.get('event_name', '')
        date_iso = event_data.get('date_iso', '')
        location = event_data.get('location', '')
        
        # Strip URL fragments for consistency
        if '#' in event_url and 'biblioteket.stockholm.se' not in event_url:
            event_url = event_url.split('#')[0]
        
        # Prefer URL as stable identifier if it exists and is meaningful
        if event_url and event_url != 'N/A' and not event_url.startswith('http://example.com'):
            # For Stockholm library, include time/location in hash since URL is generic
            if 'biblioteket.stockholm.se' in event_url:
                unique_str = f"{event_name}|{date_iso}|{event_data.get('time', '')}|{location}"
                return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()[:32]
            else:
                # Use URL + date as unique key for non-Stockholm sites
                unique_str = f"{event_url}|{date_iso}"
                return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()[:32]
        
        # Fallback: canonical key from name+date+location
        unique_str = f"{event_name}|{date_iso}|{location}"
        return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()[:32]
    
    def log_scrape_run(self, run_type, window_days):
        """Log metadata for a scrape run and return the run ID."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO scrape_runs (run_type, window_days)
            VALUES (?, ?)
        ''', (run_type, window_days))
        
        run_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return run_id
    
    def update_scrape_run_stats(self, run_id, events_scraped, events_marked_deleted):
        """Update scrape run statistics after comparison."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE scrape_runs 
            SET events_scraped = ?, events_marked_deleted = ?
            WHERE id = ?
        ''', (events_scraped, events_marked_deleted, run_id))
        
        conn.commit()
        conn.close()
    
    # ==================== SCHEDULER SETTINGS (Two-Scraper System) ====================
    
    def get_scheduler_setting(self, key, default=None):
        """Get a scheduler setting value."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM scheduler_settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row and row[0] else default
    
    def get_all_scheduler_settings(self):
        """Get all scheduler settings as a dictionary."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT key, value, updated_at FROM scheduler_settings")
        settings = {row[0]: {"value": row[1], "updated_at": row[2]} for row in cursor.fetchall()}
        conn.close()
        return settings
    
    def set_scheduler_setting(self, key, value):
        """Set a scheduler setting value. Returns True if the value changed."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Get current value to detect change
        cursor.execute("SELECT value FROM scheduler_settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        old_value = row[0] if row else None
        
        # Update the setting
        cursor.execute('''
            INSERT INTO scheduler_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        ''', (key, str(value), datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return old_value != str(value)
    
    def set_baseline_start_date(self, date_str):
        """Set the baseline start date. Returns True if date changed (triggers immediate run)."""
        return self.set_scheduler_setting("baseline_start_date", date_str)
    
    def get_baseline_start_date(self):
        """Get the configured baseline start date."""
        return self.get_scheduler_setting("baseline_start_date", "")
    
    def update_last_baseline_run(self):
        """Update the last baseline run timestamp."""
        self.set_scheduler_setting("last_baseline_run", datetime.now().isoformat())
    
    def update_last_incremental_run(self):
        """Update the last incremental run timestamp."""
        self.set_scheduler_setting("last_incremental_run", datetime.now().isoformat())
    
    # ==================== PENDING CHANGES (Incremental Scraper Staging) ====================
    
    def stage_insert(self, event_data, run_id=None):
        """Stage an event for insertion (found in scrape but not in DB)."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        unique_key = self.generate_unique_key(event_data)
        
        # Check if already staged
        cursor.execute("SELECT id FROM pending_changes WHERE unique_key = ? AND change_type = 'insert'", (unique_key,))
        if cursor.fetchone():
            conn.close()
            return False  # Already staged
        
        cursor.execute('''
            INSERT INTO pending_changes (change_type, event_data, unique_key, event_name, date_iso, location, run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            'insert',
            json.dumps(event_data),
            unique_key,
            event_data.get('event_name', ''),
            event_data.get('date_iso', ''),
            event_data.get('location', ''),
            run_id
        ))
        
        conn.commit()
        conn.close()
        return True
    
    def stage_delete(self, event_data, run_id=None):
        """Stage an event for deletion (found in DB but not in scrape)."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        unique_key = event_data.get('unique_key') or self.generate_unique_key(event_data)
        
        # Check if already staged
        cursor.execute("SELECT id FROM pending_changes WHERE unique_key = ? AND change_type = 'delete'", (unique_key,))
        if cursor.fetchone():
            conn.close()
            return False  # Already staged
        
        cursor.execute('''
            INSERT INTO pending_changes (change_type, event_data, unique_key, event_name, date_iso, location, run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            'delete',
            json.dumps(event_data),
            unique_key,
            event_data.get('event_name', ''),
            event_data.get('date_iso', ''),
            event_data.get('location', ''),
            run_id
        ))
        
        conn.commit()
        conn.close()
        return True
    
    def get_pending_changes(self):
        """Get all pending changes grouped by type."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, change_type, event_data, unique_key, event_name, date_iso, location, detected_at, run_id
            FROM pending_changes
            ORDER BY change_type, detected_at DESC
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        changes = {"insert": [], "delete": []}
        for row in rows:
            change = {
                "id": row[0],
                "change_type": row[1],
                "event_data": json.loads(row[2]) if row[2] else {},
                "unique_key": row[3],
                "event_name": row[4],
                "date_iso": row[5],
                "location": row[6],
                "detected_at": row[7],
                "run_id": row[8]
            }
            changes[row[1]].append(change)
        
        return changes
    
    def get_pending_counts(self):
        """Get counts of pending inserts and deletes."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        cursor.execute("SELECT change_type, COUNT(*) FROM pending_changes GROUP BY change_type")
        counts = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        
        return {
            "insert": counts.get("insert", 0),
            "delete": counts.get("delete", 0)
        }
    
    def apply_pending_changes(self):
        """Apply all pending changes to the main events table.
        
        - Inserts: Add new events to events table
        - Deletes: Hard-delete events from events table
        
        Returns dict with counts of applied changes.
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Get all pending changes
        cursor.execute("SELECT id, change_type, event_data, unique_key FROM pending_changes")
        changes = cursor.fetchall()
        
        inserted = 0
        deleted = 0
        
        for change_id, change_type, event_data_json, unique_key in changes:
            try:
                if change_type == 'insert':
                    # Insert event to main table
                    event_data = json.loads(event_data_json)
                    self._insert_event_internal(cursor, event_data)
                    inserted += 1
                    
                elif change_type == 'delete':
                    # Hard delete from events table
                    cursor.execute("DELETE FROM events WHERE unique_key = ?", (unique_key,))
                    if cursor.rowcount > 0:
                        deleted += 1
                
                # Remove from pending_changes
                cursor.execute("DELETE FROM pending_changes WHERE id = ?", (change_id,))
                
            except Exception as e:
                print(f"[ERROR] Failed to apply change {change_id}: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        return {"inserted": inserted, "deleted": deleted}
    
    def _insert_event_internal(self, cursor, event_data):
        """Internal method to insert event using existing cursor."""
        unique_key = self.generate_unique_key(event_data)
        
        cursor.execute('''
            INSERT INTO events (
                event_name, date_iso, event_url, end_date_iso, time, location, 
                target_group, target_group_normalized, status, booking_info, description, image_url, 
                unique_key, age_limit, created_at, last_scraped
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(event_name, date_iso, event_url, location) DO UPDATE SET
                end_date_iso = excluded.end_date_iso,
                time = excluded.time,
                target_group = excluded.target_group,
                target_group_normalized = excluded.target_group_normalized,
                status = excluded.status,
                booking_info = excluded.booking_info,
                description = excluded.description,
                image_url = excluded.image_url,
                unique_key = excluded.unique_key,
                age_limit = excluded.age_limit,
                last_scraped = CURRENT_TIMESTAMP
        ''', (
            event_data.get('event_name'),
            event_data.get('date_iso'),
            event_data.get('event_url'),
            event_data.get('end_date_iso'),
            event_data.get('time'),
            event_data.get('location'),
            event_data.get('target_group'),
            event_data.get('target_group_normalized'),
            event_data.get('status'),
            event_data.get('booking_info'),
            event_data.get('description'),
            event_data.get('image_url'),
            unique_key,
            event_data.get('age_limit'),
        ))
    
    def discard_pending_changes(self):
        """Discard all pending changes without applying them."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM pending_changes")
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted
    
    def discard_single_change(self, change_id):
        """Discard a single pending change."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM pending_changes WHERE id = ?", (change_id,))
        conn.commit()
        conn.close()
    
    # ==================== INCREMENTAL COMPARISON ====================
    
    def compare_and_stage_changes(self, scraped_events, start_date, end_date, run_id=None):
        """Compare scraped events against DB and stage changes.
        
        Args:
            scraped_events: List of event dicts from scraper
            start_date: Start of date range (YYYY-MM-DD)
            end_date: End of date range (YYYY-MM-DD)
            run_id: Optional run identifier
        
        Returns:
            Dict with counts: {"staged_inserts": N, "staged_deletes": N}
        """
        from urllib.parse import urlparse
        
        # Extract unique domains from scraped events
        scraped_domains = set()
        for event in scraped_events:
            url = event.get('event_url', '')
            if url:
                try:
                    domain = urlparse(url).netloc
                    scraped_domains.add(domain)
                except:
                    pass
        
        if not scraped_domains:
            # No valid domains found, cannot compare
            return {"staged_inserts": 0, "staged_deletes": 0}
        
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Get existing events in the date range from DB, filtered by scraped domains
        # Build WHERE clause with domain filters
        domain_conditions = " OR ".join(["event_url LIKE ?" for _ in scraped_domains])
        query = f'''
            SELECT id, event_name, date_iso, event_url, location, unique_key, time
            FROM events
            WHERE date_iso >= ? AND date_iso <= ?
            AND ({domain_conditions})
        '''
        
        # Build parameters: start_date, end_date, then domain patterns
        params = [start_date, end_date] + [f'%{domain}%' for domain in scraped_domains]
        
        cursor.execute(query, params)
        
        db_events = {row[5]: {
            "id": row[0],
            "event_name": row[1],
            "date_iso": row[2],
            "event_url": row[3],
            "location": row[4],
            "unique_key": row[5],
            "time": row[6]  # Include time for proper key comparison
        } for row in cursor.fetchall() if row[5]}  # Only include events with unique_key
        
        conn.close()
        
        # Build set of scraped unique keys - ONLY for events within the date range
        # This prevents events outside the comparison window from being flagged
        scraped_keys = set()
        scraped_events_in_range = []
        for event in scraped_events:
            event_date = event.get('date_iso', '')
            # Skip events outside the date range
            if event_date and (event_date < start_date or event_date > end_date):
                continue
            unique_key = self.generate_unique_key(event)
            event['unique_key'] = unique_key
            scraped_keys.add(unique_key)
            scraped_events_in_range.append(event)
        
        staged_inserts = 0
        staged_deletes = 0
        
        # Find events to INSERT (in scraped but not in DB) - only within date range
        for event in scraped_events_in_range:
            if event['unique_key'] not in db_events:
                if self.stage_insert(event, run_id):
                    staged_inserts += 1
        
        # Find events to DELETE (in DB but not in scraped)
        for unique_key, db_event in db_events.items():
            if unique_key not in scraped_keys:
                if self.stage_delete(db_event, run_id):
                    staged_deletes += 1
        
        return {"staged_inserts": staged_inserts, "staged_deletes": staged_deletes}
    
    def get_events_in_date_range(self, start_date, end_date):
        """Get all events within a date range."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM events
            WHERE date_iso >= ? AND date_iso <= ?
            ORDER BY date_iso ASC
        ''', (start_date, end_date))
        
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        events = [dict(zip(column_names, row)) for row in rows]
        conn.close()
        return events