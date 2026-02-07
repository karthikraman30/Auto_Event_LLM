import os
import json
import hashlib
from datetime import datetime, timedelta
from urllib.parse import urlparse

# Try to import supabase, fall back to None if not installed
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None

# Try to import streamlit for secrets
try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False


class DatabaseManager:
    """Database manager using Supabase for persistent cloud storage."""
    
    def __init__(self, db_path=None):
        """Initialize Supabase connection.
        
        Args:
            db_path: Ignored for Supabase (kept for backwards compatibility)
        """
        self.supabase: Client = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Supabase."""
        if not SUPABASE_AVAILABLE:
            raise ImportError("supabase package not installed. Run: pip install supabase")
        
        # Try to get credentials from multiple sources
        url = None
        key = None
        
        # 1. Try Streamlit secrets first
        if STREAMLIT_AVAILABLE:
            try:
                url = st.secrets.get("SUPABASE_URL")
                key = st.secrets.get("SUPABASE_KEY")
            except Exception:
                pass
        
        # 2. Fall back to environment variables
        if not url:
            url = os.environ.get("SUPABASE_URL")
        if not key:
            key = os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            raise ValueError(
                "Supabase credentials not found. Set SUPABASE_URL and SUPABASE_KEY in:\n"
                "  - Streamlit secrets (.streamlit/secrets.toml), or\n"
                "  - Environment variables"
            )
        
        self.supabase = create_client(url, key)
    
    # ==================== EVENTS ====================
    
    def upsert_event(self, event_data):
        """Insert new event or update existing info (Deduplication).
        
        For Stockholm library events, deduplication uses a hash suffix.
        """
        event_url = event_data.get('event_url', '')
        is_stockholm_library = 'biblioteket.stockholm.se' in event_url
        
        if is_stockholm_library:
            # Create deterministic hash for Stockholm library events
            unique_str = f"{event_data.get('event_name')}_{event_data.get('date_iso')}_{event_data.get('time')}_{event_data.get('location')}"
            unique_hash = hashlib.md5(unique_str.encode('utf-8')).hexdigest()[:8]
            event_url = f"{event_url}#{unique_hash}"
        
        unique_key = self.generate_unique_key(event_data)
        
        record = {
            'event_name': event_data.get('event_name'),
            'date_iso': event_data.get('date_iso'),
            'event_url': event_url,
            'end_date_iso': event_data.get('end_date_iso'),
            'time': event_data.get('time'),
            'location': event_data.get('location'),
            'target_group': event_data.get('target_group'),
            'target_group_normalized': event_data.get('target_group_normalized'),
            'status': event_data.get('status'),
            'booking_info': event_data.get('booking_info'),
            'description': event_data.get('description'),
            'image_url': event_data.get('image_url'),
            'unique_key': unique_key,
            'age_limit': event_data.get('age_limit', 'N/A'),
            'last_scraped': datetime.now().isoformat(),
        }
        
        # Try upsert based on unique constraint
        self.supabase.table('events').upsert(
            record,
            on_conflict='event_name,date_iso,event_url,location'
        ).execute()

    def get_all_events(self):
        """Get all events ordered by date."""
        response = self.supabase.table('events').select('*').order('date_iso').execute()
        return response.data or []

    def get_events_filtered(self, search="", venue="All Venues", date_range="All Time", 
                            target_groups=None, source="All Sources", page=1, per_page=20, filter_date=None):
        """Get filtered and paginated events."""
        query = self.supabase.table('events').select('*')
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Apply search filter
        if search:
            query = query.ilike('event_name', f'%{search}%')
        
        # Apply venue filter
        if venue and venue != "All Venues":
            query = query.eq('location', venue)
        
        # Apply source filter
        if source and source != "All Sources":
            # Get URL for this source
            urls_response = self.supabase.table('scraping_urls').select('url').eq('name', source).execute()
            if urls_response.data:
                domain = urlparse(urls_response.data[0]['url']).netloc.replace("www.", "")
                query = query.ilike('event_url', f'%{domain}%')
        
        # Apply date filters
        if filter_date:
            # For specific date filtering, get all and filter in Python due to complex logic
            pass
        else:
            # Show today onwards
            query = query.gte('date_iso', today)
            
            if date_range == "This Week":
                week_end = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
                query = query.lte('date_iso', week_end)
            elif date_range == "Next 30 Days":
                month_end = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
                query = query.lte('date_iso', month_end)
        
        # Apply target group filter
        if target_groups and len(target_groups) > 0 and "All" not in target_groups:
            group_map = {"Children": "children", "Adults": "adults", "Families": "families"}
            db_groups = [group_map.get(g, g.lower()) for g in target_groups]
            query = query.in_('target_group', db_groups)
        
        # Order by date
        query = query.order('date_iso')
        
        # Execute query
        response = query.execute()
        events = response.data or []
        
        # Expand multi-day events
        expanded_events = []
        for event in events:
            expanded_events.extend(self._expand_event_across_days(event, filter_date=filter_date))
        
        # Filter to today onwards for multi-day events
        if not filter_date:
            expanded_events = [e for e in expanded_events if e['date_iso'] >= today]
        
        # Sort by date
        expanded_events.sort(key=lambda x: x['date_iso'])
        
        # Get total count
        total = len(expanded_events)
        
        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_events = expanded_events[start_idx:end_idx]
        
        return paginated_events, total
    
    def _expand_event_across_days(self, event, filter_date=None):
        """Expand multi-day events into separate entries for each day."""
        events = []
        start_date_str = event.get('date_iso')
        end_date_str = event.get('end_date_iso')
        
        if not start_date_str:
            return [event]
        
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            today = datetime.now().date()
            
            if not end_date_str or end_date_str == 'N/A':
                return [event]
            
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
            
            if start_date == end_date:
                return [event]
            
            # Cap expansion at 30 days from today
            max_expansion_date = today + timedelta(days=30)
            effective_end_date = min(end_date, max_expansion_date)
            effective_start_date = max(start_date, today)
            
            current_date = effective_start_date
            while current_date <= effective_end_date:
                if filter_date:
                    filter_date_obj = datetime.strptime(filter_date, "%Y-%m-%d").date()
                    if current_date == filter_date_obj:
                        event_copy = event.copy()
                        event_copy['date_iso'] = current_date.strftime("%Y-%m-%d")
                        event_copy['end_date_iso'] = 'N/A'
                        events.append(event_copy)
                        break
                else:
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
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        response = self.supabase.table('events').delete().lt('date_iso', cutoff).execute()
        return len(response.data) if response.data else 0

    def delete_event(self, event_name, date_iso, event_url):
        """Delete a specific event by its unique identifiers."""
        response = self.supabase.table('events').delete().eq(
            'event_name', event_name
        ).eq('date_iso', date_iso).eq('event_url', event_url).execute()
        return len(response.data) > 0 if response.data else False

    def delete_all_events(self):
        """Delete ALL events from the database."""
        # Supabase requires a filter for delete, so we use a condition that matches all
        response = self.supabase.table('events').delete().gte('id', 0).execute()
        return len(response.data) if response.data else 0

    # ==================== DASHBOARD STATS ====================
    
    def get_stats(self):
        """Fetch summary statistics for the dashboard."""
        # Get total events
        events_response = self.supabase.table('events').select('id', count='exact').execute()
        total_events = events_response.count or 0
        
        # Get unique venues
        venues_response = self.supabase.table('events').select('location').execute()
        unique_venues = set(r['location'] for r in (venues_response.data or []) if r.get('location'))
        total_venues = len(unique_venues)
        
        # Get last sync time
        last_sync_response = self.supabase.table('events').select('last_scraped').order('last_scraped', desc=True).limit(1).execute()
        last_sync = last_sync_response.data[0]['last_scraped'] if last_sync_response.data else None
        
        return total_events, total_venues, last_sync

    def get_events_this_week(self):
        """Count events happening this week."""
        today = datetime.now().strftime("%Y-%m-%d")
        week_end = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        
        response = self.supabase.table('events').select('id', count='exact').gte(
            'date_iso', today
        ).lte('date_iso', week_end).execute()
        
        return response.count or 0

    def get_events_next_month(self):
        """Count events in the next calendar month."""
        today = datetime.now()
        if today.month == 12:
            next_month_start = datetime(today.year + 1, 1, 1)
            next_month_end = datetime(today.year + 1, 1, 31)
        else:
            next_month_start = datetime(today.year, today.month + 1, 1)
            if today.month + 1 == 12:
                next_month_end = datetime(today.year, 12, 31)
            else:
                next_month_end = datetime(today.year, today.month + 2, 1) - timedelta(days=1)
        
        response = self.supabase.table('events').select('id', count='exact').gte(
            'date_iso', next_month_start.strftime("%Y-%m-%d")
        ).lte('date_iso', next_month_end.strftime("%Y-%m-%d")).execute()
        
        return response.count or 0

    def get_active_venues_count(self):
        """Get count of enabled scraping URLs."""
        enabled_response = self.supabase.table('scraping_urls').select('id', count='exact').eq('enabled', 1).execute()
        total_response = self.supabase.table('scraping_urls').select('id', count='exact').execute()
        
        return enabled_response.count or 0, total_response.count or 0

    def get_unique_venues(self):
        """Get list of unique venue names from events."""
        response = self.supabase.table('events').select('location').not_.is_('location', 'null').execute()
        venues = sorted(set(r['location'] for r in (response.data or []) if r.get('location')))
        return venues

    def get_unique_sources(self):
        """Get list of unique source websites with friendly names.
        
        Uses per-domain existence checks to avoid Supabase's 1000-row default limit.
        """
        # Get all scraping URLs with names
        urls_response = self.supabase.table('scraping_urls').select('url, name').order('name').execute()
        
        sources = []
        for row in (urls_response.data or []):
            scrape_url = row['url']
            name = row['name']
            domain = urlparse(scrape_url).netloc.replace("www.", "")
            
            # Check if any events exist for this domain (avoids 1000-row limit)
            check = self.supabase.table('events')\
                .select('id', count='exact')\
                .ilike('event_url', f'%{domain}%')\
                .limit(1)\
                .execute()
            
            if check.count and check.count > 0:
                sources.append(name)
        
        return sorted(sources)

    # ==================== SETTINGS ====================
    
    def get_setting(self, key, default=None):
        """Get a single setting value."""
        response = self.supabase.table('settings').select('value').eq('key', key).execute()
        if response.data:
            return response.data[0]['value']
        return default

    def get_all_settings(self):
        """Get all settings as a dictionary."""
        response = self.supabase.table('settings').select('key, value').execute()
        return {r['key']: r['value'] for r in (response.data or [])}

    def save_setting(self, key, value):
        """Save a setting value."""
        self.supabase.table('settings').upsert({
            'key': key,
            'value': str(value)
        }, on_conflict='key').execute()

    def save_settings(self, settings_dict):
        """Save multiple settings at once."""
        records = [{'key': k, 'value': str(v)} for k, v in settings_dict.items()]
        self.supabase.table('settings').upsert(records, on_conflict='key').execute()

    # ==================== SCRAPING URLS ====================
    
    def get_scraping_urls(self):
        """Get all scraping URLs with their status."""
        response = self.supabase.table('scraping_urls').select('*').order('name').execute()
        return [{"id": r['id'], "url": r['url'], "name": r['name'], "enabled": bool(r['enabled'])} 
                for r in (response.data or [])]

    def get_enabled_urls(self):
        """Get only enabled scraping URLs."""
        response = self.supabase.table('scraping_urls').select('url').eq('enabled', 1).execute()
        return [r['url'] for r in (response.data or [])]

    def toggle_url(self, url_id, enabled):
        """Enable or disable a scraping URL."""
        self.supabase.table('scraping_urls').update({
            'enabled': 1 if enabled else 0
        }).eq('id', url_id).execute()

    def add_scraping_url(self, url, name):
        """Add a new scraping URL."""
        try:
            self.supabase.table('scraping_urls').insert({
                'url': url,
                'name': name,
                'enabled': 1
            }).execute()
            return True
        except Exception:
            return False

    def delete_scraping_url(self, url_id):
        """Delete a scraping URL."""
        self.supabase.table('scraping_urls').delete().eq('id', url_id).execute()

    # ==================== SCRAPING LOGS ====================
    
    def add_log(self, run_type, status, events_found=0, failures=0, warnings=None):
        """Add a scraping log entry."""
        warnings_json = json.dumps(warnings) if warnings else None
        self.supabase.table('scraping_logs').insert({
            'type': run_type,
            'status': status,
            'events_found': events_found,
            'failures': failures,
            'warnings': warnings_json
        }).execute()

    def get_logs(self, days=30, status_filter="All"):
        """Get scraping logs optionally filtered by status."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        
        query = self.supabase.table('scraping_logs').select('*').gte('timestamp', cutoff)
        
        if status_filter and status_filter != "All":
            query = query.eq('status', status_filter)
        
        response = query.order('timestamp', desc=True).execute()
        logs = response.data or []
        
        # Parse warnings JSON
        for log in logs:
            if log.get("warnings"):
                try:
                    log["warnings"] = json.loads(log["warnings"])
                except:
                    pass
        
        return logs

    def clear_old_logs(self, days=90):
        """Clear logs older than specified days."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        response = self.supabase.table('scraping_logs').delete().lt('timestamp', cutoff).execute()
        return len(response.data) if response.data else 0

    # ==================== SCRAPE FAILURES ====================
    
    def log_scrape_failure(self, url, url_name, error_message, error_type='UNKNOWN', run_type='manual'):
        """Log a scraping failure for a specific URL."""
        # Check for existing active failure
        existing = self.supabase.table('scrape_failures').select('id').eq(
            'url', url
        ).eq('is_active', 1).execute()
        
        if existing.data:
            # Update existing
            self.supabase.table('scrape_failures').update({
                'error_message': error_message,
                'error_type': error_type,
                'occurred_at': datetime.now().isoformat(),
                'run_type': run_type
            }).eq('id', existing.data[0]['id']).execute()
        else:
            # Insert new
            self.supabase.table('scrape_failures').insert({
                'url': url,
                'url_name': url_name,
                'error_message': error_message,
                'error_type': error_type,
                'run_type': run_type
            }).execute()
    
    def get_active_failures(self):
        """Get all active (unresolved) scraping failures."""
        response = self.supabase.table('scrape_failures').select(
            'id, url, url_name, error_message, error_type, occurred_at, run_type'
        ).eq('is_active', 1).order('occurred_at', desc=True).execute()
        return response.data or []
    
    def resolve_failure(self, url):
        """Mark a failure as resolved when the URL scrapes successfully."""
        response = self.supabase.table('scrape_failures').update({
            'is_active': 0,
            'resolved_at': datetime.now().isoformat()
        }).eq('url', url).eq('is_active', 1).execute()
        return len(response.data) > 0 if response.data else False
    
    def get_failure_history(self, days=7):
        """Get failure history including resolved failures from the last N days."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        response = self.supabase.table('scrape_failures').select('*').gte(
            'occurred_at', cutoff
        ).order('occurred_at', desc=True).execute()
        return response.data or []
    
    def cleanup_old_failures(self, days=30):
        """Remove old resolved failures to keep database clean."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        response = self.supabase.table('scrape_failures').delete().eq(
            'is_active', 0
        ).lt('resolved_at', cutoff).execute()
        return len(response.data) if response.data else 0

    # ==================== ANALYTICS ====================
    
    def get_events_by_venue(self):
        """Get event counts grouped by venue/location."""
        response = self.supabase.table('events').select('location').not_.is_('location', 'null').execute()
        
        venue_counts = {}
        for r in (response.data or []):
            loc = r.get('location')
            if loc:
                venue_counts[loc] = venue_counts.get(loc, 0) + 1
        
        result = [{"venue": k, "count": v} for k, v in venue_counts.items()]
        result.sort(key=lambda x: x['count'], reverse=True)
        return result

    def get_events_by_target_group(self):
        """Get event counts grouped by target group."""
        response = self.supabase.table('events').select('target_group').not_.is_('target_group', 'null').execute()
        
        group_counts = {}
        for r in (response.data or []):
            tg = r.get('target_group')
            if tg:
                group_counts[tg] = group_counts.get(tg, 0) + 1
        
        return group_counts

    def get_events_timeline(self, weeks=4):
        """Get event counts per week for timeline chart."""
        timeline = []
        today = datetime.now()
        
        for week in range(weeks):
            week_start = today + timedelta(weeks=week)
            week_end = week_start + timedelta(days=6)
            
            response = self.supabase.table('events').select('id', count='exact').gte(
                'date_iso', week_start.strftime("%Y-%m-%d")
            ).lte('date_iso', week_end.strftime("%Y-%m-%d")).execute()
            
            timeline.append({"week": f"Week {week + 1}", "count": response.count or 0})
        
        return timeline

    # ==================== SELECTORS (for spider) ====================
    
    def get_selectors(self, url):
        """Get saved selectors for a URL pattern."""
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        url_path = parsed.path.rstrip('/')
        
        # Try exact match first
        response = self.supabase.table('selector_configs').select(
            'container_selector, item_selectors_json'
        ).eq('domain', domain).or_(f"url_pattern.eq.{url_path},url_pattern.eq.{url_path}/").execute()
        
        if not response.data:
            # Fallback to domain only
            response = self.supabase.table('selector_configs').select(
                'container_selector, item_selectors_json'
            ).eq('domain', domain).limit(1).execute()
        
        if response.data and response.data[0].get('item_selectors_json'):
            return {
                "container": response.data[0]['container_selector'],
                "items": json.loads(response.data[0]['item_selectors_json'])
            }
        return None

    def save_selectors(self, url, container_selector, item_selectors):
        """Save selectors for a URL pattern."""
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        url_pattern = parsed.path
        
        self.supabase.table('selector_configs').upsert({
            'domain': domain,
            'url_pattern': url_pattern,
            'container_selector': container_selector,
            'item_selectors_json': json.dumps(item_selectors),
            'last_updated': datetime.now().isoformat()
        }, on_conflict='domain,url_pattern').execute()

    def get_all_selector_configs(self):
        """Get all selector configurations from the database."""
        response = self.supabase.table('selector_configs').select('*').order('last_updated', desc=True).execute()
        return response.data or []

    def delete_selector_config(self, url):
        """Delete selector configuration for a URL."""
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "")
        url_pattern = parsed.path
        
        self.supabase.table('selector_configs').delete().eq(
            'domain', domain
        ).eq('url_pattern', url_pattern).execute()

    # ==================== UPDATES TAB (Comparison) ====================
    
    def get_added_events_today(self):
        """Get events where created_at matches today's date."""
        today = datetime.now().strftime("%Y-%m-%d")
        
        response = self.supabase.table('events').select('*').gte(
            'created_at', f"{today}T00:00:00"
        ).lt('created_at', f"{today}T23:59:59").order('event_url').order('date_iso').execute()
        
        return response.data or []
    
    def get_deleted_events(self, days_ahead=10):
        """Get events that may have been deleted (not updated in latest scrape)."""
        today = datetime.now()
        future_date = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        today_str = today.strftime("%Y-%m-%d")
        
        # Get last two scrape timestamps
        logs_response = self.supabase.table('scraping_logs').select('timestamp').in_(
            'status', ['OK', 'Warn']
        ).order('timestamp', desc=True).limit(2).execute()
        
        recent_logs = logs_response.data or []
        
        if len(recent_logs) < 2:
            return []
        
        latest_scrape = recent_logs[0]['timestamp']
        previous_scrape = recent_logs[1]['timestamp']
        
        # Get events in date range that weren't updated in latest scrape
        response = self.supabase.table('events').select('*').gte(
            'date_iso', today_str
        ).lte('date_iso', future_date).lt(
            'last_scraped', latest_scrape[:10]  # Compare date only
        ).order('event_url').order('date_iso').execute()
        
        return response.data or []
    
    def get_last_scrape_info(self):
        """Get information about the last two scrape runs."""
        response = self.supabase.table('scraping_logs').select(
            'timestamp, type, status, events_found'
        ).in_('status', ['OK', 'Warn']).order('timestamp', desc=True).limit(2).execute()
        
        rows = response.data or []
        
        if not rows:
            return None, None
        
        latest = rows[0] if len(rows) > 0 else None
        previous = rows[1] if len(rows) > 1 else None
        
        return latest, previous
    
    # ==================== SCHEDULED SCRAPING & COMPARATOR ====================
    
    @staticmethod
    def generate_unique_key(event_data):
        """Generate a stable unique key for an event."""
        event_name = event_data.get('event_name', '')
        date_iso = event_data.get('date_iso', '')
        location = event_data.get('location', '')
        
        if not location or location == 'N/A':
            location = ''
        
        unique_str = f"{event_name}|{date_iso}|{location}"
        return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()[:32]
    
    def log_scrape_run(self, run_type, window_days):
        """Log metadata for a scrape run and return the run ID."""
        response = self.supabase.table('scrape_runs').insert({
            'run_type': run_type,
            'window_days': window_days
        }).execute()
        
        if response.data:
            return response.data[0]['id']
        return None
    
    def update_scrape_run_stats(self, run_id, events_scraped, events_marked_deleted):
        """Update scrape run statistics after comparison."""
        self.supabase.table('scrape_runs').update({
            'events_scraped': events_scraped,
            'events_marked_deleted': events_marked_deleted
        }).eq('id', run_id).execute()
    
    # ==================== SCHEDULER SETTINGS ====================
    
    def get_scheduler_setting(self, key, default=None):
        """Get a scheduler setting value."""
        response = self.supabase.table('scheduler_settings').select('value').eq('key', key).execute()
        if response.data and response.data[0].get('value'):
            return response.data[0]['value']
        return default
    
    def get_all_scheduler_settings(self):
        """Get all scheduler settings as a dictionary."""
        response = self.supabase.table('scheduler_settings').select('key, value, updated_at').execute()
        return {r['key']: {"value": r['value'], "updated_at": r['updated_at']} for r in (response.data or [])}
    
    def set_scheduler_setting(self, key, value):
        """Set a scheduler setting value. Returns True if the value changed."""
        # Get current value
        response = self.supabase.table('scheduler_settings').select('value').eq('key', key).execute()
        old_value = response.data[0]['value'] if response.data else None
        
        # Update
        self.supabase.table('scheduler_settings').upsert({
            'key': key,
            'value': str(value),
            'updated_at': datetime.now().isoformat()
        }, on_conflict='key').execute()
        
        return old_value != str(value)
    
    def set_baseline_start_date(self, date_str):
        """Set the baseline start date."""
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
    
    # ==================== PENDING CHANGES ====================
    
    def stage_insert(self, event_data, run_id=None):
        """Stage an event for insertion."""
        unique_key = self.generate_unique_key(event_data)
        
        # Check if already staged
        existing = self.supabase.table('pending_changes').select('id').eq(
            'unique_key', unique_key
        ).eq('change_type', 'insert').execute()
        
        if existing.data:
            return False
        
        self.supabase.table('pending_changes').insert({
            'change_type': 'insert',
            'event_data': json.dumps(event_data),
            'unique_key': unique_key,
            'event_name': event_data.get('event_name', ''),
            'date_iso': event_data.get('date_iso', ''),
            'location': event_data.get('location', ''),
            'run_id': run_id
        }).execute()
        
        return True
    
    def stage_delete(self, event_data, run_id=None):
        """Stage an event for deletion."""
        unique_key = event_data.get('unique_key') or self.generate_unique_key(event_data)
        
        # Check if already staged
        existing = self.supabase.table('pending_changes').select('id').eq(
            'unique_key', unique_key
        ).eq('change_type', 'delete').execute()
        
        if existing.data:
            return False
        
        self.supabase.table('pending_changes').insert({
            'change_type': 'delete',
            'event_data': json.dumps(event_data),
            'unique_key': unique_key,
            'event_name': event_data.get('event_name', ''),
            'date_iso': event_data.get('date_iso', ''),
            'location': event_data.get('location', ''),
            'run_id': run_id
        }).execute()
        
        return True
    
    def get_pending_changes(self):
        """Get all pending changes grouped by type."""
        response = self.supabase.table('pending_changes').select('*').order('change_type').order('detected_at', desc=True).execute()
        
        changes = {"insert": [], "delete": []}
        for row in (response.data or []):
            change = {
                "id": row['id'],
                "change_type": row['change_type'],
                "event_data": json.loads(row['event_data']) if row['event_data'] else {},
                "unique_key": row['unique_key'],
                "event_name": row['event_name'],
                "date_iso": row['date_iso'],
                "location": row['location'],
                "detected_at": row['detected_at'],
                "run_id": row['run_id']
            }
            changes[row['change_type']].append(change)
        
        return changes
    
    def get_pending_counts(self):
        """Get counts of pending inserts and deletes."""
        response = self.supabase.table('pending_changes').select('change_type').execute()
        
        counts = {"insert": 0, "delete": 0}
        for row in (response.data or []):
            ct = row['change_type']
            if ct in counts:
                counts[ct] += 1
        
        return counts
    
    def apply_pending_changes(self):
        """Apply all pending changes to the main events table."""
        response = self.supabase.table('pending_changes').select('*').execute()
        changes = response.data or []
        
        inserted = 0
        deleted = 0
        
        for change in changes:
            try:
                if change['change_type'] == 'insert':
                    event_data = json.loads(change['event_data'])
                    self.upsert_event(event_data)
                    inserted += 1
                    
                elif change['change_type'] == 'delete':
                    self.supabase.table('events').delete().eq('unique_key', change['unique_key']).execute()
                    deleted += 1
                
                # Remove from pending
                self.supabase.table('pending_changes').delete().eq('id', change['id']).execute()
                
            except Exception as e:
                print(f"[ERROR] Failed to apply change {change['id']}: {e}")
                continue
        
        return {"inserted": inserted, "deleted": deleted}
    
    def discard_pending_changes(self):
        """Discard all pending changes without applying them."""
        response = self.supabase.table('pending_changes').delete().gte('id', 0).execute()
        return len(response.data) if response.data else 0
    
    def discard_single_change(self, change_id):
        """Discard a single pending change."""
        self.supabase.table('pending_changes').delete().eq('id', change_id).execute()
    
    # ==================== INCREMENTAL COMPARISON ====================
    
    def compare_and_stage_changes(self, scraped_events, start_date, end_date, run_id=None):
        """Compare scraped events against DB and stage changes."""
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
            return {"staged_inserts": 0, "staged_deletes": 0}
        
        # Get existing events in date range filtered by domains
        all_db_events = []
        for domain in scraped_domains:
            response = self.supabase.table('events').select(
                'id, event_name, date_iso, event_url, location, unique_key, time'
            ).gte('date_iso', start_date).lte('date_iso', end_date).ilike(
                'event_url', f'%{domain}%'
            ).execute()
            all_db_events.extend(response.data or [])
        
        db_events = {e['unique_key']: e for e in all_db_events if e.get('unique_key')}
        
        # Build set of scraped unique keys within date range
        scraped_keys = set()
        scraped_events_in_range = []
        for event in scraped_events:
            event_date = event.get('date_iso', '')
            if event_date and (event_date < start_date or event_date > end_date):
                continue
            unique_key = self.generate_unique_key(event)
            event['unique_key'] = unique_key
            scraped_keys.add(unique_key)
            scraped_events_in_range.append(event)
        
        staged_inserts = 0
        staged_deletes = 0
        url_updates = 0
        
        # Find events to INSERT
        for event in scraped_events_in_range:
            if event['unique_key'] not in db_events:
                if self.stage_insert(event, run_id):
                    staged_inserts += 1
        
        # Find events to DELETE
        for unique_key, db_event in db_events.items():
            if unique_key not in scraped_keys:
                if self.stage_delete(db_event, run_id):
                    staged_deletes += 1
        
        # Check for URL changes - update automatically
        for event in scraped_events_in_range:
            unique_key = event.get('unique_key')
            if unique_key and unique_key in db_events:
                db_event = db_events[unique_key]
                new_url = event.get('event_url', '')
                old_url = db_event.get('event_url', '')
                
                if new_url and old_url and new_url.rstrip('/') != old_url.rstrip('/'):
                    self.supabase.table('events').update({
                        'event_url': new_url,
                        'last_scraped': datetime.now().isoformat()
                    }).eq('id', db_event['id']).execute()
                    url_updates += 1
                    print(f"[URL UPDATE] {db_event['event_name']} ({db_event['date_iso']}): {old_url} → {new_url}")
        
        if url_updates > 0:
            print(f"  → Updated {url_updates} event URLs")
        
        return {"staged_inserts": staged_inserts, "staged_deletes": staged_deletes, "url_updates": url_updates}
    
    def get_events_in_date_range(self, start_date, end_date):
        """Get all events within a date range."""
        response = self.supabase.table('events').select('*').gte(
            'date_iso', start_date
        ).lte('date_iso', end_date).order('date_iso').execute()
        
        return response.data or []
