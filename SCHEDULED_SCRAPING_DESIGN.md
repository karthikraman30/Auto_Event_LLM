# Scheduled Event Scraping System Design

## Overview

Automated event scraping system with two scheduled jobs that detect additions and deletions per-URL within defined time windows.

## Scheduling

### Sunday 06:00 - Full Baseline Scrape
- **Window**: 30 days ahead
- **Scope**: All enabled URLs in settings
- **Purpose**: Establish complete baseline dataset
- **Comparator**: Marks events as deleted if missing from 30-day window (with grace period)

### Wednesday 06:00 - Delta Mid-Week Scrape
- **Window**: 10 days ahead
- **Scope**: All enabled URLs in settings
- **Purpose**: Catch rapid changes and cancellations in near-term events
- **Comparator**: Marks events as deleted ONLY if within 10-day window (with grace period)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         SCHEDULER                                │
│  (APScheduler BackgroundScheduler in admin_console.py)          │
│                                                                   │
│  Sunday 06:00  → run_scheduled_scrape(days=30, run_type='full') │
│  Wednesday 06:00 → run_scheduled_scrape(days=10, run_type='delta')│
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                    SCRAPER ORCHESTRATOR                          │
│                  (run_parallel.py::main)                         │
│                                                                   │
│  1. Log scrape run metadata to scrape_runs table                │
│  2. Get enabled URLs from scraping_urls table                   │
│  3. Run spiders in parallel (ProcessPoolExecutor)               │
│  4. Merge results to DB via upsert_event()                      │
│  5. Call comparator: run_comparator(run_type, days, timestamp)  │
│  6. Update scrape_runs with stats (events_scraped, deleted)     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                      COMPARATOR                                  │
│           (db_manager.py::run_comparator)                        │
│                                                                   │
│  For each event in DB overlapping scrape window:                │
│    - If last_scraped >= run_timestamp → Clear missing_count     │
│    - If last_scraped < run_timestamp:                           │
│        - If missing_count < 1 → Increment missing_count (grace) │
│        - If missing_count >= 1 → Mark as deleted                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                    ADMIN CONSOLE                                 │
│              (admin_console.py → Updates Tab)                    │
│                                                                   │
│  - Show Added Events (created_at matches today)                 │
│  - Show Deleted Events (deletion_status='deleted')              │
│  - Restore button (set deletion_status='active')                │
│  - Delete permanently button                                     │
└─────────────────────────────────────────────────────────────────┘
```

## Database Schema Changes

### Events Table - New Columns

```sql
-- Stable unique identifier (canonical key for deduplication)
unique_key TEXT

-- Deletion tracking
deletion_status TEXT DEFAULT 'active'  -- 'active' | 'deleted'
deleted_at TIMESTAMP NULL
deleted_by_run_type TEXT NULL  -- 'full' | 'delta'
deleted_window_days INTEGER NULL  -- 30 | 10
missing_count INTEGER DEFAULT 0  -- Grace period counter
```

### Scrape Runs Table (New)

```sql
CREATE TABLE scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    run_type TEXT NOT NULL,  -- 'full' | 'delta'
    window_days INTEGER NOT NULL,  -- 30 | 10
    events_scraped INTEGER DEFAULT 0,
    events_marked_deleted INTEGER DEFAULT 0
);
```

### Indexes

```sql
CREATE INDEX idx_events_dates ON events(date_iso, end_date_iso);
CREATE INDEX idx_events_last_scraped ON events(last_scraped);
CREATE INDEX idx_events_deletion_status ON events(deletion_status);
CREATE INDEX idx_events_unique_key ON events(unique_key);
```

## Comparison Algorithm

### Unique Key Generation

**Option B (Implemented)**: Canonical `unique_key` column

```python
def generate_unique_key(event_data):
    event_url = event_data.get('event_url', '')
    event_name = event_data.get('event_name', '')
    date_iso = event_data.get('date_iso', '')
    location = event_data.get('location', '')
    
    # For Stockholm library: include time/location in hash
    if 'biblioteket.stockholm.se' in event_url:
        unique_str = f"{event_name}|{date_iso}|{event_data.get('time')}|{location}"
        return sha256(unique_str)[:32]
    
    # For other sites: URL + date
    if event_url and event_url != 'N/A':
        unique_str = f"{event_url}|{date_iso}"
        return sha256(unique_str)[:32]
    
    # Fallback: name+date+location
    unique_str = f"{event_name}|{date_iso}|{location}"
    return sha256(unique_str)[:32]
```

### Comparator Pseudocode

```python
def run_comparator(run_type, window_days, run_timestamp):
    """
    Compare scraped events against DB to mark deletions within window.
    
    Args:
        run_type: 'full' (30 days) or 'delta' (10 days)
        window_days: 30 or 10
        run_timestamp: datetime when scrape started
    
    Returns:
        Number of events marked as deleted
    """
    today = date.today()
    window_end = today + timedelta(days=window_days)
    
    # Get all active events overlapping with scrape window
    candidate_events = SELECT * FROM events
        WHERE deletion_status = 'active'
        AND (
            -- Single-day events
            (end_date_iso IS NULL AND date_iso >= today AND date_iso <= window_end)
            OR
            -- Multi-day events (start <= window_end AND end >= today)
            (end_date_iso IS NOT NULL AND date_iso <= window_end AND end_date_iso >= today)
        )
    
    deleted_count = 0
    
    FOR each event IN candidate_events:
        IF event.last_scraped >= run_timestamp:
            # Event was updated in this scrape - clear grace
            IF event.missing_count > 0:
                UPDATE events SET missing_count = 0 WHERE id = event.id
            CONTINUE
        
        # Event should have been scraped but wasn't found
        IF event.missing_count < 1:
            # Grace period: increment missing count
            UPDATE events SET missing_count = missing_count + 1 WHERE id = event.id
        ELSE:
            # Mark as deleted (missed 2+ consecutive scrapes)
            UPDATE events SET
                deletion_status = 'deleted',
                deleted_at = run_timestamp,
                deleted_by_run_type = run_type,
                deleted_window_days = window_days
            WHERE id = event.id
            
            deleted_count += 1
    
    RETURN deleted_count
```

## Edge Cases

### 1. Event Date Change

**Scenario**: Event moved from Jan 20 to Jan 25

**Behavior**: 
- Old date entry marked as `deleted` (unique key includes date)
- New date inserted as separate row with new `unique_key`
- Admin UI should detect pattern: same `event_name` + `event_url` but different dates

**Implementation**: Optional enhancement to group related events in UI showing "Date Changed: Jan 20 → Jan 25"

### 2. Event Title Change

**Scenario**: Event title changes but URL/date/location unchanged

**Behavior**:
- `upsert_event()` updates `event_name` in existing row (same unique_key)
- No deletion marking
- `last_scraped` updated to indicate freshness

**Future Enhancement**: Add `title_history TEXT` column to track past titles (JSON array)

### 3. Temporary Missing Event

**Scenario**: Website temporarily down or event hidden during scrape

**Behavior**:
- **First miss**: `missing_count` incremented to 1, status stays `active`
- **Second miss**: Marked as `deleted`
- **Found again**: `missing_count` reset to 0, status remains/restored to `active`

**Grace Period**: Prevents false positives from transient website issues

### 4. Multi-Day Event Spanning Window

**Scenario**: 7-day event (Jan 15-22) with delta window (Jan 18-28)

**Behavior**:
- Event overlaps window → Should be re-scraped
- If not found → Mark as deleted after grace period
- Logic: `date_iso <= window_end AND end_date_iso >= today`

### 5. Sunday Run After Wednesday

**Scenario**: Event deleted in day 25 (outside 10-day delta window)

**Behavior**:
- **Wednesday delta**: Event outside window → NOT checked → `missing_count` stays 0
- **Next Sunday full**: Event in 30-day window → Checked → Missing count incremented → Deleted on 2nd Sunday miss

**Key**: Each run type only checks events within its own window

## Testing Plan

### Unit Tests (`test_comparator_logic.py`)

1. **test_event_in_10day_window_marked_deleted**
   - Insert event in day 5
   - Run delta comparator twice (simulate 2 Wednesday runs)
   - Assert: Event deleted after 2nd miss

2. **test_event_outside_10day_window_not_marked_deleted**
   - Insert event in day 15
   - Run delta comparator
   - Assert: Event NOT touched, `missing_count` = 0

3. **test_event_in_30day_window_marked_deleted_by_full_run**
   - Insert event in day 25
   - Run full comparator twice
   - Assert: Event deleted by 2nd full run

4. **test_multiday_event_partial_overlap**
   - Insert event spanning days -2 to +5
   - Run delta comparator twice
   - Assert: Event deleted (overlaps window)

5. **test_event_found_after_grace_clears_missing_count**
   - Insert event, run comparator (grace), re-upsert event, run comparator again
   - Assert: `missing_count` reset to 0

### Integration Tests (`test_full_delta_workflow.py`)

1. **test_sunday_to_wednesday_workflow**
   - Sunday: Insert events at days 0, 6, 12, 18, 24
   - Wednesday: Re-scrape only day 0 (simulate day 6 deleted)
   - Wednesday 2nd: Re-scrape only day 0
   - Assert: Day 6 deleted, days 12/18/24 still active

2. **test_new_event_added_between_runs**
   - Sunday: Insert event A
   - Wednesday: Insert event B
   - Assert: Event B has later `created_at`

3. **test_event_deleted_in_day_25_caught_by_sunday**
   - Insert event at day 25
   - Wednesday delta: Assert NOT checked
   - Sunday full (1st): Assert grace period
   - Sunday full (2nd): Assert deleted

### Running Tests

```bash
# Unit tests
python test_comparator_logic.py

# Integration tests
python test_full_delta_workflow.py

# Both
python test_comparator_logic.py && python test_full_delta_workflow.py
```

### Test Validation Criteria

✓ **New Events**: `created_at` timestamp detection  
✓ **Deleted Events**: Only within scrape window, respects grace period  
✓ **No False Positives**: Events outside window untouched  
✓ **Multi-day Events**: Partial overlap handled correctly  
✓ **Recovery**: Found events clear grace counter  

## Running the System

### Manual Triggers (Admin Console)

```
Dashboard Tab → 🚀 Scrape Now (30-day full run)
Settings Tab → 🔄 Run Delta Scan (10-day delta run)
```

### Command Line

```bash
# Full 30-day baseline
python run_parallel.py --days 30 --run-type full

# Delta 10-day mid-week
python run_parallel.py --days 10 --run-type delta

# Custom window
python run_parallel.py --days 15 --run-type delta
```

### Automated Schedule

Scheduler runs automatically when `admin_console.py` starts:
- **Sunday 06:00**: Full 30-day baseline
- **Wednesday 06:00**: Delta 10-day scan

## Admin Console - Updates Tab

### Features

1. **Added Events**
   - Shows events where `created_at` matches today
   - Grouped by source URL
   - Displays: name, date, location, time

2. **Deleted Events**
   - Shows events where `deletion_status = 'deleted'`
   - Displays: name, date, deleted_at, run_type (full/delta)
   - Actions:
     - ✓ Restore: Set `deletion_status = 'active'`
     - 🗑️ Delete Permanently: Hard delete from DB

3. **Filters** (Future Enhancement)
   - Date range picker
   - URL/source filter
   - Run type filter (full/delta)

## Troubleshooting

### Event falsely marked as deleted

**Cause**: Website was temporarily down during both grace period checks

**Fix**: Click "Restore" button in Updates tab

### Event outside 10-day window marked deleted by Wednesday

**Check**: Event date range. Multi-day events spanning into window ARE checked.

**Example**: Event Jan 7-Jan 20 with delta window Jan 18-28 → Overlaps → Will be checked

### Event in day 25 not caught by Wednesday

**Expected**: Wednesday only checks 10-day window. Event will be caught by next Sunday full run.

## Future Enhancements

1. **Field-Level Change Detection**
   - Add `event_changes` table to track title/date/location changes
   - Detect modifications vs. delete+add patterns

2. **Notification System**
   - Email alerts for significant deletions (> 10% of events)
   - Slack/Discord webhooks for daily summaries

3. **Smart Grace Period**
   - Adjust grace based on event proximity (events tomorrow = no grace)
   - Per-URL grace settings (unreliable sources = longer grace)

4. **Change Export**
   - CSV export of added/deleted/modified events
   - Integration with external analytics tools

5. **Retention Policy**
   - Purge `deleted` events older than 90 days
   - Archive to separate table for historical analysis

## Files Modified

- `event_category/event_category/utils/db_manager.py`: Schema changes, comparator logic
- `run_parallel.py`: Added `run_type` parameter, comparator invocation
- `admin_console.py`: Dual scheduler, Updates tab UI changes
- `test_comparator_logic.py`: Unit tests (NEW)
- `test_full_delta_workflow.py`: Integration tests (NEW)
- `SCHEDULED_SCRAPING_DESIGN.md`: This document (NEW)
