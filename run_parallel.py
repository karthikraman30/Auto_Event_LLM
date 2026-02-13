import subprocess
import json
import os
import sys
import argparse

# Fix SSL certificate verification for macOS
try:
    import certifi
    os.environ['SSL_CERT_FILE'] = certifi.where()
    os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
except ImportError:
    pass  # certifi not installed, use system defaults

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
import calendar
import pandas as pd

# Path setup to import the DatabaseManager
sys.path.append(os.path.join(os.getcwd(), "event_category"))
from event_category.utils.db_manager import DatabaseManager

# CRITICAL: Capture environment at module load time, BEFORE ProcessPoolExecutor spawns workers
# ProcessPoolExecutor creates NEW processes that don't inherit env vars set after fork
PARENT_ENV = os.environ.copy()


def get_baseline_date_range():
    """Calculate date range for Monthly Baseline scraper.
    
    Returns:
        tuple: (start_date, end_date) as YYYY-MM-DD strings
        - Start: 1st day of current month
        - End: 5th day of next month
    """
    today = datetime.now()
    
    # Start of current month
    start_date = today.replace(day=1)
    
    # 5th day of next month
    if today.month == 12:
        next_month = today.replace(year=today.year + 1, month=1, day=5)
    else:
        next_month = today.replace(month=today.month + 1, day=5)
    
    return start_date.strftime("%Y-%m-%d"), next_month.strftime("%Y-%m-%d")


def get_incremental_date_range():
    """Calculate date range for Incremental scraper (every 3 days).
    
    Returns:
        tuple: (start_date, end_date) as YYYY-MM-DD strings
        - Start: today
        - End: today + 6 days (covers next 5-6 days)
    """
    today = datetime.now()
    end_date = today + timedelta(days=6)
    
    return today.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def calculate_days_from_range(start_date_str, end_date_str):
    """Calculate number of days between two date strings."""
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    return (end - start).days + 1

def run_spider(args):
    """Run spider for a single URL and return result info.
    
    Args is a tuple of (url, index, env_dict, days) to work with ProcessPoolExecutor.
    """
    url, index, env_dict, days = args
    
    # Use absolute paths for production compatibility (Streamlit Cloud)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    event_category_dir = os.path.join(base_dir, "event_category")
    output_filename = f"temp_outputs/events_{index}.json"
    full_output_path = os.path.join(event_category_dir, output_filename)
    
    # Determine which spider to use based on URL
    spider_name = "unified_events"  # default
    
    cmd = [sys.executable, "-m", "scrapy", "crawl", spider_name, "-a", f"url={url}", "-a", f"days={days}", "-O", output_filename]
    
    # Debug logging for production
    print(f"[DEBUG] run_spider: Processing {url} with days={days}")
    print(f"[DEBUG] run_spider: GEMINI_API_KEY present in env: {'GEMINI_API_KEY' in env_dict}")
    print(f"[DEBUG] run_spider: cwd={event_category_dir}")
    
    try:
        # CRITICAL: Use the environment passed from main(), not os.environ
        print(f"[DEBUG] run_spider: Running command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd, 
            cwd=event_category_dir, 
            check=True, 
            timeout=3600,  # 1 hour timeout
            env=env_dict,  # Use explicitly passed environment
            capture_output=True,
            text=True
        )
        print(f"[DEBUG] run_spider: Command stdout: {result.stdout}")
        print(f"[DEBUG] run_spider: Command stderr: {result.stderr}")
        
        # Check for HTTP errors in stderr (spider may complete but with errors)
        stderr_lower = result.stderr.lower() if result.stderr else ''
        http_error = None
        if 'response <403' in stderr_lower or 'status_count/403' in stderr_lower:
            http_error = "HTTP 403 Forbidden - Website is blocking the scraper or is down for maintenance"
        elif 'response <404' in stderr_lower or 'status_count/404' in stderr_lower:
            http_error = "HTTP 404 Not Found - Page URL may have changed"
        elif 'response <5' in stderr_lower or 'status_count/50' in stderr_lower:
            http_error = "HTTP 5xx Server Error - Website is experiencing issues"
        elif 'connectionlost' in stderr_lower or 'connection refused' in stderr_lower:
            http_error = "Connection Error - Could not reach the website"
        elif 'timeout' in stderr_lower and 'gave up' in stderr_lower:
            http_error = "Connection Timeout - Website took too long to respond"
        
        if os.path.exists(full_output_path):
            # Check if the output file has actual events or is empty
            try:
                with open(full_output_path, 'r') as f:
                    content = f.read().strip()
                    events = json.loads(content) if content else []
                    if len(events) == 0 and http_error:
                        # File exists but empty due to HTTP error - this is a failure
                        return {"url": url, "path": None, "success": False, "error": http_error}
                    elif len(events) == 0 and not http_error:
                        # File exists but empty without HTTP error - could be no events in date range
                        # Check if there were any actual page responses
                        if 'scraped 0 items' in stderr_lower and 'httperror/response_ignored' in stderr_lower:
                            return {"url": url, "path": None, "success": False, "error": http_error or "No events scraped - page may have returned an error"}
                    return {"url": url, "path": full_output_path, "success": True, "error": None}
            except (json.JSONDecodeError, IOError) as e:
                return {"url": url, "path": None, "success": False, "error": f"Failed to read output file: {str(e)}"}
        return {"url": url, "path": None, "success": False, "error": f"Output file not created. stdout: {result.stdout[-500:] if result.stdout else 'empty'}"}
    except subprocess.TimeoutExpired:
        return {"url": url, "path": None, "success": False, "error": "Timeout after 30 minutes"}
    except subprocess.CalledProcessError as e:
        # Include stderr for debugging
        error_msg = f"Exit code {e.returncode}"
        if e.stderr:
            error_msg += f": {e.stderr[-300:]}"
        return {"url": url, "path": None, "success": False, "error": error_msg}
    except Exception as e:
        return {"url": url, "path": None, "success": False, "error": str(e)}

def merge_to_db(results):
    """Merge scraped events to database and return statistics."""
    print(f"[DEBUG] merge_to_db: Starting with {len(results)} results")
    
    try:
        db = DatabaseManager()
        print(f"[DEBUG] merge_to_db: DatabaseManager initialized")
    except Exception as e:
        print(f"[DEBUG] merge_to_db: Failed to initialize DatabaseManager: {e}")
        raise
    
    total_events = 0
    all_events = []  # Collect all events for incremental comparison
    
    for i, result in enumerate(results):
        print(f"[DEBUG] merge_to_db: Processing result {i+1}/{len(results)}")
        print(f"[DEBUG] merge_to_db: Result success: {result['success']}")
        
        if result["success"] and result["path"] and os.path.exists(result["path"]):
            try:
                print(f"[DEBUG] merge_to_db: Reading file: {result['path']}")
                with open(result["path"], 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Handle possible malformed JSON (multiple arrays concatenated)
                try:
                    events = json.loads(content)
                except json.JSONDecodeError as je:
                    print(f"[DEBUG] merge_to_db: JSON decode error - {je}, trying to fix...")
                    # Find the first complete JSON array
                    bracket_count = 0
                    end_pos = 0
                    for idx, char in enumerate(content):
                        if char == '[':
                            bracket_count += 1
                        elif char == ']':
                            bracket_count -= 1
                            if bracket_count == 0:
                                end_pos = idx + 1
                                break
                    events = json.loads(content[:end_pos])
                    print(f"[DEBUG] merge_to_db: Fixed JSON, found {len(events)} events in first array")
                
                print(f"[DEBUG] merge_to_db: Found {len(events)} events in file")
                
                for j, event in enumerate(events):
                    try:
                        print(f"[DEBUG] merge_to_db: Inserting event {j+1}/{len(events)}")
                        db.upsert_event(event)
                        all_events.append(event)
                    except Exception as e:
                        print(f"[DEBUG] merge_to_db: Failed to upsert event {j+1}: {e}")
                        print(f"[DEBUG] merge_to_db: Event data: {event}")
                        continue
                
                total_events += len(events)
                print(f"[DEBUG] merge_to_db: Successfully processed {len(events)} events")
                os.remove(result["path"])
                print(f"[DEBUG] merge_to_db: Removed temp file: {result['path']}")
            except Exception as e:
                print(f"[DEBUG] merge_to_db: Failed to process result file {result['path']}: {e}")
                continue
        else:
            if not result["success"]:
                print(f"[DEBUG] merge_to_db: Skipping failed result: {result['error']}")
            elif not result["path"]:
                print(f"[DEBUG] merge_to_db: Skipping result with no path")
            elif not os.path.exists(result["path"]):
                print(f"[DEBUG] merge_to_db: Skipping result with missing file: {result['path']}")
    
    print(f"[DEBUG] merge_to_db: Completed. Total events: {total_events}")
    return total_events, all_events


def collect_scraped_events(results):
    """Collect all scraped events without inserting to DB (for incremental comparison)."""
    all_events = []
    
    for result in results:
        if result["success"] and result["path"] and os.path.exists(result["path"]):
            try:
                with open(result["path"], 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Handle possible malformed JSON (multiple arrays concatenated)
                try:
                    events = json.loads(content)
                except json.JSONDecodeError:
                    # Find the first complete JSON array
                    bracket_count = 0
                    end_pos = 0
                    for idx, char in enumerate(content):
                        if char == '[':
                            bracket_count += 1
                        elif char == ']':
                            bracket_count -= 1
                            if bracket_count == 0:
                                end_pos = idx + 1
                                break
                    events = json.loads(content[:end_pos])
                
                all_events.extend(events)
                os.remove(result["path"])
            except Exception as e:
                print(f"[DEBUG] collect_scraped_events: Failed to process {result['path']}: {e}")
                continue
    
    return all_events


def get_event_count_from_file(file_path):
    """Count events in a scraped JSON file."""
    if not file_path or not os.path.exists(file_path):
        return 0
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        events = json.loads(content) if content.strip() else []
        return len(events)
    except:
        return 0


def retry_failed_urls(failed_urls, days, url_names, db, run_type):
    """Retry failed URLs sequentially (one at a time) to avoid resource contention."""
    import gc
    retry_results = []
    
    for url in failed_urls:
        url_name = url_names.get(url, url)
        print(f"[RETRY] Retrying {url_name}...")
        
        # Use a unique index for retry files
        retry_index = abs(hash(url)) % 10000
        result = run_spider((url, f"retry_{retry_index}", PARENT_ENV, days))
        
        if result['success']:
            event_count = get_event_count_from_file(result['path'])
            print(f"[RETRY] ✅ {url_name} succeeded with {event_count} events")
            db.resolve_failure(url)
        else:
            print(f"[RETRY] ❌ {url_name} still failed: {result['error']}")
            # Log failure again
            db.log_scrape_failure(
                url=url,
                url_name=url_name,
                error_message=f"Retry failed: {result['error']}",
                error_type='RETRY_FAILED',
                run_type=run_type
            )
        
        retry_results.append(result)
        gc.collect()
    
    return retry_results


def main(days=30, run_type='baseline', start_date=None):
    """
    Run parallel scraping for all enabled URLs.
    
    Args:
        days: Number of days to scrape (default 30, used to calculate end date from start_date)
        run_type: Type of run:
            - 'baseline': Monthly baseline scraper (custom start date or 1st of month)
                         Direct upsert to events table, source of truth
            - 'incremental': Every-3-days scraper (next 5-6 days)
                            Compares against DB, stages changes for review
            - 'full': Legacy alias for baseline
            - 'delta': Legacy alias for incremental
        start_date: Custom start date in YYYY-MM-DD format (for baseline runs)
    
    Returns dict with: events (count), failures (count), warnings (list), staged_inserts, staged_deletes
    """
    # Normalize run_type
    if run_type in ['full', 'baseline']:
        run_type = 'baseline'
    elif run_type in ['delta', 'incremental']:
        run_type = 'incremental'
    
    # Calculate date range based on run type
    today = datetime.now()
    
    if run_type == 'baseline':
        if start_date:
            # Custom start date provided - use it!
            print(f"[DEBUG] main: Using custom start_date: {start_date}")
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = start_date_obj + timedelta(days=days)
            start_date = start_date_obj.strftime("%Y-%m-%d")
            end_date = end_date_obj.strftime("%Y-%m-%d")
        elif days != 30:  # Custom days specified but no start date
            end_date = today + timedelta(days=days)
            start_date = today
        else:
            # Default: use standard baseline range (1st of month → 5th of next month)
            start_date, end_date = get_baseline_date_range()
            days = calculate_days_from_range(start_date, end_date)
    else:  # incremental
        # Incremental always uses fixed 7-day range for frequent monitoring
        start_date, end_date = get_incremental_date_range()
        days = calculate_days_from_range(start_date, end_date)
    
    # Ensure start_date and end_date are always strings (YYYY-MM-DD format)
    # This is critical for consistent DB queries in compare_and_stage_changes()
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")
    
    # Debug: Log environment status
    print(f"[DEBUG] main: GEMINI_API_KEY in PARENT_ENV: {'GEMINI_API_KEY' in PARENT_ENV}")
    print(f"[DEBUG] main: GEMINI_API_KEY in os.environ: {'GEMINI_API_KEY' in os.environ}")
    print(f"[DEBUG] main: Scraping with run_type={run_type}, days={days}")
    print(f"[DEBUG] main: Date range: {start_date} to {end_date}")
    
    db = DatabaseManager()
    
    # Log this scrape run to scrape_runs table
    run_id = db.log_scrape_run(run_type, days)
    run_timestamp = datetime.utcnow()  # Use UTC to match SQLite CURRENT_TIMESTAMP
    print(f"[DEBUG] main: Logged scrape run #{run_id} - {run_type} ({days} days)")
    
    # Get enabled URLs from database
    urls = db.get_enabled_urls()
    
    if not urls:
        print("No enabled URLs to scrape.")
        return {"events": 0, "failures": 0, "warnings": ["No enabled URLs configured"]}
    
    print(f"[DEBUG] main: Found {len(urls)} URLs to scrape")
    for i, url in enumerate(urls):
        print(f"[DEBUG] main: URL {i+1}: {url}")
    
    # Use absolute path for temp_outputs directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(base_dir, "event_category", "temp_outputs")
    os.makedirs(temp_dir, exist_ok=True)
    print(f"[DEBUG] main: Temp directory: {temp_dir}")
    print(f"[DEBUG] main: Temp dir exists: {os.path.exists(temp_dir)}")
    print(f"[DEBUG] main: Temp dir writable: {os.access(temp_dir, os.W_OK)}")
    
    results = []
    failures = 0
    warnings = []
    
    # Get URL names for error reporting
    url_names = {}
    for url_info in db.get_scraping_urls():
        url_names[url_info['url']] = url_info['name']
    
    # Pass environment explicitly to each worker
    # Detect environment: Cloud Run has PORT env var set, Streamlit may have STREAMLIT_SERVER_PORT
    is_cloud_run = os.environ.get('PORT') and not os.environ.get('STREAMLIT_SERVER_PORT')
    
    if is_cloud_run:
        # Cloud Run with 16GB RAM, 4 CPUs - can handle 2 parallel Playwright workers comfortably
        max_workers = 2
    else:
        # Streamlit Cloud has ~1GB RAM - use sequential to prevent OOM
        max_workers = 1
    
    print(f"[DEBUG] Using max_workers={max_workers} (Cloud Run: {is_cloud_run})")
    
    import gc
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Create args tuples with (url, index, env_dict, days)
        args_list = [(url, i, PARENT_ENV, days) for i, url in enumerate(urls)]
        futures = {executor.submit(run_spider, args): args[0] for args in args_list}
        
        for idx, future in enumerate(as_completed(futures)):
            result = future.result()
            results.append(result)
            print(f"[PROGRESS] Completed {idx+1}/{len(urls)}: {result['url']}")
            print(f"[DEBUG] main: Got result for {result['url']}")
            print(f"[DEBUG] main: Success: {result['success']}")
            
            # Force garbage collection after each URL to prevent OOM on Streamlit Cloud
            gc.collect()
            
            if result['success']:
                print(f"[DEBUG] main: Output file: {result['path']}")
                if result['path'] and os.path.exists(result['path']):
                    file_size = os.path.getsize(result['path'])
                    print(f"[DEBUG] main: File size: {file_size} bytes")
                else:
                    print(f"[DEBUG] main: Output file not found!")
                
                # URL succeeded - resolve any active failure for this URL
                db.resolve_failure(result['url'])
            else:
                print(f"[DEBUG] main: Error: {result['error']}")
            
            if not result["success"]:
                failures += 1
                # Extract site name from URL for warning message
                url_name = url_names.get(result["url"], None)
                if not url_name:
                    try:
                        from urllib.parse import urlparse
                        url_name = urlparse(result["url"]).netloc.replace("www.", "").split(".")[0].capitalize()
                    except:
                        url_name = result["url"]
                
                # Classify error type
                error_msg = result['error'] or 'Unknown error'
                if '403' in error_msg:
                    error_type = 'HTTP_403'
                elif '404' in error_msg:
                    error_type = 'HTTP_404'
                elif '500' in error_msg or '502' in error_msg or '503' in error_msg:
                    error_type = 'HTTP_SERVER_ERROR'
                elif 'timeout' in error_msg.lower():
                    error_type = 'TIMEOUT'
                elif 'connection' in error_msg.lower():
                    error_type = 'CONNECTION_ERROR'
                else:
                    error_type = 'UNKNOWN'
                
                # Log failure to scrape_failures table
                db.log_scrape_failure(
                    url=result["url"],
                    url_name=url_name,
                    error_message=error_msg,
                    error_type=error_type,
                    run_type=run_type
                )
                
                warnings.append(f"{url_name}: {error_msg}")
    
    # === RETRY LOGIC: Check for failed URLs and retry them ===
    failed_urls = []
    for result in results:
        if not result['success']:
            failed_urls.append(result['url'])
        elif result['path']:
            # Also retry URLs that produced 0 events (silent failures)
            event_count = get_event_count_from_file(result['path'])
            if event_count == 0:
                print(f"[WARNING] {result['url']} succeeded but has 0 events - adding to retry")
                failed_urls.append(result['url'])
    
    if failed_urls:
        print(f"\n[RETRY] {len(failed_urls)} URL(s) need retry: {[url_names.get(u, u) for u in failed_urls]}")
        retry_results = retry_failed_urls(failed_urls, days, url_names, db, run_type)
        
        # Update results: replace failed results with retry results
        for retry_result in retry_results:
            # Find and replace the original result
            for i, orig_result in enumerate(results):
                if orig_result['url'] == retry_result['url']:
                    if retry_result['success']:
                        results[i] = retry_result
                        # Decrement failure count if retry succeeded
                        if not orig_result['success']:
                            failures -= 1
                    break
        
        # Update warnings list
        still_failed = [r for r in retry_results if not r['success']]
        if still_failed:
            print(f"[RETRY] ⚠️ {len(still_failed)} URL(s) still failed after retry")
    else:
        print(f"\n[OK] All {len(urls)} URLs completed successfully")
    
    # Process results based on run type
    staged_inserts = 0
    staged_deletes = 0
    
    if run_type == 'baseline':
        # BASELINE SCRAPER: Direct upsert to events table (source of truth)
        try:
            total_events, _ = merge_to_db(results)
            db.update_last_baseline_run()
        except Exception as e:
            print(f"❌ Failed to merge results to database: {e}")
            raise
        
        # Update scrape run stats
        db.update_scrape_run_stats(run_id, total_events, 0)
        
        print(f"Baseline scraping complete: {total_events} events, {failures} failures")
        
    else:
        # INCREMENTAL SCRAPER: Compare and stage changes for review
        try:
            # Collect scraped events without inserting
            scraped_events = collect_scraped_events(results)
            total_events = len(scraped_events)
            
            # Compare against DB and stage changes
            comparison = db.compare_and_stage_changes(scraped_events, start_date, end_date, str(run_id))
            staged_inserts = comparison["staged_inserts"]
            staged_deletes = comparison["staged_deletes"]
            url_updates = comparison.get("url_updates", 0)
            
            db.update_last_incremental_run()
            
        except Exception as e:
            print(f"❌ Failed to process incremental scrape: {e}")
            raise
        
        # Update scrape run stats
        db.update_scrape_run_stats(run_id, total_events, staged_deletes)
        
        print(f"Incremental scraping complete: {total_events} events scraped")
        print(f"  → Staged {staged_inserts} inserts, {staged_deletes} deletes for review")
        if url_updates > 0:
            print(f"  → Updated {url_updates} event URLs automatically")
    
    print(f"Scraping complete: {total_events} events, {failures} failures")
    
    return {
        "events": total_events,
        "failures": failures,
        "warnings": warnings if warnings else None,
        "staged_inserts": staged_inserts,
        "staged_deletes": staged_deletes,
        "run_type": run_type
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run parallel event scraping')
    parser.add_argument('--days', type=int, default=30, help='Number of days to scrape (default: 30)')
    parser.add_argument('--run-type', type=str, default='baseline', 
                        choices=['baseline', 'incremental', 'full', 'delta'], 
                        help='Type of run: baseline (monthly) or incremental (every 3 days)')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Custom start date in YYYY-MM-DD format (for baseline runs)')
    args = parser.parse_args()
    
    result = main(days=args.days, run_type=args.run_type, start_date=args.start_date)
    print(f"Result: {result}")