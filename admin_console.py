import streamlit as st
import pandas as pd
import os
import sys
import math
import subprocess
import re
import json
import requests
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

sys.path.append(os.path.join(os.getcwd(), "event_category"))
from event_category.utils.db_manager import DatabaseManager
from seed_selectors import ensure_selectors_seeded

# --- CLOUD RUN CONFIGURATION ---
CLOUD_RUN_URL = "https://event-scraper-563240550469.us-central1.run.app"

def trigger_cloud_run_scraper(mode: str, timeout: int = 600) -> dict:
    """Trigger Cloud Run scraper via HTTP POST.
    
    Args:
        mode: 'baseline' or 'incremental'  
        timeout: Request timeout in seconds (default 10 min)
        
    Returns:
        dict with 'success', 'message', and optionally 'data'
    """
    try:
        response = requests.post(
            CLOUD_RUN_URL,
            json={"mode": mode},
            headers={"Content-Type": "application/json"},
            timeout=timeout
        )
        
        if response.status_code == 200:
            return {"success": True, "message": "Scraper completed", "data": response.json()}
        else:
            return {"success": False, "message": f"Error {response.status_code}: {response.text[:500]}"}
    except requests.exceptions.Timeout:
        return {"success": False, "message": "Request timed out - scraper may still be running in background"}
    except requests.exceptions.RequestException as e:
        return {"success": False, "message": f"Request failed: {str(e)}"}

# --- AUTO-SEED SELECTORS ON STARTUP ---
# This ensures deployed apps have default selectors even though selectors.db is gitignored
_seed_result = ensure_selectors_seeded(verbose=True)

# --- FIX: Add Python path at module level for Streamlit Cloud ---
# This ensures all functions can find the event_category modules
current_dir = os.getcwd()
event_category_dir = os.path.join(current_dir, "event_category")
nested_event_dir = os.path.join(current_dir, "event_category", "event_category")

# Add paths to sys.path at module level
for path in [current_dir, event_category_dir, nested_event_dir]:
    if path not in sys.path:
        sys.path.insert(0, path)

# --- CONSTANTS ---
RUN_PARALLEL_FILE = "run_parallel.py"
UNKNOWN_ERROR = "Unknown error"

# --- PYTHON PATH (use venv Python for subprocess) ---
# --- PYTHON PATH (use venv Python for subprocess) ---
# Check if running on Streamlit Cloud (headless) or locally
if os.path.exists(os.path.join(os.getcwd(), "venv")):
    VENV_PYTHON = os.path.join(os.getcwd(), "venv", "bin", "python")
else:
    # On Streamlit Cloud, use the same python that launched the app
    VENV_PYTHON = sys.executable

if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = sys.executable  # Fallback

def test_direct_scraping():
    """Test direct scraping without subprocess."""
    try:
        # Fix Python path for Streamlit Cloud - same fix as get_subprocess_env
        import sys
        import os
        
        # Add all necessary paths to sys.path - same as subprocess fix
        current_dir = os.getcwd()
        event_category_dir = os.path.join(current_dir, "event_category")
        nested_event_dir = os.path.join(current_dir, "event_category", "event_category")
        
        # Add paths in order of preference
        for path in [current_dir, event_category_dir, nested_event_dir]:
            if path not in sys.path:
                sys.path.insert(0, path)
        
        print(f"Python paths: {sys.path[:5]}")  # Debug first 5 paths
        print(f"Current dir: {current_dir}")
        print(f"Event category dir exists: {os.path.exists(event_category_dir)}")
        print(f"Nested event dir exists: {os.path.exists(nested_event_dir)}")
        
        # Import and test the main scraping function directly
        import importlib.util
        
        # Load run_parallel.py as a module
        spec = importlib.util.spec_from_file_location("run_parallel", RUN_PARALLEL_FILE)
        run_parallel = importlib.util.module_from_spec(spec)
        
        # Execute the module
        spec.loader.exec_module(run_parallel)
        
        # Test database connection first
        db = run_parallel.DatabaseManager()
        urls = db.get_enabled_urls()
        
        if not urls:
            return {
                "direct_scraping_working": False,
                "error": "No enabled URLs found",
                "urls_count": 0
            }
        
        # Try to run a simple test with just one URL
        test_url = urls[0]
        
        # Test the run_spider function directly
        env = os.environ.copy()
        
        # Add secrets if available
        # Note: st.secrets might not be available in all contexts
        try:
            import streamlit as st
            if hasattr(st, "secrets") and "GEMINI_API_KEY" in st.secrets:
                env["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]
        except ImportError:
            pass  # Streamlit not available in this context
        
        return {
            "direct_scraping_working": True,
            "test_url": test_url,
            "env_keys": list(env.keys()),
            "message": "Direct import successful, ready to test scraping"
        }
        
    except Exception as e:
        return {
            "direct_scraping_working": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

def test_scrapy_setup():
    """Test if Scrapy project is properly configured."""
    try:
        import subprocess
        import sys
        import os
        
        # Fix Python path for Streamlit Cloud
        current_dir = os.getcwd()
        event_category_dir = os.path.join(current_dir, "event_category")
        
        # Test if we can run scrapy list command
        cmd = [sys.executable, "-m", "scrapy", "list"]
        
        # Set up environment with correct Python path
        env = os.environ.copy()
        env['PYTHONPATH'] = event_category_dir
        
        print(f"Running scrapy in: {event_category_dir}")
        print(f"Command: {' '.join(cmd)}")
        print(f"PYTHONPATH: {env['PYTHONPATH']}")
        
        result = subprocess.run(
            cmd, 
            cwd=event_category_dir, 
            capture_output=True,
            text=True,
            timeout=30,
            env=env
        )
        
        return {
            "scrapy_working": result.returncode == 0,
            "spiders_found": result.stdout.strip().split('\n') if result.returncode == 0 else [],
            "error": result.stderr if result.returncode != 0 else None,
            "working_dir": event_category_dir,
            "command": ' '.join(cmd),
            "python_path": env['PYTHONPATH']
        }
        
    except Exception as e:
        return {
            "scrapy_working": False,
            "error": str(e),
            "working_dir": event_category_dir if 'event_category_dir' in locals() else "unknown"
        }

def test_basic_functionality():
    """Test basic functionality to isolate the issue."""
    try:
        # Test database connection
        db = DatabaseManager()
        urls = db.get_enabled_urls()
        
        return {
            "database_working": True,
            "enabled_urls_count": len(urls),
            "urls": urls[:3] if urls else []  # Show first 3 URLs
        }
    except Exception as e:
        return {
            "database_working": False,
            "error": str(e)
        }

def scrape_directly():
    """Fallback scraping method that imports functions directly instead of subprocess."""
    try:
        # Import the main function from run_parallel.py
        import importlib.util
        import sys
        
        # Load run_parallel.py as a module
        spec = importlib.util.spec_from_file_location("run_parallel", RUN_PARALLEL_FILE)
        run_parallel = importlib.util.module_from_spec(spec)
        
        # Set up environment for the module
        import os
        env = get_subprocess_env()
        for key, value in env.items():
            os.environ[key] = value
        
        # Execute the module
        spec.loader.exec_module(run_parallel)
        
        # Call the main function
        result = run_parallel.main()
        return result
        
    except Exception as e:
        return {"events": 0, "failures": 1, "warnings": [f"Direct scraping failed: {str(e)}"]}

def get_subprocess_env():
    """
    Create environment dict for subprocess, injecting secrets and
    setting the PYTHONPATH so modules can be found in deployment.
    """
    env = os.environ.copy()
    
    # --- FIX: Set SSL certificate path for macOS ---
    try:
        import certifi
        env['SSL_CERT_FILE'] = certifi.where()
        env['REQUESTS_CA_BUNDLE'] = certifi.where()
    except ImportError:
        pass  # certifi not installed, use system defaults
    
    # --- FIX: Set the Python Path for the Subprocess ---
    current_dir = os.getcwd()
    # This points to the folder containing the 'event_category' package
    event_category_dir = os.path.join(current_dir, "event_category")
    
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = f"{current_dir}{os.pathsep}{event_category_dir}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = f"{current_dir}{os.pathsep}{event_category_dir}"

    # --- Existing Secret Injection ---
    sensitive_keys = ["GEMINI_API_KEY"]
    try:
        if hasattr(st, "secrets"):
            for key in sensitive_keys:
                if key in st.secrets:
                    env[key] = st.secrets[key]
                elif "env" in st.secrets and key in st.secrets["env"]:
                    env[key] = st.secrets["env"][key]
    except Exception as e:
        print(f"Warning: Could not access Streamlit secrets: {e}")
        
    return env

# --- PAGE CONFIG ---
st.set_page_config(page_title="Event Scraper Admin", layout="wide", page_icon="🎭")

# --- DATABASE ---
db = DatabaseManager()

# --- BACKGROUND SCHEDULER ---
def run_scheduled_scrape(run_type='baseline'):
    """Run scraping job and log results using subprocess for parallel execution.
    
    Args:
        run_type: 'baseline' (monthly) or 'incremental' (every 3 days)
    """
    import subprocess
    import re
    try:
        env = get_subprocess_env()
        result = subprocess.run(
            [VENV_PYTHON, RUN_PARALLEL_FILE, '--run-type', run_type],
            cwd=os.getcwd(),
            capture_output=True,
            text=True,
            timeout=2700,
            env=env
        )
        
        output = result.stdout
        if "Scraping complete:" in output:
            match = re.search(r'Scraping complete: (\d+) events, (\d+) failures', output)
            if match:
                events_count = int(match.group(1))
                failures = int(match.group(2))
            else:
                events_count = 0
                failures = 0
            
            status = "Warn" if failures > 0 else "OK"
            warnings = [line for line in output.split('\n') if 'Error' in line or 'Warning' in line]
            log_type = f"{run_type.capitalize()}"
            db.add_log(log_type, status, events_count, failures, warnings if warnings else None)
        else:
            db.add_log(run_type.capitalize(), "Warn", 0, 0, ["Could not parse scraping results"])
    except subprocess.TimeoutExpired:
        db.add_log(run_type.capitalize(), "Error", 0, 1, ["Scraping timed out after 45 minutes"])
    except Exception as e:
        db.add_log(run_type.capitalize(), "Error", 0, 1, [str(e)])

def setup_scheduler():
    """Initialize scheduler with two jobs: Monthly baseline and Every-3-days incremental."""
    scheduler = BackgroundScheduler()
    
    # 1st of every month at 06:00: Baseline scraper (current month + 5 days into next)
    scheduler.add_job(
        lambda: run_scheduled_scrape(run_type='baseline'), 
        CronTrigger(day=1, hour=6, minute=0)
    )
    print("✓ Scheduled Monthly Baseline scraper (1st of month at 06:00)")
    
    # Every 3 days at 06:00: Incremental scraper (next 5-6 days)
    scheduler.add_job(
        lambda: run_scheduled_scrape(run_type='incremental'),
        CronTrigger(day='*/3', hour=6, minute=0)
    )
    print("✓ Scheduled Incremental scraper (every 3 days at 06:00)")
    
    scheduler.start()
    return scheduler
    
@st.cache_resource
def install_playwright_browsers():
    """
    Install Playwright browsers (Chromium) on first run.
    Uses @st.cache_resource to ensure it only runs once per session/deploy.
    """
    print("Checking/Installing Playwright browsers...")
    try:
        # Check if browser is installed by trying to get the path
        # If this fails or returns empty, we might need to install
        import subprocess
        
        # Only install chromium to save time/space
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
        print("Playwright installation complete.")
    except Exception as e:
        print(f"Error installing Playwright: {e}")

# Run installation on startup
install_playwright_browsers()

if 'scheduler' not in st.session_state:
    st.session_state.scheduler = setup_scheduler()

# --- HEADER ---
st.title("🎭 Event Scraper Admin Console")

# --- TABS ---
tabs = st.tabs(["📊 Dashboard", "⚙️ Settings", "📝 Logs", "🎯 Selectors", "🔄 Updates"])

# =============================================================================
# TAB 1: DASHBOARD
# =============================================================================
with tabs[0]:
    # --- OVERVIEW SECTION ---
    st.markdown("---")
    st.subheader("📋 OVERVIEW")
    
    total_events, total_venues_db, last_sync = db.get_stats()
    this_week = db.get_events_this_week()
    next_month = db.get_events_next_month()
    active_venues, total_venue_urls = db.get_active_venues_count()
    
    # Calculate next sync
    settings = db.get_all_settings()
    freq = settings.get("schedule_frequency", "weekly")
    day = settings.get("schedule_day", "monday")
    time_str = settings.get("schedule_time", "06:00")
    
    # Format last sync as relative time
    if last_sync:
        try:
            last_dt = datetime.strptime(last_sync, "%Y-%m-%d %H:%M:%S")
            delta = datetime.now() - last_dt
            if delta.days > 0:
                last_sync_display = f"{delta.days} days ago"
            elif delta.seconds // 3600 > 0:
                last_sync_display = f"{delta.seconds // 3600} hours ago"
            else:
                last_sync_display = f"{delta.seconds // 60} min ago"
        except:
            last_sync_display = last_sync
    else:
        last_sync_display = "Never"
    
    # Next sync display
    next_sync_display = f"{day.capitalize()} {time_str}" if freq == "weekly" else f"Daily {time_str}"
    
    # 6 metrics in 2 rows
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Events", total_events)
    col2.metric("Active Venues", f"{active_venues}/{total_venue_urls}")
    col3.metric("This Week", this_week)
    
    col4, col5, col6 = st.columns(3)
    col4.metric("Next Month", next_month)
    col5.metric("Last Sync", last_sync_display)
    col6.metric("Next Sync", next_sync_display)
    
    # --- SCRAPE FAILURES NOTIFICATION ---
    active_failures = db.get_active_failures()
    if active_failures:
        st.markdown("---")
        st.error(f"⚠️ **{len(active_failures)} URL(s) failed during scraping**")
        with st.expander("🔴 View Failure Details", expanded=True):
            for failure in active_failures:
                # Format the timestamp
                try:
                    occurred_dt = datetime.strptime(failure['occurred_at'], "%Y-%m-%d %H:%M:%S")
                    time_ago = datetime.now() - occurred_dt
                    if time_ago.days > 0:
                        time_display = f"{time_ago.days} days ago"
                    elif time_ago.seconds // 3600 > 0:
                        time_display = f"{time_ago.seconds // 3600} hours ago"
                    else:
                        time_display = f"{time_ago.seconds // 60} minutes ago"
                except:
                    time_display = failure['occurred_at']
                
                # Display failure details
                error_type_emoji = {
                    'HTTP_403': '🚫',
                    'HTTP_404': '❓',
                    'HTTP_SERVER_ERROR': '💥',
                    'TIMEOUT': '⏱️',
                    'CONNECTION_ERROR': '🔌',
                    'UNKNOWN': '❌'
                }.get(failure.get('error_type', 'UNKNOWN'), '❌')
                
                st.markdown(f"""
                **{error_type_emoji} {failure['url_name'] or 'Unknown Site'}**  
                📍 `{failure['url']}`  
                ⚠️ {failure['error_message'][:200]}{'...' if len(failure['error_message']) > 200 else ''}  
                🕐 Failed: {time_display} | Type: `{failure.get('error_type', 'UNKNOWN')}`
                """)
                st.markdown("---")
            
            st.caption("💡 These failures will be automatically cleared when the URLs are successfully scraped again.")
    
    # --- ACTIONS SECTION ---
    st.markdown("---")
    st.subheader("🎬 ACTIONS")
    action_col1, action_col2, action_col3, action_col4 = st.columns([1, 0.5, 1, 1])
    
    if 'log_buffer' not in st.session_state:
        st.session_state.log_buffer = ""
    
    if 'confirm_clear_events' not in st.session_state:
        st.session_state.confirm_clear_events = False
    
    with action_col1:
        scrape_btn = st.button("🚀 Scrape Now", use_container_width=True)
    
    with action_col2:
        scrape_days = st.number_input("Days", min_value=1, max_value=90, value=30, help="Number of days to scrape", label_visibility="collapsed")
    
    if scrape_btn:
        st.session_state.log_buffer = f"Starting parallel scrape for {scrape_days} days...\n"
        # Log that we started a scrape (will be updated with final result)
        db.add_log("Manual", "Started", 0, 0, [f"Started scrape for {scrape_days} days"])
        with st.spinner("Scraping all venues... check the Logs tab for progress."):
            try:
                import subprocess
                import sys
                
                env = get_subprocess_env()
                
                # Use subprocess.run with --days argument
                result = subprocess.run(
                    [VENV_PYTHON, RUN_PARALLEL_FILE, "--days", str(scrape_days)],
                    cwd=os.getcwd(),
                    capture_output=True,
                    text=True,
                    timeout=2700,  # 45 minutes timeout
                    env=env
                )
                
                # Store output for parsing
                st.session_state.log_buffer = result.stdout
                if result.stderr:
                    st.session_state.log_buffer += "\n--- STDERR ---\n" + result.stderr
                
                if result.returncode == 0:
                    # Parse results from log buffer
                    match = re.search(r'Scraping complete: (\d+) events, (\d+) failures', result.stdout)
                    if match:
                        events_count = int(match.group(1))
                        failures = int(match.group(2))
                        status = "Warn" if failures > 0 else "OK"
                        db.add_log("Manual", status, events_count, failures, None)
                        st.success(f"✅ Scrape completed! {events_count} events, {failures} failures")
                    else:
                        # Log anyway even if we can't parse the output
                        db.add_log("Manual", "OK", 0, 0, ["Scrape completed but could not parse output"])
                        st.success("✅ Scrape completed (output parsing issue)")
                    # Reset clear events confirmation after successful scrape
                    st.session_state.confirm_clear_events = False
                    st.rerun()  # Refresh to show new counts in metrics
                else:
                    error_msg = f"Return code: {result.returncode}\nSTDERR: {result.stderr}"
                    db.add_log("Manual", "Error", 0, 1, [error_msg])
                    st.error(f"❌ Scraping failed. Return code: {result.returncode}")
                    st.text_area("Error Details", result.stderr, height=200)
                    
            except subprocess.TimeoutExpired:
                error_msg = "Scraping timed out after 45 minutes"
                db.add_log("Manual", "Error", 0, 1, [error_msg])
                st.error("❌ " + error_msg)
            except Exception as e:
                error_msg = f"Subprocess failed: {str(e)}"
                st.warning("⚠️ Subprocess method failed, trying direct import...")
                st.write(f"Error: {e}")
                
                # Try fallback method
                try:
                    st.write("Attempting direct scraping...")
                    result = scrape_directly()
                    
                    if result["events"] > 0:
                        status = "Warn" if result["failures"] > 0 else "OK"
                        db.add_log("Manual", status, result["events"], result["failures"], result.get("warnings"))
                        st.success(f"✅ Direct scrape completed! {result['events']} events found.")
                        if result["failures"] > 0:
                            st.warning(f"⚠️ {result['failures']} failures occurred")
                        st.rerun()
                    else:
                        db.add_log("Manual", "Error", 0, 1, result.get("warnings", ["Direct scraping failed"]))
                        st.error("❌ Direct scraping also failed")
                        
                except Exception as fallback_error:
                    final_error = f"Both methods failed. Subprocess: {str(e)}, Direct: {str(fallback_error)}"
                    db.add_log("Manual", "Error", 0, 1, [final_error])
                    st.error("❌ Both scraping methods failed")
                    st.text_area("Final Error", final_error, height=200)
    
    with action_col3:
        events = db.get_all_events()
        if events:
            df_export = pd.DataFrame(events)
            
            # Use target_group_normalized values in the target_group column
            if 'target_group_normalized' in df_export.columns:
                df_export['target_group'] = df_export['target_group_normalized'].fillna('all_ages')
            
            # Exclude internal database columns from export
            columns_to_exclude = [
                'id', 'target_group_normalized', 'deletion_status', 'deleted_at', 
                'deleted_by_run_type', 'deleted_window_days', 'missing_count'
            ]
            df_export = df_export.drop(columns=[col for col in columns_to_exclude if col in df_export.columns])
            csv = df_export.to_csv(index=False).encode('utf-8')
            st.download_button("📁 Export Excel", csv, "events.csv", "text/csv", use_container_width=True)
        else:
            st.button("📁 Export Excel", disabled=True, use_container_width=True)
    
    with action_col4:
        if st.button("🗑️ Clear Events", use_container_width=True):
            # Show confirmation dialog
            if st.session_state.get('confirm_clear_events', False):
                # User confirmed, proceed with clearing
                try:
                    deleted_count = db.delete_all_events()
                    st.success(f"✅ Successfully cleared {deleted_count} events from database!")
                    # Reset confirmation state after successful clear
                    st.session_state.confirm_clear_events = False
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error clearing events: {str(e)}")
            else:
                # First click - show confirmation
                st.session_state.confirm_clear_events = True
                st.warning("⚠️ Are you sure? Click 'Clear Events' again to confirm.")
                st.rerun()
    
    # --- FILTERS SECTION ---
    st.markdown("---")
    st.subheader("🔍 FILTERS")
    
    # Initialize session state for pagination
    if 'page' not in st.session_state:
        st.session_state.page = 1
    
    if 'use_calendar_filter' not in st.session_state:
        st.session_state.use_calendar_filter = False
    
    # Row 1: Search, Venue, Source Website, Date Range
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    
    with filter_col1:
        search = st.text_input("Search", placeholder="e.g. Workshop")
    
    with filter_col2:
        venues = ["All Venues"] + db.get_unique_venues()
        venue = st.selectbox("Venue", venues)
    
    with filter_col3:
        sources = ["All Sources"] + db.get_unique_sources()
        source = st.selectbox("Source Website", sources)
    
    with filter_col4:
        filter_mode = st.selectbox("Filter By Date", ["Date Range", "Specific Date"])
    
    # Row 2: Date filters
    if filter_mode == "Date Range":
        date_range = st.selectbox("Date Range", ["Next 30 Days", "This Week", "All Time"], key="date_range_select")
        filter_date = None
    else:
        date_range = "All Time"  # Not used in specific date mode
        filter_date = st.date_input("Select Date", value=datetime.now(), key="filter_specific_date")
    
    target_groups = st.multiselect("Target Group", options=["All", "Children", "Adults", "Families"], 
                                    default=["All"])
    
    # --- EVENTS TABLE ---
    st.markdown("---")
    
    per_page = 20
    
    events_filtered, total_count = db.get_events_filtered(
        search=search, venue=venue, date_range=date_range, 
        target_groups=target_groups, source=source, page=st.session_state.page, per_page=per_page,
        filter_date=filter_date.strftime("%Y-%m-%d") if filter_date else None
    )
    
    # Calculate total pages based on total expanded count
    total_pages = math.ceil(total_count / per_page) if total_count > 0 else 1
    
    # Group events by name and date to combine multiple time slots
    grouped_events = {}
    for event in events_filtered:
        event_key = (event['event_name'], event['date_iso'])
        if event_key not in grouped_events:
            grouped_events[event_key] = {
                'event_name': event['event_name'],
                'date_iso': event['date_iso'],
                'location': event['location'],
                'target_group': event.get('target_group_normalized') or event.get('target_group'),
                'description': event['description'],
                'booking_info': event['booking_info'],
                'event_url': event['event_url'],
                'status': event['status'],
                'times': [],
                'urls': []
            }
        
        # Add time and URL if not already present
        if event['time'] and event['time'] not in grouped_events[event_key]['times']:
            grouped_events[event_key]['times'].append(event['time'])
        if event['event_url'] and event['event_url'] not in grouped_events[event_key]['urls']:
            grouped_events[event_key]['urls'].append(event['event_url'])
    
    # Convert grouped events back to list for display
    events_display = list(grouped_events.values())
    
    # Count cancelled events in current view (handle None status values)
    cancelled_count = sum(1 for e in events_display if (e.get('status') or 'scheduled').lower() == 'cancelled')
    
    # Display events section header with cancelled count
    events_header = "📋 EVENTS"
    if cancelled_count > 0:
        events_header += f" (⚠️ {cancelled_count} cancelled)"
    
    st.markdown("---")
    st.subheader(events_header)
    
    # Custom CSS for card styling
    st.markdown("""
    <style>
    .stExpander {
        border: none !important;
        background: transparent !important;
    }
    div[data-testid="stExpander"] > details {
        border: none !important;
        background: transparent !important;
    }
    div[data-testid="stExpander"] > details > summary {
        padding: 0 !important;
        font-size: 0.85rem;
        color: #8b5cf6;
    }
    .age-badge {
        background: linear-gradient(135deg, #6366f1, #8b5cf6);
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        display: inline-block;
    }
    </style>
    """, unsafe_allow_html=True)
    
    if events_display:
        for idx, event in enumerate(events_display):
            # Format date for display
            try:
                date_obj = datetime.strptime(event['date_iso'], "%Y-%m-%d")
                display_date = date_obj.strftime("%d %b %Y")
            except:
                display_date = event['date_iso'] or "Date TBA"
            
            location = event['location'] or "Location TBA"
            age_group = (event.get('target_group_normalized') or event.get('target_group') or "all_ages").replace("_", " ").title()
            description = event['description'] or "No description available."
            
            # Format multiple times separated by comma
            if event['times']:
                time_display = ", ".join(sorted(event['times']))
            else:
                time_display = "Time TBA"
                
            booking_display = event['booking_info'] or "Booking TBA"
            event_url = event['urls'][0] if event['urls'] else '#'  # Use first URL
            event_status = (event.get('status') or 'scheduled').lower()
            is_cancelled = event_status == 'cancelled'
            
            # Create card container with special styling for cancelled events
            if is_cancelled:
                # Cancelled event styling - greyed out with red accent
                st.markdown(f"""
                <div style="border: 2px solid #ff4444; border-radius: 8px; padding: 15px; background-color: #fff5f5; opacity: 0.85;">
                    <div style="position: relative;">
                        <span style="position: absolute; top: -10px; right: 10px; background-color: #ff4444; color: white; padding: 4px 12px; border-radius: 20px; font-weight: bold; font-size: 12px;">CANCELLED</span>
                        <h3 style="color: #666; text-decoration: line-through;">{event['event_name']}</h3>
                        <div style="display: flex; gap: 20px; margin: 10px 0; color: #999; font-size: 13px;">
                            <span>📅 {display_date}</span>
                            <span>📍 {location}</span>
                            <span>👥 {age_group}</span>
                        </div>
                        <p style="color: #999; font-size: 13px; margin: 10px 0;">{description[:150]}{"..." if len(description) > 150 else ""}</p>
                        <div style="display: flex; gap: 20px; font-size: 13px; color: #999;">
                            <span>⏰ {time_display}</span>
                            <span>🎟️ {booking_display}</span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                # Normal event card
                with st.container(border=True):
                    # Title
                    st.markdown(f"### {event['event_name']}")
                    
                    # Metadata row
                    meta_cols = st.columns([2, 2, 1.5])
                    with meta_cols[0]:
                        st.caption(f"📅 {display_date}")
                    with meta_cols[1]:
                        st.caption(f"📍 {location}")
                    with meta_cols[2]:
                        st.markdown(f'<span class="age-badge">👥 {age_group}</span>', unsafe_allow_html=True)
                    
                    # Description with read more
                    if len(description) > 150:
                        st.write(description[:150] + "...")
                        with st.expander("Read more"):
                            st.write(description)
                    else:
                        st.write(description)
                    
                    # Footer row with time, booking, and action buttons
                    footer_cols = st.columns([1, 1, 0.7, 0.7, 1])
                    with footer_cols[0]:
                        st.caption(f"⏰ {time_display}")
                    with footer_cols[1]:
                        st.caption(f"🎟️ {booking_display}")
                    with footer_cols[2]:
                        # Accept button - event stays (just visual confirmation)
                        if st.button("✓ Accept", key=f"accept_{idx}_{event['event_name'][:20]}_{event['date_iso']}", type="primary", use_container_width=True):
                            st.toast(f"✅ Event '{event['event_name']}' accepted!", icon="✅")
                    with footer_cols[3]:
                        # Reject button - delete event
                        if st.button("✗ Reject", key=f"reject_{idx}_{event['event_name'][:20]}_{event['date_iso']}", type="secondary", use_container_width=True):
                            # Delete the event from database
                            deleted = db.delete_event(event['event_name'], event['date_iso'], event_url)
                            if deleted:
                                st.toast(f"🗑️ Event '{event['event_name']}' rejected and removed!", icon="🗑️")
                                st.rerun()
                            else:
                                st.error("Failed to delete event")
                    with footer_cols[4]:
                        st.link_button("View Event →", event_url, use_container_width=True)
    else:
        st.info("No events found matching your filters.")
    
    # --- PAGINATION ---
    grouped_count = len(events_display)
    st.caption(f"Showing {grouped_count} event{(grouped_count != 1) and 's' or ''} (grouped from {total_count} total instances)")
    
    pag_col1, pag_col2, pag_col3 = st.columns([1, 3, 1])
    with pag_col1:
        if st.button("◀ Prev", disabled=st.session_state.page <= 1):
            st.session_state.page -= 1
            st.rerun()
    with pag_col3:
        if st.button("Next ▶", disabled=st.session_state.page >= total_pages):
            st.session_state.page += 1
            st.rerun()

# =============================================================================
# TAB 2: SETTINGS
# =============================================================================
with tabs[1]:
    settings = db.get_all_settings()
    
    # --- TWO-SCRAPER SYSTEM ---
    st.markdown("---")
    st.subheader("⏰ SCRAPING SCHEDULE")
    st.markdown("The system uses two scrapers to maintain accurate event data:")
    
    # --- 1️⃣ MONTHLY BASELINE SCRAPER ---
    st.markdown("---")
    st.markdown("### 1️⃣ Monthly Baseline Scraper")
    st.info("""
    **Purpose:** Source of truth for the database.
    
    - Runs on the **1st day of every month** at 06:00
    - Scrapes: **Current month + 5 days into next month**
    - **Direct insert** to database (no review needed)
    """)
    
    # Get current baseline settings
    scheduler_settings = db.get_all_scheduler_settings()
    current_baseline_date = db.get_baseline_start_date()
    last_baseline_run = scheduler_settings.get("last_baseline_run", {}).get("value", "Never")
    
    # Display last run time
    if last_baseline_run and last_baseline_run != "Never":
        try:
            last_dt = datetime.fromisoformat(last_baseline_run)
            last_baseline_display = last_dt.strftime("%b %d, %Y at %H:%M")
        except:
            last_baseline_display = last_baseline_run
    else:
        last_baseline_display = "Never"
    
    st.caption(f"**Last baseline run:** {last_baseline_display}")
    
    # Calculate next scheduled run (1st of next month)
    today = datetime.now()
    if today.day == 1 and today.hour < 6:
        next_baseline = today.replace(hour=6, minute=0, second=0)
    else:
        if today.month == 12:
            next_baseline = datetime(today.year + 1, 1, 1, 6, 0, 0)
        else:
            next_baseline = datetime(today.year, today.month + 1, 1, 6, 0, 0)
    st.caption(f"**Next scheduled run:** {next_baseline.strftime('%A, %b %d, %Y at %H:%M')}")
    
    # Custom baseline start date picker
    st.markdown("**Custom Start Date** (optional)")
    st.caption("Set a custom date to trigger an immediate baseline run. Leave empty for scheduled runs only.")
    
    baseline_col1, baseline_col2 = st.columns([2, 1])
    
    with baseline_col1:
        # Parse current baseline date or use None
        if current_baseline_date:
            try:
                default_date = datetime.strptime(current_baseline_date, "%Y-%m-%d").date()
            except:
                default_date = None
        else:
            default_date = None
        
        new_baseline_date = st.date_input(
            "Baseline Start Date",
            value=default_date,
            help="Select a date to trigger baseline scraper. Past dates will show a warning.",
            key="baseline_date_input"
        )
    
    with baseline_col2:
        if st.button("🚀 Run Baseline Now", use_container_width=True):
            with st.spinner("🌐 Triggering Cloud Run baseline scraper..."):
                try:
                    result = trigger_cloud_run_scraper("baseline", timeout=1800)  # 30 min timeout
                    
                    if result["success"]:
                        data = result.get("data", {})
                        events_count = data.get("events_scraped", 0)
                        failures = data.get("failures", 0)
                        status = "Warn" if failures > 0 else "OK"
                        db.add_log("Baseline", status, events_count, failures, None)
                        st.success(f"✅ Baseline scrape completed! {events_count} events added to database.")
                        st.rerun()
                    else:
                        db.add_log("Baseline", "Error", 0, 1, [result["message"][:500]])
                        st.error(f"❌ Baseline scrape failed: {result['message']}")
                except Exception as e:
                    db.add_log("Baseline", "Error", 0, 1, [str(e)])
                    st.error(f"❌ Baseline scrape failed: {str(e)}")
    
    # Handle date change with warning for past dates
    if new_baseline_date:
        new_date_str = new_baseline_date.strftime("%Y-%m-%d")
        
        # Check if past date
        if new_baseline_date < datetime.now().date():
            st.warning("⚠️ **Past date selected** - Baseline will run for the historical period starting from this date.")
        
        # Check if date changed
        if new_date_str != current_baseline_date:
            st.info(f"📅 Date changed from `{current_baseline_date or 'None'}` to `{new_date_str}`")
            
            if st.button("🔄 Apply & Run Baseline Immediately", type="primary"):
                # Save the new date
                date_changed = db.set_baseline_start_date(new_date_str)
                
                if date_changed:
                    st.warning("⚡ Triggering immediate baseline run due to date change...")
                    # Trigger baseline run with the NEW custom date
                    with st.spinner("Running baseline scraper..."):
                        try:
                            env = get_subprocess_env()
                            # Pass the custom start date to run_parallel.py
                            cmd = [VENV_PYTHON, RUN_PARALLEL_FILE, "--run-type", "baseline", "--start-date", new_date_str]
                            result = subprocess.run(
                                cmd,
                                cwd=os.getcwd(),
                                capture_output=True,
                                text=True,
                                timeout=2700,
                                env=env
                            )
                            
                            if result.returncode == 0:
                                st.success("✅ Baseline scrape triggered successfully!")
                                st.rerun()
                            else:
                                st.error(f"❌ Baseline scrape failed: {result.stderr[:200] if result.stderr else 'Unknown error'}")
                        except Exception as e:
                            st.error(f"❌ Failed to run baseline: {str(e)}")
    
    # --- 2️⃣ INCREMENTAL SCRAPER ---
    st.markdown("---")
    st.markdown("### 2️⃣ Incremental Scraper (Every 3 Days)")
    st.info("""
    **Purpose:** Detect changes (new events, deleted events) between baseline runs.
    
    - Runs **every 3 days** at 06:00
    - Scrapes: **Next 5-6 days** from execution date
    - **Stages changes for review** (see Updates tab)
    - Compares against DB using unique event key
    """)
    
    # Get last incremental run
    last_incremental_run = scheduler_settings.get("last_incremental_run", {}).get("value", "Never")
    if last_incremental_run and last_incremental_run != "Never":
        try:
            last_dt = datetime.fromisoformat(last_incremental_run)
            last_incremental_display = last_dt.strftime("%b %d, %Y at %H:%M")
        except:
            last_incremental_display = last_incremental_run
    else:
        last_incremental_display = "Never"
    
    st.caption(f"**Last incremental run:** {last_incremental_display}")
    
    # Calculate next incremental run (every 3 days)
    st.caption(f"**Schedule:** Every 3 days at 06:00")
    
    # Get pending changes count for display
    pending_counts = db.get_pending_counts()
    if pending_counts["insert"] > 0 or pending_counts["delete"] > 0:
        st.warning(f"⚠️ **Pending changes:** {pending_counts['insert']} inserts, {pending_counts['delete']} deletes → Go to **Updates** tab to review")
    
    incremental_col1, incremental_col2 = st.columns([2, 1])
    
    with incremental_col2:
        if st.button("🔄 Run Incremental Now", use_container_width=True):
            with st.spinner("🌐 Triggering Cloud Run incremental scraper..."):
                try:
                    result = trigger_cloud_run_scraper("incremental", timeout=600)  # 10 min timeout
                    
                    if result["success"]:
                        data = result.get("data", {})
                        events_count = data.get("events_scraped", 0)
                        failures = data.get("failures", 0)
                        staged_ins = data.get("staged_inserts", 0)
                        staged_del = data.get("staged_deletes", 0)
                        
                        status = "Warn" if failures > 0 else "OK"
                        db.add_log("Incremental", status, events_count, failures, None)
                        
                        st.success(f"✅ Incremental scrape completed!")
                        if staged_ins > 0 or staged_del > 0:
                            st.info(f"📋 Staged **{staged_ins}** inserts, **{staged_del}** deletes for review")
                            st.warning("👉 Go to the **Updates** tab to review and apply changes")
                        st.rerun()
                    else:
                        db.add_log("Incremental", "Error", 0, 1, [result["message"][:500]])
                        st.error(f"❌ Incremental scrape failed: {result['message']}")
                except Exception as e:
                    db.add_log("Incremental", "Error", 0, 1, [str(e)])
                    st.error(f"❌ Incremental scrape failed: {str(e)}")
    
    # --- ACTIVE VENUES ---
    st.markdown("---")
    st.subheader("🏛️ ACTIVE VENUES")
    
    # Add New Venue
    with st.expander("➕ Add New Venue", expanded=False):
        with st.form("add_venue_form"):
            new_venue_name = st.text_input("Venue Name", placeholder="e.g. My New Venue")
            new_venue_url = st.text_input("Venue URL", placeholder="https://example.com/events")
            submitted = st.form_submit_button("Add Venue")
            
            if submitted:
                if new_venue_name and new_venue_url:
                    if db.add_scraping_url(new_venue_url, new_venue_name):
                        st.success(f"Added {new_venue_name}!")
                        st.rerun()
                    else:
                        st.error("Failed to add venue. URL might already exist.")
                else:
                    st.warning("Please provide both name and URL.")

    # List Venues
    scraping_urls = db.get_scraping_urls()
    
    if scraping_urls:
        st.write("##### Configured Venues")
        
        # Header
        col1, col2, col3, col4 = st.columns([0.5, 2, 3, 0.5])
        col1.markdown("**Run**")
        col2.markdown("**Name**")
        col3.markdown("**URL**")
        col4.markdown("**Del**")
        
        for url_data in scraping_urls:
            c1, c2, c3, c4 = st.columns([0.5, 2, 3, 0.5])
            
            # Enable/Disable Checkbox
            is_enabled = c1.checkbox("Enable", value=url_data["enabled"], key=f"enable_{url_data['id']}", label_visibility="collapsed")
            
            # Name
            c2.write(url_data["name"])
            
            # URL (clickable link)
            c3.markdown(f"[{url_data['url']}]({url_data['url']})")
            
            # Delete Button
            if c4.button("🗑️", key=f"del_{url_data['id']}", help="Delete this venue"):
                db.delete_scraping_url(url_data["id"])
                st.rerun()
            
            # Auto-save toggle changes
            if is_enabled != url_data["enabled"]:
                db.toggle_url(url_data["id"], is_enabled)
                st.rerun()
    else:
        st.info("No venues configured. Add one above!")


# =============================================================================
# TAB 3: LOGS
# =============================================================================
with tabs[2]:
    st.markdown("---")
    st.subheader("📜 Scraping Logs")
    
    log_col1, log_col2 = st.columns([2, 1])
    with log_col1:
        st.caption("Showing last 30 days")
    with log_col2:
        log_filter = st.selectbox("Filter by", ["All", "OK", "Warn", "Error"], label_visibility="collapsed")
    
    logs = db.get_logs(days=30, status_filter=log_filter)
    
    if logs:
        # Create log table
        log_data = []
        for log in logs:
            timestamp = log["timestamp"]
            try:
                # Parse timestamp and convert to IST (UTC+5:30)
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                # Assume timestamp is in UTC, convert to IST
                from datetime import timezone, timedelta as td
                ist_offset = td(hours=5, minutes=30)
                dt_utc = dt.replace(tzinfo=timezone.utc)
                dt_ist = dt_utc.astimezone(timezone(ist_offset))
                date_display = dt_ist.strftime("%b %d %H:%M")
            except:
                date_display = timestamp
            
            status_icon = "✅" if log["status"] == "OK" else "⚠️" if log["status"] == "Warn" else "❌"
            events_display = f"{log['events_found']}"
            if log["failures"] > 0:
                events_display += f" ({log['failures']} fail)"
            
            log_data.append({
                "Date/Time": date_display,
                "Type": log["type"],
                "Status": f"{status_icon} {log['status']}",
                "Events Found": events_display
            })
        
        st.dataframe(pd.DataFrame(log_data), width='stretch', hide_index=True)
        
        # Show expandable warnings
        for log in logs:
            if log.get("warnings") and log["status"] != "OK":
                try:
                    # Convert to IST for warning display as well
                    dt = datetime.strptime(log["timestamp"], "%Y-%m-%d %H:%M:%S")
                    from datetime import timezone, timedelta as td
                    ist_offset = td(hours=5, minutes=30)
                    dt_utc = dt.replace(tzinfo=timezone.utc)
                    dt_ist = dt_utc.astimezone(timezone(ist_offset))
                    date_display = dt_ist.strftime("%b %d, %Y")
                except:
                    date_display = log["timestamp"]
                
                with st.expander(f"⚠️ {date_display} - Warnings"):
                    for warning in log["warnings"]:
                        st.write(f"└ {warning}")
    else:
        st.info("No logs found for the selected period.")
    
    # Action buttons
    st.markdown("---")
    log_btn_col1, log_btn_col2 = st.columns(2)
    with log_btn_col1:
        if logs:
            log_df = pd.DataFrame(log_data)
            csv = log_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Export Logs", csv, "scraping_logs.csv", width='stretch')
    with log_btn_col2:
        if st.button("🗑️ Clear Old Logs", width='stretch'):
            deleted = db.clear_old_logs(90)
            st.success(f"Cleared {deleted} old log entries.")
            st.rerun()

# =============================================================================
# TAB 4: SELECTORS
# =============================================================================
with tabs[3]:
    st.markdown("---")
    st.subheader("🎯 SELECTOR MANAGEMENT")
    st.markdown("Manage CSS selectors for event extraction without running the spider.")
    
    # Get all selector configurations from database
    try:
        all_selectors = db.get_all_selector_configs()
    except Exception as e:
        st.error(f"Error loading selectors: {str(e)}")
        all_selectors = []
    
    if not all_selectors:
        st.info("No selector configurations found. Selectors will be auto-saved when scraping new URLs.")
    else:
        # Display existing selectors
        st.markdown("---")
        st.subheader(f"📋 CONFIGURED SELECTORS ({len(all_selectors)} URLs)")
        
        # Create tabs for each selector or a single view with columns
        for idx, selector_config in enumerate(all_selectors):
            domain = selector_config.get('domain', 'Unknown')
            url_pattern = selector_config.get('url_pattern', '')
            container_sel = selector_config.get('container_selector', '')
            item_sels_json = selector_config.get('item_selectors_json', '{}')
            last_updated = selector_config.get('last_updated', 'Unknown')
            
            # Parse item selectors
            try:
                item_sels = json.loads(item_sels_json) if isinstance(item_sels_json, str) else item_sels_json
            except Exception as e:
                st.error(f"Error parsing selectors: {e}")
                item_sels = {}
            
            # Create collapsible card for each selector config
            with st.expander(f"🌐 {domain} — {url_pattern or '/'}", expanded=False):
                # Display current selectors in a table format
                st.markdown("**Current Selectors:**")
                
                # Create a more readable display for selectors
                if item_sels:
                    # Display Container selector first
                    st.markdown(f"**Container:** `{container_sel}`")
                    
                    # Display each field selector with better formatting
                    st.markdown("**Field Selectors:**")
                    for field_name, field_selector in item_sels.items():
                        # Format field name for display (replace underscores with spaces and title case)
                        display_name = field_name.replace('_', ' ').title()
                        st.markdown(f"• **{display_name}**: `{field_selector}`")
                else:
                    st.markdown(f"**Container:** `{container_sel}`")
                    st.info("No item selectors configured yet.")
                
                st.caption(f"Last updated: {last_updated}")
                
                # Edit button
                st.markdown("**Edit Selectors:**")
                
                edit_col1, edit_col2 = st.columns(2)
                
                with edit_col1:
                    if st.button(f"✏️ Edit Field-by-Field", key=f"edit_fields_{idx}"):
                        st.session_state[f"editing_{idx}"] = "fields"
                
                with edit_col2:
                    if st.button(f"📝 Edit as JSON", key=f"edit_json_{idx}"):
                        st.session_state[f"editing_{idx}"] = "json"
                
                # Show edit interface based on mode
                if st.session_state.get(f"editing_{idx}") == "fields":
                    st.markdown("**Field-by-Field Editor:**")
                    
                    # Create form for editing
                    with st.form(f"edit_form_fields_{idx}"):
                        new_container = st.text_input(
                            "Container Selector",
                            value=container_sel,
                            help="CSS selector for the container element holding events"
                        )
                        
                        # Create inputs for each item selector
                        new_items = {}
                        for field_name, field_selector in item_sels.items():
                            # Format field name for display (replace underscores with spaces and title case)
                            display_name = field_name.replace('_', ' ').title()
                            new_items[field_name] = st.text_input(
                                f"{display_name} Selector",
                                value=field_selector,
                                help=f"CSS selector for extracting {field_name}"
                            )
                        
                        # Form buttons
                        form_col1, form_col2 = st.columns(2)
                        
                        with form_col1:
                            save_changes = st.form_submit_button("💾 Save Changes")
                        
                        with form_col2:
                            if st.form_submit_button("❌ Cancel"):
                                st.session_state[f"editing_{idx}"] = None
                                st.rerun()
                        
                        if save_changes:
                            try:
                                # Save selectors to database
                                full_url = f"https://{domain}{url_pattern}" if url_pattern else f"https://{domain}"
                                db.save_selectors(full_url, new_container, new_items)
                                st.success("✅ Selectors saved successfully!")
                                st.session_state[f"editing_{idx}"] = None
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error saving selectors: {str(e)}")
                
                elif st.session_state.get(f"editing_{idx}") == "json":
                    st.markdown("**JSON Editor:**")
                    st.info("Edit the complete selector configuration as JSON. Paste updated config and save.")
                    
                    # Create JSON object for editing
                    edit_config = {
                        "container_selector": container_sel,
                        "item_selectors": item_sels
                    }
                    
                    with st.form(f"edit_form_json_{idx}"):
                        json_text = st.text_area(
                            "Selector Configuration (JSON)",
                            value=json.dumps(edit_config, indent=2),
                            height=200,
                            help="Edit the JSON configuration directly"
                        )
                        
                        # Form buttons
                        form_col1, form_col2 = st.columns(2)
                        
                        with form_col1:
                            save_json = st.form_submit_button("💾 Save Changes")
                        
                        with form_col2:
                            if st.form_submit_button("❌ Cancel"):
                                st.session_state[f"editing_{idx}"] = None
                                st.rerun()
                        
                        if save_json:
                            try:
                                # Parse JSON
                                parsed_config = json.loads(json_text)
                                new_container = parsed_config.get("container_selector", "")
                                new_items = parsed_config.get("item_selectors", {})
                                
                                # Validate JSON structure
                                if not new_container or not isinstance(new_items, dict):
                                    st.error("Invalid JSON structure. Must have 'container_selector' (string) and 'item_selectors' (object).")
                                else:
                                    # Save selectors to database
                                    full_url = f"https://{domain}{url_pattern}" if url_pattern else f"https://{domain}"
                                    db.save_selectors(full_url, new_container, new_items)
                                    st.success("✅ Selectors saved successfully!")
                                    st.session_state[f"editing_{idx}"] = None
                                    st.rerun()
                            except json.JSONDecodeError as e:
                                st.error(f"Invalid JSON: {str(e)}")
                            except Exception as e:
                                st.error(f"Error saving selectors: {str(e)}")
                
                # Delete button at the bottom
                st.markdown("---")
                if st.button(f"🗑️ Delete Selectors", key=f"delete_{idx}", help="Remove this selector configuration"):
                    try:
                        full_url = f"https://{domain}{url_pattern}" if url_pattern else f"https://{domain}"
                        db.delete_selector_config(full_url)
                        st.success(f"✅ Deleted selectors for {domain}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting selectors: {str(e)}")
    
    # Add new selector section
    st.markdown("---")
    st.subheader("➕ ADD NEW SELECTOR")
    
    with st.expander("Add Selectors for a New URL", expanded=False):
        st.info("Use this form to add selectors for a new URL without running the spider.")
        
        with st.form("add_selector_form"):
            new_url = st.text_input(
                "URL",
                placeholder="https://example.com/events",
                help="Full URL pattern for the website"
            )
            
            new_container = st.text_input(
                "Container Selector",
                placeholder=".event-item, article.event, div.event-card",
                help="CSS selector for the container element holding events"
            )
            
            st.markdown("**Item Selectors** (fields to extract from each event):")
            
            item_selector_fields = {
                "event_name": "Event Name",
                "date_iso": "Date",
                "time": "Time",
                "location": "Location",
                "description": "Description",
                "booking_info": "Booking Info",
                "target_group": "Target Group",
                "status": "Status",
                "image_url": "Image URL (img selector)"
            }
            
            new_items = {}
            for field_key, field_label in item_selector_fields.items():
                new_items[field_key] = st.text_input(
                    f"{field_label} Selector",
                    placeholder=f"CSS selector for {field_key}",
                    help=f"Selector to extract {field_key}"
                )
            
            # Submit button
            add_submit = st.form_submit_button("➕ Add Selector Configuration")
            
            if add_submit:
                # Validate inputs
                if not new_url or not new_container:
                    st.error("URL and Container Selector are required.")
                elif not any(new_items.values()):
                    st.error("At least one Item Selector is required.")
                else:
                    try:
                        # Filter out empty selectors
                        filtered_items = {k: v for k, v in new_items.items() if v}
                        
                        # Save to database
                        db.save_selectors(new_url, new_container, filtered_items)
                        st.success(f"✅ Selector configuration added for {new_url}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error adding selector: {str(e)}")

# =============================================================================
# TAB 5: UPDATES (Pending Changes from Incremental Scraper)
# =============================================================================
with tabs[4]:
    st.markdown("---")
    st.subheader("🔄 PENDING CHANGES")
    st.markdown("Review and apply changes detected by the **Incremental Scraper**.")
    
    # Get pending changes from staging table
    pending_changes = db.get_pending_changes()
    pending_inserts = pending_changes.get("insert", [])
    pending_deletes = pending_changes.get("delete", [])
    
    total_pending = len(pending_inserts) + len(pending_deletes)
    
    if total_pending == 0:
        st.info("✨ No pending changes. Run the **Incremental Scraper** from the Settings tab to detect changes.")
    else:
        # Get the most recent detection timestamp from pending changes
        all_changes = pending_inserts + pending_deletes
        if all_changes:
            latest_detected = max(c.get("detected_at", "") for c in all_changes if c.get("detected_at"))
            if latest_detected:
                try:
                    # Parse and format the timestamp nicely
                    from datetime import timezone, timedelta as td
                    dt = datetime.strptime(latest_detected, "%Y-%m-%d %H:%M:%S")
                    # Convert to IST (UTC+5:30)
                    ist_offset = td(hours=5, minutes=30)
                    dt_utc = dt.replace(tzinfo=timezone.utc)
                    dt_ist = dt_utc.astimezone(timezone(ist_offset))
                    formatted_date = dt_ist.strftime("%B %d, %Y at %H:%M")
                except:
                    formatted_date = latest_detected
                
                st.info(f"🕐 **Last Incremental Scrape:** {formatted_date}")
        
        # Summary metrics
        summary_col1, summary_col2, summary_col3 = st.columns(3)
        with summary_col1:
            st.metric("🆕 Events to Add", len(pending_inserts))
        with summary_col2:
            st.metric("🗑️ Events to Delete", len(pending_deletes))
        with summary_col3:
            st.metric("📊 Total Pending", total_pending)
        
        # Apply/Discard buttons
        st.markdown("---")
        action_col1, action_col2, action_col3 = st.columns([1, 1, 2])
        
        with action_col1:
            if st.button("✅ Apply All Changes", type="primary", use_container_width=True):
                if len(pending_deletes) > 0:
                    # Show confirmation for deletes
                    st.session_state.confirm_apply_changes = True
                else:
                    # No deletes, apply directly
                    result = db.apply_pending_changes()
                    st.success(f"✅ Applied changes: {result['inserted']} inserted, {result['deleted']} deleted")
                    st.rerun()
        
        with action_col2:
            if st.button("🗑️ Discard All", use_container_width=True):
                discarded = db.discard_pending_changes()
                st.info(f"Discarded {discarded} pending changes")
                st.rerun()
        
        # Confirmation dialog for deletions
        if st.session_state.get('confirm_apply_changes', False):
            st.warning(f"⚠️ **Confirmation Required**")
            st.error(f"This will **permanently delete {len(pending_deletes)} event(s)** from the database. This cannot be undone!")
            
            confirm_col1, confirm_col2 = st.columns(2)
            with confirm_col1:
                if st.button("✅ Yes, Apply All Changes", type="primary", use_container_width=True):
                    result = db.apply_pending_changes()
                    st.success(f"✅ Applied: {result['inserted']} inserted, {result['deleted']} deleted")
                    st.session_state.confirm_apply_changes = False
                    st.rerun()
            with confirm_col2:
                if st.button("❌ Cancel", use_container_width=True):
                    st.session_state.confirm_apply_changes = False
                    st.rerun()
    
    # --- EVENTS TO ADD (Green) ---
    if pending_inserts:
        st.markdown("---")
        st.subheader("🆕 EVENTS TO ADD")
        st.markdown("*These events were found in the scrape but don't exist in the database.*")
        
        # Group by source URL
        inserts_by_source = {}
        for change in pending_inserts:
            event_data = change.get("event_data", {})
            event_url = event_data.get("event_url", "")
            from urllib.parse import urlparse
            try:
                domain = urlparse(event_url).netloc.replace("www.", "")
            except:
                domain = "Unknown"
            if domain not in inserts_by_source:
                inserts_by_source[domain] = []
            inserts_by_source[domain].append(change)
        
        for source, changes in inserts_by_source.items():
            with st.expander(f"🌐 {source} ({len(changes)} new)", expanded=True):
                for change in changes:
                    event_data = change.get("event_data", {})
                    detected_at = change.get("detected_at", "")
                    
                    # Format detection date
                    detected_display = ""
                    if detected_at:
                        try:
                            from datetime import timezone, timedelta as td
                            dt = datetime.strptime(detected_at, "%Y-%m-%d %H:%M:%S")
                            ist_offset = td(hours=5, minutes=30)
                            dt_utc = dt.replace(tzinfo=timezone.utc)
                            dt_ist = dt_utc.astimezone(timezone(ist_offset))
                            detected_display = dt_ist.strftime("%b %d, %H:%M")
                        except:
                            detected_display = detected_at
                    
                    # Green-tinted card for inserts
                    st.markdown(f"""
                    <div style="border-left: 4px solid #22c55e; padding: 10px; margin: 10px 0; background-color: #f0fdf4; border-radius: 4px;">
                        <strong style="color: #166534;">{event_data.get('event_name', 'Unknown')}</strong>
                        {f'<span style="float: right; color: #3b82f6; font-size: 14px; font-weight: 600;">🕐 {detected_display}</span>' if detected_display else ''}
                    </div>
                    """, unsafe_allow_html=True)
                    
                    col1, col2, col3 = st.columns(3)
                    col1.caption(f"📅 {event_data.get('date_iso', 'N/A')}")
                    col2.caption(f"📍 {event_data.get('location', 'N/A')}")
                    col3.caption(f"⏰ {event_data.get('time', 'N/A')}")
                    
                    # Show description preview
                    desc = event_data.get('description', '')
                    if desc and len(desc) > 100:
                        st.caption(f"📝 {desc[:100]}...")
                    elif desc:
                        st.caption(f"📝 {desc}")
                    
                    # Individual discard button
                    if st.button(f"❌ Discard", key=f"discard_insert_{change['id']}", help="Remove this event from pending"):
                        db.discard_single_change(change['id'])
                        st.toast(f"Discarded: {event_data.get('event_name', 'Event')}")
                        st.rerun()
                    
                    st.markdown("---")
    
    # --- EVENTS TO DELETE (Red) ---
    if pending_deletes:
        st.markdown("---")
        st.subheader("🗑️ EVENTS TO DELETE")
        st.markdown("*These events exist in the database but were **not found** in the latest scrape.*")
        st.warning("⚠️ Applying these changes will **permanently remove** these events from the database.")
        
        # Group by source URL
        deletes_by_source = {}
        for change in pending_deletes:
            event_data = change.get("event_data", {})
            event_url = event_data.get("event_url", "")
            from urllib.parse import urlparse
            try:
                domain = urlparse(event_url).netloc.replace("www.", "")
            except:
                domain = "Unknown"
            if domain not in deletes_by_source:
                deletes_by_source[domain] = []
            deletes_by_source[domain].append(change)
        
        for source, changes in deletes_by_source.items():
            with st.expander(f"🌐 {source} ({len(changes)} to delete)", expanded=True):
                for change in changes:
                    event_data = change.get("event_data", {})
                    detected_at = change.get("detected_at", "")
                    
                    # Format detection date
                    detected_display = ""
                    if detected_at:
                        try:
                            from datetime import timezone, timedelta as td
                            dt = datetime.strptime(detected_at, "%Y-%m-%d %H:%M:%S")
                            ist_offset = td(hours=5, minutes=30)
                            dt_utc = dt.replace(tzinfo=timezone.utc)
                            dt_ist = dt_utc.astimezone(timezone(ist_offset))
                            detected_display = dt_ist.strftime("%b %d, %H:%M")
                        except:
                            detected_display = detected_at
                    
                    # Red-tinted card for deletes
                    st.markdown(f"""
                    <div style="border-left: 4px solid #ef4444; padding: 10px; margin: 10px 0; background-color: #fef2f2; border-radius: 4px;">
                        <strong style="color: #991b1b;">{change.get('event_name') or event_data.get('event_name', 'Unknown')}</strong>
                        {f'<span style="float: right; color: #3b82f6; font-size: 14px; font-weight: 600;">🕐 {detected_display}</span>' if detected_display else ''}
                    </div>
                    """, unsafe_allow_html=True)
                    
                    col1, col2, col3 = st.columns(3)
                    col1.caption(f"📅 {change.get('date_iso') or event_data.get('date_iso', 'N/A')}")
                    col2.caption(f"📍 {change.get('location') or event_data.get('location', 'N/A')}")
                    col3.caption(f"🔑 Key: {change.get('unique_key', 'N/A')[:16]}...")
                    
                    # Individual discard button (keeps event in DB)
                    if st.button(f"↩️ Keep Event", key=f"discard_delete_{change['id']}", help="Remove from pending (event stays in DB)"):
                        db.discard_single_change(change['id'])
                        st.toast(f"Kept: {change.get('event_name', 'Event')} - removed from deletion queue")
                        st.rerun()
                    
                    st.markdown("---")
    
    # --- RECENTLY ADDED EVENTS (for reference) ---
    st.markdown("---")
    st.subheader("📋 RECENTLY ADDED EVENTS")
    st.caption("Events added to the database today (from baseline scraper or applied changes)")
    
    added_events = db.get_added_events_today()
    
    if added_events:
        # Group by source URL
        added_by_source = {}
        for event in added_events:
            from urllib.parse import urlparse
            parsed = urlparse(event['event_url'])
            domain = parsed.netloc.replace("www.", "")
            if domain not in added_by_source:
                added_by_source[domain] = []
            added_by_source[domain].append(event)
        
        st.success(f"✅ **{len(added_events)}** event(s) added today")
        
        for source, events in added_by_source.items():
            with st.expander(f"🌐 {source} ({len(events)} new)", expanded=False):
                for event in events:
                    st.markdown(f"**{event['event_name']}**")
                    col1, col2, col3 = st.columns(3)
                    col1.caption(f"📅 {event['date_iso']}")
                    col2.caption(f"📍 {event.get('location', 'N/A')}")
                    col3.caption(f"⏰ {event.get('time', 'N/A')}")
                    st.markdown("---")
    else:
        st.info("No new events added today.")