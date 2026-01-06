import streamlit as st
import pandas as pd
import os
import sys
import math
import subprocess
import re
import json
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

sys.path.append(os.path.join(os.getcwd(), "event_category"))
from event_category.utils.db_manager import DatabaseManager

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
def run_scheduled_scrape():
    """Run scraping job and log results using subprocess for parallel execution."""
    import subprocess
    import re
    try:
        env = get_subprocess_env()
        result = subprocess.run(
            [VENV_PYTHON, RUN_PARALLEL_FILE],
            cwd=os.getcwd(),
            capture_output=True,
            text=True,
            timeout=1800,
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
            db.add_log("Auto", status, events_count, failures, warnings if warnings else None)
        else:
            db.add_log("Auto", "Warn", 0, 0, ["Could not parse scraping results"])
    except subprocess.TimeoutExpired:
        db.add_log("Auto", "Error", 0, 1, ["Scraping timed out after 30 minutes"])
    except Exception as e:
        db.add_log("Auto", "Error", 0, 1, [str(e)])

def setup_scheduler():
    """Initialize scheduler based on settings."""
    settings = db.get_all_settings()
    freq = settings.get("schedule_frequency", "weekly")
    
    scheduler = BackgroundScheduler()
    
    if freq == "custom":
        # Custom scheduling - convert to daily cron trigger at specific time
        custom_datetime_str = settings.get("schedule_datetime")
        if custom_datetime_str:
            try:
                custom_datetime = datetime.fromisoformat(custom_datetime_str)
                # Extract hour and minute from custom datetime
                hour = custom_datetime.hour
                minute = custom_datetime.minute
                # Use CronTrigger to run every day at the specified time (more reliable)
                scheduler.add_job(run_scheduled_scrape, CronTrigger(hour=hour, minute=minute))
                print(f"Scheduled custom scrape for every day at {hour:02d}:{minute:02d}")
            except Exception as e:
                print(f"Error parsing custom datetime: {e}")
                # Fallback to weekly if custom datetime is invalid
                freq = "weekly"
    
    if freq != "custom":
        # Weekly or Daily scheduling
        day = settings.get("schedule_day", "monday")
        time_str = settings.get("schedule_time", "06:00")
        hour, minute = map(int, time_str.split(":"))
        
        if freq == "daily":
            scheduler.add_job(run_scheduled_scrape, CronTrigger(hour=hour, minute=minute))
        else:  # weekly
            day_map = {"monday": "mon", "tuesday": "tue", "wednesday": "wed", "thursday": "thu",
                       "friday": "fri", "saturday": "sat", "sunday": "sun"}
            scheduler.add_job(run_scheduled_scrape, CronTrigger(day_of_week=day_map.get(day, "mon"), 
                                                                hour=hour, minute=minute))
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
tabs = st.tabs(["📊 Dashboard", "⚙️ Settings", "📝 Logs", "📈 Analytics", "🎯 Selectors"])

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
    
    # --- ACTIONS SECTION ---
    st.markdown("---")
    st.subheader("🎬 ACTIONS")
    action_col1, action_col2, action_col3 = st.columns([1, 1, 3])
    
    if 'log_buffer' not in st.session_state:
        st.session_state.log_buffer = ""
    
    with action_col1:
        if st.button("🚀 Scrape Now", width='stretch'):
            st.session_state.log_buffer = "Starting parallel scrape...\n"
            with st.spinner("Scraping all venues... check the Logs tab for progress."):
                try:
                    import subprocess
                    import sys
                    
                    env = get_subprocess_env()
                    
                    # Use subprocess.run instead of Popen for better error handling
                    result = subprocess.run(
                        [VENV_PYTHON, RUN_PARALLEL_FILE],
                        cwd=os.getcwd(),
                        capture_output=True,
                        text=True,
                        timeout=1800,  # 30 minutes timeout
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
                        st.success("✅ Scrape completed successfully!")
                        st.rerun()  # Refresh to show new counts in metrics
                    else:
                        error_msg = f"Return code: {result.returncode}\nSTDERR: {result.stderr}"
                        db.add_log("Manual", "Error", 0, 1, [error_msg])
                        st.error(f"❌ Scraping failed. Return code: {result.returncode}")
                        st.text_area("Error Details", result.stderr, height=200)
                        
                except subprocess.TimeoutExpired:
                    error_msg = "Scraping timed out after 30 minutes"
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
    
    with action_col2:
        events = db.get_all_events()
        if events:
            df_export = pd.DataFrame(events)
            # Exclude internal database columns from export
            columns_to_exclude = ['id', 'last_scraped']
            df_export = df_export.drop(columns=[col for col in columns_to_exclude if col in df_export.columns])
            csv = df_export.to_csv(index=False).encode('utf-8')
            st.download_button("📁 Export Excel", csv, "events.csv", "text/csv", use_container_width=True)
        else:
            st.button("📁 Export Excel", disabled=True, width='stretch')
    
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
                'target_group': event['target_group'],
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
    
    # Count cancelled events in current view
    cancelled_count = sum(1 for e in events_display if e.get('status', 'scheduled').lower() == 'cancelled')
    
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
            age_group = (event['target_group'] or "all_ages").replace("_", " ").title()
            description = event['description'] or "No description available."
            
            # Format multiple times separated by comma
            if event['times']:
                time_display = ", ".join(sorted(event['times']))
            else:
                time_display = "Time TBA"
                
            booking_display = event['booking_info'] or "Booking TBA"
            event_url = event['urls'][0] if event['urls'] else '#'  # Use first URL
            event_status = event.get('status', 'scheduled').lower()
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
    
    # --- SCRAPING SCHEDULE ---
    st.markdown("---")
    st.subheader("⏰ SCRAPING SCHEDULE")
    
    freq_options = ["weekly", "daily", "custom"]
    current_freq = settings.get("schedule_frequency", "weekly")
    frequency = st.radio("Frequency", freq_options, index=freq_options.index(current_freq), horizontal=True)
    
    # Show different options based on frequency
    if frequency == "custom":
        st.info("📅 Set a specific date and time for the next scheduled scrape")
        
        custom_col1, custom_col2 = st.columns(2)
        with custom_col1:
            custom_date = st.date_input("Select Date", value=datetime.now().date(), key="schedule_custom_date")
        with custom_col2:
            # Use text input for manual time entry in HH:MM format
            current_time_str = settings.get("schedule_time", "06:00")
            custom_time_str = st.text_input("Enter Time (HH:MM)", value=current_time_str, placeholder="e.g. 14:30")
            
            # Parse the time string
            try:
                time_parts = custom_time_str.split(":")
                custom_time = datetime.strptime(custom_time_str, "%H:%M").time()
            except:
                st.warning("⚠️ Invalid time format. Use HH:MM (e.g., 14:30)")
                custom_time = datetime.now().time()
        
        # Combine date and time
        custom_datetime = datetime.combine(custom_date, custom_time)
        st.caption(f"**Scheduled run:** {custom_datetime.strftime('%A, %b %d, %Y at %H:%M')}")
        
        schedule_day = None  # Not used for custom
        schedule_time = None  # Not used for custom
        
    elif frequency == "daily":
        st.info("⏰ Scraping will run every day at the specified time")
        
        # Only show time picker for daily
        current_time_str = settings.get("schedule_time", "06:00")
        try:
            current_time_obj = datetime.strptime(current_time_str, "%H:%M").time()
        except:
            current_time_obj = datetime.strptime("06:00", "%H:%M").time()
        
        schedule_time_input = st.time_input("Select Time", value=current_time_obj)
        schedule_time = f"{schedule_time_input.hour:02d}:{schedule_time_input.minute:02d}"
        
        # Display next scheduled run
        today = datetime.now()
        next_run = today.replace(hour=schedule_time_input.hour, minute=schedule_time_input.minute, second=0)
        if next_run <= today:
            next_run += timedelta(days=1)
        st.caption(f"**Next scheduled run:** {next_run.strftime('%A, %b %d, %Y at %H:%M')}")
        
        schedule_day = None  # Not used for daily
        custom_datetime = None
        
    else:  # weekly
        st.info("📆 Scraping will run on the selected day and time each week")
        
        sched_col1, sched_col2 = st.columns(2)
        
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        current_day = settings.get("schedule_day", "monday")
        with sched_col1:
            schedule_day = st.selectbox("Day", days, index=days.index(current_day))
        
        current_time_str = settings.get("schedule_time", "06:00")
        try:
            current_time_obj = datetime.strptime(current_time_str, "%H:%M").time()
        except:
            current_time_obj = datetime.strptime("06:00", "%H:%M").time()
        
        with sched_col2:
            schedule_time_input = st.time_input("Select Time", value=current_time_obj)
            schedule_time = f"{schedule_time_input.hour:02d}:{schedule_time_input.minute:02d}"
        
        # Calculate next scheduled run
        today = datetime.now()
        day_num = days.index(schedule_day)
        days_ahead = day_num - today.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        next_run_date = today + timedelta(days=days_ahead)
        next_run = next_run_date.replace(hour=schedule_time_input.hour, minute=schedule_time_input.minute, second=0)
        st.caption(f"**Next scheduled run:** {next_run.strftime('%A, %b %d, %Y at %H:%M')}")
        
        custom_datetime = None
    
    if st.button("💾 Save Schedule"):
        if frequency == "custom":
            # Validate custom datetime is in the future
            now = datetime.now()
            if custom_datetime <= now:
                st.error(f"❌ Error: Scheduled time must be in the future! Current time is {now.strftime('%Y-%m-%d %H:%M')}. Please select a time after this.")
            else:
                # Save custom datetime
                db.save_settings({
                    "schedule_frequency": frequency,
                    "schedule_datetime": custom_datetime.isoformat()
                })
                st.success(f"Schedule saved! Scraping will run at {custom_datetime.strftime('%A, %b %d, %Y at %H:%M')}")
                
                # Restart scheduler
                if st.session_state.scheduler:
                    st.session_state.scheduler.shutdown(wait=False)
                st.session_state.scheduler = setup_scheduler()
        else:
            # Save weekly/daily schedule
            db.save_settings({
                "schedule_frequency": frequency,
                "schedule_day": schedule_day,
                "schedule_time": schedule_time
            })
            st.success("Schedule saved!")
            
            # Restart scheduler
            if st.session_state.scheduler:
                st.session_state.scheduler.shutdown(wait=False)
            st.session_state.scheduler = setup_scheduler()
    
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
    
    # --- EVENT FILTERING ---
    st.markdown("---")
    st.subheader("🗓️ EVENT FILTERING")
    
    date_range_options = ["30", "45", "60", "90"]
    current_range = settings.get("date_range_days", "45")
    date_range_days = st.selectbox("Date Range (days from today)", date_range_options, 
                                    index=date_range_options.index(current_range) if current_range in date_range_options else 1)
    
    auto_delete = st.checkbox("Auto-delete old events", value=settings.get("auto_delete_enabled", "false") == "true")
    
    delete_options = ["30", "60", "90", "180"]
    current_delete = settings.get("auto_delete_days", "90")
    delete_days = st.selectbox("Delete events older than (days)", delete_options,
                               index=delete_options.index(current_delete) if current_delete in delete_options else 2,
                               disabled=not auto_delete)
    
    if st.button("💾 Save Filtering Settings"):
        db.save_settings({
            "date_range_days": date_range_days,
            "auto_delete_enabled": str(auto_delete).lower(),
            "auto_delete_days": delete_days
        })
        st.success("Filtering settings saved!")
    
    # --- NOTIFICATIONS ---
    st.markdown("---")
    st.subheader("📧 NOTIFICATIONS")
    
    email_enabled = st.checkbox("Email notifications", value=settings.get("email_enabled", "false") == "true")
    email_address = st.text_input("Email", value=settings.get("email_address", ""), 
                                   placeholder="admin@example.com", disabled=not email_enabled)
    
    st.write("Send email on:")
    notify_complete = st.checkbox("Scraping completed", value=settings.get("notify_on_complete", "true") == "true", 
                                   disabled=not email_enabled)
    notify_failure = st.checkbox("Scraping failed", value=settings.get("notify_on_failure", "true") == "true",
                                  disabled=not email_enabled)
    notify_summary = st.checkbox("Weekly summary", value=settings.get("notify_weekly_summary", "false") == "true",
                                  disabled=not email_enabled)
    
    notif_col1, notif_col2 = st.columns(2)
    with notif_col1:
        if st.button("📧 Test Email", disabled=not email_enabled):
            st.info("Email testing not yet implemented")
    with notif_col2:
        if st.button("💾 Save Notifications"):
            db.save_settings({
                "email_enabled": str(email_enabled).lower(),
                "email_address": email_address,
                "notify_on_complete": str(notify_complete).lower(),
                "notify_on_failure": str(notify_failure).lower(),
                "notify_weekly_summary": str(notify_summary).lower()
            })
            st.success("Notification settings saved!")

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
# TAB 4: ANALYTICS
# =============================================================================
with tabs[3]:
    # --- EVENTS BY VENUE ---
    st.markdown("---")
    st.subheader("🏛️ EVENTS BY VENUE")
    
    venue_data = db.get_events_by_venue()
    if venue_data:
        venue_df = pd.DataFrame(venue_data)
        st.bar_chart(venue_df.set_index("venue")["count"])
        
        # Also show as text
        for item in venue_data:
            bar_length = int((item["count"] / max(v["count"] for v in venue_data)) * 20)
            bar = "█" * bar_length
            st.caption(f"{item['venue']}: {bar} {item['count']} events")
    else:
        st.info("No venue data available.")
    
    # --- EVENTS BY TARGET GROUP ---
    st.markdown("---")
    st.subheader("👥 EVENTS BY TARGET GROUP")
    
    target_data = db.get_events_by_target_group()
    if target_data:
        total = sum(target_data.values())
        
        # Display as 3 columns with emoji
        emoji_map = {"children": "👶", "adults": "🧑", "families": "👨‍👩‍👧", "teens": "🧒", "all_ages": "👥"}
        
        target_cols = st.columns(min(len(target_data), 4))
        for i, (group, count) in enumerate(target_data.items()):
            percentage = (count / total * 100) if total > 0 else 0
            emoji = emoji_map.get(group, "👥")
            with target_cols[i % len(target_cols)]:
                st.metric(f"{emoji} {group.capitalize()}", f"{percentage:.0f}%")
    else:
        st.info("No target group data available.")
    
    # --- EVENTS TIMELINE ---
    st.markdown("---")
    st.subheader("📈 EVENTS TIMELINE")
    
    timeline_data = db.get_events_timeline(weeks=4)
    if timeline_data and any(item["count"] > 0 for item in timeline_data):
        timeline_df = pd.DataFrame(timeline_data)
        st.area_chart(timeline_df.set_index("week")["count"])
    else:
        st.info("No timeline data available.")

# =============================================================================
# TAB 5: SELECTORS
# =============================================================================
with tabs[4]:
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
                "status": "Status"
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