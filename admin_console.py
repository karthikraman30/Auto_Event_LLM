import streamlit as st
import pandas as pd
import os
import sys
import math
import subprocess
import re
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

sys.path.append(os.path.join(os.getcwd(), "event_category"))
from event_category.utils.db_manager import DatabaseManager

# --- PYTHON PATH ---
# In Streamlit Cloud (and most containerized envs), sys.executable is the correct python.
# We trust it unless we explicitly find a venv locally (dev mode).
VENV_PYTHON = sys.executable
if os.path.exists(os.path.join(os.getcwd(), "venv")):
    VENV_PYTHON = os.path.join(os.getcwd(), "venv", "bin", "python")

def get_subprocess_env():
    """
    Create environment dict for subprocess, verifying secrets are present.
    """
    env = os.environ.copy()
    
    # 1. Inject secrets from st.secrets (Streamlit Cloud)
    # This is critical because normal os.environ might not have them in Cloud
    if hasattr(st, "secrets"):
        # Direct key access
        for key in ["GEMINI_API_KEY", "GOOGLE_API_KEY", "GROQ_API_KEY"]:
            if key in st.secrets:
                env[key] = st.secrets[key]
        
        # "env" section access (common pattern)
        if "env" in st.secrets:
            for key, value in st.secrets["env"].items():
                env[key] = value

    # 2. Validation Logging (Visible in Streamlit Cloud logs, not UI)
    if "GEMINI_API_KEY" not in env:
        print("CRITICAL WARNING: GEMINI_API_KEY not found in environment for subprocess!")
    else:
        print("Subprocess Environment: GEMINI_API_KEY is present.")

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
            [VENV_PYTHON, "run_parallel.py"],
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
    day = settings.get("schedule_day", "monday")
    time_str = settings.get("schedule_time", "06:00")
    hour, minute = map(int, time_str.split(":"))
    
    scheduler = BackgroundScheduler()
    
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
tabs = st.tabs(["📊 Dashboard", "⚙️ Settings", "📝 Logs", "📈 Analytics"])

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
        if st.button("🚀 Scrape Now", use_container_width=True):
            st.session_state.log_buffer = "Starting parallel scrape...\n"
            with st.spinner("Scraping all venues... check the Logs tab for progress."):
                import subprocess
                import sys
                
                env = get_subprocess_env()
                # Use sys.executable to ensure we use the same environment
                process = subprocess.Popen(
                    [VENV_PYTHON, "run_parallel.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    cwd=os.getcwd(),
                    env=env
                )
                
                # Capture output in real-time
                for line in process.stdout:
                    st.session_state.log_buffer += line
                
                process.wait()
                
                if process.returncode == 0:
                    # Parse results from log buffer
                    match = re.search(r'Scraping complete: (\d+) events, (\d+) failures', st.session_state.log_buffer)
                    if match:
                        events_count = int(match.group(1))
                        failures = int(match.group(2))
                        status = "Warn" if failures > 0 else "OK"
                        db.add_log("Manual", status, events_count, failures, None)
                    st.success("✅ Scrape completed successfully!")
                    st.rerun()  # Refresh to show new counts in metrics
                else:
                    db.add_log("Manual", "Error", 0, 1, ["Scraping failed"])
                    st.error("❌ Scraping failed. See Logs tab for details.")
    
    with action_col2:
        events = db.get_all_events()
        if events:
            df_export = pd.DataFrame(events)
            csv = df_export.to_csv(index=False).encode('utf-8')
            st.download_button("📁 Export Excel", csv, "events.csv", "text/csv", use_container_width=True)
        else:
            st.button("📁 Export Excel", disabled=True, use_container_width=True)
    
    # --- FILTERS SECTION ---
    st.markdown("---")
    st.subheader("🔍 FILTERS")
    
    # Initialize session state for pagination
    if 'page' not in st.session_state:
        st.session_state.page = 1
    
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
        date_range = st.selectbox("Date Range", ["Next 30 Days", "This Week", "All Time"])
    
    target_groups = st.multiselect("Target Group", options=["All", "Children", "Adults", "Families"], 
                                    default=["All"])
    
    # --- EVENTS TABLE ---
    st.markdown("---")
    st.subheader("📋 EVENTS")
    
    per_page = 20
    events_filtered, total_count = db.get_events_filtered(
        search=search, venue=venue, date_range=date_range, 
        target_groups=target_groups, source=source, page=st.session_state.page, per_page=per_page
    )
    
    total_pages = math.ceil(total_count / per_page) if total_count > 0 else 1
    
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
    
    if events_filtered:
        for idx, event in enumerate(events_filtered):
            # Format date for display (with optional end date)
            try:
                date_obj = datetime.strptime(event['date_iso'], "%Y-%m-%d")
                display_date = date_obj.strftime("%d %b %Y")
                
                # Add end date if present
                end_date = event.get('end_date_iso')
                if end_date and end_date != 'N/A':
                    try:
                        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
                        display_date += f" → {end_date_obj.strftime('%d %b %Y')}"
                    except:
                        pass
            except:
                display_date = event['date_iso'] or "Date TBA"
            
            location = event['location'] or "Location TBA"
            age_group = (event['target_group'] or "all_ages").replace("_", " ").title()
            description = event['description'] or "No description available."
            time_display = event['time'] or "Time TBA"
            booking_display = event['booking_info'] or "Booking TBA"
            event_url = event.get('event_url', '#')
            
            # Create card container
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
                
                # Footer row
                footer_cols = st.columns([1, 1, 1])
                with footer_cols[0]:
                    st.caption(f"⏰ {time_display}")
                with footer_cols[1]:
                    st.caption(f"🎟️ {booking_display}")
                with footer_cols[2]:
                    st.link_button("View Event →", event_url, use_container_width=True)
    else:
        st.info("No events found matching your filters.")
    
    # --- PAGINATION ---
    st.caption(f"Showing {(st.session_state.page - 1) * per_page + 1}-{min(st.session_state.page * per_page, total_count)} of {total_count}")
    
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
    
    sched_col1, sched_col2 = st.columns(2)
    
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    current_day = settings.get("schedule_day", "monday")
    with sched_col1:
        schedule_day = st.selectbox("Day", days, index=days.index(current_day))
    
    times = [f"{h:02d}:00" for h in range(24)]
    current_time = settings.get("schedule_time", "06:00")
    with sched_col2:
        schedule_time = st.selectbox("Time", times, index=times.index(current_time) if current_time in times else 6)
    
    # Calculate next scheduled run
    today = datetime.now()
    day_num = days.index(schedule_day)
    days_ahead = day_num - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    next_run_date = today + timedelta(days=days_ahead)
    hour = int(schedule_time.split(":")[0])
    next_run = next_run_date.replace(hour=hour, minute=0, second=0)
    st.caption(f"**Next scheduled run:** {next_run.strftime('%A, %b %d, %Y at %H:%M')}")
    
    if st.button("💾 Save Schedule"):
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
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                date_display = dt.strftime("%b %d %H:%M")
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
        
        st.dataframe(pd.DataFrame(log_data), use_container_width=True, hide_index=True)
        
        # Show expandable warnings
        for log in logs:
            if log.get("warnings") and log["status"] != "OK":
                try:
                    dt = datetime.strptime(log["timestamp"], "%Y-%m-%d %H:%M:%S")
                    date_display = dt.strftime("%b %d, %Y")
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
            st.download_button("📥 Export Logs", csv, "scraping_logs.csv", use_container_width=True)
    with log_btn_col2:
        if st.button("🗑️ Clear Old Logs", use_container_width=True):
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