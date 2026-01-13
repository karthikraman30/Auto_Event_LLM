#!/usr/bin/env python3
"""Quick test to demonstrate Stockholm library handler"""
import subprocess
import sys
import json
import sqlite3
from datetime import datetime

# Clear existing events from the database for this URL
def clear_existing_events():
    conn = sqlite3.connect('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/selectors.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM events WHERE event_url LIKE '%biblioteket.stockholm.se%'")
    conn.commit()
    conn.close()
    print("✅ Cleared existing Stockholm library events from database\n")

# Run the spider
def run_spider():
    print("🕷️  Running spider for biblioteket.stockholm.se/evenemang...\n")
    result = subprocess.run(
        ['scrapy', 'crawl', 'unified_events', '-a', 'url=https://biblioteket.stockholm.se/evenemang', '--loglevel=WARNING'],
        cwd='/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/event_category',
        capture_output=True,
        text=True
    )
    return result

# Show results from database
def show_results():
    conn = sqlite3.connect('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/selectors.db')
    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM events WHERE event_url LIKE '%biblioteket.stockholm.se%'")
    total = cursor.fetchone()[0]
    
    print(f"\n{'='*80}")
    print(f"📊 RESULTS: Found {total} events from biblioteket.stockholm.se/evenemang")
    print(f"{'='*80}\n")
    
    # Show first 5 events with details
    cursor.execute("""
        SELECT event_name, date_iso, time, target_group, target_group_normalized, 
               location, booking_info, status, event_url
        FROM events 
        WHERE event_url LIKE '%biblioteket.stockholm.se%'
        ORDER BY date_iso
        LIMIT 5
    """)
    
    events = cursor.fetchall()
    for i, event in enumerate(events, 1):
        name, date, time, target, target_norm, location, booking, status, url = event
        print(f"Event #{i}:")
        print(f"  📌 Name: {name}")
        print(f"  📅 Date: {date}")
        print(f"  🕐 Time: {time}")
        print(f"  👥 Target Group: {target} (normalized: {target_norm})")
        print(f"  📍 Location: {location}")
        print(f"  🎫 Booking: {booking}")
        print(f"  ✅ Status: {status}")
        print(f"  🔗 URL: {url[:60]}...")
        print()
    
    if total > 5:
        print(f"... and {total - 5} more events\n")
    
    # Show target group distribution
    cursor.execute("""
        SELECT target_group_normalized, COUNT(*) as count
        FROM events 
        WHERE event_url LIKE '%biblioteket.stockholm.se%'
        GROUP BY target_group_normalized
        ORDER BY count DESC
    """)
    
    print(f"{'='*80}")
    print("📈 TARGET GROUP DISTRIBUTION:")
    print(f"{'='*80}")
    for tg, count in cursor.fetchall():
        print(f"  {tg}: {count} events")
    
    conn.close()

if __name__ == '__main__':
    print("🚀 Stockholm Library Handler Test\n")
    
    # Step 1: Clear existing data
    clear_existing_events()
    
    # Step 2: Run the spider
    result = run_spider()
    
    if result.returncode != 0:
        print(f"❌ Spider failed with error:\n{result.stderr}")
        sys.exit(1)
    
    # Step 3: Show results
    show_results()
    
    print(f"\n{'='*80}")
    print("✅ Test completed successfully!")
    print(f"{'='*80}\n")
