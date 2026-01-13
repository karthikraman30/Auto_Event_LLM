#!/usr/bin/env python3
"""Check how many events are in the next 1 month"""
import sqlite3
from datetime import datetime, timedelta

# Connect to database
conn = sqlite3.connect('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/selectors.db')
cursor = conn.cursor()

# Calculate dates
today = datetime.now().date()
one_month_later = today + timedelta(days=30)

print(f"Today: {today}")
print(f"One month from now: {one_month_later}")
print(f"\n{'='*80}")

# Get total events from biblioteket.stockholm.se
cursor.execute("""
    SELECT COUNT(*) FROM events 
    WHERE event_url LIKE '%biblioteket.stockholm.se%'
""")
total_all = cursor.fetchone()[0]
print(f"Total events from biblioteket.stockholm.se (all dates): {total_all}")

# Get events in the next 1 month
cursor.execute("""
    SELECT COUNT(*) FROM events 
    WHERE event_url LIKE '%biblioteket.stockholm.se%'
    AND date_iso >= ? 
    AND date_iso <= ?
""", (str(today), str(one_month_later)))
next_month_count = cursor.fetchone()[0]

print(f"Events in next 1 month (until {one_month_later}): {next_month_count}")
print(f"{'='*80}\n")

# Show sample events
cursor.execute("""
    SELECT event_name, date_iso, time, location, target_group
    FROM events 
    WHERE event_url LIKE '%biblioteket.stockholm.se%'
    AND date_iso >= ? 
    AND date_iso <= ?
    ORDER BY date_iso
    LIMIT 10
""", (str(today), str(one_month_later)))

print("Sample events (first 10):")
print(f"{'='*80}")
for i, (name, date, time, location, target) in enumerate(cursor.fetchall(), 1):
    print(f"{i}. {name}")
    print(f"   Date: {date} | Time: {time}")
    print(f"   Location: {location} | Target: {target}")
    print()

# Show date distribution
cursor.execute("""
    SELECT date_iso, COUNT(*) as count
    FROM events 
    WHERE event_url LIKE '%biblioteket.stockholm.se%'
    AND date_iso >= ? 
    AND date_iso <= ?
    GROUP BY date_iso
    ORDER BY date_iso
""", (str(today), str(one_month_later)))

print(f"\n{'='*80}")
print("Events by date:")
print(f"{'='*80}")
for date, count in cursor.fetchall():
    print(f"{date}: {count} events")

conn.close()
