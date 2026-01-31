"""Script to regenerate unique keys for all existing events using the new logic."""
import sqlite3
import hashlib
import sys
sys.path.insert(0, '/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM')

def generate_unique_key(event_name, date_iso, location):
    """Generate a stable unique key using name+date+location."""
    if not location or location == 'N/A':
        location = ''
    unique_str = f"{event_name}|{date_iso}|{location}"
    return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()[:32]

conn = sqlite3.connect('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/selectors.db')
cursor = conn.cursor()

# Get all events
cursor.execute("SELECT id, event_name, date_iso, location, unique_key FROM events")
events = cursor.fetchall()

print(f"Total events to update: {len(events)}")

updated = 0
for event_id, event_name, date_iso, location, old_key in events:
    new_key = generate_unique_key(event_name, date_iso, location)
    if new_key != old_key:
        cursor.execute("UPDATE events SET unique_key = ? WHERE id = ?", (new_key, event_id))
        updated += 1

conn.commit()
conn.close()

print(f"Updated {updated} events with new unique keys")
print("Done!")
