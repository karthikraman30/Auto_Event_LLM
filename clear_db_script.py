from event_category.event_category.utils.db_manager import DatabaseManager
import os
import sys

# Add the project root to python path to allow imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.append(project_root)

# Re-import with correct path if needed, but since we are running from root usually...
# Let's just try to instantiate and run.
try:
    db = DatabaseManager()
    count_before = len(db.get_all_events())
    print(f"Events before: {count_before}")
    
    deleted = db.delete_all_events()
    print(f"Deleted {deleted} events.")
    
    count_after = len(db.get_all_events())
    print(f"Events after: {count_after}")
except Exception as e:
    print(f"Error: {e}")
