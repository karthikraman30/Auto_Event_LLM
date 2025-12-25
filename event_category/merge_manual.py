import pandas as pd
import json
import os
from datetime import datetime

output_files = ["temp_outputs/events_0.json"]
all_events = []

for file_path in output_files:
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                all_events.extend(data)
                print(f"Loaded {len(data)} events from {file_path}")

if all_events:
    df = pd.DataFrame(all_events)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = f"events_{timestamp}_manual.xlsx"
    df.to_excel(output, index=False)
    print(f"Saved to {output}")
else:
    print("No events found")
