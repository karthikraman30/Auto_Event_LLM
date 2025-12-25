#!/usr/bin/env python3
"""
Parallel Spider Runner
Spawns one Scrapy spider per URL in separate subprocesses for maximum performance.
"""

import subprocess
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import pandas as pd

# URLs to scrape
URLS = [
    "https://biblioteket.stockholm.se/evenemang",
    "https://www.skansen.se/en/calendar/",
    "https://biblioteket.stockholm.se/forskolor",
    "https://www.modernamuseet.se/stockholm/sv/kalender/",
    "https://armemuseum.se/kalender/",  # Enabled for debugging
    "https://www.tekniskamuseet.se/pa-gang/"
]

OUTPUT_DIR = "event_category/temp_outputs"
# [MODIFIED] Unique output filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
FINAL_OUTPUT = f"event_category/events_{timestamp}.xlsx"

def run_spider(url, index):
    """Run a single spider for a specific URL using safe argument passing."""
    output_filename = f"temp_outputs/events_{index}.json"
    log_filename = f"temp_outputs/spider_{index}.log"
    
    full_output_path = os.path.join("event_category", output_filename)
    full_log_path = os.path.join("event_category", log_filename)
    
    # [FIX] Use a LIST of arguments, not a string. This prevents quoting errors.
    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "universal_events",
        "-a", f"url={url}",
        "-O", output_filename,  # Overwrite mode
        "--logfile", log_filename
    ]
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting spider {index+1}: {url}")
    
    try:
        # [FIX] shell=False is safer and more reliable for list arguments
        result = subprocess.run(
            cmd,
            cwd="event_category", 
            capture_output=True,
            text=True,
            timeout=1800,  # [OPTIMIZED] Increased from 900s to 1800s (30 min)
            shell=False 
        )
        
        if os.path.exists(full_output_path):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Completed spider {index+1}: {url}")
            return full_output_path
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Spider {index+1} failed. See logs at: {full_log_path}")
            if result.stderr:
                print(f"   Error snippet: {result.stderr[:200]}...")
            return None

    except subprocess.TimeoutExpired:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Spider {index+1} timed out: {url}")
        return None
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Spider {index+1} error: {e}")
        return None

def merge_results(output_files):
    """Merge all JSON outputs into a single Excel file."""
    all_events = []
    
    for file_path in output_files:
        if file_path and os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        try:
                            events = json.loads(content)
                            if isinstance(events, list):
                                all_events.extend(events)
                                print(f"Loaded {len(events)} events from {file_path}")
                        except json.JSONDecodeError:
                            print(f"Warning: Could not decode JSON from {file_path}")
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
    
    if not all_events:
        print("No events collected!")
        return
    
    # Deduplicate
    seen = set()
    unique_events = []
    for event in all_events:
        # Create a unique key using name, date, and time
        # [MODIFIED] Added time to key to prevent dropping same-day events at different times
        key = (
            event.get('event_name', ''), 
            event.get('date_iso', ''), 
            event.get('time', '')
        )
        if key not in seen:
            seen.add(key)
            unique_events.append(event)
    
    print(f"\nTotal unique events: {len(unique_events)}")
    
    # Write to Excel
    try:
        df = pd.DataFrame(unique_events)
        
        # [MODIFIED] Standardize target_group column
        if 'target_group' in df.columns and 'target_group_normalized' in df.columns:
            # Drop raw column and rename normalized
            df = df.drop(columns=['target_group'])
            df = df.rename(columns={'target_group_normalized': 'target_group'})
            
        df.to_excel(FINAL_OUTPUT, index=False)
        print(f"Results saved to {FINAL_OUTPUT}")
    except Exception as e:
        print(f"Error saving Excel file: {e}")
    
    # Cleanup temp files
    for file_path in output_files:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    print("Temporary files cleaned up.")

def main():
    print(f"\n{'='*60}")
    print(f"Parallel Spider Runner - {len(URLS)} URLs")
    print(f"{'='*60}\n")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    start_time = datetime.now()
    output_files = []
    
    # max_workers=2 is safe for stability
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(run_spider, url, i): i for i, url in enumerate(URLS)}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                output_files.append(result)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nAll spiders completed in {elapsed:.1f} seconds")
    
    merge_results(output_files)
    
    print(f"\n{'='*60}")
    print("Done!")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()