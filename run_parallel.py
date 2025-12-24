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
    "https://biblioteket.stockholm.se/forskolor",
    "https://www.tekniskamuseet.se/pa-gang/?date=",
    "https://www.modernamuseet.se/stockholm/sv/kalender/"
]

OUTPUT_DIR = "event_category/temp_outputs"
FINAL_OUTPUT = "event_category/events.xlsx"

def run_spider(url, index):
    """Run a single spider for a specific URL."""
    output_file = f"temp_outputs/events_{index}.json"
    full_output_path = os.path.join("event_category", output_file)
    
    # Use shell command string for proper argument handling
    cmd = f'cd event_category && {sys.executable} -m scrapy crawl universal_events -a url="{url}" -o {output_file} -t json --nolog'
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting spider {index+1}: {url}")
    
    
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per spider
        )
        
        # Check if output file was created (more reliable than returncode)
        if os.path.exists(full_output_path):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Completed spider {index+1}: {url}")
            return full_output_path
        else:
            error_msg = result.stderr[:300] if result.stderr else result.stdout[:300]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Spider {index+1} failed: {error_msg}")
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
                    events = json.load(f)
                    all_events.extend(events)
                print(f"Loaded {len(events)} events from {file_path}")
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
    
    if not all_events:
        print("No events collected!")
        return
    
    # Deduplicate
    seen = set()
    unique_events = []
    for event in all_events:
        key = (event.get('event_name', ''), event.get('date_iso', ''))
        if key not in seen:
            seen.add(key)
            unique_events.append(event)
    
    print(f"\nTotal unique events: {len(unique_events)}")
    
    # Write to Excel
    df = pd.DataFrame(unique_events)
    df.to_excel(FINAL_OUTPUT, index=False)
    print(f"Results saved to {FINAL_OUTPUT}")
    
    # Cleanup temp files
    for file_path in output_files:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    print("Temporary files cleaned up.")

def main():
    print(f"\n{'='*60}")
    print(f"Parallel Spider Runner - {len(URLS)} URLs")
    print(f"{'='*60}\n")
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    start_time = datetime.now()
    output_files = []
    
    # Run spiders in parallel using ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=len(URLS)) as executor:
        futures = {executor.submit(run_spider, url, i): i for i, url in enumerate(URLS)}
        
        for future in as_completed(futures):
            result = future.result()
            output_files.append(result)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nAll spiders completed in {elapsed:.1f} seconds")
    
    # Merge results
    merge_results(output_files)
    
    print(f"\n{'='*60}")
    print("Done!")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
