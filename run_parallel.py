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
    "https://biblioteket.stockholm.se/forskolor"
]

OUTPUT_DIR = "event_category/temp_outputs"
FINAL_OUTPUT = "event_category/events.xlsx"

def run_spider(url, index):
    """Run a single spider for a specific URL using safe argument passing."""
    # Define filenames relative to the project root for logging
    output_filename = f"temp_outputs/events_{index}.json"
    log_filename = f"temp_outputs/spider_{index}.log"
    
    # Absolute path for checking file existence later
    full_output_path = os.path.join("event_category", output_filename)
    full_log_path = os.path.join("event_category", log_filename)
    
    # Construct command - use shell=True with proper quoting for URLs with special chars
    # Note: -O (overwrite) instead of -o (append) to prevent JSON parse issues
    cmd = f'{sys.executable} -m scrapy crawl universal_events -a "url={url}" -O {output_filename} --logfile {log_filename}'
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting spider {index+1}: {url}")
    
    try:
        # Run subprocess inside the 'event_category' directory
        result = subprocess.run(
            cmd,
            shell=True,
            cwd="event_category", 
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per spider
        )
        
        # Check if output file was created
        if os.path.exists(full_output_path):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Completed spider {index+1}: {url}")
            return full_output_path
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Spider {index+1} failed. See logs at: {full_log_path}")
            # Print a snippet of the error if available from stderr
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
                    # Handle empty files gracefully
                    content = f.read().strip()
                    if content:
                        events = json.loads(content)
                        all_events.extend(events)
                        print(f"Loaded {len(events)} events from {file_path}")
                    else:
                        print(f"File {file_path} was empty.")
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
    
    if not all_events:
        print("No events collected!")
        return
    
    # Deduplicate based on name + date
    seen = set()
    unique_events = []
    for event in all_events:
        key = (event.get('event_name', ''), event.get('date_iso', ''))
        if key not in seen:
            seen.add(key)
            unique_events.append(event)
    
    print(f"\nTotal unique events: {len(unique_events)}")
    
    # Write to Excel
    try:
        df = pd.DataFrame(unique_events)
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
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    start_time = datetime.now()
    output_files = []
    
    # Run spiders in parallel
    # NOTE: max_workers=2 is safer for stability. Increase to 4 if your PC is powerful.
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(run_spider, url, i): i for i, url in enumerate(URLS)}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
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