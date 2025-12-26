import subprocess
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import pandas as pd

# Path setup to import the DatabaseManager
sys.path.append(os.path.join(os.getcwd(), "event_category"))
from event_category.utils.db_manager import DatabaseManager

def run_spider(url, index):
    """Run spider for a single URL and return result info."""
    output_filename = f"temp_outputs/events_{index}.json"
    full_output_path = os.path.join("event_category", output_filename)
    
    cmd = [sys.executable, "-m", "scrapy", "crawl", "universal_events", "-a", f"url={url}", "-O", output_filename]
    
    try:
        subprocess.run(cmd, cwd="event_category", check=True, timeout=1800)
        if os.path.exists(full_output_path):
            return {"url": url, "path": full_output_path, "success": True, "error": None}
        return {"url": url, "path": None, "success": False, "error": "Output file not created"}
    except subprocess.TimeoutExpired:
        return {"url": url, "path": None, "success": False, "error": "Timeout after 30 minutes"}
    except Exception as e:
        return {"url": url, "path": None, "success": False, "error": str(e)}

def merge_to_db(results):
    """Merge scraped events to database and return statistics."""
    db = DatabaseManager()
    total_events = 0
    
    for result in results:
        if result["success"] and result["path"] and os.path.exists(result["path"]):
            with open(result["path"], 'r', encoding='utf-8') as f:
                events = json.load(f)
                for event in events:
                    db.upsert_event(event)
                total_events += len(events)
            os.remove(result["path"])
    
    return total_events

def main():
    """
    Run parallel scraping for all enabled URLs.
    Returns dict with: events (count), failures (count), warnings (list of error messages)
    """
    db = DatabaseManager()
    
    # Get enabled URLs from database
    urls = db.get_enabled_urls()
    
    if not urls:
        print("No enabled URLs to scrape.")
        return {"events": 0, "failures": 0, "warnings": ["No enabled URLs configured"]}
    
    os.makedirs("event_category/temp_outputs", exist_ok=True)
    
    results = []
    failures = 0
    warnings = []
    
    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(run_spider, url, i): url for i, url in enumerate(urls)}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            
            if not result["success"]:
                failures += 1
                # Extract site name from URL for warning message
                try:
                    from urllib.parse import urlparse
                    site_name = urlparse(result["url"]).netloc.replace("www.", "").split(".")[0].capitalize()
                except:
                    site_name = result["url"]
                warnings.append(f"{site_name}: {result['error']}")
    
    # Merge successful results to database
    total_events = merge_to_db(results)
    
    print(f"Scraping complete: {total_events} events, {failures} failures")
    
    return {
        "events": total_events,
        "failures": failures,
        "warnings": warnings if warnings else None
    }

if __name__ == "__main__":
    result = main()
    print(f"Result: {result}")