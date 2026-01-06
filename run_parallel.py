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

# CRITICAL: Capture environment at module load time, BEFORE ProcessPoolExecutor spawns workers
# ProcessPoolExecutor creates NEW processes that don't inherit env vars set after fork
PARENT_ENV = os.environ.copy()

def run_spider(args):
    """Run spider for a single URL and return result info.
    
    Args is a tuple of (url, index, env_dict) to work with ProcessPoolExecutor.
    """
    url, index, env_dict = args
    
    # Use absolute paths for production compatibility (Streamlit Cloud)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    event_category_dir = os.path.join(base_dir, "event_category")
    output_filename = f"temp_outputs/events_{index}.json"
    full_output_path = os.path.join(event_category_dir, output_filename)
    
    # Determine which spider to use based on URL
    spider_name = "unified_events"  # default
    
    cmd = [sys.executable, "-m", "scrapy", "crawl", spider_name, "-a", f"url={url}", "-O", output_filename]
    
    # Debug logging for production
    print(f"[DEBUG] run_spider: Processing {url}")
    print(f"[DEBUG] run_spider: GEMINI_API_KEY present in env: {'GEMINI_API_KEY' in env_dict}")
    print(f"[DEBUG] run_spider: cwd={event_category_dir}")
    
    try:
        # CRITICAL: Use the environment passed from main(), not os.environ
        result = subprocess.run(
            cmd, 
            cwd=event_category_dir, 
            check=True, 
            timeout=1800,
            env=env_dict,  # Use explicitly passed environment
            capture_output=True,
            text=True
        )
        if os.path.exists(full_output_path):
            return {"url": url, "path": full_output_path, "success": True, "error": None}
        return {"url": url, "path": None, "success": False, "error": f"Output file not created. stdout: {result.stdout[-500:] if result.stdout else 'empty'}"}
    except subprocess.TimeoutExpired:
        return {"url": url, "path": None, "success": False, "error": "Timeout after 30 minutes"}
    except subprocess.CalledProcessError as e:
        # Include stderr for debugging
        error_msg = f"Exit code {e.returncode}"
        if e.stderr:
            error_msg += f": {e.stderr[-300:]}"
        return {"url": url, "path": None, "success": False, "error": error_msg}
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
    # Debug: Log environment status
    print(f"[DEBUG] main: GEMINI_API_KEY in PARENT_ENV: {'GEMINI_API_KEY' in PARENT_ENV}")
    print(f"[DEBUG] main: GEMINI_API_KEY in os.environ: {'GEMINI_API_KEY' in os.environ}")
    
    db = DatabaseManager()
    
    # Get enabled URLs from database
    urls = db.get_enabled_urls()
    
    if not urls:
        print("No enabled URLs to scrape.")
        return {"events": 0, "failures": 0, "warnings": ["No enabled URLs configured"]}
    
    print(f"[DEBUG] main: Found {len(urls)} URLs to scrape")
    
    # Use absolute path for temp_outputs directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(base_dir, "event_category", "temp_outputs")
    os.makedirs(temp_dir, exist_ok=True)
    
    results = []
    failures = 0
    warnings = []
    
    # Pass environment explicitly to each worker
    with ProcessPoolExecutor(max_workers=2) as executor:
        # Create args tuples with (url, index, env_dict)
        args_list = [(url, i, PARENT_ENV) for i, url in enumerate(urls)]
        futures = {executor.submit(run_spider, args): args[0] for args in args_list}
        
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