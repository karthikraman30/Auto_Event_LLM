#!/usr/bin/env python3
"""
Test script to scrape National Museum using AI fallback
This will demonstrate:
1. AI extracting events from a new website
2. AI discovering CSS selectors
3. Auto-saving selectors to selectors.db
"""

import subprocess
import sys
import os

def run_test():
    print("=" * 80)
    print("🎨 TESTING: National Museum Calendar Scraping")
    print("URL: https://www.nationalmuseum.se/kalendarium")
    print("=" * 80)
    print("\nThis test will:")
    print("  1. Check selectors.db (should be empty for this URL)")
    print("  2. Use AI to extract events")
    print("  3. AI will discover CSS selectors")
    print("  4. Save selectors to database for future use")
    print("\n" + "=" * 80 + "\n")
    
    # Run scrapy spider for just this URL
    cmd = [
        "scrapy", "crawl", "universal_events",
        "-a", "url=https://www.nationalmuseum.se/kalendarium",
        "-o", "nationalmuseum_test.xlsx",
        "-s", "LOG_LEVEL=INFO"
    ]
    
    print(f"Running command: {' '.join(cmd)}\n")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)) + "/event_category",
            capture_output=False,
            text=True
        )
        
        print("\n" + "=" * 80)
        if result.returncode == 0:
            print("✅ Test completed successfully!")
            print("\nCheck the following:")
            print("  1. nationalmuseum_test.xlsx - Contains extracted events")
            print("  2. selectors.db - Should now have selectors for nationalmuseum.se")
        else:
            print("❌ Test failed with exit code:", result.returncode)
        print("=" * 80)
        
        return result.returncode
    except Exception as e:
        print(f"❌ Error running test: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(run_test())
