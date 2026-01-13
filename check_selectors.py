#!/usr/bin/env python3
"""
Check stored selectors for Stockholm library sites
"""

import sys
import os
sys.path.append('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM')

from event_category.event_category.utils.db_manager import DatabaseManager

def check_selectors():
    db = DatabaseManager()
    
    # Check selectors for both Stockholm library URLs
    urls = [
        "https://biblioteket.stockholm.se/evenemang",
        "https://biblioteket.stockholm.se/forskolor"
    ]
    
    for url in urls:
        print(f"\n=== Selectors for {url} ===")
        selectors = db.get_selectors(url)
        if selectors:
            print(f"Container: {selectors.get('container')}")
            print("Items:")
            for field, selector in selectors.get('items', {}).items():
                print(f"  {field}: {selector}")
        else:
            print("No selectors found in database")
    
    # Also check all selector configs
    print(f"\n=== All Selector Configs in DB ===")
    configs = db.get_all_selector_configs()
    for config in configs:
        print(f"Domain: {config['domain']}")
        print(f"Path: {config['url_pattern']}")
        print(f"Container: {config['container_selector']}")
        print(f"Updated: {config['last_updated']}")
        print("-" * 50)

if __name__ == "__main__":
    check_selectors()
