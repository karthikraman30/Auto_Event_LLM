#!/usr/bin/env python3
"""
Migration Script: Seed selectors.db with hardcoded selectors from universal_spider.py

This script migrates the hardcoded CSS selectors for:
1. Stockholm Library (biblioteket.stockholm.se/evenemang)
2. Stockholm Library Preschools (biblioteket.stockholm.se/forskolor)  
3. Skansen fallback selectors (skansen.se/en/calendar/)

Run this once to populate the database, then the spider will read from selectors.db
instead of using hardcoded values.

Usage:
    python seed_selectors.py
"""

import sqlite3
import json
import os
from datetime import datetime

# Database path - try multiple locations
DB_PATHS = [
    "selectors.db",
    "event_category/selectors.db",
]

def get_db_path():
    """Find the selectors.db file."""
    for path in DB_PATHS:
        if os.path.exists(path):
            print(f"✓ Found database at: {path}")
            return path
    # Default to event_category path if creating new
    return "event_category/selectors.db"

def seed_selectors():
    """Seed the database with hardcoded selectors."""
    
    db_path = get_db_path()
    conn = sqlite3.connect(db_path, timeout=30.0)
    cursor = conn.cursor()
    
    # Ensure table exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS selector_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            url_pattern TEXT NOT NULL,
            container_selector TEXT,
            item_selectors_json TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(domain, url_pattern)
        )
    ''')
    
    # ===========================================
    # SELECTORS TO MIGRATE (ALL 5 SITES)
    # ===========================================
    
    selectors_config = [
        # 1. Stockholm Library - Evenemang
        {
            "domain": "biblioteket.stockholm.se",
            "url_pattern": "/evenemang",
            "container": "article",
            "items": {
                "event_name": "h2 a",
                "event_url": "h2 a",
                "date_iso": "time",
                "date_range_text": "time",
                "time": "section > div:nth-child(3) p",
                "location": "section > div:nth-child(4) p",
                "target_group_raw": "section p",
                "status_indicator": "div p",
                "booking_status": "p"
            }
        },
        
        # 2. Stockholm Library - Forskolor (same selectors, different path)
        {
            "domain": "biblioteket.stockholm.se",
            "url_pattern": "/forskolor",
            "container": "article",
            "items": {
                "event_name": "h2 a",
                "event_url": "h2 a",
                "date_iso": "time",
                "date_range_text": "time",
                "time": "section > div:nth-child(3) p",
                "location": "section > div:nth-child(4) p",
                "target_group_raw": "section p",
                "status_indicator": "div p",
                "booking_status": "p"
            }
        },
        
        # 3. Skansen Calendar
        {
            "domain": "skansen.se",
            "url_pattern": "/en/calendar/",
            "container": "ul.calendarList__list li.calendarItem",
            "items": {
                "event_name": ".calendarItem__titleLink h5",
                "event_url": ".calendarItem__titleLink",  # href attribute
                "time": ".calendarItem__information p",
                "description": ".calendarItem__description p",
                "target_group": "ul.calendarItem__tags li.tag"
            }
        },
        
        # 4. Moderna Museet
        # Note: Uses Scrapy Selector with day-based structure (.calendar__day[data-date])
        {
            "domain": "modernamuseet.se",
            "url_pattern": "/stockholm/sv/kalender/",
            "container": "article.calendar__item",  # Events within .calendar__day
            "items": {
                "event_name": ".calendar__item-title::text",
                "event_url": ".calendar__item-share a.read-more::attr(href)",
                "time": ".calendar__item-category time::text",
                "description": ".calendar__item-extended-content p::text",
                "location": ".calendar__item-share li a::text",  # After location icon
                "target_group": ".calendar__item-category li::text"
            },
            # Extra metadata for specialized handler
            "parser_type": "scrapy_selector",
            "parent_container": ".calendar__day",
            "date_attr": "data-date"
        },
        
        # 5. Tekniska Museet
        # Note: Uses BeautifulSoup with Cloudscraper for Cloudflare bypass
        {
            "domain": "tekniskamuseet.se",
            "url_pattern": "/pa-gang/",
            "container": ".event-archive-item-inner",
            "items": {
                "event_name": ".archive-item-link h3 span",
                "event_url": ".archive-item-link",  # href attribute
                "date_iso": ".archive-item-date span",
                "target_group_age": ".event-archive-item-age span",
                "target_group_type": ".event-archive-item-type span",
                "target_group_tags": ".archive-item-tags li span"
            },
            # Extra metadata for specialized handler
            "parser_type": "beautifulsoup",
            "requires_cloudscraper": True,
            "detail_description_selector": "main p"
        },
        
        # 6. Armemuseum - Calendar (List Page)
        # Hybrid approach: Extract name/date from calendar cards, then fetch description from detail page
        {
            "domain": "armemuseum.se",
            "url_pattern": "/kalender/",
            "container": "a[href*='/event/']",  # Event card links
            "items": {
                "event_name": "span.font-mulish.font-black",
                "date_range": "span.text-xs.leading-7.font-roboto",  # e.g., "28 december - 6 januari"
                "event_url": "a"  # href attribute
            },
            "parser_type": "hybrid_two_step",  # Extracts name/date from list, description from detail
            "link_patterns": ["/event/"]
        },
        
        # 7. Armemuseum - Event Detail Page (NEW - replaces AI extraction)
        {
            "domain": "armemuseum.se",
            "url_pattern": "/event/",  # Pattern for detail pages
            "container": "body",  # Single page extraction
            "items": {
                "event_name": "span.font-mulish.font-black",
                "date_range": "span.text-xs.leading-7.font-roboto",  # "28 december - 6 januari"
                "description": ".richtext p",
                # Location and target_group extracted from description text
            },
            "parser_type": "detail_page",
            "extract_location_from_description": True,
            "extract_target_from_description": True
        },
    ]
    
    # ===========================================
    # INSERT/UPDATE SELECTORS
    # ===========================================
    
    print("\n📦 Seeding selectors into database...\n")
    
    for config in selectors_config:
        domain = config["domain"]
        url_pattern = config["url_pattern"]
        container = config["container"]
        items_json = json.dumps(config["items"])
        
        cursor.execute('''
            INSERT INTO selector_configs (domain, url_pattern, container_selector, item_selectors_json, last_updated)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(domain, url_pattern) DO UPDATE SET
                container_selector = excluded.container_selector,
                item_selectors_json = excluded.item_selectors_json,
                last_updated = excluded.last_updated
        ''', (domain, url_pattern, container, items_json, datetime.now().isoformat()))
        
        print(f"  ✓ {domain}{url_pattern}")
        print(f"    Container: {container}")
        print(f"    Fields: {list(config['items'].keys())}")
        print()
    
    conn.commit()
    
    # ===========================================
    # VERIFY
    # ===========================================
    
    print("📋 Verification - Current selectors in database:\n")
    cursor.execute("SELECT domain, url_pattern, container_selector, item_selectors_json FROM selector_configs")
    rows = cursor.fetchall()
    
    for row in rows:
        domain, pattern, container, items = row
        items_dict = json.loads(items) if items else {}
        print(f"  • {domain}{pattern}")
        print(f"    Container: {container}")
        print(f"    Fields: {list(items_dict.keys())}")
        print()
    
    conn.close()
    
    print(f"✅ Successfully seeded {len(selectors_config)} selector configurations!")
    print(f"   Database: {db_path}")

if __name__ == "__main__":
    seed_selectors()
