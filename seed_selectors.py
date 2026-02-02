#!/usr/bin/env python3
"""
Migration Script: Seed Supabase with hardcoded selectors from universal_spider.py

This script migrates the hardcoded CSS selectors for:
1. Stockholm Library (biblioteket.stockholm.se/evenemang)
2. Stockholm Library Preschools (biblioteket.stockholm.se/forskolor)  
3. Skansen fallback selectors (skansen.se/en/calendar/)

Run this once to populate the database, then the spider will read from Supabase
instead of using hardcoded values.

Usage:
    python seed_selectors.py
    
Can also be imported and called programmatically:
    from seed_selectors import ensure_selectors_seeded
    ensure_selectors_seeded()  # Only seeds if DB is empty
"""

import json
import os
from datetime import datetime

# Try to import supabase
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

# Try to import streamlit for secrets
try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False


def get_supabase_client():
    """Get Supabase client with credentials from secrets or env vars."""
    if not SUPABASE_AVAILABLE:
        raise ImportError("supabase package not installed. Run: pip install supabase")
    
    url = None
    key = None
    
    # Try Streamlit secrets first
    if STREAMLIT_AVAILABLE:
        try:
            url = st.secrets.get("SUPABASE_URL")
            key = st.secrets.get("SUPABASE_KEY")
        except Exception:
            pass
    
    # Fall back to environment variables
    if not url:
        url = os.environ.get("SUPABASE_URL")
    if not key:
        key = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError(
            "Supabase credentials not found. Set SUPABASE_URL and SUPABASE_KEY in:\n"
            "  - Streamlit secrets (.streamlit/secrets.toml), or\n"
            "  - Environment variables"
        )
    
    return create_client(url, key)

def seed_selectors():
    """Seed the database with hardcoded selectors."""
    
    supabase = get_supabase_client()
    print("✓ Connected to Supabase")
    
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
                "booking_status": "p",
                "image_url": "img"
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
                "booking_status": "p",
                "image_url": "img"
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
                "target_group": "ul.calendarItem__tags li.tag",
                "image_url": ".calendarItem__image"
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
    
    print("\n📦 Seeding selectors into Supabase...\n")
    
    for config in selectors_config:
        domain = config["domain"]
        url_pattern = config["url_pattern"]
        container = config["container"]
        items_json = json.dumps(config["items"])
        
        # Upsert into Supabase
        supabase.table('selector_configs').upsert({
            'domain': domain,
            'url_pattern': url_pattern,
            'container_selector': container,
            'item_selectors_json': items_json,
            'last_updated': datetime.now().isoformat()
        }, on_conflict='domain,url_pattern').execute()
        
        print(f"  ✓ {domain}{url_pattern}")
        print(f"    Container: {container}")
        print(f"    Fields: {list(config['items'].keys())}")
        print()
    
    # ===========================================
    # VERIFY
    # ===========================================
    
    print("📋 Verification - Current selectors in Supabase:\n")
    response = supabase.table('selector_configs').select('domain, url_pattern, container_selector, item_selectors_json').execute()
    rows = response.data or []
    
    for row in rows:
        domain = row['domain']
        pattern = row['url_pattern']
        container = row['container_selector']
        items = row['item_selectors_json']
        items_dict = json.loads(items) if items else {}
        print(f"  • {domain}{pattern}")
        print(f"    Container: {container}")
        print(f"    Fields: {list(items_dict.keys())}")
        print()
    
    print(f"✅ Successfully seeded {len(selectors_config)} selector configurations!")
    print(f"   Database: Supabase Cloud")
    return len(selectors_config)


def ensure_selectors_seeded(verbose=False):
    """
    Check if Supabase has any selectors; if empty, seed with defaults.
    
    This is safe to call on every app startup - it only seeds if the database
    is empty or missing, preventing duplicate entries.
    
    Args:
        verbose: If True, print status messages
        
    Returns:
        dict with status info: {'seeded': bool, 'count': int, 'message': str}
    """
    try:
        supabase = get_supabase_client()
        
        # Check if any selectors exist
        response = supabase.table('selector_configs').select('id', count='exact').execute()
        count = response.count or 0
        
        if count == 0:
            if verbose:
                print("🔧 No selectors found in Supabase. Auto-seeding...")
            seeded_count = seed_selectors()
            return {
                'seeded': True,
                'count': seeded_count,
                'message': f'Auto-seeded {seeded_count} selector configurations'
            }
        else:
            if verbose:
                print(f"✓ Supabase already has {count} selector configurations")
            return {
                'seeded': False,
                'count': count,
                'message': f'Database already has {count} selector configurations'
            }
            
    except Exception as e:
        error_msg = f"Error checking/seeding selectors: {e}"
        if verbose:
            print(f"❌ {error_msg}")
        return {
            'seeded': False,
            'count': 0,
            'message': error_msg
        }


if __name__ == "__main__":
    seed_selectors()
