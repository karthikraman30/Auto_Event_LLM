#!/usr/bin/env python3
"""
Debug script to see what the orchestrator is actually discovering and extracting
"""
import sys
import os
sys.path.append('event_category')

from google import genai
from event_category.utils.auto_selector_discovery import EventScraperOrchestrator
import logging
import asyncio
from playwright.async_api import async_playwright

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Initialize Gemini
    api_key = os.getenv("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)
    
    # Create orchestrator
    orchestrator = EventScraperOrchestrator(client, logger)
    
    # Get page HTML
    url = "https://www.nationalmuseum.se/kalendarium"
    
    print("="*80)
    print(f"Fetching HTML from: {url}")
    print("="*80)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(2000)
        
        html_content = await page.content()
        print(f"\nHTML length: {len(html_content)} chars\n")
        
        await browser.close()
    
    # Step 1: Discover selectors
    print("="*80)
    print("STEP 1: SELECTOR DISCOVERY")
    print("="*80)
    
    discovery_result = orchestrator.discovery.discover_website_structure(html_content, url)
    
    if discovery_result:
        print("\n✅ Discovery Result:")
        import json
        print(json.dumps(discovery_result, indent=2))
    else:
        print("\n❌ Discovery failed!")
        return
    
    # Step 2: Extract events
    print("\n" + "="*80)
    print("STEP 2: EVENT EXTRACTION")
    print("="*80)
    
    selectors = discovery_result.get('selectors', {})
    events = orchestrator.discovery.extract_events_with_selectors(html_content, selectors)
    
    print(f"\n✅ Extracted {len(events)} events")
    
    if events:
        for i, event in enumerate(events, 1):
            print(f"\nEvent {i}:")
            print(f"  Name: {event.get('event_name')}")
            print(f"  Date: {event.get('date_iso')}")
            print(f"  Time: {event.get('time')}")
    else:
        print("\n⚠️  NO EVENTS EXTRACTED")
        print("\nPossible issues:")
        print("  1. Selectors might be null/invalid")
        print("  2. Extraction prompt might not work with discovered selectors")
        print("  3. Page structure might not match selector expectations")

if __name__ == "__main__":
    asyncio.run(main())
