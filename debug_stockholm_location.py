#!/usr/bin/env python3
"""Debug script to test Stockholm library location extraction"""
import asyncio
from playwright.async_api import async_playwright
import sqlite3
import json

async def test_location_extraction():
    print("🔍 Testing Stockholm library location extraction\n")
    
    # Get selectors from database
    conn = sqlite3.connect('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/selectors.db')
    cursor = conn.cursor()
    cursor.execute('SELECT container_selector, item_selectors_json FROM selector_configs WHERE domain = "biblioteket.stockholm.se" AND url_pattern = "/evenemang"')
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        print("❌ No selectors found in database")
        return
    
    container_selector, items_json = row
    items = json.loads(items_json)
    
    print("📋 Selectors from database:")
    print(f"  Container: {container_selector}")
    print(f"  Location selector: {items.get('location')}")
    print()
    
    # Test extraction with Playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        print("🌐 Loading page: https://biblioteket.stockholm.se/evenemang")
        await page.goto('https://biblioteket.stockholm.se/evenemang', wait_until='networkidle')
        await page.wait_for_timeout(2000)
        
        # Click cookie button if present
        try:
            cookie_btn = page.locator("button:has-text('Godkänn'), button:has-text('Acceptera')")
            if await cookie_btn.count() > 0:
                await cookie_btn.first.click(force=True, timeout=2000)
                await page.wait_for_timeout(1000)
        except:
            pass
        
        # Find all event containers
        containers = page.locator(container_selector)
        count = await containers.count()
        print(f"✅ Found {count} event containers\n")
        
        # Extract first 3 events to test
        for i in range(min(3, count)):
            print(f"--- Event {i+1} ---")
            container = containers.nth(i)
            
            # Extract each field
            for field_name, selector in items.items():
                try:
                    target = container.locator(selector).first
                    if await target.count() > 0:
                        if field_name == 'event_url':
                            value = await target.get_attribute('href')
                        else:
                            value = await target.inner_text()
                        
                        # Clean up whitespace
                        value = ' '.join(value.split()) if value else None
                        
                        if field_name in ['event_name', 'location', 'time']:
                            symbol = "✅" if value else "❌"
                            print(f"  {symbol} {field_name}: {value}")
                    else:
                        if field_name in ['event_name', 'location', 'time']:
                            print(f"  ❌ {field_name}: (selector matched 0 elements)")
                except Exception as e:
                    if field_name in ['event_name', 'location', 'time']:
                        print(f"  ❌ {field_name}: Error - {e}")
            print()
        
        await browser.close()

if __name__ == '__main__':
    asyncio.run(test_location_extraction())
