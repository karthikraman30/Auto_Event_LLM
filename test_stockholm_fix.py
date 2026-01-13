#!/usr/bin/env python3
import sys
import os
import asyncio
from scrapy.http import HtmlResponse
from playwright.async_api import async_playwright

# Add the event_category path
sys.path.append('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/event_category')
from event_category.spiders.universal_spider import MultiSiteEventSpider
from event_category.utils.db_manager import DatabaseManager

async def test_stockholm_scraping():
    # Create spider instance
    spider = MultiSiteEventSpider()
    spider.db = DatabaseManager()
    
    # Test URL
    url = "https://biblioteket.stockholm.se/evenemang"
    
    print(f"Testing scraping for: {url}")
    
    # Check selectors
    selectors = spider.db.get_selectors(url)
    if not selectors:
        print("❌ No selectors found in database")
        return
    
    print(f"✅ Found selectors: container='{selectors.get('container')}'")
    print(f"✅ Item selectors: {list(selectors.get('items', {}).keys())}")
    
    # Test with Playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # Navigate to the page
            await page.goto(url, wait_until="domcontentloaded")
            
            # Wait for Stockholm Library content to load
            print("⏳ Waiting 7 seconds for dynamic content...")
            await page.wait_for_timeout(7000)
            
            # Test selector extraction
            container_sel = selectors.get('container')
            elements = await page.locator(container_sel).all()
            print(f"📊 Found {len(elements)} event containers")
            
            if len(elements) > 0:
                print("✅ SUCCESS: Events found!")
                # Extract details from first event
                first_element = elements[0]
                item_map = selectors.get('items', {})
                
                print("\n📋 First event details:")
                for field, sel in item_map.items():
                    try:
                        target = first_element.locator(sel).first
                        if await target.count() > 0:
                            value = await target.inner_text()
                            print(f"  {field}: {value[:100]}...")
                        else:
                            print(f"  {field}: [NOT FOUND]")
                    except Exception as e:
                        print(f"  {field}: [ERROR: {e}]")
            else:
                print("❌ No events found")
                
        except Exception as e:
            print(f"❌ Error during scraping: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_stockholm_scraping())
