#!/usr/bin/env python3
import asyncio
import sys
import os
from playwright.async_api import async_playwright

# Add the event_category path
sys.path.append('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/event_category')
from event_category.spiders.universal_spider import MultiSiteEventSpider
from event_category.utils.db_manager import DatabaseManager

async def test_local_scraping():
    # Create spider instance
    spider = MultiSiteEventSpider()
    spider.db = DatabaseManager()
    
    # Test URL
    url = "https://biblioteket.stockholm.se/forskolor"
    
    print(f"🧪 Testing local scraping for: {url}")
    print("=" * 50)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # Navigate to the page
            await page.goto(url, wait_until="domcontentloaded")
            print(f"✅ Page loaded successfully")
            
            # Wait for dynamic content (reduced from 7s to 3s)
            print("⏳ Waiting 3s for dynamic content...")
            await page.wait_for_timeout(3000)
            
            # Get selectors from database
            selectors = spider.db.get_selectors(url)
            if not selectors:
                print("❌ No selectors found in database")
                return
            
            print(f"✅ Found selectors: container='{selectors.get('container')}'")
            
            # Extract events using selectors
            container_sel = selectors.get('container')
            elements = await page.locator(container_sel).all()
            print(f"📊 Found {len(elements)} event containers")
            
            if len(elements) > 0:
                print("\n🎉 SUCCESS: Events found!")
                
                # Test extracting first event
                first_element = elements[0]
                item_map = selectors.get('item_selectors_json', {})
                
                # Parse the JSON if it's a string
                if isinstance(item_map, str):
                    import json
                    item_map = json.loads(item_map)
                
                print(f"\n📋 First event details:")
                for field, sel in item_map.items():
                    try:
                        target = first_element.locator(sel).first
                        if await target.count() > 0:
                            value = await target.inner_text()
                            print(f"  {field}: {value[:50]}...")
                        else:
                            print(f"  {field}: [NOT FOUND]")
                    except Exception as e:
                        print(f"  {field}: [ERROR: {e}]")
                
                # Test "Visa fler" button
                load_more_btn = page.locator('button:has-text("Visa fler")')
                if await load_more_btn.count() > 0:
                    is_visible = await load_more_btn.is_visible()
                    print(f"\n🔄 'Visa fler' button found: {'Visible' if is_visible else 'Hidden'}")
                    
                    if is_visible:
                        print("🔄 Testing 'Visa fler' click...")
                        await load_more_btn.click()
                        await page.wait_for_timeout(800)
                        
                        # Count events after click
                        new_elements = await page.locator(container_sel).all()
                        print(f"📊 After click: {len(new_elements)} events (+{len(new_elements)-len(elements)} new)")
                else:
                    print("\n🔄 No 'Visa fler' button found")
                
            else:
                print("❌ No events found")
                
        except Exception as e:
            print(f"❌ Error during scraping: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_local_scraping())
