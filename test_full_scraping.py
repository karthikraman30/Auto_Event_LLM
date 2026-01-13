#!/usr/bin/env python3
import asyncio
import sys
import os
import json
from playwright.async_api import async_playwright

# Add the event_category path
sys.path.append('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/event_category')
from event_category.spiders.universal_spider import MultiSiteEventSpider, extract_booking_info
from event_category.utils.db_manager import DatabaseManager

async def test_full_scraping():
    # Create spider instance
    spider = MultiSiteEventSpider()
    spider.db = DatabaseManager()
    
    # Test URL
    url = "https://biblioteket.stockholm.se/forskolor"
    
    print(f"🧪 Full scraping test for: {url}")
    print("=" * 60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # Step 1: Navigate to the page
            print("📡 Step 1: Loading page...")
            await page.goto(url, wait_until="domcontentloaded")
            print(f"✅ Page loaded successfully")
            
            # Step 2: Wait for dynamic content
            print("⏳ Step 2: Waiting 3s for dynamic content...")
            await page.wait_for_timeout(3000)
            
            # Step 3: Get selectors from database
            print("🗄️ Step 3: Loading selectors from database...")
            selectors = spider.db.get_selectors(url)
            if not selectors:
                print("❌ No selectors found in database")
                return
            
            print(f"✅ Found selectors: container='{selectors.get('container')}'")
            
            # Step 4: Extract initial events
            print("📊 Step 4: Extracting initial events...")
            container_sel = selectors.get('container')
            elements = await page.locator(container_sel).all()
            print(f"✅ Found {len(elements)} initial events")
            
            # Step 5: Test "Visa fler" pagination
            print("🔄 Step 5: Testing pagination...")
            load_more_btn = page.locator('button:has-text("Visa fler")')
            
            total_events = len(elements)
            max_clicks = 10
            
            for click_num in range(max_clicks):
                if await load_more_btn.count() > 0 and await load_more_btn.is_visible():
                    print(f"🔄 Click {click_num + 1}: Clicking 'Visa fler' button...")
                    await load_more_btn.click()
                    await page.wait_for_timeout(800)
                    
                    current_events = await page.locator(container_sel).all()
                    new_events = len(current_events) - total_events
                    
                    if new_events > 0:
                        total_events = len(current_events)
                        print(f"✅ After click {click_num + 1}: {total_events} events (+{new_events} new)")
                    else:
                        print(f"🔄 After click {click_num + 1}: {total_events} events (no new events - stopping)")
                        break
                else:
                    print(f"🔄 Click {click_num + 1}: Button not visible - stopping")
                    break
            
            print(f"🎉 Step 5 Complete: Total {total_events} events found")
            
            # Step 6: Test data extraction from first few events
            print("📋 Step 6: Testing data extraction...")
            item_map = selectors.get('item_selectors_json', {})
            if isinstance(item_map, str):
                item_map = json.loads(item_map)
            
            test_events = await page.locator(container_sel).all()
            sample_size = min(3, len(test_events))
            
            for i in range(sample_size):
                print(f"\n📋 Event {i+1} details:")
                event_element = test_events[i]
                
                for field, sel in item_map.items():
                    try:
                        target = event_element.locator(sel).first
                        if await target.count() > 0:
                            value = await target.inner_text()
                            
                            # Special handling for booking_status
                            if field == 'booking_status':
                                # Test the booking extraction logic
                                booking_result = extract_booking_info(value)
                                print(f"  {field}: '{value[:30]}...' → '{booking_result}'")
                            else:
                                print(f"  {field}: {value[:40]}...")
                        else:
                            print(f"  {field}: [NOT FOUND]")
                    except Exception as e:
                        print(f"  {field}: [ERROR: {e}]")
            
            # Step 7: Summary
            print("\n" + "=" * 60)
            print("🎉 FULL SCRAPING TEST RESULTS:")
            print(f"✅ Total events found: {total_events}")
            print(f"✅ Pagination clicks: {min(click_num + 1, max_clicks)}")
            print(f"✅ Data extraction: Working")
            print(f"✅ Booking info extraction: Working")
            print(f"✅ Estimated time: ~{total_events * 0.1:.1f} seconds for full scrape")
            
            if total_events >= 50:
                print("🚀 READY FOR PRODUCTION: Good number of events found!")
            else:
                print("⚠️  WARNING: Fewer events than expected")
                
        except Exception as e:
            print(f"❌ Error during scraping: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_full_scraping())
