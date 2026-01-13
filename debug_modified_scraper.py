#!/usr/bin/env python3
"""
Debug the modified Tekniska scraper to identify why 0 events are returned
"""

import asyncio
from playwright.async_api import async_playwright
from datetime import datetime, timedelta

async def debug_modified_scraper():
    """Debug the modified Tekniska scraper"""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=1000)
        page = await browser.new_page()
        
        try:
            url = "https://www.tekniskamuseet.se/pa-gang/"
            print(f"Visiting {url}...")
            await page.goto(url, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            
            # Test calendar dates extraction
            calendar_dates = await page.evaluate("""
                () => {
                    const el = document.querySelector('#archive-calendar-container');
                    if (!el || !el.dataset.dates) return [];
                    try {
                        return JSON.parse(el.dataset.dates);
                    } catch {
                        return [];
                    }
                }
            """)
            
            print(f"Calendar dates found: {len(calendar_dates)}")
            if calendar_dates:
                print(f"First 5 dates: {calendar_dates[:5]}")
            
            # Filter for next 30 days
            today = datetime.now().date()
            limit = today + timedelta(days=30)
            
            relevant_dates = []
            for d in calendar_dates:
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d").date()
                    if today <= dt <= limit:
                        relevant_dates.append(d)
                except:
                    continue
            
            relevant_dates.sort()
            print(f"Relevant dates (next 30 days): {len(relevant_dates)}")
            
            if not relevant_dates:
                print("No relevant dates found!")
                return
            
            # Test the container selector
            container_selector = ".event-archive-item"
            event_cards = page.locator(container_selector)
            count = await event_cards.count()
            print(f"Events found with selector '{container_selector}': {count}")
            
            # Test alternative selectors
            alternative_selectors = [
                ".event-archive-item-inner",
                ".archive-item-link",
                "[class*='event']",
                "[class*='archive']"
            ]
            
            print("Testing alternative selectors:")
            for selector in alternative_selectors:
                try:
                    alt_count = await page.locator(selector).count()
                    print(f"  {selector}: {alt_count} elements")
                except:
                    print(f"  {selector}: Error")
            
            # Test clicking on first date
            first_date = relevant_dates[0]
            print(f"\nTesting click on date: {first_date}")
            
            clicked = await page.evaluate(
                """(d) => {
                    const el = document.querySelector(`[data-date="${d}"]`);
                    if (el) {
                        console.log('Found element with data-date:', d);
                        el.click();
                        return true;
                    }
                    console.log('No element found with data-date:', d);
                    return false;
                }""",
                first_date
            )
            
            print(f"Date clicked: {clicked}")
            
            if clicked:
                await page.wait_for_timeout(2000)
                await page.wait_for_load_state("networkidle")
                
                # Check events after click
                new_count = await event_cards.count()
                print(f"Events after clicking: {new_count}")
                
                if new_count > 0:
                    print("Testing event extraction:")
                    for i in range(min(3, new_count)):
                        try:
                            card = event_cards.nth(i)
                            # Test different selectors for event name
                            name_selectors = [
                                "h3 span",
                                ".archive-item-link h3 span",
                                "h3",
                                ".event-name",
                                "[class*='title']"
                            ]
                            
                            for name_sel in name_selectors:
                                try:
                                    name = await card.locator(name_sel).inner_text()
                                    print(f"  Event {i+1} ({name_sel}): {name}")
                                    break
                                except:
                                    continue
                        except:
                            print(f"  Event {i+1}: Could not extract name")
            
            # Check what selectors are actually available
            print("\nChecking database selectors:")
            # This would need the actual database connection, but let's see what's on the page
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            print("Keeping browser open for 10 seconds...")
            await asyncio.sleep(10)
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_modified_scraper())
