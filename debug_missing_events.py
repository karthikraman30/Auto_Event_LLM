#!/usr/bin/env python3
"""
Debug why we're only getting 3 events instead of 5
"""

import asyncio
from playwright.async_api import async_playwright
from datetime import datetime, timedelta

async def debug_missing_events():
    """Debug why early January events are missing"""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=1000)
        page = await browser.new_page()
        
        try:
            url = "https://www.tekniskamuseet.se/pa-gang/"
            print(f"Visiting {url}...")
            await page.goto(url, wait_until="networkidle")
            await page.wait_for_timeout(5000)
            
            # Get calendar dates
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
            print(f"First 10 relevant dates: {relevant_dates[:10]}")
            
            # Get visible events
            event_cards = page.locator(".event-archive-item-inner")
            count = await event_cards.count()
            print(f"\nVisible events: {count}")
            
            # Check each event for date info
            for i in range(count):
                try:
                    card = event_cards.nth(i)
                    name = await card.locator("h3 span").inner_text()
                    
                    # Look for date patterns in the card text
                    card_text = await card.inner_text()
                    
                    import re
                    date_pattern = r'(\d{4}-\d{2}-\d{2})(?:\s*-\s*(\d{4}-\d{2}-\d{2}))?'
                    date_match = re.search(date_pattern, card_text)
                    
                    if date_match:
                        actual_date = date_match.group(1)
                        end_date = date_match.group(2) if date_match.group(2) else 'N/A'
                        print(f"  {i+1}. {name} - Card date: {actual_date} - {end_date}")
                    else:
                        print(f"  {i+1}. {name} - NO CARD DATE")
                        
                except Exception as e:
                    print(f"  {i+1}. Error: {e}")
            
            # Check if early January events should be assigned calendar dates
            print(f"\nChecking calendar date assignment...")
            print(f"Relevant dates available: {len(relevant_dates)}")
            print(f"Events without card dates: {count - 3}")  # We know 3 have card dates
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            print("Keeping browser open for 5 seconds...")
            await asyncio.sleep(5)
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_missing_events())
