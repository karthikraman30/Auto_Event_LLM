#!/usr/bin/env python3
"""
Debug location and description extraction
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_location_description():
    """Debug location and description extraction"""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=1000)
        page = await browser.new_page()
        
        try:
            url = "https://www.tekniskamuseet.se/pa-gang/"
            print(f"Visiting {url}...")
            await page.goto(url, wait_until="networkidle")
            await page.wait_for_timeout(5000)
            
            # Get visible events
            event_cards = page.locator(".event-archive-item-inner")
            count = await event_cards.count()
            print(f"\nVisible events: {count}")
            
            # Check each event for location and description info
            for i in range(min(3, count)):  # Check first 3 events
                try:
                    card = event_cards.nth(i)
                    name = await card.locator("h3 span").inner_text()
                    
                    # Get full card text
                    card_text = await card.inner_text()
                    print(f"\n{i+1}. {name}")
                    print(f"Full card text: {repr(card_text)}")
                    
                    # Try to find location elements
                    try:
                        location_selectors = [
                            ".location",
                            "[class*='location']", 
                            "[class*='place']",
                            "[class*='venue']",
                            ".event-location",
                            ".venue"
                        ]
                        for sel in location_selectors:
                            try:
                                loc_elem = await card.locator(sel).inner_text()
                                if loc_elem and loc_elem.strip():
                                    print(f"  Location found with {sel}: {repr(loc_elem)}")
                                    break
                            except:
                                continue
                        else:
                            print("  No location element found")
                    except:
                        print("  Error finding location")
                    
                    # Check if "Tensta" appears in the text (might be location)
                    if "Tensta" in card_text:
                        print(f"  Found 'Tensta' in card text - likely location")
                    
                except Exception as e:
                    print(f"  Error: {e}")
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            print("Keeping browser open for 5 seconds...")
            await asyncio.sleep(5)
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_location_description())
