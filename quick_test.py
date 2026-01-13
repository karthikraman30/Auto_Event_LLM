#!/usr/bin/env python3
import asyncio
from playwright.async_api import async_playwright

async def quick_test():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("Loading forskolor page...")
            start_time = asyncio.get_event_loop().time()
            
            await page.goto("https://biblioteket.stockholm.se/forskolor", wait_until="domcontentloaded")
            
            load_time = asyncio.get_event_loop().time() - start_time
            print(f"Page loaded in {load_time:.2f} seconds")
            
            # Wait for content
            await page.wait_for_timeout(3000)
            
            # Count events
            events = await page.locator("article").count()
            print(f"Found {events} events on the page")
            
            # Check for "Visa fler" button
            load_more = await page.locator('button:has-text("Visa fler")').count()
            print(f"Found {load_more} 'Visa fler' buttons")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(quick_test())
