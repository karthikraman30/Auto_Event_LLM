#!/usr/bin/env python3
import asyncio
from playwright.async_api import async_playwright

async def test_pagination():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            await page.goto("https://biblioteket.stockholm.se/forskolor", wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)
            
            container_sel = "article"
            initial_events = await page.locator(container_sel).all()
            print(f"📊 Initial events: {len(initial_events)}")
            
            # Try clicking "Visa fler" multiple times
            load_more_btn = page.locator('button:has-text("Visa fler")')
            
            for i in range(3):
                if await load_more_btn.count() > 0 and await load_more_btn.is_visible():
                    print(f"🔄 Click {i+1}: Clicking 'Visa fler' button...")
                    await load_more_btn.click()
                    await page.wait_for_timeout(800)
                    
                    current_events = await page.locator(container_sel).all()
                    print(f"📊 After click {i+1}: {len(current_events)} events")
                    
                    if len(current_events) == len(initial_events):
                        print("🔄 No new events loaded - might be at the end")
                        break
                else:
                    print(f"🔄 Click {i+1}: Button not visible")
                    break
            
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_pagination())
