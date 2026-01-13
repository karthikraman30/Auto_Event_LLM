#!/usr/bin/env python3
import asyncio
from playwright.async_api import async_playwright

async def debug_load_more_button():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Use headed mode to see what's happening
        page = await browser.new_page()
        
        try:
            await page.goto("https://biblioteket.stockholm.se/forskolor", wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)
            
            print("🔍 Investigating 'Visa fler' button...")
            
            # Find all possible load more buttons
            selectors = [
                'button:has-text("Visa fler")',
                'button:has-text("Visa fler evenemang")',
                'a:has-text("Visa fler")',
                'button[class*="load"]',
                'a[class*="load"]',
                '.load-more',
                '.show-more'
            ]
            
            for selector in selectors:
                buttons = await page.locator(selector).all()
                if buttons:
                    print(f"✅ Found {len(buttons)} buttons with selector: {selector}")
                    for i, btn in enumerate(buttons):
                        try:
                            text = await btn.inner_text()
                            visible = await btn.is_visible()
                            enabled = await btn.is_enabled()
                            print(f"  Button {i+1}: '{text}' | Visible: {visible} | Enabled: {enabled}")
                        except:
                            print(f"  Button {i+1}: [Error getting details]")
                else:
                    print(f"❌ No buttons found with selector: {selector}")
            
            # Try to find the specific button and check its behavior
            load_btn = page.locator('button:has-text("Visa fler")').first
            if await load_btn.count() > 0:
                print("\n🔄 Testing the first 'Visa fler' button...")
                
                # Check button state before click
                before_text = await load_btn.inner_text()
                before_visible = await load_btn.is_visible()
                before_enabled = await load_btn.is_enabled()
                print(f"Before: Text='{before_text}', Visible={before_visible}, Enabled={before_enabled}")
                
                # Click the button
                await load_btn.click()
                await page.wait_for_timeout(2000)  # Wait longer for content to load
                
                # Check button state after click
                after_text = await load_btn.inner_text()
                after_visible = await load_btn.is_visible()
                after_enabled = await load_btn.is_enabled()
                print(f"After: Text='{after_text}', Visible={after_visible}, Enabled={after_enabled}")
                
                # Count events before and after
                events_before = await page.locator("article").count()
                print(f"Events after click: {events_before}")
                
                # Wait a bit more and check again
                await page.wait_for_timeout(3000)
                events_after = await page.locator("article").count()
                print(f"Events after waiting: {events_after}")
                
                # Check if there's a loading indicator
                loading = await page.locator(".loading, .spinner, [class*='loading']").count()
                print(f"Loading indicators found: {loading}")
                
            else:
                print("❌ No 'Visa fler' button found")
                
            # Keep browser open for inspection
            print("\n🔍 Browser will stay open for 30 seconds for manual inspection...")
            await page.wait_for_timeout(30000)
                
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_load_more_button())
