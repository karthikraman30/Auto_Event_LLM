#!/usr/bin/env python3
"""
Quick check to see what's actually on the National Museum page
and why the AI discovered selectors but extracted 0 events
"""
import asyncio
from playwright.async_api import async_playwright

async def check_nationalmuseum():
    print("="*80)
    print("CHECKING NATIONAL MUSEUM PAGE STRUCTURE")
    print("="*80)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        print("\n1. Loading page...")
        await page.goto("https://www.nationalmuseum.se/kalendarium", wait_until="networkidle")
        await page.wait_for_timeout(2000)
        
        # Check for different possible event containers
        selectors_to_test = [
            "article",
            "div.event",
           "div[class*='event']",
            "div[class*='calendar']",
            ".event-item",
            ".calendar-item",
            "a[href*='kalendarium']"
        ]
        
        print("\n2. Testing common event container selectors:")
        for selector in selectors_to_test:
            count = await page.locator(selector).count()
            print(f"   {selector}: {count} elements")
        
        # Get page title
        title = await page.title()
        print(f"\n3. Page Title: {title}")
        
        # Check if there's any text mentioning events
        body_text = await page.inner_text("body")
        print(f"\n4. Page body length: {len(body_text)} characters")
        
        # Save HTML for inspection
        html = await page.content()
        with open("nationalmuseum_page.html", "w", encoding="utf-8") as f:
            f.write(html)
        print(f"\n5. Saved full HTML to nationalmuseum_page.html ({len(html)} bytes)")
        
        # Try to find ANY links
        all_links = await page.locator("a").count()
        print(f"\n6. Total links on page: {all_links}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(check_nationalmuseum())
