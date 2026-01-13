import asyncio
from playwright.async_api import async_playwright

async def debug_detail_page():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()
        
        # Test one of the event detail pages
        event_url = "https://armemuseum.se/event/jullovsaktivitet-julpyssel/"
        print(f"Testing detail page: {event_url}")
        
        try:
            await page.goto(event_url)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(2000)
            
            title = await page.title()
            print(f"Page title: {title}")
            
            # Check for description
            desc_elements = await page.locator('.richtext p').count()
            print(f"Description elements found: {desc_elements}")
            
            if desc_elements > 0:
                descriptions = await page.locator('.richtext p').all_inner_texts()
                print(f"Description text: {descriptions[0][:200]}")
            
            # Check for time elements
            time_spans = await page.locator('span').all()
            times_found = []
            
            for span in time_spans:
                text = await span.inner_text()
                if ':' in text and any(char.isdigit() for char in text):
                    times_found.append(text)
            
            print(f"Time elements found: {times_found[:5]}")
            
        except Exception as e:
            print(f"Error visiting detail page: {e}")
        
        await context.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_detail_page())
