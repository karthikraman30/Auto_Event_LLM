import asyncio
from playwright.async_api import async_playwright

async def debug_armemuseum_detail():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()
        
        # Go to one of the event detail pages
        await page.goto("https://armemuseum.se/event/helgvisning-stormaktstiden/")
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(3000)
        
        print("=== PAGE TITLE ===")
        print(await page.title())
        
        print("\n=== LOOKING FOR 'FLER DATUM' SECTION ===")
        
        # Check for the specific selector the spider is looking for
        fler_datum = page.locator('ul.richtext')
        count = await fler_datum.count()
        print(f"Found {count} 'ul.richtext' elements")
        
        if count > 0:
            for i in range(count):
                print(f"\n--- ul.richtext #{i+1} ---")
                content = await fler_datum.nth(i).inner_text()
                print(f"Content: {content}")
                
                # Check for list items
                list_items = fler_datum.nth(i).locator('li.list-none')
                li_count = await list_items.count()
                print(f"Found {li_count} 'li.list-none' items")
                
                for j in range(li_count):
                    try:
                        li = list_items.nth(j)
                        date_span = await li.locator('span.text-4xl').inner_text()
                        time_span = await li.locator('span.text-xl').inner_text()
                        print(f"  Item {j+1}: Date='{date_span}', Time='{time_span}'")
                    except Exception as e:
                        print(f"  Error parsing item {j}: {e}")
        
        print("\n=== LOOKING FOR ALTERNATIVE DATE STRUCTURES ===")
        
        # Look for any ul elements that might contain dates
        all_uls = page.locator('ul')
        ul_count = await all_uls.count()
        print(f"Found {ul_count} total 'ul' elements")
        
        for i in range(min(ul_count, 10)):  # Check first 10 uls
            try:
                ul = all_uls.nth(i)
                text = await ul.inner_text()
                if text and any(word in text.lower() for word in ['januari', 'februari', 'mars', 'april', 'maj', 'juni', 'juli', 'augusti', 'september', 'oktober', 'november', 'december']):
                    print(f"\n--- ul #{i+1} (contains dates) ---")
                    print(f"Text: {text[:200]}...")
                    
                    # Check for li items
                    lis = ul.locator('li')
                    li_count = await lis.count()
                    print(f"Has {li_count} li items")
                    
                    for j in range(min(li_count, 10)):
                        try:
                            li_text = await lis.nth(j).inner_text()
                            print(f"  li {j+1}: {li_text}")
                        except:
                            pass
            except:
                pass
        
        print("\n=== LOOKING FOR ANY ELEMENT WITH DATES ===")
        
        # Look for any element containing date patterns
        date_elements = await page.evaluate("""
            () => {
                const results = [];
                const allElements = document.querySelectorAll('*');
                const datePattern = /\\b(\\d{1,2})\\s+(januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december)\\b/gi;
                
                allElements.forEach(el => {
                    const text = el.innerText || el.textContent;
                    if (text && text.length < 500 && datePattern.test(text)) {
                        const tagName = el.tagName.toLowerCase();
                        const className = el.className || '';
                        const id = el.id || '';
                        results.push({
                            tag: tagName,
                            class: className,
                            id: id,
                            text: text.trim(),
                            html: el.outerHTML.substring(0, 200)
                        });
                    }
                });
                
                // Remove duplicates and limit results
                const unique = [];
                const seen = new Set();
                results.forEach(r => {
                    const key = r.text.substring(0, 50);
                    if (!seen.has(key)) {
                        seen.add(key);
                        unique.push(r);
                    }
                });
                
                return unique.slice(0, 20);
            }
        """)
        
        print(f"Found {len(date_elements)} elements with date patterns:")
        for i, elem in enumerate(date_elements):
            print(f"\n{i+1}. <{elem['tag']}> class='{elem['class']}' id='{elem['id']}'")
            print(f"   Text: {elem['text']}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_armemuseum_detail())
