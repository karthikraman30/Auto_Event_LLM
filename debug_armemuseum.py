import asyncio
from playwright.async_api import async_playwright

async def debug_armemuseum():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()
        
        # Go to the page
        await page.goto("https://armemuseum.se/kalender/")
        await page.wait_for_load_state("domcontentloaded")
        
        # Wait a bit for any JS to load
        await page.wait_for_timeout(3000)
        
        # Get page title
        title = await page.title()
        print(f"Page title: {title}")
        
        # Look for any links that might be events
        links = await page.evaluate("""
            () => {
                const allLinks = Array.from(document.querySelectorAll('a'));
                return allLinks.map(link => ({
                    href: link.href,
                    text: link.innerText.trim(),
                    classes: link.className
                })).filter(link => link.href && link.text);
            }
        """)
        
        print(f"\nFound {len(links)} links:")
        for i, link in enumerate(links[:20]):  # Show first 20
            print(f"{i+1}. {link['text'][:50]} -> {link['href']}")
        
        # Look for any event-related content
        event_elements = await page.evaluate("""
            () => {
                const elements = [];
                const tags = ['article', 'div', 'section', 'span', 'h1', 'h2', 'h3'];
                tags.forEach(tag => {
                    const els = document.querySelectorAll(tag);
                    els.forEach(el => {
                        const text = el.innerText.trim();
                        if (text && (text.toLowerCase().includes('event') || 
                                     text.toLowerCase().includes('kalender') ||
                                     text.toLowerCase().includes('program') ||
                                     text.toLowerCase().includes('föreläsning') ||
                                     text.toLowerCase().includes('visning'))) {
                            elements.push({
                                tag: tag,
                                text: text.substring(0, 100),
                                classes: el.className
                            });
                        }
                    });
                });
                return elements;
            }
        """)
        
        print(f"\nFound {len(event_elements)} event-related elements:")
        for i, elem in enumerate(event_elements[:10]):
            print(f"{i+1}. {elem['tag']}.{elem['classes']}: {elem['text']}")
        
        # Check the current selectors the code is looking for
        current_selectors = await page.evaluate("""
            () => {
                const results = {};
                
                // Check for event links
                const eventLinks = document.querySelectorAll('a[href*="/event/"]');
                results.event_links = eventLinks.length;
                
                // Check for specific font classes
                const fontMulish = document.querySelectorAll('span.font-mulish.font-black');
                results.font_mulish_black = fontMulish.length;
                
                const fontRoboto = document.querySelectorAll('span.text-xs.leading-7.font-roboto');
                results.font_roboto_specific = fontRoboto.length;
                
                const fontRobotoAny = document.querySelectorAll('span.font-roboto');
                results.font_roboto_any = fontRobotoAny.length;
                
                return results;
            }
        """)
        
        print(f"\nCurrent selector results:")
        for key, value in current_selectors.items():
            print(f"{key}: {value}")
        
        await context.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_armemuseum())
