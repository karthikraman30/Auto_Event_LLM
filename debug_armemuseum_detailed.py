import asyncio
from playwright.async_api import async_playwright

async def debug_armemuseum_detailed():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()
        
        # Go to the page
        await page.goto("https://armemuseum.se/kalender/")
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(3000)
        
        # Test the exact JavaScript from the spider
        cards = await page.evaluate("""() => {const c = []; const links = Array.from(document.querySelectorAll('a[href*="/event/"]')); links.forEach(l => {const n = l.querySelector('span.font-mulish.font-black'); const d = l.querySelector('span.text-xs.leading-7.font-roboto') || l.querySelector('span.font-roboto'); if (n && d && l.href.includes('/event/')) {c.push({name: n.innerText.trim(), dateRange: d.innerText.trim(), url: l.href})}}); const seen = new Set(); return c.filter(x => {if (seen.has(x.url)) return false; seen.add(x.url); return true})}""")
        
        print(f"JavaScript extraction found {len(cards)} cards")
        
        # Debug each step of the selection
        debug_info = await page.evaluate("""
            () => {
                const results = {
                    event_links: [],
                    with_name: [],
                    with_date: [],
                    final_cards: []
                };
                
                // Step 1: Find all event links
                const links = Array.from(document.querySelectorAll('a[href*="/event/"]'));
                results.event_links = links.map(l => ({
                    href: l.href,
                    text: l.innerText.trim(),
                    hasName: !!l.querySelector('span.font-mulish.font-black'),
                    hasDate: !!(l.querySelector('span.text-xs.leading-7.font-roboto') || l.querySelector('span.font-roboto'))
                }));
                
                // Step 2: Filter links with name and date
                links.forEach(l => {
                    const n = l.querySelector('span.font-mulish.font-black');
                    const d = l.querySelector('span.text-xs.leading-7.font-roboto') || l.querySelector('span.font-roboto');
                    if (n && d && l.href.includes('/event/')) {
                        results.with_name.push({
                            name: n.innerText.trim(),
                            dateRange: d.innerText.trim(),
                            url: l.href
                        });
                    }
                });
                
                return results;
            }
        """)
        
        print(f"\nDebug info:")
        print(f"Total event links found: {len(debug_info['event_links'])}")
        
        for i, link in enumerate(debug_info['event_links']):
            print(f"\nLink {i+1}:")
            print(f"  URL: {link['href']}")
            print(f"  Text: {link['text'][:100]}")
            print(f"  Has name selector: {link['hasName']}")
            print(f"  Has date selector: {link['hasDate']}")
        
        print(f"\nLinks with both name and date: {len(debug_info['with_name'])}")
        for i, card in enumerate(debug_info['with_name']):
            print(f"\nCard {i+1}:")
            print(f"  Name: {card['name']}")
            print(f"  Date: {card['dateRange']}")
            print(f"  URL: {card['url']}")
        
        await context.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_armemuseum_detailed())
