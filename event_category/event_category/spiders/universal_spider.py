import scrapy
import extruct
import html2text
import json
import os
import google.generativeai as genai
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem

class UniversalSpider(scrapy.Spider):
    name = "universal_events"
    MAX_EVENTS = 50 

    def configure_gemini(self):
        api_key = self.settings.get("GOOGLE_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            self.logger.error("GOOGLE_API_KEY is missing!")
            return None
        
        genai.configure(api_key=api_key)
        # Use gemini-2.0-flash (or gemini-1.5-flash-latest for older API)
        return genai.GenerativeModel('gemini-2.0-flash') 

    def start_requests(self):
        self.model = self.configure_gemini()
        url = getattr(self, 'url', None)
        if not url:
            self.logger.error("No URL provided!")
            return

        yield scrapy.Request(
            url,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "networkidle"),
                    PageMethod("wait_for_timeout", 5000),
                ],
            },
            callback=self.parse
        )

    async def parse(self, response):
        page = response.meta.get("playwright_page")
        if page:
            # === STRATEGY: Hybrid Scroll & Click ===
            # Many modern sites (Tekniska, Moderna) load on scroll. 
            # We scroll to bottom, then check for buttons.
            
            for _ in range(3):  # Scroll down 3 times
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)

            # Try to click "Load More" buttons in multiple languages
            load_more_candidates = [
                "Visa fler", "Ladda fler", "Hämta fler", # Swedish
                "Load more", "Show more", "More events", "See more" # English
            ]
            
            load_more_clicks = 0
            max_clicks = 3  # Keep it low to avoid infinite loops during testing
            
            while load_more_clicks < max_clicks:
                clicked = False
                for label in load_more_candidates:
                    # Case insensitive search for button text
                    btn = page.locator(f"button:has-text('{label}'), a:has-text('{label}')")
                    if await btn.count() > 0 and await btn.first.is_visible():
                        try:
                            self.logger.info(f"Clicking '{label}' button...")
                            await btn.first.click(timeout=5000)
                            await page.wait_for_timeout(3000)
                            clicked = True
                            load_more_clicks += 1
                            break # Break inner loop to re-evaluate page state
                        except:
                            continue
                
                if not clicked:
                    break

            content = await page.content()
            await page.close()
        else:
            content = response.text

        # === STRATEGY 1: JSON-LD ===
        base_url = response.url
        data = extruct.extract(content, base_url=base_url, syntaxes=['json-ld'])
        events_found = []
        for item in data.get('json-ld', []):
            if item.get('@type') in ['Event', 'MusicEvent', 'DanceEvent', 'SocialEvent', 'EducationEvent']:
                events_found.append(self.parse_json_ld(item))

        if events_found:
            self.logger.info(f"SUCCESS: Found {len(events_found)} events via JSON-LD.")
            for e in events_found:
                yield e
            return 

        # === STRATEGY 2: Gemini LLM ===
        if not self.model:
            return

        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        h.body_width = 0
        text_content = h.handle(content)

        # !!! CRITICAL FIX: Increased limit from 25,000 to 300,000 !!!
        # Gemini 1.5 Flash has a 1M token window. 300k chars is safe and necessary for full pages.
        extracted_data = self.call_gemini(text_content[:300000])
        
        if extracted_data:
            self.logger.info(f"SUCCESS: Gemini extracted {len(extracted_data)} events.")
            for event_data in extracted_data[:self.MAX_EVENTS]:
                yield self.normalize_llm_data(event_data, response.url)

    def call_gemini(self, text_content):
        self.logger.info(f"Calling Gemini with {len(text_content)} characters...")
        
        prompt = f"""
        You are an expert event data extractor.
        Extract a list of events from the text below.
        
        Input Text:
        {text_content}

        Instructions:
        1. Return ONLY a raw JSON List of objects. No Markdown (```json).
        2. Look for patterns like "Date", "Time", "Location" headers.
        3. If a specific year is not mentioned, assume the next upcoming occurrence relative to today.
        4. Schema:
           {{
             "event_name": "string",
             "date_iso": "YYYY-MM-DD",
             "time": "HH:MM",
             "location": "string",
             "description": "summary",
             "extra_attributes": {{ "price": "...", "organizer": "..." }}
           }}
        """

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    response_mime_type="application/json"
                )
            )
            return json.loads(response.text)
        except Exception as e:
            self.logger.error(f"Gemini failed: {e}")
            return []

    # ... (Keep parse_json_ld and normalize_llm_data as they were) ...
    def parse_json_ld(self, data):
        """Normalize Schema.org data"""
        item = EventCategoryItem()
        item['event_name'] = data.get('name')
        item['date_iso'] = data.get('startDate', '').split('T')[0]
        item['time'] = data.get('startDate', '').split('T')[1] if 'T' in data.get('startDate', '') else None
        
        loc = data.get('location', {})
        if isinstance(loc, dict):
            item['location'] = loc.get('name') or loc.get('address', {}).get('streetAddress')
        elif isinstance(loc, str):
            item['location'] = loc
            
        item['description'] = data.get('description')
        item['event_url'] = data.get('url') or data.get('@id')
        item['extra_attributes'] = {} 
        return item

    def normalize_llm_data(self, data, source_url):
        """Clean up LLM output"""
        item = EventCategoryItem()
        item['event_name'] = data.get('event_name')
        item['date_iso'] = data.get('date_iso')
        item['time'] = data.get('time')
        item['location'] = data.get('location')
        item['description'] = data.get('description')
        item['event_url'] = source_url 
        item['extra_attributes'] = data.get('extra_attributes', {})
        return item