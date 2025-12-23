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
    MAX_EVENTS = 50  # Limit for testing

    def configure_gemini(self):
        """Setup Google Gemini API"""
        # Get API key from settings or environment variable
        api_key = self.settings.get("GOOGLE_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            self.logger.error("GOOGLE_API_KEY is missing! Set it in settings.py or ENV.")
            return None
        
        genai.configure(api_key=api_key)
        # Using the specific model you requested
        return genai.GenerativeModel('gemini-2.5-flash') 

    def start_requests(self):
        # 1. Initialize Gemini
        self.model = self.configure_gemini()
        
        # 2. Get URL from command line argument
        url = getattr(self, 'url', None)
        if not url:
            self.logger.error("No URL provided! Usage: scrapy crawl universal_events -a url='https://...'" )
            return

        yield scrapy.Request(
            url,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "networkidle"),  # Wait for network to be idle
                    PageMethod("wait_for_timeout", 5000),  # Additional wait for JS rendering
                ],
            },
            callback=self.parse
        )

    async def parse(self, response):
        page = response.meta.get("playwright_page")
        if page:
            # Click "Visa fler evenemang" button to load more events
            load_more_clicks = 0
            # Calculate clicks needed: initial ~20 events + ~20 per click
            # For 50 events: need 2 clicks (20 + 20 + 20 = 60, then limit to 50)
            max_clicks = max(1, (self.MAX_EVENTS - 20) // 20 + 1)
            
            while load_more_clicks < max_clicks:
                try:
                    # Look for the "load more" button
                    load_more_btn = page.locator('button:has-text("Visa fler")')
                    if await load_more_btn.count() > 0 and await load_more_btn.first.is_visible():
                        await load_more_btn.first.click()
                        load_more_clicks += 1
                        self.logger.info(f"Clicked 'Load more' button ({load_more_clicks}/{max_clicks})")
                        await page.wait_for_timeout(2000)  # Wait for content to load
                    else:
                        self.logger.info("No more 'Load more' button found")
                        break
                except Exception as e:
                    self.logger.info(f"Stopped loading more: {e}")
                    break
            
            content = await page.content()
            await page.close()
        else:
            content = response.text

        # === STRATEGY 1: JSON-LD (Structured Data) ===
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
            return # Exit to save tokens

        # === STRATEGY 2: Gemini LLM Fallback ===
        self.logger.info("No JSON-LD found. Falling back to Gemini LLM...")
        
        if not self.model:
            self.logger.error("Gemini model not configured. Skipping LLM extraction.")
            return

        # 1. Convert HTML to clean text
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        h.body_width = 0
        text_content = h.handle(content)

        # 2. Call Gemini
        extracted_data = self.call_gemini(text_content)
        
        if extracted_data:
            self.logger.info(f"SUCCESS: Gemini extracted {len(extracted_data)} events.")
            for i, event_data in enumerate(extracted_data[:self.MAX_EVENTS]):
                yield self.normalize_llm_data(event_data, response.url)

    def call_gemini(self, text_content):
        """Send text to Gemini and get JSON back"""
        
        self.logger.info(f"Calling Gemini with {len(text_content)} characters of text...")
        
        # Define the schema structure for Gemini
        prompt = f"""
        You are an expert event data extractor. 
        Analyze the text below and extract all events into a JSON list.

        Input Text:
        {text_content[:25000]}

        Output Instructions:
        1. Return ONLY a valid JSON List of objects. No markdown formatting.
        2. Use this schema for each event:
           {{
             "event_name": "string",
             "date_iso": "YYYY-MM-DD" (or null if missing),
             "time": "HH:MM" (or null if missing),
             "location": "string" (or null),
             "description": "short summary string",
             "extra_attributes": {{ "key": "value" }} (Put price, speakers, organizers here)
           }}
        3. If a field is missing, strictly set it to null.
        """

        try:
            # Generate content
            self.logger.info("Sending request to Gemini API...")
            response = self.model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    response_mime_type="application/json"
                )
            )
            
            self.logger.info(f"Gemini response received. Raw text length: {len(response.text) if response.text else 0}")
            self.logger.debug(f"Gemini raw response (first 500 chars): {response.text[:500] if response.text else 'None'}")
            
            result = json.loads(response.text)
            
            # Handle if Gemini wraps it in a root object like {"events": [...]}
            if isinstance(result, dict):
                # Look for a list value inside the dict
                for key, value in result.items():
                    if isinstance(value, list):
                        self.logger.info(f"Found {len(value)} events in nested dict key '{key}'")
                        return value
                # If no list found, maybe the dict itself is a single event
                self.logger.info("Single event dict found")
                return [result]
            
            self.logger.info(f"Direct list returned with {len(result)} events")
            return result
            
        except Exception as e:
            import traceback
            self.logger.error(f"Gemini Extraction failed: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return []

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
        item['extra_attributes'] = {} # Empty for JSON-LD usually
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