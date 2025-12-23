import scrapy
import html2text
import json
import os
import re
from datetime import datetime, timedelta
import google.generativeai as genai
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem

class MultiSiteEventSpider(scrapy.Spider):
    name = "universal_events"
    
    # 1. DEFINE YOUR TARGET WEBSITES HERE
    start_urls = [
        "https://biblioteket.stockholm.se/evenemang"

    ]

    def configure_gemini(self):
        api_key = self.settings.get("GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not api_key:
            self.logger.error("GEMINI_API_KEY is missing! Set it in .env file.")
            return None
        genai.configure(api_key=api_key)
        return genai.GenerativeModel("gemini-2.0-flash")

    def start_requests(self):
        self.model = self.configure_gemini()
        if not self.model:
            return

        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_timeout", 3000), # Wait for initial JS
                    ],
                },
                callback=self.parse
            )

    async def parse(self, response):
        page = response.meta.get("playwright_page")
        if not page:
            self.logger.error(f"Playwright page not found for {response.url}")
            return

        self.logger.info(f"Processing Site: {response.url}")
        
        # === STEP A: AGGREGATE TEXT (Scroll & Click 'Load More') ===
        # This combines content from the whole listing into one text block
        
        # 1. Scroll to trigger lazy loading
        for _ in range(4): 
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)

        # 2. Universal 'Load More' Clicker
        # Clicks up to 15 times to get enough events for the month
        load_words = ["Visa fler", "Ladda fler", "Load more", "Show more", "More events", "Nästa"]
        for _ in range(15): 
            clicked = False
            for word in load_words:
                # Look for buttons/links containing these words
                btn = page.locator(f"button:has-text('{word}'), a:has-text('{word}')").first
                if await btn.count() > 0 and await btn.is_visible():
                    try:
                        self.logger.info(f"Clicking load button: '{word}'")
                        await btn.click(force=True, timeout=5000)
                        await page.wait_for_timeout(2000)
                        clicked = True
                        break # Move to next click iteration
                    except: pass
            if not clicked: break # No more buttons found

        content = await page.content()
        await page.close()

        # === STEP B: CLEAN HTML TO TEXT ===
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        h.body_width = 0
        # Reduce noise by ignoring nav/footer if possible, but raw HTML2Text is usually fine for LLMs
        text_content = h.handle(content)

        # === STEP C: AI ENGINE (Map Columns) ===
        # Process content in chunks to capture all events
        chunk_size = 20000
        all_extracted_data = []
        
        # Split content into chunks and process each
        for i in range(0, min(len(text_content), 80000), chunk_size):
            chunk = text_content[i:i + chunk_size]
            self.logger.info(f"Processing chunk {i // chunk_size + 1} (chars {i} to {i + len(chunk)})")
            chunk_data = self.call_ai_engine(chunk)
            if chunk_data:
                all_extracted_data.extend(chunk_data)
        
        # Remove duplicates by event_name + date_iso
        seen = set()
        extracted_data = []
        for event in all_extracted_data:
            key = (event.get('event_name', ''), event.get('date_iso', ''))
            if key not in seen:
                seen.add(key)
                extracted_data.append(event)
        
        # === STEP D: FILTER & STORE ===
        today = datetime.now().date()
        next_month = today + timedelta(days=30)
        
        if extracted_data:
            self.logger.info(f"AI extracted {len(extracted_data)} raw events from {response.url}")
            
            for event_data in extracted_data:
                # 1. Map AI output to our Item Columns
                item = EventCategoryItem()
                item['event_name'] = event_data.get('event_name') or 'Unknown Event'
                item['location'] = event_data.get('location') or 'N/A'
                item['time'] = event_data.get('time') or 'N/A'
                item['description'] = event_data.get('description') or 'N/A'
                item['event_url'] = response.url # Link back to the main list
                item['status'] = 'scheduled'
                
                # 2. Date Parsing & Filtering (Next 1 Month)
                date_str = event_data.get('date_iso') # AI returns YYYY-MM-DD
                
                if date_str:
                    try:
                        event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        
                        # LOGIC: Filter for next 1 month
                        if today <= event_date <= next_month:
                            item['date_iso'] = date_str
                            item['date'] = date_str
                            item['target_group'] = event_data.get('target_group', 'All')
                            # Normalize target group roughly for Excel
                            item['target_group_normalized'] = self.simple_normalize(item['target_group'])
                            
                            yield item
                        else:
                            self.logger.debug(f"Skipping date {event_date}: Outside 1-month range")
                            
                    except ValueError:
                        self.logger.warning(f"Date parse error: {date_str}")
                        continue

    def call_ai_engine(self, text_content):
        """
        The 'AI Engine' that maps scraped text to columns using Gemini.
        """
        prompt = f"""
        You are an Event Extraction Engine.
        Task: Extract a list of events from the text below.
        
        Current Date: {datetime.now().strftime('%Y-%m-%d')}
        IMPORTANT: For dates in January and beyond, use year 2026 (since current date is December 2025).
        
        Input Text:
        {text_content}
        
        Requirements:
        1. Output ONLY a valid JSON list of objects. No markdown, no code fences.
        2. Extract fields: event_name, date_iso (YYYY-MM-DD), time, location, target_group (e.g. "Kids", "Adults"), description.
        3. KEEP DESCRIPTIONS VERY SHORT - maximum 50 characters.
        4. TIME EXTRACTION:
           - If the event name contains time (e.g. "Fri fredag kl 18-20"), extract "18:00-20:00" to the time field.
           - The event_name should NOT include the time - just the event title (e.g. "Fri fredag").
           - Use 24-hour format for times (e.g., "14:00", "18:00-20:00").
        5. DATE LOGIC:
           - December 2025 dates: use 2025-12-XX
           - January onwards dates: use 2026-01-XX, 2026-02-XX, etc.
           - Convert written dates (e.g., "25 dec", "20 jan") to YYYY-MM-DD format.
        
        JSON Structure:
        [
          {{
            "event_name": "Fri fredag",
            "date_iso": "2025-12-26",
            "time": "18:00-20:00",
            "location": "Main Hall",
            "target_group": "Adults",
            "description": "Free Friday event with art"
          }}
        ]
        """
        
        try:
            response = self.model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=8192,  # Prevent truncation
                )
            )
            
            # Extract JSON from response, handling potential markdown code blocks
            response_text = response.text.strip()
            
            # Remove markdown code fences if present
            if response_text.startswith("```"):
                # Remove opening fence (```json or ```)
                response_text = re.sub(r'^```(?:json)?\n?', '', response_text)
                # Remove closing fence
                response_text = re.sub(r'\n?```$', '', response_text)
            
            # Try to parse JSON, with fallback for truncated responses
            try:
                result = json.loads(response_text)
            except json.JSONDecodeError as json_err:
                self.logger.warning(f"JSON parse error, attempting to fix: {json_err}")
                # Try to fix truncated JSON by closing brackets
                fixed_text = response_text.rstrip()
                # Remove any incomplete object/string at the end
                if fixed_text.endswith(','):
                    fixed_text = fixed_text[:-1]
                # Count and close open brackets
                open_braces = fixed_text.count('{') - fixed_text.count('}')
                open_brackets = fixed_text.count('[') - fixed_text.count(']')
                fixed_text += '}' * max(0, open_braces)
                fixed_text += ']' * max(0, open_brackets)
                result = json.loads(fixed_text)
            
            # Handle variations in JSON structure (root object vs list)
            if isinstance(result, list): return result
            if isinstance(result, dict):
                # Return the first list found in the dict
                for val in result.values():
                    if isinstance(val, list): return val
            return []
            
        except Exception as e:
            self.logger.error(f"AI Engine Error: {e}")
            return []

    # Target group mapping for normalization
    TARGET_GROUP_MAPPING = {
        # Children (0-12)
        'barn': 'children',
        'bebis': 'children',
        'småbarn': 'children',
        'förskolebarn': 'children',
        'kid': 'children',
        'child': 'children',
        # Teens (13-19)
        'ungdom': 'teens',
        'tonåring': 'teens',
        'ungdomar': 'teens',
        'teen': 'teens',
        # Adults (20+)
        'vuxen': 'adults',
        'vuxna': 'adults',
        'senior': 'adults',
        'seniorer': 'adults',
        'pensionär': 'adults',
        'adult': 'adults',
        # Families
        'familj': 'families',
        'familjer': 'families',
        'family': 'families',
        'families': 'families',
        # All ages
        'alla': 'all_ages',
        'alla åldrar': 'all_ages',
        'all': 'all_ages',
        'general': 'all_ages',
    }

    def simple_normalize(self, target_str):
        """Normalize target group using comprehensive mapping"""
        if not target_str:
            return 'all_ages'
        
        t = target_str.lower()
        
        # Check for exact or partial matches in mapping
        for key, value in self.TARGET_GROUP_MAPPING.items():
            if key in t:
                return value
        
        return 'all_ages'