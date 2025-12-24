import scrapy
import json
import os
import re
from datetime import datetime, timedelta
import google.generativeai as genai
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem
from event_category.utils.db_manager import DatabaseManager

class MultiSiteEventSpider(scrapy.Spider):
    name = "universal_events"
    
    # 1. FINAL URL LIST (All 4 Sites)
    start_urls = [
        "https://biblioteket.stockholm.se/evenemang",
        "https://biblioteket.stockholm.se/forskolor",
        "https://www.tekniskamuseet.se/pa-gang/?date=",
        "https://www.modernamuseet.se/stockholm/sv/kalender/"
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
        self.db = DatabaseManager()
        if not self.model:
            return

        # Support single URL mode for parallel execution
        single_url = getattr(self, 'url', None)
        urls_to_process = [single_url] if single_url else self.start_urls

        for url in urls_to_process:

            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_timeout", 3000), 
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
        
        # === STEP A: COOKIE CONSENT ===
        try:
            cookie_btns = page.locator("button:has-text('Godkänn'), button:has-text('Acceptera'), button:has-text('Jag förstår'), button[id*='cookie']")
            if await cookie_btns.count() > 0:
                await cookie_btns.first.click(force=True, timeout=2000)
                await page.wait_for_timeout(1000)
        except: pass

        # === STEP B: SCROLL & LOAD MORE ===
        for _ in range(4): 
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)

        # Click Load More ~40 times (Covers approx 45 days)
        load_words = ["Visa fler", "Ladda fler", "Load more", "Show more", "More events", "Nästa", "Visa alla"]
        for _ in range(40): 
            clicked = False
            for word in load_words:
                btn = page.locator(f"button:has-text('{word}'), a:has-text('{word}')").first
                if await btn.count() > 0 and await btn.is_visible():
                    try:
                        self.logger.info(f"Clicking load button: '{word}'")
                        await btn.click(force=True, timeout=5000)
                        await page.wait_for_timeout(2000)
                        clicked = True
                        break 
                    except: pass
            if not clicked: break 

        # === STEP C: ATTEMPT FAST PATH (SELECTORS) ===
        selectors = self.db.get_selectors(response.url)
        extracted_data = []
        fast_path_success = False

        if selectors:
            self.logger.info(f"Pointers found for {response.url}. Attempting Fast Path...")
            fast_data = await self.extract_with_selectors(page, selectors)
            if fast_data and len(fast_data) > 0:
                self.logger.info(f"Fast Path extracted {len(fast_data)} events.")
                extracted_data = fast_data
                fast_path_success = True
            else:
                self.logger.info("Fast Path failed or returned no data. Falling back to AI Path.")

        # === STEP D: AI PATH (IF FAST PATH FAILED) ===
        if not fast_path_success:
            self.logger.info("Extracting event elements for AI processing...")
            # Broad selectors for all 4 sites
            selector_str = (
                'article, '
                'div[class*="event"], li[class*="event"], '
                '.card, .teaser, .program-item, '
                '.activity, .listing-item, .c-card'
            )
            event_elements = await page.locator(selector_str).all()
            
            if not event_elements:
                 self.logger.info("Generic selectors found nothing. Trying broad 'article' tag.")
                 event_elements = await page.locator('article').all()

            self.logger.info(f"Found {len(event_elements)} potential event elements")

            event_batches = []
            current_batch = []
            html_snippets = []
            
            for i, element in enumerate(event_elements):
                try:
                    text = await element.inner_text()
                    clean_text = re.sub(r'\n+', '\n', text).strip()
                    
                    if len(clean_text) > 40:  
                        current_batch.append(clean_text)
                        # Keep first 3 HTML snippets for selector discovery
                        if i < 3:
                            html_snippets.append(await element.inner_html())
                    
                    if len(current_batch) >= 5:
                        event_batches.append("\n---\n".join(current_batch))
                        current_batch = []
                except Exception as e:
                    self.logger.warning(f"Error extracting text from element: {e}")
            
            if current_batch:
                event_batches.append("\n---\n".join(current_batch))

            # Process batches with AI
            all_extracted_data = []
            for i, batch_text in enumerate(event_batches):
                self.logger.info(f"Processing batch {i+1}/{len(event_batches)}")
                # For the first batch, we ask for selectors too and pass HTML
                if i == 0:
                    ai_result = self.call_ai_engine(batch_text, include_selectors=True, html_context=html_snippets)
                    if ai_result:
                        data = ai_result.get('events', [])
                        discovered_selectors = ai_result.get('selectors')
                        if discovered_selectors:
                            self.logger.info(f"AI discovered selectors: {discovered_selectors}")
                            self.db.save_selectors(
                                response.url, 
                                discovered_selectors.get('container'),
                                discovered_selectors.get('items')
                            )
                        all_extracted_data.extend(data)
                else:
                    ai_result = self.call_ai_engine(batch_text, include_selectors=False)
                    if ai_result:
                        all_extracted_data.extend(ai_result if isinstance(ai_result, list) else ai_result.get('events', []))
            
            # Deduplication
            seen = set()
            for event in all_extracted_data:
                key = (event.get('event_name', ''), event.get('date_iso', ''))
                if key not in seen:
                    seen.add(key)
                    extracted_data.append(event)

        await page.close()
        
        # === STEP E: FILTER & STORE ===
        today = datetime.now().date()
        limit_date = today + timedelta(days=45) 
        
        if extracted_data:
            self.logger.info(f"AI extracted {len(extracted_data)} unique events. Filtering dates...")
            
            for event_data in extracted_data:
                item = EventCategoryItem()
                item['event_name'] = event_data.get('event_name') or 'Unknown Event'
                item['location'] = event_data.get('location') or 'N/A'
                item['time'] = event_data.get('time') or 'N/A'
                item['description'] = event_data.get('description') or 'N/A'
                item['event_url'] = response.url 
                item['end_date_iso'] = event_data.get('end_date_iso') or 'N/A'
                
                # --- STATUS CHECK ---
                raw_status = event_data.get('status', 'scheduled').lower()
                if 'cancel' in raw_status or 'inst' in raw_status:
                    item['status'] = 'cancelled'
                else:
                    item['status'] = 'scheduled'

                # --- TARGET GROUP LOGIC ---
                # 1. STRICT OVERRIDE: If URL contains "forskolor", FORCE PRESCHOOL
                if "forskolor" in response.url:
                    item['target_group'] = "Preschool"
                    item['target_group_normalized'] = "preschool_groups"
                else:
                    # 2. STANDARD LOGIC: Use AI detection + Age Parsing
                    item['target_group'] = event_data.get('target_group', 'All')
                    item['target_group_normalized'] = self.simple_normalize(item['target_group'])

                # --- DATE PARSING & FILTERING ---
                date_str = event_data.get('date_iso')
                
                if not date_str:
                    continue

                try:
                    event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    
                    if today <= event_date <= limit_date:
                        item['date_iso'] = date_str
                        item['date'] = date_str
                        yield item
                    else:
                        pass 
                        
                except ValueError:
                    continue

    async def extract_with_selectors(self, page, selectors):
        extracted = []
        container_sel = selectors.get('container')
        item_map = selectors.get('items', {})
        
        if not container_sel: return []
        
        elements = await page.locator(container_sel).all()
        for el in elements:
            item = {}
            for field, sel in item_map.items():
                try:
                    target = el.locator(sel).first
                    if await target.count() > 0:
                        item[field] = await target.inner_text()
                    else:
                        item[field] = None
                except:
                    item[field] = None
            
            if item.get('event_name'):
                # Basic cleaning for Fast Path
                item['event_name'] = item['event_name'].strip()
                extracted.append(item)
        return extracted

    def call_ai_engine(self, text_content, include_selectors=False, html_context=None):
        selector_instructions = ""
        html_section = ""
        json_format = """
        [
          {
            "event_name": "Event Name",
            "date_iso": "2025-12-01",
            "end_date_iso": null,
            "time": "10:00",
            "location": "Venue",
            "target_group": "Adults",
            "description": "Short description.",
            "status": "scheduled"
          }
        ]
        """
        
        if include_selectors:
            if html_context:
                html_snippets_str = "\n---\n".join(html_context[:3])
                html_section = f"""
                STRUCTURE CONTEXT (HTML snippets of events):
                {html_snippets_str}
                """

            selector_instructions = """
            6. SELECTOR DISCOVERY:
               - Based on the provided HTML structure, identify the most reliable CSS selector for the event container.
               - Identify CSS selectors for EACH field (relative to the container).
               - Use stable classes or tags. Avoid dynamic IDs.
            """
            json_format = """
            {
              "events": [
                {
                  "event_name": "Event Name",
                  "date_iso": "2025-12-01",
                  "end_date_iso": null,
                  "time": "10:00",
                  "location": "Venue",
                  "target_group": "Adults",
                  "description": "Short description.",
                  "status": "scheduled"
                }
              ],
              "selectors": {
                "container": "article.event-card",
                "items": {
                  "event_name": "h2",
                  "date_iso": ".date",
                  "time": ".time",
                  "location": ".venue",
                  "description": ".teaser"
                }
              }
            }
            """

        prompt = f"""
        You are an Event Extraction Engine.
        Task: Extract a list of events from the text below.
        
        {html_section}
        
        Input Format: The text contains multiple event listings separated by "---".
        
        Current Date: {datetime.now().strftime('%Y-%m-%d')}
        IMPORTANT: For dates in January and beyond, use year 2026.
        
        Input Text:
        {text_content}
        
        Requirements:
        1. Output ONLY a valid JSON. No markdown.
        2. Extract fields: event_name, date_iso (YYYY-MM-DD), end_date_iso, time, location, target_group, description, status.
        
        3. STATUS LOGIC:
           - Look for keywords like "Inställt", "Cancelled", "Fullbokat".
           - If found, set "status": "cancelled".
           - Otherwise, set "status": "scheduled".
        
        4. DESCRIPTION EXTRACTION:
           - Extract the teaser text or subtitle. Max 250 chars.
           - Do NOT return "N/A" if text is available.
        
        5. DATE LOGIC:
           - "date_iso": Start date.
           - "end_date_iso": End date (or null).
           - Convert Swedish months (december->12, januari->01).
        
        {selector_instructions}
        
        JSON Structure:
        {json_format}
        """
        
        try:
            response = self.model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=8192, 
                )
            )
            response_text = response.text.strip()
            if response_text.startswith("```"):
                response_text = re.sub(r'^```(?:json)?\n?', '', response_text)
                response_text = re.sub(r'\n?```$', '', response_text)
            
            try:
                result = json.loads(response_text)
            except json.JSONDecodeError:
                fixed_text = response_text.rstrip()
                if fixed_text.endswith(','): fixed_text = fixed_text[:-1]
                open_braces = fixed_text.count('{') - fixed_text.count('}')
                open_brackets = fixed_text.count('[') - fixed_text.count(']')
                fixed_text += '}' * max(0, open_braces)
                fixed_text += ']' * max(0, open_brackets)
                result = json.loads(fixed_text)
            
            if isinstance(result, list): return result
            if isinstance(result, dict):
                if include_selectors:
                    return result
                for val in result.values():
                    if isinstance(val, list): return val
            return []
        except Exception as e:
            self.logger.error(f"AI Engine Error: {e}")
            return []

    def simple_normalize(self, target_str):
        """
        Normalize target group using Age Parsing and Keywords.
        """
        if not target_str: return 'all_ages'
        t = target_str.lower()
        
        # --- 1. KEYWORD CHECKS ---
        if 'barn' in t or 'kid' in t or 'bebis' in t or 'småbarn' in t or 'förskola' in t: 
            return 'children'
        
        if 'ungdom' in t or 'teen' in t or 'tonåring' in t or 'unga' in t: 
            return 'teens'
        
        if 'familj' in t or 'family' in t: 
            return 'families'
            
        if 'vuxen' in t or 'vuxna' in t or 'adult' in t or 'senior' in t: 
            return 'adults'

        # --- 2. AGE PARSING (e.g., "10-12 år", "Från 15 år") ---
        age_match = re.search(r'(\d{1,2})(?:[-–\s]+(\d{1,2}))?\s*(?:år|year|age)', t)
        
        if age_match:
            try:
                min_age = int(age_match.group(1))
                if min_age < 13:
                    return 'children'
                elif 13 <= min_age < 20:
                    return 'teens'
                else:
                    return 'adults'
            except:
                pass

        return 'all_ages'