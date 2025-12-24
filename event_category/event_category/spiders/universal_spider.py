import scrapy
import json
import os
import re
from datetime import datetime, timedelta
# [NEW] Import the new Google GenAI library
from google import genai
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem
from event_category.utils.db_manager import DatabaseManager

# Swedish month name to number mapping
SWEDISH_MONTHS = {
    'januari': 1, 'jan': 1,
    'februari': 2, 'feb': 2,
    'mars': 3, 'mar': 3,
    'april': 4, 'apr': 4,
    'maj': 5,
    'juni': 6, 'jun': 6,
    'juli': 7, 'jul': 7,
    'augusti': 8, 'aug': 8,
    'september': 9, 'sep': 9, 'sept': 9,
    'oktober': 10, 'okt': 10,
    'november': 11, 'nov': 11,
    'december': 12, 'dec': 12,
}

def parse_swedish_date(date_str):
    """
    Parse Swedish date string to ISO format (YYYY-MM-DD).
    Handles formats like: '25 december', 'tis 24 dec', '2025-01-15', '2025-12-26 10:30'
    Returns None if parsing fails.
    """
    if not date_str:
        return None
    
    date_str = date_str.strip().lower()
    
    # Already in ISO format (with or without time)?
    # Handles: "2025-12-26" or "2025-12-26 10:30"
    iso_match = re.match(r'^(\d{4}-\d{2}-\d{2})', date_str)
    if iso_match:
        return iso_match.group(1)  # Return just the date part
    
    # Try to extract day and month
    # Pattern: optional weekday, day number, month name
    match = re.search(r'(\d{1,2})\s+([a-zåäö]+)', date_str)
    if match:
        day = int(match.group(1))
        month_name = match.group(2)
        month = SWEDISH_MONTHS.get(month_name)
        
        if month:
            today = datetime.now()
            year = today.year
            # If month is earlier than current month, assume next year
            if month < today.month or (month == today.month and day < today.day):
                year += 1
            return f"{year}-{month:02d}-{day:02d}"
    
    return None

def extract_time_only(time_str):
    """
    Extract only the time component from a datetime string.
    Handles: "2025-12-26 10:30" → "10:30", "10.30" → "10:30", "Tid: 14:00-15:00" → "14:00-15:00"
    """
    if not time_str:
        return 'N/A'
    
    time_str = time_str.strip()
    
    # Pattern 1: datetime format "2025-12-26 10:30" or "2025-12-26T10:30"
    match = re.search(r'\d{4}-\d{2}-\d{2}[T\s](\d{1,2}[:.]\d{2}(?:[:.]\d{2})?)', time_str)
    if match:
        return match.group(1).replace('.', ':')
    
    # Pattern 2: Swedish format "Tid: 14:00-15:00" or "Tid: 14:00"
    match = re.search(r'Tid:\s*(\d{1,2}[:.]\d{2}(?:\s*-\s*\d{1,2}[:.]\d{2})?)', time_str, re.IGNORECASE)
    if match:
        return match.group(1).replace('.', ':')
    
    # Pattern 3: Just time like "10:30" or "10.30" or "10:30-12:00"
    match = re.search(r'^(\d{1,2}[:.]\d{2}(?:\s*-\s*\d{1,2}[:.]\d{2})?)$', time_str)
    if match:
        return match.group(1).replace('.', ':')
    
    return time_str  # Return as-is if no pattern matches

def extract_target_from_name(event_name):
    """
    Extract target group from event name based on age patterns.
    Examples: "för 3-6 år" → "children", "för 7 år och upp" → "children", 
              "4-12 månader" → "babies"
    Returns tuple: (target_group_display, target_group_normalized) or (None, None)
    """
    if not event_name:
        return None, None
    
    name_lower = event_name.lower()
    
    # Pattern: babies (månader = months)
    if 'månader' in name_lower or 'mån' in name_lower:
        match = re.search(r'(\d+)[-–]?(\d+)?\s*månader?', name_lower)
        if match:
            return "Babies (0-12 months)", "babies"
    
    # Pattern: age ranges like "3-6 år", "för 3–6 år", "7 år och upp"
    match = re.search(r'(?:för\s+)?(\d+)[-–](\d+)\s*år', name_lower)
    if match:
        min_age = int(match.group(1))
        max_age = int(match.group(2))
        
        if max_age <= 6:
            return f"Children ({min_age}-{max_age} years)", "children"
        elif min_age <= 12:
            return f"Children ({min_age}-{max_age} years)", "children"
        elif min_age < 18:
            return f"Teens ({min_age}-{max_age} years)", "teens"
        else:
            return f"Adults ({min_age}+ years)", "adults"
    
    # Pattern: "för X år och upp" or "från X år" - MUST have keyword prefix
    # Avoids false positives like "Rauschenberg 100 år"
    match = re.search(r'(?:för|från)\s+(\d+)\s*år(?:\s*och\s*upp|\s*uppåt|\s*\+)?', name_lower)
    if match:
        min_age = int(match.group(1))
        if min_age <= 6:
            return f"Children ({min_age}+ years)", "children"
        elif min_age <= 12:
            return f"Children ({min_age}+ years)", "children"
        elif min_age < 18:
            return f"Teens ({min_age}+ years)", "teens"
        else:
            return f"Adults ({min_age}+ years)", "adults"
    
    # Keywords for families
    if 'familj' in name_lower or 'family' in name_lower:
        return "Families", "families"
    
    # Keywords for babies
    if 'baby' in name_lower or 'bebis' in name_lower:
        return "Babies", "babies"
    
    return None, None

class MultiSiteEventSpider(scrapy.Spider):
    name = "universal_events"
    
    # 1. FINAL URL LIST (All 4 Sites)
    start_urls = [
        "https://biblioteket.stockholm.se/evenemang",
        "https://biblioteket.stockholm.se/forskolor",
        # "https://armemuseum.se/kalender/",
        # "https://www.modernamuseet.se/stockholm/sv/kalender/"
    ]

    def configure_gemini(self):
        """Initialize the Gemini Client using the new SDK."""
        api_key = self.settings.get("GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not api_key:
            self.logger.error("GEMINI_API_KEY is missing! Set it in .env file.")
            return None
        
        # [NEW] Return the Client object
        return genai.Client(api_key=api_key)

    def start_requests(self):
        self.client = self.configure_gemini()
        self.db = DatabaseManager()
        
        if not self.client:
            self.logger.critical("Failed to initialize Gemini Client. Stopping spider.")
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
                '.activity, .listing-item, .c-card, '
                '#properties-list > a'
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
        limit_date = today + timedelta(days=30)  # 1 month from today
        
        if extracted_data:
            self.logger.info(f"AI extracted {len(extracted_data)} unique events. Filtering dates...")
            
            for event_data in extracted_data:
                item = EventCategoryItem()
                event_name = event_data.get('event_name') or 'Unknown Event'
                item['event_name'] = event_name
                item['location'] = event_data.get('location') or 'N/A'
                
                # Extract only time component (not full datetime)
                raw_time = event_data.get('time') or 'N/A'
                item['time'] = extract_time_only(raw_time)
                
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
                    # 2. Try to extract target group from event name (age patterns)
                    name_target, name_target_norm = extract_target_from_name(event_name)
                    if name_target:
                        item['target_group'] = name_target
                        item['target_group_normalized'] = name_target_norm
                    else:
                        # 3. FALLBACK: Use AI detection + Age Parsing
                        item['target_group'] = event_data.get('target_group', 'All')
                        item['target_group_normalized'] = self.simple_normalize(item['target_group'])

                # --- DATE PARSING & FILTERING ---
                raw_date = event_data.get('date_iso')
                
                if not raw_date:
                    continue

                # Try parsing Swedish date format (from Fast Path) or ISO format (from AI)
                date_str = parse_swedish_date(raw_date)
                if not date_str:
                    # If parse_swedish_date returns None, skip this event
                    continue
                
                try:
                    event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    
                    if today <= event_date <= limit_date:
                        item['date_iso'] = date_str
                        item['date'] = date_str
                        # [new] Instead of yielding item, go to detail page
                        yield scrapy.Request(
                            item['event_url'],
                            callback=self.parse_details,
                            meta={'item': item},
                            dont_filter=True 
                        )
                        
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
                        value = None
                        
                        # For date/time fields, try to get datetime attribute from <time> elements
                        if field in ('date_iso', 'time') and 'time' in sel:
                            # Try to get the datetime attribute first
                            datetime_attr = await target.get_attribute('datetime')
                            if datetime_attr:
                                value = datetime_attr
                            else:
                                value = await target.inner_text()
                        else:
                            value = await target.inner_text()
                        
                        item[field] = value.strip() if value else None
                    else:
                        item[field] = None
                except:
                    item[field] = None
            
            if item.get('event_name'):
                # Basic cleaning for Fast Path
                item['event_name'] = item['event_name'].strip()
                extracted.append(item)
        return extracted

    async def parse_details(self, response):
        item = response.meta['item']
        self.logger.info(f"Extracting details for: {item['event_name']}")
        
        # simple text extraction
        text = " ".join(response.xpath('//body//text()').getall())
        text = re.sub(r'\s+', ' ', text).strip()
        
        input_text = f"Event Name: {item['event_name']}\n\n" + text[:8000]
        ai_result = self.call_ai_engine(input_text, extract_details=True)
        
        if ai_result:
             # handle list return (take first item)
             if isinstance(ai_result, list) and len(ai_result) > 0:
                 details = ai_result[0]
             elif isinstance(ai_result, dict):
                 details = ai_result
             else:
                 details = {}

             if details.get('description'):
                 item['description'] = details['description']
             if details.get('location'):
                 item['location'] = details['location']
             if details.get('target_group'):
                 item['target_group'] = details['target_group']
                 item['target_group_normalized'] = self.simple_normalize(details['target_group'])
        
        yield item

    def call_ai_engine(self, text_content, include_selectors=False, html_context=None, **kwargs):
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

        if kwargs.get('extract_details'):
            json_format = """
            {
              "description": "Full description...",
              "location": "Full address...",
              "target_group": "Children (3-6 years)"
            }
            """
            prompt = f"""
            Task: Extract event details from the text below.
            
            Input Text:
            {text_content}
            
            Fields to Extract:
            1. description: The full event description.
            2. location: The specific room, place, or address.
            3. target_group: Who is this for? (e.g. "Adults", "Children 3-5 years", "Families").
            
            Return JSON only.
            {json_format}
            """
        else:
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
            # [NEW] Use the new generate_content syntax
            response = self.client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
                config={
                    "response_mime_type": "application/json",
                    "temperature": 0.1,
                }
            )
            
            # [NEW] Access text directly from the response object
            response_text = response.text.strip()
            
            # Clean up potential markdown formatting
            if response_text.startswith("```"):
                response_text = re.sub(r'^```(?:json)?\n?', '', response_text)
                response_text = re.sub(r'\n?```$', '', response_text)
            
            try:
                result = json.loads(response_text)
            except json.JSONDecodeError:
                # Try simple auto-repair for truncated JSON
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