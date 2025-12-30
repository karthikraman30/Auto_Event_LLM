import scrapy
import json
import os
import re
from datetime import datetime, timedelta
# [NEW] Import the new Google GenAI library
import google.generativeai as genai
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem
from event_category.utils.db_manager import DatabaseManager
# [NEW] For Auto Selector Discovery system
from event_category.utils.auto_selector_discovery import EventScraperOrchestrator
# [NEW] For Selector Discovery Service (AI-based)
from event_category.utils.selector_discovery_service import SelectorDiscoveryService
# [NEW] For generic pagination handler
from event_category.utils.pagination_handler import PaginationHandler
# [NEW] For Cloudflare bypass (Tekniska museet)
import cloudscraper
from bs4 import BeautifulSoup

# Swedish month name to number mapping
SWEDISH_MONTHS = {
    'januari': 1, 'jan': 1, 'january': 1,
    'februari': 2, 'feb': 2, 'february': 2,
    'mars': 3, 'mar': 3, 'march': 3,
    'april': 4, 'apr': 4,
    'maj': 5, 'may': 5,
    'juni': 6, 'jun': 6, 'june': 6,
    'juli': 7, 'jul': 7, 'july': 7,
    'augusti': 8, 'aug': 8, 'august': 8,
    'september': 9, 'sep': 9, 'sept': 9,
    'oktober': 10, 'okt': 10, 'october': 10, 'oct': 10,
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
            # Check for explicit year in the string (e.g. "26 dec 2025")
            year_match = re.search(r'\b(20\d{2})\b', date_str)
            if year_match:
                year = int(year_match.group(1))
            else:
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

def extract_location_from_title(event_title):
    """
    Extract location from Skansen event titles.
    Examples: 
        "Food and beverages at Bollnästorget" → "Bollnästorget"
        "Christmas concerts in Seglora Church" → "Seglora Church"
        "Delsbo Farmstead" → "Delsbo Farmstead" (title is the location)
    Returns extracted location or "Skansen" as default.
    """
    if not event_title:
        return "Skansen"
    
    title = event_title.strip()
    
    # Pattern 1: "... at [Location]" (English)
    match = re.search(r'\bat\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', title, re.IGNORECASE)
    if match:
        location = match.group(1).strip()
        if len(location) > 2:  # Avoid single letters
            return location
    
    # Pattern 2: "... in [Location]" (English)
    match = re.search(r'\bin\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', title, re.IGNORECASE)
    if match:
        location = match.group(1).strip()
        if len(location) > 2:
            return location
    
    # Pattern 3: "... i [Location]" (Swedish)
    match = re.search(r'\bi\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', title, re.IGNORECASE)
    if match:
        location = match.group(1).strip()
        if len(location) > 2:
            return location
    
    # Pattern 4: "... på [Location]" (Swedish "at")
    match = re.search(r'\bpå\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', title, re.IGNORECASE)
    if match:
        location = match.group(1).strip()
        if len(location) > 2:
            return location
    
    # Pattern 5: Check if title itself is a location name (contains "Farmstead", "Church", etc.)
    location_keywords = ['farmstead', 'church', 'kyrka', 'gård', 'torg', 'stage', 'hall', 'house', 'hus']
    title_lower = title.lower()
    for keyword in location_keywords:
        if keyword in title_lower:
            return title  # The title itself is the location
    
    return "Skansen"  # Default fallback

def detect_cancelled_status(event_name, description='', status_text=''):
    """
    Detect if an event is cancelled or fully booked based on keywords.
    Checks event name, description, and status text for cancellation indicators.
    Returns: 'cancelled', 'fullbokat', or 'scheduled'
    """
    # Combine all text to search
    combined = f"{event_name} {description} {status_text}".lower()
    
    # Cancellation keywords (Swedish and English)
    cancelled_keywords = [
        'inställt', 'inställd', 'cancelled', 'canceled', 
        'avlyst', 'avlyser', 'ställs in', 'avbokat'
    ]
    
    # Fully booked keywords
    fullbokat_keywords = ['fullbokat', 'fullbokad', 'fully booked', 'sold out', 'slutsålt']
    
    # Check for cancelled
    for keyword in cancelled_keywords:
        if keyword in combined:
            return 'cancelled'
    
    # Check for fully booked
    for keyword in fullbokat_keywords:
        if keyword in combined:
            return 'fullbokat'
    
    return 'scheduled'

def extract_booking_info(booking_text):
    """
    Extract booking information from Stockholm Library event text.
    Returns: 'Requires booking', 'Fullbokat', 'Drop-in', or 'N/A'
    """
    if not booking_text:
        return 'N/A'
    
    text = booking_text.lower()
    
    # Check for "Fullbokat" (fully booked)
    if 'fullbokat' in text or 'fullbokad' in text:
        return 'Fullbokat'
    
    # Check for booking required
    if 'boka plats' in text or 'du behöver boka' in text or 'bokning krävs' in text:
        return 'Requires booking'
    
    # Check for booking opens info (also means booking required)
    if 'bokningen öppnar' in text:
        return 'Requires booking'
    
    # Check for drop-in (no booking needed)
    if 'drop-in' in text or 'dropin' in text:
        return 'Drop-in'
    
    return 'N/A'

class MultiSiteEventSpider(scrapy.Spider):
    name = "universal_events"
    
    # 1. FINAL URL LIST (All Sites + Test)
    start_urls = [
        "https://biblioteket.stockholm.se/evenemang",
        "https://biblioteket.stockholm.se/forskolor",
        "https://www.skansen.se/en/calendar/",
        "https://www.tekniskamuseet.se/pa-gang/",
        "https://armemuseum.se/kalender/",
        "https://www.modernamuseet.se/stockholm/sv/kalender/",
        # [TEST] National Museum - Testing AI fallback
        "https://www.nationalmuseum.se/kalendarium"
    ]

    def configure_gemini(self):
        """Initialize the Gemini API using google.generativeai."""
        api_key = self.settings.get("GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
        if not api_key:
            self.logger.error("GEMINI_API_KEY is missing! Set it in .env file.")
            return None
        
        # Configure the API key for google.generativeai
        genai.configure(api_key=api_key)
        
        # Return a wrapper object that mimics the Client interface
        class GeminiClient:
            def __init__(self, api_key):
                self.api_key = api_key
                self.models = self
            
            def generate_content(self, model, contents, config=None):
                """Generate content using genai.GenerativeModel"""
                gen_model = genai.GenerativeModel(model)
                response = gen_model.generate_content(
                    contents,
                    generation_config=genai.types.GenerationConfig(
                        temperature=config.get('temperature', 0.1) if config else 0.1,
                        response_mime_type=config.get('response_mime_type', 'text/plain') if config else 'text/plain'
                    )
                )
                return response
        
        return GeminiClient(api_key)

    def start_requests(self):
        self.client = self.configure_gemini()
        self.db = DatabaseManager()
        
        # [NEW] Initialize Auto Selector Discovery Orchestrator with DB manager
        self.orchestrator = EventScraperOrchestrator(
            ai_client=self.client,
            logger=self.logger,
            db_manager=self.db  # Pass DB manager for selector saving
        )
        
        if not self.client:
            self.logger.critical("Failed to initialize Gemini Client. Stopping spider.")
            return

        # Support single URL mode for parallel execution
        single_url = getattr(self, 'url', None)
        urls_to_process = [single_url] if single_url else self.start_urls

        for url in urls_to_process:
            # [NEW] Special handling for Cloudflare-protected sites (Tekniska museet)
            if "tekniskamuseet.se" in url:
                self.logger.info(f"Using Cloudflare bypass mode for: {url}")
                yield scrapy.Request(
                    url,
                    meta={
                        "playwright": True,
                        "playwright_include_page": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_timeout", 8000),  # Wait for Cloudflare challenge
                            PageMethod("wait_for_load_state", "networkidle"),
                            PageMethod("wait_for_timeout", 3000),  # Extra wait after load
                        ],
                        "handle_httpstatus_list": [403],  # Allow 403 responses to pass
                    },
                    callback=self.parse,
                    dont_filter=True
                )
            else:
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

        # === STEP A.5: GENERIC PAGINATION FOR NEW SITES ===
        # This runs BEFORE site-specific handlers
        # Only applies to NEW/UNKNOWN sites (not Skansen, Tekniska, Moderna, Armemuseum)
        is_known_site = any([
            "skansen.se" in response.url,
            "tekniskamuseet.se" in response.url,
            "modernamuseet.se" in response.url,
            "armemuseum.se" in response.url
        ])
        
        if not is_known_site:
            self.logger.info("NEW SITE: Applying generic pagination handler...")
            try:
                pagination_handler = PaginationHandler(self.logger)
                pagination_count = await pagination_handler.handle_pagination(page)
                if pagination_count > 0:
                    self.logger.info(f"✅ Pagination complete: {pagination_count} actions performed")
                    await page.wait_for_timeout(1000)  # Wait after final pagination
            except Exception as e:
                self.logger.warning(f"Pagination error: {e}")

        if "skansen.se" in response.url:
            self.logger.info("Detected Skansen. Using Day-by-Day Crawling Strategy.")
            
            # [NEW] Buffer to collect events across all days for consolidation
            # Key: event_name, Value: dict with event data and list of dates
            event_buffer = {}
            
            # Loop for 30 days
            for day_offset in range(30):
                # 1. Extract Date
                # Selector: .calendarTopBar__dropdownButton span.p (e.g. "Select date: Today, 24 December 2025")
                try:
                    date_el = page.locator(".calendarTopBar__dropdownButton span.p")
                    date_text = await date_el.inner_text()
                    # Clean text: remove "Select date:" prefix
                    date_text = date_text.replace("Select date:", "").strip()
                    # Parse "Today, 24 December 2025" or just "24 December 2025"
                    # We can use our parse_swedish_date helper, but might need to split off "Today,"
                    if "," in date_text:
                        date_text = date_text.split(",", 1)[1].strip()
                    
                    current_date_iso = parse_swedish_date(date_text)
                    if not current_date_iso:
                        self.logger.warning(f"Could not parse date: '{date_text}'. Stopping Skansen loop.")
                        break
                        
                    self.logger.info(f"Scraping Skansen for date: {current_date_iso}")
                except Exception as e:
                    self.logger.error(f"Error extracting date on Skansen: {e}")
                    break

                # 2. Extract Events on current page
                # Try to use DB selectors first
                skansen_selectors = self.db.get_selectors(response.url)
                extracted_on_page = []

                if skansen_selectors:
                    self.logger.info(f"Using DB selectors for Skansen: {skansen_selectors.get('container')}")
                    extracted_on_page = await self.extract_with_selectors(page, skansen_selectors)
                    
                    # [MODIFIED] Buffer events instead of yielding immediately
                    for item_data in extracted_on_page:
                        event_name = item_data.get('event_name')
                        if not event_name:
                            continue
                        
                        # If event not in buffer, create new entry
                        if event_name not in event_buffer:
                            # URL handling
                            raw_url = item_data.get('event_url')
                            event_url = response.urljoin(raw_url) if raw_url else response.url
                            
                            # Target Group
                            tg_raw = item_data.get('target_group')
                            if tg_raw:
                                tg_cleaned = tg_raw.replace('\n', ', ')
                            else:
                                tg_cleaned = "All"
                            
                            event_buffer[event_name] = {
                                'event_name': event_name,
                                'event_url': event_url,
                                'time': extract_time_only(item_data.get('time')),
                                'description': item_data.get('description') or 'N/A',
                                'location': extract_location_from_title(event_name),
                                'target_group': tg_cleaned,
                                'target_group_normalized': self.simple_normalize(tg_cleaned),
                                'status': detect_cancelled_status(event_name, item_data.get('description', '')),
                                'booking_info': 'N/A',  # Not available for Skansen
                                'dates': [current_date_iso]  # Track all dates
                            }
                        else:
                            # Event exists, add this date to the list
                            event_buffer[event_name]['dates'].append(current_date_iso)
                else:
                    self.logger.info("No DB selectors for Skansen. Using fallback hardcoded logic.")
                    # Fallback Hardcoded Logic
                    events = await page.locator("ul.calendarList__list li.calendarItem").all()
                    self.logger.info(f"Found {len(events)} events for {current_date_iso}")
                    
                    for event in events:
                        # Title
                        title_el = event.locator(".calendarItem__titleLink h5")
                        if await title_el.count() > 0:
                            event_name = await title_el.inner_text()
                        else:
                            continue  # Skip if no title
    
                        # If event not in buffer, create new entry
                        if event_name not in event_buffer:
                            # Link
                            link_el = event.locator(".calendarItem__titleLink")
                            if await link_el.count() > 0:
                                rel_link = await link_el.get_attribute("href")
                                event_url = response.urljoin(rel_link)
                            else:
                                event_url = response.url
    
                            # Time
                            time_el = event.locator(".calendarItem__information p")
                            if await time_el.count() > 0:
                                raw_time = await time_el.inner_text()
                                time_val = extract_time_only(raw_time)
                            else:
                                time_val = 'N/A'
    
                            # Description
                            desc_el = event.locator(".calendarItem__description p")
                            if await desc_el.count() > 0:
                                description = await desc_el.inner_text()
                            else:
                                description = 'N/A'
                            
                            # Target Group (Tags)
                            tags_el = event.locator("ul.calendarItem__tags li.tag")
                            try:
                                tags_text = await tags_el.all_inner_texts()
                            except:
                                tags_text = []
                            
                            target_group = ", ".join(tags_text) if tags_text else "All"
                            
                            event_buffer[event_name] = {
                                'event_name': event_name,
                                'event_url': event_url,
                                'time': time_val,
                                'description': description,
                                'location': extract_location_from_title(event_name),
                                'target_group': target_group,
                                'target_group_normalized': self.simple_normalize(target_group),
                                'status': detect_cancelled_status(event_name, description),
                                'booking_info': 'N/A',  # Not available for Skansen
                                'dates': [current_date_iso]
                            }
                        else:
                            # Event exists, add this date
                            event_buffer[event_name]['dates'].append(current_date_iso)
                
                # 3. Next Day
                # Click "Next day" button
                try:
                    next_btn = page.locator("button.link:has-text('Next day')")
                    if await next_btn.count() == 0:
                        # Fallback try checking parent container
                        next_btn = page.locator(".calendarTopBar__button:last-child button.link")
                    
                    if await next_btn.count() > 0 and await next_btn.is_visible():
                        await next_btn.click()
                        # Wait for date update or network idle
                        await page.wait_for_timeout(1000)  # Small pause
                        await page.wait_for_load_state("networkidle")
                    else:
                        self.logger.info("No 'Next day' button found. Stopping.")
                        break
                except Exception as e:
                    self.logger.warning(f"Error navigating to next day: {e}")
                    break
            
            # No AI fallback - selector-only approach
            if len(event_buffer) == 0:
                self.logger.warning("Skansen: No events extracted with selectors. Manual selector input required.")
            
            await page.close()
            return
            
            # [NEW] CONSOLIDATION: Yield unique events with start/end dates
            self.logger.info(f"Consolidating {len(event_buffer)} unique Skansen events...")
            
            for event_name, event_data in event_buffer.items():
                dates = sorted(event_data['dates'])  # Sort dates chronologically
                
                item = EventCategoryItem()
                item['event_name'] = event_data['event_name']
                item['event_url'] = event_data['event_url']
                item['time'] = event_data['time']
                item['description'] = event_data['description']
                item['location'] = event_data['location']
                item['target_group'] = event_data['target_group']
                item['target_group_normalized'] = event_data['target_group_normalized']
                item['status'] = event_data['status']
                
                # Set start date (first occurrence) and end date (last occurrence)
                item['date_iso'] = dates[0]  # Start date
                item['date'] = dates[0]
                
                if len(dates) > 1:
                    item['end_date_iso'] = dates[-1]  # End date
                else:
                    item['end_date_iso'] = 'N/A'  # Single day event
                
                self.logger.info(f"  -> {event_name}: {dates[0]} to {dates[-1] if len(dates) > 1 else 'single day'}")
                yield item
            
            await page.close()
            return
        
        # === TEKNISKA MUSEET HANDLER (CLOUDSCRAPER) ===
        # Uses cloudscraper to bypass Cloudflare, completely isolated from other sites
        if "tekniskamuseet.se" in response.url:
            self.logger.info("Detected Tekniska museet. Using Cloudscraper for Cloudflare bypass.")
            
            # [NEW] Get selectors from database
            tekniska_selectors = self.db.get_selectors(response.url)
            if tekniska_selectors:
                self.logger.info(f"Using DB selectors for Tekniska: {tekniska_selectors.get('container')}")
                sel = tekniska_selectors.get('items', {})
            else:
                self.logger.info("No DB selectors for Tekniska. Using fallback hardcoded selectors.")
                sel = {
                    'event_name': '.archive-item-link h3 span',
                    'event_url': '.archive-item-link',
                    'date_iso': '.archive-item-date span',
                    'target_group_age': '.event-archive-item-age span',
                    'target_group_type': '.event-archive-item-type span',
                    'target_group_tags': '.archive-item-tags li span'
                }
            container_sel = tekniska_selectors.get('container', '.event-archive-item-inner') if tekniska_selectors else '.event-archive-item-inner'
            
            # Close the Playwright page - we'll use cloudscraper instead
            if page:
                await page.close()
            
            # Fetch HTML using cloudscraper (bypasses Cloudflare)
            try:
                scraper = cloudscraper.create_scraper()
                cf_response = scraper.get(response.url)
                
                if cf_response.status_code != 200:
                    self.logger.error(f"Cloudscraper failed with status: {cf_response.status_code}")
                    return
                
                html = cf_response.text
                self.logger.info(f"Cloudscraper successfully fetched {len(html)} bytes")
                
            except Exception as e:
                self.logger.error(f"Cloudscraper error: {e}")
                return
            
            # Parse with BeautifulSoup using selectors from DB
            soup = BeautifulSoup(html, 'html.parser')
            events = soup.select(container_sel)
            self.logger.info(f"Found {len(events)} Tekniska museet event cards")
            
            today = datetime.now().date()
            limit_date = today + timedelta(days=45)
            
            for event in events:
                try:
                    # Title - use selector from DB
                    title_el = event.select_one(sel.get('event_name', '.archive-item-link h3 span'))
                    if not title_el:
                        continue
                    event_name = title_el.get_text(strip=True)
                    
                    # URL - use selector from DB
                    link_el = event.select_one(sel.get('event_url', '.archive-item-link'))
                    if link_el and link_el.get('href'):
                        event_url = response.urljoin(link_el['href'])
                    else:
                        event_url = response.url
                    
                    # Date - use selector from DB
                    date_el = event.select_one(sel.get('date_iso', '.archive-item-date span'))
                    date_iso = None
                    end_date_iso = None
                    
                    if date_el:
                        raw_date = date_el.get_text(strip=True)
                        self.logger.debug(f"Raw date for {event_name}: {raw_date}")
                        
                        # Check for date range (contains " - " separator)
                        if ' - ' in raw_date:
                            parts = raw_date.split(' - ')
                            if len(parts) == 2:
                                date_iso = parse_swedish_date(parts[0].strip())
                                end_date_iso = parse_swedish_date(parts[1].strip())
                        else:
                            date_iso = parse_swedish_date(raw_date)
                    
                    # If no valid start date, skip
                    if not date_iso:
                        self.logger.warning(f"Could not parse date for: {event_name}")
                        continue
                    
                    # Date filtering: include events where end_date >= today (running events)
                    # or start_date is within limit
                    try:
                        start_date = datetime.strptime(date_iso, "%Y-%m-%d").date()
                        
                        # If we have an end date, check if event is still running
                        if end_date_iso:
                            end_date = datetime.strptime(end_date_iso, "%Y-%m-%d").date()
                            # Include if: event is currently running OR starts within limit
                            if not (end_date >= today and start_date <= limit_date):
                                continue
                        else:
                            # Single-day event: check if within date range
                            if not (today <= start_date <= limit_date):
                                continue
                    except ValueError:
                        continue
                    
                    # === TARGET GROUP EXTRACTION FROM CARD LABELS (using DB selectors) ===
                    target_parts = []
                    
                    # 1. Age range label - use selector from DB
                    age_el = event.select_one(sel.get('target_group_age', '.event-archive-item-age span'))
                    if age_el:
                        age_text = age_el.get_text(strip=True)
                        if age_text:
                            target_parts.append(age_text)
                    
                    # 2. Activity type/location label - use selector from DB
                    type_el = event.select_one(sel.get('target_group_type', '.event-archive-item-type span'))
                    location = "Tekniska museet"  # Default location
                    if type_el:
                        type_text = type_el.get_text(strip=True)
                        if type_text:
                            # Check if it's a location indicator
                            if type_text.lower() == 'tensta':
                                location = "Tekniska i Tensta"
                            else:
                                # Not a location, add to target_parts
                                target_parts.append(type_text)
                    
                    # 3. Tags - use selector from DB
                    tags_els = event.select(sel.get('target_group_tags', '.archive-item-tags li span'))
                    for tag_el in tags_els:
                        tag_text = tag_el.get_text(strip=True)
                        if tag_text:
                            target_parts.append(tag_text)
                    
                    # Combine target group info
                    target_group = ", ".join(target_parts) if target_parts else "All"
                    
                    # Normalize target group based on age pattern
                    target_group_normalized = self.normalize_tekniska_target(target_group)
                    
                    # === FETCH DESCRIPTION FROM DETAIL PAGE ===
                    description = 'N/A'
                    if event_url and event_url != response.url:
                        try:
                            detail_response = scraper.get(event_url)
                            if detail_response.status_code == 200:
                                detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                                # Use 'main p' selector - works for Tekniska museet detail pages
                                desc_el = detail_soup.select_one('main p')
                                if desc_el:
                                    description = desc_el.get_text(strip=True)[:500]  # Limit to 500 chars
                                    self.logger.debug(f"Got description for {event_name}: {description[:50]}...")
                        except Exception as e:
                            self.logger.warning(f"Could not fetch detail page for {event_name}: {e}")
                    
                    # Create Item
                    item = EventCategoryItem()
                    item['event_name'] = event_name
                    item['event_url'] = event_url
                    item['date_iso'] = date_iso
                    item['date'] = date_iso
                    item['end_date_iso'] = end_date_iso if end_date_iso else 'N/A'
                    item['time'] = 'N/A'  # Time usually on detail page
                    item['location'] = location  # Use extracted location
                    item['description'] = description
                    item['target_group'] = target_group
                    item['target_group_normalized'] = target_group_normalized
                    item['status'] = detect_cancelled_status(event_name, description)
                    item['booking_info'] = 'N/A'  # Not available for Tekniska
                    
                    self.logger.info(f"  -> {event_name}: {date_iso} to {end_date_iso or 'N/A'} | Target: {target_group}")
                    yield item
                    
                except Exception as e:
                    self.logger.warning(f"Error extracting Tekniska event: {e}")
                    continue
            
            # No AI fallback - selector-only approach
            if len(events) == 0:
                self.logger.warning("Tekniska: No events found with selectors. Manual selector input required.")
            
            return  # Exit after Tekniska handler
            
            return  # Exit after Tekniska handler
        
        
        # === MODERNA MUSEET HANDLER ===
        if "modernamuseet.se" in response.url:
            self.logger.info("Detected Moderna Museet. Using specialized DOM parser.")
            
            # [NEW] Get selectors from database
            moderna_selectors = self.db.get_selectors(response.url)
            if moderna_selectors:
                self.logger.info(f"Using DB selectors for Moderna: {moderna_selectors.get('container')}")
                moderna_sel = moderna_selectors.get('items', {})
            else:
                self.logger.info("No DB selectors for Moderna. Using fallback hardcoded selectors.")
                moderna_sel = {
                    'event_name': '.calendar__item-title::text',
                    'event_url': '.calendar__item-share a.read-more::attr(href)',
                    'time': '.calendar__item-category time::text',
                    'description': '.calendar__item-extended-content p::text',
                    'location': '.calendar__item-share li a::text',
                    'target_group': '.calendar__item-category li::text'
                }
            container_sel = moderna_selectors.get('container', 'article.calendar__item') if moderna_selectors else 'article.calendar__item'
            
            # Since content is SSR, we can parse page content directly
            # We use Selector on the page content to allow using standard Scrapy selectors
            content = await page.content()
            sel = scrapy.Selector(text=content)
            
            # Locate all day containers
            # Structure: .calendar__day[data-date="YYYY-MM-DD"]
            days = sel.css('.calendar__day')
            self.logger.info(f"Found {len(days)} day blocks.")
            
            extracted_count = 0
            current_count = 0
            
            today = datetime.now().date()
            # [MODIFIED] Look ahead 45 days (approx 1.5 months) 
            limit_date = today + timedelta(days=45)
            
            for day in days:
                date_str = day.attrib.get('data-date')
                if not date_str:
                    continue
                
                try:
                    event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    self.logger.warning(f"Invalid date format: {date_str}")
                    continue
                
                # Filter past events or too far future
                if event_date < today:
                    continue
                if event_date > limit_date:
                    self.logger.info(f"Date {date_str} exceeds limit {limit_date}. Stopping extraction.")
                    break # Assuming chronological order
                
                # Extract events within this day - use container from DB
                events = day.css(container_sel)
                
                for event in events:
                    try:
                        # 1. Title - use selector from DB
                        title = event.css(moderna_sel.get('event_name', '.calendar__item-title::text')).get()
                        if not title:
                            continue
                        title = title.strip()
                        
                        # 2. Link - use selector from DB
                        event_url = event.css(moderna_sel.get('event_url', '.calendar__item-share a.read-more::attr(href)')).get()
                        
                        if not event_url:
                            event_url = response.url # Default to calendar page if no specific link
                        else:
                            event_url = response.urljoin(event_url)
                            
                        # 3. Time - use selector from DB
                        time_val = event.css(moderna_sel.get('time', '.calendar__item-category time::text')).get()
                        if not time_val:
                            time_val = 'N/A'
                        else:
                            time_val = extract_time_only(time_val)
                            
                        # 4. Description - use selector from DB
                        description = event.css(moderna_sel.get('description', '.calendar__item-extended-content p::text')).get()
                        if not description:
                            description = 'N/A'
                        else:
                            description = description.strip()
                            
                        # 5. Location - use selector from DB (with fallback logic)
                        location = "Moderna Museet"
                        share_items = event.css('.calendar__item-share li')
                        for li in share_items:
                            if li.css('svg use[xlink\\:href*="#location"]'):
                                loc_text = li.css('a::text').get()
                                if loc_text:
                                    location = loc_text.strip()
                                    break
                        
                        # 6. Target Group - use selector from DB
                        tags = []
                        cat_items = event.css('.calendar__item-category li')
                        for li in cat_items:
                            if not li.css('time'):
                                t_text = li.css('::text').get()
                                if t_text:
                                    tags.append(t_text.strip())
                        
                        target_group = ", ".join(tags) if tags else "All"
                        target_group_normalized = self.simple_normalize(target_group)
                        
                        # Create Item
                        item = EventCategoryItem()
                        item['event_name'] = title
                        item['event_url'] = event_url
                        item['date_iso'] = date_str
                        item['date'] = date_str
                        item['end_date_iso'] = 'N/A' # Single day usually
                        item['time'] = time_val
                        item['location'] = location
                        item['description'] = description
                        item['target_group'] = target_group
                        item['target_group_normalized'] = target_group_normalized
                        item['status'] = detect_cancelled_status(title, description)
                        item['booking_info'] = 'N/A' # Specific booking info parsing logic could be added if needed
                        
                        self.logger.info(f"  -> {title}: {date_str} | {time_val}")
                        yield item
                        extracted_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error parsing Moderna event: {e}")
                        continue

            self.logger.info(f"Moderna Museet: Extracted {extracted_count} events.")
            
            # No AI fallback - selector-only approach
            if extracted_count == 0:
                self.logger.warning("Moderna: No events extracted with selectors. Manual selector input required.")
            
            await page.close()
            return
            
            await page.close()
            return

        # === STEP B: SCROLL & LOAD MORE ===
        for _ in range(4): 
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)

        # Click Load More ~40 times (Covers approx 45 days)
        # [MODIFIED] Increased limit for Stockholm Library to capture events through February
        limit = 20 if "biblioteket.stockholm.se" in response.url else 40
        load_words = ["Visa fler", "Ladda fler", "Load more", "Show more", "More events", "Nästa", "Visa alla"]
        for _ in range(limit): 
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
        # [MODIFIED] Now reads from selectors.db for ALL sites (including Stockholm Library)
        # Run seed_selectors.py to populate the database with selectors
        selectors = self.db.get_selectors(response.url)
        
        if selectors:
            self.logger.info(f"Using DB selectors for {response.url}: container='{selectors.get('container')}'")
        else:
            self.logger.info(f"No DB selectors found for {response.url}. Triggering AI discovery...")
            
            # [NEW] AI SELECTOR DISCOVERY: If no selectors in DB, discover and cache them
            try:
                # Get HTML content from current page
                html_content = await page.content()
                
                # Initialize discovery service
                discovery_service = SelectorDiscoveryService(
                    ai_client=self.client,
                    logger=self.logger,
                    db_manager=self.db
                )
                
                # Discover and save selectors
                result = discovery_service.discover_and_save(response.url, html_content)
                
                if result['success']:
                    confidence = result.get('confidence', 0.0)
                    self.logger.info(f"✅ AI Discovery successful (confidence: {confidence:.0%})")
                    
                    if result.get('saved'):
                        # Reload selectors from database
                        selectors = self.db.get_selectors(response.url)
                        self.logger.info(f"💾 Selectors cached to database for future scrapes")
                    else:
                        # Use discovered selectors without saving (low confidence)
                        if confidence > 0.3:  # Minimum threshold to attempt extraction
                            selectors = {
                                'container': result['selectors'].get('container'),
                                'items': discovery_service._convert_to_spider_format(result['selectors'].get('items', {}))
                            }
                            self.logger.warning(f"⚠️ Using discovered selectors but not caching (confidence: {confidence:.0%})")
                else:
                    self.logger.error(f"❌ AI Discovery failed: {result.get('error', 'Unknown error')}")
            except Exception as e:
                self.logger.error(f"AI Discovery exception: {e}", exc_info=True)
        
        extracted_data = []
        fast_path_success = False
        
        # [NEW] Special Handling for Armemuseum (Two-Step Crawling)
        # Strategy: Extract name/date from calendar cards, then get description from detail page
        if "armemuseum.se" in response.url:
            self.logger.info("Detected Armemuseum. Using Hybrid Extraction Strategy.")
            
            # [NEW] Get selectors from database
            arme_selectors = self.db.get_selectors(response.url)
            if arme_selectors:
                self.logger.info(f"Using DB selectors for Armemuseum: {arme_selectors.get('container')}")
                sel = arme_selectors.get('items', {})
            else:
                self.logger.info("No DB selectors for Armemuseum. Using fallback hardcoded selectors.")
                sel = {
                    'event_name': 'span.font-mulish.font-black',
                    'date_range': 'span.text-xs.leading-7.font-roboto'
                }
            
            # Extract event cards from the calendar page using JavaScript
            # Each card contains: event_name, date_range, event_url
            # NOTE: The <a> element itself contains the name/date spans (not a parent div!)
            event_cards = await page.evaluate("""
                () => {
                    const cards = [];
                    // Find all event links on the calendar page
                    const eventLinks = Array.from(document.querySelectorAll('a[href*="/event/"]'));
                    
                    eventLinks.forEach(link => {
                        // Query directly on the link element (not on parent)
                        // because the spans are children of <a>
                        const nameEl = link.querySelector('span.font-mulish.font-black');
                        const name = nameEl ? nameEl.innerText.trim() : null;
                        
                        // Get date range (e.g., "28 december - 6 januari")
                        // Try multiple selectors for robustness
                        let dateEl = link.querySelector('span.text-xs.leading-7.font-roboto');
                        if (!dateEl) {
                            dateEl = link.querySelector('span.font-roboto');
                        }
                        const dateRange = dateEl ? dateEl.innerText.trim() : null;
                        
                        // Get event URL
                        const url = link.href;
                        
                        if (name && url && url.includes('/event/')) {
                            cards.push({ name, dateRange, url });
                        }
                    });
                    
                    // Deduplicate by URL
                    const seen = new Set();
                    return cards.filter(c => {
                        if (seen.has(c.url)) return false;
                        seen.add(c.url);
                        return true;
                    });
                }
            """)
            
            self.logger.info(f"Found {len(event_cards)} event cards on calendar page")
            
            # Process each event card
            for card in event_cards:
                event_name = card.get('name', 'Unknown Event')
                date_range = card.get('dateRange', '')
                event_url = card.get('url', '')
                
                self.logger.info(f"  -> {event_name}: {date_range}")
                
                # Parse date range (e.g., "28 december - 6 januari")
                date_iso = None
                end_date_iso = None
                if date_range:
                    if ' - ' in date_range:
                        parts = date_range.split(' - ')
                        if len(parts) == 2:
                            date_iso = parse_swedish_date(parts[0].strip())
                            end_date_iso = parse_swedish_date(parts[1].strip())
                    else:
                        date_iso = parse_swedish_date(date_range)
                
                if not date_iso:
                    self.logger.warning(f"Could not parse date from: {date_range}")
                    continue
                
                # Request detail page for description, passing the extracted data
                yield scrapy.Request(
                    event_url,
                    callback=self.parse_details,
                    dont_filter=True,  # [NEW] Allow recurring events with same URL but different dates
                    meta={
                        'playwright': True,
                        'playwright_include_page': True,
                        'playwright_page_methods': [
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                            PageMethod("wait_for_timeout", 1000),
                        ],
                        'is_event_detail': True,
                        # Pass extracted data from calendar page
                        'arme_event_name': event_name,
                        'arme_date_iso': date_iso,
                        'arme_end_date_iso': end_date_iso,
                    }
                )
            
            # No AI fallback - selector-only approach
            if len(event_cards) == 0:
                self.logger.warning("Armemuseum: No event cards found. Manual selector input required.")
            
            # Close page and return (skip generic logic)
            await page.close()
            return
            
            # Close page and return (skip generic logic)
            await page.close()
            return

        # === STEP C: PAGINATION HANDLING (LOAD MORE BUTTONS) ===
        # IMPORTANT: This must happen BEFORE extraction to load ALL events first
        # Some sites (e.g., National Museum) use "Load More" or "Visa mer" buttons to dynamically load events
        try:
            load_more_selectors = [
                'a.show-more-text',  # National Museum: "Visa mer"
                'button:has-text("Visa mer")',
                'button:has-text("Load more")',
                'a:has-text("Visa mer")',
                'a:has-text("Load more")',
                '.show-more',
                '.load-more',
                'button[class*="load"]',
                'a[class*="load"]'
            ]
            
            load_more_clicks = 0
            max_clicks = 10  # Safety limit - sufficient for 1 month of events
            
            for selector in load_more_selectors:
                while load_more_clicks < max_clicks:
                    try:
                        load_btn = page.locator(selector)
                        if await load_btn.count() > 0:
                            is_visible = await load_btn.is_visible()
                            is_enabled = await load_btn.is_enabled()
                            
                            if is_visible and is_enabled:
                                self.logger.info(f"Found '{selector}' - clicking to load more events...")
                                await load_btn.click(force=True)
                                await page.wait_for_timeout(1500)  # Wait for content to load
                                load_more_clicks += 1
                            else:
                                break  # Button not visible or enabled, try next selector
                        else:
                            break  # No button found, try next selector
                    except Exception as e:
                        self.logger.debug(f"Error clicking '{selector}': {e}")
                        break
                
                if load_more_clicks > 0:
                    self.logger.info(f"Successfully clicked 'load more' {load_more_clicks} times")
                    break  # Found and clicked the right selector, exit loop
            
            if load_more_clicks == 0:
                self.logger.info("No load more buttons found on page")
                
        except Exception as e:
            self.logger.warning(f"Pagination handling error: {e}")

        # === STEP D: FAST PATH (EXTRACT USING DB SELECTORS) ===
        # Now runs AFTER all events are loaded via pagination
        if selectors:
            self.logger.info(f"Pointers found for {response.url}. Attempting Fast Path...")
            fast_data = await self.extract_with_selectors(page, selectors)
            if fast_data and len(fast_data) > 0:
                self.logger.info(f"Fast Path extracted {len(fast_data)} events.")
                extracted_data = fast_data
                fast_path_success = True
            else:
                self.logger.info("Fast Path failed or returned no data. Falling back to AI Path.")

        # === STEP E: AI PATH (IF FAST PATH FAILED) ===
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
                        # [NEW] Keep HTML blocks for DOM-rich export (first 10 events)
                        if i < 10:
                            html_snippets.append(await element.inner_html())
                    
                    if len(current_batch) >= 5:
                        event_batches.append("\n---\n".join(current_batch))
                        current_batch = []
                except Exception as e:
                    self.logger.warning(f"Error extracting text from element: {e}")
            
            # [DEBUG] Log HTML snippets
            if html_snippets:
                self.logger.info(f"DEBUG: Found {len(html_snippets)} HTML snippets.")
                for i, snippet in enumerate(html_snippets[:3]):
                    self.logger.info(f"DEBUG: HTML Snippet {i+1}:\n{snippet}")

            if current_batch:
                event_batches.append("\n---\n".join(current_batch))
            
            # [NEW] ULTIMATE FALLBACK: If no event elements found, grab entire page body
            if not event_batches:
                self.logger.warning("No event elements found with generic selectors. Using entire page body as fallback.")
                try:
                    page_body_text = await page.inner_text("body")
                    # Clean and limit text
                    page_body_text = re.sub(r'\s+', ' ', page_body_text).strip()[:15000]
                    if page_body_text:
                        event_batches.append(page_body_text)
                        self.logger.info(f"Extracted {len(page_body_text)} characters from page body for AI processing")
                except Exception as e:
                    self.logger.error(f"Failed to extract page body: {e}")

            # [NEW] Process with EventScraperOrchestrator (Auto-Discovery System)
            self.logger.info("Using EventScraperOrchestrator for automatic selector discovery and extraction")
            
            try:
                # Get full page HTML
                page_html = await page.content()
                
                # Use orchestrator to discover selectors and extract events
                all_extracted_data = self.orchestrator.scrape_new_website(
                    url=response.url,
                    html_content=page_html
                )
                
                # If selectors were discovered and cached, save them to database
                domain = self.orchestrator._extract_domain(response.url)
                if domain in self.orchestrator.selector_cache:
                    discovered_selectors = self.orchestrator.selector_cache[domain]
                    confidence = discovered_selectors.get('confidence', {}).get('overall', 0)
                    
                    self.logger.info(f"Selector discovery confidence: {confidence:.1%}")
                    
                    # Save to database if confidence is acceptable (>0.5)
                    if confidence > 0.5:
                        self.db.save_selectors(
                            response.url,
                            discovered_selectors.get('container'),
                            discovered_selectors.get('items')
                        )
                        self.logger.info(f"Saved discovered selectors to database for {domain}")
                
            except Exception as e:
                self.logger.error(f"Orchestrator error: {e}", exc_info=True)
                all_extracted_data = []
            
            # [NEW] Store HTML snippets for correlation with extracted events
            # This allows us to save HTML blocks alongside the extracted event data
            html_blocks_for_export = html_snippets  # HTML from elements we analyzed
            
            # [NEW] Time Slot Consolidation + Deduplication
            # Logic:
            # 1. Same event on DIFFERENT days = separate entries (each day is unique)
            # 2. Same event on SAME day with MULTIPLE times = one entry with combined time slots
            consolidated = {}
            html_block_index = 0
            
            for event in all_extracted_data:
                event_name = event.get('event_name', '')
                date_iso = event.get('date_iso', '')
                time_slot = event.get('time', '')
                
                # Key for consolidation: (event_name, date_iso) - merges same event + same day
                key = (event_name, date_iso)
                
                if key in consolidated:
                    # Same event on same day - merge time slots
                    existing_time = consolidated[key].get('time', '')
                    if time_slot and time_slot not in existing_time:
                        if existing_time and existing_time != 'N/A':
                            consolidated[key]['time'] = f"{existing_time}, {time_slot}"
                        else:
                            consolidated[key]['time'] = time_slot
                    self.logger.debug(f"Merged time slot for {event_name} on {date_iso}: {consolidated[key]['time']}")
                else:
                    # New event or different day - create new entry
                    consolidated[key] = event.copy()
                    # [NEW] Attach HTML block if available (for DOM-rich export)
                    if html_block_index < len(html_blocks_for_export):
                        consolidated[key]['_html_block'] = html_blocks_for_export[html_block_index]
                        html_block_index += 1
            
            extracted_data = list(consolidated.values())
            self.logger.info(f"Consolidated {len(all_extracted_data)} raw events into {len(extracted_data)} unique event-day combinations")

        await page.close()
        
        # === STEP E: FILTER & STORE ===
        today = datetime.now().date()
        limit_date = today + timedelta(days=31)  # ~1 month from today (through Jan 25th)
        
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
                
                # [NEW] Add HTML block if available (for DOM-rich JSON export - new websites only)
                if '_html_block' in event_data:
                    item['html_block'] = event_data['_html_block']
                    item['container_selector'] = 'auto_discovered_element'  # Mark as auto-discovered
                
                # [MODIFIED] Use extracted URL if available (e.g. from Fast Path href)
                raw_url = event_data.get('event_url')
                if raw_url:
                    item['event_url'] = response.urljoin(raw_url)
                else:
                    item['event_url'] = response.url
                
                item['end_date_iso'] = event_data.get('end_date_iso') or 'N/A'
                
                # --- STATUS CHECK ---
                # [MODIFIED] Check multiple sources for cancelled status
                raw_status = (event_data.get('status') or '').lower()
                status_indicator = (event_data.get('status_indicator') or '').lower()
                event_name_lower = event_name.lower()
                
                # Detect cancelled from: status field, INSTÄLLT prefix in name, or overlay indicator
                is_cancelled = (
                    'cancel' in raw_status or 
                    'inställt' in raw_status or
                    'inställt' in status_indicator or
                    event_name_lower.startswith('inställt')
                )
                item['status'] = 'cancelled' if is_cancelled else 'scheduled'
                
                # [NEW] Extract booking info for Stockholm Library events
                booking_status_raw = event_data.get('booking_status', '')
                status_indicator = event_data.get('status_indicator', '') or ''
                
                combined_booking_text = f"{booking_status_raw} {status_indicator}".strip()
                
                if combined_booking_text and any(x in combined_booking_text.lower() for x in ['öppnar', 'stänger', 'boka', 'fullbokat']):
                    # 1. Clean "None" artifacts
                    clean_text = combined_booking_text.replace('None', '').strip()
                    
                    # 2. [NEW] Remove "Datum:" and everything following it
                    # This transforms "Du behöver boka plats Datum: Söndag..." -> "Du behöver boka plats"
                    if "Datum:" in clean_text:
                        clean_text = clean_text.split("Datum:")[0].strip()
                        
                    item['booking_info'] = clean_text
                else:
                    item['booking_info'] = extract_booking_info(combined_booking_text)
                
                # [NEW] Clean "INSTÄLLT:" prefix from displayed event name
                if event_name_lower.startswith('inställt:'):
                    event_name = event_name[9:].strip()  # Remove "INSTÄLLT:" prefix
                    item['event_name'] = event_name

                # --- TARGET GROUP LOGIC ---
                # Priority:
                # 1. STRICT OVERRIDE: If URL contains "forskolor", FORCE PRESCHOOL
                # 2. Use website's Målgrupp field (target_group_raw) - contains "Målgrupp:" prefixed text
                # 3. Extract target group from event name (age patterns)
                # 4. FALLBACK: Use AI detection + Age Parsing
                
                if "forskolor" in response.url:
                    item['target_group'] = "Preschool"
                    item['target_group_normalized'] = "preschool_groups"
                else:
                    # Check for website's Målgrupp field
                    raw_target = event_data.get('target_group_raw', '')
                    if raw_target and 'målgrupp' in raw_target.lower():
                        # Extract the value after "Målgrupp:"
                        if ':' in raw_target:
                            target_value = raw_target.split(':', 1)[1].strip()
                        else:
                            target_value = raw_target.replace('Målgrupp', '').strip()
                        item['target_group'] = target_value
                        item['target_group_normalized'] = self.simple_normalize(target_value)
                    else:
                        # Try to extract target group from event name (age patterns)
                        name_target, name_target_norm = extract_target_from_name(event_name)
                        if name_target:
                            item['target_group'] = name_target
                            item['target_group_normalized'] = name_target_norm
                        else:
                            # FALLBACK: Use AI detection + Age Parsing
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
                        
                        # [NEW] Parse end date from date_range_text (e.g., "Tisdag 26 dec - onsdag 31 dec")
                        date_range_text = event_data.get('date_range_text', '')
                        if date_range_text and ' - ' in date_range_text:
                            # Split by " - " and try to parse the second part as end date
                            parts = date_range_text.split(' - ')
                            if len(parts) == 2:
                                end_date_str = parse_swedish_date(parts[1].strip())
                                if end_date_str:
                                    item['end_date_iso'] = end_date_str
                        
                        # [MODIFIED] Check if we need to fetch details
                        # [OPTIMIZED] Smart detail page fetching:
                        # Only fetch detail page if description is truly missing or very short
                        # This saves significant time for sites like National Museum where 
                        # listing page already has good descriptions
                        description = item.get('description', 'N/A')
                        has_good_description = (
                            description and 
                            description != 'N/A' and 
                            len(description) > 30  # Description is substantial
                        )
                        
                        is_stockholm_library = "biblioteket.stockholm.se" in response.url
                        needs_detail_fetch = (
                            is_stockholm_library or  # Stockholm library needs detail for booking info
                            not has_good_description  # Only fetch if description is missing/short
                        )
                        
                        if needs_detail_fetch and item['event_url'] and item['event_url'] != response.url:
                             self.logger.info(f"Fetching details for '{item['event_name']}' from: {item['event_url']}")
                             yield scrapy.Request(
                                 item['event_url'],
                                 callback=self.parse_details,
                                 dont_filter=True,  # [NEW] Allow recurring events with same URL but different dates
                                 meta={
                                     'item': item,
                                     'source_url': response.url,  # [NEW] Pass original source URL for context
                                     'playwright': True,
                                     'playwright_include_page': True,
                                     'playwright_page_methods': [
                                         PageMethod("wait_for_load_state", "domcontentloaded"),
                                         PageMethod("wait_for_timeout", 1000),  # [OPTIMIZED] Reduced from 2000ms
                                     ],
                                 }
                             )
                        else:
                            yield item
                        
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
                    # [NEW] Special handling for booking_status - get ALL paragraphs and search for booking text
                    if field == 'booking_status':
                        all_paragraphs = el.locator(sel)
                        count = await all_paragraphs.count()
                        booking_text = ''
                        for i in range(count):
                            try:
                                p_text = await all_paragraphs.nth(i).inner_text()
                                if p_text:
                                    p_lower = p_text.lower()
                                    # Check if this paragraph contains booking-related keywords
                                    if any(kw in p_lower for kw in ['boka', 'bokning', 'drop-in', 'dropin', 'fullbokat', 'fullbokad']):
                                        booking_text = p_text
                                        break
                            except:
                                continue
                        item[field] = booking_text if booking_text else None
                        continue
                    
                    target = el.locator(sel).first
                    if await target.count() > 0:
                        value = None
                        
                        # [NEW] Special handling for event_url to get href
                        if field == 'event_url':
                            value = await target.get_attribute('href')
                        # For date/time fields, try to get datetime attribute from <time> elements
                        elif field in ('date_iso', 'time') and 'time' in sel:
                            # Try to get the datetime attribute first
                            datetime_attr = await target.get_attribute('datetime')
                            if datetime_attr:
                                value = datetime_attr
                            else:
                                value = await target.inner_text()
                        else:
                            value = await target.inner_text()
                        
                        if value:
                             # robust cleaning
                             value = re.sub(r'\s+', ' ', value).strip()
                             item[field] = value
                        else:
                             item[field] = None
                    else:
                        item[field] = None
                except:
                    item[field] = None
            
            if item.get('event_name'):
                extracted.append(item)
        return extracted

    async def parse_details(self, response):
        page = response.meta.get("playwright_page")
        
        self.logger.info(f"Extracting details from: {response.url}")
        
        # === ARMEMUSEUM DETAIL PAGE HANDLER ===
        # Uses data passed from calendar page (name, date) + extracts description from detail page
        if "armemuseum.se/event/" in response.url:
            # Get data passed from calendar page
            event_name = response.meta.get('arme_event_name')
            date_iso = response.meta.get('arme_date_iso')
            end_date_iso = response.meta.get('arme_end_date_iso')
            
            # If no calendar data was passed, skip (this shouldn't happen with proper flow)
            if not event_name or not date_iso:
                self.logger.warning(f"No calendar data passed for: {response.url}")
                if page:
                    await page.close()
                return
            
            self.logger.info(f"Processing Armemuseum detail: {event_name}")
            
            try:
                if page:
                    # Extract description from detail page (all .richtext p elements)
                    desc_els = page.locator('.richtext p')
                    desc_texts = await desc_els.all_inner_texts() if await desc_els.count() > 0 else []
                    description = ' '.join(desc_texts).strip()[:500]  # Limit to 500 chars
                    await page.close()
                else:
                    description = 'N/A'
                
                # Extract location from description (look for "Armémuseum" or similar)
                location = "Armémuseum"  # Default
                
                # Extract target group from description (keyword detection)
                target_group = "All"
                desc_lower = description.lower()
                if any(kw in desc_lower for kw in ['barn', 'familj', 'kids', 'children']):
                    target_group = "Families and Children"
                elif any(kw in desc_lower for kw in ['vuxna', 'adult']):
                    target_group = "Adults"
                
                # Create Item
                item = EventCategoryItem()
                item['event_name'] = event_name
                item['event_url'] = response.url
                item['date_iso'] = date_iso
                item['date'] = date_iso
                item['end_date_iso'] = end_date_iso or 'N/A'
                item['time'] = 'N/A'  # Not available on this site
                item['location'] = location
                item['description'] = description or 'N/A'
                item['target_group'] = target_group
                item['target_group_normalized'] = self.simple_normalize(target_group)
                item['status'] = detect_cancelled_status(event_name, description)
                item['booking_info'] = 'N/A'
                
                self.logger.info(f"  -> {event_name}: {date_iso} to {end_date_iso or 'N/A'}")
                yield item
                return
                
            except Exception as e:
                self.logger.error(f"Error extracting Armemuseum detail page: {e}")
                if page:
                    await page.close()
                return
        
        # === FALLBACK: AI EXTRACTION FOR OTHER SITES ===
        if not page:
             # If playwright page is missing, fallback to response.text
             text = " ".join(response.xpath('//body//text()').getall())
        else:
             text = await page.inner_text("body")
             await page.close()
        
        # Clean text
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Prepare prompt for full event extraction from a single page
        # Using a unified prompt structure for detail pages
        prompt = f"""
        You are an Event Extraction Engine.
        Task: Extract a single event from the detail page text below.
        
        Current Date: {datetime.now().strftime('%Y-%m-%d')}
        Future Year context: 2026.
        
        Input Text:
        {text[:15000]} # Limit text length
        
        Requirements:
        1. Output ONLY a valid JSON object (not a list).
        2. Extract fields: event_name, date_iso (YYYY-MM-DD), end_date_iso, time, location, target_group, description, status.
        3. If specific fields are missing, infer from context or use null/N/A.
        4. "description" should be a concise summary (max 300 chars).
        
        JSON Structure:
        {{
          "event_name": "Event Name",
          "date_iso": "2026-01-01",
          "end_date_iso": null,
          "time": "14:00",
          "location": "Museum Hall",
          "target_group": "Children",
          "description": "...",
          "status": "scheduled"
        }}
        """
        
        try:
             # Call AI
             response_gen = self.client.models.generate_content(
                model="gemini-2.5-pro",
                contents=prompt,
                config={
                    "response_mime_type": "application/json",
                    "temperature": 0.1,
                }
            )
             result_text = response_gen.text.strip()
             if result_text.startswith("```"):
                 result_text = re.sub(r'^```(?:json)?\n?', '', result_text)
                 result_text = re.sub(r'\n?```$', '', result_text)
             
             data = json.loads(result_text)
             
             # Create Item
             item = EventCategoryItem()
             item['event_name'] = data.get('event_name') or 'Unknown Event'
             item['location'] = data.get('location') or 'N/A'
             item['time'] = extract_time_only(data.get('time'))
             item['description'] = data.get('description') or 'N/A'
             item['event_url'] = response.url
             
             # Date handling
             raw_date = data.get('date_iso')
             if raw_date and parse_swedish_date(raw_date):
                 item['date_iso'] = parse_swedish_date(raw_date)
                 item['date'] = item['date_iso']
             else:
                 # Fallback: if date is missing in detail, we might skip or mark as N/A. 
                 # For now, if no date, we can't index it properly.
                 self.logger.warning(f"No valid date found for {response.url}")
                 return

             item['end_date_iso'] = data.get('end_date_iso') or 'N/A'
             item['status'] = data.get('status', 'scheduled')
             
             # Preserve booking_info from listing page (passed via item in meta)
             original_item = response.meta.get('item')
             item['booking_info'] = original_item.get('booking_info', 'N/A') if original_item else 'N/A'
             
             # [MODIFIED] Check source_url from meta for forskolor context
             # (detail page URLs don't contain 'forskolor', need to check original source)
             source_url = response.meta.get('source_url', response.url)
             if "forskolor" in source_url:
                 item['target_group'] = "Preschool"
                 item['target_group_normalized'] = "preschool_groups"
             else:
                 item['target_group'] = data.get('target_group', 'All')
                 item['target_group_normalized'] = self.simple_normalize(item['target_group'])
             
             yield item
             
        except Exception as e:
            self.logger.error(f"Error parsing details for {response.url}: {e}")

    def call_ai_engine(self, text_content, include_selectors=False, html_context=None, **kwargs):
        """
        Optimized event extraction with better prompting and structure
        """
        current_date = datetime.now().strftime('%Y-%m-%d')
        current_year = datetime.now().year
        next_year = current_year + 1
        
        # Base JSON format for events
        base_event_format = {
            "event_name": "Event Name",
            "date_iso": "2025-12-01",
            "end_date_iso": None,
            "time": "10:00",
            "location": "Venue Name",
            "target_group": "Adults",
            "description": "Brief description in original language",
            "status": "scheduled"
        }
        
        # Handle detail extraction (single event deep-dive)
        if kwargs.get('extract_details'):
            prompt = self._build_detail_extraction_prompt(text_content)
            return self._execute_ai_call(prompt, is_detail_extraction=True)
        
        # Handle selector discovery + event extraction
        if include_selectors:
            prompt = self._build_selector_discovery_prompt(
                text_content, html_context, current_date, next_year
            )
        else:
            # Handle simple event list extraction
            prompt = self._build_event_extraction_prompt(
                text_content, current_date, next_year
            )
        
        return self._execute_ai_call(prompt, include_selectors=include_selectors)
    


    def _build_event_extraction_prompt(self, text_content, current_date, next_year):
        """Build prompt for extracting events without selectors"""
        return f"""You are an Event Extraction Specialist for Swedish cultural websites.

    CURRENT CONTEXT:
    - Today's date: {current_date}
    - Current year: {datetime.now().year}
    - Date interpretation: Dates in January-March without explicit year should use {next_year}

    INPUT DATA:
    {text_content}

    EXTRACTION RULES:

    1. LANGUAGE PRESERVATION:
    - Keep ALL text (event_name, description, location) in the ORIGINAL language
    - Do NOT translate Swedish to English
    - Example: "Sagostund för barn" stays as "Sagostund för barn"

    2. DATE EXTRACTION (CRITICAL):
    Swedish month mapping:
    - januari/jan → 01, februari/feb → 02, mars → 03, april → 04
    - maj → 05, juni/jun → 06, juli/jul → 07, augusti/aug → 08
    - september/sep → 09, oktober/okt → 10, november/nov → 11, december/dec → 12
    
    Format rules:
    - Single date: "5 december" → "date_iso": "{datetime.now().year}-12-05", "end_date_iso": null
    - Date range: "5-8 december" → "date_iso": "{datetime.now().year}-12-05", "end_date_iso": "{datetime.now().year}-12-08"
    - Cross-month: "28 dec - 3 jan" → date_iso: "{datetime.now().year}-12-28", end_date_iso: "{next_year}-01-03"
    - Weekday parsing: "Lördag 14 december" → ignore weekday, extract "14 december"
    - Year inference: If month is Jan-Mar and we're in Nov-Dec, use {next_year}

    3. TIME EXTRACTION:
    - Format: "HH:MM" (24-hour)
    - Examples: "kl. 10:00" → "10:00", "14.30" → "14:30"
    - Time range: "10:00-12:00" → use start time "10:00"
    - Missing time → null

    4. STATUS DETECTION:
    - Keywords indicating cancellation: "Inställt", "Cancelled", "Avbokad", "Fullbokat" (if explicitly cancelled)
    - Default: "scheduled"
    - Set to "cancelled" ONLY if explicitly stated

    5. LOCATION EXTRACTION:
    - Extract venue name: "Stadsbiblioteket" or "Barn- och ungdomsbiblioteket, Malmö"
    - Include room/floor if available: "Sagoteket, plan 2"
    - Keep in original language

    6. TARGET GROUP:
    - Look for age indicators: "barn 3-6 år" → "Children (3-6 years)"
    - Common Swedish patterns: "vuxna" → "Adults", "familjer" → "Families"
    - If not specified → null

    7. DESCRIPTION:
    - Extract the first descriptive sentence or teaser (max 250 characters)
    - Keep in Swedish/original language
    - Avoid generic text like "Välkommen!" alone
    - If no meaningful description available → null (NOT "N/A")

    8. EVENT NAME:
    - Primary title of the event in original language
    - Clean up extra whitespace

    OUTPUT FORMAT:
    Return ONLY valid JSON array (no markdown, no explanation):
    [
    {{
        "event_name": "Babyrytmik",
        "date_iso": "2025-12-05",
        "end_date_iso": null,
        "time": "10:00",
        "location": "Stadsbiblioteket",
        "target_group": "Babies (0-1 year)",
        "description": "Sjung och rör dig tillsammans med ditt barn",
        "status": "scheduled"
    }}
    ]

    IMPORTANT: 
    - If text contains separator "---", treat each section as separate event
    - Extract ALL events found in the input
    - Skip events with insufficient data (missing both name and date)
    """

    def _build_selector_discovery_prompt(self, text_content, html_context, current_date, next_year):
        """Build prompt for discovering CSS selectors + extracting events"""
        
        # Prepare HTML samples
        html_samples = ""
        if html_context and len(html_context) > 0:
            # Take up to 5 samples for better pattern recognition
            samples = html_context[:5]
            html_samples = "\n\n---HTML SAMPLE SEPARATOR---\n\n".join(samples)
        
        return f"""You are a Web Scraping Expert specializing in Swedish event websites.

    CURRENT CONTEXT:
    - Today's date: {current_date}
    - Year inference: Jan-Mar dates should use {next_year}

    HTML STRUCTURE SAMPLES:
    {html_samples}

    EVENT TEXT DATA:
    {text_content}

    YOUR TASKS:
    1. Analyze the HTML structure to identify reliable CSS selectors
    2. Extract all events from the text data

    SELECTOR DISCOVERY RULES:

    1. CONTAINER SELECTOR:
    - Find the repeating element that wraps each event
    - Prefer: semantic tags (article, section) with stable classes
    - Avoid: generic divs, dynamic IDs (e.g., id="event-12345")
    - Examples of GOOD selectors:
        * "article.event-item"
        * "div.event-card"
        * ".events-list > li"
    - Look for common patterns across ALL HTML samples

    2. FIELD SELECTORS (relative to container):
    - Must work with .querySelector() or .select_one() from container
    - Prioritize:
        a) Semantic tags: <h2>, <h3>, <time>, <address>
        b) Stable classes: .event-title, .event-date (not .col-md-6)
        c) Data attributes: [data-event-date]
    
    Required field mapping:
    - event_name: Title/heading (h2, h3, .title, .event-name)
    - date_iso: Date text (<time>, .date, .event-date)
    - time: Time string (.time, .event-time, <time>)
    - location: Venue (.location, .venue, address)
    - description: Teaser/summary (p, .description, .teaser)
    
    Optional fields (if identifiable):
    - target_group: Age/audience info
    - status: Cancellation indicators

    3. SELECTOR QUALITY CHECKS:
    - Test selector specificity: not too generic ("div") or too specific (.class1.class2.class3)
    - Ensure selectors work across multiple events in samples
    - If a field has no reliable selector → set to null

    EVENT EXTRACTION RULES:
    (Same as previous - extract with Swedish language preservation, proper date parsing, etc.)

    Swedish months: januari→01, februari→02, mars→03, april→04, maj→05, juni→06, 
                    juli→07, augusti→08, september→09, oktober→10, november→11, december→12

    OUTPUT FORMAT (JSON only, no markdown):
    {{
    "selectors": {{
        "container": "article.event-card",
        "items": {{
        "event_name": "h2.event-title",
        "date_iso": "time.event-date",
        "time": "span.event-time",
        "location": "address.venue",
        "description": "p.event-description",
        "target_group": ".audience-tag",
        "status": null
        }}
    }},
    "events": [
        {{
        "event_name": "Sagostund",
        "date_iso": "2025-12-15",
        "end_date_iso": null,
        "time": "10:00",
        "location": "Stadsbiblioteket",
        "target_group": "Children (3-6 years)",
        "description": "En mysig sagostund för de minsta",
        "status": "scheduled"
        }}
    ]
    }}

    CRITICAL NOTES:
    - Selectors must be relative to container (not full page paths)
    - Return null for field selectors that can't be reliably determined
    - Keep all text in original Swedish
    - Extract ALL events from the input data
    """

    def _build_detail_extraction_prompt(self, text_content):
        """Build prompt for extracting detailed information from single event page"""
        return f"""Extract detailed event information from the following page content.

    INPUT TEXT:
    {text_content}

    EXTRACTION RULES:

    1. DESCRIPTION:
    - Extract the FULL event description (all paragraphs)
    - Keep in ORIGINAL language (Swedish/Spanish/etc) - DO NOT TRANSLATE
    - Preserve paragraph breaks with \\n\\n
    - Maximum 2000 characters

    2. LOCATION:
    - Extract complete address or venue details
    - Include room/floor/building if specified
    - Format: "Venue Name, Address, City" or "Room, Venue Name"

    3. TARGET GROUP:
    - Identify audience: "Adults", "Children (3-6 years)", "Families", "Teenagers", etc.
    - Look for Swedish keywords: "barn", "vuxna", "familjer", "ungdomar"
    - Extract age ranges when specified

    OUTPUT FORMAT (JSON only):
    {{
    "description": "Full event description in original language...",
    "location": "Complete venue information",
    "target_group": "Specific audience"
    }}

    If any field cannot be determined, return null for that field.
    """

    def _execute_ai_call(self, prompt, include_selectors=False, is_detail_extraction=False):
        """Execute the AI call with proper error handling and validation"""
        try:
            # Log prompt for debugging (first 1500 chars)
            self.logger.info(f"AI Prompt Preview:\n{prompt[:1500]}...")
            
            response = self.client.models.generate_content(
                model="gemini-2.5-pro",
                contents=prompt,
                config={
                    "response_mime_type": "application/json",
                    "temperature": 0.1,
                }
            )
            
            response_text = response.text.strip()
            
            # Clean markdown artifacts
            if response_text.startswith("```"):
                response_text = re.sub(r'^```(?:json)?\n?', '', response_text)
                response_text = re.sub(r'\n?```$', '', response_text)
            
            # Parse JSON with auto-repair
            try:
                result = json.loads(response_text)
            except json.JSONDecodeError as e:
                self.logger.warning(f"JSON decode error, attempting repair: {e}")
                result = self._repair_json(response_text)
            
            # Validate and return appropriate format
            if is_detail_extraction:
                return result if isinstance(result, dict) else {}
            
            if include_selectors:
                # Validate selector response
                if not isinstance(result, dict) or 'selectors' not in result:
                    self.logger.error("Invalid selector response format")
                    return {"selectors": {}, "events": []}
                return result
            
            # Simple event list
            if isinstance(result, list):
                return self._validate_events(result)
            if isinstance(result, dict):
                for val in result.values():
                    if isinstance(val, list):
                        return self._validate_events(val)
            
            return []
            
        except Exception as e:
            self.logger.error(f"AI Engine Error: {e}", exc_info=True)
            return [] if not include_selectors else {"selectors": {}, "events": []}


    def _repair_json(self, text):
        """Attempt to repair malformed JSON"""
        fixed = text.rstrip()
        
        # Remove trailing commas
        if fixed.endswith(','):
            fixed = fixed[:-1]
        
        # Balance braces and brackets
        open_braces = fixed.count('{') - fixed.count('}')
        open_brackets = fixed.count('[') - fixed.count(']')
        
        fixed += '}' * max(0, open_braces)
        fixed += ']' * max(0, open_brackets)
        
        return json.loads(fixed)



    def _validate_events(self, events):
        """Validate extracted events and filter out invalid entries"""
        valid_events = []
        
        for event in events:
            # Must have at minimum: event name and date
            if not event.get('event_name') or not event.get('date_iso'):
                self.logger.warning(f"Skipping invalid event: {event}")
                continue
            
            # Validate date format
            try:
                datetime.strptime(event['date_iso'], '%Y-%m-%d')
            except ValueError:
                self.logger.warning(f"Invalid date format for event: {event['event_name']}")
                continue
            
            valid_events.append(event)
        
        return valid_events


    def simple_normalize(self, target_str):
        """
        Normalize target group using Age Parsing and Keywords.
        """
        if not target_str: return 'all_ages'
        t = target_str.lower()
        
        # --- 1. KEYWORD CHECKS ---
        # Check for children keywords (Swedish and English)
        if 'barn' in t or 'kid' in t or 'bebis' in t or 'småbarn' in t or 'förskola' in t: 
            return 'children'
        
        # [NEW] Skansen English tag: "For children"
        if 'for children' in t or 'för barn' in t:
            return 'children'
        
        if 'ungdom' in t or 'teen' in t or 'tonåring' in t or 'unga' in t: 
            return 'teens'
        
        if 'familj' in t or 'family' in t: 
            return 'families'
            
        if 'vuxen' in t or 'vuxna' in t or 'adult' in t or 'senior' in t: 
            return 'adults'
        
        # [NEW] All ages / general audience keywords
        if 'all' in t or 'alla' in t or 'general' in t:
            return 'all_ages'

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

        # Default fallback - changed from 'adults' to 'all_ages' for better accuracy
        return 'all_ages'

    def normalize_tekniska_target(self, target_str):
        """
        Normalize Tekniska museet target group from card labels.
        Handles age patterns like "12-15", "8+", "15-19" and keywords like "Klubb", "Lov".
        """
        if not target_str:
            return 'all_ages'
        
        t = target_str.lower()
        
        # Check for age range patterns (e.g., "12-15", "8-12")
        age_range = re.search(r'(\d{1,2})\s*[-–]\s*(\d{1,2})', t)
        if age_range:
            min_age = int(age_range.group(1))
            max_age = int(age_range.group(2))
            
            if max_age <= 6:
                return 'preschool'
            elif max_age <= 11:
                return 'children'  # Up to 11 years = children
            elif min_age >= 10 and max_age <= 19:
                return 'teens'  # 10-19 range = teens (includes 12-15)
            else:
                return 'adults'
        
        # Check for "X+" patterns (e.g., "8+", "15+")
        age_plus = re.search(r'(\d{1,2})\s*\+', t)
        if age_plus:
            min_age = int(age_plus.group(1))
            if min_age <= 6:
                return 'children'
            elif min_age <= 12:
                return 'children'
            elif min_age < 18:
                return 'teens'
            else:
                return 'adults'
        
        # Keywords from Tekniska museet
        if 'småbarn' in t or 'bebis' in t:
            return 'preschool'
        if 'barn' in t or 'kid' in t:
            return 'children'
        if 'klubb' in t:  # Robotklubben etc - usually for older kids/teens
            return 'teens'
        if 'lov' in t:  # School holiday events - usually for children
            return 'children'
        if 'kurs' in t:  # Courses - usually adults
            return 'adults'
        if 'familj' in t or 'family' in t:
            return 'families'
        
        return 'all_ages'