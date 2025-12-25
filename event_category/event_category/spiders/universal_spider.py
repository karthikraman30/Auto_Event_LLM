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
    
    # 1. FINAL URL LIST (All 5 Sites)
    start_urls = [
        "https://biblioteket.stockholm.se/evenemang",
        "https://biblioteket.stockholm.se/forskolor",
        "https://www.skansen.se/en/calendar/",
        "https://www.tekniskamuseet.se/pa-gang/",
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
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            events = soup.select('.event-archive-item-inner')
            self.logger.info(f"Found {len(events)} Tekniska museet event cards")
            
            today = datetime.now().date()
            limit_date = today + timedelta(days=45)
            
            for event in events:
                try:
                    # Title
                    title_el = event.select_one('.archive-item-link h3 span')
                    if not title_el:
                        continue
                    event_name = title_el.get_text(strip=True)
                    
                    # URL
                    link_el = event.select_one('.archive-item-link')
                    if link_el and link_el.get('href'):
                        event_url = response.urljoin(link_el['href'])
                    else:
                        event_url = response.url
                    
                    # Date - parse range format like "2025-12-20 - 2026-01-06"
                    date_el = event.select_one('.archive-item-date span')
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
                    
                    # === TARGET GROUP EXTRACTION FROM CARD LABELS ===
                    target_parts = []
                    
                    # 1. Age range label (e.g., "12-15", "8+", "15-19")
                    age_el = event.select_one('.event-archive-item-age span')
                    if age_el:
                        age_text = age_el.get_text(strip=True)
                        if age_text:
                            target_parts.append(age_text)
                    
                    # 2. Activity type/location label (e.g., "Tensta", "Kurser")
                    # Note: "Tensta" is actually a location (Tekniska i Tensta branch)
                    type_el = event.select_one('.event-archive-item-type span')
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
                    
                    # 3. Tags (e.g., "Klubb", "Lov", "Event")
                    tags_els = event.select('.archive-item-tags li span')
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
            
            return  # Exit after Tekniska handler
        
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
        selectors = self.db.get_selectors(response.url)
        
        if "biblioteket.stockholm.se" in response.url:
            selectors = {
                'container': 'article',
                'items': {
                    'event_name': 'h2 a',
                    'event_url': 'h2 a',  # Get event link for detail page navigation
                    'date_iso': 'time',
                    'date_range_text': 'time',  # [NEW] Full date text for extracting end date (e.g., "Tisdag 26 dec - onsdag 31 dec")
                    'time': 'section > div:nth-child(3) p',
                    'location': 'section > div:nth-child(4) p',
                    'target_group_raw': 'section p',  # [NEW] Look for Målgrupp field  
                    'status_indicator': 'div p',  # For detecting "Inställt" overlay
                    'booking_status': 'p'  # [FIXED] Get all paragraph elements to find booking info text
                    # [REMOVED] 'description' - force detail page fetch for full descriptions
                }
            }
            self.logger.info("Using hardcoded stable selectors for Stockholm Library.")
        extracted_data = []
        fast_path_success = False
        
        # [NEW] Special Handling for Armemuseum (Two-Step Crawling)
        if "armemuseum.se" in response.url:
            self.logger.info("Detected Armemuseum. Using Two-Step Crawling Strategy.")
            # Find all event links
            # Based on inspection, links might be in <a> tags or clickable elements.
            # Using a broad strategy to find links to /event/ or similar
            
            # Extract all links
            links = await page.evaluate("""
                () => {
                    const links = Array.from(document.querySelectorAll('a'));
                    return links.map(a => a.href);
                }
            """)
            
            event_links = set()
            for link in links:
                # Filter for event links 
                if "/event/" in link or "/kalender/" in link: # Adjust based on actual URL structure
                     if link != response.url: # Exclude self
                        event_links.add(link)
            
            self.logger.info(f"Found {len(event_links)} potential event links: {list(event_links)[:5]}...")
            
            for link in event_links:
                yield scrapy.Request(
                    link,
                    callback=self.parse_details,
                    meta={
                        'playwright': True,
                        'playwright_include_page': True,
                        'playwright_page_methods': [
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                            PageMethod("wait_for_timeout", 1000),  # [OPTIMIZED] Reduced from 2000ms
                        ],
                        'is_event_detail': True # Flag to indicate this is a detail page
                    }
                )
            
            # Close page and return (skip generic logic)
            await page.close()
            return

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
            
            # [DEBUG] Log HTML snippets
            if html_snippets:
                self.logger.info(f"DEBUG: Found {len(html_snippets)} HTML snippets.")
                for i, snippet in enumerate(html_snippets[:3]):
                    self.logger.info(f"DEBUG: HTML Snippet {i+1}:\n{snippet}")

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
                
                # [MODIFIED] Use extracted URL if available (e.g. from Fast Path href)
                raw_url = event_data.get('event_url')
                if raw_url:
                    item['event_url'] = response.urljoin(raw_url)
                else:
                    item['event_url'] = response.url
                
                item['end_date_iso'] = event_data.get('end_date_iso') or 'N/A'
                
                # --- STATUS CHECK ---
                # [MODIFIED] Check multiple sources for cancelled status
                raw_status = event_data.get('status', '').lower()
                status_indicator = event_data.get('status_indicator', '').lower()
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
                # Also check status_indicator for booking info (e.g. "Du behöver boka plats")
                combined_booking_text = f"{booking_status_raw} {status_indicator}"
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
                        # Force detail page fetch for:
                        # 1. forskolor events (to get proper descriptions)
                        # 2. evenemang events (to get proper descriptions and target groups)
                        # 3. Any event with missing description or location
                        is_stockholm_library = "biblioteket.stockholm.se" in response.url
                        needs_detail_fetch = (
                            is_stockholm_library or  # [NEW] Always fetch for stockholm library events
                            item['description'] == 'N/A' or 
                            item['location'] == 'N/A'
                        )
                        
                        if needs_detail_fetch and item['event_url'] and item['event_url'] != response.url:
                             self.logger.info(f"Fetching details for '{item['event_name']}' from: {item['event_url']}")
                             yield scrapy.Request(
                                 item['event_url'],
                                 callback=self.parse_details,
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
        if not page:
             # If playwright page is missing (shouldn't happen with current meta), fallback to response.text
             text = " ".join(response.xpath('//body//text()').getall())
        else:
             text = await page.inner_text("body")
             await page.close()

        self.logger.info(f"Extracting details from: {response.url}")
        
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
                model="gemini-2.0-flash",
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
            1. description: The full event description. IMPORTANT: Keep the description in its original language (Swedish/Spanish/etc). Do NOT translate.
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
               - IMPORTANT: Keep the description in its ORIGINAL LANGUAGE (Swedish/Spanish/etc). Do NOT translate.
            
            5. DATE LOGIC:
               - "date_iso": Start date.
               - "end_date_iso": End date (or null).
               - Convert Swedish months (december->12, januari->01).
            
            {selector_instructions}
            
            JSON Structure:
            {json_format}
            """
        
        try:
            # [DEBUG] Log the prompt content to see what text is being sent
            self.logger.info(f"DEBUG: AI Prompt Content (first 2000 chars):\n{prompt[:2000]}")

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