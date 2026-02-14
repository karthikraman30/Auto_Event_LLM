import scrapy
import re
from datetime import datetime, timedelta
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem
from event_category.utils.db_manager import DatabaseManager
import cloudscraper
from bs4 import BeautifulSoup

# Swedish month mapping
SWEDISH_MONTHS = {
    'januari': 1, 'jan': 1, 'january': 1, 'february': 2, 'februari': 2, 'feb': 2,
    'mars': 3, 'mar': 3, 'march': 3, 'april': 4, 'apr': 4,
    'maj': 5, 'may': 5, 'juni': 6, 'jun': 6, 'june': 6,
    'juli': 7, 'jul': 7, 'july': 7, 'augusti': 8, 'aug': 8, 'august': 8,
    'september': 9, 'sep': 9, 'sept': 9, 'oktober': 10, 'okt': 10, 'oct': 10, 'october': 10,
    'november': 11, 'nov': 11, 'december': 12, 'dec': 12,
}

# Reverse mapping for formatting dates in Swedish (for GraphQL API)
SWEDISH_MONTH_NAMES = {
    1: 'januari', 2: 'februari', 3: 'mars', 4: 'april', 5: 'maj', 6: 'juni',
    7: 'juli', 8: 'augusti', 9: 'september', 10: 'oktober', 11: 'november', 12: 'december'
}

SWEDISH_DAY_NAMES = {
    0: 'måndag', 1: 'tisdag', 2: 'onsdag', 3: 'torsdag', 4: 'fredag', 5: 'lördag', 6: 'söndag'
}

# Stockholm Library GraphQL API configuration
USE_GRAPHQL_FOR_STOCKHOLM = True  # Feature flag to toggle GraphQL vs button-clicking
STOCKHOLM_GRAPHQL_URL = "https://biblioteket.stockholm.se/graphql/"
STOCKHOLM_GRAPHQL_PAGE_SIZE = 100  # Events per request (API default is 20)

# GraphQL query for Stockholm Library event search
# NOTE: Date filtering is done client-side - the API returns upcoming events by default
STOCKHOLM_EVENT_SEARCH_QUERY = """
query eventSearch(
  $query: String!
  $size: Int
  $from: Int
  $isSchoolEvent: Boolean
) {
  eventSearch(
    query: $query
    size: $size
    from: $from
    isSchoolEvent: $isSchoolEvent
  ) {
    results
    events {
      id
      title
      eventSlugId
      description {
        preamble
      }
      image {
        url
        caption
        altText
      }
      location
      library
      externalEventLink
      dateTime {
        startDate
        stopDate
        startTime
        stopTime
      }
      targetAudiences
      languages
      category
      subcategory
      canceled
      bookable
      bookingStatus
      otherInformation
      recurring
    }
  }
}
"""

def format_swedish_date(date_obj):
    """Format a date object as Swedish string for GraphQL API (e.g., 'fredag 14 februari 2026')"""
    if isinstance(date_obj, str):
        date_obj = datetime.strptime(date_obj, "%Y-%m-%d").date()
    day_name = SWEDISH_DAY_NAMES[date_obj.weekday()]
    month_name = SWEDISH_MONTH_NAMES[date_obj.month]
    return f"{day_name} {date_obj.day} {month_name} {date_obj.year}"

def parse_graphql_swedish_date(date_str):
    """Parse Swedish date from GraphQL response (e.g., 'söndag 15 februari 2026') to ISO format"""
    if not date_str:
        return None
    # Use existing parse_swedish_date which handles this format
    return parse_swedish_date(date_str)

def parse_swedish_date(date_str):
    if not date_str:
        return None
    date_str = date_str.strip().lower()
    iso_match = re.match(r'^(\d{4}-\d{2}-\d{2})', date_str)
    if iso_match:
        return iso_match.group(1)
    match = re.search(r'(\d{1,2})\s+([a-zåäö]+)', date_str)
    if match:
        day = int(match.group(1))
        month = SWEDISH_MONTHS.get(match.group(2))
        if month:
            year_match = re.search(r'\b(20\d{2})\b', date_str)
            year = int(year_match.group(1)) if year_match else datetime.now().year
            if not year_match and (month < datetime.now().month or (month == datetime.now().month and day < datetime.now().day)):
                year += 1
            return f"{year}-{month:02d}-{day:02d}"
    return None

def extract_time_only(time_str):
    if not time_str:
        return 'N/A'
    time_str = time_str.strip()
    match = re.search(r'\d{4}-\d{2}-\d{2}[T\s](\d{1,2}[:.]\d{2}(?:[:.]\d{2})?)', time_str)
    if match:
        return match.group(1).replace('.', ':')
    match = re.search(r'Tid:\s*(\d{1,2}[:.]\d{2}(?:\s*-\s*\d{1,2}[:.]\d{2})?)', time_str, re.IGNORECASE)
    if match:
        return match.group(1).replace('.', ':')
    match = re.search(r'^(\d{1,2}[:.]\d{2}(?:\s*-\s*\d{1,2}[:.]\d{2})?)$', time_str)
    if match:
        return match.group(1).replace('.', ':')
    return time_str

def extract_location_from_title(title):
    """Legacy function - use extract_skansen_location for better results"""
    if not title:
        return "Skansen"
    for pattern in [r'\bat\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', r'\bin\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', r'\bi\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', r'\bpå\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)']:
        match = re.search(pattern, title, re.IGNORECASE)
        if match and len(match.group(1).strip()) > 2:
            return match.group(1).strip()
    if any(kw in title.lower() for kw in ['farmstead', 'church', 'kyrka', 'gård', 'torg', 'stage', 'hall', 'house', 'hus']):
        return title
    return "Skansen"

# Known Skansen venue keywords and their display names
SKANSEN_VENUES = {
    # Animals & Zoo
    'seal enclosure': 'Seal Enclosure',
    'upper seal': 'Upper Seal Enclosure',
    'children\'s zoo': 'Children\'s Zoo',
    'barnens zoo': 'Children\'s Zoo',
    'lill-skansen': 'Lill-Skansen',
    'lillskansen': 'Lill-Skansen',
    'bear': 'Bear Enclosure',
    'björn': 'Bear Enclosure',
    'wolf': 'Wolf Enclosure',
    'varg': 'Wolf Enclosure',
    'moose': 'Moose Enclosure',
    'älg': 'Moose Enclosure',
    'reindeer': 'Reindeer Enclosure',
    'ren': 'Reindeer Enclosure',
    
    # Science & Museum
    'baltic sea science center': 'Baltic Sea Science Center',
    'baltic sea': 'Baltic Sea Science Center',
    'östersjöns science center': 'Baltic Sea Science Center',
    'aquarium': 'Aquarium',
    
    # Food & Dining
    'bollnästorget': 'Bollnästorget',
    'bollnäs': 'Bollnästorget',
    'stora gungan': 'Stora Gungan Tavern',
    'tavern': 'Stora Gungan Tavern',
    'krogen': 'Stora Gungan Tavern',
    'solliden': 'Solliden Restaurant',
    
    # Historical Buildings & Workshops
    'bakery': 'Bakery',
    'bageriet': 'Bakery',
    'old shop': 'Old Shop (Kryddboden)',
    'kryddboden': 'Old Shop (Kryddboden)',
    'bookbindery': 'Bookbindery',
    'bokbinderiet': 'Bookbindery',
    'printer': 'Printer\'s Workshop',
    'tryckeriet': 'Printer\'s Workshop',
    'tinsmith': 'Tinsmith\'s Workshop',
    'bleckslagare': 'Tinsmith\'s Workshop',
    'ironmonger': 'Ironmonger\'s Store',
    'järnhandel': 'Ironmonger\'s Store',
    'glassblower': 'Glassblower\'s Hut',
    'glasblåsare': 'Glassblower\'s Hut',
    'pottery': 'Pottery',
    'krukmakeri': 'Pottery',
    'sámi camp': 'Sámi Camp',
    'samernas': 'Sámi Camp',
    
    # Churches & Ceremonial
    'seglora church': 'Seglora Church',
    'seglora kyrka': 'Seglora Church',
    'church': 'Seglora Church',
    'kyrka': 'Seglora Church',
    
    # Town & Districts
    'city quarter': 'City Quarters',
    'town quarter': 'Town Quarters',
    'stadskvarter': 'City Quarters',
    'escalator': 'City Quarters (near escalator)',
    
    # Farmsteads
    'skogaholm': 'Skogaholm Manor',
    'älvros': 'Älvros Farmstead',
    'moragården': 'Mora Farmstead',
    'oktorpsgården': 'Oktorpsgården',
    'delsbogården': 'Delsbogården',
    
    # Stables
    'stable': 'Stable',
    'stallet': 'Stable',
    'horse': 'Horse Stable',
    'häst': 'Horse Stable',
    
    # Museum
    'snus': 'Snus and Match Museum',
    'match museum': 'Snus and Match Museum',
    'tobacco': 'Snus and Match Museum',
}

def extract_skansen_location(title, description=''):
    """
    Extract location from Skansen event title and description.
    Checks both fields for known venue keywords.
    """
    if not title:
        return "Skansen"
    
    combined_text = f"{title} {description or ''}".lower()
    
    # 1. First check for known venue keywords in combined text
    for keyword, venue_name in SKANSEN_VENUES.items():
        if keyword in combined_text:
            return venue_name
    
    # 2. Try regex patterns for "at/in/på" phrases in title
    for pattern in [
        r'\bat\s+(?:the\s+)?([A-ZÅÄÖ][\w\s\']+?)(?:\s*[-–,\.]|$)',
        r'\bin\s+(?:the\s+)?([A-ZÅÄÖ][\w\s\']+?)(?:\s*[-–,\.]|$)',
        r'\bi\s+([A-ZÅÄÖ][\w\s\']+?)(?:\s*[-–,\.]|$)',
        r'\bpå\s+([A-ZÅÄÖ][\w\s\']+?)(?:\s*[-–,\.]|$)',
        r'\binside\s+(?:the\s+)?([A-ZÅÄÖ][\w\s\']+?)(?:\s*[-–,\.]|$)',
    ]:
        match = re.search(pattern, title, re.IGNORECASE)
        if match and len(match.group(1).strip()) > 2:
            return match.group(1).strip()
    
    # 3. Also try regex on description for "at the X" patterns
    if description:
        for pattern in [
            r'\bat\s+(?:the\s+)?([A-ZÅÄÖ][\w\s\']+?)(?:\s*[-–,\.]|$)',
            r'\binside\s+(?:the\s+)?([A-ZÅÄÖ][\w\s\']+?)(?:\s*[-–,\.]|$)',
        ]:
            match = re.search(pattern, description, re.IGNORECASE)
            if match and len(match.group(1).strip()) > 2:
                extracted = match.group(1).strip()
                # Avoid extracting overly generic words
                if extracted.lower() not in ['the', 'a', 'an', 'skansen', 'our', 'this']:
                    return extracted
    
    # 4. Check if the event name itself is a known venue type
    if any(kw in title.lower() for kw in ['farmstead', 'church', 'kyrka', 'gård', 'torg', 'stage', 'hall', 'house', 'hus', 'shop', 'museum', 'workshop']):
        return title
    
    # 5. Default fallback
    return "Skansen"

def detect_cancelled_status(name, desc='', status=''):
    combined = f"{name} {desc} {status}".lower()
    if any(kw in combined for kw in ['inställt', 'inställd', 'cancelled', 'canceled', 'avlyst', 'ställs in', 'avbokat']):
        return 'cancelled'
    if any(kw in combined for kw in ['fullbokat', 'fullbokad', 'fully booked', 'sold out', 'slutsålt']):
        return 'fullbokat'
    return 'scheduled'

def extract_booking_info(text):
    if not text:
        return 'N/A'
    t = text.lower()
    if 'fullbokat' in t or 'fullbokad' in t:
        return 'Fullbokat'
    if any(kw in t for kw in ['boka plats', 'du behöver boka', 'bokning krävs', 'bokningen öppnar']):
        return 'Requires booking'
    if 'drop-in' in t or 'dropin' in t:
        return 'Drop-in'
    return 'N/A'

def extract_target_group_from_name(event_name, description=''):
    """Extract target group from event name and description for Moderna Museet"""
    text = f"{event_name} {description}".lower()
    
    # Check for general guided tours (English) - these are for all ages, not just families
    # This must come BEFORE family keyword check to override FAMILJEVISNING label
    if 'guided tour' in event_name.lower():
        return "All", "all_ages"
    
    # Check for specific age ranges in event name
    age_match = re.search(r'för\s+(\d{1,2})(?:[-–]\s*(\d{1,2}))?\s*år', event_name.lower())
    if age_match:
        min_age = int(age_match.group(1))
        if age_match.group(2):  # Has max age
            max_age = int(age_match.group(2))
            if max_age <= 6:
                return "Children", "children"
            elif max_age <= 12:
                return "Children", "children"
            elif min_age >= 13:
                return "Teens", "teens"
        else:  # Only min age specified
            if min_age <= 12:
                return "Children", "children"
            elif min_age >= 13:
                return "Teens", "teens"
    
    # Check for family-related keywords (including Armemuseum labels like FAMILJEVISNING)
    if any(kw in text for kw in ['familj', 'familjevisning', 'lovprogram', 'jullov', 'sommarlov', 'sportlov', 'helgvisning']):
        return "Families", "families"
    
    # Check for children-specific keywords
    if any(kw in text for kw in ['barn', 'konstgympa', 'verkstan', 'pyssel']):
        return "Children", "children"
    
    # Check for teen-specific keywords  
    if any(kw in text for kw in ['ungdom', 'teen', 'tonåring']):
        return "Teens", "teens"
    
    # Default to all ages
    return "All", "all_ages"

class UnifiedEventSpider(scrapy.Spider):
    name = "unified_events"
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'DOWNLOAD_DELAY': 0.5,  # Reduced from 2 for faster scraping
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 8,  # Process 8 requests in parallel
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,  # Limit per domain to be polite
    }
    start_urls = [
        "https://biblioteket.stockholm.se/evenemang",
        "https://biblioteket.stockholm.se/forskolor",
        "https://www.skansen.se/en/calendar/",
        "https://www.tekniskamuseet.se/pa-gang/",
        "https://armemuseum.se/kalender/",
        "https://www.modernamuseet.se/stockholm/sv/kalender/",
        "https://www.nationalmuseum.se/kalendarium"
    ]
    
    def __init__(self, url=None, days=30, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.scrape_days = int(days)  # Configurable days parameter (default 30)
        self.spider_stats = {}  # Track GraphQL and other statistics

    def extract_age_limit(self, description):
        """
        Extract age limit from description text.
        Looks for patterns like "7-9 år", "7-9 years", "från 7 år", etc.
        Returns standardized age limit string or empty string if not found.
        """
        if not description:
            return ""
        
        description_lower = description.lower()
        
        # Pattern 1: Age range like "7-9 år" or "7-9 years"
        age_range_match = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*(år|years?)', description_lower)
        if age_range_match:
            min_age = age_range_match.group(1)
            max_age = age_range_match.group(2)
            return f"{min_age}-{max_age}"
        
        # Pattern 2: Minimum age like "från 7 år" or "from 7 years"
        min_age_match = re.search(r'(från|from)\s+(\d+)\s*(år|years?)', description_lower)
        if min_age_match:
            min_age = min_age_match.group(2)
            return f"{min_age}+"
        
        # Pattern 3: Age range with "för dig X-Y år" like "för dig 7-9 år"
        for_you_match = re.search(r'för\s+dig\s+(\d+)\s*[-–]\s*(\d+)\s*(år|years?)', description_lower)
        if for_you_match:
            min_age = for_you_match.group(1)
            max_age = for_you_match.group(2)
            return f"{min_age}-{max_age}"
        
        # Pattern 4: Single age with "år" like "7 år" (less common but possible)
        single_age_match = re.search(r'(\d+)\s*(år|years?)(?![\s-])', description_lower)
        if single_age_match:
            age = single_age_match.group(1)
            return f"{age}"
        
        # Pattern 5: Age in target group format like "För dig 7-9 år"
        target_age_match = re.search(r'för\s+dig\s+(\d+)\s*[-–]\s*(\d+)\s*år', description_lower)
        if target_age_match:
            min_age = target_age_match.group(1)
            max_age = target_age_match.group(2)
            return f"{min_age}-{max_age}"
        
        # Pattern 6: Under age like "under 19 år" or "under19 years" (no space)
        under_age_match = re.search(r'under\s*(\d+)\s*(år|years?)', description_lower)
        if under_age_match:
            max_age = under_age_match.group(2)
            return f"0-{max_age}"
        
        return ""

    def start_requests(self):
        self.db = DatabaseManager()
        urls = [self.url] if self.url else self.start_urls
        for url in urls:
            if "tekniskamuseet.se" in url:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                yield scrapy.Request(url, headers=headers, meta={"playwright": True, "playwright_include_page": True, "playwright_page_methods": [PageMethod("wait_for_timeout", 15000), PageMethod("wait_for_load_state", "networkidle"), PageMethod("wait_for_timeout", 8000)], "handle_httpstatus_list": [403, 429]}, callback=self.parse, dont_filter=True)
            elif "biblioteket.stockholm.se" in url:
                # Use domcontentloaded for Stockholm library sites - NO additional wait
                # Just proceed immediately after DOM is ready
                yield scrapy.Request(url, meta={
                    "playwright": True, 
                    "playwright_include_page": True, 
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded")
                        # Removed wait_for_timeout - proceed immediately
                    ],
                    "playwright_context_kwargs": {
                        "default_navigation_timeout": 30000  # 30 second timeout
                    }
                }, callback=self.parse)
            else:
                yield scrapy.Request(url, meta={"playwright": True, "playwright_include_page": True, "playwright_page_methods": [PageMethod("wait_for_load_state", "networkidle"), PageMethod("wait_for_timeout", 3000)]}, callback=self.parse)

    async def parse(self, response):
        page = response.meta.get("playwright_page")
        if not page:
            return
        self.logger.info(f"Processing: {response.url}")
        try:
            cookie_btns = page.locator("button:has-text('Godkänn'), button:has-text('Acceptera')")
            if await cookie_btns.count() > 0:
                await cookie_btns.first.click(force=True, timeout=2000)
                await page.wait_for_timeout(1000)
        except: 
            pass
        if "skansen.se" in response.url:
            async for item in self.handle_skansen(page, response):
                yield item
            return
        if "tekniskamuseet.se" in response.url:
            async for item in self.handle_tekniska(page, response):
                yield item
            return
        if "modernamuseet.se" in response.url:
            async for item in self.handle_moderna(page, response):
                yield item
            return
        if "armemuseum.se" in response.url:
            async for item in self.handle_armemuseum(page, response):
                yield item
            return
        async for item in self.handle_generic(page, response):
            yield item

    async def handle_skansen(self, page, response):
        selectors = self.db.get_selectors(response.url)
        if not selectors:
            self.logger.warning("No selectors for Skansen")
            await page.close()
            return

        # Iterate day-by-day (Skansen is a true calendar)
        for day_num in range(self.scrape_days):
            # 1️⃣ Get currently selected calendar date
            try:
                date_el = page.locator(".calendarTopBar__dropdownButton span.p")
                date_text = (await date_el.inner_text()).replace("Select date:", "").strip()

                if "," in date_text:
                    date_text = date_text.split(",", 1)[1].strip()

                current_date = parse_swedish_date(date_text)
                if not current_date:
                    self.logger.warning(f"Could not parse Skansen date: {date_text}")
                    break
            except Exception as e:
                self.logger.warning(f"Error extracting Skansen date: {e}")
                break

            # 2️⃣ Check if events exist for this day (don't wait forever)
            try:
                # Wait a short time for events to load, but don't fail if there are none
                await page.wait_for_selector(
                    "ul.calendarList__list li.calendarItem",
                    timeout=5000  # Reduced timeout
                )
                events_found = True
            except:
                # No events found for this day, that's okay
                events_found = False
                self.logger.info(f"No events found for Skansen on {current_date}")

            # 3️⃣ Extract events FOR THIS DAY ONLY (if any exist)
            if events_found:
                # Click "Show more" button repeatedly to load ALL events
                # Skansen hides some events behind this button!
                try:
                    max_show_more_clicks = 10
                    for click_attempt in range(max_show_more_clicks):
                        # First scroll down to make the button visible
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await page.wait_for_timeout(500)
                        
                        # Look for "Show more" button
                        show_more_btn = page.locator("button:has-text('Show more'), a:has-text('Show more'), .calendarList__button:has-text('Show more')")
                        
                        if await show_more_btn.count() > 0 and await show_more_btn.first.is_visible():
                            # Count events before clicking
                            before_count = await page.locator("ul.calendarList__list li.calendarItem").count()
                            
                            # Click "Show more"
                            await show_more_btn.first.click()
                            await page.wait_for_timeout(1500)  # Wait for new events to load
                            
                            # Count events after clicking
                            after_count = await page.locator("ul.calendarList__list li.calendarItem").count()
                            self.logger.info(f"Clicked 'Show more': {before_count} → {after_count} events")
                            
                            if after_count == before_count:
                                # No new events loaded, button didn't work
                                break
                        else:
                            # No more "Show more" button, all events loaded
                            self.logger.info(f"No more 'Show more' button found")
                            break
                    
                    final_count = await page.locator("ul.calendarList__list li.calendarItem").count()
                    self.logger.info(f"Loaded {final_count} total events for {current_date}")
                except Exception as e:
                    self.logger.warning(f"Error during 'Show more' loading: {e}")
                
                extracted = await self.extract_with_selectors(page, selectors)

                for item_data in extracted:
                    name = item_data.get('event_name')
                    if not name:
                        continue

                    item = EventCategoryItem()
                    item['event_name'] = name.strip()
                    item['event_url'] = response.urljoin(item_data.get('event_url', ''))
                    item['date_iso'] = current_date
                    item['date'] = current_date
                    item['end_date_iso'] = 'N/A'  # IMPORTANT: Skansen has NO ranges
                    item['time'] = extract_time_only(item_data.get('time'))
                    item['description'] = item_data.get('description') or 'N/A'
                    item['location'] = extract_skansen_location(name, item['description'])

                    tg = item_data.get('target_group')
                    item['target_group'] = tg or 'All'
                    item['target_group_normalized'] = self.simple_normalize(tg)

                    item['status'] = detect_cancelled_status(name, item['description'])
                    item['booking_info'] = 'N/A'
                    item['image_url'] = item_data.get('image_url')
                    item['age_limit'] = 'N/A'  # Set to N/A for Skansen

                    yield item

            # 4️⃣ Move to next day
            try:
                next_btn = page.locator("button.link:has-text('Next day')")
                if await next_btn.count() > 0 and await next_btn.is_visible():
                    await next_btn.click()
                    # Wait longer for all events to load after navigation
                    await page.wait_for_timeout(2000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except:
                        pass  # Continue even if networkidle times out
                else:
                    self.logger.info(f"No more 'Next day' button found after {day_num + 1} days")
                    break
            except Exception as e:
                self.logger.warning(f"Error clicking next day on Skansen: {e}")
                break

        await page.close()


    async def handle_tekniska(self, page, response):

        selectors = self.db.get_selectors(response.url)
        if not selectors:
            self.logger.warning("No selectors for Tekniska")
            await page.close()
            return

        sel = selectors.get('items', {})
        container_selector = ".event-archive-item-inner"

        try:
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)

            # Check if page is blocked by Cloudflare
            page_title = await page.title()
            if "cloudflare" in page_title.lower() or "challenge" in page_title.lower():
                self.logger.warning("Page blocked by Cloudflare, waiting longer...")
                await page.wait_for_timeout(10000)
                await page.wait_for_load_state("networkidle")

            # 1️⃣ Read available calendar dates
            calendar_dates = await page.evaluate("""
                () => {
                    const el = document.querySelector('#archive-calendar-container');
                    if (!el || !el.dataset.dates) return [];
                    try {
                        return JSON.parse(el.dataset.dates);
                    } catch {
                        return [];
                    }
                }
            """)

            if not calendar_dates:
                self.logger.warning("No calendar dates found")
                await page.close()
                return

            # 2️⃣ Filter next N days based on scrape_days parameter
            today = datetime.now().date()
            limit = today + timedelta(days=self.scrape_days)

            relevant_dates = []
            for d in calendar_dates:
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d").date()
                    if today <= dt <= limit:
                        relevant_dates.append(d)
                except:
                    continue

            relevant_dates.sort()
            self.logger.info(f"Tekniska: {len(relevant_dates)} dates in next {self.scrape_days} days")

            seen = set()

            # 3️⃣ Extract events with their actual dates from the cards
            event_cards = page.locator(container_selector)
            count = await event_cards.count()
            
            self.logger.info(f"Found {count} events already visible on page")
            
            seen = set()
            
            for i in range(count):
                try:
                    card = event_cards.nth(i)
                    
                    name = await card.locator(sel['event_name']).inner_text()
                    link = await card.locator(sel['event_url']).get_attribute("href")
                    event_url = response.urljoin(link)

                    # --- IMAGE EXTRACTION LOGIC ---
                    image_url = None
                    try:
                        # 1. Try DB selector for the card image
                        img_sel = sel.get('image_url', 'img.wp-post-image')
                        img_el = card.locator(img_sel).first
                        
                        if await img_el.count() > 0:
                            # Try standard src, then data-src for lazy-loading
                            image_url = await img_el.get_attribute('src') or await img_el.get_attribute('data-src')
                        
                        # 2. Fallback: If no image in card, grab the general site metadata image
                        if not image_url:
                            meta_img = page.locator('meta[property="og:image"]').first
                            if await meta_img.count() > 0:
                                image_url = await meta_img.get_attribute('content')
                    except Exception as e:
                        self.logger.debug(f"Image extraction failed for Tekniska event {name}: {e}")
                    # ------------------------------
                    
                    # TARGET GROUP AND AGE LIMIT
                    target_parts = []
                    age_limit = 'N/A'  # Store the raw age range separately

                    # Age-based target group (best)
                    try:
                        age_el = card.locator(sel.get('target_group_age'))
                        if await age_el.count() > 0:
                            age_txt = (await age_el.first.inner_text()).strip()
                            if age_txt:
                                target_parts.append(age_txt)
                                age_limit = age_txt  # Store the raw age value (e.g., "0-6", "8+", "12-15")
                    except:
                        pass

                    # Type-based target group (fallback / enrichment)
                    try:
                        type_el = card.locator(sel.get('target_group_type'))
                        if await type_el.count() > 0:
                            type_txt = (await type_el.first.inner_text()).strip()
                            if type_txt:
                                target_parts.append(type_txt)
                    except:
                        pass

                    target_group = ", ".join(target_parts) if target_parts else "All"
                    target_group_normalized = self.normalize_tekniska_target(target_group)

                    # Try to extract actual date from the card first
                    actual_date = None
                    end_date = None
                    try:
                        # Look for date patterns in the card text
                        card_text = await card.inner_text()
                        
                        # Check for Swedish date patterns like "2026-03-02 - 2026-06-21"
                        import re
                        date_pattern = r'(\d{4}-\d{2}-\d{2})(?:\s*-\s*(\d{4}-\d{2}-\d{2}))?'
                        date_match = re.search(date_pattern, card_text)
                        
                        if date_match:
                            actual_date = date_match.group(1)
                            if date_match.group(2):  # If there's an end date
                                end_date = date_match.group(2)
                            self.logger.info(f"Found actual date on card: {actual_date} - {end_date or 'N/A'} for event: {name}")
                    except Exception as e:
                        self.logger.debug(f"Could not extract date from card: {e}")
                    
                    # If we found an actual date on the card, use it
                    if actual_date:
                        date_to_use = actual_date
                        self.logger.info(f"Found actual date on card: {actual_date} for event: {name}")
                    else:
                        # Fall back to calendar date assignment (only for events without card dates)
                        if i < len(relevant_dates):
                            date_to_use = relevant_dates[i]
                            self.logger.info(f"Using calendar date {date_to_use} for event: {name}")
                        else:
                            continue  # Skip if no calendar date available
                    
                    # Filter to only include events within scrape_days
                    try:
                        event_date = datetime.strptime(date_to_use, "%Y-%m-%d").date()
                        today = datetime.now().date()
                        date_limit = today + timedelta(days=self.scrape_days)
                        
                        # Check if event starts within date range OR if it has an end date that overlaps
                        event_in_range = False
                        
                        if today <= event_date <= date_limit:
                            # Event starts within date range
                            event_in_range = True
                        elif end_date and end_date != 'N/A':
                            # Check if event is ongoing during date range
                            try:
                                end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
                                if event_date <= date_limit and end_dt >= today:
                                    # Event overlaps with date range period
                                    event_in_range = True
                                    self.logger.info(f"Event {name} overlaps with date range: {event_date} to {end_date}")
                            except ValueError:
                                pass
                        
                        if not event_in_range:
                            self.logger.info(f"Skipping event {name} on {date_to_use} - outside {self.scrape_days} day range")
                            continue
                    except ValueError:
                        self.logger.warning(f"Invalid date format {date_to_use} for event {name}")
                        continue
                    
                    key = (name, date_to_use, link)
                    if key not in seen:
                        seen.add(key)

                        # Get description from event page if possible
                    desc = 'N/A'
                    try:
                        scraper = cloudscraper.create_scraper()
                        dr = scraper.get(event_url, timeout=10)
                        if dr.status_code == 200:
                            ds = BeautifulSoup(dr.text, 'html.parser')

                            # Tekniska descriptions usually live here
                            p_tags = ds.select('main p')
                            texts = [p.get_text(strip=True) for p in p_tags if len(p.get_text(strip=True)) > 30]

                            if texts:
                                desc = max(texts, key=len)[:500]
                    except:
                        pass
                    
                    # LOCATION EXTRACTION FOR TEKNISKA MUSEET
                    # Two main locations:
                    # 1. Main museum: Museivägen 7, Stockholm (Norra Djurgården)
                    # 2. Tensta branch: Hagstråket 11, Tensta
                    location = "Tekniska museet, Museivägen 7"  # Default to main museum
                    
                    try:
                        # Check the event type label for "Tensta" indicator
                        type_label_el = card.locator('.event-archive-item-type span')
                        if await type_label_el.count() > 0:
                            type_label_txt = (await type_label_el.first.inner_text()).strip().lower()
                            if 'tensta' in type_label_txt:
                                location = "Tekniska museet - Tensta, Hagstråket 11"
                        
                        # Also check the event title for "Tensta" (e.g., "Helgäventyr i Tensta")
                        if 'tensta' in name.lower():
                            location = "Tekniska museet - Tensta, Hagstråket 11"
                    except Exception as e:
                        self.logger.debug(f"Location extraction fallback for Tekniska: {e}")

                    item = EventCategoryItem()
                    item['event_name'] = name.strip()
                    item['event_url'] = response.urljoin(link)
                    item['date_iso'] = date_to_use
                    item['date'] = date_to_use
                    item['end_date_iso'] = end_date or 'N/A'
                    item['time'] = 'N/A'
                    item['location'] = location
                    item['description'] = desc
                    item['target_group'] = target_group
                    item['target_group_normalized'] = target_group_normalized
                    item['status'] = detect_cancelled_status(name)
                    item['booking_info'] = 'N/A'
                    item['image_url'] = image_url
                    item['age_limit'] = age_limit  # Age range like "0-6", "8+", "12-15"

                    yield item
                        
                except Exception as e:
                    self.logger.error(f"Error processing event {i}: {e}")
                    continue
            
            self.logger.info(f"Processed {len(seen)} events from {count} visible events")

        except Exception as e:
            self.logger.error(f"Tekniska calendar error: {e}")

        finally:
            await page.close()

    async def handle_moderna(self, page, response):
        selectors = self.db.get_selectors(response.url)
        if not selectors:
            self.logger.warning("No selectors for Moderna")
            await page.close()
            return
        sel_items = selectors.get('items', {})
        container = selectors.get('container')
        content = await page.content()
        s = scrapy.Selector(text=content)
        days = s.css('.calendar__day')
        today = datetime.now().date()
        limit = today + timedelta(days=self.scrape_days)
        for day in days:
            date_str = day.attrib.get('data-date')
            if not date_str:
                continue
            try:
                ed = datetime.strptime(date_str, "%Y-%m-%d").date()
                if ed < today or ed > limit:
                    continue
            except:
                continue
            for event in day.css(container):
                title = event.css(sel_items.get('event_name', '.calendar__item-title::text')).get()
                if not title:
                    continue
                item = EventCategoryItem()
                item['event_name'] = title.strip()
                item['event_url'] = response.urljoin(event.css(sel_items.get('event_url', '::attr(href)')).get() or '')
                item['time'] = extract_time_only(event.css(sel_items.get('time', 'time::text')).get())
                item['description'] = (event.css(sel_items.get('description', 'p::text')).get() or 'N/A').strip()
                # Location extraction: prioritize the specific location link (contains 'karta-museet' in href)
                # The .read-more class links are exhibition names, not locations
                location_text = event.css('.calendar__item-share a[href*="karta-museet"]::text').get()
                if not location_text:
                    # Try alternative: links that are not .read-more and not .facebook
                    location_text = event.css('.calendar__item-share li a:not(.read-more):not(.facebook)::text').get()
                item['location'] = (location_text or 'Moderna Museet').strip()
                item['date_iso'] = date_str
                item['date'] = date_str
                item['end_date_iso'] = 'N/A'
                
                # Extract target group from event name and description
                target_group, target_group_normalized = extract_target_group_from_name(title.strip(), item['description'])
                item['target_group'] = target_group
                item['target_group_normalized'] = target_group_normalized
                
                item['status'] = detect_cancelled_status(title, item['description'])
                item['booking_info'] = 'N/A'
                # Use the 'event' selector to find the image INSIDE the current event block
                image_selector = sel_items.get('image_url', '.calendar__item-thumbnail img::attr(src)')
                img = event.css(image_selector).get()

                # Fallback: If no image in the block, try the metadata (og:image) from the full page 's'
                if not img:
                    img = s.css('meta[property="og:image"]::attr(content)').get()

                item['image_url'] = img
                
                # Extract age limit from description for Moderna Museet
                item['age_limit'] = self.extract_age_limit(item['description']) or 'N/A'
                
                yield item
        await page.close()

    async def handle_armemuseum(self, page, response):
        # Extract cards with image URLs
        cards = await page.evaluate("""() => {
            const c = []; 
            const links = Array.from(document.querySelectorAll('a[href*="/event/"]')); 
            links.forEach(l => {
                const n = l.querySelector('span.font-mulish.font-black'); 
                const d = l.querySelector('span.text-xs.leading-7.font-roboto') || l.querySelector('span.font-roboto'); 
                const img = l.querySelector('img');
                if (n && d && l.href.includes('/event/')) {
                    c.push({
                        name: n.innerText.trim(), 
                        dateRange: d.innerText.trim(), 
                        url: l.href,
                        imageUrl: img ? img.src : null
                    })
                }
            }); 
            const seen = new Set(); 
            return c.filter(x => {if (seen.has(x.url)) return false; seen.add(x.url); return true})
        }""")
        await page.close()
        for card in cards:
            name = card.get('name', 'Unknown')
            dr = card.get('dateRange', '')
            url = card.get('url', '')
            image_url = card.get('imageUrl')
            date_iso = None
            end_iso = None
            if ' - ' in dr:
                parts = dr.split(' - ')
                date_iso = parse_swedish_date(parts[0].strip())
                end_iso = parse_swedish_date(parts[1].strip())
            else:
                date_iso = parse_swedish_date(dr)
            if not date_iso:
                continue
            yield scrapy.Request(url, callback=self.parse_arme_detail, dont_filter=True, meta={
                'playwright': True, 
                'playwright_include_page': True, 
                'playwright_page_methods': [PageMethod("wait_for_load_state", "domcontentloaded")], 
                'event_name': name, 
                'date_iso': date_iso, 
                'end_date_iso': end_iso,
                'image_url': image_url
            })

    async def parse_arme_detail(self, response):
        import re
        page = response.meta.get("playwright_page")
        name = response.meta.get('event_name')
        date_iso = response.meta.get('date_iso')
        end_iso = response.meta.get('end_date_iso')
        image_url = response.meta.get('image_url')  # Get image from listing page
        if not name or not date_iso:
            if page:
                await page.close()
            return
        desc = 'N/A'
        time_info = 'N/A'
        event_type_label = ''  # For target group like "FAMILJEVISNING", "SPORTLOV"
        if page:
            # Extract event type label (e.g., "FAMILJEVISNING", "SPORTLOV")
            # This is displayed above the title and indicates target group
            try:
                label_el = page.locator('div.text-xl.uppercase.mb-4, div.font-roboto.uppercase')
                if await label_el.count() > 0:
                    event_type_label = (await label_el.first.inner_text()).strip()
                    self.logger.info(f"Found event type label for {name}: {event_type_label}")
            except Exception as e:
                self.logger.debug(f"Could not extract event type label: {e}")
            
            # Extract description
            desc_els = page.locator('.richtext p')
            if await desc_els.count() > 0:
                desc = ' '.join(await desc_els.all_inner_texts()).strip()[:500]
            
            # Extract timing information
            try:
                # Look for specific time patterns in the page
                time_elements = await page.locator('span:has-text(":00"), span:has-text(":30")').all()
                times = []
                for element in time_elements:
                    text = await element.inner_text()
                    # Extract time patterns like "15:00", "10:00", etc.
                    time_match = re.search(r'\b\d{1,2}:\d{2}\b', text)
                    if time_match:
                        times.append(time_match.group())
                
                # Also look for time elements in the date/time sections
                if not times:
                    # Look for spans that contain time patterns
                    time_spans = await page.locator('span').all()
                    for span in time_spans:
                        text = await span.inner_text()
                        time_match = re.search(r'\b\d{1,2}:\d{2}\b', text)
                        if time_match:
                            times.append(time_match.group())
                
                if times:
                    # Remove duplicates and get the most common time
                    unique_times = list(dict.fromkeys(times))
                    if len(unique_times) == 1:
                        time_info = unique_times[0]
                    else:
                        # For multiple times, show the first one as it's usually the main time
                        time_info = unique_times[0]
                
                # Also check for general timing information in practical info section
                if time_info == 'N/A':
                    practical_info = await page.locator('.richtext:has-text("Praktisk information") p').all_inner_texts()
                    for info_text in practical_info:
                        if 'kl' in info_text.lower() or 'tid' in info_text.lower() or 'öppettider' in info_text.lower():
                            # Only extract if it contains a specific time pattern
                            time_match = re.search(r'\b\d{1,2}[:.]\d{2}\b', info_text)
                            if time_match:
                                time_info = time_match.group()
                                break
                            # Skip general descriptions like "när som helst" or "mellan X och Y"
                            # These should remain as 'N/A' since they're not specific times
                            
            except Exception as e:
                self.logger.warning(f"Could not extract time for {response.url}: {e}")
                time_info = 'N/A'
            
            # NEW: Check for recurring events (Fler datum section) AND main event date
            try:
                # Dictionary to group times by date: {date: [time1, time2, ...]}
                occurrences_dict = {}
                
                # First, extract the main event date
                try:
                    main_date_elements = await page.locator('span.ml-8.py-4.text-text.text-5xl').all()
                    for element in main_date_elements:
                        try:
                            main_date_text = await element.inner_text()
                            # Parse Swedish date format
                            parsed_date = parse_swedish_date(main_date_text.strip())
                            if parsed_date:
                                # Extract time for this main date if available
                                main_time = time_info  # Use the time extracted earlier
                                if parsed_date not in occurrences_dict:
                                    occurrences_dict[parsed_date] = []
                                if main_time and main_time != 'N/A':
                                    occurrences_dict[parsed_date].append(main_time)
                                self.logger.info(f"Found main event date: {parsed_date} at {main_time}")
                                break  # Take the first valid main date
                        except Exception as e:
                            self.logger.warning(f"Could not parse main date: {e}")
                            continue
                except Exception as e:
                    self.logger.warning(f"Error extracting main date: {e}")
                
                # Then, extract additional dates from "Fler datum" section
                # Try multiple selectors as the structure varies
                fler_datum_sections = [
                    'ul.richtext',  # Standard list
                    'div:has-text("FLER DATUM FÖR EVENEMANGET") + *',  # Section after header
                    'div.richtext:has(ul)',  # Richtext containing ul
                ]
                
                for selector in fler_datum_sections:
                    try:
                        fler_datum_list = page.locator(selector)
                        fler_datum_count = await fler_datum_list.count()
                        
                        if fler_datum_count > 0:
                            self.logger.info(f"Found 'Fler datum' section with selector '{selector}' for {name}")
                            
                            # Get all list items that contain dates
                            list_items = fler_datum_list.locator('li.list-none, li')
                            count = await list_items.count()
                            
                            for i in range(count):
                                try:
                                    li = list_items.nth(i)
                                    li_text = await li.inner_text()
                                    
                                    # Look for date and time patterns
                                    date_span = await li.locator('span.text-4xl, span:has-text("februari"), span:has-text("mars")').first.inner_text() if await li.locator('span.text-4xl, span:has-text("februari"), span:has-text("mars")').count() > 0 else None
                                    
                                    if date_span:
                                        # Parse Swedish date format
                                        parsed_date = parse_swedish_date(date_span.strip())
                                        
                                        # Extract time - look for span with time or parse from text
                                        time_match = re.search(r'\b(\d{1,2}:\d{2})\b', li_text)
                                        parsed_time = time_match.group(1) if time_match else time_info
                                        
                                        if parsed_date:
                                            if parsed_date not in occurrences_dict:
                                                occurrences_dict[parsed_date] = []
                                            if parsed_time and parsed_time != 'N/A' and parsed_time not in occurrences_dict[parsed_date]:
                                                occurrences_dict[parsed_date].append(parsed_time)
                                except Exception as e:
                                    self.logger.warning(f"Could not parse occurrence {i}: {e}")
                                    continue
                            
                            if occurrences_dict:
                                break  # Found dates, no need to try other selectors
                    except Exception as e:
                        self.logger.debug(f"Selector '{selector}' didn't work: {e}")
                        continue
                
                # Yield separate events for each date with combined times
                if occurrences_dict:
                    self.logger.info(f"Yielding {len(occurrences_dict)} date(s) for {name}")
                    for occurrence_date, times_list in sorted(occurrences_dict.items()):
                        # Combine multiple times into comma-separated string
                        combined_time = ', '.join(sorted(set(times_list))) if times_list else time_info
                        
                        item = EventCategoryItem()
                        item['event_name'] = name
                        item['event_url'] = response.url
                        item['date_iso'] = occurrence_date
                        item['date'] = occurrence_date
                        item['end_date_iso'] = 'N/A'  # Single occurrence
                        item['time'] = combined_time if combined_time else 'N/A'
                        item['location'] = "Armémuseum"
                        item['description'] = desc
                        # Extract target group from event name, description, AND event type label
                        combined_text = f"{desc} {event_type_label}"
                        target_group, target_group_normalized = extract_target_group_from_name(name, combined_text)
                        item['target_group'] = target_group
                        item['target_group_normalized'] = target_group_normalized
                        item['status'] = detect_cancelled_status(name, desc)
                        item['booking_info'] = 'N/A'
                        item['image_url'] = image_url
                        item['age_limit'] = 'N/A'  # Set to N/A for Armemuseum
                        yield item
                    
                    await page.close()
                    return  # Don't yield the original range-based item since we have specific dates
                        
            except Exception as e:
                self.logger.warning(f"Error checking for recurring events: {e}")
                # Fall through to single event logic
            
            await page.close()
        
        # Single occurrence event (no Fler datum section found)
        item = EventCategoryItem()
        item['event_name'] = name
        item['event_url'] = response.url
        item['date_iso'] = date_iso
        item['date'] = date_iso
        item['end_date_iso'] = end_iso or 'N/A'
        item['time'] = time_info
        item['location'] = "Armémuseum"
        item['description'] = desc
        # Extract target group from event name, description, AND event type label
        combined_text = f"{desc} {event_type_label}"
        target_group, target_group_normalized = extract_target_group_from_name(name, combined_text)
        item['target_group'] = target_group
        item['target_group_normalized'] = target_group_normalized
        item['status'] = detect_cancelled_status(name, desc)
        item['booking_info'] = 'N/A'
        item['image_url'] = image_url
        item['age_limit'] = 'N/A'  # Set to N/A for Armemuseum
        yield item

    async def handle_generic(self, page, response):
        # ============================================================
        # GRAPHQL FAST PATH: Skip browser automation for Stockholm Library
        # Works for both /evenemang (isSchoolEvent=false) and /forskolor (isSchoolEvent=true)
        # ============================================================
        if USE_GRAPHQL_FOR_STOCKHOLM and "biblioteket.stockholm.se" in response.url:
            is_school_event = "forskolor" in response.url
            self.logger.info(f"Stockholm GraphQL: Using direct API for {response.url} (isSchoolEvent={is_school_event})")
            await page.close()  # Don't need browser for GraphQL
            graphql_yielded = 0
            try:
                async for item in self.handle_stockholm_library_graphql(response, is_school_event=is_school_event):
                    graphql_yielded += 1
                    yield item
                if graphql_yielded > 0:
                    self.logger.info(f"Stockholm GraphQL: Successfully yielded {graphql_yielded} events")
                    return
            except Exception as e:
                self.logger.warning(f"Stockholm GraphQL: Failed ({e}), falling back to button-clicking")
                # Re-fetch page for fallback - need to reconstruct the request
                # For now, just return empty and log the error
                return
        
        # ============================================================
        # TRADITIONAL PATH: Scroll and click "load more" buttons
        # ============================================================
        # OPTIMIZED: Track event count to detect when no more events are loading
        # Stockholm library typically shows ~25-30 events per page, each click loads ~10-15 more
        if "biblioteket.stockholm.se" in response.url:
            # For Stockholm: Allow enough iterations for full month but with smart early exit
            # With early exit detection, we can safely set a high limit
            max_iterations = min(50, max(30, 20 + self.scrape_days))
        else:
            max_iterations = 20
        
        for _ in range(4):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
        
        # Library site uses 'article' elements; generic sites use broader selector
        is_library = "biblioteket.stockholm.se" in response.url
        event_counter_selector = "article" if is_library else "article, .event, .card, li[class*='event']"
        # Library's GraphQL-backed pagination needs more tolerance for loading delays
        max_no_new_events = 5 if is_library else 3
        click_wait_ms = 2500 if is_library else 1500
        scroll_wait_ms = 1200 if is_library else 800
        
        self.logger.info(f"Starting load more loop (max {max_iterations} iterations for {self.scrape_days} days, counter='{event_counter_selector}')")
        consecutive_failures = 0
        no_new_events_count = 0
        previous_event_count = 0
        
        for i in range(max_iterations):
            clicked = False
            for word in ["Visa fler", "Ladda fler", "Load more", "Visa mer"]:
                try:
                    btn = page.locator(f"button:has-text('{word}'), a:has-text('{word}')").first
                    if await btn.count() > 0 and await btn.is_visible():
                        # Count events before clicking to detect if button actually loads more
                        current_event_count = await page.locator(event_counter_selector).count()
                        
                        self.logger.info(f"Clicking '{word}' button (iteration {i+1}/{max_iterations}, {current_event_count} events visible)")
                        await btn.click(force=True, timeout=3000)
                        await page.wait_for_timeout(click_wait_ms)
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await page.wait_for_timeout(scroll_wait_ms)
                        
                        # Check if new events were loaded
                        new_event_count = await page.locator(event_counter_selector).count()
                        if new_event_count == current_event_count:
                            no_new_events_count += 1
                            self.logger.info(f"No new events loaded ({no_new_events_count}/{max_no_new_events} strikes)")
                            if no_new_events_count >= max_no_new_events:
                                self.logger.info(f"Button stopped loading events after {i+1} clicks, stopping (final count: {new_event_count})")
                                break  # Exit loop - no more events to load
                        else:
                            no_new_events_count = 0  # Reset counter if events loaded
                            self.logger.info(f"Loaded {new_event_count - current_event_count} new events (total: {new_event_count})")
                        
                        clicked = True
                        consecutive_failures = 0
                        break
                except Exception as e:
                    self.logger.debug(f"Error clicking '{word}' button: {e}")
                    continue
            if not clicked:
                consecutive_failures += 1
                # Try scrolling to trigger lazy loading before giving up
                if consecutive_failures <= 3:
                    self.logger.info(f"No load button found, scrolling to try loading more (attempt {consecutive_failures}/3)")
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1500)
                else:
                    self.logger.info(f"No more load buttons found after {consecutive_failures} attempts, stopping after {i+1} iterations")
                    break
        
        # Log final event count for debugging
        final_count = await page.locator(event_counter_selector).count()
        self.logger.info(f"Finished loading: {final_count} total events on page after {i+1} iterations")
        
        selectors = self.db.get_selectors(response.url)
        if not selectors:
            self.logger.warning(f"No selectors for {response.url}")
            await page.close()
            return
        
        # Special handling for Stockholm library to extract target audience from JSON data
        if "biblioteket.stockholm.se" in response.url:
            async for item in self.handle_stockholm_library(page, response, selectors):
                yield item
            return
        
        extracted = await self.extract_with_selectors(page, selectors)
        await page.close()
        today = datetime.now().date()
        limit = today + timedelta(days=self.scrape_days)
        for data in extracted:
            raw = data.get('date_iso')
            if not raw:
                continue
            date_str = parse_swedish_date(raw)
            if not date_str:
                continue
            try:
                ed = datetime.strptime(date_str, "%Y-%m-%d").date()
                if not (today <= ed <= limit):
                    continue
            except:
                continue
            item = EventCategoryItem()
            item['event_name'] = data.get('event_name', 'Unknown')
            item['event_url'] = response.urljoin(data.get('event_url', ''))
            item['date_iso'] = date_str
            item['date'] = date_str
            item['end_date_iso'] = 'N/A'
            item['time'] = extract_time_only(data.get('time'))
            item['location'] = data.get('location', 'N/A')
            item['description'] = data.get('description', 'N/A')
            item['status'] = detect_cancelled_status(item['event_name'], item['description'])
            booking_raw = str(data.get('booking_status', '')).replace('None', '').strip()
            if "Datum:" in booking_raw:
                booking_raw = booking_raw.split("Datum:")[0].strip()
            item['booking_info'] = extract_booking_info(booking_raw)
            item['image_url'] = data.get('image_url')
            item['age_limit'] = 'N/A'  # Set to N/A for generic sites
            if "forskolor" in response.url:
                item['target_group'] = "Preschool"
                item['target_group_normalized'] = "preschool_groups"
            else:
                raw_t = data.get('target_group_raw', '')
                if raw_t and 'målgrupp' in raw_t.lower():
                    tv = raw_t.split(':', 1)[1].strip() if ':' in raw_t else raw_t.replace('Målgrupp', '').strip()
                    item['target_group'] = tv
                    item['target_group_normalized'] = self.simple_normalize(tv)
                else:
                    item['target_group'] = 'All'
                    item['target_group_normalized'] = 'all_ages'
            desc = item.get('description', 'N/A')
            if (desc == 'N/A' or len(desc) < 30) and item['event_url'] != response.url:
                yield scrapy.Request(item['event_url'], callback=self.parse_detail, dont_filter=True, meta={'item': item, 'source_url': response.url, 'playwright': True, 'playwright_include_page': True, 'playwright_page_methods': [PageMethod("wait_for_load_state", "domcontentloaded")]})
            else:
                yield item

    async def handle_stockholm_library(self, page, response, selectors):
        """Special handler for Stockholm library to extract target audience from JSON data"""
        extracted = await self.extract_with_selectors(page, selectors)
        await page.close()
        today = datetime.now().date()
        limit = today + timedelta(days=self.scrape_days)
        
        # Track date coverage for debugging
        dates_seen = set()
        events_yielded = 0
        events_skipped_past = 0
        events_skipped_future = 0
        
        self.logger.info(f"Stockholm Library: Processing {len(extracted)} extracted events for date range {today} to {limit}")
        
        for data in extracted:
            raw = data.get('date_iso')
            if not raw:
                continue

            # Split date range handling en-dashes, em-dashes, and hyphens
            parts = re.split(r'\s*[-–—]\s*', raw)
            start_date = parse_swedish_date(parts[0])
            # FIX: Set end_date to None if there's no date range, not start_date
            end_date = parse_swedish_date(parts[1]) if len(parts) > 1 else None

            # FIX: Handle year boundary issues for date ranges
            # If end date has explicit year, use it to correctly set start date year
            if start_date and end_date and len(parts) > 1:
                start_year = int(start_date.split('-')[0])
                start_month = int(start_date.split('-')[1])
                start_day = start_date.split('-')[2]
                end_year = int(end_date.split('-')[0])
                end_month = int(end_date.split('-')[1])
                
                # Check if end date has explicit year in the raw string
                has_explicit_year = re.search(r'\b(20\d{2})\b', parts[1])
                # Check if start date has explicit year in the raw string
                start_has_explicit_year = re.search(r'\b(20\d{2})\b', parts[0])
                
                # If end has explicit year but start doesn't, we need to figure out start's year
                if has_explicit_year and not start_has_explicit_year:
                    # Simple logic: determine if this is a year-crossing range
                    if start_month > end_month:
                        # Year boundary crossing (e.g., "15 dec - 15 jan 2026")
                        # Start is in previous year
                        best_start_date = f"{end_year - 1}-{start_month:02d}-{start_day}"
                    else:
                        # Same calendar year range (e.g., "2 feb - 6 feb 2026")
                        # Start is in same year as end
                        best_start_date = f"{end_year}-{start_month:02d}-{start_day}"
                    
                    start_date = best_start_date
                    self.logger.info(f"Adjusted start date to {start_date} based on end date {end_date}")

            if not start_date:
                continue

            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else start_dt

            # Track dates seen for debugging
            dates_seen.add(start_date)

            # ✅ Correct overlap logic
            if end_dt < today:
                events_skipped_past += 1
                continue  # Event already finished

            if start_dt > limit:
                events_skipped_future += 1
                continue  # Event starts too far in future
            
            events_yielded += 1
            # Extract target audience from event detail page using regular HTTP (fast!)
            event_url = response.urljoin(data.get('event_url', ''))
            yield scrapy.Request(event_url, callback=self.parse_stockholm_library_detail, dont_filter=True, meta={
                'data': data, 
                'start_date': start_date,
                'end_date': end_date,
                'source_url': response.url
            })
        
        # Log summary of date coverage
        sorted_dates = sorted(dates_seen)
        self.logger.info(f"Stockholm Library: Yielded {events_yielded} events, skipped {events_skipped_past} past, {events_skipped_future} future")
        self.logger.info(f"Stockholm Library: Date range seen: {sorted_dates[0] if sorted_dates else 'none'} to {sorted_dates[-1] if sorted_dates else 'none'}")
        self.logger.info(f"Stockholm Library: All unique dates ({len(sorted_dates)}): {sorted_dates[:15]}{'...' if len(sorted_dates) > 15 else ''}")

    async def handle_stockholm_library_graphql(self, response, is_school_event=False):
        """
        Direct GraphQL API handler for Stockholm library - bypasses browser automation.
        Much faster and more reliable than clicking 'Visa fler' button repeatedly.
        Date filtering is done client-side since the API returns all upcoming events.
        """
        today = datetime.now().date()
        limit = today + timedelta(days=self.scrape_days)
        
        self.logger.info(f"Stockholm GraphQL: Fetching events for {today} to {limit} ({self.scrape_days} days, isSchoolEvent={is_school_event})")
        
        # Track statistics
        total_fetched = 0
        events_yielded = 0
        events_filtered_date = 0
        from_offset = 0
        
        scraper = cloudscraper.create_scraper()
        
        while True:
            # Note: Date filtering is done client-side - API returns all upcoming events
            variables = {
                "query": "",  # Empty query = all events
                "from": from_offset,
                "size": STOCKHOLM_GRAPHQL_PAGE_SIZE,
                "isSchoolEvent": is_school_event
            }
            
            try:
                api_response = scraper.post(
                    STOCKHOLM_GRAPHQL_URL,
                    json={
                        "query": STOCKHOLM_EVENT_SEARCH_QUERY,
                        "variables": variables,
                        "_operation": "eventSearch"
                    },
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "*/*",
                        "Origin": "https://biblioteket.stockholm.se",
                        "Referer": response.url
                    },
                    timeout=30
                )
                
                if api_response.status_code != 200:
                    self.logger.error(f"Stockholm GraphQL: API returned status {api_response.status_code}")
                    self.logger.error(f"Stockholm GraphQL: Response: {api_response.text[:500]}")
                    break
                
                data = api_response.json()
                event_search = data.get('data', {}).get('eventSearch', {})
                total_results = event_search.get('results', 0)
                events = event_search.get('events', [])
                
                if not events:
                    self.logger.info(f"Stockholm GraphQL: No more events at offset {from_offset}")
                    break
                
                total_fetched += len(events)
                self.logger.info(f"Stockholm GraphQL: Fetched {len(events)} events (offset {from_offset}, total available: {total_results})")
                
                # Process each event with date filtering
                for event in events:
                    item = self._parse_graphql_event(event, is_school_event, response.url, today, limit)
                    if item:
                        events_yielded += 1
                        yield item
                    elif item is False:
                        # Item was filtered out by date
                        events_filtered_date += 1
                
                # Check if we've fetched all events
                from_offset += STOCKHOLM_GRAPHQL_PAGE_SIZE
                if from_offset >= total_results:
                    self.logger.info(f"Stockholm GraphQL: Reached end of results ({total_results} total)")
                    break
                    
            except Exception as e:
                self.logger.error(f"Stockholm GraphQL: Error fetching events: {e}")
                import traceback
                self.logger.error(f"Stockholm GraphQL: Traceback: {traceback.format_exc()}")
                # Fall back to traditional method on error
                self.logger.warning("Stockholm GraphQL: Falling back to button-clicking method")
                return  # Caller will handle fallback
        
        self.logger.info(f"Stockholm GraphQL: Complete - fetched {total_fetched}, yielded {events_yielded}, filtered by date {events_filtered_date}")
        self.spider_stats['graphql_events_fetched'] = total_fetched
        self.spider_stats['graphql_events_yielded'] = events_yielded
        self.spider_stats['graphql_events_filtered_date'] = events_filtered_date

    def _parse_graphql_event(self, event, is_school_event, source_url, today=None, limit=None):
        """Parse a single event from GraphQL response into EventCategoryItem.
        Returns item if valid, None if parse error, False if filtered by date.
        """
        try:
            # Parse dates from Swedish format
            date_time = event.get('dateTime', {})
            start_date = parse_graphql_swedish_date(date_time.get('startDate'))
            stop_date = parse_graphql_swedish_date(date_time.get('stopDate'))
            
            if not start_date:
                return None
            
            # Date filtering (client-side since API doesn't support date range filtering)
            if today and limit:
                try:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                    end_dt = datetime.strptime(stop_date, "%Y-%m-%d").date() if stop_date else start_dt
                    
                    # Skip events that have already ended
                    if end_dt < today:
                        return False
                    
                    # Skip events that start too far in the future
                    if start_dt > limit:
                        return False
                except ValueError:
                    pass  # If date parsing fails, include the event
            
            # Build event URL from slug - use correct path for school vs regular events
            slug = event.get('eventSlugId', '')
            if slug:
                url_path = 'forskolor' if is_school_event else 'evenemang'
                event_url = f"https://biblioteket.stockholm.se/{url_path}/{slug}"
            else:
                event_url = ''
            
            # Extract time
            start_time = date_time.get('startTime', '')
            stop_time = date_time.get('stopTime', '')
            if start_time and stop_time:
                time_str = f"{start_time}-{stop_time}"
            elif start_time:
                time_str = start_time
            else:
                time_str = 'N/A'
            
            # Extract description from preamble
            description = event.get('description', {})
            desc_text = description.get('preamble', '') if description else ''
            if not desc_text:
                desc_text = event.get('otherInformation', '') or 'N/A'
            
            # Get image URL
            image = event.get('image', {})
            image_url = image.get('url', '') if image else ''
            
            # Determine target group
            target_audiences = event.get('targetAudiences', [])
            if is_school_event or "forskolor" in source_url:
                target_group = "Preschool"
                target_group_normalized = "preschool_groups"
            elif target_audiences:
                target_group = ', '.join(target_audiences)
                target_group_normalized = self.simple_normalize(target_group)
            else:
                target_group = 'All'
                target_group_normalized = 'all_ages'
            
            # Determine booking info
            if event.get('bookable'):
                booking_status = event.get('bookingStatus', '')
                if booking_status and 'fullbokat' in str(booking_status).lower():
                    booking_info = 'Fully booked'
                else:
                    booking_info = 'Requires booking'
            else:
                booking_info = 'N/A'
            
            # Determine event status
            title = event.get('title', '')
            status = 'cancelled' if event.get('canceled') else detect_cancelled_status(title, desc_text)
            
            # Extract location - prefer library name
            location = event.get('library') or event.get('location') or 'N/A'
            
            # Create item
            item = EventCategoryItem()
            item['event_name'] = title
            item['event_url'] = event_url
            item['date_iso'] = start_date
            item['date'] = start_date
            item['end_date_iso'] = stop_date if stop_date and stop_date != start_date else 'N/A'
            item['time'] = time_str
            item['location'] = location
            item['description'] = desc_text[:500] if desc_text else 'N/A'
            item['target_group'] = target_group
            item['target_group_normalized'] = target_group_normalized
            item['status'] = status
            item['booking_info'] = booking_info
            item['image_url'] = image_url if image_url else None
            item['age_limit'] = self.extract_age_limit(desc_text) or 'N/A'
            
            return item
            
        except Exception as e:
            self.logger.warning(f"Stockholm GraphQL: Error parsing event {event.get('id', 'unknown')}: {e}")
            return None

    def parse_stockholm_library_detail(self, response):
        data = response.meta.get('data')
        start_date = response.meta.get('start_date')
        end_date = response.meta.get('end_date')

        if not data or not start_date:
            return

        desc = data.get('description', 'N/A')

        try:
            scraper = cloudscraper.create_scraper()
            r = scraper.get(response.url, timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                ps = soup.select('.event-description p, .event-content p, p')
                texts = [p.get_text(strip=True) for p in ps if len(p.get_text(strip=True)) > 30]
                if texts:
                    desc = max(texts, key=len)[:500]
        except:
            pass

        item = EventCategoryItem()
        item['event_name'] = data.get('event_name')
        item['event_url'] = response.url
        item['date_iso'] = start_date              # ✅ listing page
        item['date'] = start_date
        item['end_date_iso'] = end_date or 'N/A'   # ✅ listing page
        item['time'] = extract_time_only(data.get('time'))
        item['location'] = data.get('location', 'N/A')
        item['description'] = desc
        item['status'] = detect_cancelled_status(item['event_name'], desc)
        
        # Extract and normalize target group from listing page data
        source_url = response.meta.get('source_url', '')
        if "forskolor" in source_url:
            item['target_group'] = "Preschool"
            item['target_group_normalized'] = "preschool_groups"
        else:
            raw_target = data.get('target_group_raw', '')
            if raw_target and 'målgrupp' in raw_target.lower():
                # Extract the value after "Målgrupp:"
                target_value = raw_target.split(':', 1)[1].strip() if ':' in raw_target else raw_target.replace('Målgrupp', '').strip()
                item['target_group'] = target_value
                item['target_group_normalized'] = self.simple_normalize(target_value)
            else:
                item['target_group'] = 'All'
                item['target_group_normalized'] = 'all_ages'
        
        item['booking_info'] = extract_booking_info(data.get('booking_status', ''))
        item['image_url'] = data.get('image_url')
        
        # Extract age limit from description, default to N/A if not found
        age_limit = self.extract_age_limit(desc)
        item['age_limit'] = age_limit if age_limit else 'N/A'

        yield item

    async def parse_detail(self, response):
        page = response.meta.get("playwright_page")
        item = response.meta.get('item')
        if not item:
            if page:
                try:
                    await page.close()
                except:
                    pass
            return
        
        desc = 'N/A'
        try:
            if page:
                for sel in ['.description', '.event-description', '[class*="description"]', 'p', '.content']:
                    try:
                        els = page.locator(sel)
                        if await els.count() > 0:
                            texts = await els.all_inner_texts()
                            valid = [t.strip() for t in texts if len(t.strip()) > 20]
                            if valid:
                                desc = max(valid, key=len)[:500]
                                break
                    except Exception as e:
                        self.logger.debug(f"Error extracting description with selector {sel}: {e}")
                        continue
        except Exception as e:
            self.logger.warning(f"Error in parse_detail for {item.get('event_url')}: {e}")
        finally:
            if page:
                try:
                    await page.close()
                except Exception as e:
                    self.logger.debug(f"Error closing page: {e}")
        
        item['description'] = desc
        item['age_limit'] = 'N/A'  # Set to N/A for general detail parsing
        yield item

    async def extract_with_selectors(self, page, selectors):
        extracted = []
        container = selectors.get('container')
        items = selectors.get('items', {})
        if not container:
            return []
        elements = await page.locator(container).all()
        for el in elements:
            item = {}
            for field, sel in items.items():
                try:
                    # Handle target_group_raw - need to find the specific p tag with "Målgrupp:"
                    if field == 'target_group_raw':
                        all_p = el.locator(sel)
                        cnt = await all_p.count()
                        txt = ''
                        for i in range(cnt):
                            try:
                                pt = await all_p.nth(i).inner_text()
                                if pt and 'målgrupp' in pt.lower():
                                    txt = pt
                                    break
                            except:
                                continue
                        item[field] = txt if txt else None
                        continue
                    
                    if field == 'booking_status':
                        all_p = el.locator(sel)
                        cnt = await all_p.count()
                        txt = ''
                        for i in range(cnt):
                            try:
                                pt = await all_p.nth(i).inner_text()
                                if pt and any(k in pt.lower() for k in ['boka', 'bokning', 'drop-in', 'fullbokat']):
                                    txt = pt
                                    break
                            except:
                                continue
                        item[field] = txt if txt else None
                        continue
                    
                    # Handle image_url extraction - look for src attribute on img tags
                    if field == 'image_url':
                        target = el.locator(sel).first
                        if await target.count() > 0:
                            # Try to get src attribute from img tag
                            src = await target.get_attribute('src')
                            if not src:
                                # Try data-src for lazy-loaded images
                                src = await target.get_attribute('data-src')
                            if not src:
                                # Try srcset and get first URL
                                srcset = await target.get_attribute('srcset')
                                if srcset:
                                    src = srcset.split(',')[0].split()[0]
                            item[field] = src if src else None
                        else:
                            item[field] = None
                        continue
                    
                    target = el.locator(sel).first
                    if await target.count() > 0:
                        val = None
                        if field == 'event_url':
                            val = await target.get_attribute('href')
                        elif field in ('date_iso', 'time') and 'time' in sel:
                            dt = await target.get_attribute('datetime')
                            val = dt if dt else await target.inner_text()
                        else:
                            val = await target.inner_text()
                        if val:
                            item[field] = re.sub(r'\s+', ' ', val).strip()
                        else:
                            item[field] = None
                    else:
                        item[field] = None
                except:
                    item[field] = None
            if item.get('event_name'):
                extracted.append(item)
        return extracted

    def simple_normalize(self, target_str):
        if not target_str:
            return 'all_ages'
        t = target_str.lower()
        if any(k in t for k in ['barn', 'kid', 'bebis', 'småbarn', 'förskola', 'for children', 'för barn']):
            return 'children'
        if any(k in t for k in ['ungdom', 'teen', 'tonåring', 'unga']):
            return 'teens'
        if 'familj' in t or 'family' in t:
            return 'families'
        if any(k in t for k in ['vuxen', 'vuxna', 'adult', 'senior']):
            return 'adults'
        if any(k in t for k in ['all', 'alla', 'general']):
            return 'all_ages'
        age_match = re.search(r'(\d{1,2})(?:[-–\s]+(\d{1,2}))?\s*(?:år|year|age)', t)
        if age_match:
            try:
                min_age = int(age_match.group(1))
                return 'children' if min_age < 13 else ('teens' if min_age < 20 else 'adults')
            except:
                pass
        return 'all_ages'

    def normalize_tekniska_target(self, target_str):
        if not target_str:
            return 'all_ages'
        t = target_str.lower()
        age_range = re.search(r'(\d{1,2})\s*[-–]\s*(\d{1,2})', t)
        if age_range:
            min_age = int(age_range.group(1))
            max_age = int(age_range.group(2))
            if max_age <= 6:
                return 'children'
            elif min_age >= 10 and max_age <= 19:
                return 'teens'
            else:
                return 'adults'
        age_plus = re.search(r'(\d{1,2})\s*\+', t)
        if age_plus:
            min_age = int(age_plus.group(1))
            return 'children' if min_age <= 12 else ('teens' if min_age < 18 else 'adults')
        if 'småbarn' in t or 'bebis' in t:
            return 'preschool'
        if 'barn' in t or 'kid' in t:
            return 'children'
        if 'klubb' in t:
            return 'teens'
        if 'lov' in t:
            return 'children'
        if 'kurs' in t:
            return 'adults'
        if 'familj' in t or 'family' in t:
            return 'families'
        return 'all_ages'