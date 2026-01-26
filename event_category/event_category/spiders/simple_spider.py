import scrapy
import json
import os
import re
from datetime import datetime, timedelta
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem
from event_category.utils.db_manager import DatabaseManager

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
    """Parse Swedish date string to ISO format (YYYY-MM-DD)."""
    if not date_str:
        return None
    
    date_str = date_str.strip().lower()
    
    # Already in ISO format?
    iso_match = re.match(r'^(\d{4}-\d{2}-\d{2})', date_str)
    if iso_match:
        return iso_match.group(1)
    
    # Try to extract day and month
    match = re.search(r'(\d{1,2})\s+([a-zåäö]+)', date_str)
    if match:
        day = int(match.group(1))
        month_name = match.group(2)
        month = SWEDISH_MONTHS.get(month_name)
        
        if month:
            # Check for explicit year
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
    """Extract only the time component from a datetime string."""
    if not time_str:
        return 'N/A'
    
    time_str = time_str.strip()
    
    # Pattern 1: datetime format
    match = re.search(r'\d{4}-\d{2}-\d{2}[T\s](\d{1,2}[:.]\d{2}(?:[:.]\d{2})?)', time_str)
    if match:
        return match.group(1).replace('.', ':')
    
    # Pattern 2: Swedish format
    match = re.search(r'Tid:\s*(\d{1,2}[:.]\d{2}(?:\s*-\s*\d{1,2}[:.]\d{2})?)', time_str, re.IGNORECASE)
    if match:
        return match.group(1).replace('.', ':')
    
    # Pattern 3: Just time
    match = re.search(r'^(\d{1,2}[:.]\d{2}(?:\s*-\s*\d{1,2}[:.]\d{2})?)$', time_str)
    if match:
        return match.group(1).replace('.', ':')
    
    return time_str

def extract_booking_info(booking_text):
    """Extract booking information from Stockholm Library event text."""
    if not booking_text:
        return 'N/A'
    
    text = booking_text.lower()
    
    # Check for "Fullbokat" (fully booked)
    if 'fullbokat' in text or 'fullbokad' in text:
        return 'Fullbokat'
    
    # Check for booking required
    if 'boka plats' in text or 'du behöver boka' in text or 'bokning krävs' in text:
        return 'Requires booking'
    
    # Check for booking opens info
    if 'bokningen öppnar' in text:
        return 'Requires booking'
    
    # Check for drop-in
    if 'drop-in' in text or 'dropin' in text:
        return 'Drop-in'
    
    return 'N/A'

def detect_cancelled_status(event_name, description='', status_text=''):
    """Detect if an event is cancelled or fully booked."""
    combined = f"{event_name} {description} {status_text}".lower()
    
    cancelled_keywords = [
        'inställt', 'inställd', 'cancelled', 'canceled', 
        'avlyst', 'avlyser', 'ställs in', 'avbokat'
    ]
    
    fullbokat_keywords = ['fullbokat', 'fullbokad', 'fully booked', 'sold out', 'slutsålt']
    
    for keyword in cancelled_keywords:
        if keyword in combined:
            return 'cancelled'
    
    for keyword in fullbokat_keywords:
        if keyword in combined:
            return 'fullbokat'
    
    return 'scheduled'

class SimpleEventSpider(scrapy.Spider):
    name = "simple_events"
    
    # Both URLs we want to scrape
    start_urls = [
        "https://biblioteket.stockholm.se/forskolor",
        "https://biblioteket.stockholm.se/evenemang",
    ]

    def start_requests(self):
        self.db = DatabaseManager()
        
        for url in self.start_urls:
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
        
        # Handle cookie consent
        try:
            cookie_btns = page.locator("button:has-text('Godkänn'), button:has-text('Acceptera'), button:has-text('Jag förstår'), button[id*='cookie']")
            if await cookie_btns.count() > 0:
                await cookie_btns.first.click(force=True, timeout=2000)
                await page.wait_for_timeout(1000)
        except Exception:
            pass

        # Scroll and load more
        for _ in range(4): 
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)

        # Click Load More button - increased for better coverage
        # For Stockholm library and similar sites, need more clicks to load all events
        if "biblioteket.stockholm.se" in response.url:
            limit = 45  # Increased for Stockholm library to cover 10+ days
        else:
            limit = 20
        load_words = ["Visa fler", "Ladda fler", "Load more", "Show more", "More events", "Nästa", "Visa alla"]
        consecutive_failures = 0
        for i in range(limit): 
            clicked = False
            for word in load_words:
                btn = page.locator(f"button:has-text('{word}'), a:has-text('{word}')").first
                if await btn.count() > 0 and await btn.is_visible():
                    try:
                        self.logger.info(f"Clicking load button: '{word}' (iteration {i+1}/{limit})")
                        await btn.click(force=True, timeout=5000)
                        await page.wait_for_timeout(2000)
                        clicked = True
                        consecutive_failures = 0
                        break 
                    except Exception:
                        pass
            if not clicked:
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    break
                # Try scrolling to trigger lazy loading
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500) 

        # Get selectors from database
        selectors = self.db.get_selectors(response.url)
        
        if not selectors:
            self.logger.error(f"No selectors found in database for {response.url}")
            await page.close()
            return

        self.logger.info(f"Using DB selectors: container='{selectors.get('container')}'")
        
        # Extract events using selectors
        extracted_data = await self.extract_with_selectors(page, selectors)
        
        self.logger.info(f"Extracted {len(extracted_data)} events from listing page")
        
        # Process extracted events
        today = datetime.now().date()
        limit_date = today + timedelta(days=30)  # 1 month limit
        
        for event_data in extracted_data:
            try:
                # Parse date
                raw_date = event_data.get('date_iso')
                if not raw_date:
                    continue
                
                date_str = parse_swedish_date(raw_date)
                if not date_str:
                    continue
                
                try:
                    event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    
                    if today <= event_date <= limit_date:
                        # Create item
                        item = EventCategoryItem()
                        item['event_name'] = event_data.get('event_name', 'Unknown Event')
                        item['event_url'] = response.urljoin(event_data.get('event_url', '')) if event_data.get('event_url') else response.url
                        item['date_iso'] = date_str
                        item['date'] = date_str
                        item['end_date_iso'] = 'N/A'  # Single day events
                        item['time'] = extract_time_only(event_data.get('time'))
                        item['location'] = event_data.get('location', 'N/A')
                        item['description'] = event_data.get('description', 'N/A')
                        
                        # Handle target group - forskolor means preschool
                        item['target_group'] = "Preschool"
                        item['target_group_normalized'] = "preschool_groups"
                        
                        item['status'] = detect_cancelled_status(item['event_name'], item['description'])
                        item['booking_info'] = extract_booking_info(event_data.get('booking_status', ''))
                        
                        # For Stockholm Library, we need to fetch detail pages for description
                        if item['description'] == 'N/A' or len(item['description']) < 30:
                            if item['event_url'] and item['event_url'] != response.url:
                                self.logger.info(f"Fetching details for '{item['event_name']}' from: {item['event_url']}")
                                yield scrapy.Request(
                                    item['event_url'],
                                    callback=self.parse_details,
                                    dont_filter=True,
                                    meta={
                                        'item': item,
                                        'source_url': response.url,
                                        'playwright': True,
                                        'playwright_include_page': True,
                                        'playwright_page_methods': [
                                            PageMethod("wait_for_load_state", "domcontentloaded"),
                                            PageMethod("wait_for_timeout", 1000),
                                        ],
                                    }
                                )
                            else:
                                yield item
                        else:
                            yield item
                        
                except ValueError:
                    continue
                    
            except Exception as e:
                self.logger.error(f"Error processing event: {e}")
                continue

        await page.close()

    async def extract_with_selectors(self, page, selectors):
        extracted = []
        container_sel = selectors.get('container')
        item_map = selectors.get('items', {})
        
        if not container_sel: 
            return []
        
        elements = await page.locator(container_sel).all()
        self.logger.info(f"Found {len(elements)} event containers")
        
        for el in elements:
            item = {}
            for field, sel in item_map.items():
                try:
                    # Special handling for booking_status
                    if field == 'booking_status':
                        all_paragraphs = el.locator(sel)
                        count = await all_paragraphs.count()
                        booking_text = ''
                        for i in range(count):
                            try:
                                p_text = await all_paragraphs.nth(i).inner_text()
                                if p_text:
                                    p_lower = p_text.lower()
                                    if any(kw in p_lower for kw in ['boka', 'bokning', 'drop-in', 'dropin', 'fullbokat', 'fullbokad']):
                                        booking_text = p_text
                                        break
                            except Exception:
                                continue
                        item[field] = booking_text if booking_text else None
                        continue
                    
                    target = el.locator(sel).first
                    if await target.count() > 0:
                        value = None
                        
                        # Special handling for event_url to get href
                        if field == 'event_url':
                            value = await target.get_attribute('href')
                        # For date/time fields, try datetime attribute
                        elif field in ('date_iso', 'time') and 'time' in sel:
                            datetime_attr = await target.get_attribute('datetime')
                            if datetime_attr:
                                value = datetime_attr
                            else:
                                value = await target.inner_text()
                        else:
                            value = await target.inner_text()
                        
                        if value:
                             value = re.sub(r'\s+', ' ', value).strip()
                             item[field] = value
                        else:
                             item[field] = None
                    else:
                        item[field] = None
                except Exception:
                    item[field] = None
            
            if item.get('event_name'):
                extracted.append(item)
        
        return extracted

    async def parse_details(self, response):
        page = response.meta.get("playwright_page")
        item = response.meta.get('item')
        
        if not item:
            if page:
                await page.close()
            return
        
        self.logger.info(f"Extracting details from: {response.url}")
        
        try:
            if page:
                # Try to extract description from common selectors
                description_selectors = [
                    '.description',
                    '.event-description',
                    '[class*="description"]',
                    'p',
                    '.content'
                ]
                
                description = 'N/A'
                for desc_sel in description_selectors:
                    desc_els = page.locator(desc_sel)
                    if await desc_els.count() > 0:
                        desc_texts = await desc_els.all_inner_texts()
                        # Get the longest paragraph as description
                        valid_texts = [text.strip() for text in desc_texts if text.strip() and len(text.strip()) > 20]
                        if valid_texts:
                            description = max(valid_texts, key=len)[:500]  # Limit to 500 chars
                            break
                
                await page.close()
            else:
                description = 'N/A'
            
            # Update item with description
            item['description'] = description
            
            yield item
            
        except Exception as e:
            self.logger.error(f"Error extracting details: {e}")
            if page:
                await page.close()
            # Still yield the item even if detail extraction failed
            yield item
