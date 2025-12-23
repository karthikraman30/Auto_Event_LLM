"""
Spider to scrape ALL events from Stockholm Library website.
Extracts: event_name, date, time, location, target_group, description, category, series
Clicks "Visa fler evenemang" button repeatedly until all events are loaded.
Then follows each event link to get detailed information.
"""

import re
from datetime import datetime, timedelta

import scrapy
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem

# Swedish month names to numbers mapping
SWEDISH_MONTHS = {
    'januari': 1, 'februari': 2, 'mars': 3, 'april': 4,
    'maj': 5, 'juni': 6, 'juli': 7, 'augusti': 8,
    'september': 9, 'oktober': 10, 'november': 11, 'december': 12
}

# Target group normalization mapping
TARGET_GROUP_MAPPING = {
    # Children (0-12)
    'barn': 'children',
    'bebis': 'children',
    'småbarn': 'children',
    'förskolebarn': 'children',
    # Teens (13-19)
    'ungdom': 'teens',
    'tonåring': 'teens',
    'ungdomar': 'teens',
    # Adults (20+)
    'vuxen': 'adults',
    'vuxna': 'adults',
    'senior': 'adults',
    'seniorer': 'adults',
    'pensionär': 'adults',
    # Families
    'familj': 'families',
    'familjer': 'families',
    # All ages
    'alla': 'all_ages',
    'alla åldrar': 'all_ages',
}


def parse_swedish_date(date_str):
    """
    Parse Swedish date string to ISO format (YYYY-MM-DD).
    Returns tuple: (start_date, end_date)
    If single day, end_date is "N/A".
    """
    if not date_str:
        return "", "N/A"
    
    date_str = date_str.lower().strip()
    
    # Helper to parse "22 december [2024]"
    def parse_single(d_str, default_year=None):
        match = re.search(r'(\d{1,2})\s+([a-zåäö]+)(?:\s+(\d{4}))?', d_str)
        if match:
            day = int(match.group(1))
            month_name = match.group(2)
            year = int(match.group(3)) if match.group(3) else default_year
            
            if not year:
                year = datetime.now().year

            month = None
            for sw_month, num in SWEDISH_MONTHS.items():
                if month_name.startswith(sw_month[:3]): 
                    month = num
                    break
            
            if month:
                try:
                    return datetime(year, month, day)
                except ValueError:
                    pass
        return None

    # Handle range
    if ' - ' in date_str or '–' in date_str:
        parts = re.split(r'\s*[-–]\s*', date_str)
        if len(parts) >= 2:
            start_str = parts[0]
            end_str = parts[1]
            
            end_dt = parse_single(end_str)
            if end_dt:
                # Use end date year as default for start date if missing
                start_dt = parse_single(start_str, default_year=end_dt.year)
                
                # Handle year rollover (e.g. Dec 22 - Jan 2)
                # If start month is > end month, assume start year is previous year
                if start_dt and start_dt.month > end_dt.month and start_dt.year == end_dt.year:
                    start_dt = start_dt.replace(year=start_dt.year - 1)
                
                if start_dt:
                    return start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
    
    # Fallback / Single date
    dt = parse_single(date_str)
    if dt:
        return dt.strftime('%Y-%m-%d'), "N/A"
    
    return "", "N/A"


def normalize_target_group(target_group_str):
    """
    Normalize Swedish target group to simplified English categories.
    Returns combined categories if multiple are found (e.g., children_and_teens).
    """
    if not target_group_str:
        return ""
    
    target_lower = target_group_str.lower().strip()
    
    # Collect all matching categories
    found_categories = set()
    
    # Check for keyword matches
    for swedish, english in TARGET_GROUP_MAPPING.items():
        if swedish in target_lower:
            found_categories.add(english)
    
    # Try to detect age ranges
    age_match = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*år', target_lower)
    if age_match:
        min_age = int(age_match.group(1))
        max_age = int(age_match.group(2))
        
        if max_age <= 12:
            found_categories.add('children')
        elif min_age >= 13 and max_age <= 19:
            found_categories.add('teens')
        elif min_age >= 18:
            found_categories.add('adults')
        else:
            # Spans multiple categories
            if min_age <= 12:
                found_categories.add('children')
            if max_age >= 13:
                found_categories.add('teens')
            if max_age >= 18:
                found_categories.add('adults')
    
    # Single age check (e.g., "från 7 år")
    single_age = re.search(r'från\s*(\d+)\s*år', target_lower)
    if single_age:
        age = int(single_age.group(1))
        if age <= 12:
            found_categories.add('children')
        elif age <= 19:
            found_categories.add('teens')
        else:
            found_categories.add('adults')
    
    # Return combined categories or original if no match
    if not found_categories:
        return target_group_str
    
    # Sort for consistent ordering: children, teens, adults, families, all_ages
    order = ['children', 'teens', 'adults', 'families', 'all_ages']
    sorted_cats = [c for c in order if c in found_categories]
    
    # If only one category, return it directly
    if len(sorted_cats) == 1:
        return sorted_cats[0]
    
    # Combine with _and_
    return '_and_'.join(sorted_cats)


class EventsSpider(scrapy.Spider):
    name = "events"
    allowed_domains = ["biblioteket.stockholm.se"]
    start_urls = [
        "https://biblioteket.stockholm.se/evenemang",
        "https://biblioteket.stockholm.se/forskolor",  # Preschool groups
    ]
    
    # Configure max events to scrape PER URL (set to 0 or None for unlimited)
    MAX_EVENTS = 50  # Set to 0 for all events, or a number to limit
    
    # Date filter: only include events within this many days from today (set to 0 to disable)
    DATE_FILTER_DAYS = 0  # Disabled - show all events

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        # Wait for events to load initially
                        PageMethod("wait_for_selector", "article", timeout=30000),
                        PageMethod("wait_for_timeout", 2000),
                    ],
                    "source_url": url,  # Track source for target group normalization
                },
                callback=self.parse,
            )

    async def parse(self, response):
        """Parse the events page and collect event URLs to follow."""
        page = response.meta.get("playwright_page")
        source_url = response.meta.get("source_url", response.url)
        
        if page:
            # Skip clicking "Show more" if we have a limit set (initial load has ~20 events)
            if self.MAX_EVENTS and self.MAX_EVENTS <= 20:
                self.logger.info(f"Skipping 'Show more' clicks - limiting to {self.MAX_EVENTS} events")
            else:
                # Keep clicking "Show more" button until we have enough events or button disappears
                click_count = 0
                max_clicks = 100 if not self.MAX_EVENTS else (self.MAX_EVENTS // 10)  # Approx 10 events per click
                
                while click_count < max_clicks:
                    try:
                        # Look for the "Visa fler evenemang och aktiviteter" button
                        show_more_button = await page.query_selector('button:has-text("Visa fler evenemang")')
                        
                        if not show_more_button:
                            # Try alternative selector
                            show_more_button = await page.query_selector('text="Visa fler evenemang och aktiviteter"')
                        
                        if not show_more_button:
                            self.logger.info(f"No more 'Show more' button found after {click_count} clicks")
                            break
                        
                        # Check if button is visible
                        is_visible = await show_more_button.is_visible()
                        if not is_visible:
                            self.logger.info("Show more button is not visible")
                            break
                        
                        # Click the button
                        await show_more_button.click()
                        click_count += 1
                        self.logger.info(f"Clicked 'Show more' button ({click_count} times)")
                        
                        # Wait for new events to load
                        await page.wait_for_timeout(2000)
                        
                    except Exception as e:
                        self.logger.info(f"Stopped clicking: {e}")
                        break
            
            # Get the final page content after all events are loaded
            content = await page.content()
            await page.close()
            
            # Create a new response with the updated content
            from scrapy.http import HtmlResponse
            response = HtmlResponse(
                url=response.url,
                body=content.encode('utf-8'),
                encoding='utf-8',
            )
        
        # Find all event articles
        events = response.xpath("//article")
        total_events = len(events)
        
        # Limit events if MAX_EVENTS is set
        if self.MAX_EVENTS:
            events = events[:self.MAX_EVENTS]
        
        self.logger.info(f"Found {total_events} total events, processing {len(events)}")

        for event in events:
            # Extract basic info from the listing
            name_parts = event.xpath(".//h2/a//text()").getall()
            event_name = " ".join(name_parts).strip() if name_parts else ""
            
            # Extract event URL for detailed info
            event_url = event.xpath(".//h2/a/@href").get()
            
            # Extract date (Datum)
            date_text = event.xpath(".//p[b[contains(text(), 'Datum:')]]/time/text()").get()
            date = date_text.strip() if date_text else ""
            
            # Extract time (Tid)
            time_parts = event.xpath(".//p[b[contains(text(), 'Tid:')]]/time/text()").getall()
            time = "-".join(time_parts).strip()
            if not time:
                time = "N/A"
            
            # Extract location - look for paragraph with location pin icon
            location_candidates = event.xpath(".//section//p[span and not(b)]/text()").getall()
            location = " ".join([loc.strip() for loc in location_candidates if loc.strip()])
            if not location:
                location = event.xpath(".//p[span[@aria-hidden='true']]/text()").get()
                location = location.strip() if location else ""
            
            # Extract target group (Målgrupp)
            target_group = event.xpath(".//p[b[contains(text(), 'Målgrupp:')]]/text()").get()
            target_group = target_group.strip() if target_group else ""
            
            # Only follow if we have an event name and URL
            if event_name and event_url:
                # Make the URL absolute
                full_url = response.urljoin(event_url)
                
                # Follow the event link to get additional details
                yield scrapy.Request(
                    full_url,
                    meta={
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_selector", "article", timeout=30000),
                        ],
                        # Pass basic info to the detail parser
                        "event_name": event_name,
                        "date": date,
                        "time": time,
                        "location": location,
                        "target_group": target_group,
                        "event_url": full_url,
                        "source_url": source_url,  # Track source for normalization
                    },
                    callback=self.parse_event_detail,
                )

    def parse_event_detail(self, response):
        """Parse the event detail page for additional information."""
        item = EventCategoryItem()
        
        # Get basic info passed from the listing page
        item["event_name"] = response.meta.get("event_name", "")
        item["date"] = response.meta.get("date", "")
        start_date, end_date = parse_swedish_date(item["date"])
        item["date_iso"] = start_date
        item["end_date_iso"] = end_date
        item["time"] = response.meta.get("time", "")
        item["location"] = response.meta.get("location", "")
        item["target_group"] = response.meta.get("target_group", "")
        
        # Normalize target group - use "preschool_groups" for events from /forskolor
        source_url = response.meta.get("source_url", "")
        if "/forskolor" in source_url:
            item["target_group_normalized"] = "preschool_groups"
        else:
            item["target_group_normalized"] = normalize_target_group(item["target_group"])
        
        # Detect cancelled events (INSTÄLLT means cancelled in Swedish)
        event_name = item["event_name"].lower()
        if "inställt" in event_name or "inställd" in event_name or "ställs in" in event_name:
            item["status"] = "cancelled"
        else:
            item["status"] = "scheduled"
        
        item["event_url"] = response.meta.get("event_url", "")
        
        # Extract description - usually in paragraphs within the main content
        # Look for the main description text (paragraphs after the event header info)
        description_parts = response.xpath("//article//p[not(b) and not(span[@aria-hidden])]/text()").getall()
        # Filter out empty strings and join
        description = " ".join([p.strip() for p in description_parts if p.strip()])
        item["description"] = description
        
        # Apply date filter if enabled
        if self.DATE_FILTER_DAYS > 0 and item["date_iso"]:
            try:
                event_date = datetime.strptime(item["date_iso"], "%Y-%m-%d")
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                max_date = today + timedelta(days=self.DATE_FILTER_DAYS)
                
                if event_date < today or event_date > max_date:
                    self.logger.info(f"Skipping event outside date range: {item['event_name']} ({item['date_iso']})")
                    return  # Skip this event
            except ValueError:
                pass  # If date parsing fails, include the event anyway
        
        self.logger.info(f"Scraped details for: {item['event_name']}")
        
        yield item
