import scrapy
import html2text
import json
import os
import re
from datetime import datetime
from groq import Groq
from scrapy_playwright.page import PageMethod
from event_category.items import EventCategoryItem

# --- 1. CONFIGURATION: Target Group Mapping ---
# This maps keywords (English & Swedish) to your standard Excel categories.
TARGET_GROUP_MAPPING = {
    # Children (0-12)
    'barn': 'children', 'småbarn': 'children', 'förskolebarn': 'children',
    'children': 'children', 'kids': 'children', 'bebis': 'children',
    # Teens (13-19)
    'ungdom': 'teens', 'tonåring': 'teens', 'unga': 'teens',
    'teens': 'teens', 'teenagers': 'teens',
    # Adults (20+)
    'vuxen': 'adults', 'vuxna': 'adults', 'senior': 'adults',
    'adults': 'adults',
    # Families
    'familj': 'families', 'family': 'families',
    # All ages
    'alla': 'all_ages', 'all ages': 'all_ages', 'everyone': 'all_ages',
    'all_ages': 'all_ages'
}

def normalize_target_group(target_group_str):
    """Normalize target group string to standard categories (e.g., 'Barn' -> 'children')."""
    if not target_group_str:
        return "all_ages"  # Default if unspecified
    
    target_lower = target_group_str.lower().strip()
    found_categories = set()
    
    # Check for keyword matches
    for term, category in TARGET_GROUP_MAPPING.items():
        if term in target_lower:
            found_categories.add(category)
            
    # Heuristic for age ranges (e.g. "5-10 år" or "5-10 years")
    age_match = re.search(r'(\d+)\s*[-–]\s*(\d+)', target_lower)
    if age_match:
        min_age, max_age = int(age_match.group(1)), int(age_match.group(2))
        if max_age <= 12: found_categories.add('children')
        elif min_age >= 13 and max_age <= 19: found_categories.add('teens')
        elif min_age >= 18: found_categories.add('adults')

    if not found_categories:
        return target_group_str # Return original if no match found
        
    return ', '.join(sorted(found_categories))

class UniversalSpider(scrapy.Spider):
    name = "universal_events"

    def configure_groq(self):
        api_key = self.settings.get("GROQ_API_KEY") or os.getenv("GROQ_API_KEY")
        if not api_key:
            self.logger.error("GROQ_API_KEY is missing! Set it in .env file.")
            return None
        return Groq(api_key=api_key) 

    def start_requests(self):
        self.client = self.configure_groq()
        
        # 2. DYNAMIC URL HANDLING
        # Get the URL passed via command line: -a url="https://..."
        target_url = getattr(self, 'url', None)
        
        if not target_url:
            self.logger.error("No URL provided! Usage: scrapy crawl universal_events -a url='https://example.com'")
            return

        yield scrapy.Request(
            target_url,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "networkidle"),
                    PageMethod("wait_for_timeout", 3000), # Wait for JS rendering
                ],
            },
            callback=self.parse
        )

    async def parse(self, response):
        page = response.meta.get("playwright_page")
        if page:
            self.logger.info(f"Scraping: {response.url}")
            
            # === STEP A: SCROLL & CLICK (Universal Logic) ===
            # 1. Scroll to bottom to trigger lazy loading
            for _ in range(4): 
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)

            # 2. Try to click "Load More" buttons in various languages
            load_words = ["Visa fler", "Ladda fler", "Hämta fler", "Load more", "Show more", "More events"]
            for _ in range(3): # Max 3 clicks to prevent infinite loops
                clicked = False
                for word in load_words:
                    # Look for button or link with the text
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

            content = await page.content()
            await page.close()
        else:
            content = response.text

        # === STEP B: PREPARE CONTENT ===
        if not self.client: return

        h = html2text.HTML2Text()
        h.ignore_links = True; h.ignore_images = True; h.body_width = 0
        text_content = h.handle(content)

        # === STEP C: CALL GROQ ===
        # Groq free tier has 12k token limit, ~25k chars
        events = self.call_groq(text_content[:25000])
        
        # === STEP D: PROCESS & FILTER (Weekly View Logic) ===
        today = datetime.now().date()
        
        if events:
            self.logger.info(f"Groq extracted {len(events)} events.")
            
            for event_data in events:
                # 1. Weekly View Filter: Skip past events
                date_str = event_data.get('date_iso')
                self.logger.debug(f"Processing event: {event_data.get('event_name')} - date: {date_str}")
                if date_str:
                    try:
                        event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        if event_date < today:
                            self.logger.debug(f"Skipping past event: {date_str} < {today}")
                            continue  # SKIP past events
                    except ValueError as e:
                        self.logger.debug(f"Date parse error for '{date_str}': {e}") 

                # 2. Normalize Target Group
                raw_target = event_data.get('target_group', '')
                normalized_target = normalize_target_group(raw_target)

                # 3. Create Item
                item = EventCategoryItem()
                item['event_name'] = event_data.get('event_name')
                item['date_iso'] = date_str
                item['date'] = date_str  # Use ISO date for display as well
                item['time'] = event_data.get('time')
                item['location'] = event_data.get('location')
                item['target_group'] = raw_target
                item['description'] = event_data.get('description')
                item['event_url'] = response.url 
                
                # IMPORTANT: Set normalized target group for the Excel Column H
                try:
                    item['target_group_normalized'] = normalized_target
                except KeyError:
                    # Fallback if field missing in items.py
                    item['extra_attributes'] = {'target_group_normalized': normalized_target}
                
                # Add status - use proper field, not extra_attributes
                item['status'] = 'scheduled'
                
                yield item

    def call_groq(self, text_content):
        prompt = f"""Extract a list of events from the text below.

Input Text:
{text_content}

Instructions:
1. Return ONLY a raw JSON List of objects. No Markdown formatting, no code blocks.
2. TODAY'S DATE IS: {datetime.now().strftime('%Y-%m-%d')}. Use this to determine the correct year for events.
3. Extract only UPCOMING events. Skip any events that have already passed.
4. Extract "Target Group" (Who is this for? e.g., "Barn 5 år", "Adults").

CRITICAL - DATE HEADERS:
5. Many event websites use DATE HEADERS followed by events that inherit that date. Examples:
   - "23 DEC TISDAG" or "DECEMBER 23" followed by events with only times like "10:30", "11:00"
   - "Fredag 27 december" followed by multiple events
   - Events listed under a date header INHERIT that date, even if the event itself only shows a time.
6. Pay close attention to section headers, date dividers, or calendar-style layouts.
7. If you see a pattern like:
     "23 DEC TISDAG"
       Event A (10:30)
       Event B (11:00)
     "26 DEC"
       Event C (14:00)
   Then Event A and B have date 2025-12-23, and Event C has date 2025-12-26.

IMPORTANT DATE PARSING RULES:
8. When you see "23 DEC TISDAG", this means December 23, 2025 (2025-12-23)
9. When you see "26 DEC", this means December 26, 2025 (2025-12-26)
10. The format is "DAY MONTH" where DAY is the date number and MONTH is the 3-letter month abbreviation
11. ALL events under a date header inherit that exact date
12. Do NOT invent different dates - use the exact date shown in the header
13. Current year is 2025, so "23 DEC" = "2025-12-23"

JSON Schema:
[
  {{
    "event_name": "string",
    "date_iso": "YYYY-MM-DD",
    "time": "HH:MM",
    "location": "string",
    "target_group": "string",
    "description": "short summary"
  }}
]
"""
        try:
            response = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            # Handle if Groq wraps it in a root object
            if isinstance(result, dict):
                for key, value in result.items():
                    if isinstance(value, list):
                        return value
                return [result]
            return result
        except Exception as e:
            self.logger.error(f"Groq extraction failed: {e}")
            return []