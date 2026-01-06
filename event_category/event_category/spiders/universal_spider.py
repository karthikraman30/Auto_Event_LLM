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
    if not title:
        return "Skansen"
    for pattern in [r'\bat\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', r'\bin\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', r'\bi\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)', r'\bpå\s+([A-ZÅÄÖ][\w\s]+?)(?:\s*[-–]|$)']:
        match = re.search(pattern, title, re.IGNORECASE)
        if match and len(match.group(1).strip()) > 2:
            return match.group(1).strip()
    if any(kw in title.lower() for kw in ['farmstead', 'church', 'kyrka', 'gård', 'torg', 'stage', 'hall', 'house', 'hus']):
        return title
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
    
    # Check for family-related keywords
    if any(kw in text for kw in ['familj', 'familjevisning', 'lovprogram', 'jullov', 'sommarlov']):
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
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
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

    def start_requests(self):
        self.db = DatabaseManager()
        single_url = getattr(self, 'url', None)
        urls = [single_url] if single_url else self.start_urls
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
    # 🔥 Wait until events are rendered (CRITICAL)
        await page.wait_for_selector(
            "ul.calendarList__list li.calendarItem",
            timeout=20000
        )

        selectors = self.db.get_selectors(response.url)
        if not selectors:
            self.logger.warning("No selectors for Skansen")
            await page.close()
            return

        # Iterate day-by-day (Skansen is a true calendar)
        for _ in range(30):
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

            # 2️⃣ Extract events FOR THIS DAY ONLY
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
                item['location'] = extract_location_from_title(name)

                tg = item_data.get('target_group')
                item['target_group'] = tg or 'All'
                item['target_group_normalized'] = self.simple_normalize(tg)

                item['status'] = detect_cancelled_status(name, item['description'])
                item['booking_info'] = 'N/A'

                yield item

            # 3️⃣ Move to next day
            try:
                next_btn = page.locator("button.link:has-text('Next day')")
                if await next_btn.count() > 0 and await next_btn.is_visible():
                    await next_btn.click()
                    await page.wait_for_timeout(500)

                    await page.wait_for_selector(
                        "ul.calendarList__list li.calendarItem",
                        timeout=20000
                    )
                else:
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

            # 2️⃣ Filter next 30 days
            today = datetime.now().date()
            limit = today + timedelta(days=30)

            relevant_dates = []
            for d in calendar_dates:
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d").date()
                    if today <= dt <= limit:
                        relevant_dates.append(d)
                except:
                    continue

            relevant_dates.sort()
            self.logger.info(f"Tekniska: {len(relevant_dates)} dates in next 30 days")

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


                    
                    # TARGET GROUP
                    target_parts = []

                    # Age-based target group (best)
                    try:
                        age_el = card.locator(sel.get('target_group_age'))
                        if await age_el.count() > 0:
                            age_txt = (await age_el.first.inner_text()).strip()
                            if age_txt:
                                target_parts.append(age_txt)
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
                    
                    # Filter to only include events within next 1 month
                    try:
                        event_date = datetime.strptime(date_to_use, "%Y-%m-%d").date()
                        today = datetime.now().date()
                        one_month_later = today + timedelta(days=30)
                        
                        # Check if event starts within next month OR if it has an end date that overlaps
                        event_in_range = False
                        
                        if today <= event_date <= one_month_later:
                            # Event starts within next month
                            event_in_range = True
                        elif end_date and end_date != 'N/A':
                            # Check if event is ongoing during next month
                            try:
                                end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
                                if event_date <= one_month_later and end_dt >= today:
                                    # Event overlaps with next month period
                                    event_in_range = True
                                    self.logger.info(f"Event {name} overlaps with next month: {event_date} to {end_date}")
                            except ValueError:
                                pass
                        
                        if not event_in_range:
                            self.logger.info(f"Skipping event {name} on {date_to_use} - outside next 1 month range")
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
                    
                    location = "Tekniska museet"
                    try:
                        loc_el = card.locator(sel.get('location'))
                        if await loc_el.count() > 0:
                            txt = (await loc_el.first.inner_text()).strip()
                            if txt:
                                location = txt
                    except:
                        pass

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
        limit = today + timedelta(days=45)
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
                item['location'] = (event.css(sel_items.get('location', '.calendar__item-share li a::text')).get() or 'Moderna Museet').strip()
                item['date_iso'] = date_str
                item['date'] = date_str
                item['end_date_iso'] = 'N/A'
                
                # Extract target group from event name and description
                target_group, target_group_normalized = extract_target_group_from_name(title.strip(), item['description'])
                item['target_group'] = target_group
                item['target_group_normalized'] = target_group_normalized
                
                item['status'] = detect_cancelled_status(title, item['description'])
                item['booking_info'] = 'N/A'
                yield item
        await page.close()

    async def handle_armemuseum(self, page, response):
        cards = await page.evaluate("""() => {const c = []; const links = Array.from(document.querySelectorAll('a[href*="/event/"]')); links.forEach(l => {const n = l.querySelector('span.font-mulish.font-black'); const d = l.querySelector('span.text-xs.leading-7.font-roboto') || l.querySelector('span.font-roboto'); if (n && d && l.href.includes('/event/')) {c.push({name: n.innerText.trim(), dateRange: d.innerText.trim(), url: l.href})}}); const seen = new Set(); return c.filter(x => {if (seen.has(x.url)) return false; seen.add(x.url); return true})}""")
        await page.close()
        for card in cards:
            name = card.get('name', 'Unknown')
            dr = card.get('dateRange', '')
            url = card.get('url', '')
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
            yield scrapy.Request(url, callback=self.parse_arme_detail, dont_filter=True, meta={'playwright': True, 'playwright_include_page': True, 'playwright_page_methods': [PageMethod("wait_for_load_state", "domcontentloaded")], 'event_name': name, 'date_iso': date_iso, 'end_date_iso': end_iso})

    async def parse_arme_detail(self, response):
        import re
        page = response.meta.get("playwright_page")
        name = response.meta.get('event_name')
        date_iso = response.meta.get('date_iso')
        end_iso = response.meta.get('end_date_iso')
        if not name or not date_iso:
            if page:
                await page.close()
            return
        desc = 'N/A'
        time_info = 'N/A'
        if page:
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
            
            # NEW: Check for recurring events (Fler datum section)
            try:
                fler_datum_list = page.locator('ul.richtext')
                fler_datum_count = await fler_datum_list.count()
                
                if fler_datum_count > 0:
                    self.logger.info(f"Found 'Fler datum' section for {name}, extracting specific occurrence dates")
                    occurrences = []
                    
                    # Get additional dates from list
                    list_items = fler_datum_list.locator('li.list-none')
                    count = await list_items.count()
                    
                    for i in range(count):
                        try:
                            li = list_items.nth(i)
                            date_span = await li.locator('span.text-4xl').inner_text()
                            time_span = await li.locator('span.text-xl').inner_text()
                            
                            # Parse Swedish date format
                            parsed_date = parse_swedish_date(date_span.strip())
                            parsed_time = time_span.strip() if time_span else time_info
                            
                            if parsed_date:
                                occurrences.append((parsed_date, parsed_time))
                        except Exception as e:
                            self.logger.warning(f"Could not parse occurrence {i}: {e}")
                            continue
                    
                    # Yield separate events for each occurrence
                    if occurrences:
                        self.logger.info(f"Yielding {len(occurrences)} occurrence(s) for {name}")
                        for occurrence_date, occurrence_time in occurrences:
                            item = EventCategoryItem()
                            item['event_name'] = name
                            item['event_url'] = response.url
                            item['date_iso'] = occurrence_date
                            item['date'] = occurrence_date
                            item['end_date_iso'] = 'N/A'  # Single occurrence
                            item['time'] = occurrence_time if occurrence_time and occurrence_time != 'N/A' else time_info
                            item['location'] = "Armémuseum"
                            item['description'] = desc
                            item['target_group'] = "All"
                            item['target_group_normalized'] = 'all_ages'
                            item['status'] = detect_cancelled_status(name, desc)
                            item['booking_info'] = 'N/A'
                            yield item
                        
                        await page.close()
                        return  # Don't yield the original range-based item
                        
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
        item['target_group'] = "All"
        item['target_group_normalized'] = 'all_ages'
        item['status'] = detect_cancelled_status(name, desc)
        item['booking_info'] = 'N/A'
        yield item

    async def handle_generic(self, page, response):
        # Scroll and click "load more" buttons to get all events
        # Increased iterations for Stockholm library to load full month of events
        max_iterations = 25 if "biblioteket.stockholm.se" in response.url else 20
        
        for _ in range(4):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
        
        self.logger.info(f"Starting load more loop (max {max_iterations} iterations)")
        for i in range(max_iterations):
            clicked = False
            for word in ["Visa fler", "Ladda fler", "Load more", "Visa mer"]:
                try:
                    btn = page.locator(f"button:has-text('{word}'), a:has-text('{word}')").first
                    if await btn.count() > 0 and await btn.is_visible():
                        self.logger.info(f"Clicking '{word}' button (iteration {i+1})")
                        await btn.click(force=True, timeout=3000)
                        await page.wait_for_timeout(1500)
                        clicked = True
                        break
                except Exception as e:
                    self.logger.debug(f"Error clicking '{word}' button: {e}")
                    continue
            if not clicked:
                self.logger.info(f"No more load buttons found, stopping after {i+1} iterations")
                break
        
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
        limit = today + timedelta(days=30)
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
        limit = today + timedelta(days=30)
        
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
            # If end date has explicit year and start date month > end date month,
            # the start date is likely in the previous year
            if start_date and end_date and len(parts) > 1:
                start_year = int(start_date.split('-')[0])
                start_month = int(start_date.split('-')[1])
                end_year = int(end_date.split('-')[0])
                end_month = int(end_date.split('-')[1])
                
                # Check if end date has explicit year in the raw string
                has_explicit_year = re.search(r'\b(20\d{2})\b', parts[1])
                
                # If end has explicit year and start month > end month, adjust start year
                if has_explicit_year and start_month > end_month and start_year == end_year:
                    start_date = f"{end_year - 1}-{start_month:02d}-{start_date.split('-')[2]}"

            if not start_date:
                continue

            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else start_dt

            # ✅ Correct overlap logic
            if end_dt < today:
                continue  # Event already finished

            if start_dt > limit:
                continue  # Event starts too far in future
            
            # Extract target audience from event detail page using regular HTTP (fast!)
            event_url = response.urljoin(data.get('event_url', ''))
            yield scrapy.Request(event_url, callback=self.parse_stockholm_library_detail, dont_filter=True, meta={
                'data': data, 
                'start_date': start_date,
                'end_date': end_date,
                'source_url': response.url
            })

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

        yield item

    async def parse_detail(self, response):
        page = response.meta.get("playwright_page")
        item = response.meta.get('item')
        if not item:
            if page:
                await page.close()
            return
        desc = 'N/A'
        if page:
            for sel in ['.description', '.event-description', '[class*="description"]', 'p', '.content']:
                els = page.locator(sel)
                if await els.count() > 0:
                    texts = await els.all_inner_texts()
                    valid = [t.strip() for t in texts if len(t.strip()) > 20]
                    if valid:
                        desc = max(valid, key=len)[:500]
                        break
            await page.close()
        item['description'] = desc
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
                return 'preschool'
            elif max_age <= 11:
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