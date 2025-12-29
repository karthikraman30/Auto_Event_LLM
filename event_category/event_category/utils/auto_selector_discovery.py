import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Any


class AutoSelectorDiscovery:
    """
    Automatic selector discovery system that learns selectors from any new website
    """
    
    def __init__(self, client, logger):
        self.client = client
        self.logger = logger
        self.required_fields = [
            "event_name",
            "date_iso",
            "time",
            "location",
            "description",
            "target_group",
            "status"
        ]
    
    def discover_website_structure(self, html_content: str, sample_url: str) -> Dict:
        """
        Analyze a new website and discover selectors automatically
        
        Args:
            html_content: Raw HTML from the events listing page
            sample_url: URL of the page being analyzed
            
        Returns:
            Dictionary containing selectors and confidence scores
        """
        self.logger.info(f"Starting automatic selector discovery for: {sample_url}")
        
        prompt = self._build_discovery_prompt(html_content, sample_url)
        result = self._execute_ai_call(prompt)
        
        if result and 'selectors' in result:
            # Validate discovered selectors
            validated = self._validate_selectors(result['selectors'], html_content)
            result['selectors'] = validated
            result['validation_passed'] = validated.get('confidence', {}).get('overall', 0) > 0.7
            
        return result
    
    def extract_events_with_selectors(
        self, 
        html_content: str, 
        selectors: Dict,
        detailed_extraction: bool = False
    ) -> List[Dict]:
        """
        Extract events using discovered selectors
        
        Args:
            html_content: Raw HTML content
            selectors: Previously discovered selector configuration
            detailed_extraction: Whether to extract detailed descriptions
            
        Returns:
            List of extracted events
        """
        prompt = self._build_extraction_prompt(
            html_content, 
            selectors,
            detailed_extraction
        )
        result = self._execute_ai_call(prompt)
        
        events = result.get('events', []) if isinstance(result, dict) else result
        return self._validate_events(events)
    
    def _build_discovery_prompt(self, html_content: str, url: str) -> str:
        """Build comprehensive prompt for automatic selector discovery"""
        
        # Truncate HTML if too long (keep first 15000 chars for context)
        html_sample = html_content[:15000] if len(html_content) > 15000 else html_content
        
        return f"""You are an Expert Web Scraping AI specializing in automatic selector discovery for event websites.

MISSION: Analyze the HTML structure and discover reliable CSS selectors for extracting events.

TARGET URL: {url}

HTML CONTENT:
{html_sample}

REQUIRED FIELDS TO EXTRACT:
1. event_name - Event title/name
2. date_iso - Event date in YYYY-MM-DD format
3. end_date_iso - End date for multi-day events (optional)
4. time - Event time (HH:MM format)
5. location - Venue/address
6. description - Brief description or teaser
7. target_group - Target audience (e.g., "Adults", "Children 3-6 years")
8. status - Event status (scheduled/cancelled)

ANALYSIS PROCESS:

STEP 1 - IDENTIFY EVENT CONTAINERS:
- Scan the HTML for repeating elements that represent individual events
- Look for patterns: <article>, <div class="event">, <li> in event lists, etc.
- The container should wrap ALL information for ONE event
- Analyze 3-5 event instances to confirm the pattern

STEP 2 - IDENTIFY FIELD SELECTORS:
For each required field, find the most reliable selector:

Priority order:
  a) Semantic HTML tags: <time>, <h1>-<h6>, <address>, <article>
  b) Stable class names: .event-title, .event-date, .location
  c) Data attributes: [data-event-name], [data-date]
  d) Structural selectors: first h2, first time element

Avoid:
  - Dynamic IDs: id="event-12345"
  - Generic classes: .col-md-6, .container, .row
  - Deep nesting: div > div > div > span
  - Position-based only: :nth-child(5) without context

STEP 3 - SELECTOR RULES:

Container selector:
  - Must be absolute from document root
  - Should match ALL event instances
  - Example: "article.event-card", "div.event-list > div.event-item"

Field selectors:
  - Must be RELATIVE to container (will be used with container.querySelector())
  - Use simple, stable paths
  - Examples:
    * "h2.title" (find h2 with class title inside container)
    * "time" (find first time tag)
    * ".description p" (find p inside element with class description)
    * "[data-location]" (find element with data-location attribute)

STEP 4 - HANDLE MISSING FIELDS:
  - If a field cannot be reliably extracted, set selector to null
  - Explain why in the "notes" section

STEP 5 - DATE/TIME HANDLING:
  - Identify where dates appear (might be in <time> tags, divs, spans)
  - Note the format used: "5 december 2025", "2025-12-05", "Dec 5"
  - For Swedish sites: look for Swedish month names (januari, februari, etc.)
  - Time might be in separate element or combined with date

STEP 6 - MULTI-ELEMENT FIELDS:
  - If information is split across multiple elements:
    * Example: Date in one <span>, month in another
    * Provide MULTIPLE selectors as array: ["span.day", "span.month"]
  - If date range spans elements: ["time.start-date", "time.end-date"]

OUTPUT FORMAT (JSON only, no markdown):
{{
  "website_analysis": {{
    "url": "{url}",
    "detected_structure": "Description of HTML pattern found",
    "event_count_detected": 5,
    "language_detected": "Swedish/English/etc"
  }},
  
  "selectors": {{
    "container": "article.event-item",
    "container_alternative": "div.event-card",
    
    "items": {{
      "event_name": {{
        "selector": "h2.event-title",
        "alternative": "h3.title",
        "attribute": null,
        "notes": "Primary title heading"
      }},
      
      "date_iso": {{
        "selector": "time.event-date",
        "alternative": ".date-info span",
        "attribute": "datetime",
        "notes": "May need parsing from Swedish format",
        "format_example": "5 december 2025"
      }},
      
      "end_date_iso": {{
        "selector": "time.event-end-date",
        "alternative": null,
        "attribute": "datetime",
        "notes": "Only for multi-day events"
      }},
      
      "time": {{
        "selector": "span.event-time",
        "alternative": "time.event-time",
        "attribute": null,
        "notes": "Time string like '10:00' or '14.30'"
      }},
      
      "location": {{
        "selector": "address.venue",
        "alternative": ".location",
        "attribute": null,
        "notes": "Venue name and address"
      }},
      
      "description": {{
        "selector": "p.event-description",
        "alternative": ".teaser",
        "attribute": null,
        "notes": "Brief description/teaser text"
      }},
      
      "target_group": {{
        "selector": ".audience-tag",
        "alternative": null,
        "attribute": null,
        "notes": "Age group or audience type"
      }},
      
      "status": {{
        "selector": ".status-badge",
        "alternative": null,
        "attribute": null,
        "notes": "Look for 'Inställt' or 'Cancelled'"
      }},
      
      "detail_link": {{
        "selector": "a.event-link",
        "alternative": "h2 a",
        "attribute": "href",
        "notes": "Link to detailed event page"
      }}
    }}
  }},
  
  "extraction_rules": {{
    "date_format": "Swedish text format: 'DD month YYYY'",
    "time_format": "HH:MM or HH.MM",
    "language": "Swedish",
    "status_keywords": ["Inställt", "Fullbokat"],
    "multi_day_indicator": "Date range with dash",
    "special_notes": "Any site-specific quirks"
  }},
  
  "confidence": {{
    "overall": 0.85,
    "container": 0.95,
    "field_scores": {{
      "event_name": 0.95,
      "date_iso": 0.90,
      "time": 0.80,
      "location": 0.85,
      "description": 0.75,
      "target_group": 0.60,
      "status": 0.50
    }}
  }},
  
  "sample_events": [
    {{
      "event_name": "Example extracted event",
      "date_iso": "2025-12-15",
      "time": "10:00",
      "location": "Sample Venue",
      "description": "Example description in original language",
      "target_group": "Adults",
      "status": "scheduled"
    }}
  ]
}}

CRITICAL INSTRUCTIONS:
- Selectors must be TESTED against the HTML provided
- Return confidence scores (0.0 to 1.0) for each field
- Include alternative selectors as fallback options
- If data is in attributes (like datetime in <time>), specify which attribute
- Extract 2-3 sample events to verify selectors work
- Keep all extracted text in ORIGINAL language (especially Swedish)
- Be precise: vague selectors like "div" or "span" alone are NOT acceptable
"""

    def _build_extraction_prompt(
        self, 
        html_content: str, 
        selectors: Dict,
        detailed: bool
    ) -> str:
        """Build prompt for extracting events using known selectors"""
        
        html_sample = html_content[:12000] if len(html_content) > 12000 else html_content
        current_date = datetime.now().strftime('%Y-%m-%d')
        next_year = datetime.now().year + 1
        
        return f"""Extract all events from the HTML using the provided selectors.

CURRENT DATE: {current_date}
YEAR INFERENCE: Dates in Jan-Mar should use year {next_year}

HTML CONTENT:
{html_sample}

SELECTOR CONFIGURATION:
{json.dumps(selectors, indent=2)}

EXTRACTION INSTRUCTIONS:

1. LOCATE CONTAINERS:
   - Use container selector: {selectors.get('container', 'N/A')}
   - Find ALL matching containers in the HTML
   - Each container = one event

2. EXTRACT FIELDS:
   For each container, extract fields using provided selectors:
   - Use primary selector first
   - If null/empty, try alternative selector
   - If attribute specified, extract from that attribute
   - Otherwise extract text content

3. DATE PARSING (SWEDISH SITES):
   Swedish months: januari→01, februari→02, mars→03, april→04, maj→05, juni→06,
                   juli→07, augusti→08, september→09, oktober→10, november→11, december→12
   
   Patterns:
   - "5 december" → {datetime.now().year}-12-05
   - "5-8 december" → date_iso: {datetime.now().year}-12-05, end_date_iso: {datetime.now().year}-12-08
   - "Lördag 14 december kl 10:00" → extract date + time separately
   - Year inference: If month is Jan-Mar and current month is Nov-Dec, use {next_year}

4. TIME PARSING:
   - Extract time in HH:MM format
   - "kl. 10:00", "14.30", "10-12" → extract start time
   - Convert "14.30" to "14:30"

5. LANGUAGE PRESERVATION:
   - Keep ALL text in ORIGINAL language
   - DO NOT translate Swedish to English
   - Example: "Sagostund för barn" stays exactly as is

6. STATUS DETECTION:
   - Look for keywords: "Inställt", "Cancelled", "Avbokad", "Fullbokat"
   - Default: "scheduled"
   - Only set "cancelled" if explicitly indicated

7. DESCRIPTION:
   - Extract teaser/description text
   - Max 250 characters
   - Keep original language
   - If empty → null (not "N/A")

8. TARGET GROUP:
   - Swedish patterns: "barn 3-6 år" → "Children (3-6 years)"
   - "vuxna" → "Adults", "familjer" → "Families"
   - If not found → null

OUTPUT FORMAT (JSON only):
{{
  "events": [
    {{
      "event_name": "Event title in original language",
      "date_iso": "2025-12-05",
      "end_date_iso": null,
      "time": "10:00",
      "location": "Venue name",
      "target_group": "Target audience",
      "description": "Description in original language",
      "status": "scheduled",
      "detail_link": "https://example.com/event/123"
    }}
  ],
  "extraction_stats": {{
    "total_found": 5,
    "successfully_parsed": 5,
    "missing_fields": ["end_date_iso", "target_group"]
  }}
}}

CRITICAL:
- Extract ALL events found in HTML
- Skip events with missing event_name AND date_iso
- Preserve original language
- Use provided selectors exactly as specified
"""

    def _validate_selectors(self, selectors: Dict, html_content: str) -> Dict:
        """
        Validate selector quality and add confidence scoring
        """
        validated = selectors.copy()
        
        # Check if container exists
        container = selectors.get('container', '')
        if not container or len(container) < 3:
            self.logger.warning("Invalid container selector")
            if 'confidence' not in validated:
                validated['confidence'] = {}
            validated['confidence']['overall'] = 0.0
            return validated
        
        # Score based on selector quality
        items = selectors.get('items', {})
        total_score = 0
        field_count = 0
        
        for field_name in self.required_fields:
            if field_name in items:
                field_config = items[field_name]
                selector = field_config.get('selector') if isinstance(field_config, dict) else field_config
                
                if selector and selector != "null" and selector is not None:
                    # Simple quality scoring
                    score = 0.5  # base score
                    if any(tag in str(selector) for tag in ['time', 'h1', 'h2', 'h3', 'address']):
                        score += 0.3  # semantic bonus
                    if '.' in str(selector) or '[' in str(selector):
                        score += 0.2  # has class/attribute
                    
                    total_score += min(score, 1.0)
                    field_count += 1
        
        overall_confidence = total_score / len(self.required_fields) if field_count > 0 else 0.0
        
        if 'confidence' not in validated:
            validated['confidence'] = {}
        validated['confidence']['overall'] = round(overall_confidence, 2)
        
        return validated
    
    def _validate_events(self, events: List[Dict]) -> List[Dict]:
        """Validate and clean extracted events"""
        valid_events = []
        
        for event in events:
            # Must have event name and date
            if not event.get('event_name') or not event.get('date_iso'):
                self.logger.warning(f"Skipping invalid event: {event.get('event_name', 'NO NAME')}")
                continue
            
            # Validate date format
            try:
                datetime.strptime(event['date_iso'], '%Y-%m-%d')
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid date for event: {event['event_name']}")
                continue
            
            # Clean up null values
            cleaned_event = {}
            for key, value in event.items():
                if value not in [None, "null", "N/A", ""]:
                    cleaned_event[key] = value
                else:
                    cleaned_event[key] = None
            
            valid_events.append(cleaned_event)
        
        return valid_events
    
    def _execute_ai_call(self, prompt: str) -> Dict:
        """Execute AI call with error handling"""
        try:
            self.logger.info(f"Executing AI call (prompt length: {len(prompt)} chars)")
            
            response = self.client.models.generate_content(
                model="gemini-2.5-pro",
                contents=prompt,
                config={
                    "response_mime_type": "application/json",
                    "temperature": 0.1,
                }
            )
            
            response_text = response.text.strip()
            
            # Clean markdown
            if response_text.startswith("```"):
                response_text = re.sub(r'^```(?:json)?\n?', '', response_text)
                response_text = re.sub(r'\n?```$', '', response_text)
            
            # Parse JSON
            try:
                result = json.loads(response_text)
                return result
            except json.JSONDecodeError as e:
                self.logger.warning(f"JSON parse error: {e}, attempting repair")
                return self._repair_json(response_text)
                
        except Exception as e:
            self.logger.error(f"AI execution error: {e}", exc_info=True)
            return {}
    
    def _repair_json(self, text: str) -> Dict:
        """Attempt to repair malformed JSON"""
        try:
            fixed = text.rstrip()
            if fixed.endswith(','):
                fixed = fixed[:-1]
            
            open_braces = fixed.count('{') - fixed.count('}')
            open_brackets = fixed.count('[') - fixed.count(']')
            
            fixed += '}' * max(0, open_braces)
            fixed += ']' * max(0, open_brackets)
            
            return json.loads(fixed)
        except:
            self.logger.error("JSON repair failed")
            return {}


# USAGE EXAMPLE
class EventScraperOrchestrator:
    """
    Main orchestrator for automatic event scraping from any website
    """
    
    def __init__(self, ai_client, logger, selector_cache=None):
        self.discovery = AutoSelectorDiscovery(ai_client, logger)
        self.logger = logger
        self.selector_cache = selector_cache or {}  # Store selectors per domain
    
    def scrape_new_website(self, url: str, html_content: str) -> List[Dict]:
        """
        Automatically discover selectors and extract events from a new website
        """
        domain = self._extract_domain(url)
        
        # Check if we have cached selectors for this domain
        if domain in self.selector_cache:
            self.logger.info(f"Using cached selectors for {domain}")
            selectors = self.selector_cache[domain]
        else:
            self.logger.info(f"Discovering selectors for new website: {domain}")
            
            # STEP 1: Discover selectors
            discovery_result = self.discovery.discover_website_structure(html_content, url)
            
            if not discovery_result or not discovery_result.get('selectors'):
                self.logger.error("Failed to discover selectors")
                return []
            
            selectors = discovery_result['selectors']
            confidence = discovery_result.get('confidence', {}).get('overall', 0)
            
            self.logger.info(f"Selector discovery confidence: {confidence}")
            
            # Cache selectors if confidence is high
            if confidence > 0.7:
                self.selector_cache[domain] = selectors
                self.logger.info(f"Cached selectors for {domain}")
        
        # STEP 2: Extract events using discovered selectors
        events = self.discovery.extract_events_with_selectors(html_content, selectors)
        
        self.logger.info(f"Extracted {len(events)} events from {domain}")
        
        return events
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        from urllib.parse import urlparse
        return urlparse(url).netloc
    
    def save_selectors(self, filepath: str):
        """Save discovered selectors to file"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.selector_cache, f, indent=2, ensure_ascii=False)
    
    def load_selectors(self, filepath: str):
        """Load previously discovered selectors"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.selector_cache = json.load(f)
            self.logger.info(f"Loaded selectors for {len(self.selector_cache)} domains")
        except FileNotFoundError:
            self.logger.warning("No selector cache file found")
    
    def get_selector_quality_report(self) -> str:
        """Generate human-readable report of discovered selectors"""
        report = []
        for domain, selectors in self.selector_cache.items():
            confidence = selectors.get('confidence', {}).get('overall', 0)
            report.append(f"{domain}: {confidence:.1%} confidence")
        return "\n".join(report) if report else "No selectors cached yet"
