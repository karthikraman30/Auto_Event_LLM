import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Any


def parse_combined_datetime(combined_str: str) -> tuple[str, str]:
    """
    Parse a combined datetime string into separate date and time components.
    
    Args:
        combined_str: String containing date and time (e.g., "2025-12-30 14:00", "30 december 14:00")
    
    Returns:
        tuple: (date_iso, time) where date_iso is "YYYY-MM-DD" and time is "HH:MM"
    """
    if not combined_str or combined_str in ["null", "N/A", ""]:
        return None, None
    
    combined_str = combined_str.strip()
    
    # Common patterns to match
    patterns = [
        # ISO format: "2025-12-30 14:00"
        r'(\d{4}-\d{2}-\d{2})\s+(\d{1,2}:\d{2})',
        # Swedish format: "30 december 14:00"
        r'(\d{1,2})\s+(januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december)\s+(\d{1,2}:\d{2})',
        # With "kl": "30 december kl 14:00"
        r'(\d{1,2})\s+(januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december)\s+kl\s+(\d{1,2}:\d{2})',
        # Time only: "14:00" or "14.30"
        r'(\d{1,2}:\d{2})',
        r'(\d{1,2}\.\d{2})',
    ]
    
    current_year = datetime.now().year
    month_map = {
        'januari': '01', 'februari': '02', 'mars': '03', 'april': '04',
        'maj': '05', 'juni': '06', 'juli': '07', 'augusti': '08',
        'september': '09', 'oktober': '10', 'november': '11', 'december': '12'
    }
    
    for pattern in patterns:
        match = re.search(pattern, combined_str, re.IGNORECASE)
        if match:
            groups = match.groups()
            
            # Handle ISO format: "2025-12-30 14:00"
            if len(groups) == 2 and re.match(r'\d{4}-\d{2}-\d{2}', groups[0]):
                date_iso = groups[0]
                time_str = groups[1]
                return date_iso, time_str
            
            # Handle Swedish format: "30 december 14:00"
            elif len(groups) == 3 and groups[1].lower() in month_map:
                day = groups[0].zfill(2)
                month = month_map[groups[1].lower()]
                time_str = groups[2]
                date_iso = f"{current_year}-{month}-{day}"
                return date_iso, time_str
            
            # Handle time only: "14:00"
            elif len(groups) == 1:
                time_str = groups[0].replace('.', ':')
                return None, time_str
    
    # If no pattern matches, return None
    return None, None


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
        
        # NEW: Try HTML + Text Correlation approach first
        self.logger.info("Attempting HTML + Text Correlation discovery...")
        result = self._discover_with_html_text_correlation(html_content, sample_url)
        
        if result and result.get('confidence', {}).get('overall', 0) > 0.6:
            self.logger.info("✅ HTML + Text Correlation successful with high confidence")
            
            # CRITICAL: Validate selectors actually work against HTML
            selectors = result.get('selectors', {})
            if selectors:
                validation = self._validate_selectors_against_html(selectors, html_content)
                result['selector_validation'] = validation
                
                if not validation.get('valid', False):
                    self.logger.warning(f"Selectors failed validation: {validation.get('issues', [])}")
                else:
                    self.logger.info(f"✅ Selectors validated successfully ({validation.get('adjusted_confidence', 1.0):.0%} fields working)")
            
            return result
        
        # Fallback to original method if correlation didn't work well
        self.logger.info("HTML + Text Correlation confidence too low, falling back to original discovery...")
        prompt = self._build_discovery_prompt(html_content, sample_url)
        result = self._execute_ai_call(prompt)
        
        if result and 'selectors' in result:
            # Validate discovered selectors
            validated = self._validate_selectors(result['selectors'], html_content)
            result['selectors'] = validated
            result['validation_passed'] = validated.get('confidence', {}).get('overall', 0) > 0.7
            
            # CRITICAL: Also test these selectors against actual HTML
            validation = self._validate_selectors_against_html(result['selectors'], html_content)
            result['selector_validation'] = validation
            
            if not validation.get('valid', False):
                self.logger.warning(f"Fallback selectors failed validation: {validation.get('issues', [])}")
            else:
                self.logger.info(f"✅ Fallback selectors validated ({validation.get('adjusted_confidence', 1.0):.0%} fields working)")
            
        return result
    
    def _discover_with_html_text_correlation(self, html_content: str, sample_url: str) -> Dict:
        """
        NEW: Discover selectors using HTML + Rendered Text Correlation
        
        This approach:
        1. Extracts sample event blocks from HTML
        2. Extracts rendered text from each block
        3. Sends BOTH HTML and text to AI
        4. AI maps HTML elements to the actual text fields
        5. Creates bidirectional validation
        
        Args:
            html_content: Raw HTML content
            sample_url: URL being analyzed
            
        Returns:
            Dictionary with selectors and confidence scores
        """
        self.logger.info("Starting HTML + Text Correlation discovery...")
        
        try:
            # Step 1: Extract sample event blocks from HTML
            samples = self._extract_sample_events_with_text(html_content)
            
            if not samples or len(samples) == 0:
                self.logger.warning("Could not extract sample events for correlation")
                return {}
            
            self.logger.info(f"Extracted {len(samples)} sample events for correlation analysis")
            
            # Step 2: Build correlation prompt and send to AI
            prompt = self._build_correlation_discovery_prompt(html_content, samples, sample_url)
            result = self._execute_ai_call(prompt)
            
            if result and 'selectors' in result:
                validated = self._validate_selectors(result['selectors'], html_content)
                result['selectors'] = validated
                confidence = validated.get('confidence', {}).get('overall', 0)
                result['confidence'] = validated.get('confidence', {})
                result['method'] = 'HTML + Text Correlation'
                
                self.logger.info(f"HTML + Text Correlation confidence: {confidence:.0%}")
                return result
            
            return {}
            
        except Exception as e:
            self.logger.warning(f"HTML + Text Correlation error: {e}")
            return {}
    
    def _extract_sample_events_with_text(self, html_content: str) -> List[Dict]:
        """
        Extract sample event blocks with their rendered text representation
        
        Returns:
            List of dicts with 'html' and 'text' keys
        """
        samples = []
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Common event container selectors
            containers = soup.find_all(['article', 'div'], class_=re.compile(r'event|calendar|listing|card|item', re.I))
            
            # Get first 3-5 samples
            for container in containers[:5]:
                if len(samples) >= 5:
                    break
                
                html_block = str(container)[:500]  # Get first 500 chars of HTML
                text_block = container.get_text(separator='\n', strip=True)[:300]  # Get rendered text
                
                if len(text_block) > 20:  # Only if substantial text
                    samples.append({
                        'html': html_block,
                        'text': text_block
                    })
            
            return samples
            
        except Exception as e:
            self.logger.debug(f"Error extracting samples: {e}")
            return []
    
    def _build_correlation_discovery_prompt(self, html_content: str, samples: List[Dict], url: str) -> str:
        """
        Build prompt for HTML + Text Correlation discovery
        
        Sends both HTML structure and rendered text to AI for bidirectional validation
        """
        
        html_sample = html_content[:10000] if len(html_content) > 10000 else html_content
        
        # Format samples with both HTML and text
        samples_text = ""
        for i, sample in enumerate(samples[:3]):
            samples_text += f"""
SAMPLE EVENT {i+1}:
---HTML---
{sample['html']}

---RENDERED TEXT---
{sample['text']}
---

"""
        
        return f"""You are an Expert Web Scraping AI specializing in CSS selector discovery using HTML + Text Correlation.

TARGET URL: {url}

CORRELATION TECHNIQUE:
You will receive:
1. Raw HTML snippets from event containers
2. Rendered text extracted from those same containers
3. Your task is to MAP HTML elements to the actual text fields

This bidirectional approach validates selectors are correct by matching:
HTML structure → Text content correlation

SAMPLE EVENTS WITH HTML AND RENDERED TEXT:
{samples_text}

FULL PAGE HTML (for context):
{html_sample}

TASK:
1. Analyze the correlation between HTML elements and rendered text
2. Identify which HTML elements contain which data fields:
   - Event name/title
   - Date/Date range
   - Time
   - Location/Venue
   - Description/Teaser
   - Target group/Audience
   - Status indicators
3. Discover CSS selectors that match EXACTLY to the rendered text content

CRITICAL RULES:
- A selector is CORRECT if it isolates text that matches the rendered content
- Cross-reference: If text says "Visning av utställningen" in the rendered output, the selector must point to that exact text
- Test selectors against multiple samples to ensure consistency
- If HTML structure is ambiguous, prefer the most specific selector path
- Swedish site: Keep all text in Swedish

OUTPUT FORMAT (JSON only):
{{
  "analysis": {{
    "method": "HTML + Text Correlation",
    "sample_count": 3,
    "correlation_quality": "High/Medium/Low",
    "notes": "Any patterns or quirks observed"
  }},
  
  "selectors": {{
    "container": "Most specific container selector for one event",
    "container_alternative": "Alternative container selector if exists",
    
    "items": {{
      "event_name": {{
        "selector": "Selector that matches event title text",
        "attribute": null,
        "notes": "Should isolate titles like 'Visning av utställningen'"
      }},
      
      "date_iso": {{
        "selector": "Selector for date element",
        "attribute": "datetime",
        "notes": "Extract text like '10 december' or similar"
      }},
      
      "time": {{
        "selector": "Selector for time element",
        "attribute": null,
        "notes": "Should match times like '13:00' or '14.30'"
      }},
      
      "location": {{
        "selector": "Selector for location/venue text",
        "attribute": null,
        "notes": ""
      }},
      
      "description": {{
        "selector": "Selector for description/teaser text",
        "attribute": null,
        "notes": ""
      }},
      
      "target_group": {{
        "selector": "Selector for audience/target group",
        "attribute": null,
        "notes": ""
      }},
      
      "status": {{
        "selector": "Selector for cancelled status indicators",
        "attribute": null,
        "notes": "Look for 'Inställt', 'Cancelled' keywords"
      }}
    }}
  }},
  
  "confidence": {{
    "overall": 0.85,
    "correlation_match": 0.90,
    "selector_precision": 0.85,
    "method": "HTML + Text Correlation"
  }},
  
  "sample_validations": [
    {{
      "sample": 1,
      "validation": "✅ All text fields found and matched to selectors correctly",
      "sample_text_excerpt": "Visning av utställningen Skönhet och sanning..."
    }}
  ]
}}

CRITICAL: Your selectors MUST correlate with the RENDERED TEXT shown in the samples.
If you see text "Visning av utställningen" in rendered output, your selector must successfully extract that exact text.
"""
    
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
        """
        Build the Ultimate AI prompt for automatic selector discovery and 
        data extraction for Gemini 2.5 Pro.
        """
        
        # Truncate HTML to stay within safe token limits while providing enough context
        html_sample = html_content[:20000] if len(html_content) > 20000 else html_content
        current_date = datetime.now().strftime('%Y-%m-%d')
        current_year = datetime.now().year
        next_year = current_year + 1
        
        return f"""
### ROLE
You are an Expert Web Scraping & Reasoning AI specializing in Swedish cultural and event websites. Your task is to analyze the HTML of a new website, discover the repeating CSS structure for events, and extract sample data.

### CONTEXT
- Target URL: {url}
- Current Date: {current_date}
- Year Inference: If a date is found in January–March and the current month is late in the year, assume the year is {next_year}.

### PHASE 1: REASONING (THINKING PROCESS)
Before providing selectors, perform a deep analysis:
1. Identify the repeating HTML element (container) that wraps each individual event card or list item.
2. Locate the event title and determine if it is in an <h2>, <h3>, or a specific class.
3. Find the date and time. Identify if they are in <time> tags, data attributes, or plain text.
4. Check for Swedish keywords:
   - Months: januari, februari, mars, april, maj, juni, juli, augusti, september, oktober, november, december.
   - Status: "Inställt" (Cancelled), "Fullbokat" (Fully Booked), "Boka" (Booking).
   - Target Groups: "barn" (children), "vuxna" (adults), "familj" (family), "år" (years).

### PHASE 2: SELECTOR DISCOVERY RULES
- **Container Selector**: Must be an absolute selector matching ALL event instances (e.g., "article.event-card").
- **Item Selectors**: Must be RELATIVE to the container (e.g., "h2.title", not "article.event-card h2.title").
- **Stability**: Prioritize semantic tags (time, address, h2) and data-attributes. AVOID dynamic IDs (id="event-1234") or layout-only classes (.col-md-6).

### REFERENCE EXAMPLE (FEW-SHOT)
Raw HTML Fragment: 
<div class="events-list"><article class="item"><h3>Jazz i Parken</h3><time datetime="2025-06-15">15 juni</time><span class="loc">Skansen</span></article></div>

Desired Discovery:
{{
  "selectors": {{
    "container": "div.events-list > article.item",
    "items": {{
      "event_name": "h3",
      "date_iso": "time",
      "location": "span.loc"
    }}
  }}
}}

### PHASE 3: DATA EXTRACTION RULES
- **Language**: Preserve all original Swedish text for names, locations, and descriptions.
- **Normalization**: 
  - Convert Swedish dates to ISO (YYYY-MM-DD).
  - Convert times (e.g., "14.30") to HH:MM (14:30).
  - Status: Set to 'cancelled' only if explicit keywords (Inställt/Fullbokat) appear.

### INPUT HTML
{html_sample}

### REQUIRED OUTPUT (JSON ONLY)
{{
  "thinking_process": "Explain your logic for choosing the container and field selectors here.",
  "selectors": {{
    "container": "css_selector_here",
    "items": {{
      "event_name": "selector",
      "date_iso": "selector",
      "time": "selector",
      "location": "selector",
      "description": "selector",
      "target_group": "selector",
      "status": "selector",
      "booking_info": "selector",
      "event_url": "selector"
    }}
  }},
  "sample_events": [
    {{
      "event_name": "...",
      "date_iso": "YYYY-MM-DD",
      "time": "HH:MM",
      "location": "...",
      "description": "...",
      "target_group": "...",
      "status": "scheduled/cancelled",
      "booking_info": "...",
      "event_url": "..."
    }}
  ],
  "confidence": {{
    "overall": 0.0 to 1.0,
    "field_scores": {{ "event_name": 0.9, "date_iso": 0.8 ... }}
  }}
}}
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

3. DETECT COMBINED DATETIME SELECTORS:
   - Check if date_iso and time selectors are identical
   - If identical → Combined datetime scenario (extract once, split later)
   - If different → Separate selectors scenario (extract separately)

4. DATE PARSING (SWEDISH SITES):
   Swedish months: januari→01, februari→02, mars→03, april→04, maj→05, juni→06,
                   juli→07, augusti→08, september→09, oktober→10, november→11, december→12
   
   Patterns:
   - "5 december" → {datetime.now().year}-12-05
   - "5-8 december" → date_iso: {datetime.now().year}-12-05, end_date_iso: {datetime.now().year}-12-08
   - "Lördag 14 december kl 10:00" → extract date + time separately
   - Year inference: If month is Jan-Mar and current month is Nov-Dec, use {next_year}

5. TIME PARSING:
   - Extract time in HH:MM format
   - "kl. 10:00", "14.30", "10-12" → extract start time
   - Convert "14.30" to "14:30"
   - If combined datetime selector: extract full string, will be parsed later

6. LANGUAGE PRESERVATION:
   - Keep ALL text in ORIGINAL language
   - DO NOT translate Swedish to English
   - Example: "Sagostund för barn" stays exactly as is

7. STATUS DETECTION:
   - Look for keywords: "Inställt", "Cancelled", "Avbokad", "Fullbokat"
   - Default: "scheduled"
   - Only set "cancelled" if explicitly indicated

8. DESCRIPTION:
   - Extract teaser/description text
   - Max 250 characters
   - Keep original language
   - If empty → null (not "N/A")

9. TARGET GROUP (CRITICAL - Extract from multiple sources):
   - Swedish patterns: "barn 3-6 år" → "Children (3-6 years)"
   - "vuxna" → "Adults", "familjer" → "Families"
   - Event type analysis:
     * "Guidad visning" → "Adults" (guided tours)
     * "Jullov", "lov" → "Children" (holiday activities)
     * "ateljé", "workshop" → "Children/Families" (studio activities)
     * "Drop-in", "fri entré" → "All ages"
   - Look for age indicators: "från X år", "för X-åringar"
   - If no clear audience → "All ages" (not null)

10. LOCATION (CRITICAL - Find actual venue):
    - Search within event container for specific location indicators:
      * Room names: "Salen", "Ateljén", "Rummet", "Salen"
      * Building sections: "Nationalmuseum", "Östasiatiska"
      * Exhibition halls: "Porträttsalen", "Samlingarna"
    - Look for data attributes: data-location, data-venue
    - Check for address/venue info in event details
    - ONLY use "N/A" if absolutely no location info found

11. BOOKING INFORMATION (NEW):
     - Look for booking requirements:
       * "Boka plats", "Bokning krävs" → "Requires booking"
       * "Drop-in", "Dropin" → "Drop-in"
       * "Fri entré", "Gratis" → "Free entry"
       * "Fullbokat" → "Fully booked"
     - Check for booking buttons/links text
     - If no booking info → "N/A"

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
      "detail_link": "Extract ACTUAL event detail URL, not calendar page.
      Look for <a href> tags within each event container.
      If no specific link exists, use the calendar URL as fallback.",
      "booking_info": "Booking requirements extracted from text"
    }}
  ],
  "extraction_stats": {{
    "total_found": 5,
    "successfully_parsed": 5,
    "missing_fields": ["end_date_iso", "target_group"]
  }}
}}

CRITICAL INSTRUCTIONS:
- BE SYSTEMATIC AND THOROUGH: Don't stop at first pattern you find
- TEST MULTIPLE APPROACHES: Try different container/field combinations
- BE CONFIDENT: If you find a repeating pattern with clear event data, use it!
- AVOID FALSE NEGATIVES: Better to have some false positives than miss all events
- SWEDISH CONTEXT: This is a Swedish museum - expect Swedish content and patterns
- MODERN WEBSITES: Use semantic HTML5 tags and data attributes
- QUALITY OVER CAUTION: If selectors extract meaningful event data, confidence should be 0.7+

EXTRACTION REQUIREMENTS:
- Extract ALL events found in HTML (don't miss any)
- Event names must be actual titles (not "Event" or generic text)
- Dates must be parseable to ISO format
- Locations should be specific rooms/venues, not just "N/A"
- Target groups should be detected from event types and content
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
    
    def _validate_selectors_against_html(self, selectors: Dict, html_content: str) -> Dict:
        """
        Test if discovered selectors actually work against the HTML.
        This is CRITICAL to verify selectors work before caching them.
        
        Args:
            selectors: Dictionary with container and items selectors
            html_content: Raw HTML to test selectors against
            
        Returns:
            Validation report with actual test results and adjusted confidence
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            self.logger.warning("BeautifulSoup not available for validation, skipping selector testing")
            return {'valid': True, 'tested': False}
        
        validation_report = {
            'valid': True,
            'tested': True,
            'container_matches': 0,
            'field_validation': {},
            'issues': [],
            'adjusted_confidence': 1.0
        }
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Test container selector
            container_selector = selectors.get('container', '')
            if not container_selector:
                validation_report['issues'].append('No container selector provided')
                validation_report['valid'] = False
                validation_report['adjusted_confidence'] = 0.0
                return validation_report
            
            # Try to find container elements
            container_elements = soup.select(container_selector)
            validation_report['container_matches'] = len(container_elements)
            
            if len(container_elements) == 0:
                validation_report['issues'].append(f"Container selector '{container_selector}' matches 0 elements")
                validation_report['valid'] = False
                validation_report['adjusted_confidence'] = 0.0
                self.logger.warning(f"⚠️ Container selector '{container_selector}' found NO matches in HTML")
                return validation_report
            
            self.logger.info(f"✅ Container selector '{container_selector}' matched {len(container_elements)} elements")
            
            # Test each field selector on the first few containers
            items = selectors.get('items', {})
            tested_containers = min(3, len(container_elements))
            
            for field_name in self.required_fields:
                if field_name not in items:
                    validation_report['field_validation'][field_name] = {
                        'selector': None,
                        'matches': 0,
                        'extraction_success': False,
                        'status': 'NOT_PROVIDED'
                    }
                    continue
                
                field_config = items[field_name]
                field_selector = field_config.get('selector') if isinstance(field_config, dict) else field_config
                
                if not field_selector or field_selector == "null":
                    validation_report['field_validation'][field_name] = {
                        'selector': field_selector,
                        'matches': 0,
                        'extraction_success': False,
                        'status': 'EMPTY_SELECTOR'
                    }
                    continue
                
                # Test selector on each container
                total_matches = 0
                extracted_samples = []
                
                for container in container_elements[:tested_containers]:
                    try:
                        matches = container.select(field_selector)
                        total_matches += len(matches)
                        
                        if matches and len(extracted_samples) < 2:
                            # Extract sample text
                            text = matches[0].get_text(strip=True)
                            if text:
                                extracted_samples.append(text[:80])
                    except:
                        pass
                
                success = total_matches > 0
                validation_report['field_validation'][field_name] = {
                    'selector': field_selector,
                    'matches': total_matches,
                    'extraction_success': success,
                    'samples': extracted_samples,
                    'status': 'PASS' if success else 'FAIL'
                }
                
                if success:
                    self.logger.info(f"  ✅ {field_name}: selector '{field_selector}' matched {total_matches} elements")
                else:
                    self.logger.warning(f"  ❌ {field_name}: selector '{field_selector}' matched 0 elements")
                    validation_report['issues'].append(f"Field '{field_name}' selector '{field_selector}' found no matches")
            
            # Calculate adjusted confidence based on actual test results
            passed_fields = sum(1 for v in validation_report['field_validation'].values() 
                               if v.get('status') == 'PASS')
            total_fields = len([v for v in validation_report['field_validation'].values() 
                               if v.get('status') in ['PASS', 'FAIL']])
            
            if total_fields > 0:
                validation_report['adjusted_confidence'] = passed_fields / total_fields
                self.logger.info(f"Selector validation: {passed_fields}/{total_fields} fields working ({validation_report['adjusted_confidence']:.0%})")
            
            # Mark as invalid if less than 60% of fields work
            if validation_report['adjusted_confidence'] < 0.6:
                validation_report['valid'] = False
                self.logger.warning(f"⚠️ Selectors fail validation: only {validation_report['adjusted_confidence']:.0%} of fields working")
            
            return validation_report
            
        except Exception as e:
            self.logger.error(f"Error during selector validation: {e}")
            validation_report['issues'].append(f"Validation error: {str(e)}")
            validation_report['tested'] = False
            return validation_report
    
    def _validate_events(self, events: List[Dict]) -> List[Dict]:
        """Validate and clean extracted events"""
        valid_events = []
        
        for event in events:
            # Must have event name
            if not event.get('event_name'):
                self.logger.warning(f"Skipping invalid event: {event.get('event_name', 'NO NAME')}")
                continue
            
            # Handle combined datetime scenario
            date_iso = event.get('date_iso')
            time = event.get('time')
            
            # Check if date_iso and time are identical (combined selector scenario)
            if date_iso and time and date_iso == time:
                # Parse combined datetime
                parsed_date, parsed_time = parse_combined_datetime(date_iso)
                
                if parsed_date:
                    event['date_iso'] = parsed_date
                else:
                    # If parsing failed, skip this event
                    self.logger.warning(f"Failed to parse combined datetime for event: {event['event_name']}")
                    continue
                
                # Set time (may be None if only date was found)
                event['time'] = parsed_time
            
            # Validate date format
            try:
                if event.get('date_iso'):
                    datetime.strptime(event['date_iso'], '%Y-%m-%d')
                else:
                    # If no valid date, skip this event
                    self.logger.warning(f"No valid date for event: {event['event_name']}")
                    continue
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
    
    def __init__(self, ai_client, logger, db_manager=None, selector_cache=None):
        self.discovery = AutoSelectorDiscovery(ai_client, logger)
        self.logger = logger
        self.db = db_manager
        self.selector_cache = selector_cache or {}  # Store selectors per domain
        
        # Optional: Initialize manual selector manager if DB available
        self.manual_selector_manager = None
        if db_manager:
            try:
                from event_category.utils.manual_selector_manager import ManualSelectorManager
                self.manual_selector_manager = ManualSelectorManager(db_manager, logger)
            except ImportError:
                self.logger.warning("ManualSelectorManager not available")
    
    def _ensure_all_required_fields(self, items: Dict) -> Dict:
        """
        Ensure all required fields are present in the items dictionary.
        Missing fields are set to empty string so they appear in the admin UI.
        This is called only for NEW URLs when AI discovers selectors.
        """
        required_fields = [
            "event_name",
            "date_iso", 
            "time",
            "location",
            "description",
            "target_group",
            "status",
            "event_url"
        ]
        
        complete_items = {}
        for field in required_fields:
            if field in items:
                # Keep the discovered selector
                complete_items[field] = items[field]
            else:
                # Add empty placeholder for missing field
                complete_items[field] = ""
        
        # Also preserve any extra fields that were discovered
        for field, value in items.items():
            if field not in complete_items:
                complete_items[field] = value
        
        return complete_items
    
    def scrape_new_website(self, url: str, html_content: str) -> List[Dict]:
        """
        Automatically discover selectors and extract events from a new website.
        If discovery fails, prompts user for manual selector input.
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
                
                # Offer manual input option
                if self.manual_selector_manager:
                    selectors = self.manual_selector_manager.prompt_for_manual_selectors(url)
                    if not selectors:
                        return []
                else:
                    return []
            else:
                selectors = discovery_result['selectors']
                confidence = discovery_result.get('confidence', {}).get('overall', 0)
                
                self.logger.info(f"Selector discovery confidence: {confidence}")
                
                # Check validation but don't block on it
                validation = discovery_result.get('selector_validation', {})
                validation_passed = validation.get('valid', False)
                adjusted_confidence = validation.get('adjusted_confidence', 0)
                
                if validation_passed:
                    self.logger.info(f"✅ Selectors validated successfully ({adjusted_confidence:.0%})")
                else:
                    self.logger.warning(f"⚠️  Selectors failed validation: {validation.get('issues', [])}")
                    self.logger.warning(f"   Validation score: {adjusted_confidence:.0%}")
                    self.logger.info(f"   Saving to DB anyway for user review...")
                
                # ALWAYS save AI-discovered selectors to DB
                if self.db:
                    try:
                        self.db.save_selectors(
                            url,
                            selectors.get('container', ''),
                            self._ensure_all_required_fields(selectors.get('items', {}))
                        )
                        self.logger.info(f"✅ Saved AI-discovered selectors to DB for {domain}")
                    except Exception as e:
                        self.logger.warning(f"Could not save to DB: {e}")
                
                # Offer user the chance to review/edit the AI selectors
                if self.manual_selector_manager:
                    self.logger.info("\nOffering user review/edit option...")
                    original_selectors = json.dumps(selectors, sort_keys=True)  # For comparison
                    reviewed_selectors = self.manual_selector_manager.review_and_edit_selectors(url, selectors)
                    
                    if reviewed_selectors is None:
                        self.logger.warning(f"User skipped selectors for {domain}")
                        return []
                    
                    reviewed_json = json.dumps(reviewed_selectors, sort_keys=True)
                    selectors = reviewed_selectors
                    
                    # If user edited, save the updated selectors
                    if reviewed_json != original_selectors:
                        if self.db:
                            try:
                                self.db.save_selectors(
                                    url,
                                    selectors.get('container', ''),
                                    self._ensure_all_required_fields(selectors.get('items', {}))
                                )
                                self.logger.info(f"✅ Saved user-edited selectors to DB for {domain}")
                            except Exception as e:
                                self.logger.warning(f"Could not save edited selectors: {e}")
            
            # Cache selectors for this session
            if selectors:
                self.selector_cache[domain] = selectors
                self.logger.info(f"Cached selectors for {domain}")
        
        # If no selectors found, return empty
        if selectors is None:
            self.logger.warning(f"No selectors available for {domain}")
            return []
        
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
