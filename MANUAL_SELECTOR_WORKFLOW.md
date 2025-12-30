# Manual Selector Review/Edit Workflow

## Where is the Edit Option?

The review/edit interface appears **during spider execution** when a new website URL is being processed. Here's the exact flow:

### Trigger Points

The manual review/edit happens in **EventScraperOrchestrator.scrape_new_website()** which is called from:
- **File**: `event_category/spiders/universal_spider.py`
- **Line**: 1249 (in the STEP A.6 generic event extraction section)
- **When**: After pagination, when processing new websites that aren't Skansen/Tekniska/Moderna/Armemuseum

```python
# From universal_spider.py line 1249
all_extracted_data = self.orchestrator.scrape_new_website(
    url=response.url,
    html_content=page_html
)
```

### Exact Flow During Execution

When you run the spider with a new URL:

```
1. Spider starts
   └─ Initializes orchestrator (with DB manager)
   
2. Navigates to URL and clicks pagination
   └─ Loads more events
   
3. Extracts HTML content
   └─ Calls orchestrator.scrape_new_website()
   
4. AI DISCOVERS SELECTORS
   └─ Calls discovery.discover_website_structure()
   
5. SAVES TO DATABASE IMMEDIATELY
   └─ db.save_selectors(url, selectors)
   └─ ✅ Selectors now in database
   
6. SHOWS USER REVIEW/EDIT INTERFACE
   └─ Calls manual_selector_manager.review_and_edit_selectors()
   └─ Prompts user with 4 options
   
7. User chooses action:
   ├─ [1] Accept as-is        → Use for extraction
   ├─ [2] Edit field-by-field  → Edit one at a time
   ├─ [3] Edit as JSON         → Paste modified JSON
   └─ [4] Skip                 → Discard selectors
   
8. If edited, UPDATE DATABASE
   └─ db.save_selectors(url, edited_selectors)
   └─ ✅ Updated selectors in database
   
9. EXTRACT EVENTS
   └─ discovery.extract_events_with_selectors()
   └─ Uses final selectors to extract data
```

## What the User Sees in Terminal

### Step 1: Pagination Loading
```
2025-12-29 17:57:11 [universal_events] INFO: Found 'a.show-more-text' - clicking to load more events...
2025-12-29 17:57:13 [universal_events] INFO: Successfully clicked 'load more' 5 times
```

### Step 2: AI Discovery
```
2025-12-29 17:57:14 [universal_events] INFO: Using EventScraperOrchestrator for automatic selector discovery...
2025-12-29 17:57:14 [universal_events] INFO: Discovering selectors for new website: www.nationalmuseum.se
2025-12-29 17:57:14 [universal_events] INFO: Starting automatic selector discovery for: https://www.nationalmuseum.se/kalendarium
2025-12-29 17:57:14 [universal_events] INFO: Attempting HTML + Text Correlation discovery...
2025-12-29 17:57:45 [universal_events] INFO: HTML + Text Correlation confidence: 69%
2025-12-29 17:57:45 [universal_events] INFO: ✅ HTML + Text Correlation successful with high confidence
```

### Step 3: Auto-Save to DB
```
2025-12-29 17:57:45 [universal_events] INFO: ✅ Saved AI-discovered selectors to DB for www.nationalmuseum.se
2025-12-29 17:57:45 [universal_events] INFO: Selector discovery confidence: 0.69
```

### Step 4: Review/Edit Prompt
```
================================================================================
REVIEW AI-DISCOVERED SELECTORS
================================================================================

URL: https://www.nationalmuseum.se/kalendarium

AI-Discovered Selectors:
  Container: article.article-card
  Fields:
    ✓ event_name: h2.title
    ✓ date_iso: time
    ✓ location: span.loc
    ✗ target_group: (empty)

----------------================================================================

================================================================================
REVIEW OPTIONS:
================================================================================
1. Accept selectors as-is (use for extraction)
2. Edit selectors (field-by-field)
3. Edit as JSON
4. Skip and discard selectors
--------------------------------------------------------------------------------

Select option (1-4): _
```

### User Action - Option 1: Accept
```
Select option (1-4): 1
✅ Accepting AI-discovered selectors
[universal_events] INFO: Extracting events using discovered selectors...
[universal_events] INFO: ✅ Extracted 25 events from www.nationalmuseum.se
```

### User Action - Option 2: Edit Field-by-Field
```
Select option (1-4): 2
Editing selectors field-by-field...

--------------------------------------------------------------------------------
EDIT CONTAINER SELECTOR
--------------------------------------------------------------------------------
Current: article.article-card
New value (or press Enter to keep): article.event-item

--------------------------------------------------------------------------------
EDIT FIELD SELECTORS
--------------------------------------------------------------------------------

event_name:
  Current: h2.title
  New value (or press Enter to keep): h2.event-title

date_iso:
  Current: time
  New value (or press Enter to keep): 

time:
  Current: (empty)
  New value (or press Enter to keep): span.time

location:
  Current: span.loc
  New value (or press Enter to keep): 

description:
  Current: (empty)
  New value (or press Enter to keep): p.description

target_group:
  Current: (empty)
  New value (or press Enter to keep): span.audience

status:
  Current: (empty)
  New value (or press Enter to keep): 

✅ Selectors updated
✅ Saved user-edited selectors to DB for www.nationalmuseum.se
[universal_events] INFO: Extracting events using discovered selectors...
[universal_events] INFO: ✅ Extracted 32 events from www.nationalmuseum.se
```

### User Action - Option 3: Edit as JSON
```
Select option (1-4): 3
Editing selectors as JSON...

--------------------------------------------------------------------------------
CURRENT SELECTORS (JSON)
--------------------------------------------------------------------------------
{
  "container": "article.article-card",
  "items": {
    "event_name": "h2.title",
    "date_iso": "time",
    "location": "span.loc",
    "target_group": null
  }
}

--------------------------------------------------------------------------------
PASTE YOUR EDITED JSON (Ctrl+D or Ctrl+Z to finish)
--------------------------------------------------------------------------------

{
  "container": "article.event-listing",
  "items": {
    "event_name": "h2.event-title",
    "date_iso": "[data-date]",
    "time": "span.time",
    "location": "span.venue",
    "description": "p.desc",
    "target_group": "span.audience",
    "status": "[data-status]"
  }
}

✅ Selectors updated
✅ Saved user-edited selectors to DB for www.nationalmuseum.se
[universal_events] INFO: ✅ Extracted 28 events from www.nationalmuseum.se
```

## Code Location Reference

### Main Files Involved

| File | Purpose | Lines |
|------|---------|-------|
| `universal_spider.py` | Spider that triggers orchestrator | 1249 |
| `auto_selector_discovery.py` | EventScraperOrchestrator class | 840-930 |
| `manual_selector_manager.py` | Review/edit interface | Entire file |
| `db_manager.py` | Database operations | Various |

### Key Methods

```python
# In universal_spider.py
self.orchestrator = EventScraperOrchestrator(
    ai_client=self.client,
    logger=self.logger,
    db_manager=self.db  # ← Now passes DB!
)

all_extracted_data = self.orchestrator.scrape_new_website(
    url=response.url,
    html_content=page_html
)

# In auto_selector_discovery.py (EventScraperOrchestrator)
def scrape_new_website(self, url, html_content):
    # Step 1: Discover selectors
    # Step 2: Save to DB
    # Step 3: Call review_and_edit_selectors()
    
# In manual_selector_manager.py
def review_and_edit_selectors(self, url, ai_selectors):
    # Shows the 4 options interface
    # Returns edited or original selectors

def _edit_selectors_field_by_field(self, current_selectors):
    # Field-by-field editor

def _edit_selectors_json(self, current_selectors):
    # JSON editor
```

## How to Use It

### Step 1: Run Spider with New URL
```bash
cd /Users/karthikraman/Workspace/Auto_Event_LLM/event_category
scrapy crawl universal_events -a url=https://www.example-museum.se/events -o output.json
```

### Step 2: Watch for the Prompt
When you see this in the terminal output, the review interface has appeared:

```
REVIEW OPTIONS:
1. Accept selectors as-is
2. Edit selectors (field-by-field)
3. Edit as JSON
4. Skip and discard

Select option (1-4): _
```

### Step 3: Choose Action
- **Type `1`** and press Enter to use AI selectors as-is (fastest)
- **Type `2`** and press Enter to edit each field one at a time (easiest for small changes)
- **Type `3`** and press Enter to paste modified JSON (best for complex changes)
- **Type `4`** and press Enter to skip (if AI completely wrong)

### Step 4: If Editing
For option 2 or 3, follow the prompts to modify selectors, then they're saved to DB automatically.

For option 3 (JSON), when you're done editing, press:
- **Ctrl+D** (on Mac/Linux)
- **Ctrl+Z** then Enter (on Windows)

### Step 5: Extraction Proceeds
After you respond, spider continues with extraction using the final selectors.

## Future Runs

Once you've saved selectors for a URL, subsequent runs will:
1. Load cached selectors from DB immediately
2. Skip discovery and review
3. Go straight to extraction
4. Much faster! ⚡

To re-discover/re-review, you'll need to manually delete the DB entry or clear `event_category/event_category/utils/selectors_cache.db`.

## Troubleshooting

### I don't see the review prompt
- Check that it's a NEW URL (not Skansen/Tekniska/Moderna/Armemuseum)
- Make sure terminal output shows "Using EventScraperOrchestrator..."
- Check that spider reached the generic event extraction section (STEP A.6)

### The prompt appeared but disappeared quickly
- It might have timed out - spider requires input to continue
- Run spider again or check logs for errors

### I want to re-edit already-saved selectors
- Option 1: Delete the DB and re-run spider
- Option 2: Create an admin command to edit selectors (future feature)
- For now, delete: `event_category/event_category/utils/selectors_cache.db`
