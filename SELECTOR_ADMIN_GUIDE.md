# Selector Management Admin Guide

## Overview
The **🎯 Selectors** tab in the admin console allows you to manage CSS selectors for event extraction without needing to run the spider. This is useful for:
- Editing selectors after they've been discovered
- Adding selectors for new URLs manually
- Testing selector changes before running the spider
- Maintaining selector configurations across deployments

## Features

### 1. View All Configured Selectors
The main view shows all URLs that have selector configurations in the database, organized by domain and URL pattern.

**Each selector card displays:**
- 🌐 Domain and URL pattern
- Current container selector
- All item selectors (event_name, date_iso, time, location, description, booking_info, target_group, status)
- Last updated timestamp

### 2. Edit Selectors (Two Modes)

#### Field-by-Field Editor
1. Click **✏️ Edit Field-by-Field** on any selector card
2. Edit each selector individually in a form interface
3. Fields include:
   - **Container Selector** - CSS selector for the container holding events
   - **Event Name Selector** - Extracts event title/name
   - **Date Selector** - Extracts event date (ISO format preferred)
   - **Time Selector** - Extracts event time
   - **Location Selector** - Extracts venue location
   - **Description Selector** - Extracts event description
   - **Booking Info Selector** - Extracts booking/registration details
   - **Target Group Selector** - Extracts age group/audience
   - **Status Selector** - Extracts event status (scheduled, cancelled, etc.)
4. Click **💾 Save Changes** when done
5. Selectors are immediately saved to the database

#### JSON Editor
1. Click **📝 Edit as JSON** on any selector card
2. Modify the complete configuration as JSON:
   ```json
   {
     "container_selector": ".event-item",
     "item_selectors": {
       "event_name": "h3.title",
       "date_iso": "span.date",
       "time": "span.time",
       "location": "span.location",
       "description": "p.description",
       "booking_info": "a.booking",
       "target_group": "span.age",
       "status": "span.status"
     }
   }
   ```
3. Click **💾 Save Changes** when done
4. The JSON is validated before saving

### 3. Delete Selectors
1. Click **🗑️ Delete Selectors** at the bottom of any selector card
2. Confirm deletion - the selector configuration is removed from database
3. Note: This only removes the selectors, not the actual events

### 4. Add New Selector
To add selectors for a new URL:

1. Expand **➕ ADD NEW SELECTOR** section
2. Fill in:
   - **URL** - Full URL pattern (e.g., https://example.com/events)
   - **Container Selector** - CSS selector for event containers
   - **Item Selectors** - At least one selector must be provided
3. Click **➕ Add Selector Configuration**
4. The configuration is saved to database

### 5. CSS Selector Tips

#### Finding the Right Selector
Use your browser's Developer Tools:
1. Right-click on an event element
2. Click "Inspect" or "Inspect Element"
3. Identify the CSS class or ID that wraps the event
4. Use that as your container selector

#### Common Selector Patterns
- **Class selector**: `.event-card`, `.event-item`
- **ID selector**: `#event-1`, `#events`
- **Type selector**: `article`, `div`
- **Attribute selector**: `[data-type="event"]`
- **Nested selector**: `.container .event-item`

#### Testing Selectors
Before saving, validate that your selectors:
- Match the actual HTML structure
- Are specific enough to target only events (not ads, headers, etc.)
- Work consistently across different event instances on the page

## Workflow: AI Discovery → Manual Refinement

The typical workflow is:

1. **Spider runs** → AI discovers selectors
2. **Auto-save** → Selectors saved to database
3. **Terminal review** → You accept/edit/skip via terminal prompt
4. **Manual adjustment** → Come to admin console to fine-tune selectors
5. **Save changes** → Updated selectors saved to database
6. **Next run** → Spider uses updated selectors for extraction

## Database Operations

All selector changes are immediately saved to the `selector_configs` table with:
- **domain** - Extracted from URL (e.g., "nationalmuseum.se")
- **url_pattern** - Path component of URL (e.g., "/en/events")
- **container_selector** - CSS selector for event containers
- **item_selectors_json** - JSON object mapping field names to selectors
- **last_updated** - Timestamp of last change

## Troubleshooting

### No selectors showing?
- Run the spider on a new URL to auto-discover selectors
- Or manually add selectors using the "Add New Selector" form

### Changes not appearing?
- Refresh the page (Ctrl+R or Cmd+R)
- Check that the URL and domain are correct
- Verify the JSON syntax if using JSON editor

### Selectors not working during extraction?
1. Use browser Developer Tools to verify the selectors match the HTML
2. Check for JavaScript-rendered content (selector may need to target loaded elements differently)
3. Ensure the selector matches all event instances, not just the first one
4. Consider using more specific selectors (e.g., `.container .event-item` instead of just `.item`)

## Advanced: Manual Updates via Terminal

If preferred, you can also manage selectors directly via terminal using the `manual_selector_manager.py` module:

```python
from manual_selector_manager import ManualSelectorManager
from event_category.utils.db_manager import DatabaseManager

db = DatabaseManager()
manager = ManualSelectorManager(db)

# Review and edit AI-discovered selectors
manager.review_and_edit_selectors(url, selectors)

# List all saved selectors
manager.list_manual_selectors()

# Delete a selector configuration
manager.delete_manual_selectors(url)
```

## Key Differences: Terminal vs Admin Console

| Aspect | Terminal | Admin Console |
|--------|----------|---------------|
| Interface | Command-line prompts | Web UI in browser |
| When used | During spider run | Anytime, without spider |
| Edit options | Accept/Edit field/Edit JSON/Skip | Edit field/Edit JSON/Delete |
| Database save | Automatic | Immediate on save |
| Visibility | Console output | Nice table/card layout |
| Bulk operations | Limited | Can manage multiple URLs |

## Best Practices

1. **Test before saving** - Manually verify selectors in browser tools first
2. **Document why** - If using complex selectors, add a comment for future reference
3. **Keep it simple** - Use the simplest selector that accurately targets the elements
4. **Version control** - If using Git, you can track selector changes over time
5. **Regular review** - Check if selectors still work if website redesigns

## Common Issues & Solutions

### Issue: "Invalid JSON"
**Solution**: Ensure JSON syntax is valid. Use online JSON validators if uncertain.

```json
// ✅ Correct
{ "container_selector": "...", "item_selectors": {...} }

// ❌ Incorrect (single quotes not allowed in JSON)
{ 'container_selector': '...', 'item_selectors': {...} }
```

### Issue: No events extracted with saved selectors
**Solution**: 
1. Verify selectors match actual HTML (use browser Dev Tools)
2. Check if website uses JavaScript to load events (may need to use Playwright)
3. Try broader selectors (e.g., `article` instead of `.specific-class`)

### Issue: Selectors work in one place but not another
**Solution**: 
1. Check for dynamic/JavaScript-rendered content
2. Use more specific selectors that account for page structure variations
3. Test on multiple event pages to ensure consistency

---

**Last Updated**: December 29, 2025
**Admin Console Version**: 1.0
