# Event Scraper API - Endpoint Summary

> **Base URL**: `https://us-central1-YOUR-PROJECT.cloudfunctions.net`

---

## All Available Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/events` | GET | Retrieve a paginated list of events with support for filtering by venue, source, date range, target group (families/children/all ages), and text search. Returns event details including name, date, time, location, description, image URL, and price. |
| `/venues` | GET | Get a list of all unique venue names currently stored in the database. Useful for populating a dropdown filter in the app UI (e.g., "Skansen", "Moderna Museet", "Tekniska museet"). |
| `/sources` | GET | Get a list of all source websites where events are scraped from. Returns both the URL and a friendly name. Useful for filtering events by their origin website. |
| `/stats` | GET | Retrieve aggregated dashboard statistics including: total event count, events happening this week, events next month, number of active venues, breakdown of events by venue, breakdown by target group, and a timeline of upcoming events per week. |
| `/health` | GET | Simple health check endpoint that returns the API status, service name, and version. Used for monitoring, load balancers, and deployment verification to ensure the API is running correctly. |
| `/pendingChanges` | GET, POST | View all pending event additions and deletions detected by the incremental scraper. POST to approve all changes (apply to main database), discard all changes, or discard a single specific change by ID. Allows manual review before events go live. |
| `/scrapingUrls` | GET, POST, PUT, DELETE | Manage the list of URLs that the scraper monitors for events. GET returns all configured URLs with their enabled/disabled status. POST adds a new URL. PUT enables or disables a URL. DELETE removes a URL from the scraper. |
| `/logs` | GET | Retrieve historical scraping logs showing when scrapes ran, how many events were found, success/failure status, and any warnings. Filterable by number of days and status. Useful for debugging and monitoring scraper health. |
| `/failures` | GET | View active scraping failures (URLs that are currently failing to scrape) and historical failures that have been resolved. Shows error messages, failure counts, and timestamps. Helps identify problematic source websites. |
| `/schedulerSettings` | GET, PUT | View and configure the automatic scraper schedule. GET returns current settings (baseline scrape day/hour, incremental interval, scrape window days) and timestamps of last runs. PUT updates schedule configuration. |
| `/scrapeNow` | POST | Manually trigger a scrape on demand. Can specify "incremental" (quick delta check, ~5-10 min) or "baseline" (full scrape, ~30 min). Optionally specify a single URL to scrape instead of all configured URLs. |

---

## Frontend vs Admin Endpoints

### 🎫 For User-Facing App 

| Endpoint | Method | What It's For |
|----------|--------|---------------|
| `/events` | GET | Main event listing page with filters |
| `/venues` | GET | "Filter by venue" dropdown |
| `/sources` | GET | "Filter by source" dropdown |
| `/stats` | GET | Home screen stats/dashboard (optional) |

### 🔧 For Admin Dashboard Only (Backend Management)

| Endpoint | Method | What It's For |
|----------|--------|---------------|
| `/health` | GET | Monitoring & DevOps |
| `/pendingChanges` | GET, POST | Approve new/deleted events |
| `/scrapingUrls` | GET, POST, PUT, DELETE | Add/remove scraper URLs |
| `/logs` | GET | View scraping history |
| `/failures` | GET | Debug failing scrapers |
| `/schedulerSettings` | GET, PUT | Configure auto-scrape schedule |
| `/scrapeNow` | POST | Trigger manual scrape |

---

