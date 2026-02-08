# Event Scraper API - Endpoint Documentation

> **Base URL**: `https://us-central1-YOUR-PROJECT.cloudfunctions.net`  
> **Last Updated**: 2026-02-03

---

## Overview

This API provides access to the Event Scraper system, which collects and manages event data from various Swedish cultural venues. All endpoints return JSON responses.

### Response Format

All endpoints return a consistent JSON structure:

```json
{
  "success": true,
  "data": { ... }
}
```

On error:
```json
{
  "success": false,
  "error": "Error message description"
}
```

---

## 🎫 Public Endpoints (For App Users)

These endpoints are for displaying events to end users in the mobile/web app.

---

### 1. List Events

Retrieve events with filtering, searching, and pagination.

| Property | Value |
|----------|-------|
| **Endpoint** | `/events` |
| **Method** | `GET` |
| **Auth Required** | No |

#### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | integer | `1` | Page number for pagination |
| `per_page` | integer | `20` | Number of events per page (max 100) |
| `venue` | string | `"All Venues"` | Filter by venue name (e.g., "Skansen", "Moderna Museet") |
| `search` | string | `""` | Search in event name and description |
| `source` | string | `"All Sources"` | Filter by source website |
| `date_range` | string | `"All Time"` | Options: `"All Time"`, `"This Week"`, `"This Month"` |
| `target_groups` | string | `""` | Comma-separated list: `"families,children,all_ages"` |
| `filter_date` | string | `null` | Specific date filter (ISO format: `YYYY-MM-DD`) |

#### Example Request

```
GET /events?page=1&per_page=10&venue=Skansen&target_groups=families,children
```

#### Response

```json
{
  "success": true,
  "data": [
    {
      "id": 123,
      "unique_key": "skansen_event_abc123",
      "event_name": "Midsommar Celebration",
      "event_date": "2026-06-21",
      "event_end_date": "2026-06-21",
      "event_time": "12:00",
      "location": "Skansen",
      "description": "Traditional Swedish midsummer celebration...",
      "event_url": "https://skansen.se/events/midsommar",
      "image_url": "https://skansen.se/images/midsommar.jpg",
      "price": "150 kr",
      "age_limit": "All ages",
      "target_group": "families",
      "normalized_target_group": "families",
      "source_url": "https://skansen.se/kalender",
      "created_at": "2026-01-15T10:30:00Z",
      "updated_at": "2026-02-01T14:22:00Z"
    }
  ]
}
```

---

### 2. Get Venues

Get list of all unique venue names for filter dropdowns.

| Property | Value |
|----------|-------|
| **Endpoint** | `/venues` |
| **Method** | `GET` |
| **Auth Required** | No |

#### Example Request

```
GET /venues
```

#### Response

```json
{
  "success": true,
  "data": [
    "Skansen",
    "Moderna Museet",
    "Naturhistoriska riksmuseet",
    "Tekniska museet",
    "Stockholms stadsbibliotek",
    "Armémuseum"
  ]
}
```

---

### 3. Get Sources

Get list of all source websites (where events are scraped from).

| Property | Value |
|----------|-------|
| **Endpoint** | `/sources` |
| **Method** | `GET` |
| **Auth Required** | No |

#### Example Request

```
GET /sources
```

#### Response

```json
{
  "success": true,
  "data": [
    {"url": "skansen.se", "name": "Skansen"},
    {"url": "modernamuseet.se", "name": "Moderna Museet"},
    {"url": "biblioteket.stockholm.se", "name": "Stockholm Library"}
  ]
}
```

---

### 4. Get Statistics

Get dashboard statistics and analytics.

| Property | Value |
|----------|-------|
| **Endpoint** | `/stats` |
| **Method** | `GET` |
| **Auth Required** | No |

#### Example Request

```
GET /stats
```

#### Response

```json
{
  "success": true,
  "data": {
    "total_events": 1523,
    "events_this_week": 87,
    "events_next_month": 234,
    "active_venues": 12,
    "events_by_venue": [
      {"venue": "Skansen", "count": 145},
      {"venue": "Moderna Museet", "count": 89}
    ],
    "events_by_target_group": [
      {"target_group": "families", "count": 432},
      {"target_group": "children", "count": 298},
      {"target_group": "all_ages", "count": 793}
    ],
    "events_timeline": [
      {"week": "2026-W05", "count": 42},
      {"week": "2026-W06", "count": 55}
    ]
  }
}
```

---

### 5. Health Check

Check if the API is running.

| Property | Value |
|----------|-------|
| **Endpoint** | `/health` |
| **Method** | `GET` |
| **Auth Required** | No |

#### Response

```json
{
  "status": "healthy",
  "service": "event-scraper-api",
  "version": "1.0.0"
}
```

---

## 🔧 Admin Endpoints

These endpoints are for managing the scraping system. Consider adding authentication for production.

---

### 6. Pending Changes

View and manage pending event additions/deletions before they're applied.

| Property | Value |
|----------|-------|
| **Endpoint** | `/pendingChanges` |
| **Method** | `GET`, `POST` |
| **Auth Required** | Yes (recommended) |

#### GET - View Pending Changes

```
GET /pendingChanges
```

#### Response

```json
{
  "success": true,
  "data": {
    "counts": {
      "pending_inserts": 15,
      "pending_deletes": 3
    },
    "changes": {
      "inserts": [
        {
          "id": 1,
          "event_data": {
            "event_name": "New Event",
            "event_date": "2026-03-15",
            "location": "Skansen"
          },
          "detected_at": "2026-02-03T10:00:00Z",
          "run_id": "run_abc123"
        }
      ],
      "deletes": [
        {
          "id": 2,
          "event_data": {
            "event_name": "Cancelled Event",
            "event_date": "2026-02-20"
          },
          "detected_at": "2026-02-03T10:00:00Z"
        }
      ]
    }
  }
}
```

#### POST - Apply or Discard Changes

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `action` | string | Yes | `"approve"`, `"discard"`, or `"discard_single"` |
| `change_id` | integer | For `discard_single` | ID of the specific change to discard |

**Examples:**

Approve all pending changes:
```json
POST /pendingChanges
{
  "action": "approve"
}
```

Discard all pending changes:
```json
POST /pendingChanges
{
  "action": "discard"
}
```

Discard a single change:
```json
POST /pendingChanges
{
  "action": "discard_single",
  "change_id": 123
}
```

---

### 7. Scraping URLs

Manage the list of URLs that are scraped for events.

| Property | Value |
|----------|-------|
| **Endpoint** | `/scrapingUrls` |
| **Method** | `GET`, `POST`, `PUT`, `DELETE` |
| **Auth Required** | Yes (recommended) |

#### GET - List All URLs

```
GET /scrapingUrls
```

#### Response

```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "url": "https://skansen.se/kalender",
      "name": "Skansen",
      "enabled": true,
      "created_at": "2026-01-01T00:00:00Z"
    },
    {
      "id": 2,
      "url": "https://modernamuseet.se/events",
      "name": "Moderna Museet",
      "enabled": true,
      "created_at": "2026-01-01T00:00:00Z"
    }
  ]
}
```

#### POST - Add New URL

```json
POST /scrapingUrls
{
  "url": "https://newvenue.se/events",
  "name": "New Venue"
}
```

#### PUT - Enable/Disable URL

```json
PUT /scrapingUrls
{
  "url_id": 5,
  "enabled": false
}
```

#### DELETE - Remove URL

```json
DELETE /scrapingUrls
{
  "url_id": 5
}
```

---

### 8. Scraping Logs

View logs from previous scrape runs.

| Property | Value |
|----------|-------|
| **Endpoint** | `/logs` |
| **Method** | `GET` |
| **Auth Required** | Yes (recommended) |

#### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days` | integer | `30` | Number of days of logs to retrieve |
| `status` | string | `"All"` | Filter by status: `"All"`, `"success"`, `"failed"` |

#### Example Request

```
GET /logs?days=7&status=failed
```

#### Response

```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "run_type": "incremental",
      "status": "success",
      "events_found": 45,
      "failures": 0,
      "warnings": null,
      "created_at": "2026-02-03T03:00:00Z"
    }
  ]
}
```

---

### 9. Scraping Failures

View active and historical scraping failures.

| Property | Value |
|----------|-------|
| **Endpoint** | `/failures` |
| **Method** | `GET` |
| **Auth Required** | Yes (recommended) |

#### Example Request

```
GET /failures
```

#### Response

```json
{
  "success": true,
  "data": {
    "active": [
      {
        "id": 1,
        "url": "https://broken-site.se/events",
        "url_name": "Broken Site",
        "error_message": "Connection timeout",
        "error_type": "TIMEOUT",
        "first_failure": "2026-02-01T10:00:00Z",
        "failure_count": 3
      }
    ],
    "history": [
      {
        "id": 2,
        "url": "https://fixed-site.se/events",
        "resolved_at": "2026-02-02T15:00:00Z"
      }
    ]
  }
}
```

---

### 10. Scheduler Settings

View and update scheduler configuration.

| Property | Value |
|----------|-------|
| **Endpoint** | `/schedulerSettings` |
| **Method** | `GET`, `PUT` |
| **Auth Required** | Yes (recommended) |

#### GET - View Settings

```
GET /schedulerSettings
```

#### Response

```json
{
  "success": true,
  "data": {
    "settings": {
      "baseline_enabled": true,
      "baseline_day": 1,
      "baseline_hour": 2,
      "incremental_enabled": true,
      "incremental_interval_days": 3,
      "incremental_hour": 3,
      "scrape_days": 60
    },
    "last_scrape": {
      "last_baseline_run": "2026-02-01T02:00:00Z",
      "last_incremental_run": "2026-02-03T03:00:00Z"
    }
  }
}
```

#### PUT - Update Settings

```json
PUT /schedulerSettings
{
  "incremental_interval_days": 2,
  "scrape_days": 90
}
```

---

### 11. Trigger Scrape

Manually trigger a scraping run.

| Property | Value |
|----------|-------|
| **Endpoint** | `/scrapeNow` |
| **Method** | `POST` |
| **Auth Required** | Yes (recommended) |
| **Timeout** | 9 minutes (incremental), Cloud Run (baseline) |

#### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | No | `"incremental"` (default) or `"baseline"` |
| `url` | string | No | Specific URL to scrape (optional) |

#### Examples

Trigger incremental scrape:
```json
POST /scrapeNow
{
  "type": "incremental"
}
```

Scrape specific URL:
```json
POST /scrapeNow
{
  "type": "incremental",
  "url": "https://skansen.se/kalender"
}
```

Trigger full baseline scrape (Cloud Run):
```json
POST /scrapeNow
{
  "type": "baseline"
}
```

#### Response

```json
{
  "success": true,
  "message": "Incremental scrape started",
  "type": "incremental",
  "target_url": null
}
```

---

## 📊 Event Data Schema

This is the complete structure of an event object:

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Database ID |
| `unique_key` | string | Unique identifier for deduplication |
| `event_name` | string | Name of the event |
| `event_date` | string | Start date (ISO format: `YYYY-MM-DD`) |
| `event_end_date` | string | End date for multi-day events |
| `event_time` | string | Time of the event (e.g., `"14:00"`) |
| `location` | string | Venue/location name |
| `description` | string | Full event description |
| `event_url` | string | Link to the original event page |
| `image_url` | string | URL to the event image |
| `price` | string | Price information (e.g., `"Free"`, `"150 kr"`) |
| `age_limit` | string | Age restriction (e.g., `"All ages"`, `"18+"`) |
| `target_group` | string | Raw target group from source |
| `normalized_target_group` | string | Standardized: `families`, `children`, `adults`, `seniors`, `all_ages` |
| `source_url` | string | URL of the calendar page scraped |
| `created_at` | string | When the event was first added (ISO timestamp) |
| `updated_at` | string | When the event was last updated (ISO timestamp) |

---

## 🔐 Authentication (Recommended for Admin Endpoints)

For admin endpoints, consider implementing one of these:

1. **Firebase Authentication** - JWT tokens
2. **API Key** - Simple header-based auth
3. **IP Whitelisting** - Restrict to known IPs

Example with API key:
```
GET /pendingChanges
Headers:
  X-API-Key: your-secret-api-key
```

---

## 🚦 Rate Limits

| Tier | Limit |
|------|-------|
| Public endpoints | 100 requests/minute |
| Admin endpoints | 30 requests/minute |
| Scrape triggers | 5 requests/hour |

---

## ❓ Need More Endpoints?

If you need additional functionality, here are some ideas:

- `GET /events/{id}` - Get single event by ID
- `GET /events/nearby?lat=X&lon=Y` - Events near a location
- `GET /events/featured` - Curated/featured events
- `POST /events/subscribe` - Subscribe to event notifications
- `GET /categories` - List event categories/types

Please let me know what additional endpoints you need!

---

## 📞 Contact

For questions about this API, contact: [Your Email]
