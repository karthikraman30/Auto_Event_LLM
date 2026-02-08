"""
Firebase Cloud Functions for Event Scraper API.

These HTTP endpoints are called by the frontend app to get events, venues, stats, etc.
All data is fetched from Supabase via the existing DatabaseManager.
"""
import os
import sys
import json
from datetime import datetime

# Firebase imports
from firebase_functions import https_fn, options
from firebase_admin import initialize_app

# Add parent directory to path to import existing modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from event_category.event_category.utils.db_manager import DatabaseManager

# Initialize Firebase app
initialize_app()

# CORS configuration - allow requests from any origin (frontend apps)
cors_options = options.CorsOptions(
    cors_origins="*",
    cors_methods=["GET", "POST", "OPTIONS"]
)


# ============================================================================
# CORE EVENT ENDPOINTS (For Frontend Users)
# ============================================================================

@https_fn.on_request(cors=cors_options)
def events(req: https_fn.Request) -> https_fn.Response:
    """
    GET /events - List events with filtering and pagination.
    
    Query Parameters:
        - page (int): Page number, default 1
        - per_page (int): Items per page, default 20, max 100
        - venue (str): Filter by venue name, default "All Venues"
        - search (str): Search in title/description
        - target_groups (str): Comma-separated target groups
        - source (str): Filter by source website
        - date (str): Filter by specific date (YYYY-MM-DD)
    
    Returns:
        JSON array of events with pagination info
    """
    try:
        db = DatabaseManager()
        
        # Parse query parameters
        page = int(req.args.get('page', 1))
        per_page = min(int(req.args.get('per_page', 20)), 100)  # Max 100
        venue = req.args.get('venue', 'All Venues')
        search = req.args.get('search', '')
        target_groups = req.args.get('target_groups', '')
        source = req.args.get('source', 'All Sources')
        filter_date = req.args.get('date', None)
        
        # Convert target_groups to list if provided
        target_group_list = None
        if target_groups:
            target_group_list = [t.strip() for t in target_groups.split(',')]
        
        # Fetch filtered events
        events = db.get_events_filtered(
            search=search,
            venue=venue,
            target_groups=target_group_list,
            source=source,
            page=page,
            per_page=per_page,
            filter_date=filter_date
        )
        
        return https_fn.Response(
            json.dumps({
                "success": True,
                "data": events,
                "page": page,
                "per_page": per_page
            }),
            content_type="application/json"
        )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({"success": False, "error": str(e)}),
            status=500,
            content_type="application/json"
        )


@https_fn.on_request(cors=cors_options)
def venues(req: https_fn.Request) -> https_fn.Response:
    """
    GET /venues - Get list of unique venue names.
    
    Returns:
        JSON array of venue names
    """
    try:
        db = DatabaseManager()
        venue_list = db.get_unique_venues()
        
        return https_fn.Response(
            json.dumps({
                "success": True,
                "data": venue_list
            }),
            content_type="application/json"
        )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({"success": False, "error": str(e)}),
            status=500,
            content_type="application/json"
        )


@https_fn.on_request(cors=cors_options)
def stats(req: https_fn.Request) -> https_fn.Response:
    """
    GET /stats - Get dashboard statistics.
    
    Returns:
        JSON object with:
        - total_events: Total number of events
        - events_this_week: Events happening this week
        - events_next_month: Events in next month
        - active_venues: Number of enabled venues
        - last_scrape: Last successful scrape timestamp
    """
    try:
        db = DatabaseManager()
        
        # Get various stats
        basic_stats = db.get_stats()
        events_this_week = db.get_events_this_week()
        events_next_month = db.get_events_next_month()
        active_venues = db.get_active_venues_count()
        last_scrape = db.get_last_scrape_info()
        
        return https_fn.Response(
            json.dumps({
                "success": True,
                "data": {
                    "total_events": basic_stats.get('total_events', 0),
                    "events_this_week": events_this_week,
                    "events_next_month": events_next_month,
                    "active_venues": active_venues,
                    "last_scrape": last_scrape,
                    "events_by_venue": db.get_events_by_venue(),
                    "events_by_target_group": db.get_events_by_target_group()
                }
            }),
            content_type="application/json"
        )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({"success": False, "error": str(e)}),
            status=500,
            content_type="application/json"
        )


@https_fn.on_request(cors=cors_options)
def sources(req: https_fn.Request) -> https_fn.Response:
    """
    GET /sources - Get list of unique source websites.
    
    Returns:
        JSON array of source names
    """
    try:
        db = DatabaseManager()
        source_list = db.get_unique_sources()
        
        return https_fn.Response(
            json.dumps({
                "success": True,
                "data": source_list
            }),
            content_type="application/json"
        )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({"success": False, "error": str(e)}),
            status=500,
            content_type="application/json"
        )


# ============================================================================
# ADMIN ENDPOINTS (For Admin Dashboard)
# ============================================================================

@https_fn.on_request(cors=cors_options)
def pending_changes(req: https_fn.Request) -> https_fn.Response:
    """
    GET /pending-changes - Get pending event changes for review.
    POST /pending-changes/approve - Approve all pending changes.
    POST /pending-changes/discard - Discard all pending changes.
    
    Returns:
        GET: JSON object with pending inserts and deletes
        POST: Success/failure status
    """
    try:
        db = DatabaseManager()
        
        if req.method == 'GET':
            changes = db.get_pending_changes()
            counts = db.get_pending_counts()
            
            return https_fn.Response(
                json.dumps({
                    "success": True,
                    "data": {
                        "inserts": changes.get('inserts', []),
                        "deletes": changes.get('deletes', []),
                        "insert_count": counts.get('inserts', 0),
                        "delete_count": counts.get('deletes', 0)
                    }
                }),
                content_type="application/json"
            )
            
        elif req.method == 'POST':
            # Check action parameter
            action = req.args.get('action', 'approve')
            
            if action == 'approve':
                result = db.apply_pending_changes()
                return https_fn.Response(
                    json.dumps({
                        "success": True,
                        "message": "All pending changes have been applied",
                        "applied": result
                    }),
                    content_type="application/json"
                )
            elif action == 'discard':
                db.discard_pending_changes()
                return https_fn.Response(
                    json.dumps({
                        "success": True,
                        "message": "All pending changes have been discarded"
                    }),
                    content_type="application/json"
                )
            else:
                return https_fn.Response(
                    json.dumps({"success": False, "error": f"Unknown action: {action}"}),
                    status=400,
                    content_type="application/json"
                )
        
        else:
            return https_fn.Response(
                json.dumps({"success": False, "error": "Method not allowed"}),
                status=405,
                content_type="application/json"
            )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({"success": False, "error": str(e)}),
            status=500,
            content_type="application/json"
        )


@https_fn.on_request(cors=cors_options)
def scraping_urls(req: https_fn.Request) -> https_fn.Response:
    """
    GET /scraping-urls - List all configured scraping URLs.
    
    Returns:
        JSON array of URLs with their status
    """
    try:
        db = DatabaseManager()
        urls = db.get_scraping_urls()
        
        return https_fn.Response(
            json.dumps({
                "success": True,
                "data": urls
            }),
            content_type="application/json"
        )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({"success": False, "error": str(e)}),
            status=500,
            content_type="application/json"
        )


@https_fn.on_request(cors=cors_options)
def scrape_logs(req: https_fn.Request) -> https_fn.Response:
    """
    GET /scrape-logs - Get recent scraping log entries.
    
    Query Parameters:
        - days (int): Number of days to look back, default 30
        - status (str): Filter by status (success, failed, etc.)
    
    Returns:
        JSON array of log entries
    """
    try:
        db = DatabaseManager()
        
        days = int(req.args.get('days', 30))
        status_filter = req.args.get('status', 'All')
        
        logs = db.get_logs(days=days, status_filter=status_filter)
        
        return https_fn.Response(
            json.dumps({
                "success": True,
                "data": logs
            }),
            content_type="application/json"
        )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({"success": False, "error": str(e)}),
            status=500,
            content_type="application/json"
        )


@https_fn.on_request(cors=cors_options)
def scrape_failures(req: https_fn.Request) -> https_fn.Response:
    """
    GET /scrape-failures - Get active scraping failures.
    
    Returns:
        JSON array of active failures
    """
    try:
        db = DatabaseManager()
        failures = db.get_active_failures()
        
        return https_fn.Response(
            json.dumps({
                "success": True,
                "data": failures
            }),
            content_type="application/json"
        )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({"success": False, "error": str(e)}),
            status=500,
            content_type="application/json"
        )


# ============================================================================
# HEALTH CHECK
# ============================================================================

@https_fn.on_request(cors=cors_options)
def health(req: https_fn.Request) -> https_fn.Response:
    """
    GET /health - Health check endpoint.
    
    Returns:
        JSON object with status and timestamp
    """
    try:
        # Quick DB connectivity check
        db = DatabaseManager()
        db.get_stats()  # Simple query to verify connection
        
        return https_fn.Response(
            json.dumps({
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "database": "connected"
            }),
            content_type="application/json"
        )
        
    except Exception as e:
        return https_fn.Response(
            json.dumps({
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }),
            status=503,
            content_type="application/json"
        )
