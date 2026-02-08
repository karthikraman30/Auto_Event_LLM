"""
Cloud Run Scraper Entry Point.

This is the main entry point for the Cloud Run container that runs the scrapers.
It exposes an HTTP endpoint that Cloud Scheduler can call to trigger scraping.

Usage:
    - Cloud Scheduler calls: POST / with {"mode": "baseline"} or {"mode": "incremental"}
    - Manually test: curl -X POST http://localhost:8080 -d '{"mode": "incremental"}'
"""
import os
import sys
import json
import logging
from datetime import datetime
from flask import Flask, request, jsonify

# Add parent path to import existing modules (run_parallel.py and event_category/)
# This works both locally (from cloud-run-scraper/) and in Docker container
parent_dir = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, parent_dir)
sys.path.insert(0, os.path.join(parent_dir, 'event_category'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def run_baseline_scraper():
    """
    Run the monthly baseline scraper.
    
    - Date range: 1st of current month → 5th of next month
    - Direct upsert to events table (source of truth)
    - Updates last_baseline_run timestamp
    """
    logger.info("Starting BASELINE scraper...")
    
    try:
        from run_parallel import main as run_scraper
        
        result = run_scraper(run_type='baseline')
        
        logger.info(f"Baseline scraper completed: {result}")
        return {
            "success": True,
            "mode": "baseline",
            "events_scraped": result.get('events', 0),
            "failures": result.get('failures', 0),
            "warnings": result.get('warnings', []),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Baseline scraper failed: {e}")
        return {
            "success": False,
            "mode": "baseline",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


def run_incremental_scraper():
    """
    Run the incremental (every 3 days) scraper.
    
    - Date range: today → today + 6 days
    - Compares scraped events against DB
    - Stages changes for review (pending_events table)
    - Updates last_incremental_run timestamp
    """
    logger.info("Starting INCREMENTAL scraper...")
    
    try:
        from run_parallel import main as run_scraper
        
        result = run_scraper(run_type='incremental')
        
        logger.info(f"Incremental scraper completed: {result}")
        return {
            "success": True,
            "mode": "incremental",
            "events_scraped": result.get('events', 0),
            "failures": result.get('failures', 0),
            "staged_inserts": result.get('staged_inserts', 0),
            "staged_deletes": result.get('staged_deletes', 0),
            "warnings": result.get('warnings', []),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Incremental scraper failed: {e}")
        return {
            "success": False,
            "mode": "incremental",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@app.route('/', methods=['GET', 'POST'])
def handle_request():
    """
    Main endpoint for Cloud Scheduler triggers.
    
    GET: Health check / status
    POST: Trigger scraper based on 'mode' parameter
    
    Example POST body:
        {"mode": "baseline"}   - Run monthly baseline scraper
        {"mode": "incremental"} - Run every-3-days incremental scraper
    """
    if request.method == 'GET':
        return jsonify({
            "status": "healthy",
            "service": "event-scraper",
            "modes": ["baseline", "incremental"],
            "timestamp": datetime.utcnow().isoformat()
        })
    
    # POST request - trigger scraper
    try:
        # Parse request body
        data = request.get_json(silent=True) or {}
        mode = data.get('mode', request.args.get('mode', 'incremental'))
        
        logger.info(f"Received scrape request with mode: {mode}")
        
        if mode == 'baseline':
            result = run_baseline_scraper()
        elif mode == 'incremental':
            result = run_incremental_scraper()
        else:
            return jsonify({
                "success": False,
                "error": f"Unknown mode: {mode}. Valid modes: baseline, incremental"
            }), 400
        
        status_code = 200 if result.get('success') else 500
        return jsonify(result), status_code
        
    except Exception as e:
        logger.exception(f"Unhandled error in scraper: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat()
    })


if __name__ == '__main__':
    # Run locally for testing
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting Cloud Run Scraper on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
