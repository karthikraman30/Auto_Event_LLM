# Cloud Run Scraper

This directory contains the Cloud Run containerized scraper for scheduled event scraping.

## Overview

Cloud Run is used for long-running scraping jobs because:
- **No timeout limit** (unlike Cloud Functions which has a 60 min max)
- **Scales to zero** when not in use (cost-effective)
- **Triggered by Cloud Scheduler** on a schedule

## Scrapers

| Scraper | Schedule | Cron | Description |
|---------|----------|------|-------------|
| **Baseline** | 1st of month at 6 AM | `0 6 1 * *` | Full scrape of all events |
| **Incremental** | Every 3 days at 8 AM | `0 8 */3 * *` | Quick check for changes |

## Local Testing

### Using Docker:

```bash
# Build the image
docker build -t event-scraper .

# Run locally
docker run -p 8080:8080 \
  -e SUPABASE_URL="your_url" \
  -e SUPABASE_KEY="your_key" \
  event-scraper

# Test endpoints
curl http://localhost:8080/health
curl -X POST http://localhost:8080 -H "Content-Type: application/json" -d '{"mode": "incremental"}'
```

### Without Docker:

```bash
# From project root
cd cloud-run-scraper
pip install -r requirements.txt
python main.py

# In another terminal
curl -X POST http://localhost:8080 -d '{"mode": "incremental"}'
```

## Deployment

### Option 1: Using Cloud Build (Recommended)

```bash
gcloud builds submit --config cloudbuild.yaml \
  --substitutions=_SUPABASE_URL="your_url",_SUPABASE_KEY="your_key"
```

### Option 2: Direct Deploy

```bash
gcloud run deploy event-scraper \
  --source . \
  --region us-central1 \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600s \
  --concurrency 1 \
  --set-env-vars "SUPABASE_URL=your_url,SUPABASE_KEY=your_key"
```

## Setting Up Cloud Scheduler

After deployment, set up Cloud Scheduler jobs:

### Baseline (Monthly)

```bash
gcloud scheduler jobs create http baseline-scraper \
  --schedule="0 6 1 * *" \
  --uri="https://YOUR-SERVICE-URL" \
  --http-method=POST \
  --message-body='{"mode": "baseline"}' \
  --headers="Content-Type=application/json" \
  --time-zone="Europe/Stockholm"
```

### Incremental (Every 3 Days)

```bash
gcloud scheduler jobs create http incremental-scraper \
  --schedule="0 8 */3 * *" \
  --uri="https://YOUR-SERVICE-URL" \
  --http-method=POST \
  --message-body='{"mode": "incremental"}' \
  --headers="Content-Type=application/json" \
  --time-zone="Europe/Stockholm"
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Your Supabase project URL |
| `SUPABASE_KEY` | Yes | Supabase service role key |
| `GEMINI_API_KEY` | No | For LLM-based extraction |
| `PORT` | No | Server port (default: 8080) |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check / status |
| `/` | POST | Trigger scraper (body: `{"mode": "baseline"}` or `{"mode": "incremental"}`) |
| `/health` | GET | Simple health check |
