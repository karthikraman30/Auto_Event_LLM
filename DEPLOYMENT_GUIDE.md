# Firebase + Cloud Run Deployment Guide

This guide walks you through deploying the Event Scraper backend to Firebase Cloud Functions and Cloud Run.

---

## 📁 Project Structure

After setup, your project will have:

```
Auto_Event_LLM/
├── functions/                    # Firebase Cloud Functions
│   ├── main.py                   # API endpoints
│   ├── requirements.txt          # Dependencies
│   └── .env.example              # Environment template
│
├── cloud-run-scraper/            # Cloud Run Scraper
│   ├── main.py                   # HTTP entry point
│   ├── Dockerfile                # Container definition
│   ├── requirements.txt          # Dependencies
│   ├── cloudbuild.yaml           # Deployment config
│   └── README.md                 # Cloud Run docs
│
├── event_category/               # Your existing scraping code
└── run_parallel.py               # Parallel scraper orchestrator
```

---

## 🚀 Phase 1: Firebase Setup

### Step 1.1: Get Firebase Access

Your friend will:
1. Add you to the Firebase project
2. You'll receive an email invitation
3. Accept and verify you can access [Firebase Console](https://console.firebase.google.com)

### Step 1.2: Install Firebase CLI

```bash
# Install Firebase CLI globally
npm install -g firebase-tools

# Login with your Google account
firebase login
```

### Step 1.3: Initialize Firebase in Project

```bash
cd /Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM

# Initialize Firebase Functions (Python)
firebase init functions

# When prompted:
# - Select "Python" as the language
# - Say "No" to overwriting main.py (we already created it)
# - Say "Yes" to installing dependencies
```

### Step 1.4: Set Environment Variables

In Firebase Console → Functions → Configuration:

```bash
# Or via CLI
firebase functions:config:set supabase.url="YOUR_SUPABASE_URL"
firebase functions:config:set supabase.key="YOUR_SUPABASE_KEY"
```

### Step 1.5: Deploy Cloud Functions

```bash
firebase deploy --only functions
```

After deployment, you'll get URLs like:
```
https://us-central1-YOUR-PROJECT.cloudfunctions.net/events
https://us-central1-YOUR-PROJECT.cloudfunctions.net/venues
https://us-central1-YOUR-PROJECT.cloudfunctions.net/stats
```

---

## 🐳 Phase 2: Cloud Run Setup

### Step 2.1: Enable Cloud Run API

```bash
gcloud services enable run.googleapis.com
```

### Step 2.2: Build and Deploy

**Option A: Using Cloud Build (Recommended)**

```bash
cd /Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM

# Deploy with environment variables
gcloud builds submit --config cloud-run-scraper/cloudbuild.yaml \
  --substitutions=_SUPABASE_URL="your_url",_SUPABASE_KEY="your_key"
```

**Option B: Direct Deploy**

```bash
gcloud run deploy event-scraper \
  --source . \
  --dockerfile cloud-run-scraper/Dockerfile \
  --region us-central1 \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600s \
  --concurrency 1 \
  --set-env-vars "SUPABASE_URL=your_url,SUPABASE_KEY=your_key"
```

### Step 2.3: Get the Cloud Run URL

After deployment:
```bash
gcloud run services describe event-scraper --region=us-central1 --format='value(status.url)'
```

You'll get something like:
```
https://event-scraper-abc123-uc.a.run.app
```

---

## ⏰ Phase 3: Cloud Scheduler Setup

### Step 3.1: Enable Cloud Scheduler

```bash
gcloud services enable cloudscheduler.googleapis.com
```

### Step 3.2: Create Baseline Scraper Job (Monthly)

```bash
gcloud scheduler jobs create http baseline-scraper \
  --schedule="0 6 1 * *" \
  --uri="https://YOUR-CLOUD-RUN-URL" \
  --http-method=POST \
  --message-body='{"mode": "baseline"}' \
  --headers="Content-Type=application/json" \
  --time-zone="Europe/Stockholm" \
  --description="Monthly baseline event scraper (1st of month at 6 AM)"
```

### Step 3.3: Create Incremental Scraper Job (Every 3 Days)

```bash
gcloud scheduler jobs create http incremental-scraper \
  --schedule="0 8 */3 * *" \
  --uri="https://YOUR-CLOUD-RUN-URL" \
  --http-method=POST \
  --message-body='{"mode": "incremental"}' \
  --headers="Content-Type=application/json" \
  --time-zone="Europe/Stockholm" \
  --description="Incremental scraper (every 3 days at 8 AM)"
```

### Step 3.4: Test the Jobs

```bash
# Manually trigger a job
gcloud scheduler jobs run incremental-scraper --location=us-central1
```

---

## 🧪 Local Testing

### Test Cloud Functions Locally

```bash
cd /Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM

# Install dependencies
pip install firebase-functions firebase-admin

# Start emulator
firebase emulators:start --only functions
```

### Test Cloud Run Locally (with Docker)

```bash
cd /Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM

# Build the container
docker build -f cloud-run-scraper/Dockerfile -t event-scraper .

# Run locally
docker run -p 8080:8080 \
  -e SUPABASE_URL="your_url" \
  -e SUPABASE_KEY="your_key" \
  event-scraper

# Test in another terminal
curl http://localhost:8080/health
curl -X POST http://localhost:8080 -H "Content-Type: application/json" -d '{"mode": "incremental"}'
```

### Test Cloud Run Locally (without Docker)

```bash
cd /Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/cloud-run-scraper

# Copy .env file from parent
cp ../.env ./.env

# Install dependencies
pip install -r requirements.txt

# Run Flask server
python main.py

# Test
curl http://localhost:8080/health
```

---

## 📋 API Endpoints Reference

### Frontend Endpoints (Cloud Functions)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/events` | GET | List events (paginated, filtered) |
| `/venues` | GET | Get venue names |
| `/sources` | GET | Get source websites |
| `/stats` | GET | Dashboard statistics |
| `/pending-changes` | GET | Get pending approvals |
| `/pending-changes?action=approve` | POST | Approve all changes |
| `/pending-changes?action=discard` | POST | Discard all changes |
| `/scraping-urls` | GET | List configured URLs |
| `/scrape-logs` | GET | Get scraping logs |
| `/scrape-failures` | GET | Get active failures |
| `/health` | GET | Health check |

### Scraper Endpoints (Cloud Run)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Service status |
| `/` | POST | Trigger scraper (`{"mode": "baseline"}` or `{"mode": "incremental"}`) |
| `/health` | GET | Health check |

---

## 🔧 Troubleshooting

### Cloud Functions "Module not found" Error

Make sure the import path is correct in `functions/main.py`:
```python
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from event_category.event_category.utils.db_manager import DatabaseManager
```

### Cloud Run Timeout Error

Increase timeout:
```bash
gcloud run services update event-scraper --timeout=3600s
```

### Environment Variables Not Found

Check in Cloud Console or via CLI:
```bash
gcloud run services describe event-scraper --format='yaml(spec.template.spec.containers[0].env)'
```

---

## ✅ Deployment Checklist

- [ ] Firebase access received
- [ ] Firebase CLI installed and logged in
- [ ] Cloud Functions deployed
- [ ] Cloud Run container deployed
- [ ] Cloud Scheduler jobs created
- [ ] Environment variables set (SUPABASE_URL, SUPABASE_KEY)
- [ ] Health endpoints tested
- [ ] Incremental scraper tested
- [ ] URLs shared with frontend developer

---

## 📧 Share with Your Friend

After deployment, share these URLs:

```
Firebase Cloud Functions Base URL:
https://us-central1-YOUR-PROJECT.cloudfunctions.net

Available Endpoints:
  GET  /events?page=1&venue=Skansen
  GET  /venues
  GET  /sources
  GET  /stats
  GET  /pending-changes
  POST /pending-changes?action=approve
  GET  /scraping-urls
  GET  /scrape-logs
  GET  /health
```
