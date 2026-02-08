# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install minimal system dependencies
# Playwright's --with-deps handles browser dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libffi-dev \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY cloud-run-scraper/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (only Chromium to save space)
RUN pip install playwright && playwright install chromium --with-deps

# Copy the Cloud Run entry point
COPY cloud-run-scraper/main.py ./main.py

# Copy the entire event_category module (scraping code)
COPY event_category/ ./event_category/

# Copy the parallel scraper orchestrator
COPY run_parallel.py ./run_parallel.py

# Copy .env if it exists (for local testing)
# In production, use Cloud Run environment variables
COPY .env* ./

# Set environment variables
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8080

# Run with gunicorn for production (more robust than Flask dev server)
CMD exec gunicorn --bind :$PORT --workers 1 --threads 2 --timeout 3600 main:app
