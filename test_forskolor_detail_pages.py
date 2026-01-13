#!/usr/bin/env python3
"""
Test script to verify if forskolor scraping visits detail pages for descriptions.
This will show in real-time what pages are being visited.
"""
import subprocess
import sys
import os

# Add verbose logging to see detail page visits
os.environ['SCRAPY_LOG_LEVEL'] = 'INFO'

print("=" * 80)
print("🔍 TESTING FORSKOLOR SCRAPING - Detail Page Visit Verification")
print("=" * 80)
print("\nThis test will show you:")
print("  ✓ When listing page is processed")
print("  ✓ When each detail page is visited (if any)")
print("  ✓ Total time taken")
print("\nWatch for log messages like:")
print("  - 'Fetching details for...' = Detail page is being visited")
print("  - 'Extracting details from:' = Processing detail page")
print("\n" + "=" * 80 + "\n")

# Run scrapy with the forskolor URL only
cmd = [
    'scrapy', 'crawl', 'universal_events',
    '-a', 'url=https://biblioteket.stockholm.se/forskolor',
    '-L', 'INFO',  # INFO level to see important messages
    '-s', 'LOG_FORMAT=%(levelname)s: %(message)s',  # Cleaner log format
]

print("Running command:", ' '.join(cmd))
print("\n" + "=" * 80 + "\n")

# Change to the scrapy project directory
os.chdir('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/event_category')

# Run the command
try:
    result = subprocess.run(cmd, capture_output=False, text=True)
    sys.exit(result.returncode)
except KeyboardInterrupt:
    print("\n\n⚠️  Test interrupted by user")
    sys.exit(1)
