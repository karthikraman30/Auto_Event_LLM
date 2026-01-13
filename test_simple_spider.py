#!/usr/bin/env python3
"""
Test script for the simplified spider on forskolor site
"""

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import sys
import os

# Add the project root to python path
sys.path.append('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM')

# Import our simple spider
from simple_spider import SimpleEventSpider

if __name__ == "__main__":
    print("Testing simplified spider on forskolor site...")
    
    # Get Scrapy settings
    settings = get_project_settings()
    
    # Update settings for our test
    settings.set('ROBOTSTXT_OBEY', False)
    settings.set('DOWNLOAD_DELAY', 2)
    settings.set('CONCURRENT_REQUESTS', 1)
    
    # Create crawler process
    process = CrawlerProcess(settings)
    
    # Add our spider with only forskolar URL
    process.crawl(SimpleEventSpider)
    
    # Run the spider
    process.start()
