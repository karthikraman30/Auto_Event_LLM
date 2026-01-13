#!/usr/bin/env python3
"""
Test script for scraping Stockholm Library forskolor site
"""

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import sys
import os

# Add the project root to python path
sys.path.append('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM')

from event_category.event_category.spiders.universal_spider import MultiSiteEventSpider

# Test only the forskolor site
class ForskolorTestSpider(MultiSiteEventSpider):
    name = "forskolor_test"
    
    # Override start_urls to only test forskolor
    start_urls = [
        "https://biblioteket.stockholm.se/forskolor",
    ]

if __name__ == "__main__":
    print("Testing forskolor site scraping...")
    
    # Get Scrapy settings
    settings = get_project_settings()
    
    # Update settings for our test
    settings.set('ROBOTSTXT_OBEY', False)
    settings.set('DOWNLOAD_DELAY', 2)
    settings.set('CONCURRENT_REQUESTS', 1)
    
    # Create crawler process
    process = CrawlerProcess(settings)
    
    # Add our test spider
    process.crawl(ForskolorTestSpider)
    
    # Run the spider
    process.start()
