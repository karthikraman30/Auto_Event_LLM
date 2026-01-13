"""Quick test script to test Stockholm library handler"""
import asyncio
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from event_category.spiders.universal_spider import UnifiedEventSpider

async def main():
    # Set up the spider
    settings = get_project_settings()
    settings.set('LOG_LEVEL', 'INFO')
    
    process = CrawlerProcess(settings)
    process.crawl(UnifiedEventSpider, url='https://biblioteket.stockholm.se/evenemang')
    process.start()

if __name__ == '__main__':
    asyncio.run(main())
