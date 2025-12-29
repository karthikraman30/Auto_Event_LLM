"""
DOM-Rich JSON Lines Pipeline

Exports raw HTML blocks to JSON Lines format for new websites (without selectors).
This enables better AI training and offline selector discovery.
"""
import json
from datetime import datetime
from itemadapter import ItemAdapter


class DomRichJsonPipeline:
    """
    Export DOM-rich JSON Lines for new websites (no selectors in DB)
    Only activates when html_block field is present
    
    Output format (.jl file):
    {
        "url": "https://example.com/event/123",
        "html_block": "<article>...</article>",
        "container_selector": "article.event-card",
        "metadata": {
            "scraped_at": "2025-12-28T23:00:00",
            "source_url": "https://example.com/events",
            "discovery_needed": true
        }
    }
    """
    
    def open_spider(self, spider):
        """Initialize JSON Lines file on spider start"""
        self.file = open('dom_rich_events.jl', 'w', encoding='utf-8')
        self.items_count = 0
        spider.logger.info("DOM-Rich JSON Pipeline activated")
    
    def close_spider(self, spider):
        """Close file and log summary on spider close"""
        self.file.close()
        if self.items_count > 0:
            spider.logger.info(f"📦 Exported {self.items_count} HTML blocks to dom_rich_events.jl")
        else:
            spider.logger.info("No HTML blocks exported (all sites have cached selectors)")
    
    def process_item(self, item, spider):
        """
        Process item and export if it contains HTML block
        
        Args:
            item: Scrapy item (EventCategoryItem)
            spider: Spider instance
            
        Returns:
            item: Pass through to next pipeline
        """
        adapter = ItemAdapter(item)
        
        # Only export if html_block exists (new website without selectors)
        html_block = adapter.get('html_block')
        if html_block:
            # Extract source URL from spider
            source_url = ''
            if hasattr(spider, 'url'):
                source_url = spider.url  # Single URL mode
            elif hasattr(spider, 'start_urls') and spider.start_urls:
                source_url = spider.start_urls[0]
            
            dom_data = {
                'url': adapter.get('event_url', ''),
                'html_block': html_block,
                'container_selector': adapter.get('container_selector', 'unknown'),
                'metadata': {
                    'scraped_at': datetime.now().isoformat(),
                    'source_url': source_url,
                    'discovery_needed': True,
                    'event_name': adapter.get('event_name', ''),  # For reference
                }
            }
            
            # Write as single line JSON
            line = json.dumps(dom_data, ensure_ascii=False)
            self.file.write(line + '\n')
            self.items_count += 1
        
        return item  # Pass through to Excel pipeline
