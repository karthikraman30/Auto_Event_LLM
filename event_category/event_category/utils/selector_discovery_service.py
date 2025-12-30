#!/usr/bin/env python3
"""
Selector Discovery Service

Provides AI-based selector discovery for new websites during scraping.
Integrates AutoSelectorDiscovery with the spider and database.
"""
import asyncio
from typing import Dict, Optional
from playwright.async_api import async_playwright
from event_category.utils.auto_selector_discovery import AutoSelectorDiscovery
from event_category.utils.db_manager import DatabaseManager


class SelectorDiscoveryService:
    """
    Service to discover and cache CSS selectors for new websites using AI
    """
    
    def __init__(self, ai_client, logger, db_manager=None):
        """
        Initialize the discovery service
        
        Args:
            ai_client: Gemini AI client instance
            logger: Logger instance
            db_manager: DatabaseManager instance (optional, creates new if None)
        """
        self.discovery = AutoSelectorDiscovery(ai_client, logger)
        self.logger = logger
        self.db = db_manager or DatabaseManager()
    
    async def fetch_page_html_async(self, url: str) -> Optional[str]:
        """
        Fetch HTML content from URL using Playwright
        
        Args:
            url: Website URL to fetch
            
        Returns:
            HTML content as string, or None if failed
        """
        try:
            self.logger.info(f"Fetching HTML from: {url}")
            
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                page = await browser.new_page()
                
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)  # Allow JS to render
                
                html_content = await page.content()
                await browser.close()
                
                self.logger.info(f"Fetched {len(html_content)} bytes from {url}")
                return html_content
                
        except Exception as e:
            self.logger.error(f"Failed to fetch HTML from {url}: {e}")
            return None
    
    def fetch_page_html_sync(self, url: str) -> Optional[str]:
        """
        Synchronous wrapper for fetch_page_html_async
        
        Args:
            url: Website URL to fetch
            
        Returns:
            HTML content as string, or None if failed
        """
        return asyncio.run(self.fetch_page_html_async(url))
    
    def discover_selectors(self, url: str, html_content: str) -> Dict:
        """
        Discover CSS selectors for a website using AI
        
        Args:
            url: Website URL
            html_content: HTML content of the page
            
        Returns:
            Dictionary with:
                - success: bool
                - selectors: Dict (if successful)
                - confidence: float (if successful)
                - error: str (if failed)
        """
        try:
            self.logger.info(f"🔍 Starting AI selector discovery for: {url}")
            
            # Run AI discovery
            discovery_result = self.discovery.discover_website_structure(html_content, url)
            
            if not discovery_result or not discovery_result.get('selectors'):
                self.logger.error("AI discovery returned no selectors")
                return {
                    'success': False,
                    'error': 'AI discovery returned empty result'
                }
            
            selectors = discovery_result.get('selectors', {})
            confidence = discovery_result.get('confidence', {}).get('overall', 0.0)
            
            self.logger.info(f"✅ Discovery complete with confidence: {confidence:.0%}")
            
            return {
                'success': True,
                'selectors': selectors,
                'confidence': confidence,
                'discovery_result': discovery_result
            }
            
        except Exception as e:
            self.logger.error(f"Selector discovery failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def save_selectors_to_db(self, url: str, selectors: Dict, confidence: float = None) -> bool:
        """
        Save discovered selectors to database
        
        Args:
            url: Website URL
            selectors: Selector configuration dictionary
            confidence: Discovery confidence score (0.0-1.0)
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            container = selectors.get('container')
            items = selectors.get('items', {})
            
            if not container:
                self.logger.error("Cannot save selectors: missing container")
                return False
            
            # Convert AI selector format to database format
            # AI format: items = {"field": {"selector": "...", "alternative": "..."}}
            # DB format: items = {"field": "..."}
            
            db_items = {}
            for field_name, field_config in items.items():
                if isinstance(field_config, dict):
                    # Extract primary selector
                    selector = field_config.get('selector')
                    if selector and selector != "null" and selector is not None:
                        db_items[field_name] = selector
                elif isinstance(field_config, str):
                    db_items[field_name] = field_config
            
            self.logger.info(f"Saving selectors to database for {url}")
            self.logger.debug(f"Container: {container}")
            self.logger.debug(f"Items: {db_items}")
            
            # Save to database
            self.db.save_selectors(url, container, db_items)
            
            self.logger.info(f"💾 Selectors saved successfully (confidence: {confidence:.0%})" if confidence else "💾 Selectors saved successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save selectors to database: {e}", exc_info=True)
            return False
    
    def discover_and_save(self, url: str, html_content: str) -> Dict:
        """
        Complete workflow: discover selectors and save to database
        
        Args:
            url: Website URL
            html_content: HTML content of the page
            
        Returns:
            Dictionary with:
                - success: bool
                - selectors: Dict (if successful)
                - confidence: float (if successful)
                - saved: bool (if successful)
                - error: str (if failed)
        """
        # Step 1: Discover selectors
        discovery_result = self.discover_selectors(url, html_content)
        
        if not discovery_result['success']:
            return discovery_result
        
        selectors = discovery_result['selectors']
        confidence = discovery_result.get('confidence', 0.0)
        
        # Step 2: Save to database (only if confidence > 0.3) - TEMPORARILY LOWERED
        if confidence > 0.3:
            saved = self.save_selectors_to_db(url, selectors, confidence)
            discovery_result['saved'] = saved
        else:
            self.logger.warning(f"⚠️ Confidence too low ({confidence:.0%}), not saving to database")
            discovery_result['saved'] = False
            discovery_result['warning'] = f"Confidence too low: {confidence:.0%}"
        
        return discovery_result
    
    def get_or_discover_selectors(self, url: str, html_content: str = None) -> Optional[Dict]:
        """
        Get selectors from database, or discover if not found
        
        Args:
            url: Website URL
            html_content: HTML content (optional, will fetch if needed)
            
        Returns:
            Selectors dictionary compatible with spider, or None if failed
        """
        # Try to get from database first
        selectors = self.db.get_selectors(url)
        
        if selectors:
            self.logger.info(f"✅ Using cached selectors from database for {url}")
            return selectors
        
        # Not in database, trigger discovery
        self.logger.info(f"🔍 No cached selectors found, triggering AI discovery for {url}")
        
        # Fetch HTML if not provided
        if not html_content:
            html_content = self.fetch_page_html_sync(url)
            if not html_content:
                self.logger.error("Failed to fetch HTML for discovery")
                return None
        
        # Discover and save
        result = self.discover_and_save(url, html_content)
        
        if result['success'] and result.get('saved'):
            # Return selectors in format compatible with spider
            return {
                'container': result['selectors'].get('container'),
                'items': self._convert_to_spider_format(result['selectors'].get('items', {}))
            }
        
        return None
    
    def _convert_to_spider_format(self, items: Dict) -> Dict:
        """
        Convert AI selector format to spider-compatible format
        
        Args:
            items: Items dictionary from AI discovery
            
        Returns:
            Spider-compatible items dictionary
        """
        spider_items = {}
        for field_name, field_config in items.items():
            if isinstance(field_config, dict):
                selector = field_config.get('selector')
                if selector and selector != "null" and selector is not None:
                    spider_items[field_name] = selector
            elif isinstance(field_config, str):
                spider_items[field_name] = field_config
        return spider_items
