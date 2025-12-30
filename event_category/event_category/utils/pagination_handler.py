"""
Generic Pagination Handler for new/unknown websites

Detects and clicks load more buttons, numbered pagination, and other pagination mechanisms
to load all available events before extraction.

Used ONLY for new sites that don't have specific handlers.
Existing sites (Skansen, Tekniska, Moderna, Armemuseum) use their own pagination logic.
"""

import asyncio


class PaginationHandler:
    """Handles pagination for unknown websites"""
    
    def __init__(self, logger):
        self.logger = logger
        self.max_clicks = 10  # Safety limit - sufficient for 1 month of events (~5-6 clicks needed)
        
    async def handle_pagination(self, page, max_iterations=None):
        """
        Try to load all pages/events by clicking load more buttons or pagination links.
        
        Args:
            page: Playwright page object
            max_iterations: Maximum times to try loading more (default: 5)
            
        Returns:
            int: Number of pagination actions performed
        """
        max_iterations = max_iterations or self.max_clicks
        pagination_count = 0
        
        # Strategy 1: Look for "Load More" / "Visa mer" buttons
        self.logger.info("Attempting pagination - Strategy 1: Load More buttons")
        load_more_count = await self._click_load_more_buttons(page, max_iterations)
        pagination_count += load_more_count
        
        if pagination_count > 0:
            self.logger.info(f"✅ Loaded more content {pagination_count} times with load more buttons")
            return pagination_count
        
        # Strategy 2: Look for numbered pagination (Next, 1, 2, 3, etc.)
        self.logger.info("Attempting pagination - Strategy 2: Numbered pagination")
        next_count = await self._click_next_pagination(page, max_iterations)
        pagination_count += next_count
        
        if pagination_count > 0:
            self.logger.info(f"✅ Loaded {pagination_count} pages with numbered pagination")
            return pagination_count
        
        # Strategy 3: Look for URL-based pagination (detect and modify query params)
        self.logger.info("Attempting pagination - Strategy 3: URL-based pagination")
        url_count = await self._handle_url_pagination(page, max_iterations)
        pagination_count += url_count
        
        if pagination_count > 0:
            self.logger.info(f"✅ Loaded {pagination_count} pages with URL-based pagination")
            return pagination_count
        
        self.logger.debug("No pagination found on page")
        return 0
    
    async def _click_load_more_buttons(self, page, max_iterations):
        """Try clicking "Load More" / "Visa mer" style buttons"""
        
        # Load more button selectors in order of preference
        load_more_selectors = [
            # Swedish
            ('a.show-more-text', 'National Museum style'),
            ('a:text("Visa mer")', 'Visa mer link'),
            ('button:text("Visa mer")', 'Visa mer button'),
            
            # Spanish
            ('a:text("Cargar más")', 'Cargar más link'),
            ('button:text("Cargar más")', 'Cargar más button'),
            ('a:text("Ver más")', 'Ver más link'),
            ('button:text("Ver más")', 'Ver más button'),
            ('a:text("Mostrar más")', 'Mostrar más link'),
            ('button:text("Mostrar más")', 'Mostrar más button'),
            
            # English
            ('a:text("Load more")', 'Load more link'),
            ('button:text("Load more")', 'Load more button'),
            ('a:text("Show more")', 'Show more link'),
            ('button:text("Show more")', 'Show more button'),
            
            # Generic class-based
            ('a[class*="show-more"]', 'Generic show-more class'),
            ('button[class*="show-more"]', 'Generic show-more button'),
            ('a[class*="load-more"]', 'Generic load-more class'),
            ('button[class*="load-more"]', 'Generic load-more button'),
            ('div.show-more', 'Div with show-more class'),
        ]
        
        clicks = 0
        
        for selector, description in load_more_selectors:
            attempts = 0
            
            while attempts < max_iterations:
                try:
                    btn = page.locator(selector).first
                    count = await page.locator(selector).count()
                    
                    if count == 0:
                        break  # No button found, try next selector
                    
                    # Check if button is visible and enabled
                    try:
                        is_visible = await btn.is_visible(timeout=500)
                    except:
                        is_visible = False
                    
                    if not is_visible:
                        break  # Button not visible, try next selector
                    
                    # Click the button
                    self.logger.info(f"Clicking '{description}' ({selector})")
                    await btn.click(force=True)
                    await page.wait_for_timeout(2000)  # Wait for new content to load
                    
                    clicks += 1
                    attempts += 1
                    
                except Exception as e:
                    self.logger.debug(f"Error with '{description}': {type(e).__name__}")
                    break
            
            if clicks > 0:
                self.logger.info(f"✅ Successfully used '{description}' - {clicks} clicks")
                return clicks
        
        return clicks
    
    async def _click_next_pagination(self, page, max_iterations):
        """Try clicking Next page buttons or numbered pagination"""
        
        pagination_selectors = [
            # Spanish
            ('a:text("Siguiente")', 'Siguiente (Next in Spanish)'),
            ('button:text("Siguiente")', 'Siguiente button'),
            ('a[aria-label="Siguiente"]', 'Aria label Siguiente'),
            
            # English
            ('a:text("Next")', 'Next link'),
            ('button:text("Next")', 'Next button'),
            ('a[aria-label="Next"]', 'Aria label Next'),
            
            # Numbered pagination - find highest numbered page and click
            ('a[data-page]', 'Data-page attribute'),
            ('a.pagination-link', 'Pagination link class'),
        ]
        
        clicks = 0
        
        for selector, description in pagination_selectors:
            attempts = 0
            last_page = None
            
            while attempts < max_iterations:
                try:
                    # Find all pagination links
                    links = await page.locator(selector).all()
                    
                    if not links:
                        break
                    
                    # Try to find and click the "next" or highest numbered link
                    clicked = False
                    
                    for link in links:
                        try:
                            text = await link.inner_text()
                            is_visible = await link.is_visible(timeout=500)
                            
                            if is_visible and text.strip() and text.strip() not in [last_page, '...']:
                                self.logger.info(f"Clicking pagination: '{description}' - {text}")
                                await link.click(force=True)
                                await page.wait_for_timeout(2000)
                                last_page = text
                                clicks += 1
                                attempts += 1
                                clicked = True
                                break
                        except:
                            continue
                    
                    if not clicked:
                        break
                    
                except Exception as e:
                    self.logger.debug(f"Error with '{description}': {type(e).__name__}")
                    break
            
            if clicks > 0:
                return clicks
        
        return clicks
    
    async def _handle_url_pagination(self, page, max_iterations):
        """Try to detect and modify URL-based pagination parameters"""
        
        # This is less reliable but can work for simple page=1,2,3 patterns
        current_url = page.url
        clicks = 0
        
        # Check for common pagination parameters
        pagination_params = ['page', 'p', 'offset', 'start', 'position']
        
        for param in pagination_params:
            if f'{param}=' in current_url:
                self.logger.info(f"Detected URL pagination parameter: {param}")
                
                # Try incrementing the parameter
                for page_num in range(2, max_iterations + 2):
                    try:
                        # Simple replacement - might not work for all cases
                        # This is a basic approach; more sophisticated regex could be used
                        import re
                        new_url = re.sub(
                            f'{param}=\\d+',
                            f'{param}={page_num}',
                            current_url
                        )
                        
                        if new_url == current_url:
                            # No substitution happened
                            break
                        
                        self.logger.info(f"Loading page {page_num}: {new_url}")
                        await page.goto(new_url, wait_until='networkidle', timeout=30000)
                        await page.wait_for_timeout(1000)
                        
                        clicks += 1
                    except Exception as e:
                        self.logger.debug(f"Error loading page {page_num}: {type(e).__name__}")
                        break
                
                if clicks > 0:
                    return clicks
        
        return clicks
