import json
from datetime import datetime
from typing import Dict, List, Optional


class ManualSelectorManager:
    """
    Manages manual selector input when AI discovery fails.
    Allows users to inspect page HTML and provide selectors manually.
    Stores selectors in database with metadata.
    """
    
    def __init__(self, db_manager, logger):
        self.db = db_manager
        self.logger = logger
    
    def prompt_for_manual_selectors(self, url: str, ai_result: Dict = None) -> Optional[Dict]:
        """
        Interactive prompt to get manual selectors from user.
        
        Args:
            url: Website URL that failed AI discovery
            ai_result: Optional AI discovery result to show what it tried
            
        Returns:
            Dictionary with manual selectors, or None if user cancels
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("MANUAL SELECTOR INPUT")
        self.logger.info("="*80)
        self.logger.info(f"\nURL: {url}")
        
        if ai_result:
            ai_selectors = ai_result.get('selectors', {})
            if ai_selectors:
                self.logger.info("\nAI-Discovered Selectors (FAILED VALIDATION):")
                self._display_selectors(ai_selectors)
        
        self.logger.info("\n" + "-"*80)
        self.logger.info("INSTRUCTIONS:")
        self.logger.info("-"*80)
        self.logger.info("""
1. Inspect the website HTML in your browser (F12 Developer Tools)
2. Find the CSS selectors for each field
3. Container: The element that wraps each individual event (e.g., 'div.event-card', 'article.listing')
4. Fields: Relative selectors within the container (e.g., 'h2.title', 'span.date')
5. Leave empty if field doesn't exist on page
6. Type 'skip' to skip this URL (no selectors saved)
7. Type 'json' to input full JSON structure

Examples of selectors:
  - By tag: 'h2', 'div', 'article'
  - By class: '.event-card', 'div.calendar-item'
  - By ID: '#event-title'
  - By attribute: '[data-date]', '[data-event-id]'
  - Combining: 'div.card > h2.title'
  - Attribute value: 'a[href*="event"]'
        """)
        
        manual_selectors = self._get_selector_input(url)
        
        if not manual_selectors:
            self.logger.warning("No selectors provided. Skipping.")
            return None
        
        # Validate structure
        if not self._validate_manual_selector_structure(manual_selectors):
            self.logger.error("Invalid selector structure. Aborting.")
            return None
        
        # Save to database
        success = self.save_manual_selectors(url, manual_selectors)
        
        if success:
            self.logger.info(f"✅ Manual selectors saved for {url}")
            return manual_selectors
        else:
            self.logger.error(f"❌ Failed to save selectors for {url}")
            return None
    
    def _get_selector_input(self, url: str) -> Optional[Dict]:
        """Interactive input for selectors"""
        
        print("\n" + "="*80)
        print("ENTER SELECTORS FOR THIS WEBSITE")
        print("="*80)
        
        # Option 1: JSON input
        print("\nChoice 1: Enter full JSON structure")
        print("Choice 2: Enter selectors field by field")
        print("Choice 3: Skip this URL\n")
        
        choice = input("Select option (1-3): ").strip()
        
        if choice == '1':
            return self._input_json_selectors()
        elif choice == '2':
            return self._input_field_by_field()
        elif choice == '3':
            return None
        else:
            print("Invalid choice. Skipping.")
            return None
    
    def _input_json_selectors(self) -> Optional[Dict]:
        """Let user paste full JSON"""
        print("\nPaste your JSON selector structure (Ctrl+D or Ctrl+Z to finish):")
        print('Expected format:')
        print('''{
  "container": "div.event-card",
  "items": {
    "event_name": "h2.title",
    "date_iso": "[data-date]",
    "time": "span.time",
    "location": "span.venue",
    "description": "p.desc",
    "target_group": "span.audience",
    "status": "[data-status]"
  }
}''')
        print("\nEnter JSON:")
        
        lines = []
        try:
            while True:
                line = input()
                lines.append(line)
        except EOFError:
            pass
        
        json_str = '\n'.join(lines)
        
        try:
            data = json.loads(json_str)
            return data
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON: {e}")
            return None
    
    def _input_field_by_field(self) -> Dict:
        """Get selectors one field at a time"""
        
        required_fields = [
            'event_name',
            'date_iso',
            'time',
            'location',
            'description',
            'target_group',
            'status'
        ]
        
        container = input("\nContainer selector (e.g., 'div.event', 'article.item'): ").strip()
        
        if not container:
            print("❌ Container selector is required.")
            return {}
        
        print("\nNow enter selectors for each field (leave empty if not present on page):\n")
        
        items = {}
        for field in required_fields:
            selector = input(f"  {field}: ").strip()
            if selector:
                items[field] = selector
            else:
                items[field] = None
        
        return {
            'container': container,
            'items': items
        }
    
    def _display_selectors(self, selectors: Dict, indent=2):
        """Pretty print selectors"""
        spaces = " " * indent
        
        container = selectors.get('container', '')
        print(f"{spaces}Container: {container}")
        
        items = selectors.get('items', {})
        if items:
            print(f"{spaces}Fields:")
            for field, sel in items.items():
                status = "✓" if sel and sel != "null" else "✗"
                selector_str = sel if sel and sel != "null" else "(empty)"
                print(f"{spaces}  {status} {field}: {selector_str}")
    
    def _validate_manual_selector_structure(self, selectors: Dict) -> bool:
        """Validate selector structure"""
        
        if not isinstance(selectors, dict):
            self.logger.error("Selectors must be a dictionary")
            return False
        
        if 'container' not in selectors:
            self.logger.error("Missing 'container' field")
            return False
        
        container = selectors['container']
        if not container or not isinstance(container, str):
            self.logger.error("Container must be a non-empty string")
            return False
        
        if 'items' not in selectors:
            self.logger.error("Missing 'items' field")
            return False
        
        items = selectors['items']
        if not isinstance(items, dict):
            self.logger.error("Items must be a dictionary")
            return False
        
        # At least some fields should be filled
        filled_fields = sum(1 for v in items.values() if v and v != "null")
        if filled_fields == 0:
            self.logger.error("At least one field selector must be provided")
            return False
        
        return True
    
    def save_manual_selectors(self, url: str, selectors: Dict) -> bool:
        """Save manual selectors to database"""
        try:
            # Add metadata
            selectors_with_metadata = {
                'container': selectors.get('container'),
                'items': selectors.get('items', {}),
                'metadata': {
                    'manual': True,
                    'discovered_at': datetime.now().isoformat(),
                    'method': 'manual_input',
                    'confidence': 1.0  # User-provided, assume 100% confidence
                }
            }
            
            # Save to database
            success = self.db.save_selectors(url, selectors_with_metadata)
            
            if success:
                self.logger.info(f"Saved manual selectors to database for {url}")
                self.logger.debug(f"Selectors: {selectors_with_metadata}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error saving selectors: {e}")
            return False
    
    def load_manual_selectors(self, url: str) -> Optional[Dict]:
        """Load manual selectors from database"""
        try:
            return self.db.get_selectors(url)
        except Exception as e:
            self.logger.warning(f"Error loading selectors for {url}: {e}")
            return None
    
    def list_manual_selectors(self) -> List[Dict]:
        """Get list of all manually-entered selectors"""
        try:
            return self.db.list_manual_selectors()
        except Exception as e:
            self.logger.error(f"Error listing selectors: {e}")
            return []
    
    def delete_manual_selectors(self, url: str) -> bool:
        """Delete selectors for a URL"""
        try:
            success = self.db.delete_selectors(url)
            if success:
                self.logger.info(f"Deleted selectors for {url}")
            return success
        except Exception as e:
            self.logger.error(f"Error deleting selectors: {e}")
            return False
    
    def review_and_edit_selectors(self, url: str, ai_selectors: Dict) -> Optional[Dict]:
        """
        Show AI-discovered selectors and let user review/edit them.
        
        Args:
            url: Website URL
            ai_selectors: AI-discovered selectors
            
        Returns:
            Modified selectors (or original if user didn't edit), or None if user skips
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("REVIEW AI-DISCOVERED SELECTORS")
        self.logger.info("="*80)
        self.logger.info(f"\nURL: {url}")
        
        self.logger.info("\nAI-Discovered Selectors:")
        self._display_selectors(ai_selectors)
        
        self.logger.info("\n" + "-"*80)
        
        print("\n" + "="*80)
        print("REVIEW OPTIONS:")
        print("="*80)
        print("1. Accept selectors as-is (use for extraction)")
        print("2. Edit selectors (field-by-field)")
        print("3. Edit as JSON")
        print("4. Skip and discard selectors")
        print("-"*80 + "\n")
        
        choice = input("Select option (1-4): ").strip()
        
        if choice == '1':
            self.logger.info("✅ Accepting AI-discovered selectors")
            return ai_selectors
        
        elif choice == '2':
            self.logger.info("Editing selectors field-by-field...")
            edited = self._edit_selectors_field_by_field(ai_selectors)
            if edited:
                self.logger.info("✅ Selectors updated")
                return edited
            return ai_selectors
        
        elif choice == '3':
            self.logger.info("Editing selectors as JSON...")
            edited = self._edit_selectors_json(ai_selectors)
            if edited:
                self.logger.info("✅ Selectors updated")
                return edited
            return ai_selectors
        
        elif choice == '4':
            self.logger.warning("Skipping selectors")
            return None
        
        else:
            print("Invalid choice. Using AI selectors as-is.")
            return ai_selectors
    
    def _edit_selectors_field_by_field(self, current_selectors: Dict) -> Optional[Dict]:
        """
        Edit selectors one field at a time
        Shows current value and lets user confirm/change
        """
        
        container = current_selectors.get('container', '')
        items = current_selectors.get('items', {})
        
        print("\n" + "-"*80)
        print("EDIT CONTAINER SELECTOR")
        print("-"*80)
        print(f"Current: {container}")
        new_container = input("New value (or press Enter to keep): ").strip()
        if new_container:
            container = new_container
        
        print("\n" + "-"*80)
        print("EDIT FIELD SELECTORS")
        print("-"*80)
        
        required_fields = [
            'event_name',
            'date_iso',
            'time',
            'location',
            'description',
            'target_group',
            'status'
        ]
        
        for field in required_fields:
            current = items.get(field, '')
            print(f"\n{field}:")
            print(f"  Current: {current if current else '(empty)'}")
            new_value = input("  New value (or press Enter to keep): ").strip()
            
            if new_value:
                items[field] = new_value
            elif not current:
                items[field] = None
        
        return {
            'container': container,
            'items': items
        }
    
    def _edit_selectors_json(self, current_selectors: Dict) -> Optional[Dict]:
        """
        Edit selectors as JSON
        Shows current JSON and lets user modify it
        """
        
        current_json = json.dumps(current_selectors, indent=2)
        
        print("\n" + "-"*80)
        print("CURRENT SELECTORS (JSON)")
        print("-"*80)
        print(current_json)
        
        print("\n" + "-"*80)
        print("PASTE YOUR EDITED JSON (Ctrl+D or Ctrl+Z to finish)")
        print("-"*80 + "\n")
        
        lines = []
        try:
            while True:
                line = input()
                lines.append(line)
        except EOFError:
            pass
        
        json_str = '\n'.join(lines)
        
        if not json_str.strip():
            print("No input provided. Keeping current selectors.")
            return current_selectors
        
        try:
            edited = json.loads(json_str)
            return edited
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON: {e}")
            return current_selectors
