import scrapy

class EventCategoryItem(scrapy.Item):
    # --- Standard Core Fields ---
    event_name = scrapy.Field()
    date = scrapy.Field()      # Raw date string
    date_iso = scrapy.Field()  # YYYY-MM-DD
    end_date_iso = scrapy.Field()
    time = scrapy.Field()
    location = scrapy.Field()
    target_group = scrapy.Field()
    description = scrapy.Field()
    event_url = scrapy.Field()
    status = scrapy.Field()    # scheduled/cancelled
    
    # --- New Dynamic Field ---
    # This will hold a dictionary of extra details found by the LLM
    extra_attributes = scrapy.Field()