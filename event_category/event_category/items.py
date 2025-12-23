import scrapy

class EventCategoryItem(scrapy.Item):
    event_name = scrapy.Field()
    date = scrapy.Field()
    date_iso = scrapy.Field()
    end_date_iso = scrapy.Field()
    time = scrapy.Field()
    location = scrapy.Field()
    target_group = scrapy.Field()
    
    # === ADD THIS FIELD IF MISSING ===
    target_group_normalized = scrapy.Field()
    
    description = scrapy.Field()
    event_url = scrapy.Field()
    status = scrapy.Field()
    extra_attributes = scrapy.Field()