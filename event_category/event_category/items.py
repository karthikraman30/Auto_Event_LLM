# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EventCategoryItem(scrapy.Item):
    """Item representing an event from Stockholm library."""
    event_name = scrapy.Field()
    date = scrapy.Field()
    date_iso = scrapy.Field()  # Parsed date in ISO format (YYYY-MM-DD)
    end_date_iso = scrapy.Field()  # Parsed end date in ISO format (YYYY-MM-DD) or "N/A"
    time = scrapy.Field()
    location = scrapy.Field()
    target_group = scrapy.Field()
    target_group_normalized = scrapy.Field()  # Normalized category (children, teens, adults, etc.)
    status = scrapy.Field()  # scheduled or cancelled
    description = scrapy.Field()
    event_url = scrapy.Field() # Link to event detail page
