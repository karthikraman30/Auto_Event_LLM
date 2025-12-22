# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from itemadapter import ItemAdapter
from openpyxl import Workbook


class EventCategoryPipeline:
    def process_item(self, item, spider):
        return item


class ExcelExportPipeline:
    """Pipeline to export items to Excel file."""
    
    def __init__(self):
        self.workbook = None
        self.worksheet = None
        self.row = 2  # Start from row 2 (row 1 is header)
    
    def open_spider(self, spider):
        self.workbook = Workbook()
        self.worksheet = self.workbook.active
        self.worksheet.title = "Events"
        
        # Add headers (including new fields)
        headers = [
            "Event Name", "Date", "Date ISO", "Time", "Location", 
            "Target Group", "Target Group Normalized", "Status",
            "Description", "Event URL"
        ]
        for col, header in enumerate(headers, 1):
            self.worksheet.cell(row=1, column=col, value=header)
    
    def close_spider(self, spider):
        # Save the workbook
        filename = "events.xlsx"
        self.workbook.save(filename)
        spider.logger.info(f"Exported {self.row - 2} events to {filename}")
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        self.worksheet.cell(row=self.row, column=1, value=adapter.get("event_name", ""))
        self.worksheet.cell(row=self.row, column=2, value=adapter.get("date", ""))
        self.worksheet.cell(row=self.row, column=3, value=adapter.get("date_iso", ""))
        self.worksheet.cell(row=self.row, column=4, value=adapter.get("time", ""))
        self.worksheet.cell(row=self.row, column=5, value=adapter.get("location", ""))
        self.worksheet.cell(row=self.row, column=6, value=adapter.get("target_group", ""))
        self.worksheet.cell(row=self.row, column=7, value=adapter.get("target_group_normalized", ""))
        self.worksheet.cell(row=self.row, column=8, value=adapter.get("status", ""))
        self.worksheet.cell(row=self.row, column=9, value=adapter.get("description", ""))
        self.worksheet.cell(row=self.row, column=10, value=adapter.get("event_url", ""))
        
        self.row += 1
        return item
