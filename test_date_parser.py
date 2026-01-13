import re
from datetime import datetime

# Swedish month mapping (from universal_spider.py)
SWEDISH_MONTHS = {
    'januari': 1, 'jan': 1, 'january': 1, 'february': 2, 'februari': 2, 'feb': 2,
    'mars': 3, 'mar': 3, 'march': 3, 'april': 4, 'apr': 4,
    'maj': 5, 'may': 5, 'juni': 6, 'jun': 6, 'june': 6,
    'juli': 7, 'jul': 7, 'july': 7, 'augusti': 8, 'aug': 8, 'august': 8,
    'september': 9, 'sep': 9, 'sept': 9, 'oktober': 10, 'okt': 10, 'oct': 10, 'october': 10,
    'november': 11, 'nov': 11, 'december': 12, 'dec': 12,
}

def parse_swedish_date(date_str):
    if not date_str:
        return None
    date_str = date_str.strip().lower()
    iso_match = re.match(r'^(\d{4}-\d{2}-\d{2})', date_str)
    if iso_match:
        return iso_match.group(1)
    match = re.search(r'(\d{1,2})\s+([a-zåäö]+)', date_str)
    if match:
        day = int(match.group(1))
        month = SWEDISH_MONTHS.get(match.group(2))
        if month:
            year_match = re.search(r'\b(20\d{2})\b', date_str)
            year = int(year_match.group(1)) if year_match else datetime.now().year
            if not year_match and (month < datetime.now().month or (month == datetime.now().month and day < datetime.now().day)):
                year += 1
            return f"{year}-{month:02d}-{day:02d}"
    return None

# Test cases from the screenshots
test_dates = [
    "Torsdag 13 november - torsdag 8 januari 2026",
    "Måndag 15 december - söndag 11 januari 2026",
    "Onsdag 17 december - onsdag 7 januari 2026",
    "Fredag 19 december - lördag 10 januari 2026",
    "Onsdag 7 januari",
    "Torsdag 8 januari",
]

print("Testing date parsing:")
print("=" * 60)
for date_str in test_dates:
    # Split on dash - handle en-dash, em-dash, and regular hyphen
    parts = re.split(r'\s*[-–—]\s*', date_str)
    
    print(f"Input: {date_str}")
    print(f"  Parts ({len(parts)}): {parts}")
    
    # Test part 0
    if len(parts) > 0:
        print(f"  Testing part 0: '{parts[0]}'")
        start_date = parse_swedish_date(parts[0])
        print(f"    Result: {start_date}")
        if start_date is None:
            # Debug why it failed
            lower = parts[0].strip().lower()
            iso_check = re.match(r'^(\d{4}-\d{2}-\d{2})', lower)
            print(f"    ISO match: {iso_check}")
            date_match = re.search(r'(\d{1,2})\s+([a-zåäö]+)', lower)
            print(f"    Date regex match: {date_match}")
            if date_match:
                day = int(date_match.group(1))
                month_word = date_match.group(2)
                month = SWEDISH_MONTHS.get(month_word)
                print(f"    Day={day}, Month word='{month_word}', Month number={month}")
                if month:
                    year_match = re.search(r'\b(20\d{2})\b', lower)
                    print(f"    Year match: {year_match}")
                    year = int(year_match.group(1)) if year_match else datetime.now().year
                    print(f"    Year: {year}")
                    print(f"    Current month: {datetime.now().month}")
                    if not year_match and (month < datetime.now().month or (month == datetime.now().month and day < datetime.now().day)):
                        year += 1
                        print(f"    Year adjusted to: {year}")
    else:
        start_date = None
        
    # Test part 1
    if len(parts) > 1:
        print(f"  Testing part 1: '{parts[1]}'")
        end_date = parse_swedish_date(parts[1])
        print(f"    Result: {end_date}")
    else:
        end_date = None
        print(f"  No end date (single day event)")
    
    print()
