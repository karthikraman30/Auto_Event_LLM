import re

# Swedish month mapping
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
            year = int(year_match.group(1)) if year_match else 2026
            if not year_match and (month < 1 or (month == 1 and day < 6)):
                year += 1
            return f"{year}-{month:02d}-{day:02d}"
    return None

# Test the dates from the debug
test_dates = [
    "6 januari",
    "10 januari - 1 februari"
]

for date_str in test_dates:
    print(f"Testing: '{date_str}'")
    
    if ' - ' in date_str:
        parts = date_str.split(' - ')
        date_iso = parse_swedish_date(parts[0].strip())
        end_iso = parse_swedish_date(parts[1].strip())
        print(f"  Start: {date_iso}, End: {end_iso}")
    else:
        date_iso = parse_swedish_date(date_str)
        print(f"  Parsed: {date_iso}")
    
    if not date_iso:
        print("  ❌ FAILED TO PARSE")
    else:
        print(f"  ✅ SUCCESS: {date_iso}")
    print()