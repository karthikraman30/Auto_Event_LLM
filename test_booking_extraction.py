#!/usr/bin/env python3
import sys
sys.path.append('/Users/karthikraman/Workspace/selector_manual/Auto_Event_LLM/event_category')
from event_category.spiders.universal_spider import extract_booking_info

# Test cases
test_cases = [
    "Bokningen är stängd",
    "Du behöver boka plats",
    "Fullbokat",
    "Drop-in",
    "Bokningen öppnar 15 december",
    "Anmälan är stängd",
    "No booking info",
    "",
    None
]

print("Testing booking info extraction:")
print("=" * 40)

for test in test_cases:
    result = extract_booking_info(test)
    print(f"Input: {repr(test)}")
    print(f"Output: {result}")
    print("-" * 20)
