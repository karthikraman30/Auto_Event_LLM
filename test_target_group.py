# Test target group normalization
import re

def simple_normalize(target_str):
    if not target_str:
        return 'all_ages'
    t = target_str.lower()
    if any(k in t for k in ['barn', 'kid', 'bebis', 'småbarn', 'förskola', 'for children', 'för barn']):
        return 'children'
    if any(k in t for k in ['ungdom', 'teen', 'tonåring', 'unga']):
        return 'teens'
    if 'familj' in t or 'family' in t:
        return 'families'
    if any(k in t for k in ['vuxen', 'vuxna', 'adult', 'senior']):
        return 'adults'
    if any(k in t for k in ['all', 'alla', 'general']):
        return 'all_ages'
    age_match = re.search(r'(\d{1,2})(?:[-–\s]+(\d{1,2}))?\s*(?:år|year|age)', t)
    if age_match:
        try:
            min_age = int(age_match.group(1))
            return 'children' if min_age < 13 else ('teens' if min_age < 20 else 'adults')
        except:
            pass
    return 'all_ages'

# Test cases based on Stockholm library
test_cases = [
    ("Målgrupp: Vuxna", "Vuxna"),
    ("Målgrupp: Barn", "Barn"),
    ("Målgrupp: Familjer", "Familjer"),
    ("Målgrupp: Ungdomar", "Ungdomar"),
    ("Målgrupp: 6-12 år", "6-12 år"),
    ("Målgrupp: Alla", "Alla"),
    ("", ""),
]

print("Target Group Normalization Tests:")
print("=" * 60)

for raw_target, expected_value in test_cases:
    # Extract value like the spider does
    if raw_target and 'målgrupp' in raw_target.lower():
        target_value = raw_target.split(':', 1)[1].strip() if ':' in raw_target else raw_target.replace('Målgrupp', '').strip()
    else:
        target_value = None
    
    # Normalize
    normalized = simple_normalize(target_value) if target_value else 'all_ages'
    
    print(f"Raw: '{raw_target}'")
    print(f"  Extracted value: '{target_value}'")
    print(f"  Normalized: {normalized}")
    print()
