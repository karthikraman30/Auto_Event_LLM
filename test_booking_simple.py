#!/usr/bin/env python3

def extract_booking_info(booking_text):
    """
    Extract booking information from Stockholm Library event text.
    Returns: 'Requires booking', 'Fullbokat', 'Booking closed', 'Drop-in', or 'N/A'
    """
    if not booking_text or booking_text == 'N/A':
        return 'N/A'
    
    text = booking_text.lower()
    
    # Check for "Fullbokat" (fully booked)
    if 'fullbokat' in text or 'fullbokad' in text:
        return 'Fullbokat'
    
    # Check for booking closed
    if 'bokningen är stängd' in text or 'anmälan är stängd' in text or 'bokning stängd' in text:
        return 'Booking closed'
    
    # Check for booking required
    if 'boka plats' in text or 'du behöver boka' in text or 'bokning krävs' in text:
        return 'Requires booking'
    
    # Check for booking opens info (also means booking required)
    if 'bokningen öppnar' in text:
        return 'Requires booking'
    
    # Check for drop-in (no booking needed)
    if 'drop-in' in text or 'dropin' in text:
        return 'Drop-in'
    
    # If booking text contains booking-related words but doesn't match specific patterns, return as-is
    if any(kw in text for kw in ['boka', 'bokning', 'bokningen', 'anmälan']):
        return booking_text.strip()
    
    return 'N/A'

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
