import re

def convert_ci_to_usd(price_str, currency):
    """Convert CI$ to USD using exact rate: 1 CI$ = 1.2195121951219512195121951219512 USD"""
    if currency == "CI$" and price_str:
        try:
            ci_amount = float(price_str.replace(",", ""))
            usd_amount = ci_amount * 1.2195121951219512195121951219512
            return "US$", str(round(usd_amount, 2))
        except ValueError:
            return currency, price_str
    return currency, price_str

def parse_markdown_list(md_text):
    """
    Extracts condo, residential, and land listings from Azure Realty Cayman markdown.
    Returns a list of dicts: {name, price, currency, link, listing_type, image_link}
    """
    results = []
    
    # Pattern for land listings:
    # [ ![NAME](IMAGE_URL) Land DESCRIPTION Location View Property Details ](PROPERTY_URL "TITLE")
    land_pattern = re.compile(
        r'\[ !\[([^\]]+?)\]\((https://www\.azurerealtycayman\.com/thumbs/[^\)]+)\) Land (.+?) View Property Details \]\((https://www\.azurerealtycayman\.com/property-detail/[^\s)]+) "[^"]*"\)',
        re.IGNORECASE
    )
    
    # Process land listings
    for match in land_pattern.finditer(md_text):
        name = match.group(1).strip()
        image_link = match.group(2)
        full_description = match.group(3).strip()
        link = match.group(4)
        
        # Extract property name from description (before location)
        if ',' in full_description:
            property_name = full_description.split(',')[0].strip()
        else:
            # Split by common location words
            words = full_description.split()
            location_words = ['Spotts', 'South', 'West', 'Rum', 'Savannah', 'Prospect', 'North', 'Bay']
            name_words = []
            for word in words:
                if word in location_words:
                    break
                name_words.append(word)
            property_name = ' '.join(name_words) if name_words else name
        
        # Find price after this match
        after = md_text[match.end():]
        price_match = re.search(r'(US\$|CI\$)([\d,]+)', after[:50])
        
        if price_match:
            currency = price_match.group(1)
            price = price_match.group(2).replace(",", "")
            original_currency = currency
            original_price = price
            # Convert CI$ to USD
            currency, price = convert_ci_to_usd(price, currency)
        else:
            currency = ""
            price = ""
            original_currency = ""
            original_price = ""
        
        results.append({
            "name": property_name,
            "currency": currency,
            "price": price,
            "original_currency": original_currency,
            "original_price": original_price,
            "link": link,
            "listing_type": "Land",
            "image_link": image_link
        })
    
    return results

# Test the function
try:
    with open('crawl_results.md', 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    listings = parse_markdown_list(md_content)
    
    print(f"Total listings extracted: {len(listings)}")
    print("="*80)
    print("Testing Currency Conversion (CI$ to USD)")
    print("="*80)
    
    for i, listing in enumerate(listings, 1):
        if listing['original_currency'] == 'CI$':
            print(f"{i}. {listing['name']}")
            print(f"   Original: {listing['original_currency']}{listing['original_price']}")
            print(f"   Converted: {listing['currency']}{listing['price']}")
            
            # Manual calculation for verification
            original_amount = float(listing['original_price'])
            expected_usd = round(original_amount * 1.2195121951219512195121951219512, 2)
            print(f"   Expected: US${expected_usd}")
            print(f"   Match: {'YES' if float(listing['price']) == expected_usd else 'NO'}")
            print()
        else:
            print(f"{i}. {listing['name']} - Already in USD: {listing['currency']}{listing['price']}")
            print()
    
except FileNotFoundError:
    print("crawl_results.md file not found!")
except Exception as e:
    print(f"Error: {e}")