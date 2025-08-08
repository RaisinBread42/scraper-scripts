import re

def parse_markdown_list(md_text):
    """
    Extracts condo listings from Azure Realty Cayman markdown.
    Returns a list of dicts: {name, price, currency, link, listing_type, image_link}
    """
    results = []
    
    # Pattern for condo listings:
    # [ ![NAME](IMAGE_URL) Condo NAME Location View Property Details ](PROPERTY_URL "TITLE")
    # US$1,999,999
    
    pattern = re.compile(
        r'\[ !\[([^\]]+?)\]\((https://www\.azurerealtycayman\.com/thumbs/[^\)]+)\) Condo (.+?) View Property Details \]\((https://www\.azurerealtycayman\.com/property-detail/[^\s)]+) "[^"]*"\)',
        re.IGNORECASE
    )
    
    for match in pattern.finditer(md_text):
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
            location_words = ['Spotts', 'South', 'West', 'Rum', 'Savannah', 'Prospect']
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
        else:
            currency = ""
            price = ""
        
        results.append({
            "name": property_name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": "Condo",
            "image_link": image_link
        })
    
    return results

# Test the function
try:
    with open('crawl_results.md', 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Debug: Let's see all lines with "Condo" in them
    lines = md_content.split('\n')
    condo_lines = [line for line in lines if 'Condo' in line and 'View Property Details' in line]
    
    print(f"Found {len(condo_lines)} lines with 'Condo' and 'View Property Details':")
    print("="*80)
    
    for i, line in enumerate(condo_lines, 1):
        print(f"{i}. {line[:100]}...")
        print()
    
    listings = parse_markdown_list(md_content)
    
    print(f"Regex extracted {len(listings)} listings:")
    print("="*60)
    
    for i, listing in enumerate(listings, 1):
        print(f"{i}. {listing['name']}")
        print(f"   Price: {listing['currency']}{listing['price']}")
        print(f"   Link: {listing['link']}")
        print()
    
    print(f"Total listings extracted: {len(listings)}")
    
except FileNotFoundError:
    print("crawl_results.md file not found!")
except Exception as e:
    print(f"Error: {e}")