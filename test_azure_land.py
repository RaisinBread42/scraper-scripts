import re

def parse_markdown_list(md_text):
    """
    Extracts condo, residential, and land listings from Azure Realty Cayman markdown.
    Returns a list of dicts: {name, price, currency, link, listing_type, image_link}
    """
    results = []
    
    # Pattern for condo listings:
    # [ ![NAME](IMAGE_URL) Condo NAME Location View Property Details ](PROPERTY_URL "TITLE")
    condo_pattern = re.compile(
        r'\[ !\[([^\]]+?)\]\((https://www\.azurerealtycayman\.com/thumbs/[^\)]+)\) Condo (.+?) View Property Details \]\((https://www\.azurerealtycayman\.com/property-detail/[^\s)]+) "[^"]*"\)',
        re.IGNORECASE
    )
    
    # Pattern for residential listings:
    # [ ![NAME](IMAGE_URL) Residential DESCRIPTION Location View Property Details ](PROPERTY_URL "TITLE")
    residential_pattern = re.compile(
        r'\[ !\[([^\]]+?)\]\((https://www\.azurerealtycayman\.com/thumbs/[^\)]+)\) Residential (.+?) View Property Details \]\((https://www\.azurerealtycayman\.com/property-detail/[^\s)]+) "[^"]*"\)',
        re.IGNORECASE
    )
    
    # Pattern for land listings:
    # [ ![NAME](IMAGE_URL) Land DESCRIPTION Location View Property Details ](PROPERTY_URL "TITLE")
    land_pattern = re.compile(
        r'\[ !\[([^\]]+?)\]\((https://www\.azurerealtycayman\.com/thumbs/[^\)]+)\) Land (.+?) View Property Details \]\((https://www\.azurerealtycayman\.com/property-detail/[^\s)]+) "[^"]*"\)',
        re.IGNORECASE
    )
    
    # Process condo listings
    for match in condo_pattern.finditer(md_text):
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
    
    # Process residential listings
    for match in residential_pattern.finditer(md_text):
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
            "listing_type": "Home",
            "image_link": image_link
        })
    
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
        else:
            currency = ""
            price = ""
        
        results.append({
            "name": property_name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": "Land",
            "image_link": image_link
        })
    
    return results

# Test the function
try:
    with open('crawl_results.md', 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Debug: Let's see all lines with each type
    lines = md_content.split('\n')
    condo_lines = [line for line in lines if 'Condo' in line and 'View Property Details' in line]
    residential_lines = [line for line in lines if 'Residential' in line and 'View Property Details' in line]
    land_lines = [line for line in lines if 'Land' in line and 'View Property Details' in line]
    
    print(f"Found {len(condo_lines)} condo lines")
    print(f"Found {len(residential_lines)} residential lines") 
    print(f"Found {len(land_lines)} land lines")
    print("="*80)
    
    listings = parse_markdown_list(md_content)
    
    print(f"Total listings extracted: {len(listings)}")
    print("="*60)
    
    condo_count = len([l for l in listings if l['listing_type'] == 'Condo'])
    home_count = len([l for l in listings if l['listing_type'] == 'Home'])
    land_count = len([l for l in listings if l['listing_type'] == 'Land'])
    
    print(f"Condos: {condo_count}")
    print(f"Homes: {home_count}")
    print(f"Land: {land_count}")
    print()
    
    for i, listing in enumerate(listings, 1):
        print(f"{i}. {listing['name']} ({listing['listing_type']})")
        print(f"   Price: {listing['currency']}{listing['price']}")
        print(f"   Link: {listing['link']}")
        print()
    
except FileNotFoundError:
    print("crawl_results.md file not found!")
except Exception as e:
    print(f"Error: {e}")