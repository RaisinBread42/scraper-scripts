import re
import asyncio
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_supabase
from webhook_logger import WebhookLogger

load_dotenv()

def clean_and_validate_listings(listings: List[Dict]) -> List[Dict]:
    """Clean and validate all listing data including currency conversion."""
    cleaned_listings = []
    seen_links = set()
    
    for listing in listings:
        # Skip duplicates by link
        link = listing.get('link', '')
        if link in seen_links:
            continue
        seen_links.add(link)
        
        # Currency conversion
        currency = listing.get('currency', 'CI$')
        price_str = listing.get('price', '')
        
        if currency == "CI$" and price_str:
            try:
                ci_amount = float(str(price_str).replace(",", ""))
                usd_amount = ci_amount * 1.2195121951219512195121951219512
                listing['currency'] = "US$"
                listing['price'] = round(usd_amount, 2)
            except (ValueError, TypeError):
                listing['price'] = 0.0
        else:
            try:
                listing['price'] = float(str(price_str).replace(",", "")) if price_str else 0.0
            except (ValueError, TypeError):
                listing['price'] = 0.0
        
        # Data type validation and cleaning
        if listing.get('sqft'):
            try:
                listing['sqft'] = int(str(listing['sqft']).replace(',', ''))
            except (ValueError, TypeError):
                listing['sqft'] = None
        
        if listing.get('beds'):
            try:
                listing['beds'] = int(float(listing['beds']))
            except (ValueError, TypeError):
                listing['beds'] = None
        
        if listing.get('baths'):
            try:
                listing['baths'] = int(float(listing['baths']))
            except (ValueError, TypeError):
                listing['baths'] = None
        
        if listing.get('acres'):
            try:
                listing['acres'] = float(listing['acres'])
            except (ValueError, TypeError):
                listing['acres'] = None
        
        cleaned_listings.append(listing)
    
    return cleaned_listings


async def crawl_category_pages(crawler, base_url, config):
    """Crawl all pages of a property category and collect results in memory."""
    page_number = 1
    all_listings = []
    
    while True:
        current_url = base_url if page_number == 1 else f"{base_url}#{page_number}"
        
        result = await crawler.arun(url=current_url, config=config)
        
        if not result.success:
            raise Exception(f"Failed to crawl page {page_number}: {getattr(result, 'error_message', 'Unknown error')}")
        
        if not result.markdown or len(result.markdown.strip()) < 100:
            break

        parsed_listings = parse_cireba_listings_unified(result.markdown, current_url)
        if parsed_listings:
            all_listings.extend(parsed_listings)
        
        page_number += 1
    
    return all_listings



def parse_cireba_listings_unified(md_text, url=None):
    """
    Unified parser for all CIREBA listings (all islands, properties + land).
    Replaces: parse_markdown_list, parse_little_cayman_listings, 
              parse_cayman_brac_listings, parse_land_listings
    """
    
    # Unified pattern for ALL islands and property types
    # Captures both property (SqFt/Beds/Baths) and land (Acres) formats
    unified_pattern = re.compile(
        r'\[ MLS#: (\d+)\s+([^\n]*?)\n'  # MLS number and title
        r'\s*\*\s*'  # First bullet point
        r'(?:'  # Non-capturing group for property details
            r'([\d,]+)\s+SqFt\n\s*\*\s*(\d+(?:\.\d+)?)\s+Beds?\n\s*\*\s*(\d+(?:\.\d+)?)\s+Baths?'  # Property: SqFt, Beds, Baths
            r'|'  # OR
            r'([\d.]+)\s+Acres'  # Land: Acres only
        r')\n\n'
        r'([^,\n]+),\s*'  # Location
        r'(Grand Cayman|Little Cayman|Cayman Brac)\s+'  # Island (dynamic)
        r'(CI\$|US\$)([\d,\.]+)\s*'  # Currency and price
        r'\]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)',  # Link
        re.MULTILINE | re.DOTALL
    )

    # Same image pattern (unchanged)
    image_pattern = re.compile(
        r'\[ !\[([^\]]*)\]\(([^)]*)\) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)'
    )

    # Find all image links
    image_matches = list(image_pattern.finditer(md_text))
    
    results = []
    for match in unified_pattern.finditer(md_text):
        mls_number = match.group(1)
        name = match.group(2).strip()
        location = match.group(7).strip()
        island = match.group(8).strip()
        currency = match.group(9)
        price = match.group(10).replace(",", "")
        link = match.group(11).strip()
        
        # Determine if it's property or land based on which groups matched
        is_property = match.group(3) is not None  # SqFt group exists
        is_land = match.group(6) is not None      # Acres group exists
        
        if is_property:
            # Property listing
            sqft = match.group(3).replace(",", "")
            beds = match.group(4)
            baths = match.group(5)
            acres = None
            listing_type = determine_property_type(url, name, link)
            
        elif is_land:
            # Land listing  
            sqft = None
            beds = None
            baths = None
            acres = match.group(6)
            listing_type = "Land"
        else:
            # Fallback (shouldn't happen with good regex)
            continue
        
        # Find matching image (same logic as before)
        image_link = ""
        for img_match in image_matches:
            if img_match.group(3) == link:
                image_link = img_match.group(2)
                break
        
        # Build result with full location
        full_location = f"{location}, {island}"
        
        result = {
            "name": name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": listing_type,
            "image_link": image_link,
            "mls_number": mls_number,
            "location": full_location,
            "sqft": sqft,
            "beds": beds,
            "baths": baths
        }
        
        # Add acres for land listings
        if acres is not None:
            result["acres"] = acres
            
        results.append(result)
    
    return results

def determine_property_type(url, name, link):
    """Extract property type logic into helper function."""
    if url and "listingtype_14" in url:
        return "Condo"
    elif url and "listingtype_4" in url:
        return "Home"
    elif url and "listingtype_5" in url:
        return "Duplex"
    else:
        # Fallback logic
        if "/residential-condo/" in link or "condo" in name.lower():
            return "Condo"
        elif "duplex" in name.lower():
            return "Duplex"
        else:
            return "Home"

def trigger_failed_webhook_notification(e, webhook_logger):
        error_message = str(e)
        
        # Send failure notification
        webhook_logger.send_detailed_notification(
            script_name="cireba.py",
            status="failure",
            error_message=error_message
        )

async def main():
    webhook_logger = WebhookLogger()
    
    base_urls = [
        "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N",
        "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N",
        "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_5/filterby_N",
        "https://www.cireba.com/cayman-land-for-sale/filterby_N"
    ]
    
    # ===== PHASE 1: FETCHING CRAWLED DATA =====
    all_listings = []
    try:
        async with AsyncWebCrawler() as crawler:
            cleaned_md_generator = DefaultMarkdownGenerator(content_source="cleaned_html")
            config = CrawlerRunConfig(
                css_selector="div#grid-view",
                markdown_generator=cleaned_md_generator,
                cache_mode=CacheMode.BYPASS,
                wait_for_images=False,
                scan_full_page=True,
                scroll_delay=0.3               
            )
            
            for base_url in base_urls:
                category_listings = await crawl_category_pages(crawler, base_url, config)
                all_listings.extend(category_listings)
                
    except Exception as e:
        print(f"Failed during fetching crawled data: {e}")
        trigger_failed_webhook_notification(e, webhook_logger)
        return
    
    # ===== PHASE 2: PARSING =====
    parsed_listings = []
    try:
        parsed_listings = clean_and_validate_listings(all_listings)
                
    except Exception as e:
        print(f"Failed during parsing: {e}")
        trigger_failed_webhook_notification(e, webhook_logger)
        return
    
    # ===== PHASE 3: SAVING TO SUPABASE =====
    try:
        save_to_supabase(parsed_listings)
        
        webhook_logger.send_detailed_notification(
            script_name="cireba.py",
            status="success",
            category_results=parsed_listings
        )
        
    except Exception as e:
        print(f"Failed during saving to supabase: {e}")
        trigger_failed_webhook_notification(e, webhook_logger)
        return

# Run the async main function
asyncio.run(main())