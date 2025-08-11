import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_supabase, deduplicate_listings, normalize_listing_type, get_existing_mls_numbers, filter_new_listings, save_new_mls_numbers
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()  # Add this line

# Create log file with today's date
LOG_FILE = f"cireba-{datetime.now().strftime('%Y-%m-%d')}.txt"

# Create directory for raw crawl results
RAW_RESULTS_DIR = f"raw_crawl_results_{datetime.now().strftime('%Y-%m-%d')}"

def log_message(message):
    """Write message to log file, overwriting if first message of the day."""
    with open(LOG_FILE, 'w' if not hasattr(log_message, 'initialized') else 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
    if not hasattr(log_message, 'initialized'):
        log_message.initialized = True

def get_category_name(url):
    """Extract category name from URL for file naming."""
    if "listingtype_14" in url:
        return "condos"
    elif "listingtype_4" in url:
        return "homes"
    elif "listingtype_5" in url:
        return "duplexes"
    elif "cayman-land-for-sale" in url:
        return "land"
    else:
        return "unknown"

def save_crawl_result(result, url, page_number):
    """Save raw crawl result to file for later parsing."""
    # Create results directory if it doesn't exist
    if not os.path.exists(RAW_RESULTS_DIR):
        os.makedirs(RAW_RESULTS_DIR)
    
    category = get_category_name(url)
    filename = f"{category}-page-{page_number}.json"
    filepath = os.path.join(RAW_RESULTS_DIR, filename)
    
    # Prepare data to save
    save_data = {
        "url": url,
        "page_number": page_number,
        "timestamp": datetime.now().isoformat(),
        "success": result.success,
        "markdown": result.markdown if result.success else None,
        "status_code": getattr(result, 'status_code', None),
        "error": str(result.error) if hasattr(result, 'error') and result.error else None
    }
    
    # Save to JSON file
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)
    
    log_message(f"💾 Saved raw result to {filename}")
    return filepath

def load_crawl_result(filepath):
    """Load raw crawl result from file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        log_message(f"📖 Loaded raw result from {os.path.basename(filepath)}")
        return data
    except Exception as e:
        log_message(f"❌ Error loading {filepath}: {e}")
        return None

def get_saved_crawl_files(category):
    """Get all saved crawl files for a specific category, sorted by page number."""
    if not os.path.exists(RAW_RESULTS_DIR):
        return []
    
    files = []
    for filename in os.listdir(RAW_RESULTS_DIR):
        if filename.startswith(f"{category}-page-") and filename.endswith('.json'):
            filepath = os.path.join(RAW_RESULTS_DIR, filename)
            # Extract page number from filename
            try:
                page_num = int(filename.split('-page-')[1].split('.json')[0])
                files.append((page_num, filepath))
            except ValueError:
                continue
    
    # Sort by page number
    files.sort(key=lambda x: x[0])
    return [filepath for _, filepath in files]

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

def parse_little_cayman_listings(md_text, url=None):
    """
    Extracts Little Cayman listings from markdown.
    Returns a list of dicts: {name, price, currency, link, listing_type, image_link, mls_number, sqft, beds, baths, location}
    """
    import re
    
    # Pattern for Little Cayman listings:
    # [ MLS#: NUMBER TITLE
    #   * SQFT SqFt
    #   * BEDS Beds
    #   * BATHS Baths
    #
    # LOCATION, Little Cayman PRICE ](LINK "TITLE")
    block_pattern = re.compile(
        r'\[ MLS#: (\d+)\s+([^\n]*?)\n\s*\*\s*([\d,]+)\s+SqFt\n\s*\*\s*(\d+(?:\.\d+)?)\s+Beds?\n\s*\*\s*(\d+(?:\.\d+)?)\s+Baths?\n\n([^,\n]+),\s*Little Cayman\s+(CI\$|US\$)([\d,\.]+) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)',
        re.MULTILINE | re.DOTALL
    )

    # Pattern to find image links before each property block
    image_pattern = re.compile(
        r'\[ !\[([^\]]*)\]\(([^)]*)\) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)'
    )

    # Find all image links
    image_matches = list(image_pattern.finditer(md_text))
    
    results = []
    for match in block_pattern.finditer(md_text):
        mls_number = match.group(1)
        name = match.group(2).strip()
        sqft = match.group(3).replace(",", "")
        beds = match.group(4)
        baths = match.group(5)
        location = match.group(6).strip()
        currency = match.group(7)
        price = match.group(8).replace(",", "")
        link = match.group(9).strip()
        
        # Convert CI$ to USD
        currency, price = convert_ci_to_usd(price, currency)
        
        # Find the first image for this property (look for matching link)
        image_link = ""
        for img_match in image_matches:
            if img_match.group(3) == link:
                image_link = img_match.group(2)
                break
        
        # Determine listing type based on URL - using same logic as main function
        if url and "listingtype_14" in url:
            listing_type = "Condo"
        elif url and "listingtype_4" in url:
            listing_type = "Home"
        elif url and "listingtype_5" in url:
            listing_type = "Duplex"
        else:
            # Fallback - try to determine from URL structure
            if "/residential-condo/" in link or "condo" in name.lower():
                listing_type = "Condo"
            elif "duplex" in name.lower():
                listing_type = "Duplex"
            else:
                listing_type = "Home"
        
        results.append({
            "name": name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": listing_type,
            "image_link": image_link,
            "mls_number": mls_number,
            "sqft": sqft,
            "beds": beds,
            "baths": baths,
            "location": f"{location}, Little Cayman"
        })
    return results

def parse_cayman_brac_listings(md_text, url=None):
    """
    Extracts Cayman Brac listings from markdown.
    Returns a list of dicts: {name, price, currency, link, listing_type, image_link, mls_number, sqft, beds, baths, location}
    """
    import re
    
    # Pattern for Cayman Brac listings:
    # [ MLS#: NUMBER TITLE
    #   * SQFT SqFt
    #   * BEDS Beds
    #   * BATHS Baths
    #
    # LOCATION, Cayman Brac PRICE ](LINK "TITLE")
    block_pattern = re.compile(
        r'\[ MLS#: (\d+)\s+([^\n]*?)\n\s*\*\s*([\d,]+)\s+SqFt\n\s*\*\s*(\d+(?:\.\d+)?)\s+Beds?\n\s*\*\s*(\d+(?:\.\d+)?)\s+Baths?\n\n([^,\n]+),\s*Cayman Brac\s+(CI\$|US\$)([\d,\.]+) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)',
        re.MULTILINE | re.DOTALL
    )

    # Pattern to find image links before each property block
    image_pattern = re.compile(
        r'\[ !\[([^\]]*)\]\(([^)]*)\) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)'
    )

    # Find all image links
    image_matches = list(image_pattern.finditer(md_text))
    
    results = []
    for match in block_pattern.finditer(md_text):
        mls_number = match.group(1)
        name = match.group(2).strip()
        sqft = match.group(3).replace(",", "")
        beds = match.group(4)
        baths = match.group(5)
        location = match.group(6).strip()
        currency = match.group(7)
        price = match.group(8).replace(",", "")
        link = match.group(9).strip()
        
        # Convert CI$ to USD
        currency, price = convert_ci_to_usd(price, currency)
        
        # Find the first image for this property (look for matching link)
        image_link = ""
        for img_match in image_matches:
            if img_match.group(3) == link:
                image_link = img_match.group(2)
                break
        
        # Determine listing type based on URL - using same logic as main function
        if url and "listingtype_14" in url:
            listing_type = "Condo"
        elif url and "listingtype_4" in url:
            listing_type = "Home"
        elif url and "listingtype_5" in url:
            listing_type = "Duplex"
        else:
            # Fallback - try to determine from URL structure
            if "/residential-condo/" in link or "condo" in name.lower():
                listing_type = "Condo"
            elif "duplex" in name.lower():
                listing_type = "Duplex"
            else:
                listing_type = "Home"
        
        results.append({
            "name": name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": listing_type,
            "image_link": image_link,
            "mls_number": mls_number,
            "sqft": sqft,
            "beds": beds,
            "baths": baths,
            "location": f"{location}, Cayman Brac"
        })
    return results

def parse_land_listings(md_text, url=None):
    """
    Extracts land listings from markdown.
    Returns a list of dicts: {name, price, currency, link, listing_type, image_link, mls_number, acres, location}
    """
    import re
    
    # Pattern for land listings - handles all three islands (Grand Cayman, Little Cayman, Cayman Brac):
    # [ MLS#: NUMBER TITLE
    #   * X.XX Acres
    #
    # LOCATION, ISLAND PRICE ](LINK "TITLE")
    block_pattern = re.compile(
        r'\[ MLS#: (\d+)\s+([^\n]*?)\n\s*\*\s*([\d.]+)\s+Acres\n\n([^,\n]+),\s*(Grand Cayman|Little Cayman|Cayman Brac)\s+(CI\$|US\$)([\d,\.]+) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)',
        re.MULTILINE | re.DOTALL
    )

    # Pattern to find image links before each property block
    image_pattern = re.compile(
        r'\[ !\[([^\]]*)\]\(([^)]*)\) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)'
    )

    # Find all image links
    image_matches = list(image_pattern.finditer(md_text))
    
    results = []
    for match in block_pattern.finditer(md_text):
        mls_number = match.group(1)
        name = match.group(2).strip()
        acres = match.group(3)
        location = match.group(4).strip()
        island = match.group(5).strip()
        currency = match.group(6)
        price = match.group(7).replace(",", "")
        link = match.group(8).strip()
        
        # Convert CI$ to USD
        currency, price = convert_ci_to_usd(price, currency)
        
        # Find the first image for this property (look for matching link)
        image_link = ""
        for img_match in image_matches:
            if img_match.group(3) == link:
                image_link = img_match.group(2)
                break
        
        # Land listings are always type "Land"
        listing_type = "Land"
        
        # Combine location with island for full location
        full_location = f"{location}, {island}"
        
        results.append({
            "name": name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": listing_type,
            "image_link": image_link,
            "mls_number": mls_number,
            "acres": acres,
            "location": full_location,
            "sqft": None,
            "beds": None,
            "baths": None
        })
    
    return results

def parse_markdown_list(md_text, url=None):
    """
    Extracts name, price, currency, link, listing_type, image_link, mls_number, sqft, beds, baths, and location from CIREBA markdown.
    Returns a list of dicts: {name, price, currency, link, listing_type, image_link, mls_number, sqft, beds, baths, location}
    """
    import re

    # Updated pattern for the property block based on new format:
    # [ MLS#: NUMBER TITLE
    #   * SQFT SqFt
    #   * BEDS Beds
    #   * BATHS Baths
    #
    # LOCATION, Grand Cayman PRICE ](LINK "TITLE")
    block_pattern = re.compile(
        r'\[ MLS#: (\d+)\s+([^\n]*?)\n\s*\*\s*([\d,]+)\s+SqFt\n\s*\*\s*(\d+(?:\.\d+)?)\s+Beds?\n\s*\*\s*(\d+(?:\.\d+)?)\s+Baths?\n\n([^,\n]+),\s*Grand Cayman\s+(CI\$|US\$)([\d,\.]+) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)',
        re.MULTILINE | re.DOTALL
    )

    # Pattern to find image links before each property block
    image_pattern = re.compile(
        r'\[ !\[([^\]]*)\]\(([^)]*)\) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)\s+"[^"]*"\)'
    )

    # Find all image links
    image_matches = list(image_pattern.finditer(md_text))
    
    results = []
    for match in block_pattern.finditer(md_text):
        mls_number = match.group(1)
        name = match.group(2).strip()
        sqft = match.group(3).replace(",", "")
        beds = match.group(4)
        baths = match.group(5)
        location = match.group(6).strip()
        currency = match.group(7)
        price = match.group(8).replace(",", "")
        link = match.group(9).strip()
        
        # Convert CI$ to USD
        currency, price = convert_ci_to_usd(price, currency)
        
        # Find the first image for this property (look for matching link)
        image_link = ""
        for img_match in image_matches:
            if img_match.group(3) == link:
                image_link = img_match.group(2)
                break
        
        # Determine listing type based on URL - listingtype_14 is Home
        if url and "listingtype_14" in url:
            listing_type = "Condo"
        elif url and "listingtype_4" in url:
            listing_type = "Home"
        elif url and "listingtype_5" in url:
            listing_type = "Duplex"
        else:
            # Fallback - try to determine from URL structure
            if "/residential-condo/" in link or "condo" in name.lower():
                listing_type = "Condo"
            elif "duplex" in name.lower():
                listing_type = "Duplex"
            else:
                listing_type = "Home"
        
        # Combine location with island for full location (similar to land listings)
        full_location = f"{location}, Grand Cayman"
        
        results.append({
            "name": name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": listing_type,
            "image_link": image_link,
            "mls_number": mls_number,
            "sqft": sqft,
            "beds": beds,
            "baths": baths,
            "location": full_location
        })
    
    # Parse Little Cayman listings and add to results
    little_cayman_results = parse_little_cayman_listings(md_text, url)
    results.extend(little_cayman_results)
    
    # Parse Cayman Brac listings and add to results
    cayman_brac_results = parse_cayman_brac_listings(md_text, url)
    results.extend(cayman_brac_results)
    
    # Parse land listings if this is a land URL
    if url and "cayman-land-for-sale" in url:
        land_results = parse_land_listings(md_text, url)
        results.extend(land_results)
    
    return results

async def crawl_category_pages(crawler, base_url, config):
    """
    Crawl all pages of a property category and save raw results to files.
    Stops when a page fails to crawl or returns no content.
    Returns the number of pages successfully crawled.
    """
    page_number = 1
    pages_crawled = 0
    
    while True:
        # First page uses base URL, subsequent pages append #index
        if page_number == 1:
            current_url = base_url
        else:
            current_url = f"{base_url}#{page_number}"
        
        log_message(f"🌐 Crawling page {page_number}: {current_url}")
        
        # Crawl the current page
        result = await crawler.arun(url=current_url, config=config)
        
        # Save raw result to file regardless of success
        save_crawl_result(result, current_url, page_number)
        
        if not result.success:
            log_message(f"❌ Failed to crawl page {page_number}: {current_url}")
            break
        
        # Check if page has content (simple check for markdown length)
        if not result.markdown or len(result.markdown.strip()) < 100:
            log_message(f"📭 Page {page_number} appears empty. Stopping crawl.")
            break
        
        pages_crawled += 1
        log_message(f"✅ Successfully crawled page {page_number}")
        
        # Move to next page
        page_number += 1
    
    log_message(f"🏁 Crawling complete. {pages_crawled} pages crawled for category.")
    return pages_crawled

def process_saved_category_results(base_url):
    """
    Process saved crawl results for a category, parse listings, and return results.
    Returns all listings found across all saved pages for this category.
    """
    category = get_category_name(base_url)
    saved_files = get_saved_crawl_files(category)
    
    if not saved_files:
        log_message(f"⚠️ No saved files found for category: {category}")
        return []
    
    all_category_listings = []
    
    for filepath in saved_files:
        # Load the saved crawl result
        crawl_data = load_crawl_result(filepath)
        
        if not crawl_data or not crawl_data.get('success'):
            log_message(f"⚠️ Skipping failed/invalid result: {os.path.basename(filepath)}")
            continue
        
        # Parse the markdown content
        parsed_listings = parse_markdown_list(crawl_data['markdown'], crawl_data['url'])
        
        if not parsed_listings:
            log_message(f"📭 No listings found in {os.path.basename(filepath)}")
            continue
        
        log_message(f"✅ Parsed {len(parsed_listings)} listings from {os.path.basename(filepath)}")
        all_category_listings.extend(parsed_listings)
    
    log_message(f"🎯 Total {len(all_category_listings)} listings processed for {category}")
    return all_category_listings

async def main():
    log_message("🚀 Starting CIREBA scraper with file-based crawling...")
    
    # Base URLs for each property category
    base_urls = [
        "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N",  # Condos
        "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N",   # Homes  
        "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_5/filterby_N",   # Duplexes
        "https://www.cireba.com/cayman-land-for-sale/filterby_N"                                   # Land
    ]
    
    # ===== PHASE 1: CRAWL AND SAVE RAW RESULTS =====
    # Check if we already have saved results for today
    skip_crawling = os.path.exists(RAW_RESULTS_DIR) and os.listdir(RAW_RESULTS_DIR)
    
    if skip_crawling:
        log_message(f"📁 Found existing crawl data in {RAW_RESULTS_DIR}")
        log_message("⏭️ Skipping crawling phase - using existing files")
        log_message("💡 Delete the folder to force re-crawling")
    else:
        log_message("📡 PHASE 1: Crawling pages and saving raw results...")
        
        # Create an instance of AsyncWebCrawler
        async with AsyncWebCrawler() as crawler:
            # Configure crawler settings
            cleaned_md_generator = DefaultMarkdownGenerator(
                content_source="cleaned_html",  # This is the default
            )

            config = CrawlerRunConfig(
                css_selector="div#grid-view",
                markdown_generator = cleaned_md_generator,
                wait_for_images = True,
                scan_full_page = True,
                scroll_delay=1, 
            )

            # Crawl each category and save raw results
            total_pages_crawled = 0
            for base_url in base_urls:
                category = get_category_name(base_url)
                log_message(f"\n🏗️ Crawling {category.upper()} category: {base_url}")
                
                pages_crawled = await crawl_category_pages(crawler, base_url, config)
                total_pages_crawled += pages_crawled
                
                log_message(f"✅ {category.upper()} crawling complete: {pages_crawled} pages saved")
            
            log_message(f"🎯 PHASE 1 COMPLETE: {total_pages_crawled} total pages crawled and saved")
    
    # ===== PHASE 2: PROCESS SAVED RESULTS =====
    log_message("\n🔧 PHASE 2: Processing saved results and saving to database...")
    
    # Get existing MLS numbers from database
    log_message("🔍 Checking for existing MLS numbers...")
    existing_mls_numbers = get_existing_mls_numbers()
    
    # Process each category's saved results
    all_listings = []
    all_new_mls_numbers = []
    
    for base_url in base_urls:
        category = get_category_name(base_url)
        log_message(f"\n📋 Processing saved {category.upper()} results...")
        
        # Parse listings from saved files
        category_listings = process_saved_category_results(base_url)
        
        if not category_listings:
            log_message(f"⚠️ No listings found for {category.upper()}")
            continue
        
        # Filter out already scraped listings
        new_listings = filter_new_listings(category_listings, existing_mls_numbers)
        
        if new_listings:
            all_listings.extend(new_listings)
            # Collect new MLS numbers for tracking
            new_mls_numbers = [listing['mls_number'] for listing in new_listings if listing.get('mls_number')]
            all_new_mls_numbers.extend(new_mls_numbers)
            
            # Save new listings to Supabase
            save_to_supabase(base_url, deduplicate_listings(new_listings))
            log_message(f"✅ {category.upper()} processing complete: {len(new_listings)} new listings saved")
        else:
            log_message(f"ℹ️ {category.upper()}: No new listings to save (all already exist)")
    
    # Save new MLS numbers to tracking table
    if all_new_mls_numbers:
        save_new_mls_numbers(all_new_mls_numbers)
    
    log_message(f"\n🏆 SCRAPING COMPLETE! Total new listings processed: {len(all_listings)}")
    log_message(f"📁 Raw crawl data saved in: {RAW_RESULTS_DIR}")
    log_message("💡 You can now re-run parsing by running this script again (it will skip crawling if files exist)")

# Run the async main function
asyncio.run(main())