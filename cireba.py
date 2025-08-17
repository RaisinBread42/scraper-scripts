import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_supabase, deduplicate_listings, normalize_listing_type, get_existing_mls_numbers, filter_new_listings, save_new_mls_numbers, mark_removed_listings, save_scraping_job_history
from webhook_logger import WebhookLogger
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
        "error": str(result.error_message) if hasattr(result, 'error_message') and result.error_message else None
    }
    
    # Save to JSON file
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)
    
    log_message(f"💾 Saved raw result to {filename}")

    if result.save_data.error is not None:
        e = Exception(f"Failed scraping {url} on page {page_number}: {result.save_data.error}")
        raise e

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
        parsed_listings = parse_cireba_listings_unified(crawl_data['markdown'], crawl_data['url'])
        
        if not parsed_listings:
            log_message(f"📭 No listings found in {os.path.basename(filepath)}")
            continue
        
        log_message(f"✅ Parsed {len(parsed_listings)} listings from {os.path.basename(filepath)}")
        all_category_listings.extend(parsed_listings)
    
    log_message(f"🎯 Total {len(all_category_listings)} listings processed for {category}")
    return all_category_listings



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
        
        # Convert CI$ to USD (same as before)
        currency, price = convert_ci_to_usd(price, currency)
        
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
        log_message(f"❌ SCRAPING FAILED: {error_message}")
        
        # Send failure notification
        webhook_logger.send_detailed_notification(
            script_name="cireba.py",
            status="failure",
            error_message=error_message
        )

async def main():
    log_message("🚀 Starting CIREBA scraper with file-based crawling...")
    
    # Initialize webhook logger
    webhook_logger = WebhookLogger()
    
    # Initialize tracking variables
    existing_mls_count = 0
    category_results = []
    new_mls_saved = 0
    removed_mls_numbers = []
    
    try:
        # Save scraping job history at the start
        log_message("📝 Saving scraping job history...")
        save_scraping_job_history("automated python script")
    except Exception as e:
        log_message(f"❌ Failed to save scraping job history: {e}")
        return  # Stop execution immediately
    
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
        try:
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
                
        except Exception as e:
            trigger_failed_webhook_notification(e, webhook_logger)
            return  # Stop execution immediately
    
    # ===== PHASE 2: PROCESS SAVED RESULTS =====
    try:
        log_message("\n🔧 PHASE 2: Processing saved results and saving to database...")
        
        # Get existing MLS numbers from database
        log_message("🔍 Checking for existing MLS numbers...")
        existing_mls_numbers = get_existing_mls_numbers()
        existing_mls_count = len(existing_mls_numbers)
        
        # Process each category's saved results
        all_listings = []
        all_new_mls_numbers = []
        all_current_mls_numbers = set()
        parsing_successful = True
        
        for base_url in base_urls:
            category = get_category_name(base_url)
            log_message(f"\n📋 Processing saved {category.upper()} results...")
            
            # Parse listings from saved files
            category_listings = process_saved_category_results(base_url)
            
            if not category_listings:
                log_message(f"⚠️ No listings found for {category.upper()}")
                parsing_successful = False
                continue
            
            # Collect all current MLS numbers from parsed results
            category_mls_numbers = {listing['mls_number'] for listing in category_listings if listing.get('mls_number')}
            all_current_mls_numbers.update(category_mls_numbers)
            
            # Filter out already scraped listings
            new_listings = filter_new_listings(category_listings, existing_mls_numbers)
            existing_skipped = len(category_listings) - len(new_listings)
            
            # Track category results for webhook
            category_result = {
                "category": category,
                "url": base_url,
                "new_listings": len(new_listings),
                "existing_skipped": existing_skipped
            }
            category_results.append(category_result)
            
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
            new_mls_saved = len(all_new_mls_numbers)
        
        # Mark removed listings only if parsing was successful for all categories
        if parsing_successful and all_current_mls_numbers:
            log_message("\n🔍 Checking for removed listings...")
            removed_mls_set = existing_mls_numbers - all_current_mls_numbers
            removed_mls_numbers = list(removed_mls_set)
            mark_removed_listings(all_current_mls_numbers, existing_mls_numbers)
        else:
            log_message("⚠️ Skipping removed listings check due to parsing issues or no current MLS numbers found")
        
        log_message(f"\n🏆 SCRAPING COMPLETE! Total new listings processed: {len(all_listings)}")
        log_message(f"📁 Raw crawl data saved in: {RAW_RESULTS_DIR}")
        log_message("💡 You can now re-run parsing by running this script again (it will skip crawling if files exist)")
        
        # Send success notification with detailed data
        webhook_logger.send_detailed_notification(
            script_name="cireba.py",
            status="success",
            existing_mls_count=existing_mls_count,
            category_results=category_results,
            new_mls_saved=new_mls_saved,
            removed_mls_details=removed_mls_numbers
        )
        
    except Exception as e:
        trigger_failed_webhook_notification(e, webhook_logger)
        return  # Stop execution immediately

# Run the async main function
asyncio.run(main())