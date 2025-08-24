import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_supabase, deduplicate_listings, normalize_listing_type, get_existing_mls_numbers, filter_new_listings, save_new_mls_numbers, save_scraping_job_history
from webhook_logger import WebhookLogger
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()  # Add this line

# Create directory for raw crawl results
RAW_RESULTS_DIR = f"raw_crawl_results_{datetime.now().strftime('%Y-%m-%d')}"


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
    
    return filepath

def batch_save_crawl_results(crawl_results_list):
    """Batch save all crawl results to files for better performance."""
    if not crawl_results_list:
        return
    
    # Create results directory if it doesn't exist
    if not os.path.exists(RAW_RESULTS_DIR):
        os.makedirs(RAW_RESULTS_DIR)
    
    saved_count = 0
    for crawl_data in crawl_results_list:
        try:
            category = get_category_name(crawl_data['url'])
            filename = f"{category}-page-{crawl_data['page_number']}.json"
            filepath = os.path.join(RAW_RESULTS_DIR, filename)
            
            # Save to JSON file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(crawl_data, f, ensure_ascii=False, indent=2)
            
            saved_count += 1
        except Exception as e:
            pass

def load_crawl_result(filepath):
    """Load raw crawl result from file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
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
    Crawl all pages of a property category and collect results in memory.
    Stops when a page fails to crawl or returns no content.
    Returns tuple of (pages_crawled, crawl_results_list).
    """
    page_number = 1
    pages_crawled = 0
    crawl_results = []
    category = get_category_name(base_url)
    
    while True:
        # First page uses base URL, subsequent pages append #index
        if page_number == 1:
            current_url = base_url
        else:
            current_url = f"{base_url}#{page_number}"
        
        # Crawl the current page
        result = await crawler.arun(url=current_url, config=config)
        
        if not result.success:
            break
        
        # Check if page has content (simple check for markdown length)
        if not result.markdown or len(result.markdown.strip()) < 100:
            break

        # Store result in memory instead of saving to file immediately
        crawl_data = {
            "url": current_url,
            "page_number": page_number,
            "timestamp": datetime.now().isoformat(),
            "success": result.success,
            "markdown": result.markdown,
            "status_code": getattr(result, 'status_code', None),
            "error": str(result.error_message) if hasattr(result, 'error_message') and result.error_message else None
        }
        
        crawl_results.append(crawl_data)
        
        pages_crawled += 1
        
        # Move to next page
        page_number += 1
    
    return pages_crawled, crawl_results

def process_saved_category_results(base_url):
    """
    Process saved crawl results for a category, parse listings, and return results.
    Returns all listings found across all saved pages for this category.
    """
    category = get_category_name(base_url)
    saved_files = get_saved_crawl_files(category)
    
    if not saved_files:
        return []
    
    all_category_listings = []
    
    for filepath in saved_files:
        # Load the saved crawl result
        crawl_data = load_crawl_result(filepath)
        
        if not crawl_data or not crawl_data.get('success'):
            continue
        
        # Parse the markdown content
        parsed_listings = parse_cireba_listings_unified(crawl_data['markdown'], crawl_data['url'])
        
        if not parsed_listings:
            continue
        
        all_category_listings.extend(parsed_listings)
    
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
        
        # Send failure notification
        webhook_logger.send_detailed_notification(
            script_name="cireba.py",
            status="failure",
            error_message=error_message
        )

async def main():
    # Initialize webhook logger
    webhook_logger = WebhookLogger()
    
    # Initialize tracking variables
    existing_mls_count = 0
    category_results = []
    new_mls_saved = 0

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
        pass
    else:
        try:
            pass
            
            # Create an instance of AsyncWebCrawler
            async with AsyncWebCrawler() as crawler:
                # Configure crawler settings
                cleaned_md_generator = DefaultMarkdownGenerator(
                    content_source="cleaned_html",  # This is the default
                )

                config = CrawlerRunConfig(
                    css_selector="div#grid-view",
                    markdown_generator = cleaned_md_generator,
                    cache_mode=CacheMode.BYPASS,
                    wait_for_images = False,
                    scan_full_page = True, # required! cireba behavior loads first page then paginates, so first page will reutrn always if False.
                    scroll_delay=0.3               
                )

                # Crawl each category and collect results in memory
                total_pages_crawled = 0
                all_crawl_results = []
                
                for base_url in base_urls:
                    category = get_category_name(base_url)
                    
                    pages_crawled, crawl_results = await crawl_category_pages(crawler, base_url, config)
                    total_pages_crawled += pages_crawled
                    all_crawl_results.extend(crawl_results)
                
                # Batch save all crawl results to files
                batch_save_crawl_results(all_crawl_results)
                
        except Exception as e:
            trigger_failed_webhook_notification(e, webhook_logger)
            return  # Stop execution immediately
    
    # ===== PHASE 2: PROCESS SAVED RESULTS =====
    try:
        # Get existing MLS numbers from database
        existing_mls_numbers = get_existing_mls_numbers()
        existing_mls_count = len(existing_mls_numbers)
        
        # Process each category's saved results
        all_listings = []
        all_new_mls_numbers = []
        parsing_successful = True
        
        for base_url in base_urls:
            category = get_category_name(base_url)
            
            # Parse listings from saved files
            category_listings = process_saved_category_results(base_url)
            
            if not category_listings:
                parsing_successful = False
                continue
            
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
        
        # Save new MLS numbers to tracking table
        if all_new_mls_numbers:
            save_new_mls_numbers(all_new_mls_numbers)
            new_mls_saved = len(all_new_mls_numbers)
        
        # Send success notification with detailed data
        webhook_logger.send_detailed_notification(
            script_name="cireba.py",
            status="success",
            existing_mls_count=existing_mls_count,
            category_results=category_results,
            new_mls_saved=new_mls_saved
        )
        
    except Exception as e:
        trigger_failed_webhook_notification(e, webhook_logger)
        return  # Stop execution immediately

# Run the async main function
asyncio.run(main())