import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import deduplicate_listings, normalize_listing_type, save_to_ecaytrade_table
from datetime import datetime
from duplicate_detector import filter_mls_listings
from webhook_logger import WebhookLogger

# Load environment variables from .env file
load_dotenv()  # Add this line

# Create log file with today's date
LOG_FILE = f"ecaytrade-{datetime.now().strftime('%Y-%m-%d')}.txt"

# Create directory for raw crawl results
RAW_RESULTS_DIR = f"raw_crawl_results_ecaytrade_{datetime.now().strftime('%Y-%m-%d')}"

def log_message(message):
    """Write message to log file, overwriting if first message of the day."""
    with open(LOG_FILE, 'w' if not hasattr(log_message, 'initialized') else 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
    if not hasattr(log_message, 'initialized'):
        log_message.initialized = True

def get_category_name(url):
    """Extract category name from URL for file naming."""
    if "type=lots--lands" in url:
        return "land"
    else:
        return "properties"

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

def get_location_from_url(url):
    """Extract location from URL based on location parameter."""
    if "location=Cayman%20Brac" in url:
        return "Cayman Brac"
    elif "location=Little%20Cayman" in url:
        return "Little Cayman"
    else:
        return "Grand Cayman"

def save_crawl_result(result, url, page_number):
    """Save raw crawl result to file for later parsing."""
    # Create results directory if it doesn't exist
    if not os.path.exists(RAW_RESULTS_DIR):
        os.makedirs(RAW_RESULTS_DIR)
    
    category = get_category_name(url)
    location = get_location_from_url(url)
    # Create filename with location for better organization
    location_short = location.lower().replace(" ", "_")
    filename = f"{category}-{location_short}-page-{page_number}.json"
    filepath = os.path.join(RAW_RESULTS_DIR, filename)
    
    # Prepare data to save
    save_data = {
        "url": url,
        "page_number": page_number,
        "category": category,
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
    return filepath

def load_crawl_result(filepath):
    """Load raw crawl result from file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        log_message(f"❌ Error loading {filepath}: {e}")
        return None

def get_saved_crawl_files(category=None, location=None):
    """Get all saved crawl files for a specific category and/or location, sorted by page number."""
    if not os.path.exists(RAW_RESULTS_DIR):
        return []
    
    files = []
    for filename in os.listdir(RAW_RESULTS_DIR):
        if filename.endswith('.json'):
            # Check if file matches the new naming pattern with location
            if category and location:
                location_short = location.lower().replace(" ", "_")
                expected_prefix = f"{category}-{location_short}-page-"
                if not filename.startswith(expected_prefix):
                    continue
            elif category:
                # If only category specified, match any location for that category
                if not (filename.startswith(f"{category}-grand_cayman-page-") or 
                       filename.startswith(f"{category}-cayman_brac-page-") or 
                       filename.startswith(f"{category}-little_cayman-page-")):
                    continue
            elif not category and not location:
                # Match any valid file pattern
                if not (filename.startswith("properties-") or filename.startswith("land-")):
                    continue
                
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

async def crawl_category_pages(crawler, base_url, config):
    """Crawl all pages of a category until no more listings are found."""
    page_number = 1
    pages_crawled = 0
    category = get_category_name(base_url)
    location = get_location_from_url(base_url)
    
    log_message(f"🏗️ Starting to crawl {category.upper()} category for {location}: {base_url}")
    
    while True:
        # Build URL with current page number
        current_url = base_url.replace("page=1", f"page={page_number}")
        
        log_message(f"🌐 Crawling page {page_number} ({location}): {current_url}")
        
        # Crawl the current page
        result = await crawler.arun(url=current_url, config=config)

        # Save raw result to file regardless of success
        save_crawl_result(result, current_url, page_number)
        
        if not result.success:
            log_message(f"❌ Failed to crawl page {page_number} ({location}): {current_url}")
            break
        
        # Check if page has content by looking for listings
        parsed_listings = parse_markdown_list(result.markdown, current_url, location)
        
        if not parsed_listings or len(parsed_listings) == 0:
            log_message(f"📭 Page {page_number} has no listings. Stopping crawl for {category} ({location}).")
            break
        
        pages_crawled += 1
        log_message(f"✅ Successfully crawled page {page_number} ({location}) - found {len(parsed_listings)} listings")
        
        # Move to next page
        page_number += 1
    
    log_message(f"🏁 {category.upper()} ({location}) crawling complete. {pages_crawled} pages crawled.")
    return pages_crawled


def parse_markdown_list(md_text, url=None, location=None):
    # Updated regex to capture location from __Location__ pattern in the markdown
    # Pattern captures: [ ![NAME](IMG) PROPERTY_TYPE (PRICE or "Price Upon Request") CONTENT __LOCATION__ ](LINK)
    pattern = re.compile(
        r'\[ !\[(.*?)\]\(([^\)]*)\)\s*(Condos|Apartments|Houses|Townhouses|Lots & Lands)\s*(?:(CI\$|US\$)\s*([\d,]+)|Price Upon Request)(.*?)__([^_]+)__\s*\]\((https://ecaytrade\.com/advert/\d+)\)',
        re.DOTALL
    )
    results = []
    
    # Extract base location from URL if not provided (Grand Cayman, Cayman Brac, Little Cayman)
    base_location = location
    if not base_location and url:
        base_location = get_location_from_url(url)
    
    for match in pattern.finditer(md_text):
        name = match.group(1).strip()
        image_link = match.group(2).strip()
        property_type = match.group(3).strip()
        currency = match.group(4)
        price = match.group(5)
        additional_content = match.group(6).strip()
        specific_location = match.group(7).strip()  # Location from __Location__ pattern
        link = match.group(8)
        
        # Handle price formatting
        if currency and price:
            # Regular price case
            price_clean = price.replace(",", "")
            currency_clean = currency
        else:
            price_clean = "0"  # Use "0" to indicate price upon request in numeric fields
            currency_clean = "US$"
        
        # Normalize the property type using the utility function
        listing_type = normalize_listing_type(property_type)
        
        # Format location with base location appended
        if specific_location:
            final_location = f"{specific_location}, {base_location}"
        else:
            final_location = base_location
        
        results.append({
            "name": name,
            "currency": currency_clean,
            "price": price_clean,
            "link": link,
            "listing_type": listing_type,
            "image_link": image_link,
            "location": final_location,  # Store formatted location with base appended
            "base_location": base_location,  # Store the URL-derived location as well
            "raw_property_type": property_type  # Keep original for debugging
        })
    
    return results

def process_saved_category_results(category):
    """Process saved crawl results for a category, parse listings, and return results."""
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
        
        # Extract location from the URL in the crawl data
        url = crawl_data.get('url')
        location = get_location_from_url(url) if url else None
        
        # Parse the markdown content with location info
        parsed_listings = parse_markdown_list(crawl_data['markdown'], url, location)
        
        if not parsed_listings:
            log_message(f"📭 No listings found in {os.path.basename(filepath)}")
            continue
        
        log_message(f"✅ Parsed {len(parsed_listings)} listings from {os.path.basename(filepath)} ({location})")
        all_category_listings.extend(parsed_listings)
    
    return all_category_listings

async def main():
    # Initialize webhook logger
    webhook_logger = WebhookLogger()
    
    # Initialize tracking variables for notification
    category_results = []
    new_listings_saved = 0
    
    # Base URLs for each category
    base_urls = [
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Bodden%20Town/Breakers,East%20End/High%20Rock,George%20Town,North%20Side,Red%20Bay/Prospect,Rum%20Point/Kaibo,Savannah/Newlands,Seven%20Mile%20Beach,Seven%20Mile%20Beach%20Corridor,South%20Sound,Spotts,West%20Bay&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Cayman%20Brac&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Little%20Cayman&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Bodden%20Town/Breakers,East%20End/High%20Rock,George%20Town,North%20Side,Red%20Bay/Prospect,Rum%20Point/Kaibo,Savannah/Newlands,Seven%20Mile%20Beach,Seven%20Mile%20Beach%20Corridor,South%20Sound,Spotts,West%20Bay&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Cayman%20Brac&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Little%20Cayman&sort=date-high"
    ]
    
    # ===== PHASE 1: CRAWL AND SAVE RAW RESULTS =====
    # Check if we already have saved results for today
    skip_crawling = os.path.exists(RAW_RESULTS_DIR) and os.listdir(RAW_RESULTS_DIR)
    
    if skip_crawling:
        log_message(f"📁 Found existing crawl data in {RAW_RESULTS_DIR}")
        log_message("⏭️ Skipping crawling phase - using existing files")
    else:
        try:
            log_message("📡 PHASE 1: Crawling pages and saving raw results...")
            
            # Create an instance of AsyncWebCrawler
            async with AsyncWebCrawler() as crawler:
                cleaned_md_generator = DefaultMarkdownGenerator(
                    content_source="raw_html",  # This is the default
                )

                config = CrawlerRunConfig(
                    css_selector="div#listing-results",
                    markdown_generator = cleaned_md_generator,
                    wait_for_images = False,
                    scan_full_page = True,
                    scroll_delay=1, 
                )

                # Crawl each category and save raw results
                total_pages_crawled = 0
                for base_url in base_urls:
                    category = get_category_name(base_url)
                    location = get_location_from_url(base_url)
                    log_message(f"\n🏗️ Starting {category.upper()} category crawl for {location}")
                    
                    pages_crawled = await crawl_category_pages(crawler, base_url, config)
                    total_pages_crawled += pages_crawled
                    
                    log_message(f"✅ {category.upper()} ({location}) crawling complete: {pages_crawled} pages saved")
                
                log_message(f"🎯 PHASE 1 COMPLETE: {total_pages_crawled} total pages crawled and saved")
                
        except Exception as e:
            error_message = f"Failed during crawling phase: {e}"
            log_message(f"❌ {error_message}")
            
            # Send failure notification
            try:
                webhook_logger.send_detailed_notification(
                    script_name="ecaytrade.py",
                    status="failure",
                    error_message=error_message
                )
            except Exception as webhook_error:
                log_message(f"Warning: Failed to send failure webhook: {webhook_error}")
            return  # Stop execution
    
    # ===== PHASE 2: PROCESS SAVED RESULTS =====
    try:
        log_message("\n🔧 PHASE 2: Processing saved results...")
        
        # Process each category's saved results and group by URL
        parsed_listings_by_url = {}
        categories = ["properties", "land"]
        
        for category in categories:
            log_message(f"\n📋 Processing saved {category.upper()} results...")
            
            # Parse listings from saved files
            category_listings = process_saved_category_results(category)
            
            # Apply currency conversion to all parsed listings
            if category_listings:
                log_message(f"🔄 Converting currencies for {len(category_listings)} {category} listings...")
                for listing in category_listings:
                    original_currency = listing.get('currency', 'CI$')
                    original_price = listing.get('price', '0')
                    converted_currency, converted_price = convert_ci_to_usd(original_price, original_currency)
                    listing['currency'] = converted_currency
                    listing['price'] = converted_price
                log_message(f"✅ Currency conversion completed for {category} listings")
            
            if not category_listings:
                log_message(f"⚠️ No listings found for {category.upper()}")
                # Track category results even if empty
                category_result = {
                    "category": category,
                    "url": f"ecaytrade.com/{category}",
                    "new_listings": 0,
                    "existing_skipped": 0
                }
                category_results.append(category_result)
                continue
            
            # Get the base URL for this category
            saved_files = get_saved_crawl_files(category)
            if saved_files:
                first_crawl_data = load_crawl_result(saved_files[0])
                if first_crawl_data:
                    base_url = first_crawl_data['url']
                    parsed_listings_by_url[base_url] = deduplicate_listings(category_listings)
                    log_message(f"✅ Prepared {len(category_listings)} {category} listings for duplicate detection")
                    
                    # Track category results for webhook notification
                    category_result = {
                        "category": category,
                        "url": base_url,
                        "new_listings": 0,  # Will be updated after duplicate detection
                        "existing_skipped": 0  # Will be updated after duplicate detection
                    }
                    category_results.append(category_result)
        
        # ===== PHASE 3: DUPLICATE DETECTION AND BATCH SAVE =====
        if parsed_listings_by_url:
            log_message(f"\n🔍 PHASE 3: Running duplicate detection and batch save...")
            total_listings = sum(len(listings) for listings in parsed_listings_by_url.values())
            
            # Call MLS listing filter
            success, prepared_listings = filter_mls_listings(parsed_listings_by_url)
            
            if success:
                # Save new listings to Supabase using existing function like cireba.py
                if prepared_listings:
                    log_message(f"💾 Saving {len(prepared_listings)} new listings to Supabase...")
                    
                    # Use the first URL as target_url for saving
                    first_url = next(iter(parsed_listings_by_url.keys())) if parsed_listings_by_url else ""
                    
                    # Save to ecaytrade_listings table - prepared_listings already in correct format
                    if save_to_ecaytrade_table(first_url, deduplicate_listings(prepared_listings)):
                        new_listings_saved = len(prepared_listings)
                        log_message(f"✅ Successfully saved {len(prepared_listings)} new listings to Supabase")
                    else:
                        log_message(f"❌ Failed to save listings to Supabase")
                else:
                    log_message(f"ℹ️ No new listings to save (all were duplicates or below threshold)")
                    
                # Update category results with actual new listings count
                if category_results:
                    # Split new listings evenly across categories for notification
                    listings_per_category = new_listings_saved // len(category_results) if category_results else 0
                    for i, category_result in enumerate(category_results):
                        category_result["new_listings"] = listings_per_category
                        if i == 0:  # Add remainder to first category
                            category_result["new_listings"] += new_listings_saved % len(category_results)
                        category_result["existing_skipped"] = total_listings - new_listings_saved
                            
            else:
                log_message(f"\n⚠️ PROCESSING COMPLETED WITH WARNINGS. Check duplicate-detector log for details.")
        else:
            log_message(f"\n⚠️ No listings found to process.")
        
        # Send webhook notification
        try:
            webhook_logger.send_detailed_notification(
                script_name="ecaytrade.py",
                status="success",
                existing_mls_count=0,  # ecaytrade doesn't track existing MLS like cireba
                category_results=category_results,
                new_mls_saved=new_listings_saved,
                removed_mls_details=[]  # ecaytrade doesn't track removed listings
            )
            log_message("📬 Webhook notification sent successfully")
        except Exception as e:
            log_message(f"Warning: Failed to send webhook notification: {e}")
        
    except Exception as e:
        # Handle any processing errors
        error_message = f"Failed during processing phase: {e}"
        log_message(f"❌ {error_message}")
        
        # Send failure notification
        try:
            webhook_logger.send_detailed_notification(
                script_name="ecaytrade.py",
                status="failure",
                error_message=error_message
            )
        except Exception as webhook_error:
            log_message(f"Warning: Failed to send failure webhook: {webhook_error}")
        return  # Stop execution

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())