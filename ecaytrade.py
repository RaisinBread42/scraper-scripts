import re
import asyncio
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_ecaytrade_table
from datetime import datetime
from ecaytrade_mls_filter import filter_mls_listings
from webhook_logger import WebhookLogger

# Load environment variables from .env file
load_dotenv()  # Add this line

def clean_and_validate_listings(listings: List[Dict]) -> List[Dict]:
    """Clean and validate all listing data including currency conversion."""
    cleaned_listings = []
    
    for listing in listings:
        # Currency conversion
        currency = listing.get('currency', 'CI$')
        price = listing.get('price', 0)
        
        if currency == "CI$" and price:
            try:
                usd_amount = price * 1.2195121951219512195121951219512
                listing['currency'] = "US$"
                listing['price'] = round(usd_amount, 2)
            except (ValueError, TypeError):
                listing['price'] = 0.0
        else:
            try:
                listing['price'] = float(price) if price else 0.0
            except (ValueError, TypeError):
                listing['price'] = 0.0
        
        # Data type validation
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

def get_location_from_url(url):
    """Extract location from URL based on location parameter."""
    if "location=Cayman%20Brac" in url:
        return "Cayman Brac"
    elif "location=Little%20Cayman" in url:
        return "Little Cayman"
    else:
        return "Grand Cayman"


async def crawl_category_pages(crawler, base_url, config):
    """Crawl all pages of a category and collect parsed listings until no more listings are found."""
    page_number = 1
    all_listings = []
    
    while page_number <=1:
        current_url = base_url.replace("page=1", f"page={page_number}")
        
        result = await crawler.arun(url=current_url, config=config)

        if not result.success:
            error_message = str(result.error_message) if hasattr(result, 'error_message') and result.error_message else f"Failed to crawl {current_url}"
            raise Exception(f"Crawling failed on page {page_number}: {error_message}")
        
        if not result.markdown or len(result.markdown.strip()) < 100:
            break
        
        parsed_listings = parse_markdown_list(result.markdown, current_url)
        if parsed_listings:
            all_listings.extend(parsed_listings)
        
        page_number += 1
    
    return all_listings

def parse_markdown_list(md_text, url=None):
    # Updated regex to capture location from __Location__ pattern in the markdown
    # Pattern captures: [ ![NAME](IMG) PROPERTY_TYPE (PRICE or "Price Upon Request") CONTENT __LOCATION__ ](LINK)
    pattern = re.compile(
        r'\[ !\[(.*?)\]\(([^\)]*)\)\s*(Condos|Apartments|Houses|Townhouses|Duplexes|Lots & Lands)\s*(?:(CI\$|US\$)\s*([\d,]+)|Price Upon Request)(.*?)__([^_]+)__\s*\]\((https://ecaytrade\.com/advert/\d+)\)',
        re.DOTALL
    )
    results = []
    
    # Extract base location from URL if not provided (Grand Cayman, Cayman Brac, Little Cayman)
    base_location = get_location_from_url(url)
    
    for match in pattern.finditer(md_text):
        name = match.group(1).strip()
        image_link = match.group(2).strip()
        property_type = match.group(3).strip()
        currency = match.group(4)
        price = match.group(5)
        specific_location = match.group(7).strip()  # Location from __Location__ pattern
        link = match.group(8)
        
        # Format location with base location appended
        if specific_location and specific_location == "Grand Cayman":
            final_location = f"{specific_location}, {base_location}"
        else:
            final_location = base_location # otherwise cayman brac or little cayman shows twice.
        
        results.append({
            "name": name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": property_type,
            "image_link": image_link,
            "location": final_location,
            "base_location": base_location,
        })
    
    return results

def trigger_failed_webhook_notification(e, webhook_logger):
        error_message = str(e)
        
        # Send failure notification
        webhook_logger.send_detailed_notification(
            script_name="ecaytrade.py",
            status="failure",
            error_message=error_message
        )

async def main():
    webhook_logger = WebhookLogger()

    base_urls = [
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Bodden%20Town/Breakers,East%20End/High%20Rock,George%20Town,North%20Side,Red%20Bay/Prospect,Rum%20Point/Kaibo,Savannah/Newlands,Seven%20Mile%20Beach,Seven%20Mile%20Beach%20Corridor,South%20Sound,Spotts,West%20Bay&sort=date-high",
        #"https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Cayman%20Brac&sort=date-high",
        #"https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Little%20Cayman&sort=date-high",
        #"https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Bodden%20Town/Breakers,East%20End/High%20Rock,George%20Town,North%20Side,Red%20Bay/Prospect,Rum%20Point/Kaibo,Savannah/Newlands,Seven%20Mile%20Beach,Seven%20Mile%20Beach%20Corridor,South%20Sound,Spotts,West%20Bay&sort=date-high",
        #"https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Cayman%20Brac&sort=date-high",
        #"https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Little%20Cayman&sort=date-high"
    ]
    
    # ===== PHASE 1: FETCHING CRAWLED DATA =====
    all_listings = []
    try:
        async with AsyncWebCrawler() as crawler:
            cleaned_md_generator = DefaultMarkdownGenerator(content_source="raw_html")

            config = CrawlerRunConfig(
                css_selector="div#listing-results",
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
    
    # ===== PHASE 3: REMOVING MLS LISTINGS =====
    filtered_listings = []
    try:
        if parsed_listings:
            success, filtered_listings = filter_mls_listings(parsed_listings)
            if not success:
                raise Exception("MLS filtering failed")
                
    except Exception as e:
        print(f"Failed during removing mls listings: {e}")
        return
    
    # ===== PHASE 4: SAVING TO SUPABASE =====
    try:
        if filtered_listings:
            save_to_ecaytrade_table(filtered_listings)
                
            webhook_logger.send_detailed_notification(
            script_name="ecaytrade.py",
            status="success",
            category_results=filtered_listings
        )
    except Exception as e:
        print(f"Failed during saving to supabase: {e}")
        trigger_failed_webhook_notification(e, webhook_logger)
        return

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())