import re
import asyncio
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig, DefaultMarkdownGenerator
from utilities.supabase_utils import deduplicate_listings, normalize_listing_type, save_to_ecaytrade_table
from datetime import datetime
from ecaytrade_mls_filter import filter_mls_listings

# Load environment variables from .env file
load_dotenv()  # Add this line

def convert_ci_to_usd(price, currency):
    """Convert CI$ to USD using exact rate: 1 CI$ = 1.2195121951219512195121951219512 USD"""
    if currency == "CI$" and price:
        try:
            usd_amount = price * 1.2195121951219512195121951219512
            return "US$", str(round(usd_amount, 2))
        except ValueError:
            return currency, price
    return currency, price

def get_location_from_url(url):
    """Extract location from URL based on location parameter."""
    if "location=Cayman%20Brac" in url:
        return "Cayman Brac"
    elif "location=Little%20Cayman" in url:
        return "Little Cayman"
    else:
        return "Grand Cayman"


async def crawl_category_pages(crawler, base_url, config):
    """Crawl all pages of a category and collect results in memory until no more listings are found."""
    page_number = 1
    pages_crawled = 0
    crawl_results = []
    
    while True:
        # Build URL with current page number
        current_url = base_url.replace("page=1", f"page={page_number}")
        
        # Crawl the current page
        result = await crawler.arun(url=current_url, config=config)

        if not result.success:
            error_message = str(result.error_message) if hasattr(result, 'error_message') and result.error_message else f"Failed to crawl {current_url}"
            raise Exception(f"Crawling failed on page {page_number}: {error_message}")
        
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
        
        results.append(clean_listing_data({
            "name": name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": property_type,
            "image_link": image_link,
            "location": final_location,  # Store formatted location with base appended
            "base_location": base_location,  # Store the URL-derived location as well
        }))
    
    return results

def clean_listing_data(listing):
    """Clean and convert listing data types"""

    sqft = None
    if listing.get('sqft'):
        try:
            sqft = int(listing['sqft'].replace(',', '')) if isinstance(listing['sqft'], str) else int(listing['sqft'])
        except (ValueError, TypeError):
            sqft = None
    
    beds = None
    if listing.get('beds'):
        try:
            beds = int(float(listing['beds'])) if isinstance(listing['beds'], str) else int(listing['beds'])
        except (ValueError, TypeError):
            beds = None
    
    baths = None
    if listing.get('baths'):
        try:
            baths = int(float(listing['baths'])) if isinstance(listing['baths'], str) else int(listing['baths'])
        except (ValueError, TypeError):
            baths = None
    
    price = None
    if listing.get('price'):
        try:
            price = float(listing['price'].replace(',', '')) if isinstance(listing['price'], str) else float(listing['price'])
        except (ValueError, TypeError):
            price = 0.0
    
    acres = None
    if listing.get('acres'):
        try:
            acres = float(listing['acres']) if isinstance(listing['acres'], str) else float(listing['acres'])
        except (ValueError, TypeError):
            acres = None
    
    # convert currency to US
    converted_currency, converted_price = convert_ci_to_usd(price, listing.get('currency', 'CI$'))
    
    listing['currency'] = converted_currency
    listing['price'] = converted_price

    # Normalize listing type
    listing_type = normalize_listing_type(listing.get('listing_type', ''))
    
    # Update the listing with clean data
    cleaned_listing = listing.copy()
    cleaned_listing.update({
        'sqft': sqft,
        'beds': beds,
        'baths': baths,
        'price': price,
        'acres': acres,
        'listing_type': listing_type
    })
    
    return cleaned_listing

async def main():
    
    # Base URLs for each category
    base_urls = [
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Bodden%20Town/Breakers,East%20End/High%20Rock,George%20Town,North%20Side,Red%20Bay/Prospect,Rum%20Point/Kaibo,Savannah/Newlands,Seven%20Mile%20Beach,Seven%20Mile%20Beach%20Corridor,South%20Sound,Spotts,West%20Bay&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Cayman%20Brac&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Little%20Cayman&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Bodden%20Town/Breakers,East%20End/High%20Rock,George%20Town,North%20Side,Red%20Bay/Prospect,Rum%20Point/Kaibo,Savannah/Newlands,Seven%20Mile%20Beach,Seven%20Mile%20Beach%20Corridor,South%20Sound,Spotts,West%20Bay&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Cayman%20Brac&sort=date-high",
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=25000&type=lots--lands&location=Little%20Cayman&sort=date-high"
    ]
    
    # ===== PHASE 1: CRAWL RESULTS =====
    try:
            # Create an instance of AsyncWebCrawler
            async with AsyncWebCrawler() as crawler:
                cleaned_md_generator = DefaultMarkdownGenerator(
                    content_source="raw_html",  # This is the default
                )

                config = CrawlerRunConfig(
                    css_selector="div#listing-results",
                    markdown_generator = cleaned_md_generator,
                    cache_mode=CacheMode.BYPASS,
                    wait_for_images = False,
                    scan_full_page = True, # required for ecaytrade
                    scroll_delay=0.3
                )

                # Crawl each category and collect results in memory
                total_pages_crawled = 0
                all_crawl_results = []
                
                for base_url in base_urls:
                    pages_crawled, crawl_results = await crawl_category_pages(crawler, base_url, config)
                    total_pages_crawled += pages_crawled
                    all_crawl_results.extend(crawl_results)
                    
    except Exception as e:
        print(f"Failed during crawling phase: {e}")
        return  # Stop execution
    
    # ===== PHASE 2: PROCESS ALL CRAWL RESULTS =====
    try:
        # Parse all listings from crawl results
        all_listings = []
        
        for crawl_data in all_crawl_results:
            all_listings.append = parse_markdown_list(crawl_data['markdown'], crawl_data.get('url'))
        
        if all_listings:
            # Group by URL for MLS filtering
            parsed_listings_by_url = {}
            first_url = all_crawl_results[0]['url'] if all_crawl_results else ""
            parsed_listings_by_url[first_url] = all_listings
            
            # Call MLS listing filter
            success, prepared_listings = filter_mls_listings(all_listings)
            
            if success and prepared_listings:
                # Save to ecaytrade_listings table
                save_to_ecaytrade_table(first_url, prepared_listings)
                
        # TODO: Import and call stats/webhook module here
        
    except Exception as e:
        print(f"Failed during processing phase: {e}")
        return  # Stop execution

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())