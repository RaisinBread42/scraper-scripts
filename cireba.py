import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_supabase, deduplicate_listings, normalize_listing_type

# Load environment variables from .env file
load_dotenv()  # Add this line

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
            "location": location
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
            "location": location
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
            "location": location
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

async def main():
    # Create an instance of AsyncWebCrawler
    async with AsyncWebCrawler() as crawler:
        # Run the crawler on a URL

        cleaned_md_generator = DefaultMarkdownGenerator(
            content_source="cleaned_html",  # This is the default
        )

        config = CrawlerRunConfig(
            # e.g., first 30 items from Hacker News
            css_selector="div#grid-view",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=1, 
        )

        urls = [
            # condos - pages 1-24
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#2",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#3",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#4",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#5",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#6",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#7",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#8",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#9",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#10",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#11",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#12",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#13",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#14",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#15",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#16",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#17",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#18",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#19",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#20",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#21",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#22",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#23",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#24",
            
            #single family homes
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N#2",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N#3",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N#4",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N#5",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N#6",

            # duplexes
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_5/filterby_N",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_5/filterby_N#2"

            # land - pages 1-16
            "https://www.cireba.com/cayman-land-for-sale/filterby_N",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#2",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#3",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#4",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#5",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#6",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#7",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#8",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#9",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#10",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#11",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#12",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#13",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#14",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#15",
            "https://www.cireba.com/cayman-land-for-sale/filterby_N#16"
        ]

        results = await crawler.arun_many(urls=urls, config=config)
        
        # Process each crawled page
        all_listings = []
        for i, result in enumerate(results):
            if result.success:
                print(f"Processing page {i+1}: {urls[i]}")
                
                # Parse the markdown content
                parsed_listings = parse_markdown_list(result.markdown, urls[i])
                all_listings.extend(parsed_listings)
                
                # Save each page's results to Supabase
                save_to_supabase(urls[i], deduplicate_listings(parsed_listings))
                
                print(f"Found {len(parsed_listings)} listings on page {i+1}")
            else:
                print(f"Failed to crawl page {i+1}: {urls[i]}")
        
        print(f"\nTotal listings found: {len(all_listings)}")

# Run the async main function
asyncio.run(main())