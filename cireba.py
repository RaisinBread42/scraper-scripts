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

def parse_markdown_list(md_text):
    """
    Extracts name, price, currency, and link from CIREBA markdown.
    Returns a list of dicts: {name, price, currency, link}
    """
    import re

    # Pattern for the property block
    block_pattern = re.compile(
        r'\[ MLS#: \d+\s+([^\n]+).*?\n\n([^\[\n]+)\s+(CI\$|US\$)([\d,\.]+) \]\((https://www\.cireba\.com/property-detail/[^\s)]+)',
        re.DOTALL
    )

    results = []
    for match in block_pattern.finditer(md_text):
        name = match.group(1).strip()
        # location = match.group(2).strip()  # Not used, but available
        currency = match.group(3)
        price = match.group(4).replace(",", "")
        link = match.group(5).strip()
        
        # Extract property type from CIREBA URL structure
        # URL format: /property-detail/{location}/{property-type}-for-sale-in-cayman-islands/{property-name}
        url_match = re.search(r'/property-detail/[^/]+/([^-]+)-properties?-for-sale-in-cayman-islands/', link)
        if url_match:
            url_type = url_match.group(1)
            listing_type = normalize_listing_type(url_type)
        else:
            listing_type = normalize_listing_type(name)  # Fallback to name
        results.append({
            "name": name,
            "currency": currency,
            "price": price,
            "link": link,
            "listing_type": listing_type
        })
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
            scroll_delay=0.5, 
        )

        urls = [
            "https://www.cireba.com/cayman-islands-real-estate-listings/filterby_N",
            "https://www.cireba.com/cayman-islands-real-estate-listings/filterby_N#2",
            "https://www.cireba.com/cayman-islands-real-estate-listings/filterby_N#3"
        ]

        results = await crawler.arun_many(urls=urls, config=config)
        
        # Process each crawled page
        all_listings = []
        for i, result in enumerate(results):
            if result.success:
                print(f"Processing page {i+1}: {urls[i]}")
                
                # Parse the markdown content
                parsed_listings = parse_markdown_list(result.markdown)
                all_listings.extend(parsed_listings)
                
                # Save each page's results to Supabase
                save_to_supabase(urls[i], deduplicate_listings(parsed_listings))
                
                print(f"Found {len(parsed_listings)} listings on page {i+1}")
            else:
                print(f"Failed to crawl page {i+1}: {urls[i]}")
        
        print(f"\nTotal listings found: {len(all_listings)}")

# Run the async main function
asyncio.run(main())