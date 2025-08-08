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
    Extracts name, price, currency, link, and listing type from Mod Realty Cayman markdown.
    Returns a list of dicts: {name, price, currency, link, listing_type}
    """
    import re

    name_link_pattern = re.compile(
        r'##\s*\[\s*(.*?)\s*\]\((https://modrealtycayman\.com/property/[^\s)]+)\)', re.IGNORECASE
    )
    price_pattern = re.compile(r'(CI\$)\s*([\d,]+)')

    results = []
    for name_match in name_link_pattern.finditer(md_text):
        name = name_match.group(1).strip()
        link = name_match.group(2).strip()
        after = md_text[name_match.end():]
        price_match = price_pattern.search(after)
        # Only use price if it comes before the next property block, else leave blank
        next_name_idx = after.find('## [')
        if price_match and (next_name_idx == -1 or after.find(price_match.group(0)) < next_name_idx):
            currency = price_match.group(1)
            price = price_match.group(2).replace(",", "")
        else:
            currency = ""
            price = ""
        
        # Extract listing type from the name - ModRealty includes property type in the name
        # e.g. "Luxury Turnkey 3-Bedroom at ARZA – West Bay" or "Prime 0.31-Acre Lot"
        listing_type = normalize_listing_type(name)
        
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
            css_selector="div.properties-wrapper.items-wrapper.clearfix",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://modrealtycayman.com/properties-2/?filter-orderby=newest",
            "https://modrealtycayman.com/properties-2/page/2/?filter-orderby=newest",
            "https://modrealtycayman.com/properties-2/page/3/?filter-orderby=newest"
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