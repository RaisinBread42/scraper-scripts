import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_supabase

# Load environment variables from .env file
load_dotenv()  # Add this line

def parse_markdown_list(md_text):
    """
    Extracts name, price, currency, and link from Property Cayman markdown.
    Returns a list of dicts: {name, price, currency, link}
    """
    import re

    # Pattern for price/name/link block, e.g.:
    # [ CI$ 330,000  Sandscape Residences #19 Condo in West Bay * MLS: 419608 ](https://www.propertycayman.com/buy/condo/sandscape-residences-19)
    block_pattern = re.compile(
        r'\[\s*(CI\$|US\$)\s*([\d,]+)\s+(.*?)\s*\* MLS: \d+\s*\]\((https://www\.propertycayman\.com/buy/[^\s)]+)\)',
        re.IGNORECASE
    )

    results = []
    for match in block_pattern.finditer(md_text):
        currency = match.group(1)
        price = match.group(2).replace(",", "")
        name = match.group(3).strip()
        link = match.group(4).strip()
        results.append({
            "name": name,
            "currency": currency,
            "price": price,
            "link": link
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
            css_selector="ul.grid-x.grid-margin-x.property-list.main-content",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://www.propertycayman.com/buy/?currency=mixed&sort=most-recent&type=properties",
            "https://www.propertycayman.com/buy/?current_page=2&currency=mixed&sort=most-recent&type=properties",
            "https://www.propertycayman.com/buy/?current_page=3&currency=mixed&sort=most-recent&type=properties"
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
                save_to_supabase(urls[i], parsed_listings)
                
                print(f"Found {len(parsed_listings)} listings on page {i+1}")
            else:
                print(f"Failed to crawl page {i+1}: {urls[i]}")
        
        print(f"\nTotal listings found: {len(all_listings)}")

# Run the async main function
asyncio.run(main())