import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_supabase, deduplicate_listings

# Load environment variables from .env file
load_dotenv()  # Add this line

def parse_markdown_list(md_text):
    """
    Extracts name, price, currency, and link from Azure Realty Cayman markdown.
    Returns a list of dicts: {name, price, currency, link}
    """
    import re

    # Pattern for property block: [ ![...](...) ... NAME ... ](LINK "NAME")
    block_pattern = re.compile(
        r'\[ !\[.*?\]\([^\)]*\)\s+Residential\s+(.*?)\s+[^\[]*?\]\((https://www\.azurerealtycayman\.com/property-detail/[^\s)]+)',
        re.IGNORECASE
    )
    # Pattern for price: CI$1,275,000 or US$1,499,000
    price_pattern = re.compile(r'(CI\$|US\$)\s*([\d,]+)')

    results = []
    for match in block_pattern.finditer(md_text):
        name = match.group(1).strip()
        link = match.group(2).strip()
        # Search for the next price after this block
        after = md_text[match.end():]
        price_match = price_pattern.search(after)
        if price_match:
            currency = price_match.group(1)
            price = price_match.group(2).replace(",", "")
        else:
            currency = ""
            price = ""
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
            css_selector="div#Gridbody",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://www.azurerealtycayman.com/cayman-islands-residential-properties-for-sale/filterby_N",
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