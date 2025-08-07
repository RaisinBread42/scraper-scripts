import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from supabase_utils import save_to_supabase

# Load environment variables from .env file
load_dotenv()  # Add this line

def parse_markdown_list(md_text):
    """
    Extracts name, price, currency, and link from CIREBA markdown.
    Returns a list of dicts: {name, price, currency, link}
    """
    import re

    # Regex to match the property summary block
    # Example: [ MLS#: 419608 Sandscape Residences #19 ... CI$330,000 ](https://www.cireba.com/property-detail/... "...")
    pattern = re.compile(
        r'\[ MLS#:\s*\d+\s+(.*?)\s*[\s\S]*?(CI\$|US\$)\s*([\d,\.]+) \]\((https://www\.cireba\.com/property-detail/[^\s]+)[^)]*\)',
        re.IGNORECASE
    )

    results = []
    for match in pattern.finditer(md_text):
        name = match.group(1).strip()
        currency = match.group(2)
        price = match.group(3).replace(",", "")
        link = match.group(4)
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
                save_to_supabase(urls[i], parsed_listings)
                
                print(f"Found {len(parsed_listings)} listings on page {i+1}")
            else:
                print(f"Failed to crawl page {i+1}: {urls[i]}")
        
        print(f"\nTotal listings found: {len(all_listings)}")

# Run the async main function
asyncio.run(main())