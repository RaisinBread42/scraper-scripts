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
    Extracts name, price, currency, and link from Century21 Cayman markdown.
    Returns a list of dicts: {name, price, currency, link}
    """
    import re

    # Find all image links (each property starts with this)
    img_link_pattern = re.compile(
        r'\[ !\[(.*?)\]\([^\)]*\) \]\((https://century21cayman\.com/en/d/[^\s]+) "[^"]*"\)', re.IGNORECASE
    )
    # Find all price lines (US$ 25,000,000 or CI$ 3,950,000)
    price_pattern = re.compile(r'(US\$|CI\$)\s*([\d,]+(?:\.\d+)?)')

    # Find all property name/URL blocks (##  [ ... ](URL "title"))
    name_link_pattern = re.compile(
        r'##\s*\[\s*(.*?)\s*\]\((https://century21cayman\.com/en/d/[^\s]+)[^)]*\)', re.IGNORECASE
    )

    # Find all matches for each pattern
    img_links = list(img_link_pattern.finditer(md_text))
    prices = list(price_pattern.finditer(md_text))
    name_links = list(name_link_pattern.finditer(md_text))

    # The order of properties is preserved in the markdown, so we can zip them
    results = []
    for i in range(min(len(img_links), len(prices), len(name_links))):
        name = name_links[i].group(1).strip()
        link = name_links[i].group(2).strip()
        currency = prices[i].group(1)
        price = prices[i].group(2).replace(",", "")
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
            css_selector="div#search-ajax-contents",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://century21cayman.com/en/s/for-sale/new-listing",
            "https://century21cayman.com/en/s/for-sale/new-listing/hga/2",
            "https://century21cayman.com/en/s/for-sale/new-listing/hga/3"
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