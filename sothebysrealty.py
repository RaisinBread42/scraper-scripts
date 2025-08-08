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
    Extracts name, price, currency, and link from Sotheby's Realty markdown.
    Returns a list of dicts: {name, price, currency, link}
    """
    import re

    # Regex to match price lines (KYD $2,333,333)
    price_pattern = re.compile(r'(KYD)\s*\$([\d,]+)')
    # Regex to match property name and link (####  [ Name ](link))
    name_link_pattern = re.compile(
        r'####\s*\[\s*(.*?)\s*\]\((https://www\.sothebysrealty\.ky/properties/[^\s)]+)\)', re.IGNORECASE
    )

    prices = list(price_pattern.finditer(md_text))
    name_links = list(name_link_pattern.finditer(md_text))

    results = []
    for i in range(min(len(prices), len(name_links))):
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
            css_selector="div#listing_ajax_container",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://www.sothebysrealty.ky/property-search/?property_status=new",
            "https://www.sothebysrealty.ky/property-search/page/2/?property_status=new",
            "https://www.sothebysrealty.ky/property-search/page/3/?property_status=new"
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