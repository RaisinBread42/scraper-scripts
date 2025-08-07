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
    Extracts name, price, currency, and link from RE/MAX Cayman markdown.
    Returns a list of dicts: {name, price, currency, link}
    """
    import re

    # Pattern for property name and link (the second link after the image block)
    name_link_pattern = re.compile(
        r'\[ !\[.*?\]\([^\)]*\)(?:\s*!.*?\([^\)]*\))* \]\((https://www\.remax\.ky/listings/[^\s)]+)\)[\s\S]*?\[\s*([^\]]+?)\s*\]\((https://www\.remax\.ky/listings/[^\s)]+)\)', re.IGNORECASE
    )
    # Pattern for price, e.g.: $579,000 KYD or $1,150,000 USD
    price_pattern = re.compile(r'\$([\d,]+)\s*(KYD|USD)', re.IGNORECASE)

    results = []
    for match in name_link_pattern.finditer(md_text):
        # The property link is the second link (not the image gallery)
        link = match.group(3).strip()
        name = match.group(2).strip()
        # Search for the next price after the name/link block
        after = md_text[match.end():]
        price_match = price_pattern.search(after)
        if price_match:
            price = price_match.group(1).replace(",", "")
            currency = price_match.group(2)
        else:
            price = ""
            currency = ""
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
            css_selector="main#main",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://www.remax.ky/listings/?sort=Latest",
            "https://www.remax.ky/listings/page/2/?sort=Latest",
            "https://www.remax.ky/listings/page/3/?sort=Latest"
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