import asyncio
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict
from utilities.supabase_utils import save_to_supabase, deduplicate_listings, normalize_listing_type

# Load environment variables from .env file
load_dotenv()  # Add this line

def parse_markdown_list(md_text):
    """
    Extracts name, price, currency, and link from Williams2 Real Estate markdown.
    Returns a list of dicts: {name, price, currency, link}
    """
    import re

    # Pattern for property block: [](property-link)
    block_pattern = re.compile(
        r'\[\]\((https://williams2realestate\.com/property/[^\)]+)\)[\s\S]*?\$[\d,\.]+(?: USD)?\s*/\s*KYD ([\d,\.]+)[^\n]*\n#####\s*([^\n]+)',
        re.IGNORECASE
    )

    results = []
    for match in block_pattern.finditer(md_text):
        link = match.group(1).strip()
        price = match.group(2).replace(",", "").split(".")[0]  # Remove commas and decimals
        name = match.group(3).strip()
        listing_type = normalize_listing_type(name)
        results.append({
            "name": name,
            "currency": "KYD",
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
            css_selector="div#properties",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://williams2realestate.com/search/?action=search_properties&sort=mr&pages=1&action_types=Buy&types%5B%5D=Residential&bedrooms=0",
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