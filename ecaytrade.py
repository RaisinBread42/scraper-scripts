import re
import asyncio
from supabase import create_client, Client
import json
import os
from dotenv import load_dotenv 
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from typing import List, Dict

# Load environment variables from .env file
load_dotenv()  # Add this line

def parse_markdown_list(md_text):
    # Regex to match: [ ![NAME](IMG) TYPE PRICE ... NAME ](LINK)
    pattern = re.compile(
        r'\[ !\[(.*?)\]\([^\)]*\) [^\[]*?(CI\$|US\$)\s*([\d,]+).*?\]\((https://ecaytrade\.com/advert/\d+)\)'
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

def save_to_supabase(target_url: str, results: List[Dict]) -> bool:
    """Save scraping results to Supabase table."""
    try:
        # Initialize Supabase client
        supabase: Client = create_client(
            os.environ.get("SUPABASE_URL"), 
            os.environ.get("SUPABASE_ANON_KEY")
        )
        
        # Insert into scraping_results table
        response = supabase.table('scraping_results').insert({
            "target_url": target_url,
            "results": results  # This will be stored as JSONB
        }).execute()
        
        if response.data:
            print(f"✅ Saved {len(results)} results for {target_url}")
            return True
        else:
            print(f"❌ Failed to save results for {target_url}")
            return False
            
    except Exception as e:
        print(f"Error saving to Supabase: {e}")
        return False


async def main():
    # Create an instance of AsyncWebCrawler
    async with AsyncWebCrawler() as crawler:
        # Run the crawler on a URL

        cleaned_md_generator = DefaultMarkdownGenerator(
            content_source="cleaned_html",  # This is the default
        )

        config = CrawlerRunConfig(
            # e.g., first 30 items from Hacker News
            css_selector="div#listing-results",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://ecaytrade.com/real-estate/for-sale?page=1&type=apartments+condos+duplexes+houses+townhouses&sort=date-high",
            "https://ecaytrade.com/real-estate/for-sale?page=2&type=apartments+condos+duplexes+houses+townhouses&sort=date-high",
            "https://ecaytrade.com/real-estate/for-sale?page=3&type=apartments+condos+duplexes+houses+townhouses&sort=date-high"
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