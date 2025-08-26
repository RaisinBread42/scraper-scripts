#!/usr/bin/env python3
import os
import re
import asyncio
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from crawl4ai import CacheMode, CrawlerRunConfig, DefaultMarkdownGenerator
from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig, DefaultMarkdownGenerator

"""
MLS Listing Filter Script for Property Listings
Filters out EcayTrade listings that are already included in Cireba MLS listings.
"""

# Load environment variables
load_dotenv()

class MLSListingDetector:
    def __init__(self):
        self.filtered_listings = []
    
    async def check_mls_number_in_listing(self, listing_url: str) -> bool:
        """Crawl EcayTrade listing URL and check for MLS number via regex"""
        try:
            async with AsyncWebCrawler() as crawler:
                cleaned_md_generator = DefaultMarkdownGenerator(content_source="raw_html")
                config = CrawlerRunConfig(
                    target_elements="p",
                    markdown_generator=cleaned_md_generator,
                    cache_mode=CacheMode.BYPASS,
                    wait_for_images=False,
                    scan_full_page=True,
                    scroll_delay=0.3
                )
                result = await crawler.arun(url=listing_url, config=config)
                
                if not result or not result.markdown:
                    raise Exception('failure getting mls listing from listing url')
                
                # Regex pattern to find MLS numbers (common formats: MLS-123456, MLS#123456, MLS 123456, MLS#: 419589, etc.)
                mls_pattern = r'MLS[#\s-]*:?\s*(\d{6,})|Multiple[\s]*Listing[\s]*Service[\s]*[#:]?[\s]*(\d{6,})'
                
                return bool(re.search(mls_pattern, result.markdown, re.IGNORECASE))
                
        except Exception as e:
            return False
    
    async def check_mls_match(self, new_listing: Dict) -> bool:
        """Check if EcayTrade listing as MLS number on details page"""

        listing_url = new_listing.get('link', '')
    
        has_mls_number = await self.check_mls_number_in_listing(listing_url) # doesn't have to be same MLS number, just any!

        if has_mls_number:
            return True
        else:
            return False
        
        return False
    
    async def process_listing(self, listing: Dict) -> None:
        """Process a single listing for duplicates - assumes USD pricing"""

        # Check for MLS matches
        mls_match = await self.check_mls_match(listing)
        
        if not mls_match:
            # Add to filtered listings (not in MLS)
            self.filtered_listings.append(listing)
    
async def filter_mls_listings(parsed_listings: List[Dict]) -> Tuple[bool, List[Dict]]:
    """
    Main function to filter EcayTrade listings against existing MLS listings
    
    Args:
        parsed_listings: List of parsed listings to filter
        
    Returns:
        Tuple[bool, List[Dict]]: (success, prepared_listings_for_save)
    """
    detector = MLSListingDetector()

    # Phase 1: Process all new listings
    for listing in parsed_listings:
        await detector.process_listing(listing)
    
    # Phase 3: Prepare filtered listings for save
    prepared_listings = detector.filtered_listings
    
    return True, prepared_listings
