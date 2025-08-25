#!/usr/bin/env python3
import os
import re
import asyncio
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from crawl4ai import CacheMode, CrawlerRunConfig, DefaultMarkdownGenerator
from supabase import create_client, Client
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
        self.mls_listings = []
        self.filtered_listings = []
        self.supabase = None
        
    def initialize_supabase(self):
        """Initialize Supabase client"""
        try:
            self.supabase = create_client(
                os.environ.get("SUPABASE_URL"), 
                os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
            )
            return True
        except Exception as e:
            return False
    
    
    def load_mls_listings(self) -> bool:
        """Load all existing MLS listings from Cireba"""
        
        if not self.initialize_supabase():
            return False
        
        try:
            # Fetch all listings with pagination - only from cireba_listings for duplicate comparison
            all_listings = []
            page_size = 1000
            offset = 0
            
            while True:
                response = self.supabase.table('cireba_listings').select(
                    'id, name, price, currency, link, target_url, location'
                ).range(offset, offset + page_size - 1).execute()
                
                if not response.data:
                    break
                
                all_listings.extend(response.data)
                
                if len(response.data) < page_size:
                    break
                    
                offset += page_size
            
            # Convert to cache format - all Supabase listings are already in USD
            for listing in all_listings:
                
                self.mls_listings.append({
                    'id': listing['id'],
                    'name': listing.get('name', ''),
                    'price': listing.get('price', ''),
                    'link': listing.get('link', ''),
                    'location': listing.get('location', ''),
                    'source': self.extract_source_from_url(listing.get('target_url', ''))
                })
            
            return True
            
        except Exception as e:
            return False
    
    def extract_source_from_url(self, url: str) -> str:
        """Extract source name from target URL"""
        if 'cireba.com' in url:
            return 'cireba'
        elif 'ecaytrade.com' in url:
            return 'ecaytrade'
        else:
            return 'unknown'
    
    def exact_price_match(self, price1_usd: float, price2_usd: float, tolerance: float = 10.0) -> bool:
        """Check if two USD prices match within tolerance"""
        return abs(price1_usd - price2_usd) <= tolerance
    
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
        """Check if EcayTrade listing matches existing MLS listing"""

        price = new_listing.get('price', 0)
        listing_url = new_listing.get('link', '')
        
        for existing in self.mls_listings:
            # First check: exact price match
            if self.exact_price_match(price, existing['price']):
                # Second check: crawl listing URL and check for MLS number
                
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
    
    # Phase 1: Load MLS listings
    if not detector.load_mls_listings():
        return False, []
    
    # Phase 2: Process all new listings
    for listing in parsed_listings:
        await detector.process_listing(listing)
    
    # Phase 3: Prepare filtered listings for save
    prepared_listings = detector.filtered_listings
    
    return True, prepared_listings
