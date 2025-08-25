#!/usr/bin/env python3
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from supabase import create_client, Client
from dotenv import load_dotenv
from fuzzywuzzy import fuzz

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
                    'id, name, price, currency, link, target_url'
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
    
    def exact_price_match(self, price1_usd: float, price2_usd: float, tolerance: float = 100.0) -> bool:
        """Check if two USD prices match within tolerance"""
        return abs(price1_usd - price2_usd) <= tolerance
    
    def fuzzy_name_match(self, name1: str, name2: str, threshold: int = 85) -> float:
        """Calculate fuzzy similarity between two listing names"""
        if not name1 or not name2:
            return 0.0
        
        # Clean names for comparison
        clean_name1 = name1.lower().strip()
        clean_name2 = name2.lower().strip()
        
        # Use ratio for general similarity
        similarity = fuzz.ratio(clean_name1, clean_name2)
        return similarity
    
    def check_mls_match(self, new_listing: Dict) -> Optional[Dict]:
        """Check if EcayTrade listing matches existing MLS listing"""

        price = new_listing.get('price', 0)
        new_name = new_listing.get('name', '')
        
        for existing in self.mls_listings:
            # First check: exact price match
            if self.exact_price_match(price, existing['price']):
                # Second check: fuzzy name match
                similarity = self.fuzzy_name_match(new_name, existing['name'])
                if similarity >= 85.0:
                    return {
                        'existing_listing': existing,
                        'similarity_score': similarity
                    }
        
        return None
    
    def process_listing(self, listing: Dict) -> None:
        """Process a single listing for duplicates - assumes USD pricing"""

        # Check for MLS matches
        mls_match = self.check_mls_match(listing)
        
        if not mls_match:
            # Add to filtered listings (not in MLS)
            self.filtered_listings.append(listing)
    
def filter_mls_listings(parsed_listings_by_url: Dict[str, List[Dict]]) -> Tuple[bool, List[Dict]]:
    """
    Main function to filter EcayTrade listings against existing MLS listings
    
    Args:
        parsed_listings_by_url: Dict mapping source URLs to lists of parsed listings
        
    Returns:
        Tuple[bool, List[Dict]]: (success, prepared_listings_for_save)
    """
    detector = MLSListingDetector()
    
    # Phase 1: Load MLS listings
    if not detector.load_mls_listings():
        return False, []
    
    # Phase 2: Process all new listings
    for source_url, listings in parsed_listings_by_url.items():
        for listing in listings:
            detector.process_listing(listing, source_url)
    
    # Phase 3: Prepare filtered listings for save
    prepared_listings = detector.filtered_listings
    
    return True, prepared_listings
