#!/usr/bin/env python3
"""
MLS Listing Filter Script for Property Listings
Filters out EcayTrade listings that are already included in Cireba MLS listings.
"""

import os
import json
import requests
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from supabase import create_client, Client
from dotenv import load_dotenv
try:
    from fuzzywuzzy import fuzz
except ImportError:
    # Silent install - no console output
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "fuzzywuzzy", "python-Levenshtein"], 
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    from fuzzywuzzy import fuzz

# Load environment variables
load_dotenv()

# Webhook URL placeholder - will be replaced with actual URL
WEBHOOK_URL = "https://n8n.obsidiansoftwaredev.com/webhook/13cf59c6-974b-4659-8ebb-2e171b80cbf1"


class MLSListingDetector:
    def __init__(self):
        self.mls_listings = []
        self.mls_matches_found = []
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
                # All listings from Supabase are already in USD, no conversion needed
                price_usd = float(listing.get('price', 0))
                
                self.mls_listings.append({
                    'id': listing['id'],
                    'name': listing.get('name', ''),
                    'price_usd': price_usd,
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
        new_price_usd = new_listing.get('price_usd', 0)
        new_name = new_listing.get('name', '')
        
        for existing in self.mls_listings:
            # First check: exact price match
            if self.exact_price_match(new_price_usd, existing['price_usd']):
                # Second check: fuzzy name match
                similarity = self.fuzzy_name_match(new_name, existing['name'])
                if similarity >= 85.0:
                    return {
                        'existing_listing': existing,
                        'similarity_score': similarity
                    }
        
        return None
    
    def process_listing(self, listing: Dict, source_url: str) -> None:
        """Process a single listing for duplicates - assumes USD pricing"""
        # All listings should already be in USD format
        price_str = listing.get('price', '0')
        try:
            price_usd = float(price_str.replace(",", "")) if price_str else 0.0
        except (ValueError, TypeError):
            price_usd = 0.0
        
        # Skip if below threshold
        if price_usd < 200000:
            listing_name = listing.get('name', '')
            return
        
        # Create processed listing
        processed_listing = listing.copy()
        processed_listing.update({
            'price_usd': price_usd,
            'source_url': source_url,
            'source': self.extract_source_from_url(source_url)
        })
        
        # Check for MLS matches
        mls_match = self.check_mls_match(processed_listing)
        
        if mls_match:
            # Add to MLS matches (listings already in MLS)
            self.mls_matches_found.append({
                'new_listing': processed_listing,
                'mls_match': mls_match
            })
        else:
            # Add to filtered listings (not in MLS)
            self.filtered_listings.append(processed_listing)
    
    def send_batch_webhook(self) -> bool:
        """Send batch webhook notification for all duplicates found"""
        if not self.mls_matches_found:
            return True
        
        
        # Prepare webhook payload
        payload = {
            "event_type": "batch_duplicates_detected",
            "script_run": {
                "source": "ecaytrade",
                "timestamp": datetime.now().isoformat(),
                "total_processed": len(self.mls_matches_found) + len(self.filtered_listings),
                "duplicates_count": len(self.mls_matches_found),
                "new_listings_count": len(self.filtered_listings)
            },
            "duplicates": []
        }
        
        # Add duplicate details
        for dup in self.mls_matches_found:
            new_listing = dup['new_listing']
            match_info = dup['mls_match']
            
            duplicate_entry = {
                "new_listing": {
                    "name": new_listing.get('name', ''),
                    "price_usd": new_listing.get('price_usd', 0),
                    "link": new_listing.get('link', ''),
                    "source": new_listing.get('source', '')
                },
                "matches": [{
                    "id": match_info['existing_listing']['id'],
                    "name": match_info['existing_listing']['name'],
                    "price_usd": match_info['existing_listing']['price_usd'],
                    "similarity_score": match_info['similarity_score'],
                    "link": match_info['existing_listing']['link'],
                    "source": match_info['existing_listing']['source']
                }]
            }
            payload["duplicates"].append(duplicate_entry)
        
        try:
            response = requests.post(
                WEBHOOK_URL,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                return True
            else:
                # Don't treat webhook errors as failures that prevent saving
                return True
                
        except Exception as e:
            return False
    
    def get_new_listings_for_save(self) -> List[Dict]:
        """Return processed new listings ready for Supabase save"""
        if not self.filtered_listings:
            return []
        
        
        prepared_listings = []
        
        for listing in self.filtered_listings:
            # Convert string values to appropriate types
            sqft = None
            if listing.get('sqft'):
                try:
                    sqft = int(listing['sqft'].replace(',', '')) if isinstance(listing['sqft'], str) else int(listing['sqft'])
                except (ValueError, TypeError):
                    sqft = None
            
            beds = None
            if listing.get('beds'):
                try:
                    beds = int(float(listing['beds'])) if isinstance(listing['beds'], str) else int(listing['beds'])
                except (ValueError, TypeError):
                    beds = None
            
            baths = None
            if listing.get('baths'):
                try:
                    baths = int(float(listing['baths'])) if isinstance(listing['baths'], str) else int(listing['baths'])
                except (ValueError, TypeError):
                    baths = None
            
            price = listing.get('price_usd', 0)
            
            acres = None
            if listing.get('acres'):
                try:
                    acres = float(listing['acres']) if isinstance(listing['acres'], str) else float(listing['acres'])
                except (ValueError, TypeError):
                    acres = None
            
            # Use normalize_listing_type from utilities
            from utilities.supabase_utils import normalize_listing_type
            
            row = {
                "target_url": listing.get('source_url', ''),
                "mls_number": listing.get('mls_number'),
                "name": listing.get('name', ''),
                "sqft": sqft,
                "beds": beds,
                "baths": baths,
                "location": listing.get('location'),
                "currency": "US$",  # Always USD
                "price": price,
                "link": listing.get('link', ''),
                "image_link": listing.get('image_link'),
                "listing_type": normalize_listing_type(listing.get('listing_type', ''))
            }
            
            # Add acres field if it exists (for land listings)
            if acres is not None:
                row["acres"] = acres
                
            prepared_listings.append(row)
        
        return prepared_listings
    
    def log_final_summary(self):
        """Log final processing summary"""
        total_processed = len(self.mls_matches_found) + len(self.filtered_listings)
        
        
        if self.mls_matches_found:
            pass

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
    
    total_input_listings = 0
    for source_url, listings in parsed_listings_by_url.items():
        total_input_listings += len(listings)
        
        for listing in listings:
            detector.process_listing(listing, source_url)
    
    
    # Phase 3: Webhook for duplicates
    webhook_success = detector.send_batch_webhook()
    
    # Phase 4: Prepare listings for save (but don't save them)
    prepared_listings = detector.get_new_listings_for_save()
    
    # Phase 5: Final summary
    detector.log_final_summary()
    
    return webhook_success, prepared_listings
