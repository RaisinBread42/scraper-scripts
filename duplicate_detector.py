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

# Create log file with today's date for MLS listing detector
MLS_FILTER_LOG_FILE = f"mls-listing-detector-{datetime.now().strftime('%Y-%m-%d')}.txt"

def log_mls_filter_message(message):
    """Write message to MLS listing filter log file."""
    with open(MLS_FILTER_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")

class MLSListingDetector:
    def __init__(self):
        self.existing_mls_cache = []
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
            log_mls_filter_message(f"❌ Failed to initialize Supabase: {e}")
            return False
    
    
    def load_existing_mls_cache(self) -> bool:
        """Load all existing MLS listings from Cireba into memory cache"""
        log_mls_filter_message("🔄 Loading existing MLS listings into memory cache...")
        
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
                
                self.existing_mls_cache.append({
                    'id': listing['id'],
                    'name': listing.get('name', ''),
                    'price_usd': price_usd,
                    'link': listing.get('link', ''),
                    'source': self.extract_source_from_url(listing.get('target_url', ''))
                })
            
            log_mls_filter_message(f"✅ Loaded {len(self.existing_mls_cache)} existing listings into cache")
            return True
            
        except Exception as e:
            log_mls_filter_message(f"❌ Error loading existing listings: {e}")
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
        
        for existing in self.existing_mls_cache:
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
            log_mls_filter_message(f"⏭️ Skipped listing below $200k threshold: {listing_name[:30]}... (${price_usd:,.0f})")
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
        
        log_mls_filter_message(f"📤 Sending webhook notification for {len(self.mls_matches_found)} duplicates...")
        
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
            match_info = dup['duplicate_match']
            
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
                log_mls_filter_message(f"✅ Webhook sent successfully")
                return True
            else:
                log_mls_filter_message(f"⚠️ Webhook returned status code: {response.status_code}")
                # Don't treat webhook errors as failures that prevent saving
                return True
                
        except Exception as e:
            log_mls_filter_message(f"❌ Error sending webhook: {e}")
            return False
    
    def get_new_listings_for_save(self) -> List[Dict]:
        """Return processed new listings ready for Supabase save"""
        if not self.filtered_listings:
            log_mls_filter_message("ℹ️ No new listings to prepare for save")
            return []
        
        log_mls_filter_message(f"📋 Preparing {len(self.filtered_listings)} new listings for save...")
        
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
        
        log_mls_filter_message(f"✅ Prepared {len(prepared_listings)} listings for save")
        return prepared_listings
    
    def log_final_summary(self):
        """Log final processing summary"""
        total_processed = len(self.mls_matches_found) + len(self.filtered_listings)
        
        log_mls_filter_message("🏆 DUPLICATE DETECTION COMPLETE!")
        log_mls_filter_message(f"📊 Total listings processed: {total_processed}")
        log_mls_filter_message(f"🆕 New listings ready for save: {len(self.filtered_listings)}")
        log_mls_filter_message(f"🔄 Duplicates detected: {len(self.mls_matches_found)}")
        
        if self.mls_matches_found:
            log_mls_filter_message(f"📤 Webhook notification sent for duplicates")

def filter_mls_listings(parsed_listings_by_url: Dict[str, List[Dict]]) -> Tuple[bool, List[Dict]]:
    """
    Main function to filter EcayTrade listings against existing MLS listings
    
    Args:
        parsed_listings_by_url: Dict mapping source URLs to lists of parsed listings
        
    Returns:
        Tuple[bool, List[Dict]]: (success, prepared_listings_for_save)
    """
    detector = MLSListingDetector()
    
    # Phase 1: Load existing MLS cache
    if not detector.load_existing_mls_cache():
        log_mls_filter_message("❌ Failed to load existing MLS cache")
        return False, []
    
    # Phase 2: Process all new listings
    log_mls_filter_message("🔍 Processing new listings for duplicates...")
    
    total_input_listings = 0
    for source_url, listings in parsed_listings_by_url.items():
        total_input_listings += len(listings)
        log_mls_filter_message(f"Processing {len(listings)} listings from {source_url}")
        
        for listing in listings:
            detector.process_listing(listing, source_url)
    
    log_mls_filter_message(f"✅ Processed {total_input_listings} input listings")
    log_mls_filter_message(f"   → {len(detector.new_listings)} new listings (>= $200k USD)")
    log_mls_filter_message(f"   → {len(detector.duplicates_found)} duplicates detected")
    
    # Phase 3: Webhook for duplicates
    webhook_success = detector.send_batch_webhook()
    
    # Phase 4: Prepare listings for save (but don't save them)
    prepared_listings = detector.get_new_listings_for_save()
    
    # Phase 5: Final summary
    detector.log_final_summary()
    
    return webhook_success, prepared_listings

if __name__ == "__main__":
    # Test with sample data
    sample_data = {
        "https://ecaytrade.com/real-estate/for-sale?page=1": [
            {
                "name": "Test Luxury Condo",
                "currency": "CI$",
                "price": "450000",
                "link": "https://ecaytrade.com/advert/123456",
                "listing_type": "Condo"
            }
        ]
    }
    
    log_mls_filter_message("🧪 Testing MLS listing filter with sample data...")
    success, prepared_listings = filter_mls_listings(sample_data)
    log_mls_filter_message(f"Test result: {'✅ Success' if success else '❌ Failed'}")
    log_mls_filter_message(f"Prepared {len(prepared_listings)} listings for save")