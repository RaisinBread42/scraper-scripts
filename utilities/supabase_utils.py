import os
from supabase import create_client, Client
from typing import List, Dict
from datetime import datetime

# Create log file with today's date
SUPABASE_LOG_FILE = f"supabase-{datetime.now().strftime('%Y-%m-%d')}.txt"

def log_supabase_message(message):
    """Write message to supabase log file, overwriting if first message of the day."""
    with open(SUPABASE_LOG_FILE, 'w' if not hasattr(log_supabase_message, 'initialized') else 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
    if not hasattr(log_supabase_message, 'initialized'):
        log_supabase_message.initialized = True

def normalize_listing_type(raw_type):
    """
    Normalize property type to standard categories:
    Home, Land, Condo, Apartment, Townhouse, Commercial, Multi Unit, Duplex
    """
    if not raw_type:
        return 'Home'  # Default
    
    raw_type = raw_type.lower().strip()
    
    # Land types
    if any(keyword in raw_type for keyword in ['land', 'lot', 'vacant']):
        return 'Land'
    
    # Commercial
    elif 'commercial' in raw_type:
        return 'Commercial'
    
    # Multi Unit
    elif any(keyword in raw_type for keyword in ['multi unit', 'multi-unit']):
        return 'Multi Unit'
    
    # Duplex
    elif 'duplex' in raw_type:
        return 'Duplex'
    
    # Triplex
    elif 'triplex' in raw_type:
        return 'Triplex'
    
    # Townhouse
    elif 'townhouse' in raw_type:
        return 'Townhouse'
    
    # Condo
    elif any(keyword in raw_type for keyword in ['condo', 'condominium', 'unit']):
        return 'Condo'
    
    # Apartment
    elif 'apartment' in raw_type:
        return 'Apartment'
    
    # Default fallback
    else:
        return 'Home'

def deduplicate_listings(listings):
    """
    Removes duplicates from a list of dicts based on property link.
    Returns a new list with unique listings.
    """
    seen = set()
    unique = []
    for item in listings:
        key = item.get("link")
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique

def save_to_supabase(target_url: str, results: List[Dict]) -> bool:
    """Save scraping results to Supabase table."""
    try:
        # Initialize Supabase client
        supabase: Client = create_client(
            os.environ.get("SUPABASE_URL"), 
            os.environ.get("SUPABASE_ANON_KEY")
        )
        
        # Prepare data for insertion - each result becomes a separate row
        rows_to_insert = []
        for result in results:
            # Convert string values to appropriate types
            sqft = None
            if result.get('sqft'):
                try:
                    sqft = int(result['sqft'].replace(',', '')) if isinstance(result['sqft'], str) else int(result['sqft'])
                except (ValueError, TypeError):
                    sqft = None
            
            beds = None
            if result.get('beds'):
                try:
                    beds = int(float(result['beds'])) if isinstance(result['beds'], str) else int(result['beds'])
                except (ValueError, TypeError):
                    beds = None
            
            baths = None
            if result.get('baths'):
                try:
                    baths = int(float(result['baths'])) if isinstance(result['baths'], str) else int(result['baths'])
                except (ValueError, TypeError):
                    baths = None
            
            price = None
            if result.get('price'):
                try:
                    price = float(result['price'].replace(',', '')) if isinstance(result['price'], str) else float(result['price'])
                except (ValueError, TypeError):
                    price = None
            
            acres = None
            if result.get('acres'):
                try:
                    acres = float(result['acres']) if isinstance(result['acres'], str) else float(result['acres'])
                except (ValueError, TypeError):
                    acres = None
            
            row = {
                "target_url": target_url,
                "mls_number": result.get('mls_number'),
                "name": result.get('name'),
                "sqft": sqft,
                "beds": beds,
                "baths": baths,
                "location": result.get('location'),
                "currency": result.get('currency'),
                "price": price,
                "link": result.get('link'),
                "image_link": result.get('image_link'),
                "type": normalize_listing_type(result.get('listing_type'))
            }
            
            # Add acres field if it exists (for land listings)
            if acres is not None:
                row["acres"] = acres
            rows_to_insert.append(row)
        
        # Insert all rows at once
        if rows_to_insert:
            response = supabase.table('scraping_results').insert(rows_to_insert).execute()
            
            if response.data:
                log_supabase_message(f"✅ Saved {len(response.data)} listings for {target_url}")
                return True
            else:
                log_supabase_message(f"❌ Failed to save results for {target_url}")
                return False
        else:
            log_supabase_message(f"⚠️ No valid results to save for {target_url}")
            return True
            
    except Exception as e:
        log_supabase_message(f"Error saving to Supabase: {e}")
        return False

def get_existing_mls_numbers() -> set:
    """Fetch all existing MLS numbers from mls_listings table."""
    try:
        # Initialize Supabase client
        supabase: Client = create_client(
            os.environ.get("SUPABASE_URL"), 
            os.environ.get("SUPABASE_ANON_KEY")
        )
        
        # Query all MLS numbers from mls_listings table
        response = supabase.table('mls_listings').select('number').execute()
        
        if response.data:
            # Extract MLS numbers and return as a set for fast lookup
            mls_numbers = {row['number'] for row in response.data if row['number']}
            log_supabase_message(f"✅ Found {len(mls_numbers)} existing MLS numbers in database")
            return mls_numbers
        else:
            log_supabase_message("⚠️ No existing MLS numbers found in database")
            return set()
            
    except Exception as e:
        log_supabase_message(f"Error fetching existing MLS numbers: {e}")
        return set()

def filter_new_listings(listings: List[Dict], existing_mls_numbers: set) -> List[Dict]:
    """Filter out listings that already exist in mls_listings table."""
    new_listings = []
    skipped_count = 0
    
    for listing in listings:
        mls_number = listing.get('mls_number')
        if mls_number and mls_number in existing_mls_numbers:
            skipped_count += 1
        else:
            new_listings.append(listing)
    
    log_supabase_message(f"✅ Filtered {len(new_listings)} new listings, skipped {skipped_count} existing ones")
    return new_listings

def save_new_mls_numbers(mls_numbers: List[str]) -> bool:
    """Save new MLS numbers to mls_listings table."""
    try:
        # Initialize Supabase client - try service role first, fallback to anon key
        service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        anon_key = os.environ.get("SUPABASE_ANON_KEY")
        
        # Use service role key if available (bypasses RLS), otherwise use anon key
        key_to_use = service_key if service_key else anon_key
        supabase: Client = create_client(os.environ.get("SUPABASE_URL"), key_to_use)
        
        # Prepare data for insertion
        rows_to_insert = [{"number": mls_num} for mls_num in mls_numbers if mls_num]
        
        if rows_to_insert:
            response = supabase.table('mls_listings').insert(rows_to_insert).execute()
            
            if response.data:
                log_supabase_message(f"✅ Saved {len(response.data)} new MLS numbers to tracking table")
                return True
            else:
                log_supabase_message(f"❌ Failed to save MLS numbers to tracking table")
                return False
        else:
            log_supabase_message(f"⚠️ No valid MLS numbers to save")
            return True
            
    except Exception as e:
        error_message = str(e)
        # Check if it's an RLS policy violation
        if "row-level security policy" in error_message.lower():
            log_supabase_message(f"⚠️ RLS policy blocks MLS number saving. Consider using SUPABASE_SERVICE_ROLE_KEY or updating RLS policy.")
            log_supabase_message(f"ℹ️ Continuing without MLS tracking - listings will still be saved to scraping_results table")
            return True  # Continue execution even if MLS tracking fails
        else:
            log_supabase_message(f"Error saving MLS numbers: {e}")
            return False

def mark_removed_listings(current_parsed_mls_numbers: set, existing_mls_numbers: set) -> bool:
    """
    Mark MLS listings as removed if they exist in database but not in current parsed results.
    Updates the removed_on field in scraping_results table to current UTC timestamp.
    """
    try:
        # Find MLS numbers that exist in database but not in current parsed results
        removed_mls_numbers = existing_mls_numbers - current_parsed_mls_numbers
        
        if not removed_mls_numbers:
            log_supabase_message("✅ No listings need to be marked as removed")
            return True
        
        # Initialize Supabase client - try service role first, fallback to anon key
        service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        anon_key = os.environ.get("SUPABASE_ANON_KEY")
        
        # Use service role key if available (bypasses RLS), otherwise use anon key
        key_to_use = service_key if service_key else anon_key
        supabase: Client = create_client(os.environ.get("SUPABASE_URL"), key_to_use)
        
        # Update removed_on field for MLS numbers that are no longer available
        current_utc = datetime.utcnow().isoformat()
        
        # Update each removed MLS number in scraping_results table
        successful_updates = 0
        for mls_number in removed_mls_numbers:
            try:
                response = supabase.table('scraping_results')\
                    .update({'removed_on': current_utc})\
                    .eq('mls_number', mls_number)\
                    .execute()
                
                if response.data:
                    successful_updates += 1
                    log_supabase_message(f"🗑️ Marked MLS #{mls_number} as removed on {current_utc}")
                
            except Exception as update_error:
                log_supabase_message(f"❌ Failed to mark MLS #{mls_number} as removed: {update_error}")
        
        log_supabase_message(f"✅ Successfully marked {successful_updates}/{len(removed_mls_numbers)} listings as removed")
        return successful_updates > 0 or len(removed_mls_numbers) == 0
        
    except Exception as e:
        error_message = str(e)
        # Check if it's an RLS policy violation
        if "row-level security policy" in error_message.lower():
            log_supabase_message(f"⚠️ RLS policy blocks marking removed listings. Consider using SUPABASE_SERVICE_ROLE_KEY or updating RLS policy.")
            return True  # Continue execution even if marking removed fails
        else:
            log_supabase_message(f"Error marking removed listings: {e}")
            return False