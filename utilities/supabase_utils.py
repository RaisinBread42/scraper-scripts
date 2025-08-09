import os
from supabase import create_client, Client
from typing import List, Dict

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