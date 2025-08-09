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
            rows_to_insert.append(row)
        
        # Insert all rows at once
        if rows_to_insert:
            response = supabase.table('scraping_results').insert(rows_to_insert).execute()
            
            if response.data:
                print(f"✅ Saved {len(response.data)} listings for {target_url}")
                return True
            else:
                print(f"❌ Failed to save results for {target_url}")
                return False
        else:
            print(f"⚠️ No valid results to save for {target_url}")
            return True
            
    except Exception as e:
        print(f"Error saving to Supabase: {e}")
        return False