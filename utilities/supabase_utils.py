import os
from supabase import create_client, Client
from typing import List, Dict
from datetime import datetime
from webhook_logger import WebhookLogger, trigger_failed_webhook_notification


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

def prepare_listing_row(result: Dict, target_url: str, include_mls: bool = True) -> Dict:
    """
    Prepare a single listing result for database insertion.
    Assumes data is already cleaned and validated.
    """
    row = {
        "target_url": target_url,
        "name": result.get('name'),
        "sqft": result.get('sqft'),
        "beds": result.get('beds'),
        "baths": result.get('baths'),
        "location": result.get('location'),
        "currency": result.get('currency'),
        "price": result.get('price'),
        "link": result.get('link'),
        "image_link": result.get('image_link'),
        "type": normalize_listing_type(result.get('listing_type')),
        "acres": result.get('acres')
    }

    if include_mls:
        row["mls_number"]: result.get('mls_number')

    return row

def save_to_listings_table(results: List[Dict], table_name: str, include_mls: bool = True) -> bool:
    """
    Save parsed results to specified listings table.
    
    Args:
        target_url: The URL that was scraped
        results: List of dictionaries with scraped data
        table_name: Name of the table to save to ('cireba_listings' or 'ecaytrade_listings')
        include_mls: Whether to include mls_number field (True for cireba, False for ecaytrade)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        webhook_logger = WebhookLogger()

        # Initialize Supabase client with service role key
        supabase: Client = create_client(
            os.environ.get("SUPABASE_URL"), 
            os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        )
        
        # Prepare data for insertion - each result becomes a separate row
        rows_to_insert = []
        for result in results:
            row = prepare_listing_row(result, result.get('link',''), include_mls)
            rows_to_insert.append(row)

        # Insert all rows at once
        if rows_to_insert:
            response = supabase.table(table_name).insert(rows_to_insert).execute()
            
            if response.data:
                return True
            else:
                return False
        else:
            return True
            
    except Exception as e:
        print(e)
        trigger_failed_webhook_notification(e, "supabase_utils")
        return False

def save_to_supabase(results: List[Dict]) -> bool:
    """
    Save scraping results to Supabase cireba_listings table.
    Legacy function for backward compatibility.
    """
    return save_to_listings_table(results, 'cireba_listings')

def save_to_ecaytrade_table(results: List[Dict]) -> bool:
    """
    Save parsed results to Supabase ecaytrade_listings table.
    
    Args:
        target_url: The URL that was scraped
        results: List of dictionaries with scraped data
        
    Returns:
        bool: True if successful, False otherwise
    """
    return save_to_listings_table(results, 'ecaytrade_listings', include_mls=False)

def save_scraping_job_history(source: str) -> bool:
    """Save scraping job completion to scraping_job_history table."""
    try:
        # Initialize Supabase client with service role key
        supabase: Client = create_client(
            os.environ.get("SUPABASE_URL"), 
            os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        )
        
        # Prepare data for insertion
        row_to_insert = {
            "source": source,
            "created_at": datetime.utcnow().isoformat()
        }
        
        # Insert into scraping_job_history table
        supabase.table('scraping_job_history').insert(row_to_insert).execute()
        
        # If we get here without an exception, it was successful
        return True
            
    except Exception as e:
        return False