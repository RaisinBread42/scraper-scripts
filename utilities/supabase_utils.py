import os
from supabase import create_client, Client
from typing import List, Dict

def deduplicate_listings(listings):
    """
    Removes duplicates from a list of dicts based on name, price, and currency.
    Returns a new list with unique listings.
    """
    seen = set()
    unique = []
    for item in listings:
        key = (item.get("name"), item.get("price"), item.get("currency"))
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