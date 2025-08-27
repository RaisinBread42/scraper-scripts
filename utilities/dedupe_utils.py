from typing import List, Dict

def dedupe_listings_by_url(listings: List[Dict]) -> List[Dict]:
    """
    Remove duplicate listings based on exact URL match.
    Keeps the first occurrence of each unique URL.
    
    Args:
        listings: List of listing dictionaries containing 'link' field
        
    Returns:
        List of deduplicated listings
    """
    seen_urls = set()
    deduplicated_listings = []
    
    for listing in listings:
        url = listing.get('link', '')
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduplicated_listings.append(listing)
    
    return deduplicated_listings