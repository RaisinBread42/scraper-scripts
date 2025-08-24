#!/usr/bin/env python3
"""
Database Cleanup Script
Removes cireba_listings and ecaytrade_listings older than 3 days UTC.
"""

import os
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def initialize_supabase():
    """Initialize Supabase client"""
    try:
        supabase = create_client(
            os.environ.get("SUPABASE_URL"), 
            os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        )
        return supabase
    except Exception as e:
        return None

def cleanup_old_listings(supabase: Client, table_name: str, days_old: int = 3) -> tuple[int, bool]:
    """
    Remove listings older than specified days from the given table.
    
    Args:
        supabase: Supabase client instance
        table_name: Name of the table to clean up
        days_old: Number of days old to consider for deletion
        
    Returns:
        tuple: (number_deleted, success)
    """
    
    try:
        # Calculate cutoff date (3 days ago in UTC)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
        cutoff_iso = cutoff_date.isoformat()
        
        
        # First, count how many records will be deleted
        count_response = supabase.table(table_name).select(
            'id', 
            count='exact'
        ).lt('created_at', cutoff_iso).execute()
        
        records_to_delete = count_response.count if hasattr(count_response, 'count') else 0
        
        if records_to_delete == 0:
            return 0, True
        
        
        # Delete old records in batches to avoid timeout
        batch_size = 100
        total_deleted = 0
        
        while True:
            # Get a batch of IDs to delete
            batch_response = supabase.table(table_name).select(
                'id'
            ).lt('created_at', cutoff_iso).limit(batch_size).execute()
            
            if not batch_response.data:
                break
            
            # Extract IDs from the batch
            ids_to_delete = [record['id'] for record in batch_response.data]
            
            # Delete the batch
            delete_response = supabase.table(table_name).delete().in_(
                'id', ids_to_delete
            ).execute()
            
            batch_deleted = len(delete_response.data) if delete_response.data else 0
            total_deleted += batch_deleted
            
            
            # If batch was smaller than batch_size, we're done
            if len(batch_response.data) < batch_size:
                break
        
        return total_deleted, True
        
    except Exception as e:
        return 0, False

def get_table_stats(supabase: Client, table_name: str) -> dict:
    """Get basic statistics about a table."""
    try:
        # Get total count using count metadata (not len of returned data)
        total_response = supabase.table(table_name).select(
            'id', 
            count='exact'
        ).execute()
        
        total_count = total_response.count if hasattr(total_response, 'count') else 0
        
        # Get count of recent records (last 7 days)
        recent_cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        recent_response = supabase.table(table_name).select(
            'id', 
            count='exact'
        ).gte('created_at', recent_cutoff).execute()
        
        recent_count = recent_response.count if hasattr(recent_response, 'count') else 0
        
        return {
            'total': total_count,
            'recent_7_days': recent_count
        }
    except Exception as e:
        return {'total': 0, 'recent_7_days': 0}

def main():
    """Main database cleanup function."""
    
    # Initialize Supabase
    supabase = initialize_supabase()
    if not supabase:
        return False
    
    # Tables to clean up
    tables_to_cleanup = ['cireba_listings', 'ecaytrade_listings']
    cleanup_days = 3
    
    # Track results
    total_deleted = 0
    all_successful = True
    
    # Get initial statistics
    initial_stats = {}
    for table in tables_to_cleanup:
        stats = get_table_stats(supabase, table)
        initial_stats[table] = stats
    
    
    # Cleanup each table
    for table_name in tables_to_cleanup:
        deleted_count, success = cleanup_old_listings(supabase, table_name, cleanup_days)
        total_deleted += deleted_count
        all_successful = all_successful and success
        
    
    # Get final statistics
    for table in tables_to_cleanup:
        stats = get_table_stats(supabase, table)
        initial = initial_stats.get(table, {'total': 0})
        reduction = initial['total'] - stats['total']
    
    # Summary
    
    if all_successful:
        if total_deleted > 0:
            pass
        else:
            pass
        return True
    else:
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)