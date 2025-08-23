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

# Create log file with today's date
DB_CLEANUP_LOG_FILE = f"database-cleanup-{datetime.now().strftime('%Y-%m-%d')}.txt"

def log_db_cleanup_message(message):
    """Write message to database cleanup log file."""
    with open(DB_CLEANUP_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
    print(message)

def initialize_supabase():
    """Initialize Supabase client"""
    try:
        supabase = create_client(
            os.environ.get("SUPABASE_URL"), 
            os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        )
        return supabase
    except Exception as e:
        log_db_cleanup_message(f"❌ Failed to initialize Supabase: {e}")
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
    log_db_cleanup_message(f"🧹 Cleaning up {table_name} listings older than {days_old} days...")
    
    try:
        # Calculate cutoff date (3 days ago in UTC)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
        cutoff_iso = cutoff_date.isoformat()
        
        log_db_cleanup_message(f"📅 Cutoff date: {cutoff_iso}")
        
        # First, count how many records will be deleted
        count_response = supabase.table(table_name).select(
            'id', 
            count='exact'
        ).lt('created_at', cutoff_iso).execute()
        
        records_to_delete = count_response.count if hasattr(count_response, 'count') else 0
        
        if records_to_delete == 0:
            log_db_cleanup_message(f"ℹ️ No old records found in {table_name}")
            return 0, True
        
        log_db_cleanup_message(f"🎯 Found {records_to_delete} records to delete from {table_name}")
        
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
            
            log_db_cleanup_message(f"🗑️ Deleted batch of {batch_deleted} records from {table_name}")
            
            # If batch was smaller than batch_size, we're done
            if len(batch_response.data) < batch_size:
                break
        
        log_db_cleanup_message(f"✅ Successfully deleted {total_deleted} old records from {table_name}")
        return total_deleted, True
        
    except Exception as e:
        log_db_cleanup_message(f"❌ Error cleaning up {table_name}: {e}")
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
        log_db_cleanup_message(f"⚠️ Could not get stats for {table_name}: {e}")
        return {'total': 0, 'recent_7_days': 0}

def main():
    """Main database cleanup function."""
    log_db_cleanup_message("🚀 Starting database cleanup process...")
    
    # Initialize Supabase
    supabase = initialize_supabase()
    if not supabase:
        log_db_cleanup_message("❌ Failed to initialize database connection. Exiting.")
        return False
    
    # Tables to clean up
    tables_to_cleanup = ['cireba_listings', 'ecaytrade_listings']
    cleanup_days = 3
    
    # Track results
    total_deleted = 0
    all_successful = True
    
    # Get initial statistics
    log_db_cleanup_message("📊 Initial database statistics:")
    initial_stats = {}
    for table in tables_to_cleanup:
        stats = get_table_stats(supabase, table)
        initial_stats[table] = stats
        log_db_cleanup_message(f"   {table}: {stats['total']} total, {stats['recent_7_days']} recent (7 days)")
    
    log_db_cleanup_message("=" * 60)
    
    # Cleanup each table
    for table_name in tables_to_cleanup:
        deleted_count, success = cleanup_old_listings(supabase, table_name, cleanup_days)
        total_deleted += deleted_count
        all_successful = all_successful and success
        
        log_db_cleanup_message("=" * 40)
    
    # Get final statistics
    log_db_cleanup_message("📊 Final database statistics:")
    for table in tables_to_cleanup:
        stats = get_table_stats(supabase, table)
        initial = initial_stats.get(table, {'total': 0})
        reduction = initial['total'] - stats['total']
        log_db_cleanup_message(f"   {table}: {stats['total']} total (-{reduction}), {stats['recent_7_days']} recent (7 days)")
    
    # Summary
    log_db_cleanup_message("=" * 60)
    log_db_cleanup_message("🏆 DATABASE CLEANUP SUMMARY:")
    log_db_cleanup_message(f"   Total records deleted: {total_deleted}")
    log_db_cleanup_message(f"   Cleanup age threshold: {cleanup_days} days")
    log_db_cleanup_message(f"   Tables processed: {len(tables_to_cleanup)}")
    
    if all_successful:
        if total_deleted > 0:
            log_db_cleanup_message(f"✅ SUCCESS: Database cleanup completed! Removed {total_deleted} old records.")
        else:
            log_db_cleanup_message("ℹ️ INFO: Database cleanup completed. No old records found to remove.")
        return True
    else:
        log_db_cleanup_message("⚠️ WARNING: Database cleanup completed with some errors. Check logs for details.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)