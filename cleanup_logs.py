#!/usr/bin/env python3
"""
Log Cleanup Script
Removes log files and temporary directories older than 3 days.
"""

import os
import glob
import shutil
from datetime import datetime, timedelta
from pathlib import Path

def log_cleanup_message(message):
    """Print cleanup message with timestamp."""
    print(f"{datetime.now().strftime('%H:%M:%S')} - {message}")

def is_older_than_days(file_path, days=3):
    """Check if file/directory is older than specified days."""
    try:
        file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        cutoff_time = datetime.now() - timedelta(days=days)
        return file_time < cutoff_time
    except OSError:
        return False

def cleanup_log_files(days=3):
    """Remove log files older than specified days."""
    log_cleanup_message(f"Cleaning up log files older than {days} days...")
    
    # Define log file patterns
    log_patterns = [
        "cireba-*.txt",                    # cireba.py logs
        "ecaytrade-*.txt",                 # ecaytrade.py logs  
        "supabase-*.txt",                  # supabase_utils.py logs
        "mls-listing-detector-*.txt",      # ecaytrade_mls_filter.py logs
        "run-all-scrapers-*.txt",          # run_all_scrapers.py logs
        "database-cleanup-*.txt",          # cleanup_database.py logs
    ]
    
    total_deleted = 0
    total_size_freed = 0
    
    for pattern in log_patterns:
        matching_files = glob.glob(pattern)
        log_cleanup_message(f"Checking pattern: {pattern} - Found {len(matching_files)} files")
        
        for file_path in matching_files:
            if is_older_than_days(file_path, days):
                try:
                    file_size = os.path.getsize(file_path)
                    os.remove(file_path)
                    total_deleted += 1
                    total_size_freed += file_size
                    log_cleanup_message(f"DELETED: {file_path} ({file_size:,} bytes)")
                except OSError as e:
                    log_cleanup_message(f"ERROR deleting {file_path}: {e}")
            else:
                log_cleanup_message(f"KEEPING recent file: {file_path}")
    
    return total_deleted, total_size_freed

def cleanup_crawl_directories(days=3):
    """Remove raw crawl result directories older than specified days."""
    log_cleanup_message(f"Cleaning up crawl directories older than {days} days...")
    
    # Define directory patterns
    directory_patterns = [
        "raw_crawl_results_*",           # cireba.py raw crawl results
        "raw_crawl_results_ecaytrade_*", # ecaytrade.py raw crawl results
    ]
    
    total_deleted = 0
    total_size_freed = 0
    
    for pattern in directory_patterns:
        matching_dirs = glob.glob(pattern)
        log_cleanup_message(f"Checking pattern: {pattern} - Found {len(matching_dirs)} directories")
        
        for dir_path in matching_dirs:
            if os.path.isdir(dir_path) and is_older_than_days(dir_path, days):
                try:
                    # Calculate directory size
                    dir_size = sum(
                        os.path.getsize(os.path.join(dirpath, filename))
                        for dirpath, dirnames, filenames in os.walk(dir_path)
                        for filename in filenames
                    )
                    
                    shutil.rmtree(dir_path)
                    total_deleted += 1
                    total_size_freed += dir_size
                    log_cleanup_message(f"DELETED directory: {dir_path} ({dir_size:,} bytes)")
                except OSError as e:
                    log_cleanup_message(f"ERROR deleting directory {dir_path}: {e}")
            else:
                if os.path.isdir(dir_path):
                    log_cleanup_message(f"KEEPING recent directory: {dir_path}")
    
    return total_deleted, total_size_freed

def cleanup_temp_files(days=3):
    """Remove temporary files older than specified days."""
    log_cleanup_message(f"Cleaning up temporary files older than {days} days...")
    
    # Define temporary file patterns
    temp_patterns = [
        "crawl_results.md",              # Temporary crawl results
        "*.tmp",                         # Generic temp files
        "*.temp",                        # Generic temp files
    ]
    
    total_deleted = 0
    total_size_freed = 0
    
    for pattern in temp_patterns:
        matching_files = glob.glob(pattern)
        
        for file_path in matching_files:
            if is_older_than_days(file_path, days):
                try:
                    file_size = os.path.getsize(file_path)
                    os.remove(file_path)
                    total_deleted += 1
                    total_size_freed += file_size
                    log_cleanup_message(f"DELETED temp file: {file_path} ({file_size:,} bytes)")
                except OSError as e:
                    log_cleanup_message(f"ERROR deleting {file_path}: {e}")
    
    return total_deleted, total_size_freed

def format_bytes(bytes_size):
    """Format bytes into human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} TB"

def main():
    """Main cleanup function."""
    log_cleanup_message("Starting log cleanup process...")
    
    # Set cleanup age (3 days)
    cleanup_days = 3
    
    # Initialize counters
    total_files_deleted = 0
    total_dirs_deleted = 0
    total_temp_deleted = 0
    total_size_freed = 0
    
    try:
        # Cleanup log files
        files_deleted, size_freed = cleanup_log_files(cleanup_days)
        total_files_deleted += files_deleted
        total_size_freed += size_freed
        
        log_cleanup_message("=" * 60)
        
        # Cleanup crawl directories
        dirs_deleted, size_freed = cleanup_crawl_directories(cleanup_days)
        total_dirs_deleted += dirs_deleted
        total_size_freed += size_freed
        
        log_cleanup_message("=" * 60)
        
        # Cleanup temporary files
        temp_deleted, size_freed = cleanup_temp_files(cleanup_days)
        total_temp_deleted += temp_deleted
        total_size_freed += size_freed
        
        # Summary
        log_cleanup_message("=" * 60)
        log_cleanup_message("CLEANUP SUMMARY:")
        log_cleanup_message(f"   Log files deleted: {total_files_deleted}")
        log_cleanup_message(f"   Directories deleted: {total_dirs_deleted}")
        log_cleanup_message(f"   Temp files deleted: {total_temp_deleted}")
        log_cleanup_message(f"   Total space freed: {format_bytes(total_size_freed)}")
        
        total_items = total_files_deleted + total_dirs_deleted + total_temp_deleted
        if total_items > 0:
            log_cleanup_message(f"SUCCESS: Cleanup completed! Removed {total_items} items.")
        else:
            log_cleanup_message("INFO: No files older than 3 days found. Nothing to clean up.")
    
    except Exception as e:
        log_cleanup_message(f"ERROR: Cleanup failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)