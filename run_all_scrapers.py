#!/usr/bin/env python3
"""
Run all scrapers with job history tracking.
Specifically runs cireba.py and ecaytrade.py in sequence.
"""

import subprocess
import os
import sys
from datetime import datetime
from utilities.supabase_utils import save_scraping_job_history

# Set UTF-8 encoding
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Create log file with today's date
LOG_FILE = f"run-all-scrapers-{datetime.now().strftime('%Y-%m-%d')}.txt"

def log_message(message):
    """Write message to log file."""
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
    print(message)

def run_scraper(script_name, timeout_minutes=15):
    """
    Run a scraper script.
    
    Args:
        script_name: Name of the Python script to run (e.g., 'cireba.py')
        timeout_minutes: Timeout in minutes
    
    Returns:
        bool: True if successful, False otherwise
    """
    log_message(f"🚀 Starting {script_name}...")
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            timeout=timeout_minutes * 60,
            encoding="utf-8"
        )
        
        # Log output
        if result.stdout:
            log_message(f"📄 {script_name} output:")
            log_message(result.stdout)
        
        if result.stderr:
            log_message(f"⚠️ {script_name} errors:")
            log_message(result.stderr)
        
        if result.returncode == 0:
            log_message(f"✅ {script_name} completed successfully")
            return True
        else:
            log_message(f"❌ {script_name} failed with return code {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        log_message(f"⏰ {script_name} timed out after {timeout_minutes} minutes")
        return False
    except Exception as e:
        log_message(f"❌ Error running {script_name}: {e}")
        return False

def main():
    """Run all scrapers in sequence."""
    log_message("🌟 Starting all scrapers run...")
    
    # Update job history once at the beginning
    save_scraping_job_history("running all scrapers")
    
    # Define scrapers to run in order
    scrapers = [
        ("cireba.py", 20),      # script, timeout_minutes
        ("ecaytrade.py", 15),
        ("cleanup_logs.py", 5),
        ("cleanup_database.py", 10),
    ]
    
    results = []
    
    for script_name, timeout_minutes in scrapers:
        # Check if script exists
        if not os.path.exists(script_name):
            log_message(f"❌ Script not found: {script_name}")
            results.append(False)
            continue
        
        success = run_scraper(script_name, timeout_minutes)
        results.append(success)
        
        # Add separator between scrapers
        log_message("=" * 60)
    
    # Summary
    successful = sum(results)
    total = len(scrapers)
    
    log_message(f"🏆 SUMMARY: {successful}/{total} scrapers completed successfully")
    
    for i, (script_name, _) in enumerate(scrapers):
        status = "✅ SUCCESS" if results[i] else "❌ FAILED"
        log_message(f"   {script_name}: {status}")
    
    # Exit with non-zero code if any scraper failed
    if successful < total:
        log_message("⚠️ Some scrapers failed. Check logs for details.")
        sys.exit(1)
    else:
        log_message("🎉 All scrapers completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()