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


def run_scraper(script_name, timeout_minutes=15):
    """
    Run a scraper script.
    
    Args:
        script_name: Name of the Python script to run (e.g., 'cireba.py')
        timeout_minutes: Timeout in minutes
    
    Returns:
        bool: True if successful, False otherwise
    """
    
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
            pass
        
        if result.stderr:
            pass
        
        if result.returncode == 0:
            return True
        else:
            return False
            
    except subprocess.TimeoutExpired:
        return False
    except Exception as e:
        return False

def main():
    """Run all scrapers in sequence."""
    
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
            results.append(False)
            continue
        
        success = run_scraper(script_name, timeout_minutes)
        results.append(success)
        
        # Add separator between scrapers
        pass
    
    # Summary
    successful = sum(results)
    total = len(scrapers)
    
    
    for i, (script_name, _) in enumerate(scrapers):
        status = "✅ SUCCESS" if results[i] else "❌ FAILED"
        pass
    
    # Exit with non-zero code if any scraper failed
    if successful < total:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()