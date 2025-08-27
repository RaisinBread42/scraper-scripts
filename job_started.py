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
from webhook_logger import WebhookLogger, trigger_failed_webhook_notification
import webhook_logger

# Set UTF-8 encoding
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

def main():
    # Update job history once at the beginning
    #save_scraping_job_history("running all scrapers")
    
    webhook_logger = WebhookLogger()
    webhook_logger.send_detailed_notification('Run all job started and start date saved', 'success', {})

if __name__ == "__main__":
    main()