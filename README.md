# Real Estate Listings Scraper System

## Overview

This system scrapes real estate listings from Cireba.com and EcayTrade.com in the Cayman Islands, filters out duplicates, and stores the data in a Supabase database.

### What it does
- Scrapes property listings from two main real estate websites
- Converts prices to USD and standardizes data
- Removes duplicate MLS listings between sources
- Sends notifications about runs and errors
- Keeps logs and cleans up old data

## How it works

The main script `run_all_scrapers.py` runs these steps:

1. **Scrape Cireba** (20 min timeout) - Gets MLS listings 
2. **Scrape EcayTrade** (15 min timeout) - Gets listings, filters out MLS duplicates
3. **Clean logs** (5 min timeout) - Removes old log files
4. **Clean database** (10 min timeout) - Removes listings older than 3 days

## Scripts

### 1. Cireba Scraper (`cireba.py`)

Gets listings from Cireba.com (the main MLS site).

**What it does:**
- Scrapes homes, condos, and land from all three islands (limited to first 5 pages per base URL)
- Only gets properties $100K+ to focus on significant listings
- Processes in 3 phases: fetching → parsing → saving to database

**Recent changes:**
- No more silent failures - all conversion errors trigger webhook notifications
- Script stops if parsing fails to ensure data quality
- Added duplicate removal by link to prevent processing the same listing multiple times

### 2. EcayTrade Scraper (`ecaytrade.py`)

Gets listings from EcayTrade.com but filters out ones that are already in MLS.

**What it does:**
- Scrapes properties $100K+ and land $25K+ (limited to first 5 pages per base URL)
- Converts CI$ to USD (rate: 1.22)
- Filters out MLS duplicates by crawling each listing URL to check for MLS numbers
- Processes in 4 phases: fetching → parsing → MLS filtering → saving

**Recent changes:**
- No more silent failures - conversion errors trigger webhooks instead of setting price to 0
- All field conversions raise exceptions on failure
- Added duplicate removal by link to prevent processing the same listing multiple times

### 3. MLS Filter (`ecaytrade_mls_filter.py`)

Prevents duplicate listings between MLS (Cireba) and EcayTrade.

**How it works:**
1. For each EcayTrade listing, crawls the listing URL directly
2. Uses regex to find MLS numbers like "MLS#: 123456", "MLS-123456", or "Multiple Listing Service: 123456"
3. Filters out any listings that contain MLS numbers (no price matching needed)

**Recent changes:**
- Removed loading of existing MLS listings from database
- Simplified to direct URL crawling for MLS number detection
- Removed price matching entirely - now only checks for MLS number presence
- Limited crawling to first 5 pages per base URL for efficiency

### 4. Database Utils (`utilities/supabase_utils.py`)

Handles saving data to Supabase database.

- Normalizes property types
- Prevents duplicates within each source
- Tracks job history for monitoring

### 5. Webhook Logger (`webhook_logger.py`)

Sends notifications about scraper runs.

- Success notifications with statistics
- Failure alerts with error details  
- Reports on filtered MLS duplicates

### 6. Log Cleanup (`cleanup_logs.py`)

Removes log files older than 3 days to save storage space.

### 7. Database Cleanup (`cleanup_database.py`)

Removes listings older than 3 days from both tables to control database size and costs.

## Database Tables

- `cireba_listings` - MLS listings from Cireba.com
- `ecaytrade_listings` - Non-MLS listings from EcayTrade.com  
- `scraping_job_history` - Audit trail of all scraper runs

## Setup

### Environment Variables
```
SUPABASE_URL=<your-supabase-url>
SUPABASE_SERVICE_ROLE_KEY=<your-service-key>
N8N_WEBHOOK_URL=<your-n8n-webhook-url>
```

### Requirements
- Python 3.8+
- `crawl4ai` for web scraping and URL crawling
- `supabase` for database
- Stable internet connection

## Recent Improvements

### Better Error Handling
- All conversion errors now trigger webhook notifications
- No more silent failures that set prices to 0
- Scripts fail fast on parsing errors

### Better MLS Detection  
- Simplified to direct URL crawling for each EcayTrade listing
- Uses regex pattern matching to detect any MLS number presence
- Eliminated database lookups and price matching for faster processing
- Focuses only on latest listings by limiting to 5 pages per base URL

### Better Data Quality
- Price validation prevents zero-price listings
- All numeric conversions validated with error reporting

## Usage

### Run everything
```bash
python run_all_scrapers.py
```

### Test individual scripts
```bash
python cireba.py            # Test Cireba scraping
python ecaytrade.py         # Test EcayTrade scraping  
python cleanup_logs.py      # Test log cleanup
python cleanup_database.py  # Test database cleanup
```

## Performance

- Cireba: ~200-500 listings per run (limited to 5 pages per base URL)
- EcayTrade: ~100-300 listings after MLS filtering (limited to 5 pages per base URL)  
- MLS URL crawling: adds 1-2 seconds per listing checked
- Total runtime: 10-20 minutes with reduced page crawling and direct MLS detection

## Logs

Logs are output to console and webhook notifications are sent for errors and status updates.