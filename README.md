# Real Estate Listings Scraper System

## Business Overview

This automated system collects and processes real estate listings from multiple sources in the Cayman Islands market, providing comprehensive property data for market analysis and business intelligence. The system scrapes listings from major real estate platforms, eliminates duplicates, and stores standardized data in a centralized database.

### Key Business Value
- **Market Intelligence**: Automated collection of property listings across all major platforms
- **Data Standardization**: Normalized property types, prices (converted to USD), and locations
- **Duplicate Prevention**: Advanced filtering prevents data redundancy and maintains data quality
- **Real-time Monitoring**: Webhook notifications for new listings and system status updates
- **Audit Trail**: Complete job history and logging for compliance and troubleshooting

## System Architecture

### High-Level Workflow
The system operates through the **Master Orchestrator** (`run_all_scrapers.py`) which executes three primary phases:

1. **Data Collection Phase**: Scrapes listings from Cireba.com and EcayTrade.com
2. **Data Processing Phase**: Filters duplicates and standardizes data formats
3. **Maintenance Phase**: Cleans up log files and temporary data

## Master Orchestrator: `run_all_scrapers.py`

### Business Process Flow

#### Phase 1: Market Data Collection (35 minutes total)
1. **Cireba Scraper** (20-minute timeout)
   - Scrapes all property categories across Grand Cayman, Cayman Brac, and Little Cayman
   - Processes homes, condos, duplexes, and land properties
   - Applies $100K minimum price filter to focus on significant listings

2. **EcayTrade Scraper** (15-minute timeout)
   - Scrapes properties and land listings from all three islands
   - Uses different price thresholds ($100K for properties, $25K for land)
   - Includes MLS duplicate detection and filtering. Only want to include non-MLS listings

#### Phase 2: Data Maintenance (5 minutes)
3. **Log Cleanup** (5-minute timeout)
   - Removes log files older than 3 days
   - Cleans temporary crawl result directories
   - Maintains system storage efficiency

#### Key Features
- **Job History Tracking**: Records each run in database for audit purposes
- **Error Handling**: Individual script failures don't stop the entire process
- **Comprehensive Logging**: Detailed logs with timestamps for troubleshooting
- **Status Reporting**: Success/failure summary with script-level details

---

## Individual Components

### 1. Cireba Scraper (`cireba.py`)

**Business Purpose**: Extracts listings from Cireba.com, the primary MLS platform in the Cayman Islands.

#### Key Capabilities
- **Multi-Island Coverage**: Grand Cayman, Cayman Brac, Little Cayman
- **Category Segmentation**: Homes, Condos, Land, Commercial properties
- **Price Filtering**: $100K minimum to focus on substantial listings
- **MLS Integration**: Direct access to official MLS data

#### Data Processing Pipeline
1. **Web Crawling**: Uses advanced markdown extraction for clean data parsing
2. **Data Standardization**: Converts all prices to USD, normalizes property types
3. **Geographic Organization**: Categorizes listings by island and district
4. **Database Storage**: Saves to `cireba_listings` table with full property details

#### Business Metrics Captured
- Property name, type, and location
- Pricing in standardized USD currency
- Property specifications (beds, baths, square footage)
- MLS numbers for unique identification
- High-resolution image links

### 2. EcayTrade Scraper (`ecaytrade.py`)

**Business Purpose**: Captures listings from EcayTrade.com to provide comprehensive market coverage beyond MLS listings.

#### Key Capabilities
- **Comprehensive Coverage**: Properties ($100K+) and Land ($25K+)
- **Multi-Location Processing**: All three Cayman Islands
- **Intelligent Crawling**: Automatic page detection and processing
- **Raw Data Preservation**: Saves crawl results for potential reprocessing

#### Advanced Features
- **Currency Conversion**: Automatic CI$ to USD conversion (rate: 1 CI$ = 1.2195121951219512195121951219512 USD)
- **Location Enhancement**: Combines specific and general location data
- **Duplicate Prevention**: Integration with MLS filter before database storage
- **Robust Error Handling**: Continues processing even if individual pages fail

#### Three-Phase Processing
1. **Crawl Phase**: Downloads and saves raw HTML data
2. **Parse Phase**: Extracts structured listing data from saved HTML
3. **Filter Phase**: Applies MLS duplicate detection before database storage

### 3. MLS Listing Filter (`ecaytrade_mls_filter.py`)

**Business Purpose**: Prevents duplicate listings between MLS (Cireba) and secondary sources (EcayTrade) to maintain data quality.

#### Core Business Logic
- **Price Matching**: Uses $100 tolerance for price comparisons (accounts for listing variations)
- **Name Similarity**: 85% fuzzy matching threshold to catch similar property names
- **Quality Threshold**: Only processes listings $200K+ to focus on significant properties
- **Audit Trail**: Detailed logging of all matches and filtering decisions

#### Duplicate Detection Process
1. **MLS Loading**: Retrieves all existing Cireba listings for comparison
2. **New Listing Processing**: Evaluates each EcayTrade listing against MLS database
3. **Match Analysis**: Combines price and name similarity scoring
4. **Webhook Notifications**: Sends duplicate alerts to monitoring system
5. **Clean Data Output**: Returns only unique listings for database storage

#### Business Impact
- **Data Quality**: Eliminates redundant listings across platforms
- **Cost Efficiency**: Reduces storage and processing overhead
- **Market Accuracy**: Provides true unique listing counts for market analysis

### 4. Database Utilities (`utilities/supabase_utils.py`)

**Business Purpose**: Provides standardized data storage and retrieval operations for all scrapers.

#### Key Functions
- **Property Type Normalization**: Standardizes categories (Home, Land, Condo, etc.)
- **Duplicate Detection**: Database-level duplicate prevention within sources
- **Batch Operations**: Efficient bulk data insertion and updates
- **Job History Tracking**: Maintains audit trail of all scraping activities

#### Data Quality Features
- **Field Validation**: Ensures data integrity before database insertion
- **Type Conversion**: Handles numeric conversions for prices, square footage, etc.
- **Null Handling**: Manages missing data gracefully
- **Constraint Enforcement**: Prevents invalid data from entering system

### 5. Webhook Notifications (`webhook_logger.py`)

**Business Purpose**: Provides real-time monitoring and alerting for system operations.

#### Notification Types
- **Success Notifications**: Summary statistics for completed scraping runs
- **Failure Alerts**: Immediate notification of system errors or failures
- **Duplicate Reports**: Detailed information about filtered duplicate listings
- **Performance Metrics**: Runtime statistics and data volumes processed

### 6. System Maintenance (`cleanup_logs.py`)

**Business Purpose**: Maintains system performance and storage efficiency through automated cleanup.

#### Cleanup Operations
- **Log File Management**: Removes logs older than 3 days
- **Temporary Data Cleanup**: Clears crawl result directories after processing
- **Storage Optimization**: Reports space freed and maintains system efficiency
- **Selective Preservation**: Keeps recent files for troubleshooting purposes

---

## Data Storage Schema

### Database Tables

#### `cireba_listings`
- **Purpose**: Stores MLS listings from Cireba.com
- **Key Fields**: MLS number, property details, pricing, location data
- **Business Use**: Primary source for official market data

#### `ecaytrade_listings` 
- **Purpose**: Stores unique non-MLS listings from EcayTrade.com
- **Key Fields**: Property name, specifications, pricing, location
- **Business Use**: Secondary market data for comprehensive coverage

#### `scraping_job_history`
- **Purpose**: Audit trail of all system operations
- **Key Fields**: Script name, execution timestamp, status
- **Business Use**: System monitoring and compliance reporting

---

## Configuration and Requirements

### Environment Variables Required
```
SUPABASE_URL=<your-supabase-project-url>
SUPABASE_SERVICE_ROLE_KEY=<your-service-role-key>
```

### Python Dependencies
- **Web Crawling**: `crawl4ai` for advanced HTML processing
- **Database**: `supabase` for cloud database operations
- **Data Processing**: `fuzzywuzzy` for duplicate detection
- **Utilities**: `requests`, `dotenv` for HTTP and configuration

### System Requirements
- **Runtime**: Python 3.8+
- **Memory**: 2GB+ RAM recommended for large crawling operations
- **Storage**: 1GB+ free space for temporary data and logs
- **Network**: Stable internet connection for web scraping and database operations

---

## Monitoring and Troubleshooting

### Log Files Generated
- `run-all-scrapers-YYYY-MM-DD.txt`: Master orchestrator logs
- `cireba-YYYY-MM-DD.txt`: Cireba scraper detailed logs
- `ecaytrade-YYYY-MM-DD.txt`: EcayTrade scraper detailed logs
- `mls-listing-detector-YYYY-MM-DD.txt`: Duplicate detection logs
- `supabase-YYYY-MM-DD.txt`: Database operation logs

### Performance Metrics
- **Cireba Processing**: Typically processes 500-1000 listings per run
- **EcayTrade Processing**: Handles 200-500 listings with duplicate filtering
- **Runtime**: Complete cycle typically completes in 10-15 minutes


---

## Usage Instructions

### Daily Automated Run
```bash
python run_all_scrapers.py
```

### Individual Component Testing
```bash
python cireba.py          # Test MLS scraping only
python ecaytrade.py       # Test EcayTrade scraping only
python cleanup_logs.py    # Test maintenance operations only
```


This system provides comprehensive, automated real estate data collection with enterprise-grade reliability, data quality controls, and monitoring capabilities.