import os
from dotenv import load_dotenv
from webhook_logger import WebhookLogger

# Load environment variables
load_dotenv()

def test_success_webhook():
    """Test webhook with a success scenario based on actual log data."""
    
    logger = WebhookLogger()
    
    # Example category results based on supabase-2025-08-15.txt
    category_results = [
        {
            "category": "condos", 
            "url": "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N", 
            "new_listings": 1, 
            "existing_skipped": 694
        },
        {
            "category": "homes", 
            "url": "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N", 
            "new_listings": 0, 
            "existing_skipped": 170
        },
        {
            "category": "duplexes", 
            "url": "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_5/filterby_N", 
            "new_listings": 0, 
            "existing_skipped": 52
        },
        {
            "category": "land", 
            "url": "https://www.cireba.com/cayman-land-for-sale/filterby_N", 
            "new_listings": 1, 
            "existing_skipped": 459
        }
    ]
    
    # Example removed MLS numbers from log
    removed_mls = ["418082", "419630", "418079", "419604", "418530", "419278", "419533", "419280"]
    
    print("🧪 Testing SUCCESS webhook...")
    success = logger.send_detailed_notification(
        script_name="cireba.py",
        status="success",
        existing_mls_count=1382,
        category_results=category_results,
        new_mls_saved=2,
        removed_mls_details=removed_mls
    )
    
    if success:
        print("✅ Success webhook test completed")
    else:
        print("❌ Success webhook test failed")
    
    return success

def test_failure_webhook():
    """Test webhook with a failure scenario."""
    
    logger = WebhookLogger()
    
    print("\n🧪 Testing FAILURE webhook...")
    success = logger.send_detailed_notification(
        script_name="cireba.py",
        status="failure",
        error_message="Connection timeout to target website after 3 retry attempts"
    )
    
    if success:
        print("✅ Failure webhook test completed")
    else:
        print("❌ Failure webhook test failed")
    
    return success

def test_minimal_success_webhook():
    """Test webhook with minimal success data (no new listings, no removals)."""
    
    logger = WebhookLogger()
    
    category_results = [
        {"category": "condos", "url": "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N", "new_listings": 0, "existing_skipped": 695},
        {"category": "homes", "url": "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N", "new_listings": 0, "existing_skipped": 170},
        {"category": "duplexes", "url": "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_5/filterby_N", "new_listings": 0, "existing_skipped": 52},
        {"category": "land", "url": "https://www.cireba.com/cayman-land-for-sale/filterby_N", "new_listings": 0, "existing_skipped": 460}
    ]
    
    print("\n🧪 Testing MINIMAL SUCCESS webhook (no new listings)...")
    success = logger.send_detailed_notification(
        script_name="cireba.py",
        status="success",
        existing_mls_count=1382,
        category_results=category_results,
        new_mls_saved=0,
        removed_mls_details=[]
    )
    
    if success:
        print("✅ Minimal success webhook test completed")
    else:
        print("❌ Minimal success webhook test failed")
    
    return success

if __name__ == "__main__":
    print("🚀 Starting webhook tests...")
    print(f"Webhook URL: {os.environ.get('N8N_WEBHOOK_URL', 'NOT SET')}")
    
    if not os.environ.get('N8N_WEBHOOK_URL'):
        print("❌ N8N_WEBHOOK_URL not set in environment variables")
        print("Please add N8N_WEBHOOK_URL=your_webhook_url to your .env file")
        exit(1)
    
    # Run all tests
    test_success_webhook()
    test_failure_webhook() 
    test_minimal_success_webhook()
    
    print("\n🏁 All webhook tests completed!")