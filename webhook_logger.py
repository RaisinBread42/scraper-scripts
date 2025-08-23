import os
import requests
from typing import Optional, List, Dict
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class WebhookLogger:
    """Webhook logger that sends detailed scraping logs to n8n workflow."""
    
    def __init__(self):
        self.webhook_url = os.environ.get("N8N_WEBHOOK_URL")
        self.scrape_events = []  # Collect all log events during scraping
    
    def add_event(self, message: str, event_type: str = "info"):
        """Add a scraping event to the log collection."""
        self.scrape_events.append({
            "timestamp": datetime.now().strftime('%H:%M:%S'),
            "message": message,
            "type": event_type
        })
    
    def send_detailed_notification(self, 
                                 script_name: str, 
                                 status: str,
                                 existing_mls_count: int = 0,
                                 category_results: Optional[List[Dict]] = None,
                                 new_mls_saved: int = 0,
                                 error_message: Optional[str] = None) -> bool:
        """
        Send detailed webhook notification with scraping results that match supabase log format.
        
        Args:
            script_name: Name of the scraping script (e.g. 'cireba.py')
            status: 'success' or 'failure'
            existing_mls_count: Number of existing MLS numbers found in database
            category_results: List of dicts with category scraping results
            new_mls_saved: Number of new MLS numbers saved to tracking table
            error_message: Error message if status is 'failure'
        
        Returns:
            bool: True if webhook triggered successfully, False otherwise
        """
        if not self.webhook_url:
            print("⚠️ N8N_WEBHOOK_URL not configured in environment variables")
            return False
        
        try:
            # Calculate totals from category results
            total_new_listings = 0
            total_existing_skipped = 0
            
            if category_results:
                for result in category_results:
                    total_new_listings += result.get('new_listings', 0)
                    total_existing_skipped += result.get('existing_skipped', 0)
            
            # Create summary like the log file
            summary_lines = []
            if existing_mls_count > 0:
                summary_lines.append(f"✅ Found {existing_mls_count} existing MLS numbers in database")
            
            if category_results:
                for result in category_results:
                    category = result.get('category', 'Unknown')
                    new_count = result.get('new_listings', 0)
                    existing_count = result.get('existing_skipped', 0)
                    url = result.get('url', '')
                    
                    summary_lines.append(f"✅ Filtered {new_count} new listings, skipped {existing_count} existing ones")
                    if new_count > 0:
                        summary_lines.append(f"✅ Saved {new_count} listings for {url}")
            
            if new_mls_saved > 0:
                summary_lines.append(f"✅ Saved {new_mls_saved} new MLS numbers to tracking table")
            
            payload = {
                "script_name": script_name,
                "status": status,
                "timestamp": datetime.utcnow().isoformat(),
                "existing_mls_count": existing_mls_count,
                "total_new_listings": total_new_listings,
                "total_existing_skipped": total_existing_skipped,
                "new_mls_saved": new_mls_saved,
                "category_results": category_results or [],
                "error_message": error_message,
                "summary_lines": summary_lines,
                "detailed_summary": "\n".join(summary_lines) if summary_lines else "No processing completed",
                "scrape_events": self.scrape_events
            }
            
            response = requests.post(
                self.webhook_url, 
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                print(f"✅ Webhook notification sent successfully for {script_name}")
                return True
            else:
                print(f"❌ Webhook failed with status {response.status_code} for {script_name}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error sending webhook notification: {e}")
            return False