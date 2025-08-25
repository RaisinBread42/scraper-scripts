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
                                 category_results: Optional[List[Dict]] = None,
                                 error_message: Optional[str] = None) -> bool:
        """
        Send detailed webhook notification with scraping results that match supabase log format.
        
        Args:
            script_name: Name of the scraping script (e.g. 'cireba.py')
            status: 'success' or 'failure'
            category_results: List of dicts with category scraping results
            error_message: Error message if status is 'failure'
        
        Returns:
            bool: True if webhook triggered successfully, False otherwise
        """
        if not self.webhook_url:
            print("⚠️ N8N_WEBHOOK_URL not configured in environment variables")
            return False
        
        try:
            payload = {
                "script_name": script_name,
                "status": status,
                "timestamp": datetime.utcnow().isoformat(),
                "category_results": len(category_results),
                "error_message": error_message
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