import json
import datetime
import os
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
import time  

# Google Chronicle API authentication
SCOPES = ['https://www.googleapis.com/auth/chronicle-backstory']
SERVICE_ACCOUNT_FILE = 'Service_Account.json'

# Load credentials
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

# Create an authenticated session
http_session = AuthorizedSession(credentials)

# Chronicle API Base URL 
BASE_URL = "https://asia-southeast1-backstory.googleapis.com/v2"

def get_rules():
    """Fetch all rules from Chronicle"""
    url = f"{BASE_URL}/detect/rules"
    
    try:
        response = http_session.get(url)
        
        if response.status_code == 200:
            return response.json().get("rules", [])
        else:
            print(f"Error fetching rules: {response.text}")
            return None
    except Exception as e:
        print(f"Exception while fetching rules: {e}")
        return None

def get_detections(rule_id, start_time, end_time):
    """Fetch detections for a specific rule within a time range"""
    # Add 'ru_' prefix if not present in the rule ID
    if not rule_id.startswith('ru_'):
        rule_id = f"ru_{rule_id}"
    
    url = f"{BASE_URL}/detect/rules/{rule_id}/detections"

    params = {
        "start_time": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "end_time": end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    try:
        response = http_session.get(url, params=params)
        
        if response.status_code == 200:
            detections = response.json().get("detections", [])
            # Return both rule_id and detection_id for each detection
            return [(rule_id, detection.get('id', 'No ID')) for detection in detections]
        elif response.status_code == 429:
            print(f"Rate limit reached (10 queries/60s), waiting 6 seconds before retrying...")
            time.sleep(6)
            response = http_session.get(url, params=params)
            if response.status_code == 200:
                detections = response.json().get("detections", [])
                return [(rule_id, detection.get('id', 'No ID')) for detection in detections]
        
        print(f"Error fetching detections: {response.text}")
        return None
        
    except Exception as e:
        print(f"Exception while fetching detections: {e}")
        return None

def get_detection_details(rule_id, detection_id):
    """Fetch and save detection details to a JSON file"""
    url = f"{BASE_URL}/detect/rules/{rule_id}/detections/{detection_id}"
    
    try:
        response = http_session.get(url)
        if response.status_code == 200:
            # Create detections directory if it doesn't exist
            if not os.path.exists('detections'):
                os.makedirs('detections')
            
            # Save details to a JSON file
            filename = f"detections/detection_{detection_id}.json"
            with open(filename, 'w') as f:
                json.dump(response.json(), f, indent=2)
            return True
        elif response.status_code == 429:
            print(f"Rate limit reached, waiting 6 seconds before retrying...")
            time.sleep(6)
            response = http_session.get(url)
            if response.status_code == 200:
                if not os.path.exists('detections'):
                    os.makedirs('detections')
                filename = f"detections/detection_{detection_id}.json"
                with open(filename, 'w') as f:
                    json.dump(response.json(), f, indent=2)
                return True
        return False
    except Exception as e:
        print(f"Exception while fetching detection details: {e}")
        return False

def main():
    print("\nFetching Rules...\n")
    rules = get_rules()

    if not rules:
        print("No rules found. Exiting...\n")
        return

    # Set time range
    start_time = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    end_time = datetime.datetime.utcnow()

    print(f"\nChecking detections for all rules from {start_time} to {end_time}\n")

    for i, rule in enumerate(rules, 1):
        rule_id = rule['ruleId']
        rule_name = rule['ruleName']
        print(f"Checking rule {i}/{len(rules)}:")
        print(f"Rule Name: {rule_name}")
        print(f"Rule ID: {rule_id}")
        
        detections = get_detections(rule_id, start_time, end_time)
        
        if detections:
            print(f"Number of detections found: {len(detections)}")
            print("\nDetection IDs:")
            for rule_id, detection_id in detections:
                print(detection_id)
                # Save detection details to JSON file
                if get_detection_details(rule_id, detection_id):
                    print(f"Details saved to: detections/detection_{detection_id}.json")
        else:
            print("No detections found")
        
        print("\n" + "-"*50 + "\n")  # Separator between rules
        
        if i < len(rules):
            time.sleep(6)

if __name__ == "__main__":
    main()
