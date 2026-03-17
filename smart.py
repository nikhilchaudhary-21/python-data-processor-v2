import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import os
import json

# --- CONFIGURATION FROM SECRETS ---
# GitHub Secrets se data uthane ke liye os.environ use kiya hai
SMARTLEAD_API_KEY = os.environ.get('SMARTLEAD_API_KEY')
GOOGLE_JSON_DATA = os.environ.get('GOOGLE_SHEETS_JSON') # JSON string format mein
SHEET_NAME = "Smartlead_Jan2026_Leads"

BASE_URL = "https://server.smartlead.ai/api/v1"
# Mystery Rewards - US (Jan 12) aur uske baad ke saare campaigns ke liye cutoff
CUTOFF_DATE = datetime(2026, 1, 1) 

def setup_gsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # JSON string ko dictionary mein convert karke credentials load karna
    creds_dict = json.loads(GOOGLE_JSON_DATA)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).sheet1
    
    # Header check aur update: Agar pehli row "Campaign Name" nahi hai toh headers daal do
    first_row = sheet.row_values(1)
    headers = ["Campaign Name", "Lead Name", "Email", "Opens", "Sent At", "Open At", "Gap (Min)"]
    if not first_row or first_row[0] != "Campaign Name":
        sheet.insert_row(headers, 1)
        print("✅ Headers verified/added.")
    
    # Duplicate prevent karne ke liye existing emails fetch karein
    existing_emails = set(sheet.col_values(3)) 
    return sheet, existing_emails

def run_live_automation():
    if not SMARTLEAD_API_KEY or not GOOGLE_JSON_DATA:
        print("❌ Error: API Key or Google JSON Secret is missing!")
        return

    sheet, existing_emails = setup_gsheet()
    params = {'api_key': SMARTLEAD_API_KEY}
    
    print(f"🚀 SYNC STARTED: Fetching campaigns after {CUTOFF_DATE.date()}...")
    
    # 1. Fetch all campaigns
    try:
        response = requests.get(f"{BASE_URL}/campaigns", params=params, timeout=20)
        if response.status_code != 200:
            print(f"❌ API Error {response.status_code}")
            return
        campaigns = response.json()
    except Exception as e:
        print(f"❌ Error fetching campaigns: {e}")
        return

    for camp in campaigns:
        # Date Filter: Sirf 2026 wale campaigns process honge
        created_at_str = camp.get('created_at', '').split('.')[0].replace('Z', '')
        camp_date = datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%S')

        if camp_date >= CUTOFF_DATE:
            print(f"🔍 Checking Campaign: {camp['name']}")
            
            stats_url = f"{BASE_URL}/campaigns/{camp['id']}/statistics"
            offset = 0
            limit = 100
            
            while True:
                # API Delay for general rate limiting protection
                time.sleep(0.3)
                curr_params = {'api_key': SMARTLEAD_API_KEY, 'offset': offset, 'limit': limit}
                
                try:
                    res = requests.get(stats_url, params=curr_params, timeout=30)
                    
                    # --- NEW RATE LIMIT LOGIC: Retry on 429 ---
                    if res.status_code == 429:
                        print(f"  ⏳ Rate limited on '{camp['name']}', waiting 10s before retry...")
                        time.sleep(10)
                        continue # Same offset par dobara try karega
                        
                    if res.status_code != 200:
                        print(f"  ❌ API Error {res.status_code}. Skipping page.")
                        break

                    data = res.json().get('data', [])
                    if not data:
                        break

                    camp_leads_batch = []
                    for lead in data:
                        email = lead.get('lead_email')
                        
                        # Duplicate skip logic
                        if not email or email in existing_emails:
                            continue

                        # Rule: Open Count 2+
                        if int(lead.get('open_count', 0)) >= 2:
                            s_time = lead.get('sent_time')
                            o_time = lead.get('open_time')

                            if s_time and o_time:
                                # Parsing dates safely
                                s_dt = datetime.strptime(s_time.split('.')[0].replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
                                o_dt = datetime.strptime(o_time.split('.')[0].replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
                                
                                gap_seconds = (o_dt - s_dt).total_seconds()
                                
                                # Rule: Human Gap (2 minutes)
                                if gap_seconds >= 120:
                                    # Seconds to Minutes conversion
                                    gap_minutes = round(gap_seconds / 60, 2)
                                    
                                    row = [
                                        camp['name'], 
                                        lead.get('lead_name'), 
                                        email,
                                        lead['open_count'], 
                                        str(s_dt), 
                                        str(o_dt), 
                                        f"{gap_minutes} min"
                                    ]
                                    camp_leads_batch.append(row)
                                    existing_emails.add(email) # Local set update

                    # Live Batch Saving for current page/campaign to avoid Google 429
                    if camp_leads_batch:
                        try:
                            sheet.append_rows(camp_leads_batch)
                            print(f"  ✅ SAVED: {len(camp_leads_batch)} new leads from '{camp['name']}'")
                            time.sleep(2) # Google Quota Safety Delay
                        except Exception as e:
                            print(f"  ❌ Google Save Error: {e}")

                    if len(data) < limit:
                        break
                    offset += limit

                except Exception as e:
                    print(f"  ⚠️ Exception in campaign '{camp['name']}': {e}")
                    break

    print("🎉 Sync Process Finished Successfully.")

if __name__ == "__main__":
    run_live_automation()
