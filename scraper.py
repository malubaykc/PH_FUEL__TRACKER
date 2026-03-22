import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import sys
import io
from pathlib import Path
import pdfplumber
import re

# --- CONFIGURATION ---
DATA_FILE = Path("data/prices.json")
DOE_URL = "https://www.doe.gov.ph/oil-monitor"
BASE_URL = "https://www.doe.gov.ph"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

def run():
    print(f"--- STARTING SCRAPE: {datetime.now()} ---")
    
    try:
        # 1. GET DOE PAGE
        print(f"Connecting to {DOE_URL}...")
        res = requests.get(DOE_URL, headers=HEADERS, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # 2. FIND PDF LINKS (Aggressive Search)
        links = []
        for a in soup.find_all("a", href=True):
            text = a.get_text().strip().lower()
            href = a['href']
            # Look for ANY PDF that looks like a price summary
            if ".pdf" in href.lower():
                if any(word in text or word in href.lower() for word in ["metro", "ncr", "manila", "pump", "retail", "price"]):
                    full_url = href if href.startswith("http") else BASE_URL + href
                    links.append(full_url)

        if not links:
            print("❌ ERROR: Could not find any PDF links on the page.")
            sys.exit(1)

        # Take the very first PDF link found (usually the latest)
        latest_pdf = links[0]
        print(f"✅ Found PDF: {latest_pdf}")

        # 3. DOWNLOAD & PARSE
        pdf_res = requests.get(latest_pdf, headers=HEADERS)
        records = []
        
        with pdfplumber.open(io.BytesIO(pdf_res.content)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table: continue
                
                for row in table:
                    # Clean the row
                    row_data = [str(x).strip() for x in row if x]
                    if not row_data: continue
                    
                    brand_name = row_data[0].split('\n')[0].strip()
                    # Filter for known brands
                    if any(b in brand_name for b in ["Petron", "Shell", "Caltex", "Phoenix", "Seaoil", "Unioil", "Cleanfuel", "PTT", "Jetti"]):
                        try:
                            # Extract all numbers (prices) from the row string
                            row_str = " ".join(row_data)
                            nums = re.findall(r"\d+\.\d+", row_str)
                            
                            if len(nums) >= 3:
                                records.append({"brand": brand_name, "fuel_type": "Ron 91", "price": float(nums[0]), "region": "NCR"})
                                records.append({"brand": brand_name, "fuel_type": "Ron 95", "price": float(nums[1]), "region": "NCR"})
                                records.append({"brand": brand_name, "fuel_type": "Diesel", "price": float(nums[-1]), "region": "NCR"})
                        except: continue

        if not records:
            print("❌ Found PDF but table extraction failed.")
            sys.exit(1)

        # 4. PREPARE JSON
        db = {"meta": {"last_updated": None}, "weekly_snapshots": []}
        if DATA_FILE.exists():
            try:
                db = json.loads(DATA_FILE.read_text())
            except: pass

        today_str = date.today().isoformat()
        # Clean duplicates
        db["weekly_snapshots"] = [s for s in db["weekly_snapshots"] if s["date"] != today_str]
        
        db["weekly_snapshots"].append({
            "week": f"{date.today().year}-W{date.today().isocalendar()[1]:02d}",
            "date": today_str,
            "prices": records
        })
        
        db["meta"]["last_updated"] = datetime.utcnow().isoformat() + "Z"
        db["weekly_snapshots"] = db["weekly_snapshots"][-15:] # Keep last 15 weeks

        # 5. SAVE
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        DATA_FILE.write_text(json.dumps(db, indent=2), encoding="utf-8")
        print(f"🎉 SUCCESS! Added {len(records)} prices for {today_str}")

    except Exception as e:
        print(f"🚨 CRITICAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()
