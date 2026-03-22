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
        res = requests.get(DOE_URL, headers=HEADERS, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # 2. FIND PDF LINKS
        links = []
        for a in soup.find_all("a", href=True):
            text = a.get_text().strip().lower()
            href = a['href']
            # Targeting Metro Manila / NCR summary
            if ".pdf" in href.lower() and ("metro manila" in text or "ncr" in text or "pump price" in text):
                full_url = href if href.startswith("http") else BASE_URL + href
                links.append(full_url)

        if not links:
            print("❌ No PDF links found on the DOE page. Website might be updated with a new layout.")
            sys.exit(1)

        latest_pdf = links[0]
        print(f"✅ Targeting PDF: {latest_pdf}")

        # 3. DOWNLOAD & PARSE PDF
        pdf_res = requests.get(latest_pdf, headers=HEADERS)
        records = []
        
        with pdfplumber.open(io.BytesIO(pdf_res.content)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table: continue
                
                for row in table:
                    # Filter for rows starting with major brands
                    brand_raw = str(row[0]).split('\n')[0].strip()
                    if any(b in brand_raw for b in ["Petron", "Shell", "Caltex", "Phoenix", "Seaoil", "Unioil", "Cleanfuel", "PTT"]):
                        try:
                            # Clean the row of None values and find numbers
                            row_str = " ".join([str(x) for x in row if x])
                            nums = re.findall(r"\d+\.\d+", row_str)
                            
                            if len(nums) >= 3:
                                records.append({"brand": brand_raw, "fuel_type": "Ron 91", "price": float(nums[0]), "region": "NCR"})
                                records.append({"brand": brand_raw, "fuel_type": "Ron 95", "price": float(nums[1]), "region": "NCR"})
                                records.append({"brand": brand_raw, "fuel_type": "Diesel", "price": float(nums[-1]), "region": "NCR"})
                        except: continue

        if not records:
            print("❌ Found PDF but failed to extract price data. Layout might have changed.")
            sys.exit(1)

        # 4. LOAD EXISTING DATA OR START FRESH
        # Structure matches what your index.html expects (meta + weekly_snapshots)
        db = {
            "meta": {
                "source": "DOE Philippines",
                "last_updated": None
            },
            "weekly_snapshots": []
        }

        if DATA_FILE.exists():
            try:
                loaded = json.loads(DATA_FILE.read_text())
                if "weekly_snapshots" in loaded:
                    db = loaded
            except Exception as e:
                print(f"Warning: Could not load existing JSON, starting new. Error: {e}")

        # 5. UPDATE DATA
        today_str = date.today().isoformat()
        # Remove old entry for today if it exists to avoid duplicates
        db["weekly_snapshots"] = [s for s in db["weekly_snapshots"] if s["date"] != today_str]
        
        db["weekly_snapshots"].append({
            "week": f"{date.today().year}-W{date.today().isocalendar()[1]:02d}",
            "date": today_str,
            "prices": records
        })
        
        # Update meta and keep only last 15 updates to keep file size small
        db["meta"]["last_updated"] = datetime.utcnow().isoformat() + "Z"
        db["weekly_snapshots"] = db["weekly_snapshots"][-15:]

        # 6. FORCE SAVE
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        DATA_FILE.write_text(json.dumps(db, indent=2), encoding="utf-8")
        print(f"🎉 SUCCESS! Saved {len(records)} entries for {today_str}")

    except Exception as e:
        print(f"🚨 CRITICAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()
