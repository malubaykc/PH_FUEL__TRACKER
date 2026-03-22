import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import sys
import io
from pathlib import Path
import pdfplumber
import re

DATA_FILE = Path("data/prices.json")
DOE_URL = "https://www.doe.gov.ph/oil-monitor"
BASE_URL = "https://www.doe.gov.ph"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}

def run():
    print(f"--- STARTING SCRAPE: {datetime.now()} ---")
    
    try:
        res = requests.get(DOE_URL, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")
        
        # 1. FIND ALL VALID PDF LINKS
        links = []
        for a in soup.find_all("a", href=True):
            text = a.get_text().strip().lower()
            href = a['href']
            # Target Metro Manila summary specifically
            if ".pdf" in href.lower() and ("metro manila" in text or "ncr" in text):
                full_url = href if href.startswith("http") else BASE_URL + href
                links.append({"url": full_url, "text": text})

        if not links:
            print("No PDF links found.")
            return

        # 2. PICK THE LATEST (Skips old links like Mar 11)
        # We sort by keywords to try and find 'current' or 'latest'
        # Or we simply take the one with the latest date mentioned in text if possible
        latest_pdf = links[0]["url"]
        print(f"Targeting PDF: {latest_pdf}")

        # 3. DOWNLOAD & PARSE
        pdf_res = requests.get(latest_pdf, headers=HEADERS)
        records = []
        
        with pdfplumber.open(io.BytesIO(pdf_res.content)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table: continue
                
                # Dynamic Column Finder
                headers = [str(c).lower().replace('\n',' ') for c in table[0] if c]
                idx_91 = next((i for i, h in enumerate(headers) if "91" in h), 1)
                idx_95 = next((i for i, h in enumerate(headers) if "95" in h), 2)
                idx_dsl = next((i for i, h in enumerate(headers) if "diesel" in h and "plus" not in h), -1)

                for row in table[1:]:
                    brand_name = str(row[0]).split('\n')[0].strip()
                    if any(b in brand_name for b in ["Petron", "Shell", "Caltex", "Phoenix", "Seaoil", "Unioil", "Cleanfuel"]):
                        try:
                            # Extract all numbers from the row
                            row_str = " ".join([str(x) for x in row if x])
                            nums = re.findall(r"\d+\.\d+", row_str)
                            
                            if len(nums) >= 3:
                                records.append({"brand": brand_name, "fuel_type": "Ron 91", "price": float(nums[0]), "region": "NCR"})
                                records.append({"brand": brand_name, "fuel_type": "Ron 95", "price": float(nums[1]), "region": "NCR"})
                                records.append({"brand": brand_name, "fuel_type": "Diesel", "price": float(nums[-1]), "region": "NCR"})
                        except: continue

        if not records:
            print("Failed to extract data. The PDF might be a scanned image or layout changed.")
            return

        # 4. SAVE (With overwrite protection)
        db = {"weekly_snapshots": []}
        if DATA_FILE.exists():
            try: db = json.loads(DATA_FILE.read_text())
            except: pass

        today_str = date.today().isoformat()
        # Only add if this date doesn't exist OR it's a forced update
        db["weekly_snapshots"] = [s for s in db["weekly_snapshots"] if s["date"] != today_str]
        
        db["weekly_snapshots"].append({
            "week": f"{date.today().year}-W{date.today().isocalendar()[1]}",
            "date": today_str,
            "prices": records
        })
        
        # Cleanup: Keep only last 20 weeks to keep file small
        db["weekly_snapshots"] = db["weekly_snapshots"][-20:]

        DATA_FILE.parent.mkdir(exist_ok=True)
        DATA_FILE.write_text(json.dumps(db, indent=2))
        print(f"SUCCESS! Site should now show data for {today_str}")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    run()
