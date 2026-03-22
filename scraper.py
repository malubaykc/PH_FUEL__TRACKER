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
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}

def run():
    print(f"--- Starting Scrape: {datetime.now()} ---")
    
    # 1. Find the LATEST PDF Link
    try:
        res = requests.get(DOE_URL, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")
        pdf_url = None
        
        # We look for links that mention "Metro Manila" and "Retail Pump Price"
        # We take the first one found in the main content area
        for a in soup.find_all("a", href=True):
            text = a.get_text().lower()
            href = a['href']
            if ".pdf" in href and ("metro manila" in text or "ncr" in text):
                pdf_url = href if href.startswith("http") else BASE_URL + href
                print(f"Found Latest PDF: {pdf_url}")
                break
        
        if not pdf_url:
            print("Could not find a valid PDF link.")
            return

        # 2. Download and Parse PDF
        pdf_res = requests.get(pdf_url, headers=HEADERS)
        records = []
        with pdfplumber.open(io.BytesIO(pdf_res.content)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table: continue
                for row in table:
                    # Look for rows starting with major brands
                    brand_name = str(row[0]).split('\n')[0].strip()
                    if any(b in brand_name for b in ["Petron", "Shell", "Caltex", "Phoenix", "Seaoil", "Unioil"]):
                        try:
                            # Extracting prices (usually Ron 95 and Diesel)
                            # We search for the numbers in the row string
                            nums = re.findall(r"\d+\.\d+", " ".join([str(x) for x in row if x]))
                            if len(nums) >= 2:
                                records.append({"brand": brand_name, "fuel_type": "Ron 95", "price": float(nums[1]), "region": "NCR"})
                                records.append({"brand": brand_name, "fuel_type": "Diesel", "price": float(nums[-1]), "region": "NCR"})
                        except: continue

        if not records:
            print("Failed to extract data from PDF.")
            return

        # 3. Save to JSON
        db = {"weekly_snapshots": []}
        if DATA_FILE.exists():
            try: db = json.loads(DATA_FILE.read_text())
            except: pass

        # Prevent duplicate entries for the same date
        today_str = date.today().isoformat()
        db["weekly_snapshots"] = [s for s in db["weekly_snapshots"] if s["date"] != today_str]
        
        db["weekly_snapshots"].append({
            "week": f"{date.today().year}-W{date.today().isocalendar()[1]}",
            "date": today_str,
            "prices": records
        })
        
        DATA_FILE.parent.mkdir(exist_ok=True)
        DATA_FILE.write_text(json.dumps(db, indent=2))
        print(f"Successfully saved {len(records)} entries for {today_str}")

    except Exception as e:
        print(f"Critical Error: {e}")

if __name__ == "__main__":
    run()
