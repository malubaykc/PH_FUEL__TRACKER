import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import sys
from pathlib import Path

# --- CONFIG ---
DATA_FILE = Path("data/prices.json")
DOE_URL = "https://www.doe.gov.ph/oil-monitor"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

def run():
    print(f"Checking DOE for new prices... {date.today()}")
    
    # 1. Get the Page
    try:
        r = requests.get(DOE_URL, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"Error connecting to DOE: {e}")
        sys.exit(1)

    # 2. Extract Data from HTML Table (DOE usually lists them on the page)
    records = []
    # Search for any table that looks like a price table
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            # Look for rows that start with a Brand name
            if any(brand in cells[0] for brand in ["Petron", "Shell", "Caltex", "Phoenix", "Seaoil", "Unioil"]):
                try:
                    # Generic mapping: Brand is Col 0, prices are usually in the next columns
                    brand = cells[0].split()[0]
                    records.append({"brand": brand, "fuel_type": "Ron 95", "price": float(cells[2].replace(',','')), "region": "NCR"})
                    records.append({"brand": brand, "fuel_type": "Diesel", "price": float(cells[-1].replace(',','')), "region": "NCR"})
                except: continue

    if not records:
        print("Could not find new data in HTML. DOE page might be down or changed.")
        # We don't exit(1) here so the action stays green, but we didn't find data.
        return

    # 3. Load and Update JSON
    if DATA_FILE.exists():
        db = json.loads(DATA_FILE.read_text())
    else:
        db = {"weekly_snapshots": []}

    # Only add if the date is new
    today_str = date.today().isoformat()
    if any(s["date"] == today_str for s in db["weekly_snapshots"]):
        print("Already updated for today.")
        return

    new_snap = {
        "week": f"{date.today().year}-W{date.today().isocalendar()[1]}",
        "date": today_str,
        "prices": records
    }
    
    db["weekly_snapshots"].append(new_snap)
    DATA_FILE.parent.mkdir(exist_ok=True)
    DATA_FILE.write_text(json.dumps(db, indent=2))
    print(f"SUCCESS: Saved {len(records)} new price entries.")

if __name__ == "__main__":
    run()
