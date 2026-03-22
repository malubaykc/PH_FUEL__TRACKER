import json
import re
import sys
import io
import datetime
from datetime import datetime, date
from pathlib import Path
import requests

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = True

DOE_BASE    = "https://www.doe.gov.ph"
DOE_OIL_URL = "https://www.doe.gov.ph/oil-monitor"
DATA_FILE   = Path("data/prices.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

VALID_FUELS = {"Ron 91", "Ron 95", "Ron 97", "Diesel", "Kerosene"}

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def load_db():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except: pass
    return {"meta": {"last_updated": None}, "weekly_snapshots": [], "price_history": {}}

def save_db(db):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    db["meta"]["last_updated"] = datetime.utcnow().isoformat() + "Z"
    DATA_FILE.write_text(json.dumps(db, indent=2, ensure_ascii=False), encoding="utf-8")

def week_key(d=None):
    d = d or date.today()
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

# ─── STEP 1: FIND LATEST PDF (IMPROVED) ───────────────────────────────────────

def find_pdf_url():
    print(f"  Searching DOE page for the LATEST prices...")
    try:
        r = requests.get(DOE_OIL_URL, headers=HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")
        
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            # DOE usually titles the latest one "Summary of Retail Pump Prices in Metro Manila"
            if ".pdf" in href.lower() and ("metro manila" in text or "ncr" in text or "pump price" in text):
                full_url = href if href.startswith("http") else DOE_BASE + href
                links.append(full_url)
        
        if links:
            # We take the FIRST matching link found in the main content
            print(f"  ✓ Found latest PDF: {links[0]}")
            return links[0]
            
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None

# ─── STEP 2: PARSE PDF (IMPROVED TABLE LOGIC) ────────────────────────────────

def parse_pdf(pdf_bytes):
    records = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2: continue
                
                # Look for the header row containing Diesel or Ron
                for row in table:
                    # Clean the row
                    clean_row = [str(c).replace('\n',' ').strip() for c in row if c]
                    
                    # Identify Brand (usually column 0 or 1)
                    brand_raw = clean_row[0]
                    if any(b in brand_raw for b in ["Petron", "Shell", "Caltex", "Phoenix", "Seaoil", "Unioil", "Cleanfuel"]):
                        # This is a data row. Let's try to extract prices.
                        # Usually: Brand | Ron 91 | Ron 95 | Ron 97 | Diesel
                        try:
                            # We search for numbers in the row
                            prices = []
                            for cell in clean_row:
                                val = re.findall(r"\d+\.\d+", cell)
                                if val: prices.append(float(val[0]))
                            
                            if len(prices) >= 3:
                                records.append({"brand": brand_raw.split()[0], "fuel_type": "Ron 95", "price": prices[1], "region": "NCR"})
                                records.append({"brand": brand_raw.split()[0], "fuel_type": "Diesel", "price": prices[-1], "region": "NCR"})
                                records.append({"brand": brand_raw.split()[0], "fuel_type": "Ron 91", "price": prices[0], "region": "NCR"})
                        except: continue
    return records

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run(force=False):
    db = load_db()
    wk = week_key()
    
    pdf_url = find_pdf_url()
    if not pdf_url:
        print("  ✗ Could not find PDF link.")
        sys.exit(1)

    r = requests.get(pdf_url, headers=HEADERS)
    records = parse_pdf(r.content)

    if not records:
        print("  ✗ Could not extract prices. DOE might have changed the PDF layout.")
        sys.exit(1)

    # CHECK FOR DUPLICATES: If these prices are exactly the same as the last entry,
    # the DOE hasn't actually updated their PDF yet.
    if len(db["weekly_snapshots"]) > 0:
        last_entry = db["weekly_snapshots"][-1]["prices"]
        # Compare first brand diesel price as a sample
        if records[0]["price"] == last_entry[0]["price"] and not force:
            print(f"  ! Prices match March 11 data. DOE hasn't posted the new week yet.")
            return

    # If we are here, we have NEW data
    snapshot = {"week": wk, "date": date.today().isoformat(), "prices": records}
    db["weekly_snapshots"].append(snapshot)
    save_db(db)
    print(f"  ✓ Successfully updated with NEW prices for {wk}!")

if __name__ == "__main__":
    run("--force" in sys.argv)
