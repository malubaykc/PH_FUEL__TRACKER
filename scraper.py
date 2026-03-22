import json
import re
import io
from datetime import datetime, date
from pathlib import Path
from playwright.sync_api import sync_playwright
import pdfplumber
import requests

DATA_FILE = Path("data/prices.json")
DOE_URL = "https://www.doe.gov.ph/oil-monitor"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def get_pdf_links():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(DOE_URL, wait_until="networkidle", timeout=60000)
        links = []
        for a in page.query_selector_all("a"):
            href = a.get_attribute("href") or ""
            if ".pdf" in href.lower() and any(x in href.lower() or x in (a.inner_text().lower()) for x in ["metro", "ncr", "manila", "pump", "monitor"]):
                links.append(href if href.startswith("http") else "https://www.doe.gov.ph" + href)
        browser.close()
        return links

def run():
    print(f"--- DEBUG START: {datetime.now()} ---")
    try:
        print("Launching browser to fetch DOE page...")
        links = get_pdf_links()

        if not links:
            print("No PDF links found even with browser.")
            return

        latest_pdf = links[0]
        print(f"Target PDF: {latest_pdf}")

        pdf_res = requests.get(latest_pdf, headers=HEADERS, timeout=30)
        records = []

        with pdfplumber.open(io.BytesIO(pdf_res.content)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table:
                    continue
                for row in table:
                    row_data = [str(x).strip() for x in row if x]
                    if not row_data:
                        continue
                    brand = row_data[0].split('\n')[0].strip()
                    if any(b in brand for b in ["Petron", "Shell", "Caltex", "Phoenix", "Seaoil", "Unioil", "Cleanfuel"]):
                        nums = re.findall(r"\d+\.\d+", " ".join(row_data))
                        if len(nums) >= 3:
                            records.append({"brand": brand, "fuel_type": "Ron 91", "price": float(nums[0]), "region": "NCR"})
                            records.append({"brand": brand, "fuel_type": "Ron 95", "price": float(nums[1]), "region": "NCR"})
                            records.append({"brand": brand, "fuel_type": "Diesel", "price": float(nums[-1]), "region": "NCR"})

        if not records:
            print("PDF found but no price data extracted.")
            return

        db = {"meta": {"last_updated": None}, "weekly_snapshots": []}
        if DATA_FILE.exists():
            try:
                db = json.loads(DATA_FILE.read_text())
            except:
                pass

        today_str = date.today().isoformat()
        db["weekly_snapshots"] = [s for s in db["weekly_snapshots"] if s["date"] != today_str]
        db["weekly_snapshots"].append({
            "week": f"{date.today().year}-W{date.today().isocalendar()[1]:02d}",
            "date": today_str,
            "prices": records
        })
        db["meta"]["last_updated"] = datetime.utcnow().isoformat() + "Z"
        db["weekly_snapshots"] = db["weekly_snapshots"][-15:]

        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        DATA_FILE.write_text(json.dumps(db, indent=2), encoding="utf-8")
        print(f"SUCCESS! Added {len(records)} prices.")

    except Exception as e:
        import traceback
        print(f"ERROR: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    run()
