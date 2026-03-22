"""
PH Fuel Price Scraper — DOE Philippines
Scrapes the DOE oil-monitor page for the weekly retail pump price PDF,
extracts the price table, and appends it to data/prices.json.

Usage:
  python scraper.py           # normal run (skips if week exists)
  python scraper.py --force   # re-scrape even if week already saved
"""

import json
import re
import sys
import io
from datetime import datetime, date
from pathlib import Path

import requests

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False
    print("WARNING: pdfplumber not installed.")

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    print("WARNING: beautifulsoup4 not installed.")

DOE_BASE    = "https://www.doe.gov.ph"
DOE_OIL_URL = "https://www.doe.gov.ph/oil-monitor"
DATA_FILE   = Path("data/prices.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

BRANDS = [
    "Petron", "Shell", "Caltex", "Phoenix", "Seaoil",
    "PTT", "Flying V", "Unioil", "Jetti", "Total", "Cleanfuel"
]

FUEL_MAP = {
    "ron 91": "Ron 91",
    "unleaded": "Ron 91",
    "ron 95": "Ron 95",
    "premium": "Ron 95",
    "ron 97": "Ron 97",
    "super premium": "Ron 97",
    "diesel": "Diesel",
    "gasoil": "Diesel",
    "kerosene": "Kerosene",
}

VALID_FUELS = {"Ron 91", "Ron 95", "Ron 97", "Diesel", "Kerosene"}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def load_db():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "meta": {
            "source": "DOE Philippines",
            "url": DOE_OIL_URL,
            "description": "Weekly retail pump prices per liter (PHP)",
            "last_updated": None,
            "total_records": 0
        },
        "weekly_snapshots": [],
        "price_history": {}
    }


def save_db(db):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    db["meta"]["last_updated"] = datetime.utcnow().isoformat() + "Z"
    db["meta"]["total_records"] = sum(len(s["prices"]) for s in db["weekly_snapshots"])
    DATA_FILE.write_text(json.dumps(db, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  ✓ Saved {db['meta']['total_records']} total records → {DATA_FILE}")


def week_key(d=None):
    d = d or date.today()
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def norm_fuel(raw):
    r = raw.lower().strip()
    for k, v in FUEL_MAP.items():
        if k in r:
            return v
    return None


def norm_brand(raw):
    r = str(raw).strip()
    for b in BRANDS:
        if b.lower() in r.lower():
            return b
    cleaned = r.title()
    return cleaned if len(cleaned) > 1 else None


def is_skip_row(brand_raw):
    skip = ["total", "average", "oil company", "brand", "company", "", "none"]
    return str(brand_raw).lower().strip() in skip


# ─── Step 1: Find PDF on DOE page ─────────────────────────────────────────────

def find_pdf_url():
    if not HAS_BS4:
        print("  beautifulsoup4 not available — cannot search DOE page.")
        return None

    print(f"  Searching DOE page: {DOE_OIL_URL}")
    try:
        r = requests.get(DOE_OIL_URL, headers=HEADERS, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        keywords = ["pump", "price", "retail", "oil", "monitor"]

        # Check all <a href> tags
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            if ".pdf" in href.lower() and any(w in href.lower() + text for w in keywords):
                full = href if href.startswith("http") else DOE_BASE + href
                print(f"  ✓ Found PDF link: {full}")
                return full

        # Regex fallback — scan raw HTML
        for m in re.findall(r'https?://[^\s"\'<>]+\.pdf', r.text, re.IGNORECASE):
            if any(w in m.lower() for w in keywords):
                print(f"  ✓ Found PDF in source: {m}")
                return m

        print("  No PDF link found on DOE page.")
        return None

    except Exception as e:
        print(f"  Error fetching DOE page: {e}")
        return None


# ─── Step 2: Download PDF ─────────────────────────────────────────────────────

def download_pdf(url):
    print(f"  Downloading: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=40)
        r.raise_for_status()
        print(f"  ✓ {len(r.content):,} bytes downloaded")
        return r.content
    except Exception as e:
        print(f"  PDF download failed: {e}")
        return None


# ─── Step 3: Parse PDF ────────────────────────────────────────────────────────

def parse_pdf(pdf_bytes):
    if not HAS_PDF:
        print("  pdfplumber not available.")
        return []

    records = []
    print("  Parsing PDF tables...")

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 3:
                    continue

                # Try first row as header; if no fuel columns, try second row
                def get_fuel_cols(header_row):
                    cols = {}
                    for i, cell in enumerate(header_row):
                        ft = norm_fuel(str(cell) if cell else "")
                        if ft in VALID_FUELS:
                            cols[i] = ft
                    return cols

                header   = table[0]
                fuel_cols = get_fuel_cols(header)
                data_start = 1

                if not fuel_cols and len(table) > 1:
                    fuel_cols  = get_fuel_cols(table[1])
                    data_start = 2

                if not fuel_cols:
                    continue

                brand_col = next(
                    (i for i, h in enumerate(header)
                     if h and any(w in str(h).lower() for w in ["company", "brand", "oil"])),
                    0
                )
                region_col = next(
                    (i for i, h in enumerate(header)
                     if h and any(w in str(h).lower() for w in ["region", "area"])),
                    None
                )

                print(f"    Page {page_num}: brand_col={brand_col}, fuel_cols={fuel_cols}")

                for row in table[data_start:]:
                    if not row or brand_col >= len(row) or not row[brand_col]:
                        continue
                    if is_skip_row(row[brand_col]):
                        continue

                    brand = norm_brand(row[brand_col])
                    if not brand:
                        continue

                    region = "NCR"
                    if region_col and region_col < len(row) and row[region_col]:
                        region = str(row[region_col]).strip() or "NCR"

                    for col_idx, fuel_type in fuel_cols.items():
                        if col_idx >= len(row) or not row[col_idx]:
                            continue
                        raw = str(row[col_idx]).replace(",", "").replace("₱", "").strip()
                        try:
                            price = float(raw)
                            if 20 < price < 500:   # sanity range for PHP/L
                                records.append({
                                    "brand":     brand,
                                    "fuel_type": fuel_type,
                                    "price":     round(price, 2),
                                    "region":    region
                                })
                        except ValueError:
                            pass

    print(f"  ✓ Extracted {len(records)} records from PDF")
    return records


# ─── Step 4: HTML table fallback ──────────────────────────────────────────────

def scrape_html_table():
    if not HAS_BS4:
        return []

    print("  Trying HTML table fallback on DOE page...")
    records = []
    try:
        r = requests.get(DOE_OIL_URL, headers=HEADERS, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 3:
                continue
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
            if not any(w in " ".join(headers) for w in ["diesel", "gasoline", "ron", "price"]):
                continue

            fuel_cols = {}
            for i, h in enumerate(headers):
                ft = norm_fuel(h)
                if ft in VALID_FUELS:
                    fuel_cols[i] = ft

            if not fuel_cols:
                continue

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if not cells or is_skip_row(cells[0]):
                    continue
                brand = norm_brand(cells[0])
                if not brand:
                    continue
                for col_idx, fuel_type in fuel_cols.items():
                    if col_idx >= len(cells):
                        continue
                    raw = cells[col_idx].replace(",", "").replace("₱", "").strip()
                    try:
                        price = float(raw)
                        if 20 < price < 500:
                            records.append({
                                "brand":     brand,
                                "fuel_type": fuel_type,
                                "price":     round(price, 2),
                                "region":    "NCR"
                            })
                    except ValueError:
                        pass

        print(f"  HTML fallback found {len(records)} records")
    except Exception as e:
        print(f"  HTML fallback failed: {e}")

    return records


# ─── Main ─────────────────────────────────────────────────────────────────────

def run(force=False):
    today = date.today()
    wk    = week_key(today)

    print(f"\n{'='*55}")
    print(f"  PH Fuel Scraper  |  {today}  |  {wk}")
    print(f"{'='*55}\n")

    db = load_db()

    if not force and any(s["week"] == wk for s in db["weekly_snapshots"]):
        print(f"  Week {wk} already saved. Use --force to re-scrape.")
        return

    # 1 — try PDF
    records = []
    pdf_url = find_pdf_url()
    if pdf_url:
        pdf_bytes = download_pdf(pdf_url)
        if pdf_bytes:
            records = parse_pdf(pdf_bytes)

    # 2 — try HTML table
    if not records:
        print("  PDF scraping returned nothing — trying HTML fallback.")
        records = scrape_html_table()

    # 3 — nothing worked → fail loudly so GitHub Actions marks the run red
    if not records:
        print("\n  ✗ ERROR: Could not extract any prices from DOE website.")
        print("  Aborting — not writing empty/fake data to prices.json.")
        print("  Check https://www.doe.gov.ph/oil-monitor manually.")
        sys.exit(1)

    snapshot = {
        "week":   wk,
        "date":   today.isoformat(),
        "source": "DOE Philippines",
        "prices": records
    }
    db["weekly_snapshots"].append(snapshot)

    # keep price_history too
    for rec in records:
        key = f"{rec['brand']}|{rec['fuel_type']}"
        db["price_history"].setdefault(key, [])
        db["price_history"][key].append({
            "week":  wk,
            "date":  today.isoformat(),
            "price": rec["price"]
        })

    save_db(db)
    print(f"\n  ✓ Done. {len(records)} price entries written for {wk}.\n")


if __name__ == "__main__":
    run("--force" in sys.argv)
