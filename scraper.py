"""
PH Fuel Price Scraper — DOE Philippines
Scrapes weekly NCR retail pump prices from the DOE OIMB weekly monitoring report
and saves brand-agnostic NCR common prices + per-brand prices into data/prices.json.

PRIMARY source: prod-cms.doe.gov.ph PDF report pages (DOE OIMB official)
FALLBACK source: zigwheels.ph / fuelprice.ph (DOE-sourced, updates Tuesdays)

The DOE report format records:
  - Product (RON91, RON95, RON97/100, Diesel, Diesel Plus, Kerosene)
  - Overall Range (min - max across all NCR stations)
  - Common Price (most frequently observed price)

Usage:
  python scraper.py                       # skip if current week already saved
  python scraper.py --force               # re-scrape even if week already saved
  python scraper.py --week=2026-04-14    # scrape a specific week (uses that date's Monday)
"""

import json
import re
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────────────

DATA_FILE = Path("data/prices.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}

# DOE PDF URL pattern
DOE_PDF_BASE = "https://prod-cms.doe.gov.ph/documents/d/guest/ncr-price-monitoring-{date}-pdf"

# Fallback web sources
FALLBACK_URLS = [
    "https://www.zigwheels.ph/fuel-price",
    "https://www.fuelprice.ph/",
]

BRANDS = ["Petron", "Shell", "Caltex", "Phoenix", "Seaoil"]

# Per-brand offsets from DOE NCR common price (₱/L)
# Derived from analysis of DOE OIMB weekly monitoring tables showing per-brand prices
BRAND_OFFSETS = {
    "Petron":  {"Ron 91":  0.20, "Ron 95":  0.30, "Ron 97":  0.40, "Diesel":  0.30},
    "Shell":   {"Ron 91":  0.80, "Ron 95":  0.90, "Ron 97":  1.00, "Diesel":  0.80},
    "Caltex":  {"Ron 91":  0.40, "Ron 95":  0.50, "Ron 97":  0.60, "Diesel":  0.50},
    "Phoenix": {"Ron 91": -0.20, "Ron 95": -0.20, "Ron 97": -0.20, "Diesel": -0.10},
    "Seaoil":  {"Ron 91": -0.50, "Ron 95": -0.50, "Ron 97": -0.50, "Diesel": -0.30},
}

VALID_NCR_KEYS = {"ron91", "ron95", "ron97", "diesel"}

PRODUCT_MAP = {
    "ron 91": "ron91", "gasoline (ron91)": "ron91", "gasoline (ron 91)": "ron91",
    "ron 95": "ron95", "gasoline (ron95)": "ron95", "gasoline (ron 95)": "ron95",
    "ron 97": "ron97", "ron 100": "ron97", "gasoline (ron97/100)": "ron97",
    "gasoline (ron 97/100)": "ron97",
    "diesel": "diesel",
}


# ─── Database ─────────────────────────────────────────────────────────────────

def load_db():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  Warning: could not read existing DB: {e}")
    return {
        "meta": {
            "source": "DOE Philippines — Oil Industry Management Bureau (OIMB)",
            "url": "https://doe.gov.ph/oil-monitor",
            "description": (
                "Weekly NCR retail pump prices per liter (PHP). "
                "ncr_common = DOE prevailing common price (brand-agnostic). "
                "Per-brand prices derived from common price +/- typical brand premium/discount."
            ),
            "notes": [
                "TRAIN law (RA 10963): excise taxes effective Jan 1, 2018",
                "TRAIN 2nd tranche: Jan 1, 2019",
                "COVID-19: historic lows Apr-May 2020",
                "Russia-Ukraine war: spike Feb-Jun 2022",
                "2026 Hormuz crisis: Hormuz closure Feb 28; emergency Mar 24; peak Apr 7-13; rollbacks Apr 14 & 21"
            ],
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
    print(f"  Saved {db['meta']['total_records']} total records -> {DATA_FILE}")


def week_key(d=None):
    d = d or date.today()
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def parse_price(raw):
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        val = float(cleaned)
        return val if 10 < val < 500 else None
    except ValueError:
        return None


# ─── DOE PDF Page Scraper ─────────────────────────────────────────────────────

def scrape_doe_pdf_page(report_date):
    """
    Fetch the DOE PDF page and extract the prevailing prices summary table.
    Returns dict {ron91, ron95, ron97, diesel} of common prices, or {}.
    """
    url = DOE_PDF_BASE.format(date=report_date.strftime("%m%d%Y"))
    print(f"  Trying DOE PDF: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}")
            return {}
        print(f"  DOE PDF page loaded ({len(r.content):,} bytes)")
    except Exception as e:
        print(f"  Failed: {e}")
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    page_text = soup.get_text(" ", strip=True)
    results = {}

    # Extract from summary table: "Gasoline (RON91) min max common"
    # e.g. "Gasoline (RON91) 76.00 102.10 86.10"
    patterns = [
        (r"Gasoline\s*\(RON\s*9[17]/?100?\)[^\d]*(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)", "ron97"),
        (r"Gasoline\s*\(RON\s*95\)[^\d]*(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)", "ron95"),
        (r"Gasoline\s*\(RON\s*91\)[^\d]*(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)", "ron91"),
        (r"(?<![A-Za-z])Diesel(?!\s*Plus)[^\d]*(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)", "diesel"),
    ]
    for pattern, key in patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            lo, hi, common = float(m.group(1)), float(m.group(2)), float(m.group(3))
            if 10 < common < 500 and lo < common < hi:
                results[key] = common
                print(f"    {key}: P{common:.2f} (range P{lo:.2f}-P{hi:.2f})")

    return results


# ─── Fallback Scraper ─────────────────────────────────────────────────────────

def scrape_fallback():
    """Try zigwheels.ph / fuelprice.ph for NCR price data."""
    for url in FALLBACK_URLS:
        print(f"  Trying fallback: {url}")
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"  Failed: {e}")
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        averages = {}
        seen = set()
        page_text = soup.get_text(" ")

        for table in soup.find_all("table"):
            text = table.get_text(separator=" ").lower()
            if not any(w in text for w in ["gasoline", "diesel", "ron"]):
                continue
            for row in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue
                label = cells[0].lower().strip()
                key = None
                for prod_str, prod_key in PRODUCT_MAP.items():
                    if prod_str in label and prod_key in VALID_NCR_KEYS and prod_key not in seen:
                        key = prod_key
                        break
                if not key:
                    continue
                price = parse_price(cells[-1])
                if price:
                    seen.add(key)
                    averages[key] = price
                    print(f"    {key}: P{price:.2f}")

        if len(averages) >= 3:
            return averages

        # Regex fallback
        for pattern, key in [
            (r"RON\s*95[^\d]+(\d+\.?\d+)", "ron95"),
            (r"RON\s*91[^\d]+(\d+\.?\d+)", "ron91"),
            (r"Diesel(?!\s*Plus)[^\d]+(\d+\.?\d+)", "diesel"),
        ]:
            if key not in seen:
                m = re.search(pattern, page_text, re.IGNORECASE)
                if m:
                    price = parse_price(m.group(1))
                    if price:
                        seen.add(key)
                        averages[key] = price

        if averages:
            return averages

    return {}


# ─── Brand expansion ──────────────────────────────────────────────────────────

def expand_to_brands(ncr_common):
    key_to_ft = {"ron91": "Ron 91", "ron95": "Ron 95", "ron97": "Ron 97", "diesel": "Diesel"}
    records = []
    for brand in BRANDS:
        off = BRAND_OFFSETS[brand]
        for key, common in ncr_common.items():
            if key not in key_to_ft:
                continue
            ft = key_to_ft[key]
            records.append({"brand": brand, "fuel_type": ft, "price": round(common + off[ft], 2), "region": "NCR"})
    return records


# ─── Main ─────────────────────────────────────────────────────────────────────

def run(force=False, target_date=None):
    today = target_date or date.today()
    wk    = week_key(today)

    print(f"\n{'='*60}")
    print(f"  PH Fuel Scraper  |  {today}  |  {wk}")
    print(f"{'='*60}\n")

    db = load_db()

    if not force and any(s["week"] == wk for s in db["weekly_snapshots"]):
        print(f"  Week {wk} already saved. Use --force to re-scrape.")
        return

    weekday = today.weekday()
    monday  = today - timedelta(days=weekday)

    ncr_common = {}

    # 1. Try DOE PDF page (Mon, Tue, Sun offsets)
    for offset in [0, 1, -1, 2]:
        ncr_common = scrape_doe_pdf_page(monday + timedelta(days=offset))
        if len(ncr_common) >= 3:
            break

    # 2. Fallback
    if len(ncr_common) < 3:
        print("\n  DOE PDF incomplete - trying fallback sources...")
        ncr_common = scrape_fallback()

    if not ncr_common or "diesel" not in ncr_common:
        print("\n  ERROR: Could not extract prices.")
        print(f"  Try manually: {DOE_PDF_BASE.format(date=monday.strftime('%m%d%Y'))}")
        sys.exit(1)

    print(f"\n  NCR Common prices:")
    for k, v in ncr_common.items():
        print(f"    {k}: P{v:.2f}")

    records = expand_to_brands(ncr_common)
    print(f"\n  Expanded to {len(records)} brand-fuel records")

    snapshot = {
        "week":       wk,
        "date":       monday.isoformat(),
        "source":     "DOE Philippines OIMB",
        "ncr_common": ncr_common,
        "note":       "",
        "prices":     records
    }

    db["weekly_snapshots"] = [s for s in db["weekly_snapshots"] if s["week"] != wk]
    db["weekly_snapshots"].append(snapshot)
    db["weekly_snapshots"].sort(key=lambda s: s["date"])

    for rec in records:
        key = f"{rec['brand']}|{rec['fuel_type']}"
        db["price_history"].setdefault(key, [])
        db["price_history"][key] = [h for h in db["price_history"][key] if h["week"] != wk]
        db["price_history"][key].append({"week": wk, "date": monday.isoformat(), "price": rec["price"]})
        db["price_history"][key].sort(key=lambda h: h["date"])

    save_db(db)
    print(f"\n  Done. {len(records)} entries written for {wk}.\n")


if __name__ == "__main__":
    force = "--force" in sys.argv
    target_date = None
    for arg in sys.argv[1:]:
        if arg.startswith("--week="):
            target_date = date.fromisoformat(arg.split("=")[1])
        elif re.match(r"\d{4}-\d{2}-\d{2}", arg):
            target_date = date.fromisoformat(arg)
    run(force=force, target_date=target_date)
