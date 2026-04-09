"""
PH Fuel Price Scraper — zigwheels.ph
Scrapes weekly retail pump prices for Manila/NCR from zigwheels.ph
and appends them to data/prices.json.

zigwheels.ph sources their data from DOE weekly advisories and
updates every Tuesday — making it a reliable, bot-friendly proxy
for DOE data.

Usage:
  python scraper.py           # normal run (skips if week already saved)
  python scraper.py --force   # re-scrape even if week already saved
"""

import json
import re
import sys
from datetime import datetime, date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────────────

SOURCE_URL = "https://www.zigwheels.ph/fuel-price"
DATA_FILE  = Path("data/prices.json")

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

# Zigwheels reports average NCR prices (not per-brand).
# We map their fuel labels to our standard names.
FUEL_NAME_MAP = {
    "gasoline":    "Ron 91",   # their generic "Gasoline" = Ron 91 average
    "ron 91":      "Ron 91",
    "ron 95":      "Ron 95",
    "premium 95":  "Ron 95",
    "ron 100":     "Ron 97",   # Ron 100 mapped to Ron 97 bucket
    "ron 97":      "Ron 97",
    "diesel":      "Diesel",
    "diesel plus": "Diesel",   # diesel plus is premium diesel; use same bucket
}

VALID_FUELS = {"Ron 91", "Ron 95", "Ron 97", "Diesel"}

# Zigwheels gives city-level averages, not per-brand.
# We use "NCR Average" as the brand so the data still fits our schema.
NCR_BRAND = "NCR Average"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def load_db():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  Warning: could not read existing DB: {e}")
    return {
        "meta": {
            "source": "zigwheels.ph (DOE-sourced)",
            "url": SOURCE_URL,
            "description": "Weekly retail pump prices per liter (PHP), NCR average",
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


def parse_price(raw):
    """Extract a float from a string like '₱62.55' or '62.55'."""
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        val = float(cleaned)
        return val if 10 < val < 500 else None   # sanity check
    except ValueError:
        return None


# ─── Scraper ──────────────────────────────────────────────────────────────────

def scrape_zigwheels():
    """
    Fetches zigwheels.ph/fuel-price and extracts the fuel price table.
    Returns a list of {brand, fuel_type, price, region} records.
    """
    print(f"  Fetching: {SOURCE_URL}")
    try:
        r = requests.get(SOURCE_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        print(f"  ✓ Page loaded ({len(r.content):,} bytes)")
    except Exception as e:
        print(f"  ✗ Failed to fetch page: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    records = []
    seen_fuels = set()

    # ── Method 1: look for the price table with Fuel / Rates columns ──────────
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Check if this looks like a fuel price table
        text = table.get_text(separator=" ").lower()
        if not any(w in text for w in ["gasoline", "diesel", "ron", "fuel"]):
            continue

        print(f"  Found a fuel table with {len(rows)} rows")

        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue

            fuel_label = cells[0].lower().strip()
            price_raw  = cells[-1]   # last cell is usually the price

            fuel_type = FUEL_NAME_MAP.get(fuel_label)
            if not fuel_type:
                # Try partial match
                for key, val in FUEL_NAME_MAP.items():
                    if key in fuel_label:
                        fuel_type = val
                        break

            if not fuel_type or fuel_type not in VALID_FUELS:
                continue

            price = parse_price(price_raw)
            if price is None:
                continue

            # If we already have this fuel type, take the first (most specific) one
            if fuel_type in seen_fuels:
                continue
            seen_fuels.add(fuel_type)

            records.append({
                "brand":     NCR_BRAND,
                "fuel_type": fuel_type,
                "price":     round(price, 2),
                "region":    "NCR"
            })
            print(f"    {fuel_type}: ₱{price:.2f}")

    # ── Method 2: regex scan of raw HTML for price mentions ───────────────────
    if not records:
        print("  Table parse found nothing — trying regex fallback on raw HTML...")
        html = r.text

        # Patterns like: "RON 95 starts at ₱58.15" or "Diesel saw a price change ... to ₱56.74"
        patterns = [
            (r"RON\s*95[^₱]*₱([\d.]+)",   "Ron 95"),
            (r"RON\s*91[^₱]*₱([\d.]+)",   "Ron 91"),
            (r"RON\s*100[^₱]*₱([\d.]+)",  "Ron 97"),
            (r"Diesel[^₱\w]*₱([\d.]+)",   "Diesel"),
            (r"Gasoline[^₱\w]*₱([\d.]+)", "Ron 91"),
        ]
        for pattern, fuel_type in patterns:
            if fuel_type in seen_fuels:
                continue
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                price = parse_price(m.group(1))
                if price:
                    seen_fuels.add(fuel_type)
                    records.append({
                        "brand":     NCR_BRAND,
                        "fuel_type": fuel_type,
                        "price":     round(price, 2),
                        "region":    "NCR"
                    })
                    print(f"    (regex) {fuel_type}: ₱{price:.2f}")

    print(f"  ✓ Extracted {len(records)} price records")
    return records


# ─── Main ─────────────────────────────────────────────────────────────────────

def run(force=False):
    today = date.today()
    wk    = week_key(today)

    print(f"\n{'='*55}")
    print(f"  PH Fuel Scraper  |  {today}  |  {wk}")
    print(f"  Source: zigwheels.ph (DOE-sourced, updates Tuesdays)")
    print(f"{'='*55}\n")

    db = load_db()

    if not force and any(s["week"] == wk for s in db["weekly_snapshots"]):
        print(f"  Week {wk} already saved. Use --force to re-scrape.")
        return

    records = scrape_zigwheels()

    if not records:
        print("\n  ✗ ERROR: No prices extracted from zigwheels.ph")
        print("  Check the site manually: https://www.zigwheels.ph/fuel-price")
        sys.exit(1)

    # Make sure we have at least Diesel and one gasoline type
    fuels_found = {r["fuel_type"] for r in records}
    if "Diesel" not in fuels_found:
        print(f"\n  ✗ ERROR: Diesel price missing from results: {fuels_found}")
        sys.exit(1)

    snapshot = {
        "week":   wk,
        "date":   today.isoformat(),
        "source": "zigwheels.ph (DOE-sourced)",
        "prices": records
    }
    db["weekly_snapshots"].append(snapshot)

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
