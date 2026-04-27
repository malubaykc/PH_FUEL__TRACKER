"""
ingest_manual_pdf.py
────────────────────
Manual PDF ingestion script for PH Fuel Tracker.

USE WHEN: The GitHub Actions auto-scraper fails to download the DOE PDF
          (blocked, URL changed, etc.) and you need to update prices manually.

WORKFLOW:
  1. Download the PDF from https://www.doe.gov.ph/price-monitor manually
  2. Drop the PDF into  data/pdfs/  (e.g. NCR_Price_Monitoring_04212026.pdf)
  3. Run:  python ingest_manual_pdf.py data/pdfs/NCR_Price_Monitoring_04212026.pdf
  4. Commit and push:  git add data/prices.json && git commit -m "🛢️ Manual update Apr 21" && git push

DOES NOT REWRITE scraper.py — both scripts share the same data/prices.json.
The auto-scraper will continue to run on its Tuesday schedule as normal.
If both run for the same week, the later run simply overwrites the same week entry.
"""

import sys
import re
import json
import argparse
from datetime import datetime
from pathlib import Path

DATA_PATH = Path("data/prices.json")


# ── PARSE ─────────────────────────────────────────────────────────────────────

def parse_summary_table(text: str) -> dict:
    """
    Parse the PREVAILING RETAIL PRICES summary block at the bottom of the DOE PDF.
    Looks for lines like:
      Gasoline (RON91)       74.00  -  89.50   88.60
      Gasoline (RON95)       75.00  -  98.50   98.50
      ...
    The LAST number on each matching line is the common price.
    """
    data = {}
    for line in text.split("\n"):
        line_upper = line.upper()
        numbers = re.findall(r"\d+\.?\d*", line.replace(",", ""))
        floats = [float(n) for n in numbers if 50.0 <= float(n) <= 300.0]

        if len(floats) < 3:
            continue

        common_price = floats[-1]   # last column = common price

        if re.search(r"RON\s*97|RON\s*97/100", line_upper) and "ron97" not in data:
            data["ron97"] = common_price
        elif re.search(r"RON\s*95", line_upper) and "ron95" not in data:
            data["ron95"] = common_price
        elif re.search(r"RON\s*91", line_upper) and "ron91" not in data:
            data["ron91"] = common_price
        elif re.search(r"\bDIESEL PLUS\b", line_upper) and "diesel_plus" not in data:
            data["diesel_plus"] = common_price
        elif re.search(r"\bDIESEL\b", line_upper) and "DIESEL PLUS" not in line_upper and "diesel" not in data:
            data["diesel"] = common_price
        elif re.search(r"\bKEROSENE\b", line_upper) and "kerosene" not in data:
            data["kerosene"] = common_price

    return data


def extract_prices_from_pdf(pdf_path: Path) -> dict:
    try:
        import pdfplumber
    except ImportError:
        sys.exit("❌ pdfplumber not installed. Run:  pip install pdfplumber")

    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    print("\n--- RAW PDF TEXT (first 2000 chars) ---")
    print(text[:2000])
    print("--- END ---\n")

    prices = parse_summary_table(text)
    return prices


# ── VALIDATE ──────────────────────────────────────────────────────────────────

def is_valid(prices: dict) -> bool:
    required = ["ron91", "ron95", "ron97", "diesel"]
    for key in required:
        if key not in prices:
            print(f"❌ Missing required key: {key}")
            return False
        if not (60.0 <= prices[key] <= 250.0):
            print(f"❌ {key} = {prices[key]} is out of expected range (60–250)")
            return False
    return True


# ── LOAD / SAVE JSON ──────────────────────────────────────────────────────────

def load_json() -> dict:
    if DATA_PATH.exists():
        with open(DATA_PATH, "r") as f:
            return json.load(f)
    return {"weekly_snapshots": [], "price_history": {}, "meta": {}}


def save_json(data: dict):
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ── UPDATE DATA ───────────────────────────────────────────────────────────────

def update_data(date: datetime, prices: dict):
    data = load_json()

    week_str = date.strftime("%Y-W%U")
    date_str = date.strftime("%Y-%m-%d")

    # Check if this week already exists
    existing_index = None
    for i, s in enumerate(data.get("weekly_snapshots", [])):
        if s.get("week") == week_str:
            existing_index = i
            print(f"⚠️  Week {week_str} already exists — overwriting with manual data")
            break

    # Build per-brand prices using NCR common price for all brands.
    # NOTE: The DOE PDF reports NCR common prices only — it does NOT give a
    # single per-brand common price across all of NCR. Using the NCR common
    # for all brands is more accurate than invented per-brand offsets.
    def brand_entries(brand):
        entries = [
            {"brand": brand, "fuel_type": "Ron 91",     "price": prices["ron91"],           "region": "NCR"},
            {"brand": brand, "fuel_type": "Ron 95",     "price": prices["ron95"],           "region": "NCR"},
            {"brand": brand, "fuel_type": "Ron 97",     "price": prices["ron97"],           "region": "NCR"},
            {"brand": brand, "fuel_type": "Diesel",     "price": prices["diesel"],          "region": "NCR"},
        ]
        if prices.get("diesel_plus"):
            entries.append({"brand": brand, "fuel_type": "Diesel Plus", "price": prices["diesel_plus"], "region": "NCR"})
        if prices.get("kerosene"):
            entries.append({"brand": brand, "fuel_type": "Kerosene",    "price": prices["kerosene"],    "region": "NCR"})
        return entries

    snapshot = {
        "week":   week_str,
        "date":   date_str,
        "source": "DOE Philippines (OIMB) — manual upload",
        "note":   f"Manually ingested from PDF on {datetime.utcnow().strftime('%Y-%m-%d')}",
        "ncr_common": {
            "ron91":       prices["ron91"],
            "ron95":       prices["ron95"],
            "ron97":       prices["ron97"],
            "diesel":      prices["diesel"],
            "diesel_plus": prices.get("diesel_plus"),
            "kerosene":    prices.get("kerosene"),
        },
        "prices": (
            brand_entries("Petron") +
            brand_entries("Shell") +
            brand_entries("Caltex") +
            brand_entries("Phoenix") +
            brand_entries("Seaoil")
        ),
    }

    if existing_index is not None:
        data["weekly_snapshots"][existing_index] = snapshot
    else:
        data.setdefault("weekly_snapshots", []).append(snapshot)

    # Update price_history
    for key in ["ron91", "ron95", "ron97", "diesel"]:
        history = data.setdefault("price_history", {}).setdefault(key, [])
        data["price_history"][key] = [h for h in history if h["date"] != date_str]
        data["price_history"][key].append({"date": date_str, "week": week_str, "value": prices[key]})
        data["price_history"][key].sort(key=lambda x: x["date"])

    data.setdefault("meta", {})["last_updated"] = datetime.utcnow().isoformat() + "Z"
    save_json(data)

    print(f"\n✅ Saved to {DATA_PATH}:")
    print(f"   RON 91      = ₱{prices['ron91']}")
    print(f"   RON 95      = ₱{prices['ron95']}")
    print(f"   RON 97      = ₱{prices['ron97']}")
    print(f"   Diesel      = ₱{prices['diesel']}")
    if prices.get("diesel_plus"):
        print(f"   Diesel Plus = ₱{prices['diesel_plus']}")
    if prices.get("kerosene"):
        print(f"   Kerosene    = ₱{prices['kerosene']}")
    print(f"\n   Week: {week_str}  |  Date: {date_str}")
    print("\nNext: git add data/prices.json && git commit -m '🛢️ Manual update' && git push")


# ── INFER DATE FROM FILENAME ───────────────────────────────────────────────────

def infer_date_from_filename(pdf_path: Path) -> datetime | None:
    """
    Try to extract the date from the PDF filename.
    Supports: NCR_Price_Monitoring_04212026.pdf  →  2026-04-21
    """
    name = pdf_path.stem  # e.g. NCR_Price_Monitoring_04212026
    match = re.search(r"(\d{2})(\d{2})(\d{4})$", name)
    if match:
        month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return datetime(year, month, day)
        except ValueError:
            pass
    return None


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Manually ingest a DOE price monitoring PDF into prices.json"
    )
    parser.add_argument("pdf", type=Path, help="Path to the DOE PDF file")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Week date in YYYY-MM-DD format (default: inferred from filename or today)",
    )
    args = parser.parse_args()

    if not args.pdf.exists():
        sys.exit(f"❌ File not found: {args.pdf}")

    print("=" * 60)
    print("PH Fuel Tracker — Manual PDF Ingest")
    print(f"PDF: {args.pdf}")
    print("=" * 60)

    # Determine the date
    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d")
        print(f"📅 Using provided date: {date.strftime('%Y-%m-%d')}")
    else:
        date = infer_date_from_filename(args.pdf)
        if date:
            print(f"📅 Date inferred from filename: {date.strftime('%Y-%m-%d')}")
        else:
            date = datetime.utcnow()
            print(f"📅 Could not infer date — using today: {date.strftime('%Y-%m-%d')}")

    prices = extract_prices_from_pdf(args.pdf)
    print("\nExtracted prices:", prices)

    if not prices:
        sys.exit("🚫 No prices extracted. Check if pdfplumber can read this PDF.")

    if not is_valid(prices):
        sys.exit("🚫 Prices failed validation — aborting.")

    update_data(date, prices)


if __name__ == "__main__":
    main()
