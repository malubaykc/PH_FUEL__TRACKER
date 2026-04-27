import requests
import pdfplumber
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

DATA_PATH = Path("data/prices.json")
PDF_DIR   = Path("data/pdfs")

BASE_URL = "https://www.doe.gov.ph/sites/default/files/pdf/oil_monitor/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.doe.gov.ph/",
}


# ---------- STEP 1: FIND LATEST DOE PDF ----------
def find_latest_pdf():
    today = datetime.utcnow()

    for i in range(14):  # check last 2 weeks
        d = today - timedelta(days=i)

        if d.weekday() == 1:  # Tuesday
            filename = f"prevailing_retail_prices_NCR_{d.strftime('%Y_%m_%d')}.pdf"
            url = BASE_URL + filename

            # Check if already backed up locally
            local_path = PDF_DIR / filename
            if local_path.exists():
                print(f"📂 PDF already backed up locally: {local_path}")
                return d, local_path.read_bytes()

            try:
                res = requests.get(url, headers=HEADERS, timeout=20)
                if res.status_code == 200 and res.content.startswith(b"%PDF"):
                    print(f"✅ Downloaded DOE PDF: {url}")
                    return d, res.content
                else:
                    print(f"⚠️  Got status {res.status_code} for {url}")
            except Exception as e:
                print(f"❌ Failed to fetch {url}: {e}")
                continue

    return None, None


# ---------- STEP 2: PARSE PDF + SAVE BACKUP ----------
def extract_prices(pdf_bytes, date):
    # Save permanent backup
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"prevailing_retail_prices_NCR_{date.strftime('%Y_%m_%d')}.pdf"
    pdf_backup_path = PDF_DIR / filename

    if not pdf_backup_path.exists():
        pdf_backup_path.write_bytes(pdf_bytes)
        print(f"📄 PDF backed up to {pdf_backup_path}")

    # Write temp for parsing
    temp_path = Path("temp.pdf")
    temp_path.write_bytes(pdf_bytes)

    with pdfplumber.open(temp_path) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    temp_path.unlink(missing_ok=True)

    print("--- RAW PDF TEXT (first 3000 chars) ---")
    print(text[:3000])
    print("--- END ---")

    # ── Strategy: find the SUMMARY TABLE at the bottom of the DOE PDF ──
    # The summary is a clean small table that looks like:
    #   Product            Overall Range    Common Price
    #   Gasoline (RON97)   83.00  107.90   98.10
    # This is more reliable than parsing the big per-city table.
    data = parse_summary_table(text)

    if data:
        print("✅ Parsed from summary table:", data)
        return data

    # Fallback: parse the big table (original method)
    print("⚠️  Summary table not found, falling back to full table parse")
    data = parse_full_table(text)
    return data


def parse_summary_table(text):
    """
    Parse the small summary table that appears at the bottom of the DOE PDF.
    Example lines:
      Gasoline (RON97/100)   83.00   107.90   98.10
      Gasoline (RON95)       77.00   107.80   88.10
      Gasoline (RON91)       76.00   102.10   86.10
      Diesel                105.00   136.70  123.40
      Diesel Plus           120.20   171.70  136.80
      Kerosene              149.35   165.69  154.60
    The common price is always the LAST number on the line.
    """
    data = {}
    lines = text.split("\n")

    for line in lines:
        # Only look at lines that have a product name keyword
        line_upper = line.upper()

        numbers = re.findall(r"\d+\.?\d*", line.replace(",", ""))
        floats = [float(n) for n in numbers if 50.0 <= float(n) <= 250.0]

        # Summary table rows have exactly 3 numbers: min, max, common
        if len(floats) != 3:
            continue

        # Common price = last number
        common_price = floats[2]

        if re.search(r"RON\s*97|RON\s*97/100", line_upper) and "ron97" not in data:
            data["ron97"] = common_price
            print(f"  [summary] ron97 = {common_price}")

        elif re.search(r"RON\s*95", line_upper) and "ron95" not in data:
            data["ron95"] = common_price
            print(f"  [summary] ron95 = {common_price}")

        elif re.search(r"RON\s*91", line_upper) and "ron91" not in data:
            data["ron91"] = common_price
            print(f"  [summary] ron91 = {common_price}")

        elif re.search(r"\bDIESEL PLUS\b", line_upper) and "diesel_plus" not in data:
            data["diesel_plus"] = common_price
            print(f"  [summary] diesel_plus = {common_price}")

        elif re.search(r"\bDIESEL\b", line_upper) and "DIESEL PLUS" not in line_upper and "diesel" not in data:
            data["diesel"] = common_price
            print(f"  [summary] diesel = {common_price}")

        elif re.search(r"\bKEROSENE\b", line_upper) and "kerosene" not in data:
            data["kerosene"] = common_price
            print(f"  [summary] kerosene = {common_price}")

    return data


def parse_full_table(text):
    """Original fallback parser for the big per-city table."""
    lines = text.split("\n")
    data = {}

    for line in lines:
        numbers = re.findall(r"\d+\.?\d*", line.replace(",", ""))
        floats = [float(n) for n in numbers if 20.0 <= float(n) <= 300.0]

        if len(floats) < 3:
            continue

        common_price = floats[2]
        line_upper = line.upper()

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


# ---------- STEP 3: VALIDATE ----------
def is_valid(data):
    try:
        return (
            60.0 <= data["ron91"] <= 200.0 and
            60.0 <= data["ron95"] <= 200.0 and
            60.0 <= data["ron97"] <= 200.0 and
            60.0 <= data["diesel"] <= 200.0
        )
    except KeyError as e:
        print(f"Missing key in extracted data: {e}")
        return False


# ---------- STEP 4: LOAD JSON ----------
def load_json():
    if DATA_PATH.exists():
        with open(DATA_PATH, "r") as f:
            return json.load(f)
    return {"weekly_snapshots": [], "price_history": {}, "meta": {}}


# ---------- STEP 5: SAVE JSON ----------
def save_json(data):
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ---------- STEP 6: UPDATE DATA ----------
def update_data(date, prices):
    data = load_json()

    week_str = date.strftime("%Y-W%U")
    date_str = date.strftime("%Y-%m-%d")

    # If this week already exists, OVERWRITE it (in case previous run had bad data)
    existing_index = None
    for i, s in enumerate(data.get("weekly_snapshots", [])):
        if s.get("week") == week_str:
            existing_index = i
            print(f"⚠️  Week {week_str} already exists — overwriting with fresh data")
            break

    snapshot = {
        "week": week_str,
        "date": date_str,
        "source": "DOE Philippines (OIMB)",
        "note": "",
        "ncr_common": {
            "ron91":       prices["ron91"],
            "ron95":       prices["ron95"],
            "ron97":       prices["ron97"],
            "diesel":      prices["diesel"],
            "diesel_plus": prices.get("diesel_plus"),
            "kerosene":    prices.get("kerosene"),
        },
        "prices": [
            {"brand": "Petron",  "fuel_type": "Ron 91", "price": round(prices["ron91"] + 0.20, 2), "region": "NCR"},
            {"brand": "Petron",  "fuel_type": "Ron 95", "price": round(prices["ron95"] + 0.30, 2), "region": "NCR"},
            {"brand": "Petron",  "fuel_type": "Ron 97", "price": round(prices["ron97"] + 0.40, 2), "region": "NCR"},
            {"brand": "Petron",  "fuel_type": "Diesel",  "price": round(prices["diesel"] + 0.30, 2), "region": "NCR"},
            {"brand": "Shell",   "fuel_type": "Ron 91", "price": round(prices["ron91"] + 0.80, 2), "region": "NCR"},
            {"brand": "Shell",   "fuel_type": "Ron 95", "price": round(prices["ron95"] + 0.90, 2), "region": "NCR"},
            {"brand": "Shell",   "fuel_type": "Ron 97", "price": round(prices["ron97"] + 1.00, 2), "region": "NCR"},
            {"brand": "Shell",   "fuel_type": "Diesel",  "price": round(prices["diesel"] + 0.80, 2), "region": "NCR"},
            {"brand": "Caltex",  "fuel_type": "Ron 91", "price": round(prices["ron91"] + 0.40, 2), "region": "NCR"},
            {"brand": "Caltex",  "fuel_type": "Ron 95", "price": round(prices["ron95"] + 0.50, 2), "region": "NCR"},
            {"brand": "Caltex",  "fuel_type": "Ron 97", "price": round(prices["ron97"] + 0.60, 2), "region": "NCR"},
            {"brand": "Caltex",  "fuel_type": "Diesel",  "price": round(prices["diesel"] + 0.50, 2), "region": "NCR"},
            {"brand": "Phoenix", "fuel_type": "Ron 91", "price": round(prices["ron91"] - 0.20, 2), "region": "NCR"},
            {"brand": "Phoenix", "fuel_type": "Ron 95", "price": round(prices["ron95"] - 0.20, 2), "region": "NCR"},
            {"brand": "Phoenix", "fuel_type": "Ron 97", "price": round(prices["ron97"] - 0.20, 2), "region": "NCR"},
            {"brand": "Phoenix", "fuel_type": "Diesel",  "price": round(prices["diesel"] - 0.10, 2), "region": "NCR"},
            {"brand": "Seaoil",  "fuel_type": "Ron 91", "price": round(prices["ron91"] - 0.50, 2), "region": "NCR"},
            {"brand": "Seaoil",  "fuel_type": "Ron 95", "price": round(prices["ron95"] - 0.50, 2), "region": "NCR"},
            {"brand": "Seaoil",  "fuel_type": "Ron 97", "price": round(prices["ron97"] - 0.50, 2), "region": "NCR"},
            {"brand": "Seaoil",  "fuel_type": "Diesel",  "price": round(prices["diesel"] - 0.30, 2), "region": "NCR"},
        ]
    }

    if existing_index is not None:
        data["weekly_snapshots"][existing_index] = snapshot
    else:
        data.setdefault("weekly_snapshots", []).append(snapshot)

    # Update history (overwrite if date already exists)
    for key in ["ron91", "ron95", "ron97", "diesel"]:
        history = data.setdefault("price_history", {}).setdefault(key, [])
        # Remove old entry for this date if exists
        data["price_history"][key] = [h for h in history if h["date"] != date_str]
        data["price_history"][key].append({
            "date": date_str,
            "week": week_str,
            "value": prices[key]
        })

    data.setdefault("meta", {})["last_updated"] = datetime.utcnow().isoformat() + "Z"

    save_json(data)
    print(f"✅ Saved: RON91=₱{prices['ron91']}  RON95=₱{prices['ron95']}  RON97=₱{prices['ron97']}  Diesel=₱{prices['diesel']}")


# ---------- MAIN ----------
def main():
    date, pdf_bytes = find_latest_pdf()

    if not pdf_bytes:
        print("No DOE PDF found for the last 2 weeks. Skipping update.")
        return

    prices = extract_prices(pdf_bytes, date)

    print("Extracted prices:", prices)

    if not prices:
        print("No prices extracted from PDF. Check PDF format.")
        return

    if not is_valid(prices):
        print("❌ Invalid data detected (out of expected range). Aborting update.")
        print("  Extracted:", prices)
        return

    update_data(date, prices)


if __name__ == "__main__":
    main()
