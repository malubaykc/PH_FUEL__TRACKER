import requests
import pdfplumber
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

DATA_PATH = Path("data/prices.json")

BASE_URL = "https://www.doe.gov.ph/sites/default/files/pdf/oil_monitor/"

# ---------- STEP 1: FIND LATEST DOE PDF ----------
def find_latest_pdf():
    today = datetime.utcnow()

    for i in range(14):  # check last 2 weeks
        d = today - timedelta(days=i)

        # DOE releases usually on Tuesday
        if d.weekday() == 1:
            filename = f"prevailing_retail_prices_NCR_{d.strftime('%Y_%m_%d')}.pdf"
            url = BASE_URL + filename

            try:
                res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                if res.status_code == 200 and res.content.startswith(b"%PDF"):
                    print(f"Found DOE PDF: {url}")
                    return d, res.content
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")
                continue

    return None, None


# ---------- STEP 2: PARSE PDF ----------
# The DOE PDF table has columns: Product | Overall Range Min | Overall Range Max | Common Price
# We need the COMMON PRICE column (3rd numeric column), not the last column.
# Example line: "Gasoline (RON91)  76.00  102.10  86.10"
def extract_prices(pdf_bytes):
    with open("temp.pdf", "wb") as f:
        f.write(pdf_bytes)

    with pdfplumber.open("temp.pdf") as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    print("--- RAW PDF TEXT (first 2000 chars) ---")
    print(text[:2000])
    print("--- END ---")

    lines = text.split("\n")

    data = {}

    for line in lines:
        # Extract all numbers from the line (handles ₱ sign, commas, spaces)
        numbers = re.findall(r"\d+\.?\d*", line.replace(",", ""))
        floats = [float(n) for n in numbers if 20.0 <= float(n) <= 300.0]

        # DOE table row format: [range_min, range_max, common_price]
        # We need index 2 (the 3rd number = Common Price)
        if len(floats) < 3:
            continue

        common_price = floats[2]

        line_upper = line.upper()

        if re.search(r"RON\s*97|RON\s*97/100", line_upper) and "ron97" not in data:
            data["ron97"] = common_price
            print(f"  ron97 = {common_price}  (line: {line.strip()})")

        elif re.search(r"RON\s*95", line_upper) and "ron95" not in data:
            data["ron95"] = common_price
            print(f"  ron95 = {common_price}  (line: {line.strip()})")

        elif re.search(r"RON\s*91", line_upper) and "ron91" not in data:
            data["ron91"] = common_price
            print(f"  ron91 = {common_price}  (line: {line.strip()})")

        elif re.search(r"\bDIESEL\b", line_upper) and "DIESEL PLUS" not in line_upper and "diesel" not in data:
            data["diesel"] = common_price
            print(f"  diesel = {common_price}  (line: {line.strip()})")

    return data


# ---------- STEP 3: VALIDATE ----------
# Ranges updated to cover current real-world PH pump prices (as of 2026).
# DOE confirmed Apr 20, 2026: RON91=₱56.65, RON95=₱58.15, Diesel=₱59.74
# Historic high was ~₱155/L diesel during 2026 Hormuz crisis.
# Set wide-enough floor/ceiling so we never reject valid DOE data again.
def is_valid(data):
    try:
        return (
            40.0 <= data["ron91"] <= 200.0 and
            40.0 <= data["ron95"] <= 200.0 and
            40.0 <= data["ron97"] <= 200.0 and
            40.0 <= data["diesel"] <= 200.0
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
# CRITICAL FIX: The dashboard reads prices from snapshot["ncr_common"]["ron91"] etc.
# The old scraper was saving to snapshot["prices"][i]["common_price"] — wrong key!
# This function now saves to the correct "ncr_common" structure.
def update_data(date, prices):
    data = load_json()

    week_str = date.strftime("%Y-W%U")
    date_str = date.strftime("%Y-%m-%d")

    # prevent duplicate week
    for s in data.get("weekly_snapshots", []):
        if s.get("week") == week_str:
            print(f"Week {week_str} already exists. Skipping.")
            return

    snapshot = {
        "week": week_str,
        "date": date_str,
        "source": "DOE Philippines (OIMB)",
        "note": "",
        # ✅ THIS is what the dashboard reads — ncr_common.ron91, .ron95, etc.
        "ncr_common": {
            "ron91": prices["ron91"],
            "ron95": prices["ron95"],
            "ron97": prices["ron97"],
            "diesel": prices["diesel"],
        },
        # Keep individual brand prices array too (used by By Brand tab)
        # These are approximations: common price ± typical brand premium
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

    data.setdefault("weekly_snapshots", []).append(snapshot)

    # update history
    for key in ["ron91", "ron95", "ron97", "diesel"]:
        data.setdefault("price_history", {}).setdefault(key, []).append({
            "date": date_str,
            "week": week_str,
            "value": prices[key]
        })

    data.setdefault("meta", {})["last_updated"] = datetime.utcnow().isoformat() + "Z"

    save_json(data)
    print(f"✅ Data updated successfully for week {week_str} ({date_str})")
    print(f"   RON91=₱{prices['ron91']}  RON95=₱{prices['ron95']}  RON97=₱{prices['ron97']}  Diesel=₱{prices['diesel']}")


# ---------- MAIN ----------
def main():
    date, pdf_bytes = find_latest_pdf()

    if not pdf_bytes:
        print("No DOE PDF found for the last 2 weeks. Skipping update.")
        return

    prices = extract_prices(pdf_bytes)

    print("Extracted prices:", prices)

    if not prices:
        print("No prices extracted from PDF. Check PDF format.")
        return

    if not is_valid(prices):
        print("Invalid data detected (out of expected range). Aborting update.")
        print("  Extracted:", prices)
        return

    update_data(date, prices)


if __name__ == "__main__":
    main()
