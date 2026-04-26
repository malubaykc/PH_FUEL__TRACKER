import requests
import pdfplumber
import json
from datetime import datetime, timedelta
from pathlib import Path

DATA_PATH = Path("data/prices.json")

BASE_URL = "https://www.doe.gov.ph/sites/default/files/pdf/oil_monitor/"

# ---------- STEP 1: FIND LATEST DOE PDF ----------
def find_latest_pdf():
    today = datetime.utcnow()

    for i in range(14):  # check last 2 weeks
        d = today - timedelta(days=i)

        # DOE releases usually Tuesday
        if d.weekday() == 1:
            filename = f"prevailing_retail_prices_NCR_{d.strftime('%Y_%m_%d')}.pdf"
            url = BASE_URL + filename

            try:
                res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
                if res.status_code == 200 and res.content.startswith(b"%PDF"):
                    print(f"Found DOE PDF: {url}")
                    return d, res.content
            except:
                continue

    return None, None


# ---------- STEP 2: PARSE PDF ----------
def extract_prices(pdf_bytes):
    with open("temp.pdf", "wb") as f:
        f.write(pdf_bytes)

    with pdfplumber.open("temp.pdf") as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    lines = text.split("\n")

    data = {}

    for line in lines:
        if "RON91" in line:
            data["ron91"] = float(line.split()[-1])
        elif "RON95" in line:
            data["ron95"] = float(line.split()[-1])
        elif "RON97" in line or "RON97/100" in line:
            data["ron97"] = float(line.split()[-1])
        elif "Diesel" in line and "Plus" not in line:
            data["diesel"] = float(line.split()[-1])

    return data


# ---------- STEP 3: VALIDATE ----------
def is_valid(data):
    try:
        return (
            70 <= data["ron91"] <= 100 and
            70 <= data["ron95"] <= 110 and
            90 <= data["diesel"] <= 140
        )
    except:
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

    # prevent duplicate week
    for s in data.get("weekly_snapshots", []):
        if s.get("week") == week_str:
            print("Week already exists. Skipping.")
            return

    snapshot = {
        "week": week_str,
        "date": date_str,
        "prices": [
            {"product": "RON 91", "common_price": prices["ron91"]},
            {"product": "RON 95", "common_price": prices["ron95"]},
            {"product": "RON 97/100", "common_price": prices["ron97"]},
            {"product": "Diesel", "common_price": prices["diesel"]},
        ]
    }

    data.setdefault("weekly_snapshots", []).append(snapshot)

    # update history
    for key, label in [
        ("ron91", "ron91"),
        ("ron95", "ron95"),
        ("ron97", "ron97"),
        ("diesel", "diesel"),
    ]:
        data.setdefault("price_history", {}).setdefault(label, []).append({
            "date": date_str,
            "week": week_str,
            "value": prices[key]
        })

    data.setdefault("meta", {})["last_updated"] = date_str

    save_json(data)
    print("Data updated successfully.")


# ---------- MAIN ----------
def main():
    date, pdf_bytes = find_latest_pdf()

    if not pdf_bytes:
        print("No DOE PDF found. Skipping update.")
        return

    prices = extract_prices(pdf_bytes)

    print("Extracted:", prices)

    if not is_valid(prices):
        print("Invalid data detected. Aborting update.")
        return

    update_data(date, prices)


if __name__ == "__main__":
    main()
