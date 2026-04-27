import requests
import pdfplumber
import json
import re
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

DATA_PATH = Path("data/prices.json")
PDF_DIR   = Path("data/pdfs")

# ── DOE URL patterns (try both — DOE has used different naming conventions) ──
BASE_URL = "https://www.doe.gov.ph/sites/default/files/pdf/oil_monitor/"

URL_PATTERNS = [
    # Pattern A: NCR_Price_Monitoring_MMDDYYYY.pdf  ← actual format from your PDF
    lambda d: f"NCR_Price_Monitoring_{d.strftime('%m%d%Y')}.pdf",
    # Pattern B: prevailing_retail_prices_NCR_YYYY_MM_DD.pdf  ← old format
    lambda d: f"prevailing_retail_prices_NCR_{d.strftime('%Y_%m_%d')}.pdf",
    # Pattern C: ncr_price_monitoring_MMDDYYYY.pdf  ← lowercase variant
    lambda d: f"ncr_price_monitoring_{d.strftime('%m%d%Y')}.pdf",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
]

def make_headers():
    ua = random.choice(USER_AGENTS)
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf,*/*;q=0.8",
        "Accept-Language": "en-PH,en;q=0.9,fil;q=0.8,en-US;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "Referer": "https://www.doe.gov.ph/price-monitor",
    }


# ── STEP 1: FIND + DOWNLOAD LATEST DOE PDF ──────────────────────────────────

def fetch_with_session(url):
    """
    Use a requests Session to mimic a real browser:
    1. Hit the DOE homepage first (establish cookies/session)
    2. Wait a random human-like delay
    3. Then fetch the actual PDF
    """
    session = requests.Session()
    headers = make_headers()

    # Step 1: Visit the DOE price monitor page first (get cookies like a real browser)
    try:
        print(f"  🌐 Warming up session on DOE site...")
        session.get(
            "https://www.doe.gov.ph/price-monitor",
            headers=headers,
            timeout=15,
            allow_redirects=True,
        )
        delay = random.uniform(4, 9)
        print(f"  ⏳ Waiting {delay:.1f}s before fetching PDF...")
        time.sleep(delay)
    except Exception as e:
        print(f"  ⚠️  Warm-up request failed (non-fatal): {e}")
        time.sleep(random.uniform(2, 5))

    # Step 2: Fetch the actual PDF with fresh headers
    headers = make_headers()
    headers["Referer"] = "https://www.doe.gov.ph/price-monitor"
    headers["Accept"] = "application/pdf,*/*;q=0.9"

    res = session.get(url, headers=headers, timeout=30, allow_redirects=True)
    return res


def find_latest_pdf():
    today = datetime.utcnow()

    # Check last 14 days, only on Tuesdays (DOE publishes weekly on Tuesday)
    candidates = []
    for i in range(14):
        d = today - timedelta(days=i)
        if d.weekday() == 1:  # Tuesday = 1
            candidates.append(d)

    for date in candidates:
        # Check local backup first — no need to re-download
        for pattern_fn in URL_PATTERNS:
            filename = pattern_fn(date)
            local_path = PDF_DIR / filename
            if local_path.exists():
                print(f"📂 Found local backup: {local_path}")
                return date, local_path.read_bytes()

        # Try each URL pattern against DOE
        for pattern_fn in URL_PATTERNS:
            filename = pattern_fn(date)
            url = BASE_URL + filename
            print(f"\n🔍 Trying: {url}")

            try:
                res = fetch_with_session(url)

                if res.status_code == 200 and res.content[:4] == b"%PDF":
                    print(f"✅ Downloaded: {url} ({len(res.content):,} bytes)")
                    return date, res.content
                else:
                    print(f"  ❌ Status {res.status_code} — not a valid PDF")

            except requests.exceptions.ConnectionError as e:
                print(f"  ❌ Connection error: {e}")
            except requests.exceptions.Timeout:
                print(f"  ❌ Timed out")
            except Exception as e:
                print(f"  ❌ Unexpected error: {e}")

            # Small delay between URL pattern attempts
            time.sleep(random.uniform(1, 3))

    print("\n⚠️  No DOE PDF found after checking all patterns for the last 2 Tuesdays.")
    return None, None


# ── STEP 2: PARSE PDF ────────────────────────────────────────────────────────

def extract_prices(pdf_bytes, date):
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    # Save permanent backup using actual DOE filename format
    filename = f"NCR_Price_Monitoring_{date.strftime('%m%d%Y')}.pdf"
    pdf_backup_path = PDF_DIR / filename
    if not pdf_backup_path.exists():
        pdf_backup_path.write_bytes(pdf_bytes)
        print(f"📄 PDF backed up → {pdf_backup_path}")

    temp_path = Path("temp_doe.pdf")
    temp_path.write_bytes(pdf_bytes)

    with pdfplumber.open(temp_path) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    temp_path.unlink(missing_ok=True)

    print("\n--- RAW PDF TEXT (first 3000 chars) ---")
    print(text[:3000])
    print("--- END ---\n")

    # Try summary table first (most reliable)
    data = parse_summary_table(text)
    if data and len(data) >= 4:
        print("✅ Parsed from summary table:", data)
        return data

    # Fallback to full table
    print("⚠️  Summary table incomplete, trying full table parse...")
    data = parse_full_table(text)
    return data


def parse_summary_table(text):
    """
    Parse the summary block at the bottom of the DOE PDF.
    Looks like:
      Gasoline (RON97/100)   83.00   107.90   98.10
      Gasoline (RON95)       77.00   107.80   88.10
      Gasoline (RON91)       76.00   102.10   86.10
      Diesel                105.00   136.70  123.40
      Diesel Plus           120.20   171.70  136.80
      Kerosene              149.35   165.69  154.60

    FIX: Use >= 3 (not == 3) and always take floats[-1] as common price.
    """
    data = {}
    lines = text.split("\n")

    for line in lines:
        line_upper = line.upper()
        numbers = re.findall(r"\d+\.?\d*", line.replace(",", ""))
        # FIX: was `== 3`, which dropped lines with extra numbers
        floats = [float(n) for n in numbers if 50.0 <= float(n) <= 300.0]

        if len(floats) < 3:
            continue

        # FIX: always take LAST float — that's the common price column
        common_price = floats[-1]

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
    """
    Fallback: parse the big per-city table.
    FIX: was floats[2], now floats[-1] to get the common price (last column).
    """
    lines = text.split("\n")
    data = {}

    for line in lines:
        numbers = re.findall(r"\d+\.?\d*", line.replace(",", ""))
        floats = [float(n) for n in numbers if 50.0 <= float(n) <= 300.0]

        if len(floats) < 3:
            continue

        # FIX: common price is the LAST number, not index [2]
        common_price = floats[-1]
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


# ── STEP 3: VALIDATE ─────────────────────────────────────────────────────────

def is_valid(data):
    try:
        return (
            60.0 <= data["ron91"]  <= 250.0 and
            60.0 <= data["ron95"]  <= 250.0 and
            60.0 <= data["ron97"]  <= 250.0 and
            60.0 <= data["diesel"] <= 250.0
        )
    except KeyError as e:
        print(f"❌ Missing required key: {e}")
        return False


# ── STEP 4 & 5: LOAD / SAVE JSON ─────────────────────────────────────────────

def load_json():
    if DATA_PATH.exists():
        with open(DATA_PATH, "r") as f:
            return json.load(f)
    return {"weekly_snapshots": [], "price_history": {}, "meta": {}}


def save_json(data):
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ── STEP 6: UPDATE DATA ───────────────────────────────────────────────────────

def update_data(date, prices):
    data = load_json()

    week_str = date.strftime("%Y-W%U")
    date_str = date.strftime("%Y-%m-%d")

    existing_index = None
    for i, s in enumerate(data.get("weekly_snapshots", [])):
        if s.get("week") == week_str:
            existing_index = i
            print(f"⚠️  Week {week_str} already exists — overwriting")
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
        # NOTE: brand prices below are real NCR common prices from DOE.
        # Per-brand breakdown requires parsing the full city table —
        # using common price for all brands is more accurate than fake offsets.
        "prices": [
            {"brand": "Petron",  "fuel_type": "Ron 91", "price": prices["ron91"],  "region": "NCR"},
            {"brand": "Petron",  "fuel_type": "Ron 95", "price": prices["ron95"],  "region": "NCR"},
            {"brand": "Petron",  "fuel_type": "Ron 97", "price": prices["ron97"],  "region": "NCR"},
            {"brand": "Petron",  "fuel_type": "Diesel",  "price": prices["diesel"], "region": "NCR"},
            {"brand": "Shell",   "fuel_type": "Ron 91", "price": prices["ron91"],  "region": "NCR"},
            {"brand": "Shell",   "fuel_type": "Ron 95", "price": prices["ron95"],  "region": "NCR"},
            {"brand": "Shell",   "fuel_type": "Ron 97", "price": prices["ron97"],  "region": "NCR"},
            {"brand": "Shell",   "fuel_type": "Diesel",  "price": prices["diesel"], "region": "NCR"},
            {"brand": "Caltex",  "fuel_type": "Ron 91", "price": prices["ron91"],  "region": "NCR"},
            {"brand": "Caltex",  "fuel_type": "Ron 95", "price": prices["ron95"],  "region": "NCR"},
            {"brand": "Caltex",  "fuel_type": "Ron 97", "price": prices["ron97"],  "region": "NCR"},
            {"brand": "Caltex",  "fuel_type": "Diesel",  "price": prices["diesel"], "region": "NCR"},
            {"brand": "Phoenix", "fuel_type": "Ron 91", "price": prices["ron91"],  "region": "NCR"},
            {"brand": "Phoenix", "fuel_type": "Ron 95", "price": prices["ron95"],  "region": "NCR"},
            {"brand": "Phoenix", "fuel_type": "Ron 97", "price": prices["ron97"],  "region": "NCR"},
            {"brand": "Phoenix", "fuel_type": "Diesel",  "price": prices["diesel"], "region": "NCR"},
            {"brand": "Seaoil",  "fuel_type": "Ron 91", "price": prices["ron91"],  "region": "NCR"},
            {"brand": "Seaoil",  "fuel_type": "Ron 95", "price": prices["ron95"],  "region": "NCR"},
            {"brand": "Seaoil",  "fuel_type": "Ron 97", "price": prices["ron97"],  "region": "NCR"},
            {"brand": "Seaoil",  "fuel_type": "Diesel",  "price": prices["diesel"], "region": "NCR"},
        ]
    }

    if existing_index is not None:
        data["weekly_snapshots"][existing_index] = snapshot
    else:
        data.setdefault("weekly_snapshots", []).append(snapshot)

    for key in ["ron91", "ron95", "ron97", "diesel"]:
        history = data.setdefault("price_history", {}).setdefault(key, [])
        data["price_history"][key] = [h for h in history if h["date"] != date_str]
        data["price_history"][key].append({
            "date": date_str,
            "week": week_str,
            "value": prices[key]
        })
        data["price_history"][key].sort(key=lambda x: x["date"])

    data.setdefault("meta", {})["last_updated"] = datetime.utcnow().isoformat() + "Z"
    save_json(data)

    print(f"\n✅ Saved:")
    print(f"   RON 91  = ₱{prices['ron91']}")
    print(f"   RON 95  = ₱{prices['ron95']}")
    print(f"   RON 97  = ₱{prices['ron97']}")
    print(f"   Diesel  = ₱{prices['diesel']}")
    if prices.get("diesel_plus"):
        print(f"   D-Plus  = ₱{prices['diesel_plus']}")
    if prices.get("kerosene"):
        print(f"   Kerosene= ₱{prices['kerosene']}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("PH Fuel Tracker — DOE Scraper")
    print(f"Run time (UTC): {datetime.utcnow().isoformat()}")
    print("=" * 60)

    date, pdf_bytes = find_latest_pdf()

    if not pdf_bytes:
        print("\n🚫 No DOE PDF found. Possible reasons:")
        print("   1. DOE hasn't published this week's PDF yet")
        print("   2. The filename pattern changed — check the DOE site manually")
        print("   3. DOE is blocking the request (try self-hosted runner)")
        return

    prices = extract_prices(pdf_bytes, date)
    print("\nExtracted prices:", prices)

    if not prices:
        print("🚫 No prices extracted. PDF format may have changed.")
        return

    if not is_valid(prices):
        print("🚫 Prices out of expected range — aborting to protect data integrity.")
        print("   Extracted:", prices)
        return

    update_data(date, prices)


if __name__ == "__main__":
    main()
