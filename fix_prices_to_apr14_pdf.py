import json
from pathlib import Path

JSON_PATH = Path("data/prices.json")

REMOVE_WEEK = "2026-W17"
REMOVE_DATE = "2026-04-21"
SET_LAST_UPDATED = "2026-04-14T00:00:00Z"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def remove_apr21_snapshot(data: dict):
    weekly = data.get("weekly_snapshots", [])
    if not isinstance(weekly, list):
        return data

    data["weekly_snapshots"] = [
        s for s in weekly
        if not (
            s.get("week") == REMOVE_WEEK or
            s.get("date") == REMOVE_DATE
        )
    ]
    return data


def remove_apr21_price_history(data: dict):
    history = data.get("price_history", {})
    if not isinstance(history, dict):
        return data

    for key, entries in history.items():
        if not isinstance(entries, list):
            continue
        history[key] = [
            item for item in entries
            if not (
                item.get("week") == REMOVE_WEEK or
                item.get("date") == REMOVE_DATE
            )
        ]
    return data


def update_meta(data: dict):
    meta = data.get("meta", {})
    if not isinstance(meta, dict):
        data["meta"] = {}
        meta = data["meta"]

    meta["last_updated"] = SET_LAST_UPDATED

    weekly = data.get("weekly_snapshots", [])
    if isinstance(weekly, list):
        meta["total_records"] = sum(
            len(s.get("prices", []))
            for s in weekly
            if isinstance(s, dict)
        )

    return data


def main():
    if not JSON_PATH.exists():
        raise FileNotFoundError(f"Could not find {JSON_PATH}")

    data = load_json(JSON_PATH)

    data = remove_apr21_snapshot(data)
    data = remove_apr21_price_history(data)
    data = update_meta(data)

    save_json(JSON_PATH, data)

    print("Done.")
    print("Removed 2026-W17 / 2026-04-21 from weekly_snapshots and price_history.")
    print("Set meta.last_updated to 2026-04-14T00:00:00Z.")
    print("Your dashboard should now use the Apr 14 week that matches the DOE PDF.")


if __name__ == "__main__":
    main()
