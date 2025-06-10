import os
import time
import requests

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
FIRST_DB_ID = "20dc9afdd53b803ea6c0d89c6e2f8c2f"
SECOND_DB_ID = "4ab04fc0a82642b6bd01354ae11ea291"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}


def query_database(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    payload = {}

    while True:
        res = requests.post(url, headers=HEADERS, json=payload)
        data = res.json()

        if "results" not in data:
            print(f"❌ Hiba a lekérdezésnél: {data}")
            break

        all_results.extend(data["results"])

        if data.get("has_more"):
            payload["start_cursor"] = data["next_cursor"]
        else:
            break

    return all_results


def get_second_db_lookup():
    results = query_database(SECOND_DB_ID)
    lookup = {}
    for item in results:
        try:
            code = item["properties"]["Projektkód"]["rich_text"][0]["plain_text"].strip()
            if code:
                if code not in lookup:
                    lookup[code] = []
                lookup[code].append(item["id"])
        except (KeyError, IndexError, TypeError):
            continue
    return lookup


def get_current_relations(entry, relation_field_name="Forgatások"):
    try:
        return [rel["id"] for rel in entry["properties"][relation_field_name]["relation"]]
    except (KeyError, TypeError):
        return []


def update_relation(first_page_id, second_page_ids):
    url = f"https://api.notion.com/v1/pages/{first_page_id}"
    payload = {
        "properties": {
            "Forgatások": {
                "relation": [{"id": pid} for pid in second_page_ids]
            }
        }
    }
    res = requests.patch(url, headers=HEADERS, json=payload)
    return res.status_code == 200


def main():
    print("🔁 Kapcsolatok frissítése indul...")
    second_lookup = get_second_db_lookup()
    print(f"📄 Második DB projektkód kulcsok száma: {len(second_lookup)}")

    first_entries = query_database(FIRST_DB_ID)
    print(f"📄 Első adatbázis sorainak száma: {len(first_entries)}")

    kapcsolt = 0
    kihagyva = 0

    for entry in first_entries:
        first_id = entry["id"]
        try:
            title_property = entry["properties"]["PROJEKTKÓD"]["title"]
            code = title_property[0]["plain_text"].strip()
        except (KeyError, IndexError, TypeError):
            print(f"⚠️ Hibás vagy hiányzó projektkód: {first_id}")
            continue

        if code in second_lookup:
            ids_to_link = sorted(second_lookup[code])
            current_ids = sorted(get_current_relations(entry))

            if ids_to_link != current_ids:
                success = update_relation(first_id, ids_to_link)
                if success:
                    kapcsolt += 1
                    print(f"✅ Kapcsolat frissítve: {code} → {len(ids_to_link)} elem")
                else:
                    print(f"❌ Sikertelen frissítés: {code}")
            else:
                kihagyva += 1
                print(f"⏭️ Nincs változás: {code}")
        else:
            print(f"❗ Nincs egyező Projektkód: {code}")

    print(f"🔚 Frissített kapcsolatok: {kapcsolt}, Kihagyva (nem változott): {kihagyva}")


if __name__ == "__main__":
    while True:
        main()
        time.sleep(120)
