import os
import time
import requests

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
FIRST_DB_ID = "20dc9afdd53b803ea6c0d89c6e2f8c2f"     # Fő adatbázis
SECOND_DB_ID = "4ab04fc0a82642b6bd01354ae11ea291"   # Forgatások
THIRD_DB_ID = "1f8c9afdd53b801992e5dbf08dbc4957"    # Utómunkák

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


def get_lookup_by_project_code(database_id, field_name):
    results = query_database(database_id)
    lookup = {}
    for item in results:
        try:
            code = item["properties"][field_name]["rich_text"][0]["plain_text"].strip()
            if code:
                if code not in lookup:
                    lookup[code] = []
                lookup[code].append(item["id"])
        except (KeyError, IndexError, TypeError):
            continue
    return lookup


def get_current_relations(entry, field_name):
    try:
        return [rel["id"] for rel in entry["properties"][field_name]["relation"]]
    except (KeyError, TypeError):
        return []


def update_relation(page_id, field_name, related_ids):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            field_name: {
                "relation": [{"id": pid} for pid in related_ids]
            }
        }
    }
    res = requests.patch(url, headers=HEADERS, json=payload)
    return res.status_code == 200


def main():
    print("🔁 Kapcsolatok frissítése indul...")

    forgas_lookup = get_lookup_by_project_code(SECOND_DB_ID, "Projektkód")
    utomunka_lookup = get_lookup_by_project_code(THIRD_DB_ID, "Projektkód")

    first_entries = query_database(FIRST_DB_ID)
    kapcsolt_forg = 0
    kapcsolt_uto = 0
    kihagyva = 0

    for entry in first_entries:
        first_id = entry["id"]
        try:
            code = entry["properties"]["PROJEKTKÓD"]["title"][0]["plain_text"].strip()
        except (KeyError, IndexError, TypeError):
            print(f"⚠️ Hibás vagy hiányzó projektkód: {first_id}")
            continue

        # Forgatások
        if code in forgas_lookup:
            ids_to_link = sorted(forgas_lookup[code])
            current_ids = sorted(get_current_relations(entry, "Forgatások"))
            if ids_to_link != current_ids:
                if update_relation(first_id, "Forgatások", ids_to_link):
                    kapcsolt_forg += 1
                    print(f"🎥 Forgatás frissítve: {code} → {len(ids_to_link)} elem")
                else:
                    print(f"❌ Forgatás frissítés sikertelen: {code}")
            else:
                kihagyva += 1

        # Utómunkák
        if code in utomunka_lookup:
            ids_to_link = sorted(utomunka_lookup[code])
            current_ids = sorted(get_current_relations(entry, "Utómunkák"))
            if ids_to_link != current_ids:
                if update_relation(first_id, "Utómunkák", ids_to_link):
                    kapcsolt_uto += 1
                    print(f"🎬 Utómunka frissítve: {code} → {len(ids_to_link)} elem")
                else:
                    print(f"❌ Utómunka frissítés sikertelen: {code}")
            else:
                kihagyva += 1

    print(f"🔚 Forgatás: {kapcsolt_forg}, Utómunka: {kapcsolt_uto}, Kihagyva (nem változott): {kihagyva}")


if __name__ == "__main__":
    while True:
        main()
        time.sleep(60)
