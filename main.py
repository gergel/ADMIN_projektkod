import os
import time
import requests

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
FORGATASOK_DB_ID = os.environ.get("FORGATASOK_DB_ID")  # 20dc9afdd53b803ea6c0d89c6e2f8c2f
MASODIK_DB_ID = os.environ.get("MASODIK_DB_ID")        # 4ab04fc0a82642b6bd01354ae11ea291

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def query_database_all(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    results = []
    payload = {}

    while True:
        res = requests.post(url, headers=HEADERS, json=payload)
        data = res.json()
        if "results" not in data:
            print(f"❌ Hiba a lekérdezésnél: {data}")
            break
        results.extend(data["results"])
        if data.get("has_more"):
            payload["start_cursor"] = data["next_cursor"]
        else:
            break
    return results

def get_forgatasok_entries():
    return query_database_all(FORGATASOK_DB_ID)

def get_lookup_by_projektkod():
    second_entries = query_database_all(MASODIK_DB_ID)
    mapping = {}
    for entry in second_entries:
        try:
            kod = entry["properties"]["Projektkód"]["rich_text"][0]["plain_text"]
            page_id = entry["id"]
            if kod not in mapping:
                mapping[kod] = []
            mapping[kod].append({"id": page_id})
        except (KeyError, IndexError, TypeError):
            continue
    return mapping

def update_forgatas_relation(forgatas_id, related_ids):
    url = f"https://api.notion.com/v1/pages/{forgatas_id}"
    payload = {
        "properties": {
            "PROJEKTKÓD": {
                "relation": related_ids
            }
        }
    }
    res = requests.patch(url, headers=HEADERS, json=payload)
    return res.status_code == 200

def main():
    print("🔍 Keresés indul...")
    lookup = get_lookup_by_projektkod()
    forgatasok = get_forgatasok_entries()
    print(f"📄 Forgatások száma: {len(forgatasok)}")

    for entry in forgatasok:
        forgatas_id = entry["id"]
        try:
            projektkod = entry["properties"]["Projektkód"]["title"][0]["plain_text"]
        except (KeyError, IndexError, TypeError):
            print(f"❗ Hiányzó projektkód a {forgatas_id} sorban")
            continue

        kapcsolatok = lookup.get(projektkod)
        if kapcsolatok:
            success = update_forgatas_relation(forgatas_id, kapcsolatok)
            if success:
                print(f"✅ Frissítve: {projektkod} → {len(kapcsolatok)} kapcsolat")
            else:
                print(f"⚠️ Sikertelen frissítés: {projektkod}")
        else:
            print(f"❌ Nincs találat a második adatbázisban: {projektkod}")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(60)
