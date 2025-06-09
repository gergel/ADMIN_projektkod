import os
import time
import requests

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
FIRST_DB_ID = "20dc9afdd53b803ea6c0d89c6e2f8c2f"     # Ahol a PROJEKTKÓD title
SECOND_DB_ID = "4ab04fc0a82642b6bd01354ae11ea291"   # Ahol a Projektkód rich_text

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def query_database(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    res = requests.post(url, headers=HEADERS)
    return res.json().get("results", [])

def get_second_db_lookup():
    results = query_database(SECOND_DB_ID)
    lookup = {}
    for item in results:
        try:
            code = item["properties"]["Projektkód"]["rich_text"][0]["plain_text"]
            lookup[code] = item["id"]
        except (KeyError, IndexError, TypeError):
            continue
    return lookup

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
    print("🔁 Kapcsolatok frissítése...")
    second_lookup = get_second_db_lookup()
    first_entries = query_database(FIRST_DB_ID)

    for entry in first_entries:
        first_id = entry["id"]
        try:
            # A title mező speciális, így így kell lekérni:
            title_property = entry["properties"]["PROJEKTKÓD"]["title"]
            code = title_property[0]["plain_text"]
        except (KeyError, IndexError, TypeError):
            print(f"⚠️ Hibás vagy hiányzó projektkód: {first_id}")
            continue

        if code in second_lookup:
            success = update_relation(first_id, [second_lookup[code]])
            if success:
                print(f"✅ Kapcsolat létrehozva: {code}")
            else:
                print(f"❌ Sikertelen frissítés: {code}")
        else:
            print(f"❗ Nincs egyező elem a másik adatbázisban: {code}")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(60)
