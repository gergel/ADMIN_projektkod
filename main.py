import os
import time
import requests

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
FORGATASOK_DB_ID = os.environ["FORGATASOK_DB_ID"]  # 20dc9afdd53b803ea6c0d89c6e2f8c2f
PROJECTS_DB_ID = os.environ["PROJECTS_DB_ID"]      # 4ab04fc0a82642b6bd01354ae11ea291

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def get_all_pages(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    results = []
    has_more = True
    next_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if next_cursor:
            payload["start_cursor"] = next_cursor

        response = requests.post(url, headers=HEADERS, json=payload)
        data = response.json()

        if "results" in data:
            results.extend(data["results"])
            has_more = data.get("has_more", False)
            next_cursor = data.get("next_cursor")
        else:
            print(f"❌ Hiba a lekérdezésnél: {data}")
            break

    return results

def extract_title(page, property_name):
    try:
        return page["properties"][property_name]["title"][0]["text"]["content"]
    except (KeyError, IndexError, TypeError):
        return None

def extract_text(page, property_name):
    try:
        return page["properties"][property_name]["rich_text"][0]["text"]["content"]
    except (KeyError, IndexError, TypeError):
        return None

def update_relation(page_id, relation_ids):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            "PROJEKTKÓD": {
                "relation": [{"id": pid} for pid in relation_ids]
            }
        }
    }
    res = requests.patch(url, headers=HEADERS, json=payload)
    return res.status_code == 200

def main():
    print("🔍 Keresés indul...")

    forgatasok = get_all_pages(FORGATASOK_DB_ID)
    print(f"📄 Forgatások száma: {len(forgatasok)}")

    projektek = get_all_pages(PROJECTS_DB_ID)

    projektkod_to_id = {}
    for projekt in projektek:
        kod = extract_text(projekt, "Projektkód")
        if kod:
            projektkod_to_id.setdefault(kod, []).append(projekt["id"])

    for page in forgatasok:
        page_id = page["id"]
        projektkod = extract_title(page, "Projektkód")

        if not projektkod:
            print(f"❗ Hiányzó projektkód a {page_id} sorban")
            continue

        matching_ids = projektkod_to_id.get(projektkod)
        if not matching_ids:
            print(f"❌ Nincs találat ehhez a projektkódhoz: {projektkod}")
            continue

        success = update_relation(page_id, matching_ids)
        if success:
            print(f"✅ Kapcsolat frissítve: {projektkod} → {len(matching_ids)} elem")
        else:
            print(f"⚠️ Sikertelen frissítés: {projektkod}")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(60)
