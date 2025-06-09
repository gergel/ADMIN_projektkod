import os
import time
import requests

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
FORGATASOK_DB_ID = os.environ["FORGATASOK_DB_ID"]
PROJECTS_DB_ID = os.environ["PROJECTS_DB_ID"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def get_all_pages(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    has_more = True
    next_cursor = None

    while has_more:
        payload = {}
        if next_cursor:
            payload["start_cursor"] = next_cursor
        response = requests.post(url, headers=HEADERS, json=payload)
        data = response.json()
        all_results.extend(data["results"])
        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")

    return all_results

def extract_plain_text_from_title(properties):
    title = properties.get("Projektkód", {}).get("title", [])
    if title and "plain_text" in title[0]:
        return title[0]["plain_text"].strip()
    return None

def extract_title_page_id_map(pages):
    mapping = {}
    for page in pages:
        properties = page["properties"]
        code = extract_plain_text_from_title(properties)
        if code:
            mapping.setdefault(code, []).append(page["id"])
    return mapping

def update_relation(page_id, relation_ids):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            "Kapcsolódó projekt(ek)": {
                "relation": [{"id": rel_id} for rel_id in relation_ids]
            }
        }
    }
    response = requests.patch(url, headers=HEADERS, json=payload)
    return response.status_code == 200

def main():
    print("🔁 Keresés indul...")
    forgasok = get_all_pages(FORGATASOK_DB_ID)
    print(f"📄 Forgatások száma: {len(forgasok)}")

    projektek = get_all_pages(PROJECTS_DB_ID)
    projektkod_map = extract_title_page_id_map(projektek)

    kapcsolt = 0
    for item in forgasok:
        page_id = item["id"]
        properties = item["properties"]
        kod = extract_plain_text_from_title(properties)

        if not kod:
            print(f"❗ Hiányzó Projektkód a {page_id} sorban")
            continue

        if kod not in projektkod_map:
            print(f"❗ Nincs találat erre a Projektkódra: {kod}")
            continue

        siker = update_relation(page_id, projektkod_map[kod])
        if siker:
            kapcsolt += 1
            print(f"✅ Kapcsolat frissítve: {kod}")
        else:
            print(f"⚠️ Sikertelen frissítés: {kod}")

    print(f"🟢 Összesen frissített kapcsolatok: {kapcsolt}")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(60)
