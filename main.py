import os
import time
import requests

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
SOURCE_DB_ID = "20dc9afdd53b803ea6c0d89c6e2f8c2f"
TARGET_DB_ID = "4ab04fc0a82642b6bd01354ae11ea291"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def query_database(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    response = requests.post(url, headers=HEADERS)
    return response.json().get("results", [])

def extract_project_codes(entries, code_field_name):
    result = {}
    for entry in entries:
        props = entry.get("properties", {})
        code_data = props.get(code_field_name, {})
        if code_data.get("type") == "rich_text":
            texts = code_data.get("rich_text", [])
            if texts:
                code = texts[0].get("plain_text", "").strip()
                result[code] = entry["id"]
    return result

def update_relation(source_page_id, relation_property_name, target_page_id):
    url = f"https://api.notion.com/v1/pages/{source_page_id}"
    payload = {
        "properties": {
            relation_property_name: {
                "relation": [{"id": target_page_id}]
            }
        }
    }
    requests.patch(url, headers=HEADERS, json=payload)

def main():
    print("🔄 Notion szinkronizálás indul...")
    source_entries = query_database(SOURCE_DB_ID)
    target_entries = query_database(TARGET_DB_ID)

    target_map = extract_project_codes(target_entries, "PROJEKTKÓD")

    for entry in source_entries:
        props = entry.get("properties", {})
        code_data = props.get("Projektkód", {})
        if code_data.get("type") == "rich_text":
            texts = code_data.get("rich_text", [])
            if texts:
                code = texts[0].get("plain_text", "").strip()
                if code in target_map:
                    update_relation(entry["id"], "id", target_map[code])
                    print(f"✅ Kapcsolat beállítva: {code}")
                else:
                    print(f"⚠️ Nincs találat: {code}")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(60)
