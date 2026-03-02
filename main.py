import os
import time
import logging
import requests

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")

FIRST_DB_ID = "20dc9afdd53b803ea6c0d89c6e2f8c2f"
SECOND_DB_ID = "4ab04fc0a82642b6bd01354ae11ea291"

NOTION_VERSION = os.environ.get("NOTION_VERSION", "2022-06-28")

# Railway-en élesben hagyd INFO-n. Ha részletes kell: DEBUG.
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# 5 perc default, de állítható env-ből
SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "300"))

# Ritkított progress log: ennyi elem feldolgozása után ír 1 sort (0 = kikapcsol)
PROGRESS_EVERY = int(os.environ.get("PROGRESS_EVERY", "200"))

# Opcionális: nagyon pörgős környezetben minimális lassítás ciklusonként (0 = kikapcsol)
PER_ITEM_SLEEP_MS = int(os.environ.get("PER_ITEM_SLEEP_MS", "0"))

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("notion-relations-sync")


def query_database(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    payload = {}

    while True:
        try:
            res = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        except requests.RequestException as e:
            logger.error("DB query request error (db=%s): %s", database_id, e)
            break

        if res.status_code != 200:
            body = (res.text or "")[:500]
            logger.error("DB query failed (db=%s) status=%s body=%s", database_id, res.status_code, body)
            break

        data = res.json()
        results = data.get("results")
        if results is None:
            logger.error("DB query invalid response (db=%s): %s", database_id, str(data)[:500])
            break

        all_results.extend(results)

        if data.get("has_more"):
            payload["start_cursor"] = data.get("next_cursor")
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
                lookup.setdefault(code, []).append(item["id"])
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

    try:
        res = requests.patch(url, headers=HEADERS, json=payload, timeout=60)
    except requests.RequestException as e:
        logger.error("Update request error (page=%s): %s", first_page_id, e)
        return False

    if res.status_code != 200:
        body = (res.text or "")[:500]
        logger.error("Update failed (page=%s) status=%s body=%s", first_page_id, res.status_code, body)
        return False

    return True


def main():
    if not NOTION_TOKEN:
        logger.error("NOTION_TOKEN env var is missing. Exiting.")
        return

    logger.info("Kapcsolatok frissítése indul...")

    second_lookup = get_second_db_lookup()
    logger.info("Második DB projektkód kulcsok száma: %d", len(second_lookup))

    first_entries = query_database(FIRST_DB_ID)
    logger.info("Első adatbázis sorainak száma: %d", len(first_entries))

    updated = 0
    unchanged = 0
    no_match = 0
    bad_code = 0
    update_failed = 0

    total = len(first_entries)

    for i, entry in enumerate(first_entries, start=1):
        first_id = entry.get("id", "<unknown>")

        try:
            title_property = entry["properties"]["PROJEKTKÓD"]["title"]
            code = title_property[0]["plain_text"].strip()
        except (KeyError, IndexError, TypeError):
            bad_code += 1
            # csak figyelmeztetés, de nem minden rekordnál fog történni
            logger.warning("Hibás vagy hiányzó projektkód (page=%s)", first_id)
            continue

        ids = second_lookup.get(code)
        if not ids:
            no_match += 1
            logger.debug("Nincs egyező Projektkód: %s", code)
        else:
            ids_to_link = sorted(ids)
            current_ids = sorted(get_current_relations(entry))

            if ids_to_link != current_ids:
                if update_relation(first_id, ids_to_link):
                    updated += 1
                    logger.debug("Kapcsolat frissítve: %s → %d elem", code, len(ids_to_link))
                else:
                    update_failed += 1
            else:
                unchanged += 1
                logger.debug("Nincs változás: %s", code)

        # Ritkított progress log, hogy ne legyen log spam
        if PROGRESS_EVERY > 0 and i % PROGRESS_EVERY == 0:
            logger.info(
                "Progress: %d/%d | frissítve=%d | nem változott=%d | nincs egyezés=%d | hibás kód=%d | update hiba=%d",
                i, total, updated, unchanged, no_match, bad_code, update_failed
            )

        if PER_ITEM_SLEEP_MS > 0:
            time.sleep(PER_ITEM_SLEEP_MS / 1000.0)

    logger.info(
        "Kész. Összegzés: összes=%d | frissítve=%d | nem változott=%d | nincs egyezés=%d | hibás kód=%d | update hiba=%d",
        total, updated, unchanged, no_match, bad_code, update_failed
    )


if __name__ == "__main__":
    while True:
        main()
        time.sleep(SLEEP_SECONDS)
