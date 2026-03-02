import os
import time
import logging
import requests

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")

FIRST_DB_ID = "20dc9afdd53b803ea6c0d89c6e2f8c2f"
SECOND_DB_ID = "4ab04fc0a82642b6bd01354ae11ea291"

NOTION_VERSION = os.environ.get("NOTION_VERSION", "2022-06-28")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
SLEEP_SECONDS = int(os.environ.get("SLEEP_SECONDS", "300"))
PROGRESS_EVERY = int(os.environ.get("PROGRESS_EVERY", "200"))
PER_ITEM_SLEEP_MS = int(os.environ.get("PER_ITEM_SLEEP_MS", "0"))

FAIL_FAST_ON_DB_ERROR = os.environ.get("FAIL_FAST_ON_DB_ERROR", "1") == "1"

NAME_PROP = os.environ.get("NAME_PROP", "Name")
DATE_PROP = os.environ.get("DATE_PROP", "Date")

HTTP_TIMEOUT_SECONDS = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "600"))
QUERY_MAX_RETRIES = int(os.environ.get("QUERY_MAX_RETRIES", "6"))
QUERY_RETRY_BASE_SLEEP = float(os.environ.get("QUERY_RETRY_BASE_SLEEP", "2.0"))

# SECOND_DB szűrés chunk méret (hány projektkód legyen 1 query-ben OR filterrel)
SECOND_DB_CODES_CHUNK = int(os.environ.get("SECOND_DB_CODES_CHUNK", "25"))

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


def _sleep_with_jitter(seconds: float):
    time.sleep(seconds + (0.1 * seconds))


def _post_with_retries(url, payload, label):
    """
    Generic POST with retries for timeout/5xx/429.
    Returns: (response_json, ok)
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            res = requests.post(url, headers=HEADERS, json=payload, timeout=HTTP_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            if attempt <= QUERY_MAX_RETRIES:
                wait_s = QUERY_RETRY_BASE_SLEEP * (2 ** (attempt - 1))
                logger.warning("%s request error attempt=%d/%d: %s | retry in %.1fs", label, attempt, QUERY_MAX_RETRIES, e, wait_s)
                _sleep_with_jitter(wait_s)
                continue
            logger.error("%s request error: %s", label, e)
            return None, False

        if res.status_code == 429:
            retry_after = res.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after else (QUERY_RETRY_BASE_SLEEP * (2 ** (attempt - 1)))
            if attempt <= QUERY_MAX_RETRIES:
                logger.warning("%s rate limited (429) attempt=%d/%d | retry in %.1fs", label, attempt, QUERY_MAX_RETRIES, wait_s)
                _sleep_with_jitter(wait_s)
                continue
            body = (res.text or "")[:500]
            logger.error("%s failed (429) body=%s", label, body)
            return None, False

        if 500 <= res.status_code <= 599:
            if attempt <= QUERY_MAX_RETRIES:
                wait_s = QUERY_RETRY_BASE_SLEEP * (2 ** (attempt - 1))
                logger.warning("%s server error status=%s attempt=%d/%d | retry in %.1fs", label, res.status_code, attempt, QUERY_MAX_RETRIES, wait_s)
                _sleep_with_jitter(wait_s)
                continue
            body = (res.text or "")[:500]
            logger.error("%s failed status=%s body=%s", label, res.status_code, body)
            return None, False

        if res.status_code != 200:
            body = (res.text or "")[:500]
            logger.error("%s failed status=%s body=%s", label, res.status_code, body)
            return None, False

        try:
            return res.json(), True
        except ValueError:
            logger.error("%s invalid JSON response", label)
            return None, False


def query_database(database_id, filter_payload=None):
    """
    Returns: (results, ok)
    filter_payload: Notion database query filter dict, pl:
      {"property": "Projektkód", "rich_text": {"equals": "ABC"}}
      or {"or": [ ... ]}
    """
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_results = []
    payload = {}
    if filter_payload:
        payload["filter"] = filter_payload

    while True:
        data, ok = _post_with_retries(url, payload, label=f"DB query (db={database_id})")
        if not ok:
            return [], False

        results = data.get("results")
        if results is None:
            logger.error("DB query invalid response (db=%s): %s", database_id, str(data)[:500])
            return [], False

        all_results.extend(results)

        if data.get("has_more"):
            payload["start_cursor"] = data.get("next_cursor")
        else:
            break

    return all_results, True


def _extract_title(props, prop_name):
    try:
        arr = props[prop_name]["title"]
        if not arr:
            return ""
        return (arr[0].get("plain_text") or "").strip()
    except (KeyError, IndexError, TypeError):
        return ""


def _extract_rich_text(props, prop_name):
    try:
        arr = props[prop_name]["rich_text"]
        if not arr:
            return ""
        return (arr[0].get("plain_text") or "").strip()
    except (KeyError, IndexError, TypeError):
        return ""


def _extract_date(props, prop_name):
    try:
        d = props[prop_name]["date"]
        if not d:
            return ""
        return (d.get("start") or "").strip()
    except (KeyError, TypeError):
        return ""


def get_name_and_date(entry):
    props = entry.get("properties", {}) or {}
    name = _extract_title(props, NAME_PROP) or _extract_rich_text(props, NAME_PROP) or "<no name>"
    date = _extract_date(props, DATE_PROP) or "<no date>"
    return name, date


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
        res = requests.patch(url, headers=HEADERS, json=payload, timeout=HTTP_TIMEOUT_SECONDS)
    except requests.RequestException as e:
        logger.error("Update request error (page=%s): %s", first_page_id, e)
        return False

    if res.status_code == 429 or 500 <= res.status_code <= 599:
        # Itt is lehetne retry, de ritkán kell; ha szeretnéd, beletesszük.
        body = (res.text or "")[:500]
        logger.error("Update failed transient (page=%s) status=%s body=%s", first_page_id, res.status_code, body)
        return False

    if res.status_code != 200:
        body = (res.text or "")[:500]
        logger.error("Update failed (page=%s) status=%s body=%s", first_page_id, res.status_code, body)
        return False

    return True


def chunked(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def build_second_db_lookup_for_codes(codes):
    """
    SECOND_DB lookup csak a FIRST_DB-ben előforduló kódokra.
    Így nem kell a teljes SECOND_DB-t letölteni (ami nálad 504-hez vezet).
    """
    lookup = {}
    codes = [c for c in codes if c]

    if not codes:
        return lookup, True

    for group in chunked(codes, SECOND_DB_CODES_CHUNK):
        # OR filter: bármelyik Projektkód egyezzen
        filter_payload = {
            "or": [
                {"property": "Projektkód", "rich_text": {"equals": code}}
                for code in group
            ]
        }
        results, ok = query_database(SECOND_DB_ID, filter_payload=filter_payload)
        if not ok:
            return {}, False

        for item in results:
            try:
                code = item["properties"]["Projektkód"]["rich_text"][0]["plain_text"].strip()
                if code:
                    lookup.setdefault(code, []).append(item["id"])
            except (KeyError, IndexError, TypeError):
                continue

    return lookup, True


def main():
    if not NOTION_TOKEN:
        logger.error("NOTION_TOKEN env var is missing. Exiting.")
        return

    logger.info("Kapcsolatok frissítése indul...")

    # 1) FIRST_DB először (ebből tudjuk, milyen kódokra kell keresni)
    first_entries, ok1 = query_database(FIRST_DB_ID)
    if not ok1 and FAIL_FAST_ON_DB_ERROR:
        logger.error("Első DB lekérdezés sikertelen -> fail fast.")
        return
    logger.info("Első adatbázis sorainak száma: %d", len(first_entries))

    # Kódok kigyűjtése
    codes_needed = set()
    bad_code_peek = 0
    for entry in first_entries:
        try:
            title_property = entry["properties"]["PROJEKTKÓD"]["title"]
            code = (title_property[0]["plain_text"] or "").strip()
            if code:
                codes_needed.add(code)
        except (KeyError, IndexError, TypeError):
            bad_code_peek += 1

    logger.info("Egyedi PROJEKTKÓD-ok száma az első DB-ben: %d (hibás kód előfordulás: %d)", len(codes_needed), bad_code_peek)

    # 2) SECOND_DB lookup csak ezekre a kódokra (sokkal könnyebb query)
    second_lookup, ok2 = build_second_db_lookup_for_codes(sorted(codes_needed))
    if not ok2 and FAIL_FAST_ON_DB_ERROR:
        logger.error("Második DB lekérdezés sikertelen -> fail fast.")
        return
    logger.info("Második DB lookup kulcsok száma: %d", len(second_lookup))

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
            code = (title_property[0]["plain_text"] or "").strip()
        except (KeyError, IndexError, TypeError):
            bad_code += 1
            name, date = get_name_and_date(entry)
            logger.warning('Hibás vagy hiányzó projektkód (Name="%s", Date="%s")', name, date)
            logger.debug("Page id (debug): %s", first_id)
            continue

        ids = second_lookup.get(code)
        if not ids:
            no_match += 1
        else:
            ids_to_link = sorted(ids)
            current_ids = sorted(get_current_relations(entry))

            if ids_to_link != current_ids:
                if update_relation(first_id, ids_to_link):
                    updated += 1
                else:
                    update_failed += 1
            else:
                unchanged += 1

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
