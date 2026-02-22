import os
import time
import logging
import threading
import requests
import re
from io import BytesIO
from openpyxl import load_workbook

# ================= CONFIG =================

MS_TENANT_ID = os.getenv("TENANT_ID")
MS_CLIENT_ID = os.getenv("CLIENT_ID_DRIVE")
MS_CLIENT_SECRET = os.getenv("CLIENT_SECRET_DRIVE")

DRIVE_ID = os.getenv("DRIVE_ID")
ITEM_ID = os.getenv("ITEM_ID")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 60))

GRAPH_TOKEN_URL = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

ID_RE = re.compile(r"_(\d{4})$")
# ================= TOKEN =================

class TokenManager:
    def __init__(self):
        self.token = None
        self.expiry = 0
        self.lock = threading.Lock()

    def get_token(self):
        with self.lock:

            if self.token and time.time() < self.expiry - 300:
                return self.token

            logging.info("Refreshing Microsoft token...")

            resp = requests.post(
                GRAPH_TOKEN_URL,
                data={
                    "client_id": MS_CLIENT_ID,
                    "client_secret": MS_CLIENT_SECRET,
                    "grant_type": "client_credentials",
                    "scope": "https://graph.microsoft.com/.default",
                },
                timeout=30,
            )

            resp.raise_for_status()
            data = resp.json()

            self.token = data["access_token"]
            self.expiry = time.time() + int(data["expires_in"])

            return self.token


token_manager = TokenManager()


def graph_headers():
    return {"Authorization": f"Bearer {token_manager.get_token()}"}


# ================= RETRY =================

def request_with_retry(method, url, **kwargs):
    for attempt in range(5):
        try:
            resp = requests.request(method, url, timeout=30, **kwargs)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                logging.warning(f"Throttled. Sleeping {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                raise requests.RequestException(resp.text)

            resp.raise_for_status()
            return resp

        except Exception as e:
            wait = 2 ** attempt
            logging.warning(f"Request failed: {e}. Retry in {wait}s")
            time.sleep(wait)

    raise Exception("Max retries exceeded")


# ================= GRAPH =================

def get_last_modified():
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{ITEM_ID}"

    resp = request_with_retry("GET", url, headers=graph_headers())

    return resp.json()["lastModifiedDateTime"]


def download_excel():
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{ITEM_ID}/content"

    resp = request_with_retry("GET", url, headers=graph_headers())

    return resp.content
def get_file_metadata():
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{ITEM_ID}"

    resp = request_with_retry("GET", url, headers=graph_headers())
    data = resp.json()

    return data["lastModifiedDateTime"], data["eTag"]

def upload_excel(data, etag):
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{ITEM_ID}/content"

    headers = graph_headers()
    headers["If-Match"] = etag   # 🔥 THE MAGIC LINE

    resp = requests.put(url, headers=headers, data=data, timeout=30)

    if resp.status_code == 412:
        logging.warning("Upload rejected — file changed during processing. Skipping.")
        return False

    resp.raise_for_status()
    return True


# ================= EXCEL =================

def assign_ids(file_bytes):
    wb = load_workbook(BytesIO(file_bytes))
    global_ws = wb["__GLOBAL__"]

    used_ids = set()

    for ws in wb.worksheets:
        if ws.title == "__GLOBAL__":
            continue

        for row in ws.iter_rows(min_row=2):
            name = row[1].value
            if isinstance(name, str):
                m = ID_RE.search(name)
                if m:
                    used_ids.add(int(m.group(1)))

    last_id = max(used_ids) if used_ids else 0
    changed = False

    for ws in wb.worksheets:
        if ws.title == "__GLOBAL__":
            continue

        for row in ws.iter_rows(min_row=2):
            cell = row[1]
            name = cell.value

            if not isinstance(name, str) or not name.strip():
                continue

            if ID_RE.search(name):
                continue

            new_id = 1
            while new_id in used_ids:
                new_id += 1

            used_ids.add(new_id)
            cell.value = f"{name}_{new_id:04d}"
            changed = True
            last_id = max(last_id, new_id)

    if changed:
        global_ws["B1"].value = last_id

        out = BytesIO()
        wb.save(out)
        out.seek(0)

        return out.read(), last_id

    return None, last_id


# ================= WATCHER =================

def watcher_loop():
    logging.info("Watcher started.")

    try:
        last_seen, _ = get_file_metadata()
    except Exception:
        logging.exception("Initial metadata fetch failed.")
        last_seen = None

    while True:
        try:
            modified, etag = get_file_metadata()

            if modified != last_seen:
                logging.info("File changed. Processing...")
                file_bytes = download_excel()
                result, last_id = assign_ids(file_bytes)

                if result:
                    success = upload_excel(result, etag)
                    if success:
                        logging.info(f"IDs assigned. Last ID = {last_id}")
                        last_seen = modified
                else:
                    last_seen = modified

        except Exception:
            logging.exception("Watcher loop error")

        time.sleep(POLL_INTERVAL)
_watcher_thread = None

def start_watcher():
    global _watcher_thread

    if _watcher_thread and _watcher_thread.is_alive():
        logging.info("Watcher already running.")
        return

    _watcher_thread = threading.Thread(
        target=watcher_loop,
        daemon=True
    )
    _watcher_thread.start()

    logging.info("Background watcher started.")