import os, requests, hmac, hashlib, io, random, time, threading, gc, base64, re, pdfplumber
from flask import Flask, request, jsonify, make_response
import pandas as pd
from dotenv import load_dotenv
from openpyxl import load_workbook
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pytz import timezone
from pathlib import Path
load_dotenv()

# single session (reuse connections)
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "supply-tracker/1.0", "Content-Type": "application/x-www-form-urlencoded"})
retry_strategy = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"]
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
HTTP.mount("https://", adapter)
HTTP.mount("http://", adapter)
# thread pool to avoid unbounded thread creation
POOL = ThreadPoolExecutor(max_workers=4)  # tune 2-4 on free tier
# single lock to avoid concurrent workbook uploads
EXCEL_LOCK = threading.Lock()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
ORG_ID = os.getenv('ORG_ID')
TENANT_ID = os.getenv('TENANT_ID')
CLIENT_ID_DRIVE = os.getenv('CLIENT_ID_DRIVE')
CLIENT_SECRET_DRIVE = os.getenv('CLIENT_SECRET_DRIVE')
DRIVE_ID = os.getenv('DRIVE_ID')
FILE_ID = os.getenv('FILE_ID')
PERMS_ID = os.getenv('PERMS_ID')
HACH_FILE = os.getenv('HACH_FILE')
ACCESS_TOKEN_DRIVE = None
ACCESS_TOKEN_EXPIRY = datetime.utcnow()
ACCESS_TOKEN = None
DOC_TYPES = ["salesorders","purchaseorders"]

MAILBOXES = [
    "info@vortex.ge",
    "archil@vortex.ge",
    "Logistics@vortex.ge",
    "hach@vortex.ge"
]
WEBHOOK_URL = "https://supply-tracker-o7ro.onrender.com/webhook"
GRAPH_URL = "https://graph.microsoft.com/v1.0"

app = Flask(__name__)
# ----------- AUTH -----------
def refresh_access_token()-> str:
    global ACCESS_TOKEN
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    resp = HTTP.post(url, params=params).json()
    ACCESS_TOKEN = resp["access_token"]
    return ACCESS_TOKEN
def verify_zoho_signature(request, expected_module):
    # Select secret based on webhook type
    secret_key = (
    os.getenv("SALES_WEBHOOK_SECRET") if expected_module == "salesorders"
    else os.getenv("PURCHASE_WEBHOOK_SECRET") if expected_module == "purchaseorders"
    else os.getenv("SHIPMENT_WEBHOOK_SECRET")
    ).encode('utf-8')
    
    received_sign = request.headers.get('X-Zoho-Webhook-Signature')
    if not received_sign or not secret_key:
        return False
    
    expected_sign = hmac.new(
        secret_key,
        request.get_data(),
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(received_sign, expected_sign)
def One_Drive_Auth() -> str:
    global ACCESS_TOKEN_DRIVE, ACCESS_TOKEN_EXPIRY
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID_DRIVE,
        "client_secret": CLIENT_SECRET_DRIVE,
        "scope": "https://graph.microsoft.com/.default"
    }
    try:
        resp = HTTP.post(url, data=data, timeout=30)
        resp.raise_for_status()
        response_json = resp.json()
        
        ACCESS_TOKEN_DRIVE = response_json.get("access_token")
        expires_in = response_json.get("expires_in", 3600)
        ACCESS_TOKEN_EXPIRY = datetime.utcnow() + timedelta(seconds=expires_in - 60)  # refresh 1 min early
        
        if ACCESS_TOKEN_DRIVE:
            return ACCESS_TOKEN_DRIVE
        else:
            print("No access_token in response!")
            return None
    except Exception as e:
        print(f"Error getting access token: {e}")
        return None

def get_headers():
    global ACCESS_TOKEN_DRIVE, ACCESS_TOKEN_EXPIRY
    if (ACCESS_TOKEN_DRIVE is None) or (ACCESS_TOKEN_EXPIRY <= datetime.utcnow()):
        One_Drive_Auth()  # refresh token + expiry
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}",
        "Content-Type": "application/json"
    }
# ----------- GET DF -----------
def get_sales_order_df(order_id: str) -> pd.DataFrame:
    url = f"https://www.zohoapis.com/inventory/v1/salesorders/{order_id}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN or refresh_access_token()}",
        "X-com-zoho-inventory-organizationid": ORG_ID
    }

    response = HTTP.get(url, headers=headers)
    response.raise_for_status()
    salesorder = response.json().get("salesorder", {})

    customer_name = salesorder.get("customer_name")
    order_number = salesorder.get("salesorder_number")
    date = salesorder.get("date")
    line_items = salesorder.get("line_items", [])

    enriched_items = []
    for item in line_items:
        item_id = item.get("item_id")
        manufacturer = None

        # Lookup item details from Items API
        if item_id:
            item_url = f"https://www.zohoapis.com/inventory/v1/items/{item_id}"
            item_resp = HTTP.get(item_url, headers=headers)
            if item_resp.status_code == 200:
                item_details = item_resp.json().get("item", {})
                manufacturer = (
                    item_details.get("manufacturer")
                    or item_details.get("cf_manufacturer")
                )

        enriched_items.append({
            "SO": order_number,
            "Customer": customer_name,
            "Item": item.get("name"),
            "Code": item.get("sku"),
            "Supplier Company": manufacturer,
            "შეკვეთილი რაოდენობა": item.get("quantity"),
            "შეკვეთის გაკეთების თარიღი": date
        })
    return pd.DataFrame(enriched_items)
def get_purchase_order_df(order_id: str) -> pd.DataFrame:
    url = f"https://www.zohoapis.com/inventory/v1/purchaseorders/{order_id}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN or refresh_access_token()}",
        "X-com-zoho-inventory-organizationid": ORG_ID
    }

    response = HTTP.get(url, headers=headers)
    response.raise_for_status()
    purchaseorder = response.json().get("purchaseorder", {})
    po_number = purchaseorder.get("purchaseorder_number")
    date = purchaseorder.get("date")
    reference = purchaseorder.get("reference_number")
    if reference:
        reference = reference.strip("()").strip().rstrip(",")
    supplier_company = purchaseorder.get("vendor_name")
    line_items = purchaseorder.get("line_items", [])
    df = pd.DataFrame([
            {
                "Supplier Company": supplier_company,
                "PO": po_number,
                "შეკვეთის გაკეთების თარიღი": date,
                "Item": item.get("name"),
                "Code": item.get("sku"),
                "Reference": reference,
                "შეკვეთილი რაოდენობა": item.get("quantity"),
                "Customer": next((field.get("value_formatted") for field in item.get("item_custom_fields", []) 
                                if field.get("label") == "Customer"), None)
            }
            for item in line_items
        ])
    if supplier_company == "HACH":
        process_hach(df)
        return None
    return df
# ----------- HELPER FUNCS FOR EXCEL -----------
def get_used_range(sheet_name: str):
    """Get the used range of a worksheet"""
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/worksheets/{sheet_name}/usedRange"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}
    resp = HTTP.get(url, headers=headers, params={"valuesOnly": "false"})
    resp.raise_for_status()
    return resp.json()["address"]  # e.g. "მიმდინარე !A1:Y20"
def create_table_if_not_exists(range_address, sheet_name, has_headers=True, retries=3):
    """Return existing table on the specific sheet, or create a new one."""
    
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}

    # ✅ 1. Query ONLY tables from the specified sheet
    url_sheet_tables = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}"
        f"/workbook/worksheets/{sheet_name}/tables"
    )

    resp = HTTP.get(url_sheet_tables, headers=headers)
    resp.raise_for_status()
    sheet_tables = resp.json().get("value", [])

    # If any table exists on sheet → reuse first table
    if sheet_tables:
        return sheet_tables[0]["name"]

    # --- Create a new table ---
    url_add = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/add"
    )
    headers["Content-Type"] = "application/json"
    payload = {"address": range_address, "hasHeaders": has_headers}

    for attempt in range(retries):
        resp = HTTP.post(url_add, headers=headers, json=payload)
        if resp.status_code in [200, 201]:
            table = resp.json()
            print(f"✅ Created table '{table['name']}' at {range_address}")
            return table["name"]
        else:
            print(f"⚠️ Table creation failed ({resp.status_code}), retrying...")
            time.sleep(2)

    raise Exception(
        f"❌ Failed to create table after {retries} retries: "
        f"{resp.status_code} {resp.text}"
    )

def get_table_columns(table_name):
    """Fetch column names of an existing Excel table"""
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/{table_name}/columns"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}
    resp = HTTP.get(url, headers=headers)
    resp.raise_for_status()
    return [col["name"] for col in resp.json().get("value", [])]
def delete_table_rows(sheet_name: str, row_numbers: list[int]):
    """
    Delete worksheet rows using Graph API, works even for tables.
    """
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}",
        "Content-Type": "application/json"
    }

    for row in sorted(row_numbers, reverse=True):
        address = f"{row}:{row}"  # delete whole row
        url = (
            f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}"
            f"/workbook/worksheets/{sheet_name}/range(address='{address}')/delete"
        )

        resp = HTTP.post(url, headers=headers, json={"shift": "up"})
        if resp.status_code not in (200, 204):
            print(f"⚠️ Failed to delete row {row}: {resp.text}")
        else:
            print(f"🗑️ Deleted worksheet row {row}")
def get_table_start_row_from_used_range(sheet_name: str) -> int:
    used_addr = get_used_range(sheet_name)
    # Example: "მიმდინარე !A1:Y300"
    start_cell = used_addr.split("!")[1].split(":")[0]  # "A1"
    start_row = int(re.findall(r"\d+", start_cell)[0])
    return start_row

# ----------- MAIN LOGIC -----------
def append_dataframe_to_table(df: pd.DataFrame, sheet_name: str):
    """Normalize and append a Pandas DataFrame to an Excel table using Graph API"""
    df = df[df['Supplier Company'] != 'HACH']
    if df.empty:
        raise ValueError("❌ DataFrame is empty. Nothing to append.")
    # Ensure table exists
    range_address = get_used_range(sheet_name)
    table_name = create_table_if_not_exists(range_address, sheet_name)
    # Handle Customer/Reference substitution
    if "Customer" in df.columns and "Reference" in df.columns:
        df = df.copy()
        for index, row in df.iterrows():
            customer_val = row['Customer']
            if (customer_val is None or 
                (isinstance(customer_val, str) and customer_val.strip() == "") or 
                (pd.isna(customer_val))):
                df.at[index, 'Customer'] = row['Reference']

        # ✅ Drop Reference column after substitution
        df = df.drop(columns=["Reference"])
    # Fetch table columns
    table_columns = get_table_columns(table_name)

    # Normalize DataFrame
    new_df = df.copy()
    for col in table_columns:
        if col not in new_df.columns:
            new_df[col] = ""
    new_df['#'] = range(1, len(new_df) + 1)

    # ✅ Restrict to table columns only
    out_df = new_df[table_columns]

    # Convert DataFrame → list of lists
    rows = out_df.fillna("").astype(str).values.tolist()

    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/{table_name}/rows/add"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}", "Content-Type": "application/json"}
    payload = {"values": rows}
    resp = HTTP.post(url, headers=headers, json=payload)
    if resp.status_code in [200, 201]:
        print(f"✅ Successfully appended {len(rows)} rows to table '{table_name}'")
        return resp.json()
    else:
        print("❌ Error response content (truncated):", resp.text[:500])
        raise Exception(f"❌ Failed to append rows: {resp.status_code} {resp.text[:200]}")

def get_sheet_values(sheet_name: str):
    """Get actual usedRange values (including header row)."""
    url = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/"
        f"{FILE_ID}/workbook/worksheets/{sheet_name}/usedRange?$select=values"
    )
    
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}
    
    resp = HTTP.get(url, headers=headers)
    resp.raise_for_status()

    result = resp.json()
    return result.get("values", [])  # this is the list of rows
def update_excel(new_df: pd.DataFrame) -> None:
    """
    Update Excel file with new data.
    Automatically detects if it's a sales order or purchase order based on columns.
    If it's a purchase order (has Reference column), matches with existing sales orders.
    Numbering (#) restarts from 1 for every new batch of rows added.
    """
    with EXCEL_LOCK:
        file_stream = None
        wb = None
        existing_df = pd.DataFrame()

        try:
            # --- Step 1: Download current file from OneDrive ---
            url_download = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/content"
            headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE or One_Drive_Auth()}"}

            max_attempts = 6
            for attempt in range(max_attempts):
                try:
                    resp = HTTP.get(url_download, headers=headers, timeout=60)
                    resp.raise_for_status()
                    file_stream = io.BytesIO(resp.content)
                    wb = load_workbook(file_stream)

                    if "მიმდინარე " in wb.sheetnames:
                        ws = wb["მიმდინარე "]
                        existing_df = pd.DataFrame(ws.values)
                        existing_df.columns = existing_df.iloc[0]  # first row as header
                        existing_df = existing_df[1:]              # drop header row
                    else:
                        existing_df = pd.DataFrame()
                    break
                except Exception as e:
                    wait = min(5 * (attempt + 1), 30)
                    print(f"⚠️ Error downloading file (attempt {attempt+1}/{max_attempts}): {e}. Sleeping {wait}s")
                    time.sleep(wait)
            else:
                print("❌ Gave up downloading file after attempts")
                return

            # --- Step 2: Check if it's a purchase order ---
            if new_df.get("Reference") is not None and new_df["Reference"].apply(
                lambda x: any(r.strip() in set(existing_df.get("SO", [])) for r in str(x).split(",")) if pd.notna(x) else False
            ).any():
                # Purchase order logic...
                purch_ref_to_rows = {}
                for idx, row in new_df.iterrows():
                    ref = row.get("Reference")
                    if pd.notna(ref):
                        refs = [r.strip() for r in re.split(r"[;,]", str(ref)) if r.strip()]
                        for r in refs:
                            purch_ref_to_rows.setdefault(r, []).append(idx)

                updated_count = 0
                for sales_idx, sales_row in existing_df.iterrows():
                    so_value = sales_row.get("SO")
                    sales_item = sales_row.get("Item")

                    if pd.notna(so_value) and so_value in purch_ref_to_rows:
                        for purch_idx in purch_ref_to_rows[so_value]:
                            purch_item = new_df.at[purch_idx, "Item"]

                            items_match = (
                                (pd.isna(sales_item) and pd.notna(purch_item))
                                or (pd.isna(purch_item) and pd.notna(sales_item))
                                or (
                                    pd.notna(sales_item)
                                    and pd.notna(purch_item)
                                    and str(sales_item).strip().lower() == str(purch_item).strip().lower()
                                )
                            )

                            if items_match:
                                for col in new_df.columns:
                                    if col in existing_df.columns and col not in ["SO", "#"]:
                                        sales_value = existing_df.at[sales_idx, col]
                                        purch_value = new_df.at[purch_idx, col]
                                        if col in ["შეკვეთის გაკეთების თარიღი", "Customer", "შეკვეთილი რაოდენობა"]:
                                            if pd.notna(purch_value):
                                                existing_df.at[sales_idx, col] = purch_value
                                                updated_count += 1
                                        else:
                                            if (pd.isna(sales_value) or sales_value == "") and pd.notna(purch_value):
                                                existing_df.at[sales_idx, col] = purch_value
                                                updated_count += 1

                # --- Step 3: Replace only the 'მიმდინარე ' sheet ---
                ws = wb["მიმდინარე "]

                # Write headers if needed
                for col_idx, col_name in enumerate(existing_df.columns.tolist(), start=1):
                    ws.cell(row=1, column=col_idx).value = col_name

                # Write data values
                for row_idx, row in enumerate(existing_df.values.tolist(), start=2):
                    for col_idx, value in enumerate(row, start=1):
                        ws.cell(row=row_idx, column=col_idx).value = value

                # --- Step 4: Save workbook to memory ---
                output = io.BytesIO()
                wb.save(output)
                output.seek(0)

                # --- Step 5: Upload back with retry if locked ---
                url_upload = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/content"
                max_attempts = 10
                for attempt in range(max_attempts):
                    resp = HTTP.put(url_upload, headers=headers, data=output.getvalue())

                    if resp.status_code in (423, 409):  # Locked
                        wait_time = min(30, 2**attempt) + random.uniform(0, 2)
                        print(f"⚠️ File locked (attempt {attempt+1}/{max_attempts}), retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue

                    resp.raise_for_status()
                    range_address = get_sheet_values("მიმდინარე ")
                    table_name = create_table_if_not_exists(range_address,"მიმდინარე ")
                    print(f"✅ Upload successful. Created table named {table_name}")
                    return
                else:
                    raise RuntimeError("❌ Failed to upload: file remained locked after max retries.")
            else:
                append_dataframe_to_table(new_df, "მიმდინარე ")

        except Exception as e:
            print(f"❌ Fatal error: {e}")

        finally:
            if wb:
                try:
                    wb.close()
                except Exception:
                    pass
            if file_stream:
                try:
                    file_stream.close()
                except Exception:
                    pass
            del existing_df
            del new_df
            gc.collect()

# ----------- MONDAY CHECKING -----------
def fetch_recent_orders() -> list[dict]:
    base_url = "https://www.zohoapis.com/inventory/v1"
    result = []
    for doc_type in DOC_TYPES:
        url = f"{base_url}/{doc_type}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN or refresh_access_token()}",
            "X-com-zoho-inventory-organizationid": ORG_ID
        }
        today = datetime.utcnow().date()
        from_date = (today - timedelta(days=2)).isoformat()  # 3 days ago
        to_date = today.isoformat()
        params = {
        "date_start": from_date,
        "date_end": to_date
        }
        response = HTTP.get(url, headers=headers, params=params)
        response.raise_for_status()
        orders = response.json().get(doc_type, [])
        for order in orders:
            order_id = order.get("purchaseorder_id") if doc_type == "purchaseorders" else order.get("salesorder_id")
            order_number = order.get("purchaseorder_number") if doc_type == "purchaseorders" else order.get("salesorder_number")
            order_status = order.get("status").lower()
            if order_status == 'open' or order_status == 'approved' or order_status == 'issued':
                result.append({
                    "order_id": order_id,
                    "order_number": order_number,
                    "type": "purchaseorder" if doc_type == "purchaseorders" else "salesorder"
                })
    return result
def monday_job():
    """
    Run with apscheduler: 
    If today is Monday, fetch & process Saturday orders.
    """
    now = datetime.now()
    if now.weekday() == 0: # Monday only
        print("🔄 Checking for Saturday-created orders...")
        orders = fetch_recent_orders()
        if orders:
            # --- Step 1: Download current file from OneDrive ---
            url_download = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/content"
            headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE or One_Drive_Auth()}"}
            resp = HTTP.get(url_download, headers=headers)
            resp.raise_for_status()
            file_stream = io.BytesIO(resp.content)
            wb = load_workbook(file_stream)
            if "მიმდინარე " in wb.sheetnames:
                ws = wb["მიმდინარე "]
                existing_df = pd.DataFrame(ws.values)
                existing_df.columns = existing_df.iloc[0]  # first row as header
                existing_df = existing_df[1:]              # drop header row
            else:
                existing_df = pd.DataFrame()
            for order in orders:
                if order['type'] == "salesorder":
                    if "SO" in existing_df.columns:
                        existing_df = existing_df[existing_df["SO"] != order["order_number"]]
                else:  # purchase order
                    if "PO" in existing_df.columns:
                        existing_df = existing_df[existing_df["PO"] != order["order_number"]]
            # --- Step 4: Replace only the 'მიმდინარე ' sheet ---
            ws = wb["მიმდინარე "]

            # Write headers if needed
            for col_idx, col_name in enumerate(existing_df.columns.tolist(), start=1):
                ws.cell(row=1, column=col_idx).value = col_name

            # Write data values
            for row_idx, row in enumerate(existing_df.values.tolist(), start=2):
                for col_idx, value in enumerate(row, start=1):
                    ws.cell(row=row_idx, column=col_idx).value = value

            # --- Step 5: Save workbook to memory ---
            output = io.BytesIO()
            wb.save(output)
            output.seek(0)

            # --- Step 6: Upload back with retry if locked ---
            url_upload = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/content"

            max_attempts = 10  # up to ~5 minutes wait
            for attempt in range(max_attempts):
                resp = HTTP.put(url_upload, headers=headers, data=output.getvalue())

                if resp.status_code in (423, 409):  # Locked
                    wait_time = min(30, 2 ** attempt) + random.uniform(0, 2)
                    print(f"⚠️ File locked (attempt {attempt+1}/{max_attempts}), retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue

                resp.raise_for_status()
                range_address = get_used_range("მიმდინარე ")
                table_name = create_table_if_not_exists(range_address, "მიმდინარე ")
                print(f"✅ Cleaned table {table_name}")
            for order in orders:
                if order['type'] == "salesorder":
                    One_Drive_Auth()
                    append_dataframe_to_table(get_sales_order_df(order['order_id']), "მიმდინარე ")
                else:
                    One_Drive_Auth()
                    PO_df = get_purchase_order_df(order['order_id'])
                    PO_df_copy = PO_df.copy()   # avoid referencing outer objects
                    PO_df = None
                    PO_future = POOL.submit(update_excel, PO_df_copy)
        else:
            print("ℹ️ No new Saturday orders found, skipping cleanup.")


def process_shipment(order_number: str) -> None:
        try:
            # --- Load sheet values ---
            data = get_sheet_values("მიმდინარე ")
            if not data or not isinstance(data, list) or len(data) < 2:
                print("⚠️ No data or insufficient rows in source sheet")
                return

            # Ensure proper row formatting
            data = [list(row) for row in data]

            # Build DataFrame safely
            df_source = pd.DataFrame(data[1:], columns=data[0])

            # --- Filter matching rows ---
            order_number = str(order_number).strip()
            matching = df_source[df_source["SO"].astype(str).str.strip() == order_number].copy()


            if matching.empty:
                print(f"⚠️ No rows found for SO = {order_number}")
                return

            matching.loc[:, "ადგილმდებარეობა"] = "ჩაბარდა"
            # --- Append only (no deletion) ---
            append_dataframe_to_table(matching, "ჩამოსული")

            start_row = get_table_start_row_from_used_range("მიმდინარე ")
            table_row_indices = matching.index.tolist()
            worksheet_rows = [start_row + 1 + idx for idx in table_row_indices]


            # --- DELETE FROM THE TABLE ---
            delete_table_rows("მიმდინარე ", worksheet_rows)

            print(f"✅ Completed processing for SO {order_number}")

        except Exception as e:
            print(f"❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
def normalize_hach(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe for HACH workflow with proper column handling."""
    try:
        # Define expected columns in order
        table_cols = [
            "Item", "წერილი", "Code", "HS Code", "Details", "თარგმანი", "QTY",
            "მიწოდების ვადა", "Confirmation 1 (shipment week)", "Packing List",
            "რა რიცხვში გამოგზავნეს Packing List-ი", "რამდენი გამოიგზავნა",
            "ჩამოსვლის სავარაუდო თარიღი", "რეალური ჩამოსვლის თარიღი",
            "Qty Delivered", "Customer", "Export?", "მდებარეობა", "შენიშვნა"
        ]
        
        # Create working copy and handle missing source columns
        df_working = df.copy()
        
        # Check if required source columns exist
        required_cols = ['Item', 'Code', 'შეკვეთილი რაოდენობა', 'Customer']
        missing_cols = [col for col in required_cols if col not in df_working.columns]
        if missing_cols:
            print(f"⚠️ Warning: Missing source columns: {missing_cols}")
        
        # Select and rename available columns
        available_cols = [col for col in required_cols if col in df_working.columns]
        df_working = df_working[available_cols].copy()
        
        # Rename available columns (only those that exist)
        rename_map = {}
        if 'Item' in df_working.columns:
            rename_map['Item'] = 'Details'
        if 'შეკვეთილი რაოდენობა' in df_working.columns:
            rename_map['შეკვეთილი რაოდენობა'] = 'QTY'
            
        if rename_map:
            df_working = df_working.rename(columns=rename_map)
        
        # Add Item numbers (handle empty dataframe case)
        df_working["Item"] = range(1, len(df_working) + 1)
        
        # Create all expected columns with empty values
        for col in table_cols:
            if col not in df_working.columns:
                df_working[col] = ""
        
        # Ensure correct column order and handle missing values
        result_df = df_working[table_cols].fillna("")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Error in normalize_hach: {str(e)}")
        raise

def process_hach(df: pd.DataFrame) -> None:
    po_full = df["PO"].iloc[0]
    po_number = po_full.replace("PO-00", "")
    sheet_name = po_number
    print(f"\n📌 Creating sheet '{sheet_name}' for HACH workflow...")

    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}

    # Simple retry function for API calls
    def api_call_with_retry(method, url, json_data=None, max_retries=3):
        for attempt in range(max_retries):
            try:
                if method == "POST":
                    response = HTTP.post(url, headers=headers, json=json_data)
                elif method == "PATCH":
                    response = HTTP.patch(url, headers=headers, json=json_data)
                else:
                    response = HTTP.request(method, url, headers=headers, json=json_data)
                
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0.1, 0.5)
                    print(f"⚠️ API call failed, retrying in {wait_time:.1f}s... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    raise e

    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{HACH_FILE}/workbook/worksheets/add"
    response = api_call_with_retry("POST", url, {"name": sheet_name})
    
    info_data = [
        ["PO", po_number],
        ["SO", df["Reference"].iloc[0]],
        ["POს გაკეთების თარიღი", df["შეკვეთის გაკეთების თარიღი"].iloc[0]],
        ["დღვანდელი თარიღი", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")]
    ]

    info_range = "C3:D6"
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{HACH_FILE}/workbook/worksheets/{sheet_name}/range(address='{info_range}')"
    response = api_call_with_retry("PATCH", url, {"values": info_data})

    edges = ["EdgeTop", "EdgeBottom", "EdgeLeft", "EdgeRight"]
    for edge in edges:
        borders_url = (
            f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{HACH_FILE}"
            f"/workbook/worksheets/{sheet_name}/range(address='{info_range}')/format/borders/{edge}"
        )
        border_payload = {
            "style": "Continuous",
            "color": {"rgb": "000000"}  # black border
        }
        api_call_with_retry("PATCH", borders_url, border_payload)

    start_row = 8
    table_headers = [
        "Item", "წერილი", "Code", "HS Code", "Details", "თარგმანი", "QTY",
        "მიწოდების ვადა", "Confirmation 1 (shipment week)", "Packing List",
        "რა რიცხვში გამოგზავნეს Packing List-ი", "რამდენი გამოიგზავნა", "ჩამოსვლის სავარაუდო თარიღი", "რეალური ჩამოსვლის თარიღი",
        "Qty Delivered", "Customer", "Export?", "მდებარეობა", "შენიშვნა"
    ]

    table_rows = [[""] * len(table_headers)]
    full_table = [table_headers] + table_rows
    col_end = "T"  # 19 columns → ends at column T
    write_range = f"B{start_row}:{col_end}{start_row + len(full_table) - 1}"

    # Write values
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{HACH_FILE}/workbook/worksheets/{sheet_name}/range(address='{write_range}')"
    response = api_call_with_retry("PATCH", url, {"values": full_table})

    # Create table
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{HACH_FILE}/workbook/tables/add"
    payload = {
        "address": f"{sheet_name}!{write_range}",
        "hasHeaders": True
    }
    response = api_call_with_retry("POST", url, payload)
    table_id = response.json()["id"]
    
    normalized_df = normalize_hach(df)
    rows_to_append = normalized_df.values.tolist()

    # add rows into table with retry
    rows_url = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{HACH_FILE}"
        f"/workbook/tables/{table_id}/rows/add"
    )

    response = api_call_with_retry("POST", rows_url, {"values": rows_to_append})
    print("\n✅ HACH workflow completed successfully.")




@app.route("/")
def index():
    return "App is running. Scheduler is active."

# ----------- SALES ORDER WEBHOOK -----------
@app.route("/zoho/webhook/sales", methods=["POST"])
def sales_webhook():
    One_Drive_Auth()
    # Check one - signaure
    if not verify_zoho_signature(request, "salesorders"):
        return "Invalid signature", 403
    order_id = request.json.get("data", {}).get("salesorder_id")
    # Check two - order_id
    if not order_id:
        return "Missing order ID", 400

    try:
        append_dataframe_to_table(get_sales_order_df(order_id), "მიმდინარე ")
        return "OK", 200
    except Exception as e:
        return f"Processing error: {e}", 500

# ----------- PURCHASE ORDER WEBHOOK -----------
@app.route("/zoho/webhook/purchase", methods=["POST"])
def purchase_webhook():
    One_Drive_Auth()
    if not verify_zoho_signature(request, "purchaseorders"):
        return "Invalid signature", 403
    order_id = request.json.get("data", {}).get("purchaseorders_id")

    if not order_id:
        return "Missing order ID", 400

    try:
        PO_df = get_purchase_order_df(order_id)
        PO_df_copy = PO_df.copy()   # avoid referencing outer objects
        PO_df = None
        PO_future = POOL.submit(update_excel, PO_df_copy)
        return "OK", 200
    except Exception as e:
        
        return f"Processing error: {e}", 500

# ----------- PACKAGE ORDER WEBHOOK -----------
@app.route('/zoho/webhook/delivered', methods=['POST'])
def handle_delivery():
    One_Drive_Auth()
    if not verify_zoho_signature(request, "shipmentorders"):
            return "Invalid signature", 403
    order_num = request.json.get("data", {}).get("sales_order_number")

    if not order_num:
        return "Missing order ID", 400

    try:
        POOL.submit(process_shipment, order_num)
        return "OK", 200
    except Exception as e:
        
        return f"Processing error: {e}", 500

# -----------MAIL WEBHOOK -----------
def safe_request(method, url, **kwargs):
    """Wrapper to apply a default timeout and route through our retrying session."""
    timeout = kwargs.pop("timeout", 30)
    func = getattr(HTTP, method.lower())
    return func(url, timeout=timeout, **kwargs)
def process_message(mailbox, message_id, message_date):
    print(f"Mailbox: {mailbox}")
    print(f"message_id: {message_id}")
    print(f"message_date: {message_date}")
    with EXCEL_LOCK:
        file_stream = None
        wb = None
        orders_df = pd.DataFrame()
        perms_df = pd.DataFrame()

        # --- Step 1: Download current orders Excel file ---
        url_download = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/content"
        perms_download = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{PERMS_ID}/content"
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE or One_Drive_Auth()}"}

        max_attempts = 6
        for attempt in range(max_attempts):
            try:
                # --- Download orders file ---
                resp = HTTP.get(url_download, headers=headers, timeout=60)
                resp.raise_for_status()
                file_stream = io.BytesIO(resp.content)
                wb = load_workbook(file_stream)

                if "მიმდინარე " in wb.sheetnames:
                    ws = wb["მიმდინარე "]
                    orders_df = pd.DataFrame(ws.values)
                    orders_df.columns = orders_df.iloc[0]  # first row as header
                    orders_df = orders_df[1:]              # drop header row
                else:
                    print("⚠️ Worksheet 'მიმდინარე ' not found in orders file.")
                    orders_df = pd.DataFrame()

                # --- Download permissions Excel file ---
                try:
                    resp_perms = HTTP.get(perms_download, headers=headers, timeout=60)
                    resp_perms.raise_for_status()
                    perms_stream = io.BytesIO(resp_perms.content)
                    perms_df = pd.read_excel(perms_stream, header=1)
                    perms_stream.close()
                    perms_stream = None
                    if not {"მწარმოებლის კოდი", "მიღებული ნებართვა 1 / წერილის ნომერი"}.issubset(perms_df.columns):
                        print("⚠️ Warning: Permissions file missing required columns.")
                    else:
                        perms_df = perms_df[["მწარმოებლის კოდი", "მიღებული ნებართვა 1 / წერილის ნომერი"]]

                except Exception as e_perm:
                    print(f"⚠️ Could not download permissions Excel: {e_perm}")
                    perms_df = pd.DataFrame(columns=["მწარმოებლის კოდი", "მიღებული ნებართვა 1 / წერილის ნომერი"])

                break  # success — exit retry loop

            except Exception as e:
                wait = min(5 * (attempt + 1), 30)
                print(f"⚠️ Error downloading main file (attempt {attempt+1}/{max_attempts}): {e}. Sleeping {wait}s")
                time.sleep(wait)

        else:
            print("❌ Gave up downloading files after multiple attempts")
            return

        items_df = pd.read_csv("zoho_items.csv")
        att_url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}/attachments"
        att_resp = HTTP.get(att_url, headers=get_headers(), timeout=20)
        if att_resp.status_code != 200:
            print(f"❌ Error fetching attachments: {att_resp.status_code} - {att_resp.text}")
            return

        attachments = att_resp.json().get("value", [])

        # ✅ Check for exactly one PDF
        pdf_attachments = [
            att for att in attachments
            if att.get("name", "").lower().endswith(".pdf") or att.get("contentType") == "application/pdf"
        ]

        if len(pdf_attachments) != 1:
            print(f"❌ Expected 1 PDF attachment, found {len(pdf_attachments)} - skipping message")
            return

        att = pdf_attachments[0]
        if "contentBytes" not in att:
            print("❌ Attachment has no contentBytes - skipping")
            return
        # --- 2. Loop over attachments, decode and extract text directly ---
        all_text = ""
        for att in attachments:
            if 'contentBytes' in att and att['name'].lower().endswith('.pdf'):
                content = base64.b64decode(att['contentBytes'])
                with pdfplumber.open(io.BytesIO(content)) as pdf:
                    for page in pdf.pages:
                        all_text += (page.extract_text() or "") + "\n"

        # --- 3. Extract PO number (first occurrence) ---
        po_match = re.search(r"PO-\d+", all_text)
        po_number = po_match.group(0) if po_match else None

        if po_number:
            print(f"🎯 Found PO number: {po_number}")

            matching_idx = orders_df.index[orders_df["PO"] == po_number]

            updated_rows = 0

            for idx in matching_idx:
                code = str(orders_df.at[idx, "Code"]).strip()
                print(f"\n🔍 Processing code: '{code}'")

                # Check if this code appears in the PDF text
                if code and code in all_text:
                    print(f"✅ Match found for code {code} in PDF")

                    # Fill HS Code if missing
                    hs_row = items_df[items_df["sku"] == code]
                    hs_code = hs_row["HS_Code"].iloc[0] if not hs_row.empty else None

                    if pd.isna(orders_df.at[idx, "HS Code"]) or orders_df.at[idx, "HS Code"] == "":
                        orders_df.at[idx, "HS Code"] = hs_code
                        print("   Filled HS code")

                    # Fill confirmation date
                    if pd.isna(orders_df.at[idx, "Confirmation-ის მოსვლის თარიღი"]) or orders_df.at[idx, "Confirmation-ის მოსვლის თარიღი"] == "":
                        orders_df.at[idx, "Confirmation-ის მოსვლის თარიღი"] = message_date
                        print("   Filled confirmation date")

                    # --- Filling წერილი ---
                    print(f"   Searching permissions for code: '{code}'")
                    perm_row = perms_df[perms_df["მწარმოებლის კოდი"].astype(str).str.strip() == code]
                    
                    if not perm_row.empty:
                        num_perm = perm_row["მიღებული ნებართვა 1 / წერილის ნომერი"].iloc[0]
                        print(f"   📋 Permission number found: {num_perm}")
                        orders_df.at[idx, "წერილი"] = num_perm
                        updated_rows += 1
                        print(f"   ✅ SUCCESS: Filled წერილი with {num_perm}")
                    else:
                        orders_df.at[idx, "წერილი"] = "არ სჭირდება"

            if updated_rows == 0:
                print("⚠️ No matching item codes found in this confirmation message.")

        # 🟢 after loop, update sheet once:
        ws = wb["მიმდინარე "]

        # Write headers if needed
        for col_idx, col_name in enumerate(orders_df.columns.tolist(), start=1):
            ws.cell(row=1, column=col_idx).value = col_name

        # Write data values
        for row_idx, row in enumerate(orders_df.values.tolist(), start=2):
            for col_idx, value in enumerate(row, start=1):
                ws.cell(row=row_idx, column=col_idx).value = value

        # Save workbook to memory
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        # Upload back
        url_upload = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/content"
        max_attempts = 10
        for attempt in range(max_attempts):
            resp = HTTP.put(url_upload, headers=headers, data=output.getvalue())
            if resp.status_code in (423, 409):  # Locked
                wait_time = min(30, 2**attempt) + random.uniform(0, 2)
                print(f"⚠️ File locked (attempt {attempt+1}/{max_attempts}), retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            resp.raise_for_status()
            range_address = get_used_range("მიმდინარე ")
            table_name = create_table_if_not_exists(range_address, "მიმდინარე ")
            print(f"✅ Upload successful. Created table named {table_name}")
            file_stream.close()
            file_stream = wb = None
            del orders_df
            gc.collect()
            return
def clear_all_subscriptions():
    headers = get_headers()
    subs_url = f"{GRAPH_URL}/subscriptions"
    resp = safe_request("get", subs_url, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to list subscriptions: {resp.text}")

    subs = resp.json().get("value", [])
    print(f"Found {len(subs)} existing subscriptions")
    for sub in subs:
        sub_id = sub["id"]
        del_url = f"{GRAPH_URL}/subscriptions/{sub_id}"
        dresp = safe_request("delete", del_url, headers=headers)
        if dresp.status_code not in (202, 204):
            print(f"Could not delete {sub_id}: {dresp.text}")
        else:
            print(f"Deleted subscription {sub_id}")
def create_subscription_for_user(mailbox):
    """
    Create a subscription. Do NOT block waiting for Graph validation; Graph will call your webhook GET with validationToken.
    Keep the function thread-safe and idempotent-friendly.
    """
    expiration_time = (datetime.utcnow() + timedelta(minutes=4230)).isoformat() + "Z"
    data = {
        "changeType": "created",
        "notificationUrl": WEBHOOK_URL,
        "resource": f"users/{mailbox}/messages",
        "expirationDateTime": expiration_time,
        "clientState": "secretClientValue"
    }

    print(f"Creating subscription for {mailbox}...")
    try:
        response = safe_request("post", f"{GRAPH_URL}/subscriptions", headers=get_headers(), json=data, timeout=30)
    except Exception as e:
        print(f"❌ Network error creating subscription for {mailbox}: {e}")
        return None

    if response.status_code in (200, 201):
        sub_info = response.json()
        print(f"✅ Created subscription for {mailbox}: {sub_info.get('id')}")
        return sub_info
    elif response.status_code == 202:
        # Accepted. Graph may be validating the endpoint. Return whatever Graph sent.
        print(f"⏳ Subscription for {mailbox} accepted (202). Graph is validating the notification URL.")
        try:
            return response.json()
        except Exception:
            return {}
    else:
        print(f"❌ Failed to create subscription for {mailbox}: {response.status_code} {response.text}")
        return None
def initialize_subscriptions():
    """
    Must be called inside an application context if get_headers/One_Drive_Auth need it.
    """
    print("[initialize_subscriptions] Setting up subscriptions...")
    clear_all_subscriptions()
    futures = []
    for mailbox in MAILBOXES:
        futures.append(POOL.submit(create_subscription_for_user, mailbox))

    successful_subs = []
    for future in as_completed(futures):
        try:
            result = future.result()
            if result:
                # resource might be in format "users/{mailbox}/messages" - handle robustly
                resource = result.get('resource', '')
                parts = resource.split('/')
                mailbox_name = parts[1] if len(parts) > 1 else None
                successful_subs.append((mailbox_name or "unknown", result.get('id')))
        except Exception as e:
            print(f"❌ Error creating subscription: {e}")

    print(f"\n✅ Successfully created {len(successful_subs)}/{len(MAILBOXES)} subscriptions")
    return successful_subs
def with_app_ctx_call(fn, *args, **kwargs):
    """Helper: call fn within app context (for background tasks)"""
    with app.app_context():
        return fn(*args, **kwargs)
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # Validation: Graph sends GET with validationToken param
    validation_token = request.args.get("validationToken")
    if validation_token:
        print(f"Validation request received: {validation_token}")
        resp = make_response(validation_token, 200)
        resp.mimetype = "text/plain"
        return resp

    if request.method == "POST":
        try:
            data = request.json or {}
            notifications = data.get('value', [])
            #print(f"Webhook triggered with {len(notifications)} notifications")

            # Pattern to match in email subjects (case-insensitive)
            pattern = re.compile(r'(?i)(?:purchase order\s+)?PO-\d+\b(?![^\n]*\bhas been (?:partially\s*)?received\b)')

            for notification in notifications:
                resource = notification.get('resource', '')
                # fetch the message to get subject & id
                message_url = f"{GRAPH_URL}/{resource}"
                message_response = safe_request("get", message_url, headers=get_headers(), timeout=20)
                if message_response.status_code != 200:
                    print(f"Error fetching message: {message_response.status_code} - {message_response.text}")
                    continue
                message = message_response.json()
                subject = message.get('subject', '')
                if not re.search(r'(?i)\b(received|shipped)\b', subject):
                    if pattern.search(subject):
                        print(f"✅ Pattern matched in {subject!r} - scheduling processing")
                        # parse mailbox robustly
                        path_parts = resource.split('/')
                        mailbox = None
                        try:
                            # resource typically: users/{user-id}/messages/{message-id}
                            if len(path_parts) >= 4 and path_parts[0].lower() in ("users", "me"):
                                mailbox = path_parts[1]
                            else:
                                mailbox = "unknown"
                        except Exception:
                            mailbox = "unknown"
                            print(f"Warning: Unexpected resource format: {resource}")

                        message_id = message.get('id')
                        message_date = message.get('receivedDateTime')
                        # schedule heavy processing in thread pool
                        POOL.submit(process_message, mailbox, message_id, message_date)
            # return quickly so Graph knows we accepted the notifications
            return jsonify({"status": "accepted"}), 202
        except Exception as e:
            print(f"❌ Error processing webhook: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500
    else:
        return jsonify({"status": "active"}), 200
def _initialize_subscriptions_worker(flask_app):
    """
    This wrapper ensures an app_context is present when initialize_subscriptions is run.
    Submit this wrapper to the ThreadPoolExecutor from a Flask request handler.
    """
    with flask_app.app_context():
        try:
            initialize_subscriptions()
        except Exception as e:
            print(f"❌ initialize_subscriptions_worker exception: {e}")

@app.route("/init", methods=["GET", "POST"])
def init_subscriptions_endpoint():
    """Manual endpoint to initialize subscriptions in background"""
    try:
        print("🔄 Starting subscription initialization in background...")
        # Submit worker that establishes app context itself.
        POOL.submit(_initialize_subscriptions_worker, app)
        return jsonify({
            "status": "success",
            "message": "Subscription initialization started in background"
        }), 200
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
@app.route("/subscriptions", methods=["GET"])
def list_subscriptions():
    try:
        resp = safe_request("get", f"{GRAPH_URL}/subscriptions", headers=get_headers(), timeout=20)
        if resp.status_code == 200:
            subs = resp.json().get("value", [])
            return jsonify({
                "status": "success",
                "count": len(subs),
                "subscriptions": subs
            }), 200
        else:
            return jsonify({"status": "error", "message": resp.text}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/cleanup", methods=["GET", "POST"])
def cleanup_subscriptions():
    try:
        print("🧹 Cleaning up subscriptions...")
        # Run cleanup in background to avoid blocking
        POOL.submit(with_app_ctx_call, clear_all_subscriptions)
        return jsonify({"status": "success", "message": "Subscription cleanup scheduled"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
# ----------- HEALTH CHECK -----------
@app.route("/health")
def health():
    return {'health':'ok'}

# ----------- SCHEDULERS -----------
scheduler = BackgroundScheduler()
scheduler.add_job(
    monday_job,
    "cron",
    day_of_week="mon",
    hour=10,
    minute=5,
    timezone=timezone("Asia/Tbilisi")  # UTC+4
)
scheduler.start()