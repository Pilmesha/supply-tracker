import os
import requests
from flask import Flask, request
import pandas as pd
from dotenv import load_dotenv
import hmac
import hashlib
import io
import random
import time
from openpyxl import load_workbook
from typing import List, Optional
from urllib.parse import quote

load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
ORG_ID = os.getenv('ORG_ID')
TENANT_ID = os.getenv('TENANT_ID')
CLIENT_ID_DRIVE = os.getenv('CLIENT_ID_DRIVE')
CLIENT_SECRET_DRIVE = os.getenv('CLIENT_SECRET_DRIVE')
DRIVE_ID = os.getenv('DRIVE_ID')
FILE_ID = os.getenv('FILE_ID')

ACCESS_TOKEN_DRIVE = None
ACCESS_TOKEN = None
app = Flask(__name__)


def refresh_access_token():
    global ACCESS_TOKEN
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    resp = requests.post(url, params=params).json()
    ACCESS_TOKEN = resp["access_token"]
    return ACCESS_TOKEN

def verify_zoho_signature(request, expected_module):
    # Select secret based on webhook type
    secret_key = (
        os.getenv('SALES_WEBHOOK_SECRET') 
        if expected_module == "salesorders" 
        else os.getenv('PURCHASE_WEBHOOK_SECRET')
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
def One_Drive_Auth():
    global ACCESS_TOKEN_DRIVE
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID_DRIVE,
        "client_secret": CLIENT_SECRET_DRIVE,
        "scope": "https://graph.microsoft.com/.default"
    }

    resp = requests.post(url, data=data)
    ACCESS_TOKEN_DRIVE = resp.json().get("access_token")
    return ACCESS_TOKEN_DRIVE


def get_sales_order_df(order_id: str) -> pd.DataFrame:
    url = f"https://www.zohoapis.com/inventory/v1/salesorders/{order_id}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN or refresh_access_token()}",
        "X-com-zoho-inventory-organizationid": ORG_ID
    }

    response = requests.get(url, headers=headers)
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
            item_resp = requests.get(item_url, headers=headers)
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

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    purchaseorder = response.json().get("purchaseorder", {})

    po_number = purchaseorder.get("purchaseorder_number")
    date = purchaseorder.get("date")
    reference = purchaseorder.get("reference_number")
    line_items = purchaseorder.get("line_items", [])
    return pd.DataFrame([
            {
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

# ------------ CONFIG ------------
RETRY_STATUS = {409, 423, 429, 500, 502, 503, 504}
DEFAULT_BATCH_SIZE = 200
DEFAULT_TIMEOUT = 60  # seconds per request
# --------------------------------

def _graph_headers(session_id: Optional[str] = None, extra: Optional[dict] = None) -> dict:
    # Remove ACCESS_TOKEN_DRIVE parameter, use global
    h = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}  # ACCESS_TOKEN_DRIVE should be global
    if session_id:
        h["workbook-session-id"] = session_id
    if extra:
        h.update(extra)
    return h

# Then call it correctly:


import logging
logging.basicConfig(level=logging.DEBUG)

def _req(method: str, url: str, headers: dict, **kwargs) -> requests.Response:
    timeout = kwargs.pop("timeout", DEFAULT_TIMEOUT)
    backoff = 1.0
    
    logging.debug(f"Request: {method} {url}")
    logging.debug(f"Headers: { {k: v for k, v in headers.items() if k != 'Authorization'} }")
    
    for attempt in range(6):
        try:
            resp = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)    
            # Log detailed info for 500 errors
            if resp.status_code >= 500:
                print(f"=== 500 ERROR DETAILS ===")
                print(f"URL: {url}")
                print(f"Method: {method}")
                print(f"Status: {resp.status_code}")
                print(f"Response: {resp.text[:500]}...")  # First 500 chars
                print("=========================")
            
            if resp.status_code not in RETRY_STATUS:
                return resp
            
            logging.debug(f"Attempt {attempt+1}: Status {resp.status_code}")
            
            if resp.status_code not in RETRY_STATUS:
                return resp
                
            # Log response body for 500 errors
            if resp.status_code >= 500:
                try:
                    error_body = resp.json()
                    logging.error(f"Server error response: {error_body}")
                except:
                    logging.error(f"Server error, cannot parse JSON: {resp.text[:200]}")
                
            # Handle rate limiting
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    sleep_s = float(ra)
                except ValueError:
                    sleep_s = backoff
            else:
                sleep_s = backoff
                
            # Log the retry (add logging)
            print(f"Attempt {attempt+1} failed with status {resp.status_code}. Retrying in {sleep_s}s")
            time.sleep(min(sleep_s, 30))  # Increased max sleep
            backoff = min(backoff * 2, 30)  # Increased max backoff
            
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_exception = e
            time.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 30)
            
    # If all retries failed, raise the last exception or return last response
    if last_exception:
        raise last_exception
    return resp

def start_workbook_session(persist: bool = True) -> str:
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/createSession"
    headers = _graph_headers(session_id, {"Content-Type": "application/json"})
    resp = _req("POST", url, headers, json={"persistChanges": persist})
    resp.raise_for_status()
    return resp.json()["id"]

def close_workbook_session(session_id: str) -> None:
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/closeSession"
    headers = _graph_headers(session_id, {"Content-Type": "application/json"})
    # best-effort close; ignore errors
    try:
        _req("POST", url, headers).raise_for_status()
    except Exception:
        pass

def get_worksheet_id_by_name(session_id: str, sheet_name: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/worksheets"
    headers = _graph_headers(session_id, {"Content-Type": "application/json"})
    resp = _req("GET", url, headers)
    resp.raise_for_status()
    for ws in resp.json().get("value", []):
        if ws.get("name") == sheet_name or ws.get("name", "").strip() == sheet_name.strip():
            return ws["id"]
    raise ValueError(f"Worksheet named '{sheet_name}' not found.")

def get_used_range_address(session_id: str, worksheet_id: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/worksheets/{worksheet_id}/usedRange"
    headers = _graph_headers(session_id)
    resp = _req("GET", url, headers, params={"valuesOnly": "false"})
    
    if resp.status_code != 200:
        logging.error(f"Used range failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
    
    address = resp.json()["address"]
    logging.debug(f"Used range address: {address}")
    return address

def list_tables(session_id: str) -> List[dict]:
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables"
    headers = _graph_headers(session_id, {"Content-Type": "application/json"})
    resp = _req("GET", url, headers)
    resp.raise_for_status()
    return resp.json().get("value", [])

def delete_table(session_id: str, table_name: str) -> None:
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/{quote(table_name)}"
    headers = _graph_headers(session_id, {"Content-Type": "application/json"})
    resp = _req("DELETE", url, headers)
    # 200/204 OK; if 404, ignore
    if resp.status_code not in (200, 204, 404):
        resp.raise_for_status()

def create_table(session_id: str, range_address: str, has_headers: bool = True) -> str:
    # Validate the range address format
    if not range_address or '!' not in range_address:
        raise ValueError(f"Invalid range address: {range_address}")
    
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/add"
    headers = _graph_headers(session_id, {"Content-Type": "application/json"})
    
    logging.debug(f"Creating table with range: {range_address}")
    
    resp = _req("POST", url, headers, json={"address": range_address, "hasHeaders": has_headers})
    
    if resp.status_code != 200:
        logging.error(f"Table creation failed: {resp.status_code} - {resp.text}")
    
    resp.raise_for_status()
    return resp.json()["name"]

def get_table_columns(session_id: str, table_name: str) -> List[str]:
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/{quote(table_name)}/columns"
    headers = _graph_headers(session_id, {"Content-Type": "application/json"})
    resp = _req("GET", url, headers)
    resp.raise_for_status()
    return [c["name"] for c in resp.json().get("value", [])]

def normalize_dataframe_to_columns(df: pd.DataFrame, table_columns: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in table_columns:
        if col not in out.columns:
            out[col] = ""
    # derive Customer from Reference if applicable
    if "Customer" in table_columns and "Reference" in out.columns and out["Customer"].isna().all():
        out["Customer"] = out["Reference"]
    # reset batch numbering if the table has a '#' column
    if "#" in table_columns:
        out["#"] = range(1, len(out) + 1)
    # drop extras & reorder
    return out[table_columns]

def append_rows_batch(session_id: str, table_name: str, rows: List[List[str]]) -> None:
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/{quote(table_name)}/rows/add"
    headers = _graph_headers(session_id, {"Content-Type": "application/json"})
    resp = _req("POST", url, headers, json={"values": rows})
    resp.raise_for_status()

def append_dataframe_to_table(
    df: pd.DataFrame,
    sheet_name: str = "მიმდინარე ",
    drop_and_recreate_table: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> str:
    """
    Reliable end-to-end append with corrected function calls
    """
    print("Starting debug process...")
    if df.empty:
        raise ValueError("DataFrame is empty. Nothing to append.")
    print("1. Creating session...")
    session_id = start_workbook_session(persist=True)
    print(f"   Session ID: {session_id}")
    try:
        print("2. Getting worksheet...")
        ws_id = get_worksheet_id_by_name(session_id, sheet_name)
        print(f"   Worksheet ID: {ws_id}")
        if drop_and_recreate_table:
            # Remove any stale tables
            for t in list_tables(session_id):
                delete_table(session_id, t["name"])
            # Let workbook settle
            time.sleep(2.0)  # Increased settle time
            # Create table over current used range
            used_addr = get_used_range_address(session_id, ws_id)
            table_name = create_table(session_id, used_addr, has_headers=True)
        else:
            # Reuse existing table or create one if none
            tables = list_tables(session_id)
            if tables:
                table_name = tables[0]["name"]
            else:
                used_addr = get_used_range_address(session_id, ws_id)
                table_name = create_table(session_id, used_addr, has_headers=True)

        # Normalize DF to the table's columns
        table_columns = get_table_columns(session_id, table_name)
        norm = normalize_dataframe_to_columns(df, table_columns)
        rows = norm.astype(str).fillna("").values.tolist()

        # Append in batches with small delays between batches
        for i in range(0, len(rows), batch_size):
            append_rows_batch(session_id, table_name, rows[i:i+batch_size])
            if i + batch_size < len(rows):  # Don't sleep after last batch
                time.sleep(0.5)  # Small delay between batches

        return table_name

    except Exception as e:
        # Log the error for debugging
        print(f"Error in append_dataframe_to_table: {str(e)}")
        raise
    finally:
        close_workbook_session(session_id)


def update_excel(new_df: pd.DataFrame) -> None:
    """
    Update Excel file with new data. 
    Automatically detects if it's a sales order or purchase order based on columns.
    If it's a purchase order (has Reference column), matches with existing sales orders.
    Numbering (#) restarts from 1 for every new batch of rows added.
    """
    # --- Step 1: Download current file from OneDrive ---
    url_download = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/content"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE or One_Drive_Auth()}"}
    resp = requests.get(url_download, headers=headers)
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
    # ---Check if it's a purchase order (has Reference column) ---
    if new_df["Reference"].apply(lambda x: any(r.strip() in set(existing_df["SO"]) for r in str(x).split(',')) if pd.notna(x) else False).any():
        # Create a mapping from Reference to rows in purchase data
        purch_ref_to_rows = {}
        for idx, row in new_df.iterrows():
            ref = row['Reference']
            if pd.notna(ref):
                refs = [r.strip() for r in str(ref).split(',') if r.strip()]
                for r in refs:
                    if r not in purch_ref_to_rows:
                        purch_ref_to_rows[r] = []
                    purch_ref_to_rows[r].append(idx)
        # Update existing sales orders with purchase data where references AND items match
        updated_count = 0
        for sales_idx, sales_row in existing_df.iterrows():
            so_value = sales_row['SO']
            sales_item = sales_row['Item']
            
            if pd.notna(so_value) and so_value in purch_ref_to_rows:
                # Find matching purchase order items for this SO
                for purch_idx in purch_ref_to_rows[so_value]:
                    purch_item = new_df.at[purch_idx, 'Item']
                    
                    # Check if items match (or if either is empty/NaN)
                    items_match = (
                        (pd.isna(sales_item) and pd.notna(purch_item)) or
                        (pd.isna(purch_item) and pd.notna(sales_item)) or
                        (pd.notna(sales_item) and pd.notna(purch_item) and 
                            str(sales_item).strip().lower() == str(purch_item).strip().lower())
                    )
                    
                    if items_match:                          
                        # Update all columns except SO and #
                        for col in new_df.columns:
                            if col in existing_df.columns and col not in ['SO', '#']:
                                sales_value = existing_df.at[sales_idx, col]
                                purch_value = new_df.at[purch_idx, col]
                                if col in ['შეკვეთის გაკეთების თარიღი', 'Customer', 'შეკვეთილი რაოდენობა']:
                                    # Always use purchase value if it exists
                                    if pd.notna(purch_value):
                                        existing_df.at[sales_idx, col] = purch_value
                                        updated_count += 1
                                else:
                                    # For other columns, update only if sales value is empty and purchase value exists
                                    if (pd.isna(sales_value) or sales_value == "") and pd.notna(purch_value):
                                        existing_df.at[sales_idx, col] = purch_value
                                        updated_count += 1
        # --- Step 4: Replace only the 'მიმდინარე ' sheet ---
        if "მიმდინარე " in wb.sheetnames:
            wb.remove(wb["მიმდინარე "])
        ws_new = wb.create_sheet("მიმდინარე ")

        for r in [existing_df.columns.tolist()] + existing_df.values.tolist():
            ws_new.append(list(r))

        # --- Step 5: Save workbook to memory ---
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        # --- Step 6: Upload back with retry if locked ---
        url_upload = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/content"

        max_attempts = 10  # up to ~5 minutes wait
        for attempt in range(max_attempts):
            resp = requests.put(url_upload, headers=headers, data=output.getvalue())

            if resp.status_code in (423, 409):  # Locked
                wait_time = min(30, 2 ** attempt) + random.uniform(0, 2)
                print(f"⚠️ File locked (attempt {attempt+1}/{max_attempts}), retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue

            resp.raise_for_status()
            print("✅ Upload successful.")
            break
        else:
            raise RuntimeError("❌ Failed to upload: file remained locked after max retries.")
    else:
        append_dataframe_to_table(new_df)



@app.route("/zoho/webhook/sales", methods=["POST"])
def sales_webhook():
    # Check one - signaure
    if not verify_zoho_signature(request, "salesorders"):
        return "Invalid signature", 403
    order_id = request.json.get("data", {}).get("salesorder_id")
    # Check two - order_id
    if not order_id:
        return "Missing order ID", 400

    try:
        append_dataframe_to_table(get_sales_order_df(order_id))
        return "OK", 200
    except Exception as e:
        return f"Processing error: {e}", 500


# ----------- PURCHASE ORDER WEBHOOK -----------
@app.route("/zoho/webhook/purchase", methods=["POST"])
def purchase_webhook():
    if not verify_zoho_signature(request, "purchaseorders"):
        return "Invalid signature", 403
    order_id = request.json.get("data", {}).get("purchaseorders_id")
    if not order_id:
        return "Missing order ID", 400

    try:
        update_excel(get_purchase_order_df(order_id))
        return "OK", 200
    except Exception as e:
        return f"Processing error: {e}", 500


# if __name__ == "__main__":
#     app.run(port=5000, debug=True)