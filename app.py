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
import threading

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


def refresh_access_token()-> str:
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
def One_Drive_Auth() -> str:
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
    reference = purchaseorder.get("reference_number"),
    supplier_company = purchaseorder.get("vendor_name")
    line_items = purchaseorder.get("line_items", [])
    print(pd.DataFrame([
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
        ]))
    return pd.DataFrame([
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

def get_used_range(sheet_name="მიმდინარე "):
    """Get the used range of a worksheet"""
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/worksheets/{sheet_name}/usedRange"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}
    resp = requests.get(url, headers=headers, params={"valuesOnly": "false"})
    resp.raise_for_status()
    return resp.json()["address"]  # e.g. "მიმდინარე !A1:Y20"

def create_table_if_not_exists(range_address, has_headers=True, retries=3):
    """Create a new table if none exist yet, retry if workbook is busy"""
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    existing_tables = resp.json().get("value", [])
    if existing_tables:
        return existing_tables[0]["name"]  # reuse first table

    # Retry creating table
    url_add = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/add"
    headers["Content-Type"] = "application/json"
    payload = {"address": range_address, "hasHeaders": has_headers}

    for attempt in range(retries):
        resp = requests.post(url_add, headers=headers, json=payload)
        if resp.status_code in [200, 201]:
            table = resp.json()
            print(f"✅ Created table '{table['name']}' at range {range_address}")
            return table["name"]
        else:
            print(f"⚠️ Table creation failed ({resp.status_code}), retrying...")
            time.sleep(2)

    raise Exception(f"❌ Failed to create table after {retries} retries: {resp.status_code} {resp.text}")

def get_table_columns(table_name):
    """Fetch column names of an existing Excel table"""
    url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{FILE_ID}/workbook/tables/{table_name}/columns"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN_DRIVE}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return [col["name"] for col in resp.json().get("value", [])]

def append_dataframe_to_table(df: pd.DataFrame, sheet_name="მიმდინარე "):
    """Normalize and append a Pandas DataFrame to an Excel table using Graph API"""
    if df.empty:
        raise ValueError("❌ DataFrame is empty. Nothing to append.")
    # Ensure table exists
    range_address = get_used_range(sheet_name)
    table_name = create_table_if_not_exists(range_address)
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
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code in [200, 201]:
        print(f"✅ Successfully appended {len(rows)} rows to table '{table_name}'")
        return resp.json()
    else:
        print("❌ Error response content (truncated):", resp.text[:500])
        raise Exception(f"❌ Failed to append rows: {resp.status_code} {resp.text[:200]}")


def update_excel(new_df: pd.DataFrame) -> None:
    """
    Update Excel file with new data. 
    Automatically detects if it's a sales order or purchase order based on columns.
    If it's a purchase order (has Reference column), matches with existing sales orders.
    Numbering (#) restarts from 1 for every new batch of rows added.
    """
    try:
        max_wait_minutes = 10  # allow long waits since this is background
        deadline = time.time() + max_wait_minutes * 60
        while time.time() < deadline:
            try:
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
                break
            except Exception as e:
                print(f"⚠️ Error downloading file: {e}, retrying...")
        else:
            print("❌ Gave up downloading file after 10 minutes")
            return
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
                range_address = get_used_range("მიმდინარე ")
                table_name = create_table_if_not_exists(range_address)
                print(f"✅ Upload successful. Created table named {table_name}")
                return
            else:
                raise RuntimeError("❌ Failed to upload: file remained locked after max retries.")
        else:
            append_dataframe_to_table(new_df)
    except Exception as e:
        print(f"❌ Fatal error: {e}")



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
        append_dataframe_to_table(get_sales_order_df(order_id))
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
        threading.Thread(target=update_excel, args=(get_purchase_order_df(order_id),), daemon=True).start()
        return "OK", 200
    except Exception as e:
        
        return f"Processing error: {e}", 500
@app.route("/health")
def health():
    return {'health':'ok'}
