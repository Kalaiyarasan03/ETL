import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, String, text
from sqlalchemy.types import Text, Float, BigInteger
import pymysql
import psycopg2
import sys, ssl
from datetime import date, datetime, timedelta
import calendar
from requests.auth import HTTPBasicAuth
import requests
import re,os,json
#from urllib.parse import quote_plus
from io import StringIO
import urllib.parse
import subprocess
import time
from collections import Counter

# --- CONFIGURATION ---
CLIENT_ID = "1004.PGG55LMS082IOZCW02NIVYBBO2D98F"
CLIENT_SECRET = "9322429488907d25cd820c514765033b1754800560"  # 🔁 Replace with actual secret
TOKEN_FILE = "zoho_token.json"
DEVICE_CODE_URL = "https://accounts.zoho.in/oauth/v3/device/code"
TOKEN_URL = "https://accounts.zoho.in/oauth/v3/device/token"
REFRESH_TOKEN_URL = "https://accounts.zoho.in/oauth/v2/token"
SCOPE = "ZakyaAPI.FullAccess.all"

def main():
    if len(sys.argv) != 2:
        print("❌ Please provide exactly one argument (1–99).")
        sys.exit(1)

    try:
        param = int(sys.argv[1])
    except ValueError:
        print("❌ Argument must be an integer.")
        sys.exit(1)

    if 1 <= param <= 98:
        print(f"✅ Parameter {param} received. Proceeding with main logic...")
        # Place your logic for 1–8 here
        # Example:
        # run_etl_process(param)
    elif param == 99:
        print("🔁 Triggering etl_load_core.py with argument 99...")
        subprocess.run(["python", "etl_load_core3.py", "9"], check=True)
    else:
        print("❌ Invalid parameter. Allowed values are 1–9.")
        sys.exit(1)


def read_from_sap_odata(view_path, cred):
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if 'opu/odata/sap' in view_path.lower():
        full_url = cred['host'].rstrip('/') + '/' + view_path.lstrip('/')
    else:
        full_url = cred['host'].rstrip('/') + "/opu/odata/sap/" + view_path.strip('/')

    username = cred['username']
    password = cred['password']

    try:
        response = requests.get(
            full_url,
            auth=HTTPBasicAuth(username, password),
            headers={"Accept": "application/json"},
            verify=False
        )
        response.raise_for_status()
        data = response.json()

        #print("Top-level keys in OData response:", list(data.keys()))

        results = data.get('d', {}).get('results')
        if not isinstance(results, list):
            raise ValueError("Expected a list of records in 'd.results', got: " + str(type(results)))

        cleaned_records = [{k: v for k, v in record.items() if k != '__metadata'} for record in results]

        return pd.DataFrame(cleaned_records)

    except Exception as e:
        raise RuntimeError(f"Failed to fetch CDS data from {full_url}: {e}")
    

def infer_table_name_from_sap_odata_path(view_path):
    """
    Extracts the CDS service name from a typical OData CDS URL path.
    Example: '/sap/opu/odata/sap/ZMM_PROCUREMENT_SRV/mchSet?$format=json'
    will return 'ZMM_PROCUREMENT_SRV'
    """
    print ("view_path",view_path)
    match = re.search(r"/sap/opu/odata/sap/([^/]+)/", view_path, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    else:
        return "sap_odata_table"
# --- Save token to file ---
def save_tokens(data):
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f)

# --- Load token from file ---
def load_tokens():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    return {}

# --- Step 1: Get Device Code ---
def get_device_code():
    params = {
        "scope": SCOPE,
        "client_id": CLIENT_ID,
        "grant_type": "device_request",
        "access_type": "offline",
        "prompt": "consent"
    }
    response = requests.post(DEVICE_CODE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"🔗 Visit this link and authorize access:\n{data['verification_uri_complete']}\n")
    return data["device_code"]

# --- Step 2: Poll for Access Token using device_code ---
def poll_for_token(device_code):
    for _ in range(60):  # Poll for up to 5 minutes
        payload = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "device_token",
            "code": device_code
        }
        response = requests.post(TOKEN_URL, data=payload)
        data = response.json()
        if 'access_token' in data:
            print("✅ Access token received.")
            save_tokens(data)
            return data['access_token']
        elif data.get("error") == "authorization_pending":
            print("⏳ Waiting for user authorization...", end="\r")
            time.sleep(5)
        else:
            raise Exception(f"❌ Token polling failed: {data}")
    raise TimeoutError("⏱️ Authorization timed out.")

# --- Step 3: Refresh Access Token using saved refresh_token ---
def refresh_access_token(refresh_token):
    payload = {
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    response = requests.post(REFRESH_TOKEN_URL, data=payload)
    response.raise_for_status()
    data = response.json()
    print("🔁 Access token refreshed.")
    data['refresh_token'] = refresh_token  # preserve it if not returned
    save_tokens(data)
    return data['access_token']

# --- Main logic ---
def get_access_token():
    tokens = load_tokens()
    if 'refresh_token' in tokens:
        try:
            return refresh_access_token(tokens['refresh_token'])
        except Exception as e:
            print(f"⚠️ Refresh token failed: {e}. Trying manual device flow...")
    device_code = get_device_code()
    return poll_for_token(device_code)
    

def read_from_zakya_api(entity, from_date, to_date, org_id, access_token):

    
    url = (
        f"https://api.zakya.in/inventory/v1/export"
        f"?entity={entity}&accept=json&status=all&async_export=false"
        f"&can_export_pii_fields=false"
        f"&from_date={from_date}&to_date={to_date}"
        f"&includebatchdetails=true&includeserialnumbers=false"
        f"&organization_id={org_id}"
    )

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Accept": "application/json"
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    if 'application/json' in resp.headers.get('Content-Type', ''):
        print("✅ JSON response received.")
        data = resp.json()
        records = data.get("data", [])
        if not isinstance(records, list):
            raise ValueError("Expected list in Zakya API 'data' field.")
        return pd.DataFrame(records)
        #df = pd.DataFrame(records)
    else:
        print("🔍 Non-JSON response received (likely CSV): handling as text...")
        df = pd.read_csv(StringIO(resp.text))
        df.columns = [clean_column(col) for col in df.columns]
        df.columns = make_unique_columns(df.columns)
        duplicates = df.columns[df.columns.duplicated()].tolist()
        if duplicates:
            raise ValueError(f"❌ Duplicate columns found after cleaning: {duplicates}")
        return df
        #return pd.read_csv(StringIO(resp.text))

def optimize_column_types_for_mysql(df, varchar_threshold=100):
    """
    Converts wide string columns to TEXT to avoid MySQL row size errors.
    """
    dtype_map = {}
    for col in df.columns:
        if df[col].dtype == object:
            max_len = df[col].astype(str).map(len).max()
            if max_len and max_len > varchar_threshold:
                dtype_map[col] = Text()
            else:
                dtype_map[col] = String(length=min(max_len + 10, varchar_threshold))
    return dtype_map

def adjust_mysql_column_types(df, threshold=255):
    """
    Returns a dictionary mapping column names to SQLAlchemy types.
    Converts long string columns to TEXT, others to appropriate types.
    """
    dtype_map = {}
    for col in df.columns:
        dtype = df[col].dtype
        if dtype == object:
            max_len = df[col].astype(str).map(len).max()
            if max_len and max_len > threshold:
                dtype_map[col] = Text()
            else:
                dtype_map[col] = String(length=threshold)
        elif dtype in ['float64', 'float32']:
            dtype_map[col] = Float(precision=53)
        elif dtype in ['int64', 'int32']:
            dtype_map[col] = BigInteger()
        else:
            dtype_map[col] = String(length=threshold)  # fallback
    return dtype_map


def clean_column(col):
    # Remove anything after a colon with quotes (e.g., :"value")
    col = re.sub(r':".*?"', '', col)
    # Remove trailing dot and numbers (e.g., .379, .34)
    col = re.sub(r'\.\d+$', '', col)
    # Replace problematic characters with underscores
    col = re.sub(r'[^\w\s]', '_', col)
    return col.strip()

def force_text_columns_for_mysql(df):
    dtype_map = {}
    for col in df.columns:
        if df[col].dtype == object:
            dtype_map[col] = Text()
        elif df[col].dtype == 'float64':
            dtype_map[col] = Float()
        elif df[col].dtype == 'int64':
            dtype_map[col] = BigInteger()
    return dtype_map

def make_unique_columns(columns):
    counts = Counter()
    unique_cols = []
    for col in columns:
        counts[col] += 1
        if counts[col] > 1:
            unique_cols.append(f"{col}_{counts[col]}")
        else:
            unique_cols.append(col)
    return unique_cols

    
def calculate_next_execution_date(current_date, frequency):
    freq = frequency.upper()
    if freq == 'DAILY':
        return current_date + timedelta(days=1)
    elif freq == 'WEEKLY':
        return current_date + timedelta(weeks=1)
    elif freq == 'MONTHLY':
        next_month = current_date.month % 12 + 1
        year = current_date.year + (current_date.month // 12)
        day = min(current_date.day, calendar.monthrange(year, next_month)[1])
        return date(year, next_month, day)
    elif freq == 'QRTRLY':
        next_month = current_date.month + 3
        year = current_date.year + (next_month - 1) // 12
        month = (next_month - 1) % 12 + 1
        day = min(current_date.day, calendar.monthrange(year, month)[1])
        return date(year, month, day)
    elif freq == 'YEARLY':
        return date(current_date.year + 1, current_date.month, current_date.day)
    else:
        return current_date + timedelta(days=1)  # Default fallback

def mark_as_loaded(metadata_engine, srctbl_id, rec_read_count, rec_load_count, ref_frequency, recon_status, load_start_tm, load_end_tm):
    today = date.today()
    next_exec = calculate_next_execution_date(today, ref_frequency)

    query = text("""
        INSERT INTO execution_track (EXECUTION_DT, SRCTBL_ID, COMPLETE_TRACK,REC_READ_COUNT, REC_LOAD_COUNT, RECON_STATUS, LAST_EXEC_DT, NEXT_EXEC_DT, LOAD_START_TM, LOAD_END_TM)
        VALUES (:today, :srctbl_id, 'Y', :rec_read_count, :rec_load_count, :recon_status, :today, :next_exec, :load_start_tm, :load_end_tm)
        ON DUPLICATE KEY UPDATE 
            COMPLETE_TRACK = 'Y',
            REC_READ_COUNT = :rec_read_count,
            REC_LOAD_COUNT = :rec_load_count,
            RECON_STATUS = :recon_status,
            LAST_EXEC_DT = :today,
            NEXT_EXEC_DT = :next_exec,
            LOAD_START_TM = :load_start_tm,
            LOAD_END_TM = :load_end_tm
    """)
    with metadata_engine.begin() as conn:
        conn.execute(query, {
            "today": today,
            "srctbl_id": srctbl_id,
            "rec_read_count": rec_read_count,
            "rec_load_count": rec_load_count,
            "recon_status": recon_status,
            "next_exec": next_exec,
            "load_start_tm": load_start_tm,
            "load_end_tm": load_end_tm
        })


def get_db_engine(db_info):

    #ssl_args = {
    #    'ssl': {
    #        'ca': r'E:\Programs\ssl\fullchain.pem',
    #        'cert_reqs': ssl.CERT_NONE, 
    #        'check_hostname': False
    #    }
    #}

    db_type = db_info['db_type'].lower()
    if db_type == 'postgre':
        conn_str = f"postgresql+psycopg2://{db_info['username']}:{db_info['password']}@{db_info['host']}/{db_info['database']}"
    elif db_type == 'mysql':
        conn_str = f"mysql+pymysql://{db_info['username']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}"
    #elif db_type == 'mysql_wh':
    elif db_type == 'aws-mysql':
        conn_str = f"mysql+pymysql://{db_info['username']}:{db_info['password']}@{db_info['host']}/{db_info['database']}"
    elif db_type == 'aws_vms':
        conn_str = f"mysql+pymysql://{db_info['username']}:{db_info['password']}@{db_info['host']}/{db_info['database']}"
    elif db_type == 'mysql-ptp':
        conn_str = f"mysql+pymysql://{db_info['username']}:{db_info['password']}@{db_info['host']}/{db_info['database']}"
    elif db_type == 'zakya-api':
        conn_str = f"mysql+pymysql://{db_info['username']}:{db_info['password']}@{db_info['host']}/{db_info['database']}"
    elif db_type == 'sap-direct':
        conn_str = f"hana+hdbcli://{db_info['username']}:{db_info['password']}@{db_info['host']}:{db_info.get('port', 30015)}"
    elif db_type == 'sap-prod':
        conn_str = f"hana+hdbcli://{db_info['username']}:{db_info['password']}@{db_info['host']}:{db_info.get('port', 30041)}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    if db_type == 'mysql-ptp':
        #return create_engine(conn_str,connect_args=ssl_args)
        return create_engine(conn_str)
    else:
        return create_engine(conn_str)


def fetch_credentials(metadata_engine, db_type):
    query = f"""
        SELECT * FROM database_cred
        WHERE LOWER(db_type) = LOWER('{db_type}')
        LIMIT 1
    """
    df = pd.read_sql(query, metadata_engine)
    if df.empty:
        raise ValueError(f"No credentials found for db_type: {db_type}")
    return df.iloc[0].to_dict()


def is_already_loaded(metadata_engine, srctbl_id):
    today = date.today().isoformat()
    query = text("""
        SELECT NEXT_EXEC_DT 
        FROM execution_track
        WHERE SRCTBL_ID = :srctbl_id 
          AND NEXT_EXEC_DT > :today
        LIMIT 1
    """)
    with metadata_engine.connect() as conn:
        result = conn.execute(query, {"today": today, "srctbl_id": srctbl_id}).fetchone()
    if result:
        return True, result[0]
    else:
        return False, None




def main(datasrc_id):
    metadata_conn_str =  "mysql+pymysql://root:Mani414+++@localhost:3306/test"
    metadata_engine = create_engine(metadata_conn_str)

    query = f"SELECT * FROM srctbl_info WHERE DATASRC_ID = {datasrc_id} AND (ACTIVE_IND IS NULL OR UPPER(ACTIVE_IND) = 'Y')"
    srctbl_info_df = pd.read_sql(query, metadata_engine)

    if srctbl_info_df.empty:
        print(f"No tables found for DATASRC_ID = {datasrc_id}")
        return

    for _, row in srctbl_info_df.iterrows():
        srctbl_id = row['SRCTBL_ID']
        skip, next_exec_dt = is_already_loaded(metadata_engine, srctbl_id)
        if skip:
            print(f"⏭️  Skipping SRCTBL_ID {srctbl_id} (next load scheduled for {next_exec_dt})")
            continue
        src_db_type = row['SRC_DATABASE']
        tgt_db_type = row['TGT_DATABASE']
        src_schema = row['SRC_SCHEMA']
        tgt_schema = row['TGT_SCHEMA']
        src_table = row['SRC_TABLENAME']
        tgt_table = row['TGT_TABLENAME']

        try:
            load_start_tm = datetime.now()
            src_cred = fetch_credentials(metadata_engine, src_db_type)
            tgt_cred = fetch_credentials(metadata_engine, tgt_db_type)

            if src_db_type.lower() != 'sap-odata':
                src_engine = get_db_engine(src_cred)
            tgt_engine = get_db_engine(tgt_cred)

            if tgt_schema.upper() == 'INFER_SRC':
                tgt_schema = src_schema.strip()
            if tgt_table.upper() == 'INFER_SRC':
                tgt_table = src_table.strip()
                if src_db_type.lower() == 'sap-odata':
                    tgt_table = infer_table_name_from_sap_odata_path(src_table)
                    print("Target Tablename:",tgt_table)
            
            # Handle TRG_TABLE_SFX = 'YYYYMMDD'
            table_suffix = row.get('TRG_TABLE_SFX')
            if table_suffix and table_suffix.upper() == 'YYYYMMDD':
                today_suffix = date.today().strftime('%Y%m%d')
                tgt_table = f"{tgt_table}_{today_suffix}"
                

            if src_db_type.lower() == 'sap-odata':
                df = read_from_sap_odata(src_table, src_cred)
            if src_db_type.lower() == 'zakya-api':
                #access_token = load_zakya_access_token(json_file_path)
                access_token = get_access_token()
                print(f"\n🔐 Your access token:\n{access_token}")

                entity = src_table.strip()
                to_date = date.today()
                from_date = to_date - timedelta(days=1)
                # Format as YYYY-MM-DD
                to_date_str = to_date.strftime('%Y-%m-%d')
                from_date_str = from_date.strftime('%Y-%m-%d')
                today_str = date.today().strftime('%Y-%m-%d')
                org_id = 60035853406

                df = read_from_zakya_api(entity, from_date_str, from_date_str, org_id, access_token)
            else:
                select_query = row.get("SELECT_QUERY")
                if pd.notna(select_query) and select_query.strip():
                    #print(f"📥 Using custom SELECT_QUERY for {src_table}")
                    df = pd.read_sql(text(select_query), src_engine)
                else:
                    #print("📥 No custom query found. Using default SELECT *")
                    src_query = f"SELECT * FROM {src_schema}.{src_table}"
                    df = pd.read_sql(src_query, src_engine)

            if src_db_type.lower() == 'sap-direct':
                dtype_map = force_text_columns_for_mysql(df)
                df.to_sql(name=tgt_table, con=tgt_engine, schema=tgt_schema, if_exists='replace', index=False, dtype=dtype_map)
            else:
                # Handle timedelta columns before writing to SQL
                for col in df.select_dtypes(include='timedelta64[ns]').columns:
                    df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)
                df.to_sql(name=tgt_table, con=tgt_engine, schema=tgt_schema, if_exists='replace', index=False)

            if src_db_type.lower() == 'sap-odata':
                print(f"✅ Loaded {len(df)} rows from {src_table} to {tgt_schema}.{tgt_table}")
            else:
                print(f"✅ Loaded {len(df)} rows from {src_schema}.{src_table} to {tgt_schema}.{tgt_table}")

            #mark_as_loaded(metadata_engine, srctbl_id)
            #rec_count = len(df)
            #ref_freq = row.get("REF_FRQNCY", "DAILY")
            #mark_as_loaded(metadata_engine, srctbl_id, rec_count, ref_freq)

            rec_read_count = len(df)
            rec_load_count = rec_read_count  # assume successful load equals read

            recon_req = row.get("RECON_REQ", 0)
            if recon_req == 1:
                recon_status = "MATCH" if rec_read_count == rec_load_count else "FAILED"
            else:
                recon_status = "NA"

            ref_freq = row.get("REF_FRQNCY", "DAILY")
            load_end_tm = datetime.now()

            mark_as_loaded(metadata_engine,srctbl_id,rec_read_count,rec_load_count,ref_freq,recon_status,load_start_tm, load_end_tm)

        except Exception as e:
            print(f"❌ Error processing {src_schema}.{src_table}: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python etl_load.py <DATASRC_ID>")
        sys.exit(1)

    try:
        datasrc_id = int(sys.argv[1])
    except ValueError:
        print("DATASRC_ID must be an integer.")
        sys.exit(1)

    main(datasrc_id)


   