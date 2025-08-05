import os
import pandas as pd
import boto3
import psycopg2
import time
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
import pytz

# Load environment variables
load_dotenv()

# --- AWS S3 Setup ---
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET_NAME = os.getenv("BUCKET_NAME", "bank-bucket-kartikey")
CHUNKS_FOLDER = "bankpulse/chunks"
OUTPUT_FOLDER = "bankpulse/detections"

# --- PostgreSQL Setup ---
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()

# --- Load Customer Importance ---
importance_df = pd.read_csv("CustomerImportance.csv")
importance_df.columns = ['customerName', 'merchantId', 'importance', 'transactionType', 'fraud']
importance_map = importance_df.groupby(['customerName', 'transactionType'])['importance'].mean().to_dict()

# --- Helper Functions ---
def current_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

def list_s3_files():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=CHUNKS_FOLDER)
    if "Contents" not in response:
        return []
    return sorted([item['Key'] for item in response['Contents'] if item['Key'].endswith(".csv")])

def read_s3_csv(key):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    df = pd.read_csv(obj['Body'])
    df.columns = ['step', 'customerName', 'age', 'gender', 'zipcodeOri',
                  'merchantId', 'zipMerchant', 'transactionType', 'amount', 'fraud']
    return df

def upload_detections(detections, index):
    buffer = StringIO()
    pd.DataFrame(detections).to_csv(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{OUTPUT_FOLDER}/detection_{index}.csv", Body=buffer.getvalue())

def update_postgres(txn_df):
    for _, row in txn_df.iterrows():
        # PatId2: Update txn summary
        cur.execute("""
            INSERT INTO txn_summary (merchant_id, customer_name, txn_count, total_value)
            VALUES (%s, %s, 1, %s)
            ON CONFLICT (merchant_id, customer_name) DO UPDATE
            SET txn_count = txn_summary.txn_count + 1,
                total_value = txn_summary.total_value + EXCLUDED.total_value
        """, (row['merchantId'], row['customerName'], row['amount']))

        # PatId3: Gender counts
        if row['gender'] == 'M':
            cur.execute("""
                INSERT INTO gender_stats (merchant_id, male_count)
                VALUES (%s, 1)
                ON CONFLICT (merchant_id) DO UPDATE
                SET male_count = gender_stats.male_count + 1
            """, (row['merchantId'],))
        elif row['gender'] == 'F':
            cur.execute("""
                INSERT INTO gender_stats (merchant_id, female_count)
                VALUES (%s, 1)
                ON CONFLICT (merchant_id) DO UPDATE
                SET female_count = gender_stats.female_count + 1
            """, (row['merchantId'],))

        # PatId1: Track total txn count per merchant
        cur.execute("""
            INSERT INTO merchant_txn_count (merchant_id, txn_count)
            VALUES (%s, 1)
            ON CONFLICT (merchant_id) DO UPDATE
            SET txn_count = merchant_txn_count.txn_count + 1
        """, (row['merchantId'],))

    conn.commit()

def detect_patterns():
    detections = []

    # PatId1: UPGRADE
    cur.execute("SELECT * FROM merchant_txn_count WHERE txn_count > 50000")
    merchants = cur.fetchall()
    for merchant_id, _ in merchants:
        cur.execute("SELECT customer_name, txn_count FROM txn_summary WHERE merchant_id = %s", (merchant_id,))
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=['customerName', 'txn_count'])
        if df.empty:
            continue

        # Map importance
        df['importance'] = df['customerName'].apply(
            lambda x: importance_map.get((x, 'es_transportation'), 0)  # assuming only 1 txn type for now
        )
        top_10_txn = df['txn_count'].quantile(0.9)
        bottom_10_imp = df['importance'].quantile(0.1)

        filtered = df[(df['txn_count'] >= top_10_txn) & (df['importance'] <= bottom_10_imp)]
        for _, row in filtered.iterrows():
            detections.append({
                "YStartTime": current_ist_time(),
                "detectionTime": current_ist_time(),
                "patternId": "PatId1",
                "ActionType": "UPGRADE",
                "customerName": row['customerName'],
                "merchantId": merchant_id
            })

    # PatId2: CHILD
    cur.execute("SELECT * FROM txn_summary")
    rows = cur.fetchall()
    for merchant_id, customer_name, txn_count, total_value in rows:
        avg_value = total_value / txn_count
        if txn_count >= 80 and avg_value < 23:
            detections.append({
                "YStartTime": current_ist_time(),
                "detectionTime": current_ist_time(),
                "patternId": "PatId2",
                "ActionType": "CHILD",
                "customerName": customer_name,
                "merchantId": merchant_id
            })

    # PatId3: DEI-NEEDED
    cur.execute("SELECT * FROM gender_stats")
    rows = cur.fetchall()
    for merchant_id, male_count, female_count in rows:
        if female_count > 100 and female_count < male_count:
            detections.append({
                "YStartTime": current_ist_time(),
                "detectionTime": current_ist_time(),
                "patternId": "PatId3",
                "ActionType": "DEI-NEEDED",
                "customerName": "",
                "merchantId": merchant_id
            })

    return detections

# --- Main runner ---
def mechanism_y():
    seen_chunks = set()
    detection_batch = []
    batch_index = 0

    print("[Started Mechanism Y]")

    while True:
        chunk_files = list_s3_files()
        new_files = [key for key in chunk_files if key not in seen_chunks]

        for key in new_files:
            print(f"[Processing] {key}")
            df = read_s3_csv(key)
            seen_chunks.add(key)

            update_postgres(df)
            new_detections = detect_patterns()

            for det in new_detections:
                detection_batch.append(det)
                if len(detection_batch) == 50:
                    upload_detections(detection_batch, batch_index)
                    print(f"[Uploaded] detection_{batch_index}.csv")
                    batch_index += 1
                    detection_batch = []

        # Upload remaining detections
        if detection_batch:
            upload_detections(detection_batch, batch_index)
            print(f"[Uploaded Final] detection_{batch_index}.csv")
            batch_index += 1
            detection_batch = []

        time.sleep(1)

mechanism_y()
