import json
import os
import time
 
import boto3
from dotenv import load_dotenv
from kafka import KafkaConsumer
 
load_dotenv()
 
KAFKA_SERVER = "localhost:29092"
TOPIC = "stock-quotes"
 
RUSTFS_ENDPOINT = "http://localhost:9002"
RUSTFS_ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin")
RUSTFS_SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY", "ChangeThisSecretKey123")
BUCKET = "bronze-transactions"
 
 
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
        region_name="us-east-1",
    )
 
 
def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Bucket exists: {BUCKET}")
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket: {BUCKET}")
 
 
def main():
    s3 = get_s3_client()
    ensure_bucket(s3)
 
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bronze-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
 
    print()
    print("Kafka consumer started.")
    print("Saving records to RustFS...")
    print()
 
    for message in consumer:
        record = message.value
        symbol = record.get("symbol", "UNKNOWN")
        timestamp = record.get("fetched_at", int(time.time()))
        key = f"{symbol}/{timestamp}.json"
 
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(record),
            ContentType="application/json",
        )
        print(f"Saved {symbol} -> s3://{BUCKET}/{key}")
 
 
if __name__ == "__main__":
    main()