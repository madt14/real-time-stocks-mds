# For maintenance. Count the number of messages per bucket

import boto3
import os
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id=os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"),
    aws_secret_access_key=os.getenv("RUSTFS_SECRET_KEY", "ChangeThisSecretKey123"),
    region_name="us-east-1",
)

paginator = s3.get_paginator("list_objects_v2")
counts = {}
for page in paginator.paginate(Bucket="bronze-transactions"):
    for obj in page.get("Contents", []):
        symbol = obj["Key"].split("/")[0]
        counts[symbol] = counts.get(symbol, 0) + 1

for symbol, count in sorted(counts.items()):
    print(f"{symbol}: {count}")