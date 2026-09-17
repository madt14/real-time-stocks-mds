# For maintenance. Clearing out buckets

import os
 
import boto3
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
deleted = 0
for page in paginator.paginate(Bucket="bronze-transactions"):
    objects = page.get("Contents", [])
    if not objects:
        continue
    keys = [{"Key": obj["Key"]} for obj in objects]
    s3.delete_objects(Bucket="bronze-transactions", Delete={"Objects": keys})
    deleted += len(keys)
    print(f"Deleted {deleted} so far...")
 
print(f"Done. Deleted {deleted} objects total.")