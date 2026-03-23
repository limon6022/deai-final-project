"""
Data Ingestion Script
Downloads NYC TLC trip data and stores it in MinIO (S3-compatible) as Parquet.
"""

import os
import boto3
import requests
from botocore.client import Config

# ─── MinIO Configuration ──────────────────────────────────────────────────────
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME     = "taxi"

# ─── NYC TLC Data URLs (2024) ─────────────────────────────────────────────────
DATA_URLS = {
    "yellow_2024_01": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    "yellow_2024_02": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
    "fhvhv_2024_01":  "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet",
    "fhvhv_2024_02":  "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-02.parquet",
}

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def ensure_bucket(s3, bucket: str):
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket}")
    else:
        print(f"Bucket already exists: {bucket}")

def file_exists_in_minio(s3, name: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=f"raw/{name}.parquet")
        print(f"  Skipping {name} — already in MinIO")
        return True
    except:
        return False

def download_and_upload(s3, name: str, url: str):
    # Skip if already uploaded
    if file_exists_in_minio(s3, name):
        return

    print(f"\nDownloading {name} ...")

    # Step 1 — Download to local disk in chunks
    tmp_file = f"/tmp/{name}.parquet"
    with requests.get(url, stream=True, timeout=600) as response:
        response.raise_for_status()
        total = 0
        with open(tmp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
                    print(f"  Downloading ... {total / 1024 / 1024:.0f} MB", end="\r")

    print(f"\n  Download complete! {total / 1024 / 1024:.0f} MB saved to disk")

    # Step 2 — Upload directly to MinIO (no pandas needed!)
    print(f"  Uploading to MinIO ...")
    s3_key = f"raw/{name}.parquet"
    with open(tmp_file, "rb") as f:
        s3.upload_fileobj(f, BUCKET_NAME, s3_key)
    print(f"  Uploaded to s3://{BUCKET_NAME}/{s3_key}")

    # Step 3 — Clean up temp file
    os.remove(tmp_file)
    print(f"  Done!")

def main():
    s3 = get_s3_client()
    ensure_bucket(s3, BUCKET_NAME)

    for name, url in DATA_URLS.items():
        try:
            download_and_upload(s3, name, url)
        except Exception as e:
            print(f"  ERROR for {name}: {e}")

    print("\nIngestion complete.")

if __name__ == "__main__":
    main()