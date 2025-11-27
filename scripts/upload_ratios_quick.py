import os
import boto3
from pathlib import Path

# Credentials from user
R2_ACCOUNT_ID = "560fe29dca4a9c9d5d07ad67abdc6fb4"
R2_ACCESS_KEY_ID = "7a7a131c75ea6bf4982f349c29f68e51"
R2_SECRET_ACCESS_KEY = "615a97503f18169c3587577eee863df491db90ceb71550068849825107e678c1"
BUCKET_NAME = 'screener'

def upload_ratios():
    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )
    
    file_path = Path('frontend/api_data/ratios_data.arrow')
    s3_key = 'ratios_data.arrow'
    
    print(f"Uploading {s3_key} ({file_path.stat().st_size / 1024 / 1024:.2f} MB)...")
    
    with open(file_path, 'rb') as f:
        s3.upload_fileobj(
            f, 
            BUCKET_NAME, 
            s3_key,
            ExtraArgs={
                'ContentType': 'application/vnd.apache.arrow.stream', # Network Compression
                'CacheControl': 'public, max-age=3600'
            }
        )
    print("✅ Upload complete!")

if __name__ == "__main__":
    upload_ratios()
