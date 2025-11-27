#!/opt/homebrew/bin/python3.12
"""
Upload technical_analysis.json to Cloudflare R2 bucket
"""
import os
import boto3
from pathlib import Path
from botocore.exceptions import NoCredentialsError

# Configuration
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
BUCKET_NAME = 'screener'
FILE_PATH = Path('data/technical_analysis.json')

def upload_to_r2():
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        print("❌ Error: R2 credentials not found in environment variables.")
        return False

    if not FILE_PATH.exists():
        print(f"❌ Error: File not found: {FILE_PATH}")
        return False

    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )

    # Upload to technical_analysis.json (root)
    s3_key = 'technical_analysis.json'
    
    try:
        print(f"📤 Uploading {FILE_PATH} to R2 bucket '{BUCKET_NAME}' as {s3_key}...")
        
        with open(FILE_PATH, 'rb') as f:
            s3.upload_fileobj(
                f, 
                BUCKET_NAME, 
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'CacheControl': 'public, max-age=300'  # 5 minutes cache
                }
            )
        
        print(f"✅ Successfully uploaded to R2: {s3_key}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to upload: {e}")
        return False

if __name__ == "__main__":
    success = upload_to_r2()
    exit(0 if success else 1)
