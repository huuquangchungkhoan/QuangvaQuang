#!/usr/bin/env python3
"""
Upload Apache Arrow IPC financial data files to Cloudflare R2.
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
DATA_DIR = Path('frontend/api_data')

# Arrow IPC files to upload
ARROW_FILES = [
    ('metadata.json', 'application/json'),
    ('balance_sheet_data.arrow', 'application/octet-stream'),
    ('income_statement_data.arrow', 'application/octet-stream'),
    ('cash_flow_data.arrow', 'application/octet-stream'),
    ('note_data.arrow', 'application/octet-stream'),
]

def upload_arrow_files():
    """Upload Apache Arrow/Parquet files to R2"""
    
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        print("❌ Error: R2 credentials not found in environment variables.")
        print("   Required: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
        return False

    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )

    print("=" * 60)
    print("Uploading Apache Arrow Files to R2")
    print("=" * 60)
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Source: {DATA_DIR}\n")

    success_count = 0
    total_size = 0

    for filename, content_type in ARROW_FILES:
        file_path = DATA_DIR / filename
        
        if not file_path.exists():
            print(f"⚠️  File not found: {filename}")
            continue
        
        s3_key = filename  # Upload to root
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        total_size += file_size_mb
        
        try:
            print(f"📤 Uploading {filename} ({file_size_mb:.1f} MB)... ", end='', flush=True)
            
            with open(file_path, 'rb') as f:
                s3.upload_fileobj(
                    f, 
                    BUCKET_NAME, 
                    s3_key,
                    ExtraArgs={
                        'ContentType': content_type,
                        'CacheControl': 'public, max-age=3600'  # 1 hour cache for Arrow files
                    }
                )
            
            print("✅")
            success_count += 1
            
        except Exception as e:
            print(f"❌")
            print(f"   Error: {e}")

    print("\n" + "=" * 60)
    print(f"✅ Upload Complete!")
    print("=" * 60)
    print(f"Files uploaded: {success_count}/{len(ARROW_FILES)}")
    print(f"Total size: {total_size:.1f} MB")
    print(f"\nAccess URLs:")
    for filename, _ in ARROW_FILES[:5]:  # Show first 5
        if (DATA_DIR / filename).exists():
            print(f"  https://screener.lightinvest.vn/{filename}")
    
    return success_count == len([f for f, _ in ARROW_FILES if (DATA_DIR / f).exists()])

if __name__ == "__main__":
    success = upload_arrow_files()
    exit(0 if success else 1)
