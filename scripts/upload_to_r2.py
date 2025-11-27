import os
import boto3
from pathlib import Path
from botocore.exceptions import NoCredentialsError

# Configuration
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
BUCKET_NAME = 'screener'
DATA_DIR = Path('data')

# Data files to upload
JSON_FILES = [
    'screener.json',
    'funds.json'
]

# Arrow IPC files to upload  
ARROW_FILES = [
    'metadata.json',
    'balance_sheet_data.arrow',
    'income_statement_data.arrow',
    'cash_flow_data.arrow',
    'note_data.arrow',
    'ratios_data.arrow'
]

def upload_to_r2():
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        print("Error: R2 credentials not found in environment variables.")
        return

    s3 = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )

    # Upload JSON files from data/
    print(f"📤 Uploading JSON files from {DATA_DIR}...")
    for filename in JSON_FILES:
        file_path = DATA_DIR / filename
        if file_path.exists():
            s3_key = filename
            try:
                print(f"  {s3_key}... ", end='', flush=True)
                with open(file_path, 'rb') as f:
                    s3.upload_fileobj(
                        f, 
                        BUCKET_NAME, 
                        s3_key,
                        ExtraArgs={
                            'ContentType': 'application/json',
                            'CacheControl': 'public, max-age=300'
                        }
                    )
                print("✅")
            except Exception as e:
                print(f"❌ {e}")
        else:
            print(f"  ⚠️ File not found: {file_path}")

    # Upload Arrow files from frontend/api_data/
    arrow_dir = Path('frontend/api_data')
    print(f"\n📤 Uploading Arrow IPC files from {arrow_dir}...")
    
    total_size = 0
    uploaded_count = 0
    
    # Files to upload directly from arrow_dir (root)
    root_files = [
        'metadata.json',
        'ratios_data.arrow'
    ]
    
    for filename in root_files:
        file_path = arrow_dir / filename
        if file_path.exists():
            s3_key = filename
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            total_size += file_size_mb
            
            # Determine content type
            if filename.endswith('.json'):
                content_type = 'application/json'
            elif filename.endswith('.arrow'):
                content_type = 'application/vnd.apache.arrow.stream'
            else:
                content_type = 'application/octet-stream'
            
            try:
                print(f"  {s3_key} ({file_size_mb:.1f} MB)... ", end='', flush=True)
                with open(file_path, 'rb') as f:
                    s3.upload_fileobj(
                        f, 
                        BUCKET_NAME, 
                        s3_key,
                        ExtraArgs={
                            'ContentType': content_type,
                            'CacheControl': 'public, max-age=3600'
                        }
                    )
                print("✅")
                uploaded_count += 1
            except Exception as e:
                print(f"❌ {e}")
        else:
            print(f"  ⚠️ File not found: {file_path}")

    # Upload partitioned financial files (recursive)
    financials_dirs = ['balance_sheet', 'income_statement', 'cash_flow', 'note', 'ratios']
    
    for report_type in financials_dirs:
        report_dir = arrow_dir / report_type
        if not report_dir.exists():
            continue
            
        print(f"\n📂 Uploading {report_type} files...")
        for file_path in report_dir.glob('*.arrow'):
            s3_key = f"{report_type}/{file_path.name}"
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            total_size += file_size_mb
            
            try:
                print(f"  {s3_key} ({file_size_mb:.1f} MB)... ", end='', flush=True)
                with open(file_path, 'rb') as f:
                    s3.upload_fileobj(
                        f, 
                        BUCKET_NAME, 
                        s3_key,
                        ExtraArgs={
                            'ContentType': 'application/vnd.apache.arrow.stream',
                            'CacheControl': 'public, max-age=3600'
                        }
                    )
                print("✅")
                uploaded_count += 1
            except Exception as e:
                print(f"❌ {e}")

    # Upload Listings
    listings_dir = arrow_dir / 'listings'
    if listings_dir.exists():
        print(f"\n📤 Uploading listings from {listings_dir}...")
        for file_path in listings_dir.glob('*.json'):
            s3_key = f"listings/{file_path.name}"
            try:
                print(f"  {s3_key}... ", end='', flush=True)
                with open(file_path, 'rb') as f:
                    s3.upload_fileobj(
                        f, 
                        BUCKET_NAME, 
                        s3_key,
                        ExtraArgs={
                            'ContentType': 'application/json',
                            'CacheControl': 'public, max-age=3600'
                        }
                    )
                print("✅")
            except Exception as e:
                print(f"❌ {e}")
    
    print("\n" + "=" * 60)
    print("✅ Upload complete!")
    print(f"Arrow files uploaded: {uploaded_count}/{len(ARROW_FILES)} ({total_size:.1f} MB)")
    print("\nAccess URLs:")
    print("  https://screener.lightinvest.vn/metadata.json")
    print("  https://screener.lightinvest.vn/balance_sheet_data.arrow")
    print("  (+ other Arrow files)")

if __name__ == "__main__":
    upload_to_r2()
