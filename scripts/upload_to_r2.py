#!/usr/bin/env python3
"""
Upload screener.json to Cloudflare R2
Uses boto3 (S3-compatible API)
"""
import os
import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def upload_to_r2():
    """Upload screener.json and funds.json to Cloudflare R2 bucket"""
    
    # Get R2 credentials from environment variables
    account_id = os.getenv('R2_ACCOUNT_ID')
    access_key = os.getenv('R2_ACCESS_KEY_ID')
    secret_key = os.getenv('R2_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('R2_BUCKET_NAME', 'screener')
    custom_domain = os.getenv('R2_CUSTOM_DOMAIN', 'screener.lightinvest.vn')
    
    if not all([account_id, access_key, secret_key]):
        raise ValueError("❌ Missing R2 credentials in environment variables")
    
    # Configure S3 client for R2
    endpoint_url = f'https://{account_id}.r2.cloudflarestorage.com'
    
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto'  # R2 doesn't use regions
    )
    
    # Get project root path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Files to upload
    files_to_upload = [
        ('screener.json', 'screener.json'),
        ('funds.json', 'funds.json')
    ]
    
    uploaded_urls = []
    
    try:
        for filename, object_name in files_to_upload:
            file_path = os.path.join(project_root, 'data', filename)
            
            if not os.path.exists(file_path):
                logger.warning(f"⚠️  File not found, skipping: {filename}")
                continue
            
            logger.info(f"📤 Uploading {filename}...")
            
            # Upload with proper cache headers (30 minutes)
            s3_client.upload_file(
                file_path,
                bucket_name,
                object_name,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'CacheControl': 'public, max-age=1800',  # Cache for 30 minutes
                }
            )
            
            uploaded_urls.append(object_name)
            logger.info(f"✅ Uploaded {filename}")
        
        logger.info(f"✅ Successfully uploaded {len(uploaded_urls)} files")
        
        return uploaded_urls
        
    except ClientError as e:
        logger.error(f"❌ Upload failed: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise


def main():
    """Main execution function"""
    try:
        files = upload_to_r2()
        
        print("\n" + "="*50)
        print("✅ Upload to R2 completed!")
        print(f"� Uploaded {len(files)} files")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()
