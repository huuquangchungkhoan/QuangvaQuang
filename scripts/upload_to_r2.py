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
    """Upload screener.json to Cloudflare R2 bucket"""
    
    # Get R2 credentials from environment variables
    account_id = os.getenv('R2_ACCOUNT_ID')
    access_key = os.getenv('R2_ACCESS_KEY_ID')
    secret_key = os.getenv('R2_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('R2_BUCKET_NAME', 'vietcap-data')
    
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
    
    # Upload file
    file_path = 'data/screener.json'
    object_name = 'screener.json'
    
    try:
        logger.info(f"📤 Uploading {file_path} to R2 bucket '{bucket_name}'...")
        
        # Upload with public-read ACL and proper content type
        s3_client.upload_file(
            file_path,
            bucket_name,
            object_name,
            ExtraArgs={
                'ContentType': 'application/json',
                'CacheControl': 'public, max-age=3600',  # Cache for 1 hour
            }
        )
        
        logger.info(f"✅ Successfully uploaded to R2!")
        
        # Generate public URL
        public_url = f"https://{bucket_name}.{account_id}.r2.dev/{object_name}"
        logger.info(f"🔗 Public URL: {public_url}")
        
        return public_url
        
    except ClientError as e:
        logger.error(f"❌ Upload failed: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"❌ File not found: {file_path}")
        raise


def main():
    """Main execution function"""
    try:
        public_url = upload_to_r2()
        
        print("\n" + "="*50)
        print("✅ Upload to R2 completed!")
        print(f"🔗 Public URL: {public_url}")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()
