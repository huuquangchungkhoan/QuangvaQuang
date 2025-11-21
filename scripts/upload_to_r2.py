#!/usr/bin/env python3
"""
Upload JSON files to Cloudflare R2
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
    """Upload screener.json, funds.json and company details to R2"""
    
    # Get R2 credentials
    account_id = os.getenv('R2_ACCOUNT_ID')
    access_key = os.getenv('R2_ACCESS_KEY_ID')
    secret_key = os.getenv('R2_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('R2_BUCKET_NAME')
    
    if not all([account_id, access_key, secret_key]):
        raise ValueError("Missing R2 credentials")
    
    # Configure S3 client
    endpoint_url = f'https://{account_id}.r2.cloudflarestorage.com'
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto'
    )
    
    # Get paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    uploaded_count = 0
    
    try:
        # Upload screener + funds
        for filename in ['screener.json', 'funds.json']:
            file_path = os.path.join(project_root, 'data', filename)
            
            if not os.path.exists(file_path):
                logger.warning(f"Skipping {filename}")
                continue
            
            logger.info(f"Uploading {filename}...")
            s3_client.upload_file(
                file_path,
                bucket_name,
                filename,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'CacheControl': 'public, max-age=1800',
                }
            )
            uploaded_count += 1
        
        # Upload company files
        companies_dir = os.path.join(project_root, 'data', 'companies')
        if os.path.exists(companies_dir):
            company_files = [f for f in os.listdir(companies_dir) if f.endswith('.json')]
            
            if company_files:
                logger.info(f"Uploading {len(company_files)} companies...")
                
                for filename in company_files:
                    file_path = os.path.join(companies_dir, filename)
                    s3_client.upload_file(
                        file_path,
                        bucket_name,
                        f"companies/{filename}",
                        ExtraArgs={
                            'ContentType': 'application/json',
                            'CacheControl': 'public, max-age=3600',
                        }
                    )
                    uploaded_count += 1
                    
                    if uploaded_count % 100 == 0:
                        logger.info(f"Progress: {uploaded_count}")
        
        logger.info(f"Uploaded {uploaded_count} files")
        return uploaded_count
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise


def main():
    try:
        count = upload_to_r2()
        print("\n" + "="*50)
        print(f"Uploaded {count} files")
        print("="*50 + "\n")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)


if __name__ == '__main__':
    main()
