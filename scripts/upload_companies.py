#!/usr/bin/env python3
"""
Upload companies data to R2
"""
import os
import boto3
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def upload_companies_to_r2():
    """Upload company JSON files to R2"""
    
    # Get R2 credentials from environment
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
    
    companies_dir = Path('data/companies')
    
    if not companies_dir.exists():
        logger.error("Companies directory not found!")
        return
    
    # Get all company JSON files
    json_files = list(companies_dir.glob('*.json'))
    total_files = len(json_files)
    
    logger.info(f"Found {total_files} company files to upload")
    
    def upload_file(file_path):
        """Upload a single file"""
        try:
            s3_client.upload_file(
                str(file_path),
                bucket_name,
                f'companies/{file_path.name}',
                ExtraArgs={
                    'ContentType': 'application/json',
                    'CacheControl': 'public, max-age=3600'  # 1 hour cache
                }
            )
            return True, file_path.name
        except Exception as e:
            return False, f"{file_path.name}: {e}"
    
    # Upload with parallel execution
    start_time = time.time()
    uploaded = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(upload_file, f): f for f in json_files}
        
        for future in as_completed(futures):
            ok, msg = future.result()
            if ok:
                uploaded += 1
            else:
                failed += 1
                logger.error(f"Failed: {msg}")
            
            if uploaded % 100 == 0:
                elapsed = time.time() - start_time
                rate = uploaded / elapsed
                remaining = (total_files - uploaded) / rate if rate > 0 else 0
                logger.info(f"Progress: {uploaded}/{total_files} ({rate:.1f} files/s, ~{remaining:.0f}s left)")
    
    elapsed = time.time() - start_time
    
    logger.info(f"\n{'='*50}")
    logger.info(f"Upload completed!")
    logger.info(f"Total: {total_files}")
    logger.info(f"Success: {uploaded}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Time: {elapsed:.1f}s ({uploaded/elapsed:.1f} files/s)")
    logger.info(f"{'='*50}")


if __name__ == '__main__':
    try:
        upload_companies_to_r2()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
