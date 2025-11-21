#!/usr/bin/env python3
"""
Fetch historical ratios (PE/PB) data from Vietcap API
This data rarely changes, so we fetch it separately from daily company updates
"""
import json
import logging
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_vietcap_ratios_history(symbol):
    """Fetch financial statistics from Vietcap API - 20 quarters (5 years) with all ratios"""
    try:
        url = f'https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}/statistics-financial'
        headers = {
            'accept': 'application/json',
            'origin': 'https://trading.vietcap.com.vn',
            'referer': 'https://trading.vietcap.com.vn/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data.get('successful') and data.get('data'):
            # Return last 20 quarters (5 years)
            return data['data'][-20:] if len(data['data']) > 20 else data['data']
        return []
        
    except Exception as e:
        logger.warning(f"Failed to fetch ratios for {symbol}: {e}")
        return []


def fetch_ratio_details(symbol):
    """Fetch ratio details for a single stock"""
    ratios = fetch_vietcap_ratios_history(symbol)
    
    if ratios:
        logger.info(f"✅ {symbol}")
        return symbol, {
            'symbol': symbol,
            'last_updated': datetime.now().isoformat(),
            'data_source': 'vietcap',
            'total_quarters': len(ratios),
            'financial_stats': ratios
        }
    else:
        logger.warning(f"❌ {symbol}")
        return symbol, None


def main():
    logger.info("🚀 Starting ratios data fetch...")
    
    # Load stock list from screener
    logger.info("📡 Loading stock list from screener data...")
    screener_file = Path('data/screener.json')
    
    if not screener_file.exists():
        logger.error("screener.json not found! Run fetch_screener.py first")
        return
    
    with open(screener_file, 'r', encoding='utf-8') as f:
        screener_data = json.load(f)
    
    stocks = screener_data.get('stocks', [])
    total_stocks = len(stocks)
    logger.info(f"✅ Found {total_stocks} stocks")
    
    # Fetch ratios in parallel
    logger.info(f"📊 Fetching ratios for {total_stocks} companies")
    logger.info("⚡ Using parallel execution (NO rate limit!)")
    
    results = {}
    success_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(fetch_ratio_details, stock['ticker']): stock['ticker'] 
            for stock in stocks
        }
        
        for i, future in enumerate(as_completed(futures), 1):
            symbol, data = future.result()
            
            if data:
                results[symbol] = data
                success_count += 1
            else:
                failed_count += 1
            
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{total_stocks} ({success_count} success, {failed_count} failed)")
    
    # Save results
    output_dir = Path('data/ratios')
    output_dir.mkdir(exist_ok=True)
    
    logger.info(f"💾 Saving {len(results)} ratio files...")
    
    for symbol, data in results.items():
        output_file = output_dir / f'{symbol}.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Successfully saved {success_count} ratio files")
    logger.info(f"❌ Failed: {failed_count}")
    
    # Summary
    print("\n" + "="*50)
    print("✅ Ratios data fetch completed!")
    print(f"📊 Total stocks: {total_stocks}")
    print(f"✅ Success: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print("="*50)


if __name__ == '__main__':
    import time
    start_time = time.time()
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        elapsed = time.time() - start_time
        logger.info(f"⏱️  Total time: {elapsed:.2f}s ({elapsed/60:.1f} minutes)")
