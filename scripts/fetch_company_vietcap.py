#!/usr/bin/env python3
"""
Fetch detailed company information using Vietcap API
Creates individual JSON file per ticker
NO RATE LIMIT - can run in parallel!
Runs at 5PM daily
"""
import json
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from vnstock import Screener
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_vietcap_overview(symbol):
    """Fetch company overview from Vietcap API"""
    try:
        url = f'https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}'
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
            return data['data']
        return None
        
    except Exception as e:
        logger.warning(f"Failed to fetch overview for {symbol}: {e}")
        return None


def fetch_vietcap_foreign_flow(symbol):
    """Fetch foreign trading flow from Vietcap API"""
    try:
        url = f'https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}/daily-info'
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
            # Return last 90 days only to save space
            return data['data'][-90:] if len(data['data']) > 90 else data['data']
        return []
        
    except Exception as e:
        logger.warning(f"Failed to fetch foreign flow for {symbol}: {e}")
        return []


# PE/PB history and financial stats are fetched separately in fetch_ratios.py
# This script ONLY fetches daily updated data: overview + foreign flow


def fetch_company_details(symbol):
    """Fetch all company information from Vietcap API"""
    try:
        details = {
            'symbol': symbol,
            'last_updated': datetime.now().isoformat(),
            'data_source': 'vietcap'
        }
        
        # Fetch daily updated data only (ratios history in separate script)
        overview = fetch_vietcap_overview(symbol)
        if overview:
            details['overview'] = overview
        
        foreign_flow = fetch_vietcap_foreign_flow(symbol)
        if foreign_flow:
            details['foreign_flow_90d'] = foreign_flow
        
        # Financial stats are fetched separately in fetch_ratios.py
        # This script ONLY handles daily-updated data
        
        # Only return if we got at least overview
        if 'overview' in details:
            logger.info(f"✅ {symbol}")
            return symbol, details
        else:
            logger.warning(f"❌ {symbol} - No data")
            return symbol, None
            
    except Exception as e:
        logger.error(f"❌ {symbol}: {e}")
        return symbol, None


def main():
    try:
        start_time = time.time()
        logger.info("🚀 Starting Vietcap company data fetch...")
        
        # Get all stock symbols from fetch_screener.py data
        logger.info("📡 Loading stock list from screener data...")
        
        # Read from screener.json if exists, otherwise fetch fresh
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        screener_file = os.path.join(project_root, 'data', 'screener.json')
        
        all_stocks = []
        
        if os.path.exists(screener_file):
            logger.info("Using existing screener.json")
            with open(screener_file, 'r', encoding='utf-8') as f:
                screener_data = json.load(f)
                all_stocks = [stock['ticker'] for stock in screener_data.get('stocks', [])]
        else:
            logger.info("Screener.json not found, downloading from R2...")
            # Download screener.json from R2
            try:
                response = requests.get('https://screener.lightinvest.vn/screener.json', timeout=30)
                response.raise_for_status()
                screener_data = response.json()
                all_stocks = [stock['ticker'] for stock in screener_data.get('stocks', [])]
                logger.info(f"✅ Downloaded screener data from R2")
            except Exception as e:
                logger.error(f"Failed to download screener.json from R2: {e}")
                logger.info("Falling back to vnstock...")
                # Fallback to vnstock
                screener = Screener()
                for exchange in ['HOSE', 'HNX', 'UPCOM']:
                    logger.info(f"Loading {exchange}...")
                    try:
                        if exchange == 'HOSE':
                            df = screener.stock(exchange='HSX')
                        elif exchange == 'HNX':
                            df = screener.stock(exchange='HNX')
                        else:
                            df = screener.stock(exchange='UPCOM')
                        
                        if not df.empty:
                            symbols = df['ticker'].tolist()
                            all_stocks.extend(symbols)
                            logger.info(f"✅ {exchange}: {len(symbols)} stocks")
                    except Exception as e:
                        logger.warning(f"Failed to load {exchange}: {e}")
        
        if not all_stocks:
            logger.error("No stocks found!")
            return
            
        logger.info(f"✅ Found {len(all_stocks)} stocks")
        
        # Create output directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        companies_dir = os.path.join(project_root, 'data', 'companies')
        os.makedirs(companies_dir, exist_ok=True)
        
        success_count = 0
        fail_count = 0
        
        logger.info(f"📊 Fetching details for {len(all_stocks)} companies")
        logger.info(f"⚡ Using parallel execution (NO rate limit!)")
        
        # Parallel execution with ThreadPoolExecutor
        max_workers = 20  # Can increase this since no rate limit
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(fetch_company_details, symbol): symbol 
                for symbol in all_stocks
            }
            
            # Process completed tasks
            for idx, future in enumerate(as_completed(future_to_symbol), 1):
                symbol = future_to_symbol[future]
                
                try:
                    symbol, details = future.result()
                    
                    if details:
                        # Save individual file
                        output_file = os.path.join(companies_dir, f"{symbol}.json")
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(details, f, ensure_ascii=False, indent=2)
                        success_count += 1
                    else:
                        fail_count += 1
                    
                    # Progress update every 50 stocks
                    if idx % 50 == 0:
                        logger.info(f"Progress: {idx}/{len(all_stocks)} ({success_count} success, {fail_count} failed)")
                        
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
                    fail_count += 1
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Successfully saved {success_count} companies")
        logger.info(f"❌ Failed: {fail_count}")
        logger.info(f"⏱️  Total time: {elapsed:.2f}s ({elapsed/60:.1f} minutes)")
        
        # Print summary
        print("\n" + "="*50)
        print(f"✅ Company data fetch completed!")
        print(f"📊 Total stocks: {len(all_stocks)}")
        print(f"✅ Success: {success_count}")
        print(f"❌ Failed: {fail_count}")
        print(f"⏱️  Time: {elapsed:.2f}s ({elapsed/60:.1f} min)")
        print(f"⚡ Speed: {len(all_stocks)/elapsed:.1f} stocks/second")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
