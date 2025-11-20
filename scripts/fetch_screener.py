#!/usr/bin/env python3
"""
Fetch stock screener data from vnstock and save to JSON
Designed to run on GitHub Actions daily
"""
import json
import time
from datetime import datetime
import concurrent.futures
import pandas as pd
from vnstock import Screener, Listing
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_exchange_data(exchange):
    """Fetch data for a single exchange with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"📡 Loading {exchange} stocks (attempt {attempt + 1}/{max_retries})...")
            screener = Screener()
            df = screener.stock(params={"exchangeName": exchange}, limit=1000)
            
            if not df.empty:
                logger.info(f"✅ Loaded {len(df)} stocks from {exchange}")
                return df
            return pd.DataFrame()
            
        except Exception as e:
            logger.warning(f"⚠️ Error loading {exchange}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Failed to load {exchange} after {max_retries} attempts")
                return pd.DataFrame()
    
    return pd.DataFrame()


def fetch_index_constituents(index_name):
    """Fetch constituents for an index using vnstock"""
    try:
        lst = Listing()
        result = lst.symbols_by_group(index_name)
        
        if result is not None and not result.empty:
            # Extract tickers from result
            if hasattr(result, 'values'):
                tickers = result.values.tolist() if hasattr(result.values, 'tolist') else list(result.values)
            else:
                tickers = result.tolist()
            
            # Clean tickers
            tickers = [str(t).strip() for t in tickers if t and str(t).strip()]
            logger.info(f"✅ {index_name}: {len(tickers)} stocks")
            return (index_name, tickers)
        
        return (index_name, [])
        
    except Exception as e:
        logger.warning(f"⚠️ Error fetching {index_name}: {e}")
        return (index_name, [])


def fetch_all_screener_data():
    """Fetch complete screener data from all exchanges with index membership"""
    
    # 1. Load stock data from all exchanges in parallel
    logger.info("🚀 Fetching stock data from all exchanges...")
    exchanges = ['HOSE', 'HNX', 'UPCOM']
    all_stocks = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_exchange_data, ex) for ex in exchanges]
        for future in concurrent.futures.as_completed(futures):
            try:
                df = future.result()
                if not df.empty:
                    all_stocks.append(df)
            except Exception as e:
                logger.error(f"Error processing exchange: {e}")
    
    if not all_stocks:
        raise Exception("❌ No stock data could be loaded from any exchange")
    
    # Combine all dataframes
    screener_df = pd.concat(all_stocks, ignore_index=True)
    logger.info(f"✅ Combined {len(screener_df)} stocks from all exchanges")
    
    # 2. Fetch index membership data
    logger.info("📊 Fetching index membership data...")
    indexes = [
        # HOSE indexes
        'VN30', 'VN100', 'VNMidCap', 'VNSmallCap', 'VNAllShare', 'HOSE',
        # HNX indexes
        'HNX30', 'HNXCon', 'HNXLCap', 'HNXMSCap', 'HNXMan', 'HNX',
        # Other
        'UPCOM', 'ETF'
    ]
    
    index_membership = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_index_constituents, idx) for idx in indexes]
        for future in concurrent.futures.as_completed(futures):
            try:
                index_name, tickers = future.result()
                index_membership[index_name] = tickers
            except Exception as e:
                logger.error(f"Error processing index: {e}")
    
    # 3. Add index membership to each stock
    logger.info("🔗 Adding index membership to stocks...")
    screener_df['indexes'] = screener_df['ticker'].apply(
        lambda ticker: [idx for idx, tickers in index_membership.items() if ticker in tickers]
    )
    
    return screener_df


def convert_to_json_safe(df):
    """Convert DataFrame to JSON-safe list of dicts"""
    import numpy as np
    
    stocks = []
    for _, row in df.iterrows():
        def safe_get(value):
            if pd.isna(value) or value is None or (isinstance(value, float) and np.isnan(value)):
                return None
            return value
        
        # Map exchange names
        exchange_raw = str(row.get('exchange', ''))
        exchange_mapped = {
            'HSX': 'HOSE',
            'HNX': 'HNX', 
            'UPCOM': 'UPCOM'
        }.get(exchange_raw, exchange_raw)
        
        stock = {
            'ticker': safe_get(row.get('ticker')),
            'company_name': safe_get(row.get('company_name')),
            'exchange': exchange_mapped,
            'industry': safe_get(row.get('industry')),
            'indexes': row.get('indexes', []),
            
            # Valuation metrics
            'market_cap': safe_get(row.get('market_cap')),
            'price': safe_get(row.get('price_near_realtime')),
            'pe': safe_get(row.get('pe')),
            'pb': safe_get(row.get('pb')),
            'ps': safe_get(row.get('ps')),
            'ev_ebitda': safe_get(row.get('ev_ebitda')),
            
            # Profitability
            'roe': safe_get(row.get('roe')),
            'roa': safe_get(row.get('roa')),
            'gross_margin': safe_get(row.get('gross_margin')),
            'net_margin': safe_get(row.get('net_margin')),
            'eps': safe_get(row.get('eps')),
            
            # Growth
            'revenue_growth_1y': safe_get(row.get('revenue_growth_1y')),
            'eps_growth_1y': safe_get(row.get('eps_growth_1y')),
            
            # Technical indicators
            'rsi14': safe_get(row.get('rsi14')),
            'price_vs_sma20': safe_get(row.get('price_vs_sma20')),
            'price_vs_sma50': safe_get(row.get('price_vs_sma50')),
            
            # Trading
            'avg_trading_value_20d': safe_get(row.get('avg_trading_value_20d')),
            'dividend_yield': safe_get(row.get('dividend_yield')),
        }
        
        # Only add stocks with basic data
        if stock['ticker'] and (stock['market_cap'] or stock['price']):
            stocks.append(stock)
    
    return stocks


def main():
    """Main execution function"""
    try:
        start_time = time.time()
        logger.info("🚀 Starting screener data fetch...")
        
        # Fetch data
        screener_df = fetch_all_screener_data()
        
        # Convert to JSON-safe format
        logger.info("📝 Converting to JSON format...")
        stocks = convert_to_json_safe(screener_df)
        
        # Prepare output
        output = {
            'last_updated': datetime.now().isoformat(),
            'total_stocks': len(stocks),
            'exchanges': {
                'HOSE': len([s for s in stocks if s['exchange'] == 'HOSE']),
                'HNX': len([s for s in stocks if s['exchange'] == 'HNX']),
                'UPCOM': len([s for s in stocks if s['exchange'] == 'UPCOM']),
            },
            'stocks': stocks
        }
        
        # Save to file
        output_file = 'data/screener.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Successfully saved {len(stocks)} stocks to {output_file}")
        logger.info(f"⏱️  Total time: {elapsed:.2f}s")
        
        # Print summary
        print("\n" + "="*50)
        print(f"✅ Screener data fetch completed!")
        print(f"📊 Total stocks: {len(stocks)}")
        print(f"🏢 HOSE: {output['exchanges']['HOSE']}")
        print(f"🏢 HNX: {output['exchanges']['HNX']}")
        print(f"🏢 UPCOM: {output['exchanges']['UPCOM']}")
        print(f"📁 Output: {output_file}")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()
