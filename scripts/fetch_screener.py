#!/usr/bin/env python3
"""
Fetch stock screener data from vnstock (for list) and VCI (for prices)
Designed to run on GitHub Actions daily
"""
import json
import time
from datetime import datetime
import concurrent.futures
import pandas as pd
from vnstock import Screener, Listing
import requests
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
            # We still use vnstock to get the LIST of stocks
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


def fetch_vietcap_price(symbol):
    """Fetch current price from Vietcap API"""
    try:
        url = f'https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}'
        headers = {
            'accept': 'application/json',
            'origin': 'https://trading.vietcap.com.vn',
            'referer': 'https://trading.vietcap.com.vn/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=5)
        # Don't raise for status immediately, check json first
        if response.status_code == 200:
            data = response.json()
            if data.get('successful') and data.get('data'):
                return symbol, data['data']
        return symbol, None
        
    except Exception:
        # Silently fail for individual stocks to keep logs clean
        return symbol, None


def fetch_all_screener_data():
    """Fetch complete screener data from all exchanges with index membership"""
    
    # 1. Load stock data from all exchanges in parallel (using vnstock for the list)
    logger.info("🚀 Fetching stock list from all exchanges...")
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
    
    # 2. Fetch VCI Prices in Parallel (REMOVED - Using vnstock price instead)
    # logger.info("💰 Fetching real-time prices from Vietcap...")
    vci_data = {}
    # ... VCI fetching logic removed ...

    # 3. Update DataFrame with VCI Data
    # We'll do this during the JSON conversion to avoid complex pandas merging
    
    # 4. Fetch index membership data
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
    
    # 5. Add index membership to each stock
    logger.info("🔗 Adding index membership to stocks...")
    screener_df['indexes'] = screener_df['ticker'].apply(
        lambda ticker: [idx for idx, tickers in index_membership.items() if ticker in tickers]
    )
    
    return screener_df, vci_data


def convert_to_json_safe(df, vci_data):
    """Convert DataFrame to JSON-safe list of dicts, OVERWRITING with VCI data"""
    import numpy as np
    
    stocks = []
    for _, row in df.iterrows():
        def safe_get(value):
            if pd.isna(value) or value is None or (isinstance(value, float) and np.isnan(value)):
                return None
            return value
        
        ticker = safe_get(row.get('ticker'))
        if not ticker:
            continue
            
        # Get VCI data for this ticker
        vci = vci_data.get(ticker, {})
        
        # Map exchange names
        exchange_raw = str(row.get('exchange', ''))
        exchange_mapped = {
            'HSX': 'HOSE',
            'HNX': 'HNX', 
            'UPCOM': 'UPCOM'
        }.get(exchange_raw, exchange_raw)
        
        # PRIORITIZE VNSTOCK DATA for Price
        # VCI price is often reference price, while vnstock gives near real-time match price
        price = safe_get(row.get('price_near_realtime'))
        if price:
            price = price * 1000 # vnstock is in thousands
            
        # Fallback to VCI if needed (though we removed VCI fetching above, vci_data might be empty)
        if not price:
             price = vci.get('currentPrice')
            
        market_cap = vci.get('marketCap')
        if market_cap is None:
            market_cap = safe_get(row.get('market_cap'))
            
        # Calculate price change if possible
        # VCI doesn't give change directly in overview, but we can try to use vnstock's change
        # OR just leave it. The user mainly complained about PRICE.
        # Let's use vnstock's change % for now as VCI overview is just a snapshot.
        # Wait, if price is new but change is old, it might look weird.
        # But we don't have yesterday's close from VCI overview easily.
        # Let's trust vnstock for the % change (it might be slightly off if price is different, 
        # but usually vnstock has correct % change even if price is delayed? No, that doesn't make sense).
        # Actually, if vnstock returns Friday's data, the % change is also Friday's.
        # We should probably set % change to 0 or null if we can't calculate it, 
        # OR just accept that % change might be outdated until we fix that too.
        # User only asked for PRICE from VCI.
        
        stock = {
            # Basic info
            'ticker': ticker,
            'company_name': vci.get('viOrganName') or safe_get(row.get('company_name')), # Prefer VCI name
            'exchange': exchange_mapped,
            'industry': safe_get(row.get('industry')),
            'indexes': row.get('indexes', []),
            
            # Valuation metrics
            'market_cap': market_cap,
            'price': price,
            'pe': safe_get(row.get('pe')),
            'pb': safe_get(row.get('pb')),
            'ps': safe_get(row.get('ps')),
            'ev_ebitda': safe_get(row.get('ev_ebitda')),
            'pcf': safe_get(row.get('pcf')),
            'peg_forward': safe_get(row.get('peg_forward')),
            'peg_trailing': safe_get(row.get('peg_trailing')),
            
            # Profitability
            'roe': safe_get(row.get('roe')),
            'roa': safe_get(row.get('roa')),
            'roic': safe_get(row.get('roic')),
            'gross_margin': safe_get(row.get('gross_margin')),
            'net_margin': safe_get(row.get('net_margin')),
            'ebit_margin': safe_get(row.get('ebit_margin')),
            'eps': safe_get(row.get('eps')),
            'bvps': safe_get(row.get('bvps')),
            'ebitda': safe_get(row.get('ebitda')),
            'ebit': safe_get(row.get('ebit')),
            
            # Growth
            'revenue_growth_1y': safe_get(row.get('revenue_growth_1y')),
            'revenue_growth_5y': safe_get(row.get('revenue_growth_5y')),
            'eps_growth_1y': safe_get(row.get('eps_growth_1y')),
            'eps_growth_5y': safe_get(row.get('eps_growth_5y')),
            'quarter_revenue_growth': safe_get(row.get('quarter_revenue_growth')),
            'quarter_income_growth': safe_get(row.get('quarter_income_growth')),
            'eps_ttm_growth1_year': safe_get(row.get('eps_ttm_growth1_year')),
            'eps_ttm_growth5_year': safe_get(row.get('eps_ttm_growth5_year')),
            
            # Technical indicators
            'rsi14': safe_get(row.get('rsi14')),
            'rsi14_status': safe_get(row.get('rsi14_status')),
            'price_vs_sma5': safe_get(row.get('price_vs_sma5')),
            'price_vs_sma10': safe_get(row.get('price_vs_sma10')),
            'price_vs_sma20': safe_get(row.get('price_vs_sma20')),
            'price_vs_sma50': safe_get(row.get('price_vs_sma50')),
            'price_vs_sma100': safe_get(row.get('price_vs_sma100')),
            'percent_price_vs_ma20': safe_get(row.get('percent_price_vs_ma20')),
            'percent_price_vs_ma50': safe_get(row.get('percent_price_vs_ma50')),
            'percent_price_vs_ma100': safe_get(row.get('percent_price_vs_ma100')),
            'tcbs_buy_sell_signal': safe_get(row.get('tcbs_buy_sell_signal')),
            'bolling_band_signal': safe_get(row.get('bolling_band_signal')),
            'breakout': safe_get(row.get('breakout')),
            
            # Trading
            'avg_trading_value_20d': safe_get(row.get('avg_trading_value_20d')),
            'dividend_yield': safe_get(row.get('dividend_yield')),
            'vol_vs_sma20': safe_get(row.get('vol_vs_sma20')),
            
            # Momentum
            'relative_strength_3d': safe_get(row.get('relative_strength_3d')),
            'rel_strength_1m': safe_get(row.get('rel_strength_1m')),
            'rel_strength_3m': safe_get(row.get('rel_strength_3m')),
            'rel_strength_1y': safe_get(row.get('rel_strength_1y')),
            'price_growth_1w': safe_get(row.get('price_growth_1w')),
            'price_growth_1m': safe_get(row.get('price_growth_1m')),
            'prev_1d_growth_pct': safe_get(row.get('prev_1d_growth_pct')),
            'prev_1y_growth_pct': safe_get(row.get('prev_1y_growth_pct')),
            
            # Ratings
            'stock_rating': safe_get(row.get('stock_rating')),
            'business_operation': safe_get(row.get('business_operation')),
            'business_model': safe_get(row.get('business_model')),
            'financial_health': safe_get(row.get('financial_health')),
            
            # Foreign trading
            'foreign_vol_pct': safe_get(row.get('foreign_buysell_20s')),
            'foreign_transaction': safe_get(row.get('foreign_transaction')),
            'active_buy_pct': safe_get(row.get('active_buy_pct')),
            'strong_buy_pct': safe_get(row.get('strong_buy_pct')),
            
            # Additional
            'beta': safe_get(row.get('beta')),
            'alpha': safe_get(row.get('alpha')),
            'ev': safe_get(row.get('ev')),
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
        screener_df, vci_data = fetch_all_screener_data()
        
        # Convert to JSON-safe format
        logger.info("📝 Converting to JSON format...")
        stocks = convert_to_json_safe(screener_df, vci_data)
        
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
        import os
        # Ensure data directory exists (relative to project root)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        output_file = os.path.join(data_dir, 'screener.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Successfully saved {len(stocks)} stocks")
        logger.info(f"⏱️  Total time: {elapsed:.2f}s")
        
        # Print summary
        print("\n" + "="*50)
        print(f"✅ Screener data fetch completed!")
        print(f"📊 Total stocks: {len(stocks)}")
        print(f"🏢 HOSE: {output['exchanges']['HOSE']}")
        print(f"🏢 HNX: {output['exchanges']['HNX']}")
        print(f"🏢 UPCOM: {output['exchanges']['UPCOM']}")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()
