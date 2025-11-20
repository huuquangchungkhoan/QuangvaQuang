"""
Stock screener routes with database cache
"""
from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import logging
import time
from utils.thread_pool import get_api_thread_pool
from utils.cache_utils import get_listings_cache
from services.price_cache_service import get_price_cache_service

screener_bp = Blueprint('screener', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

# Import database cache
try:
    from screener_cache_db import screener_cache_db
    DB_CACHE_ENABLED = True
    logger.info("✅ Database cache enabled for screener")
except ImportError:
    DB_CACHE_ENABLED = False
    logger.warning("⚠️ Database cache disabled - using in-memory cache")
    # Fallback to in-memory cache
    screener_cache = {
        'data': None,
        'timestamp': None,
        'ttl': 1800  # 30 minutes cache
    }


def get_cached_screener_data():
    """Get cached screener data or fetch new data if expired"""
    import pandas as pd
    from vnstock import Screener
    import concurrent.futures
    import requests

    listings_cache = get_listings_cache()

    # Try database cache first
    if DB_CACHE_ENABLED:
        cached_data = screener_cache_db.get_cache()
        if cached_data is not None:
            return cached_data
    else:
        # Fallback to in-memory cache
        current_time = time.time()
        if (screener_cache['data'] is not None and 
            screener_cache['timestamp'] is not None and
            current_time - screener_cache['timestamp'] < screener_cache['ttl']):
            print("🚀 Using in-memory cached screener data")
            return screener_cache['data']
    
    print("📥 Fetching fresh screener data...")
    
    # Load data from all exchanges in parallel
    exchanges = ['HOSE', 'HNX', 'UPCOM']
    all_stocks = []
    
    def load_exchange_data(exchange):
        """Load data for a single exchange with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"📡 Loading {exchange} (attempt {attempt + 1}/{max_retries})...")
                exchange_screener = Screener()
                exchange_df = exchange_screener.stock(params={"exchangeName": exchange}, limit=1000)
                if not exchange_df.empty:
                    logger.info(f"✅ Loaded {len(exchange_df)} stocks from {exchange}")
                    return exchange_df
                return pd.DataFrame()
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ Timeout loading {exchange} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    logger.info(f"⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Failed to load {exchange} after {max_retries} attempts (timeout)")
                    return pd.DataFrame()
            except Exception as e:
                logger.warning(f"⚠️ Error loading {exchange}: {e} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                else:
                    return pd.DataFrame()
        return pd.DataFrame()
    
    # Load data in parallel using global thread pool
    executor = get_api_thread_pool()
    future_to_exchange = {executor.submit(load_exchange_data, exchange): exchange 
                        for exchange in exchanges}
        
    for future in concurrent.futures.as_completed(future_to_exchange):
        exchange = future_to_exchange[future]
        try:
            df = future.result()
            if not df.empty:
                all_stocks.append(df)
        except Exception as e:
            print(f"⚠️ Error processing {exchange}: {e}")
    
    if not all_stocks:
        return None
    
    # Combine all dataframes
    screener_df = pd.concat(all_stocks, ignore_index=True)
    
    # ===== ADD INDEX MEMBERSHIP DATA =====
    print("📊 Fetching index membership data from vnstock...")
    from vnstock import Listing
    
    # Only working indexes (tested)
    indexes = [
        # HOSE indexes (6)
        'VN30', 'VN100', 'VNMidCap', 'VNSmallCap', 'VNAllShare', 'HOSE',
        # HNX indexes (6)
        'HNX30', 'HNXCon', 'HNXLCap', 'HNXMSCap', 'HNXMan', 'HNX',
        # Other (2)
        'UPCOM', 'ETF'
    ]
    
    # Initialize indexes column
    screener_df['indexes'] = [[] for _ in range(len(screener_df))]
    
    def fetch_index_constituents(index_name):
        """Fetch constituents for an index using vnstock"""
        cached = listings_cache.get(index_name)
        if cached is not None:
            return (index_name, cached)

        try:
            lst = Listing()
            result = lst.symbols_by_group(index_name)
            
            # Result can be Series or DataFrame
            if result is not None and not result.empty:
                # If Series, values are tickers
                if hasattr(result, 'values'):
                    tickers = result.values.tolist() if hasattr(result.values, 'tolist') else list(result.values)
                else:
                    tickers = result.tolist()
                
                # Clean tickers (remove None, empty strings)
                tickers = [str(t).strip() for t in tickers if t and str(t).strip()]
                print(f"  ✅ {index_name}: {len(tickers)} stocks")
                listings_cache.set(index_name, tickers)
                return (index_name, tickers)
            listings_cache.set(index_name, [])
            return (index_name, [])
        except Exception as e:
            print(f"  ⚠️ Error fetching {index_name}: {e}")
            listings_cache.set(index_name, [])
            return (index_name, [])
    
    # Fetch all indexes in parallel using global thread pool
    index_membership = {}
    executor = get_api_thread_pool()
    futures = [executor.submit(fetch_index_constituents, idx) for idx in indexes]
    for future in concurrent.futures.as_completed(futures):
            try:
                index_name, tickers = future.result()
                index_membership[index_name] = tickers
            except Exception as e:
                print(f"  ⚠️ Error processing index: {e}")
    
    # Add index membership to each stock (store as JSON string to avoid nested array issues)
    import json
    for ticker in screener_df['ticker'].unique():
        stock_indexes = [idx for idx, tickers in index_membership.items() if ticker in tickers]
        # Convert to JSON string to avoid pandas nested list issues
        screener_df.loc[screener_df['ticker'] == ticker, 'indexes'] = json.dumps(stock_indexes)
    
    print(f"✅ Added index membership for {len(index_membership)} indexes")
    
    # Cache the data
    if DB_CACHE_ENABLED:
        screener_cache_db.set_cache(screener_df)
    else:
        # Fallback to in-memory cache
        current_time = time.time()
        screener_cache['data'] = screener_df
        screener_cache['timestamp'] = current_time
        print(f"💾 Cached {len(screener_df)} stocks in memory for 30 minutes")
    
    return screener_df


def init_screener_routes(data_manager):
    """Initialize screener routes with dependencies"""
    
    @screener_bp.route('/screener')
    def get_screener_data():
        """API endpoint for stock screener data using vnstock"""
        try:
            from vnstock import Screener
            import pandas as pd
            import numpy as np
            import concurrent.futures
            import json
            
            # Get screener data from vnstock - prioritize exchanges with better data
            all_stocks = []
            exchanges = ["HOSE", "HNX", "UPCOM"]
            
            def load_exchange_data(exchange):
                """Load data for a single exchange"""
                try:
                    print(f"📊 Loading {exchange} stocks...")
                    exchange_screener = Screener()  # Create new instance for thread safety
                    exchange_df = exchange_screener.stock(params={"exchangeName": exchange}, limit=600)
                    if not exchange_df.empty:
                        print(f"✅ Loaded {len(exchange_df)} stocks from {exchange}")
                        return exchange_df
                    else:
                        print(f"⚠️ No data for {exchange}")
                        return None
                except Exception as e:
                    print(f"⚠️ Error loading {exchange}: {e}")
                    return None
            
            # Load all exchanges in parallel using global thread pool
            print("🚀 Loading exchanges in parallel...")
            start_time = time.time()
            
            executor = get_api_thread_pool()
            # Submit all tasks
            future_to_exchange = {executor.submit(load_exchange_data, exchange): exchange 
                                for exchange in exchanges}
                
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_exchange):
                exchange = future_to_exchange[future]
                try:
                    result = future.result()
                    if result is not None:
                        all_stocks.append(result)
                except Exception as e:
                    print(f"⚠️ Exception in {exchange}: {e}")
            
            parallel_time = time.time() - start_time
            print(f"⚡ Parallel loading completed in {parallel_time:.2f}s")
            
            if not all_stocks:
                raise Exception("No stock data could be loaded from any exchange")
                
            # Combine all dataframes
            screener_df = pd.concat(all_stocks, ignore_index=True)
            
            # Convert to list of dictionaries with proper null handling
            stocks = []
            inactive_count = 0
            
            for _, row in screener_df.iterrows():
                def safe_get(value):
                    if pd.isna(value) or value is None or (isinstance(value, float) and np.isnan(value)):
                        return None
                    return float(value) if isinstance(value, (int, float)) else value
                
                # Map exchange names to standard format first
                exchange_raw = str(row.get('exchange', ''))
                exchange_mapped = {
                    'HSX': 'HOSE',
                    'HNX': 'HNX', 
                    'UPCOM': 'UPCOM'
                }.get(exchange_raw, exchange_raw)
                
                # Check if stock appears to be inactive/delisted
                market_cap = safe_get(row.get('market_cap'))
                pe = safe_get(row.get('pe'))
                roe = safe_get(row.get('roe'))
                price = safe_get(row.get('price_near_realtime'))
                
                # Try to get price from alternative fields first
                if price is None:
                    price = safe_get(row.get('price', None))
                
                # Store ticker for historical price lookup if no price
                ticker = str(row.get('ticker', ''))
                if price is None and ticker and len(ticker) >= 3:
                    # Check if stock has any trading activity indicators
                    has_trading_activity = (
                        market_cap is not None or
                        pe is not None or
                        roe is not None or
                        safe_get(row.get('percent_price_vs_ma20')) is not None or
                        safe_get(row.get('avg_trading_value_20d')) is not None
                    )
                    
                    # Skip completely inactive stocks
                    if not has_trading_activity:
                        inactive_count += 1
                        continue
                
                # Parse indexes from JSON string
                indexes_raw = row.get('indexes', '[]')
                if isinstance(indexes_raw, str):
                    try:
                        indexes_list = json.loads(indexes_raw)
                    except:
                        indexes_list = []
                else:
                    indexes_list = indexes_raw if isinstance(indexes_raw, list) else []
                
                stock = {
                    'ticker': str(row.get('ticker', '')),
                    'exchange': exchange_mapped,
                    'industry': str(row.get('industry', '')) if pd.notna(row.get('industry')) else '',
                    'indexes': indexes_list,  # Add indexes field (as array)
                    'market_cap': market_cap,
                    'pe': pe,
                    'pb': safe_get(row.get('pb')),
                    'roe': roe,
                    'dividend_yield': safe_get(row.get('dividend_yield')),
                    'price_near_realtime': price,
                    'price_vs_sma20': str(row.get('price_vs_sma20', '')) if pd.notna(row.get('price_vs_sma20')) else '',
                    'revenue_growth_1y': safe_get(row.get('revenue_growth_1y')),
                    'eps_growth_1y': safe_get(row.get('eps_growth_1y')),
                    'is_active': price is not None or market_cap is not None,
                    # Add more fields from vnstock
                    'relative_strength_3d': safe_get(row.get('relative_strength_3d')),
                    'rel_strength_1m': safe_get(row.get('rel_strength_1m')),
                    'rel_strength_3m': safe_get(row.get('rel_strength_3m')),
                    'rel_strength_1y': safe_get(row.get('rel_strength_1y')),
                    'rsi14': safe_get(row.get('rsi14')),
                    'avg_trading_value_20d': safe_get(row.get('avg_trading_value_20d')),
                    'foreign_vol_pct': safe_get(row.get('foreign_buysell_20s')),
                    'foreign_transaction': str(row.get('foreign_transaction', '')) if pd.notna(row.get('foreign_transaction')) else '',
                    'price_growth_1w': safe_get(row.get('price_growth_1w')),
                    'price_growth_1m': safe_get(row.get('price_growth_1m')),
                    'prev_1d_growth_pct': safe_get(row.get('prev_1d_growth_pct')),
                    'prev_1y_growth_pct': safe_get(row.get('prev_1y_growth_pct')),
                    'gross_margin': safe_get(row.get('gross_margin')),
                    'net_margin': safe_get(row.get('net_margin')),
                    'roa': safe_get(row.get('roa')),
                    'eps': safe_get(row.get('eps')),
                    # Additional comprehensive fields
                    'ev_ebitda': safe_get(row.get('ev_ebitda')),
                    'ps': safe_get(row.get('ps')),
                    'beta': safe_get(row.get('beta')),
                    'alpha': safe_get(row.get('alpha')),
                    'peg_forward': safe_get(row.get('peg_forward')),
                    'peg_trailing': safe_get(row.get('peg_trailing')),
                    'revenue_growth_5y': safe_get(row.get('revenue_growth_5y')),
                    'eps_growth_5y': safe_get(row.get('eps_growth_5y')),
                    'stock_rating': safe_get(row.get('stock_rating')),
                    'business_operation': safe_get(row.get('business_operation')),
                    'business_model': safe_get(row.get('business_model')),
                    'financial_health': safe_get(row.get('financial_health')),
                    'active_buy_pct': safe_get(row.get('active_buy_pct')),
                    'strong_buy_pct': safe_get(row.get('strong_buy_pct')),
                    'doe': safe_get(row.get('doe')),
                    'free_transfer_rate': safe_get(row.get('free_transfer_rate')),
                    'corporate_percentage': safe_get(row.get('corporate_percentage')),
                    'ev': safe_get(row.get('ev')),
                    'quarter_revenue_growth': safe_get(row.get('quarter_revenue_growth')),
                    'quarter_income_growth': safe_get(row.get('quarter_income_growth')),
                    'quarterly_income': safe_get(row.get('quarterly_income')),
                    'quarterly_revenue': safe_get(row.get('quarterly_revenue')),
                    'eps_ttm_growth1_year': safe_get(row.get('eps_ttm_growth1_year')),
                    'eps_ttm_growth5_year': safe_get(row.get('eps_ttm_growth5_year')),
                    'vol_vs_sma20': safe_get(row.get('vol_vs_sma20')),
                    'price_vs_sma5': str(row.get('price_vs_sma5', '')) if pd.notna(row.get('price_vs_sma5')) else '',
                    'price_vs_sma10': str(row.get('price_vs_sma10', '')) if pd.notna(row.get('price_vs_sma10')) else '',
                    'price_vs_sma20': str(row.get('price_vs_sma20', '')) if pd.notna(row.get('price_vs_sma20')) else '',
                    'price_vs_sma50': str(row.get('price_vs_sma50', '')) if pd.notna(row.get('price_vs_sma50')) else '',
                    'price_vs_sma100': str(row.get('price_vs_sma100', '')) if pd.notna(row.get('price_vs_sma100')) else '',
                    'percent_price_vs_ma20': safe_get(row.get('percent_price_vs_ma20')),
                    'percent_price_vs_ma50': safe_get(row.get('percent_price_vs_ma50')),
                    'percent_price_vs_ma100': safe_get(row.get('percent_price_vs_ma100')),
                    'rsi14_status': str(row.get('rsi14_status', '')) if pd.notna(row.get('rsi14_status')) else '',
                    'tcbs_buy_sell_signal': str(row.get('tcbs_buy_sell_signal', '')) if pd.notna(row.get('tcbs_buy_sell_signal')) else '',
                    'bolling_band_signal': str(row.get('bolling_band_signal', '')) if pd.notna(row.get('bolling_band_signal')) else '',
                    'breakout': str(row.get('breakout', '')) if pd.notna(row.get('breakout')) else ''
                }
                stocks.append(stock)
            
            print(f"📊 Filtered out {inactive_count} inactive/delisted stocks")
            
            # Sort stocks: prioritize those with prices, then by market cap
            stocks.sort(key=lambda x: (
                x['price_near_realtime'] is None,  # False (has price) comes first
                -(x['market_cap'] or 0)  # Higher market cap first
            ))
            
            # Calculate statistics
            with_price = sum(1 for stock in stocks if stock['price_near_realtime'] is not None)
            price_coverage = round(with_price / len(stocks) * 100, 1) if stocks else 0
            
            return jsonify({
                'success': True,
                'data': stocks,
                'count': len(stocks),
                'filtered_out': inactive_count,
                'with_price': with_price,
                'price_coverage': price_coverage,
                'message': f'Loaded {len(stocks)} active stocks ({price_coverage}% have prices, filtered out {inactive_count} inactive stocks)'
            })
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"❌ Screener API Error: {error_details}")
            
            return jsonify({
                'success': False,
                'error': str(e),
                'message': 'Failed to load screener data',
                'details': error_details
            }), 500

    @screener_bp.route('/stock/<symbol>/price')
    def get_stock_price(symbol):
        """Get quick price for a single stock"""
        try:
            price_cache = get_price_cache_service()
            price = price_cache.get_price(symbol.upper())

            if price is not None:
                return jsonify({
                    'success': True,
                    'price': price,
                    'symbol': symbol.upper()
                })

            return jsonify({
                'success': False,
                'error': 'No price data available'
            })

        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            })

    @screener_bp.route('/stock/<symbol>/single')
    def get_single_stock_data(symbol):
        """API endpoint to get single stock data WITHOUT peers (for adding to comparison)"""
        try:
            import pandas as pd
            import numpy as np
            
            symbol = symbol.upper()
            print(f"🔍 Getting single stock data for {symbol}...")
            
            # Get cached screener data
            screener_df = get_cached_screener_data()
            
            if screener_df is None:
                raise Exception("No stock data could be loaded")
            
            # Find the target stock
            stock_row = screener_df[screener_df['ticker'] == symbol]
            
            if stock_row.empty:
                return jsonify({
                    'success': False,
                    'message': f'Stock {symbol} not found'
                }), 404
            
            # Convert to dict
            def safe_get(value):
                if pd.isna(value) or value is None or (isinstance(value, float) and np.isnan(value)):
                    return None
                return value
            
            row = stock_row.iloc[0]
            target_stock = {
                'ticker': safe_get(row.get('ticker')),
                'company_name': safe_get(row.get('company_name')),
                'exchange': safe_get(row.get('exchange')),
                'industry': safe_get(row.get('industry')),
                'market_cap': safe_get(row.get('market_cap')),
                'price_near_realtime': safe_get(row.get('price_near_realtime')),
                'pe': safe_get(row.get('pe')),
                'pb': safe_get(row.get('pb')),
                'roe': safe_get(row.get('roe')),
                'roa': safe_get(row.get('roa')),
                'gross_margin': safe_get(row.get('gross_margin')),
                'net_margin': safe_get(row.get('net_margin')),
                'dividend_yield': safe_get(row.get('dividend_yield'))
            }
            
            # Enhance with cached ratios
            cached_ratios = data_manager.get_financial_ratios(symbol)
            if cached_ratios and len(cached_ratios) > 0:
                yearly_data = [r for r in cached_ratios if r.get('period_type') == 'year']
                quarterly_data = [r for r in cached_ratios if r.get('period_type') == 'quarter']
                
                if yearly_data:
                    target_stock.update(yearly_data[0])
                elif quarterly_data:
                    target_stock.update(quarterly_data[0])
                
                target_stock['yearly_ratios'] = yearly_data[:5]
                target_stock['quarterly_ratios'] = quarterly_data[:20]
            
            print(f"✅ Returning single stock: {symbol}")
            
            return jsonify({
                'success': True,
                'data': target_stock
            })
            
        except Exception as e:
            print(f"❌ Error getting single stock data: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @screener_bp.route('/stock/<symbol>/compare')
    def get_stock_comparison(symbol):
        """API endpoint to get stock comparison with top 4 peers - uses financial_ratios DB"""
        try:
            import pandas as pd
            import numpy as np
            import sqlite3
            
            symbol = symbol.upper()
            print(f"🔍 Loading comparison data for {symbol} from financial_ratios DB...")
            
            # Connect to financial database
            db_path = 'fund_data.db'
            conn = sqlite3.connect(db_path)
            
            # Get screener data for basic info (industry, current price)
            screener_df = get_cached_screener_data()
            if screener_df is None:
                raise Exception("No screener data available")
            
            # Find target stock in screener
            stock_row = screener_df[screener_df['ticker'] == symbol]
            if stock_row.empty:
                conn.close()
                return jsonify({
                    'success': False,
                    'message': f'Stock {symbol} not found'
                }), 404
            
            # Get industry for finding peers
            target_industry = stock_row.iloc[0].get('industry')
            if not target_industry or pd.isna(target_industry):
                conn.close()
                return jsonify({
                    'success': False,
                    'message': f'Industry information not available for {symbol}'
                }), 400
            
            # Find peers in same industry from screener
            peers_df = screener_df[
                (screener_df['industry'] == target_industry) & 
                (screener_df['ticker'] != symbol)
            ].copy()
            peers_df = peers_df.sort_values('market_cap', ascending=False).head(4)
            
            # Get all tickers (target + peers)
            all_tickers = [symbol] + peers_df['ticker'].tolist()
            tickers_str = ','.join([f"'{t}'" for t in all_tickers])
            
            # Query financial_ratios for all tickers with historical data
            # Note: Using explicit column list to avoid any column name issues
            query = f"""
            SELECT 
                symbol, year, quarter, period_type,
                pe_ratio, pb_ratio, ps_ratio, pcf_ratio, ev_ebitda,
                eps, bvps, market_cap, shares_outstanding,
                roe, roa, roic, gross_margin, net_margin, ebit_margin,
                ebitda, ebit, dividend_yield,
                current_ratio, quick_ratio, cash_ratio, interest_coverage,
                financial_leverage, total_debt_to_equity,
                asset_turnover, fixed_asset_turnover, inventory_turnover,
                days_receivable, days_inventory, days_payable, cash_cycle,
                equity_to_charter_capital
            FROM financial_ratios 
            WHERE symbol IN ({tickers_str})
            ORDER BY symbol, year DESC, quarter DESC
            """
            financial_df = pd.read_sql_query(query, conn)
            conn.close()
            
            print(f"✅ Found {len(financial_df)} financial records for {len(all_tickers)} stocks")
            print(f"📊 DataFrame columns: {list(financial_df.columns)}")
            if len(financial_df) > 0:
                print(f"📊 First row sample: market_cap={financial_df.iloc[0].get('market_cap')}, shares_outstanding={financial_df.iloc[0].get('shares_outstanding')}")
            
            # Helper function
            def safe_get(value):
                if pd.isna(value) or value is None or (isinstance(value, float) and np.isnan(value)):
                    return None
                return value
            
            # Build comparison data for each stock
            comparison_data = []
            for ticker in all_tickers:
                # Get basic info from screener
                screener_stock = screener_df[screener_df['ticker'] == ticker].iloc[0] if ticker in screener_df['ticker'].values else None
                if screener_stock is None:
                    continue
                
                # Get ALL financial records for this stock (for quarterly/yearly data)
                stock_financials = financial_df[financial_df['symbol'] == ticker]
                
                # Get latest record for current values
                latest_financial = stock_financials.iloc[0] if len(stock_financials) > 0 else None
                
                # Build quarterly_ratios list
                quarterly_ratios = []
                yearly_ratios = []
                for idx, fin_row in stock_financials.iterrows():
                    # Debug first row
                    if idx == stock_financials.index[0]:
                        print(f"  🔍 {ticker} first row: market_cap in row? {'market_cap' in fin_row}, value={fin_row.get('market_cap')}")
                    
                    fin_dict = {
                        'year': int(fin_row['year']) if not pd.isna(fin_row['year']) else None,
                        'quarter': int(fin_row['quarter']) if not pd.isna(fin_row['quarter']) else None,
                        'market_cap': safe_get(fin_row.get('market_cap')),
                        'shares_outstanding': safe_get(fin_row.get('shares_outstanding')),
                        'pe': safe_get(fin_row['pe_ratio']),
                        'pb': safe_get(fin_row['pb_ratio']),
                        'ps_ratio': safe_get(fin_row['ps_ratio']),
                        'pcf_ratio': safe_get(fin_row['pcf_ratio']),
                        'ev_ebitda': safe_get(fin_row['ev_ebitda']),
                        'eps': safe_get(fin_row['eps']),
                        'bvps': safe_get(fin_row['bvps']),
                        'roe': safe_get(fin_row['roe']),
                        'roa': safe_get(fin_row['roa']),
                        'roic': safe_get(fin_row['roic']),
                        'gross_margin': safe_get(fin_row['gross_margin']),
                        'net_margin': safe_get(fin_row['net_margin']),
                        'ebit_margin': safe_get(fin_row['ebit_margin']),
                        'ebit': safe_get(fin_row['ebit']),
                        'ebitda': safe_get(fin_row['ebitda']),
                        'dividend_yield': safe_get(fin_row['dividend_yield']),
                        'current_ratio': safe_get(fin_row['current_ratio']),
                        'quick_ratio': safe_get(fin_row['quick_ratio']),
                        'cash_ratio': safe_get(fin_row['cash_ratio']),
                        'financial_leverage': safe_get(fin_row['financial_leverage']),
                        'interest_coverage': safe_get(fin_row['interest_coverage']),
                        'asset_turnover': safe_get(fin_row['asset_turnover']),
                        'fixed_asset_turnover': safe_get(fin_row['fixed_asset_turnover']),
                        'inventory_turnover': safe_get(fin_row['inventory_turnover']),
                        'days_receivable': safe_get(fin_row['days_receivable']),
                        'days_inventory': safe_get(fin_row['days_inventory']),
                        'days_payable': safe_get(fin_row['days_payable']),
                        'cash_cycle': safe_get(fin_row['cash_cycle']),
                        'total_debt_to_equity': safe_get(fin_row['total_debt_to_equity']),
                        'equity_to_charter_capital': safe_get(fin_row['equity_to_charter_capital'])
                    }
                    
                    if fin_dict['quarter']:
                        quarterly_ratios.append(fin_dict)
                    else:
                        yearly_ratios.append(fin_dict)
                
                # Build stock object with basic info from screener + financials from DB
                stock = {
                    # Basic info from screener
                    'ticker': ticker,
                    'company_name': safe_get(screener_stock.get('company_name')),
                    'exchange': safe_get(screener_stock.get('exchange')),
                    'industry': safe_get(screener_stock.get('industry')),
                    'market_cap': safe_get(latest_financial['market_cap']) if latest_financial is not None else safe_get(screener_stock.get('market_cap')),
                    'price_near_realtime': safe_get(screener_stock.get('price_near_realtime')),
                    'shares_outstanding': safe_get(latest_financial['shares_outstanding']) if latest_financial is not None else None,
                    
                    # Add quarterly/yearly historical data
                    'quarterly_ratios': quarterly_ratios,
                    'yearly_ratios': yearly_ratios,
                    
                    # Latest financial ratios from DB (if available)
                    'pe': safe_get(latest_financial['pe_ratio']) if latest_financial is not None else safe_get(screener_stock.get('pe')),
                    'pb': safe_get(latest_financial['pb_ratio']) if latest_financial is not None else safe_get(screener_stock.get('pb')),
                    'ps_ratio': safe_get(latest_financial['ps_ratio']) if latest_financial is not None else None,
                    'pcf_ratio': safe_get(latest_financial['pcf_ratio']) if latest_financial is not None else None,
                    'ev_ebitda': safe_get(latest_financial['ev_ebitda']) if latest_financial is not None else None,
                    'eps': safe_get(latest_financial['eps']) if latest_financial is not None else safe_get(screener_stock.get('eps')),
                    'bvps': safe_get(latest_financial['bvps']) if latest_financial is not None else None,
                    
                    # Profitability from DB
                    'roe': safe_get(latest_financial['roe']) if latest_financial is not None else safe_get(screener_stock.get('roe')),
                    'roa': safe_get(latest_financial['roa']) if latest_financial is not None else safe_get(screener_stock.get('roa')),
                    'roic': safe_get(latest_financial['roic']) if latest_financial is not None else None,
                    'gross_margin': safe_get(latest_financial['gross_margin']) if latest_financial is not None else safe_get(screener_stock.get('gross_margin')),
                    'net_margin': safe_get(latest_financial['net_margin']) if latest_financial is not None else safe_get(screener_stock.get('net_margin')),
                    'ebit_margin': safe_get(latest_financial['ebit_margin']) if latest_financial is not None else None,
                    'ebit': safe_get(latest_financial['ebit']) if latest_financial is not None else None,
                    'ebitda': safe_get(latest_financial['ebitda']) if latest_financial is not None else None,
                    'dividend_yield': safe_get(latest_financial['dividend_yield']) if latest_financial is not None else safe_get(screener_stock.get('dividend_yield')),
                    
                    # Financial health from DB
                    'current_ratio': safe_get(latest_financial['current_ratio']) if latest_financial is not None else None,
                    'quick_ratio': safe_get(latest_financial['quick_ratio']) if latest_financial is not None else None,
                    'cash_ratio': safe_get(latest_financial['cash_ratio']) if latest_financial is not None else None,
                    'financial_leverage': safe_get(latest_financial['financial_leverage']) if latest_financial is not None else None,
                    'interest_coverage': safe_get(latest_financial['interest_coverage']) if latest_financial is not None else None,
                    
                    # Efficiency from DB
                    'asset_turnover': safe_get(latest_financial['asset_turnover']) if latest_financial is not None else None,
                    'fixed_asset_turnover': safe_get(latest_financial['fixed_asset_turnover']) if latest_financial is not None else None,
                    'inventory_turnover': safe_get(latest_financial['inventory_turnover']) if latest_financial is not None else None,
                    'days_receivable': safe_get(latest_financial['days_receivable']) if latest_financial is not None else None,
                    'days_inventory': safe_get(latest_financial['days_inventory']) if latest_financial is not None else None,
                    'days_payable': safe_get(latest_financial['days_payable']) if latest_financial is not None else None,
                    'cash_cycle': safe_get(latest_financial['cash_cycle']) if latest_financial is not None else None,
                    'total_debt_to_equity': safe_get(latest_financial['total_debt_to_equity']) if latest_financial is not None else None,
                    'equity_to_charter_capital': safe_get(latest_financial['equity_to_charter_capital']) if latest_financial is not None else None,
                    
                    # Growth from screener
                    'revenue_growth_1y': safe_get(screener_stock.get('revenue_growth_1y')),
                    'eps_growth_1y': safe_get(screener_stock.get('eps_growth_1y'))
                }
                comparison_data.append(stock)
            
            # Split into target and peers for frontend
            target_stock = comparison_data[0] if comparison_data else None
            peer_stocks = comparison_data[1:] if len(comparison_data) > 1 else []
            
            return jsonify({
                'success': True,
                'data': {
                    'target': target_stock,
                    'peers': peer_stocks
                },
                'stocks': comparison_data,  # Keep for backward compatibility
                'target_symbol': symbol,
                'industry': target_industry
            })
            
        except Exception as e:
            print(f"❌ Error getting comparison data: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    # ============================================
    # ADMIN ENDPOINTS - Cache Management
    # ============================================
    
    @screener_bp.route('/admin/screener-cache/stats', methods=['GET'])
    def admin_cache_stats():
        """Get screener cache statistics (admin only)"""
        try:
            import os
            # TODO: Add admin authentication
            admin_key = request.headers.get('X-Admin-Key')
            if admin_key != os.getenv('ADMIN_KEY', 'your-secret-admin-key'):
                return jsonify({'error': 'Unauthorized'}), 401
            
            if DB_CACHE_ENABLED:
                stats = screener_cache_db.get_cache_stats()
                cache_type = 'database'
            else:
                # In-memory cache stats
                current_time = time.time()
                cache_valid = (screener_cache['data'] is not None and 
                             screener_cache['timestamp'] is not None and
                             current_time - screener_cache['timestamp'] < screener_cache['ttl'])
                
                if cache_valid:
                    age_minutes = (current_time - screener_cache['timestamp']) / 60
                    remaining_minutes = (screener_cache['ttl'] - (current_time - screener_cache['timestamp'])) / 60
                    stock_count = len(screener_cache['data']) if screener_cache['data'] is not None else 0
                else:
                    age_minutes = None
                    remaining_minutes = 0
                    stock_count = 0
                
                stats = {
                    'cache_valid': cache_valid,
                    'age_minutes': age_minutes,
                    'remaining_minutes': remaining_minutes,
                    'stock_count': stock_count,
                    'ttl_minutes': screener_cache['ttl'] / 60
                }
                cache_type = 'in-memory'
            
            return jsonify({
                'success': True,
                'cache_type': cache_type,
                'stats': stats,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌ Admin cache stats error: {e}")
            return jsonify({'error': str(e)}), 500
    
    
    @screener_bp.route('/admin/screener-cache/refresh', methods=['POST'])
    def admin_cache_refresh():
        """Force refresh screener cache (admin only)"""
        try:
            import os
            # TODO: Add admin authentication
            admin_key = request.headers.get('X-Admin-Key')
            if admin_key != os.getenv('ADMIN_KEY', 'your-secret-admin-key'):
                return jsonify({'error': 'Unauthorized'}), 401
            
            # Clear cache
            if DB_CACHE_ENABLED:
                screener_cache_db.force_refresh()
            else:
                screener_cache['data'] = None
                screener_cache['timestamp'] = None
            
            # Fetch fresh data
            fresh_data = get_cached_screener_data()
            
            if fresh_data is not None:
                return jsonify({
                    'success': True,
                    'message': 'Cache refreshed successfully',
                    'stock_count': len(fresh_data),
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Failed to fetch fresh data'
                }), 500
                
        except Exception as e:
            logger.error(f"❌ Admin cache refresh error: {e}")
            return jsonify({'error': str(e)}), 500
    
    @screener_bp.route('/cache/clear', methods=['POST'])
    def clear_cache():
        """Clear screener cache - NO AUTH for development"""
        try:
            # Clear cache
            if DB_CACHE_ENABLED:
                cleared = screener_cache_db.clear_all()
                logger.info(f"🧹 Cleared {cleared} cache entries")
            else:
                screener_cache['data'] = None
                screener_cache['timestamp'] = None
            
            return jsonify({
                'success': True,
                'message': 'Cache cleared. Next API call will fetch fresh data with indexes.',
                'timestamp': datetime.now().isoformat()
            })
                
        except Exception as e:
            logger.error(f"❌ Clear cache error: {e}")
            return jsonify({'error': str(e)}), 500
    
    return screener_bp
