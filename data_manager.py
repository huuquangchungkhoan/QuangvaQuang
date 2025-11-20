import sqlite3
import logging
from datetime import datetime
import time
import json
import pandas as pd

# Import vnstock with error handling for circular import
try:
    import vnstock
except Exception as e:
    logging.warning(f"⚠️ vnstock import warning: {e}")
    vnstock = None

from utils.cache_utils import get_company_cache

class FundDataManager:
    def __init__(self, db_path='fund_data.db'):
        self.db_path = db_path
        try:
            from vnstock3 import Vnstock
            vnstock = Vnstock().stock(source='VCI')
            self.fund_api = vnstock.fund
        except ImportError:
            try:
                from vnstock import Fund
                self.fund_api = Fund()
            except ImportError:
                print("Please install vnstock3: pip install vnstock3 --upgrade")
                self.fund_api = None
        self.setup_logging()
        self.company_cache = get_company_cache()

    def setup_logging(self):
        """Setup logging for the data manager"""
        import os
        handlers = [logging.StreamHandler()]
        
        # Only add file handler if we have write permissions
        try:
            log_file = os.getenv('LOG_FILE', 'fund_app.log')
            handlers.append(logging.FileHandler(log_file))
        except (PermissionError, OSError):
            # Skip file logging in production/restricted environments
            pass
            
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=handlers
        )
        self.logger = logging.getLogger(__name__)

    def _company_cache_key(self, symbol: str, suffix: str) -> str:
        return f"{suffix}:{symbol.upper()}"

    def initialize_db(self):
        """Initialize the database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create funds table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS funds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT UNIQUE NOT NULL,
                fund_name TEXT,
                fund_type TEXT,
                management_company TEXT,
                nav REAL,
                nav_date TEXT,
                total_assets REAL,
                inception_date TEXT,
                expense_ratio REAL,
                min_investment REAL,
                data_json TEXT,
                updated_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create holdings table for caching top holdings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                stock_code TEXT,
                industry TEXT,
                net_asset_percent REAL,
                type_asset TEXT,
                update_at TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fund_code) REFERENCES funds (fund_code)
            )
        ''')
        
        # Create asset_allocations table for caching asset allocation data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS asset_allocations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                asset_type TEXT,
                asset_percent REAL,
                short_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fund_code) REFERENCES funds (fund_code)
            )
        ''')
        
        # Create metadata table for storing last update time
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create vnindex cache table (stores OHLCV)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vnindex_cache (
                date TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Create financial_ratios table for caching stock financial ratios
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_ratios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                year INTEGER,
                quarter INTEGER,
                period_type TEXT,
                pe_ratio REAL,
                pb_ratio REAL,
                ps_ratio REAL,
                pcf_ratio REAL,
                ev_ebitda REAL,
                eps REAL,
                bvps REAL,
                market_cap REAL,
                shares_outstanding REAL,
                roe REAL,
                roa REAL,
                roic REAL,
                gross_margin REAL,
                net_margin REAL,
                ebit_margin REAL,
                ebitda REAL,
                ebit REAL,
                dividend_yield REAL,
                current_ratio REAL,
                quick_ratio REAL,
                cash_ratio REAL,
                interest_coverage REAL,
                financial_leverage REAL,
                total_debt_to_equity REAL,
                asset_turnover REAL,
                fixed_asset_turnover REAL,
                inventory_turnover REAL,
                days_receivable REAL,
                days_inventory REAL,
                days_payable REAL,
                cash_cycle REAL,
                equity_to_charter_capital REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create index for faster queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_financial_ratios_symbol 
            ON financial_ratios (symbol)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_financial_ratios_period 
            ON financial_ratios (symbol, period_type, year, quarter)
        ''')
        
        conn.commit()
        conn.close()
        self.logger.info("Database initialized successfully")
    
    def fetch_fund_data(self):
        """Fetch fund data from vnstock API"""
        try:
            self.logger.info("Fetching fund data from vnstock API...")
            fund_list = self.fund_api.listing()
            
            if fund_list is not None and not fund_list.empty:
                self.logger.info(f"Retrieved {len(fund_list)} funds from API")
                return fund_list
            else:
                self.logger.warning("No fund data retrieved from API")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching fund data: {str(e)}")
            raise
    
    def save_fund_data(self, fund_df):
        """Save fund data to local SQLite database"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            updated_count = 0
            for _, row in fund_df.iterrows():
                # Prepare fund data - mapping vnstock 3.2.6 fields
                fund_data = {
                    'fund_code': row.get('fund_code', ''),
                    'fund_name': row.get('name', ''),
                    'fund_type': row.get('fund_type', ''),
                    'management_company': row.get('fund_owner_name', ''),
                    'nav': row.get('nav', 0),
                    'nav_date': row.get('nav_update_at', ''),
                    'total_assets': 0,  # Not available in vnstock 3.2.6
                    'inception_date': row.get('inception_date', ''),
                    'expense_ratio': row.get('management_fee', 0),
                    'min_investment': 0,  # Not available in vnstock 3.2.6
                    'data_json': json.dumps(row.to_dict(), ensure_ascii=False, default=str).replace('NaN', 'null'),
                    'updated_at': datetime.now().isoformat()
                }
                
                # Insert or update fund data
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO funds 
                    (fund_code, fund_name, fund_type, management_company, nav, nav_date, 
                     total_assets, inception_date, expense_ratio, min_investment, data_json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    fund_data['fund_code'], fund_data['fund_name'], fund_data['fund_type'],
                    fund_data['management_company'], fund_data['nav'], fund_data['nav_date'],
                    fund_data['total_assets'], fund_data['inception_date'], fund_data['expense_ratio'],
                    fund_data['min_investment'], fund_data['data_json'], fund_data['updated_at']
                ))
                updated_count += 1
            
            # Update metadata
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value, updated_at)
                VALUES (?, ?, ?)
            ''', ('last_update', datetime.now().isoformat(), datetime.now().isoformat()))
            
            conn.commit()
            self.logger.info(f"Saved {updated_count} funds to database")
            return updated_count
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error saving fund data: {str(e)}")
            raise
        finally:
            conn.close()
    
    def refresh_data(self):
        """Refresh fund data from API and save to database"""
        fund_df = self.fetch_fund_data()
        if not fund_df.empty:
            updated_count = self.save_fund_data(fund_df)
            return {
                'updated_count': updated_count,
                'timestamp': datetime.now().isoformat()
            }
        else:
            return {
                'updated_count': 0,
                'timestamp': datetime.now().isoformat()
            }
    
    def get_funds(self, search_term='', fund_type=''):
        """Get funds from database with optional filtering"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT fund_code, fund_name, fund_type, management_company, nav, nav_date,
                   total_assets, inception_date, expense_ratio, min_investment, updated_at, data_json
            FROM funds
            WHERE 1=1
        '''
        params = []
        
        if search_term:
            query += ' AND (fund_name LIKE ? OR fund_code LIKE ?)'
            params.extend([f'%{search_term}%', f'%{search_term}%'])
        
        if fund_type:
            query += ' AND fund_type = ?'
            params.append(fund_type)
        
        query += ' ORDER BY fund_name'
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
            records = df.to_dict('records')
            
            # Parse JSON data for each record
            for record in records:
                if record.get('data_json'):
                    try:
                        record['raw_data'] = json.loads(record['data_json'])
                    except json.JSONDecodeError:
                        record['raw_data'] = {}
                else:
                    record['raw_data'] = {}
            
            return records
        except Exception as e:
            self.logger.error(f"Error retrieving funds: {str(e)}")
            return []
        finally:
            conn.close()
    
    def get_fund_by_code(self, fund_code):
        """Get detailed information for a specific fund by code"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM funds WHERE fund_code = ?
                ''', (fund_code,))
                
                row = cursor.fetchone()
                if row:
                    columns = [description[0] for description in cursor.description]
                    fund_data = dict(zip(columns, row))
                    
                    # Parse the JSON data
                    if fund_data.get('data_json'):
                        try:
                            fund_data['raw_data'] = json.loads(fund_data['data_json'])
                        except json.JSONDecodeError:
                            self.logger.warning(f"Failed to parse JSON data for fund {fund_code}")
                            fund_data['raw_data'] = {}
                    else:
                        fund_data['raw_data'] = {}
                    
                    return fund_data
                else:
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error getting fund by code {fund_code}: {str(e)}")
            return None

    def get_fund_nav_report(self, fund_code, sampling=None):
        """Get NAV history for a specific fund with optional monthly sampling using short_name - WITH CACHING"""
        try:
            from datetime import datetime, timedelta
            
            # Get fund details to extract short_name
            fund_info = self.get_fund_by_code(fund_code)
            if not fund_info or 'raw_data' not in fund_info:
                self.logger.error(f"Fund {fund_code} not found in database")
                return []
            
            short_name = fund_info['raw_data'].get('short_name')
            if not short_name:
                self.logger.error(f"No short_name found for fund {fund_code}")
                return []
            
            # 🔥 CHECK CACHE FIRST
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if we have recent cache (last 24 hours)
            cursor.execute('''
                SELECT date, nav, created_at 
                FROM fund_history 
                WHERE fund_code = ? 
                ORDER BY date DESC 
                LIMIT 1
            ''', (fund_code,))
            
            cache_row = cursor.fetchone()
            cache_is_fresh = False
            
            if cache_row:
                # Check if cache is less than 24 hours old
                created_at_str = cache_row[2]
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str)
                        cache_age = datetime.now() - created_at
                        cache_is_fresh = cache_age.total_seconds() < 86400  # 24 hours
                    except:
                        pass
            
            # If cache is fresh, use it
            if cache_is_fresh:
                self.logger.info(f"✅ Using cached NAV data for {fund_code} (fresh)")
                cursor.execute('''
                    SELECT date, nav 
                    FROM fund_history 
                    WHERE fund_code = ? 
                    ORDER BY date ASC
                ''', (fund_code,))
                
                records = [{'date': row[0], 'nav': row[1]} for row in cursor.fetchall()]
                conn.close()
                
                # Apply monthly sampling if requested
                if sampling == 'monthly':
                    records = self._apply_monthly_sampling(records)
                
                return records
            
            conn.close()
            
            # 🔥 CACHE MISS OR OLD - FETCH FROM API
            self.logger.info(f"📡 Fetching NAV report for {fund_code} from vnstock API (cache miss)")
            
            # Get NAV data from vnstock API
            nav_data = self.fund_api.details.nav_report(short_name)
            
            if nav_data.empty:
                self.logger.warning(f"No NAV data returned for {short_name}")
                return []
            
            # Convert to records
            records = nav_data.to_dict('records')
            
            # 🔥 SAVE TO CACHE
            self._save_nav_to_cache(fund_code, records)
            
            # Apply monthly sampling if requested
            if sampling == 'monthly':
                records = self._apply_monthly_sampling(records)
            
            return records
            
        except Exception as e:
            self.logger.error(f"Error getting NAV report for {fund_code}: {str(e)}")
            return []
    
    def _save_nav_to_cache(self, fund_code, records):
        """Save NAV records to database cache"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Delete old data for this fund
            cursor.execute('DELETE FROM fund_history WHERE fund_code = ?', (fund_code,))
            
            # Insert new data using INSERT OR REPLACE to handle duplicates
            for record in records:
                # vnstock returns 'nav_per_unit' field, not 'nav'
                nav_value = record.get('nav_per_unit') or record.get('nav')
                cursor.execute('''
                    INSERT OR REPLACE INTO fund_history (fund_code, date, nav, created_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (fund_code, record.get('date'), nav_value))
            
            conn.commit()
            self.logger.info(f"💾 Cached {len(records)} NAV records for {fund_code}")
            
        except Exception as e:
            self.logger.error(f"Error saving NAV cache for {fund_code}: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
    
    def _apply_monthly_sampling(self, data):
        """Apply monthly sampling to data - take first trading day of each month with NAV data"""
        import pandas as pd
        
        if not data:
            return data
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date')
        
        # Group by year-month and take first record with non-null NAV
        df['year_month'] = df['date'].dt.to_period('M')
        
        # Check if 'nav' column exists, if not try 'nav_per_unit'
        nav_col = 'nav' if 'nav' in df.columns else ('nav_per_unit' if 'nav_per_unit' in df.columns else None)
        
        if not nav_col:
            # No NAV column found, return original data
            self.logger.warning("No NAV column found in data")
            return data
        
        # Filter out rows with null/NaN nav before grouping
        df_with_nav = df[df[nav_col].notna()].copy()
        
        if df_with_nav.empty:
            # If no records with NAV, return original data
            return data
        
        monthly_data = df_with_nav.groupby('year_month').first().reset_index()
        
        # Drop the helper column
        monthly_data = monthly_data.drop('year_month', axis=1)
        
        # Ensure 'nav' column exists in output (rename nav_per_unit if needed)
        if 'nav_per_unit' in monthly_data.columns and 'nav' not in monthly_data.columns:
            monthly_data['nav'] = monthly_data['nav_per_unit']
        
        # Convert back to records
        return monthly_data.to_dict('records')
    
    def get_vnindex_data(self, start_date=None, end_date=None, sampling='monthly'):
        """Get VN-Index historical data"""
        try:
            import vnstock
            from datetime import datetime, timedelta
            
            # Default to last 2 years if no dates provided
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
            
            self.logger.info(f"Getting VN-Index data from {start_date} to {end_date}")
            
            # Get VN-Index data using Quote class
            quote = vnstock.Quote(symbol='VNINDEX', source='VCI')
            vnindex_data = quote.history(
                start=start_date,
                end=end_date,
                interval='1D'
            )
            
            if vnindex_data.empty:
                self.logger.warning("No VN-Index data returned")
                return []
            
            # Convert to records format similar to NAV data
            records = []
            for _, row in vnindex_data.iterrows():
                records.append({
                    'date': row['time'],  # Use 'time' column as date
                    'close': row['close'],
                    'symbol': 'VNINDEX'
                })
            
            # Apply monthly sampling if requested
            if sampling == 'monthly':
                records = self._apply_monthly_sampling_index(records)
            
            return records
            
        except Exception as e:
            self.logger.error(f"Error getting VN-Index data: {str(e)}")
            return []
    
    def _apply_monthly_sampling_index(self, data):
        """Apply monthly sampling to index data"""
        import pandas as pd
        
        if not data:
            return data
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date')
        
        # Group by year-month and take first record of each month
        df['year_month'] = df['date'].dt.to_period('M')
        monthly_data = df.groupby('year_month').first().reset_index()
        
        # Drop the helper column
        monthly_data = monthly_data.drop('year_month', axis=1)
        
        # Convert back to records
        return monthly_data.to_dict('records')
    
    def get_vnindex_data_for_dates(self, fund_data_points):
        """Get VN-Index data for specific dates to match fund data points - WITH CACHING"""
        try:
            import vnstock
            import pandas as pd
            from datetime import datetime, timedelta
            
            if not fund_data_points:
                return []
            
            # Get date range for all fund data points
            fund_dates = []
            for fund_point in fund_data_points:
                try:
                    if isinstance(fund_point['date'], str):
                        if 'GMT' in fund_point['date']:
                            fund_date = datetime.strptime(fund_point['date'], '%a, %d %b %Y %H:%M:%S %Z')
                        else:
                            fund_date = datetime.strptime(fund_point['date'], '%Y-%m-%d')
                    else:
                        fund_date = fund_point['date']
                    fund_dates.append((fund_point, fund_date))
                except Exception as e:
                    self.logger.warning(f"Error parsing date {fund_point['date']}: {str(e)}")
                    continue
            
            if not fund_dates:
                return []
            
            # Get VN-Index data for entire range
            start_date_dt = min(date for _, date in fund_dates)
            end_date_dt = max(date for _, date in fund_dates)
            
            # Add buffer
            start_date = (start_date_dt - timedelta(days=10)).strftime('%Y-%m-%d')
            end_date = (end_date_dt + timedelta(days=10)).strftime('%Y-%m-%d')
            
            # 🔥 CHECK CACHE FIRST
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT date, close 
                FROM vnindex_cache 
                WHERE date >= ? AND date <= ?
                AND datetime(created_at) > datetime('now', '-7 days')
                ORDER BY date
            ''', (start_date, end_date))
            
            cached_rows = cursor.fetchall()
            conn.close()
            
            # If we have enough cached data
            # Note: Stock market only has ~250 trading days/year (not 365)
            # So we use 60% threshold to account for weekends and holidays
            required_days = (end_date_dt - start_date_dt).days + 20  # Include buffer
            cache_coverage = len(cached_rows) / max(required_days, 1)
            
            self.logger.info(f"🔍 VN-Index cache check: {len(cached_rows)} cached rows, {required_days} required days, {cache_coverage*100:.0f}% coverage (range: {start_date} to {end_date})")
            
            # Use 60% threshold instead of 80% because market only trades ~250 days/year
            if cache_coverage >= 0.60:
                self.logger.info(f"✅ Using cached VN-Index data: {len(cached_rows)} records ({cache_coverage*100:.0f}% coverage)")
                vnindex_dict = {}
                for row in cached_rows:
                    date_obj = datetime.strptime(row[0], '%Y-%m-%d').date()
                    vnindex_dict[date_obj] = row[1]
            else:
                # 🔥 CACHE MISS - FETCH FROM API
                self.logger.info(f"📡 Fetching VN-Index data from API (cache coverage: {cache_coverage*100:.0f}%)")
                
                quote = vnstock.Quote(symbol='VNINDEX', source='VCI')
                vnindex_data = quote.history(
                    start=start_date,
                    end=end_date,
                    interval='1D'
                )
                
                if vnindex_data.empty:
                    self.logger.warning("No VN-Index data returned")
                    return []
                
                # Convert VN-Index data to dict for fast lookup
                vnindex_dict = {}
                for _, row in vnindex_data.iterrows():
                    row_date = pd.to_datetime(row['time']).date()
                    vnindex_dict[row_date] = row['close']
                
                # 🔥 SAVE TO CACHE
                self._save_vnindex_to_cache(vnindex_dict)
            
            # Match fund dates with VN-Index data
            vnindex_records = []
            for fund_point, fund_date in fund_dates:
                fund_date_only = fund_date.date()
                
                # Find closest VN-Index date
                closest_date = None
                min_diff = float('inf')
                
                for vn_date in vnindex_dict.keys():
                    diff = abs((vn_date - fund_date_only).days)
                    if diff < min_diff:
                        min_diff = diff
                        closest_date = vn_date
                
                if closest_date and min_diff <= 7:  # Within 7 days
                    vnindex_records.append({
                        'date': fund_point['date'],  # Use same date as fund for alignment
                        'close': vnindex_dict[closest_date],
                        'symbol': 'VNINDEX'
                    })
                else:
                    self.logger.warning(f"No close VN-Index data found for {fund_date_only}")
            
            self.logger.info(f"Retrieved {len(vnindex_records)} VN-Index data points")
            return vnindex_records
            
        except Exception as e:
            self.logger.error(f"Error getting VN-Index data for dates: {str(e)}")
            return []
    
    def _save_vnindex_to_cache(self, vnindex_dict):
        """Save VN-Index data to database cache"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Insert or replace VN-Index data
            saved_count = 0
            for date_obj, close_value in vnindex_dict.items():
                date_str = date_obj.strftime('%Y-%m-%d')
                cursor.execute('''
                    INSERT OR REPLACE INTO vnindex_cache (date, close, created_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                ''', (date_str, close_value))
                saved_count += 1
            
            conn.commit()
            self.logger.info(f"💾 Cached {saved_count} VN-Index records")
            
        except Exception as e:
            self.logger.error(f"Error saving VN-Index cache: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def get_fund_top_holding(self, fund_code):
        """Get top holdings for a specific fund using short_name"""
        try:
            # Try to get from cache first
            cached_holdings = self.get_fund_holdings_from_cache(fund_code)
            if cached_holdings:
                return cached_holdings
            
            # Get fund info to find short_name
            fund_info = self.get_fund_by_code(fund_code)
            if fund_info and 'raw_data' in fund_info and 'short_name' in fund_info['raw_data']:
                short_name = fund_info['raw_data']['short_name']
                if short_name:
                    self.logger.info(f"Getting top holdings for {fund_code} using short_name: {short_name}")
                    holdings_data = self.fund_api.details.top_holding(short_name)
                    return holdings_data.to_dict('records') if not holdings_data.empty else []
            
            self.logger.warning(f"No short_name found for {fund_code} or API returned empty data")
            return []
        except Exception as e:
            self.logger.error(f"Error getting top holdings for {fund_code}: {str(e)}")
            return []
    
    def get_fund_holdings_from_cache(self, fund_code):
        """Get top holdings for a specific fund from cache database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT stock_code, industry, net_asset_percent, type_asset, update_at
                    FROM holdings 
                    WHERE fund_code = ?
                    ORDER BY net_asset_percent DESC
                ''', (fund_code,))
                
                rows = cursor.fetchall()
                if rows:
                    columns = ['stock_code', 'industry', 'net_asset_percent', 'type_asset', 'update_at']
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    return []
        except Exception as e:
            self.logger.error(f"Error getting cached holdings for {fund_code}: {str(e)}")
            return []

    def get_fund_industry_holding(self, fund_code):
        """Get industry allocation for a specific fund"""
        try:
            industry_data = self.fund_api.details.industry_holding(fund_code)
            return industry_data.to_dict('records') if not industry_data.empty else []
        except Exception as e:
            self.logger.error(f"Error getting industry holdings for {fund_code}: {str(e)}")
            raise

    def get_fund_asset_holding(self, fund_code):
        """Get asset allocation for a specific fund using short_name"""
        try:
            # Get fund info to find short_name
            fund_info = self.get_fund_by_code(fund_code)
            if fund_info and 'raw_data' in fund_info and 'short_name' in fund_info['raw_data']:
                short_name = fund_info['raw_data']['short_name']
                if short_name:
                    self.logger.info(f"Getting asset allocation for {fund_code} using short_name: {short_name}")
                    asset_data = self.fund_api.details.asset_holding(short_name)
                    if not asset_data.empty:
                        return asset_data.to_dict('records')
            
            self.logger.warning(f"No short_name found for {fund_code} or API returned empty data")
            return []
        except Exception as e:
            self.logger.error(f"Error getting asset holdings for {fund_code}: {str(e)}")
            return []

    def get_fund_assets_from_cache(self, fund_code):
        """Get asset allocation for a specific fund from cache database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT asset_type, asset_percent, short_name
                    FROM asset_allocations 
                    WHERE fund_code = ?
                    ORDER BY asset_percent DESC
                ''', (fund_code,))
                
                rows = cursor.fetchall()
                if rows:
                    columns = ['asset_type', 'asset_percent', 'short_name']
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    return []
        except Exception as e:
            self.logger.error(f"Error getting cached assets for {fund_code}: {str(e)}")
            return []

    def cache_all_asset_allocations(self, resume=False):
        """Cache asset allocation data for all funds"""
        try:
            if not resume:
                # Clear existing asset allocation cache
                cursor = self.conn.cursor()
                cursor.execute('DELETE FROM asset_allocations')
                self.conn.commit()
                self.logger.info("Cleared existing asset allocation cache")
            
            # Get all fund codes
            cursor = self.conn.cursor()
            cursor.execute('SELECT fund_code FROM funds')
            funds = cursor.fetchall()
            
            success_count = 0
            error_count = 0
            
            self.logger.info(f"Starting asset allocation caching for {len(funds)} funds...")
            self.logger.warning("Note: vnstock API asset_holding currently returns 400 errors for all funds")
            
            for i, (fund_code,) in enumerate(funds, 1):
                self.logger.info(f"Processing {i}/{len(funds)}: {fund_code}")
                
                try:
                    # Try to fetch asset allocation data
                    asset_data = self.fund_api.details.asset_holding(fund_code)
                    
                    if hasattr(asset_data, 'empty') and not asset_data.empty:
                        # Insert asset allocation data
                        for _, row in asset_data.iterrows():
                            cursor.execute('''
                                INSERT INTO asset_allocations 
                                (fund_code, asset_type, asset_percent, short_name, updated_at)
                                VALUES (?, ?, ?, ?, ?)
                            ''', (
                                fund_code,
                                row.get('asset_type', ''),
                                row.get('asset_percent', 0),
                                row.get('short_name', ''),
                                datetime.now()
                            ))
                        
                        self.conn.commit()
                        success_count += 1
                        self.logger.info(f"Successfully cached asset allocation for {fund_code}")
                    else:
                        error_count += 1
                        self.logger.warning(f"No asset allocation data for {fund_code}")
                    
                    # Rate limiting
                    time.sleep(3)
                    
                except Exception as e:
                    error_count += 1
                    error_msg = str(e)
                    if "Rate limit exceeded" in error_msg:
                        self.logger.warning(f"Rate limit hit at fund {fund_code}, pausing...")
                        time.sleep(60)  # Wait 1 minute
                        break
                    elif "400" in error_msg or "Failed to fetch data" in error_msg:
                        self.logger.warning(f"API error for {fund_code}: {error_msg}")
                        # Continue with next fund instead of breaking
                    else:
                        self.logger.error(f"Error caching asset allocation for {fund_code}: {error_msg}")
            
            self.logger.info(f"Asset allocation caching completed: {success_count} success, {error_count} errors")
            if error_count == len(funds):
                self.logger.warning("All funds failed - vnstock API asset_holding may not be available currently")
            return success_count, error_count
            
        except Exception as e:
            self.logger.error(f"Error in cache_all_asset_allocations: {str(e)}")
            return 0, len(funds) if 'funds' in locals() else 0

    # Stock company information methods
    def get_stock_overview(self, symbol):
        """Get company overview information"""
        cache_key = self._company_cache_key(symbol, 'overview')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = {}
        try:
            symbol_upper = symbol.upper()
            company = vnstock.Company(symbol=symbol_upper, source='VCI')
            overview_data = company.overview()
            
            if not overview_data.empty:
                row = overview_data.iloc[0]
                result = {
                    'symbol': str(row.get('symbol', '')),
                    'company_name': symbol_upper,
                    'issue_share': int(row.get('issue_share', 0)) if row.get('issue_share') else 0,
                    'charter_capital': int(row.get('charter_capital', 0)) if row.get('charter_capital') else 0,
                    'history': str(row.get('history', '')),
                    'company_profile': str(row.get('company_profile', '')),
                    'icb_name2': str(row.get('icb_name2', '')),
                    'icb_name3': str(row.get('icb_name3', '')),
                    'icb_name4': str(row.get('icb_name4', '')),
                    'financial_ratio_issue_share': int(row.get('financial_ratio_issue_share', 0)) if row.get('financial_ratio_issue_share') else 0
                }
        except Exception as e:
            self.logger.error(f"Error getting stock overview for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_stock_shareholders(self, symbol):
        """Get major shareholders information"""
        cache_key = self._company_cache_key(symbol, 'shareholders')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = []
        try:
            company = vnstock.Company(symbol=symbol.upper(), source='VCI')
            shareholders_data = company.shareholders()
            
            if not shareholders_data.empty:
                for _, row in shareholders_data.head(10).iterrows():
                    result.append({
                        'share_holder': row.get('share_holder', ''),
                        'quantity': row.get('quantity', 0),
                        'share_own_percent': row.get('share_own_percent', 0),
                        'update_date': row.get('update_date', '')
                    })
        except Exception as e:
            self.logger.error(f"Error getting shareholders for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_stock_officers(self, symbol):
        """Get company officers information"""
        cache_key = self._company_cache_key(symbol, 'officers')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = []
        try:
            company = vnstock.Company(symbol=symbol.upper(), source='VCI')
            officers_data = company.officers(filter_by='working')
            
            if not officers_data.empty:
                for _, row in officers_data.iterrows():
                    result.append({
                        'officer_name': row.get('officer_name', ''),
                        'officer_position': row.get('officer_position', ''),
                        'position_short_name': row.get('position_short_name', ''),
                        'officer_own_percent': row.get('officer_own_percent', 0),
                        'quantity': row.get('quantity', 0),
                        'update_date': row.get('update_date', ''),
                        'type': row.get('type', '')
                    })
        except Exception as e:
            self.logger.error(f"Error getting officers for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_stock_financials(self, symbol):
        """Get financial ratios summary"""
        cache_key = self._company_cache_key(symbol, 'financials')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = {}
        try:
            company = vnstock.Company(symbol=symbol.upper(), source='VCI')
            financials_data = company.ratio_summary()
            
            if not financials_data.empty:
                row = financials_data.iloc[0]
                
                def safe_convert(value, convert_type=float):
                    try:
                        if pd.isna(value) or value is None:
                            return 0
                        if hasattr(value, 'item'):
                            value = value.item()
                        return convert_type(value)
                    except (ValueError, TypeError):
                        return 0
                
                result = {}
                for col in financials_data.columns:
                    value = row.get(col)
                    if col == 'symbol':
                        result[col] = str(value) if value else symbol.upper()
                    elif col in ['year_report', 'issue_share']:
                        result[col] = safe_convert(value, int)
                    else:
                        result[col] = safe_convert(value, float)
                
                self.logger.info(f"Available financial columns for {symbol}: {list(financials_data.columns)}")
        except Exception as e:
            self.logger.error(f"Error getting financials for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_stock_subsidiaries(self, symbol):
        """Get subsidiaries and affiliates information"""
        cache_key = self._company_cache_key(symbol, 'subsidiaries')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = []
        try:
            company = vnstock.Company(symbol=symbol.upper(), source='VCI')
            subsidiaries_data = company.subsidiaries()
            
            if not subsidiaries_data.empty:
                for _, row in subsidiaries_data.iterrows():
                    def safe_convert(value, convert_type=float):
                        if pd.isna(value) or value is None:
                            return None
                        try:
                            return convert_type(value)
                        except (ValueError, TypeError):
                            return None
                    
                    result.append({
                        'sub_organ_code': str(row.get('sub_organ_code', '')),
                        'organ_name': str(row.get('organ_name', '')),
                        'ownership_percent': safe_convert(row.get('ownership_percent'), float),
                        'type': str(row.get('type', ''))
                    })
        except Exception as e:
            self.logger.error(f"Error getting subsidiaries for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_stock_events(self, symbol):
        """Get corporate events information"""
        cache_key = self._company_cache_key(symbol, 'events')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = []
        try:
            company = vnstock.Company(symbol=symbol.upper(), source='VCI')
            events_data = company.events()
            
            if not events_data.empty:
                for _, row in events_data.head(20).iterrows():
                    def safe_convert(value, convert_type=float):
                        if pd.isna(value) or value is None:
                            return None
                        try:
                            return convert_type(value)
                        except (ValueError, TypeError):
                            return None
                    
                    result.append({
                        'event_title': str(row.get('event_title', '')),
                        'event_list_name': str(row.get('event_list_name', '')),
                        'public_date': str(row.get('public_date', '')),
                        'record_date': str(row.get('record_date', '')),
                        'exright_date': str(row.get('exright_date', '')),
                        'ratio': safe_convert(row.get('ratio'), float),
                        'value': safe_convert(row.get('value'), float)
                    })
        except Exception as e:
            self.logger.error(f"Error getting events for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_stock_news(self, symbol):
        """Get company news"""
        cache_key = self._company_cache_key(symbol, 'news')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = []
        try:
            company = vnstock.Company(symbol=symbol.upper(), source='VCI')
            news_data = company.news()
            
            if not news_data.empty:
                for _, row in news_data.head(10).iterrows():
                    result.append({
                        'news_title': str(row.get('news_title', '')),
                        'news_sub_title': str(row.get('news_sub_title', '')),
                        'news_short_content': str(row.get('news_short_content', '')),
                        'public_date': int(row.get('public_date', 0)) if row.get('public_date') else 0,
                        'news_source_link': str(row.get('news_source_link', '')),
                        'close_price': int(row.get('close_price', 0)) if row.get('close_price') else 0,
                        'price_change_pct': float(row.get('price_change_pct', 0)) if row.get('price_change_pct') else 0
                    })
        except Exception as e:
            self.logger.error(f"Error getting news for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_stock_reports(self, symbol):
        """Get analyst reports"""
        cache_key = self._company_cache_key(symbol, 'reports')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = []
        try:
            company = vnstock.Company(symbol=symbol.upper(), source='VCI')
            reports_data = company.reports()
            
            if not reports_data.empty:
                for _, row in reports_data.iterrows():
                    result.append({
                        'name': str(row.get('name', '')),
                        'description': str(row.get('description', '')),
                        'date': str(row.get('date', '')),
                        'link': str(row.get('link', ''))
                    })
        except Exception as e:
            self.logger.error(f"Error getting reports for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_stock_trading_stats(self, symbol):
        """Get trading statistics"""
        cache_key = self._company_cache_key(symbol, 'trading_stats')
        cached = self.company_cache.get(cache_key)
        if cached is not None:
            return cached

        result = {}
#pragma: allowlist nextline secret
        try:
            company = vnstock.Company(symbol=symbol.upper(), source='VCI')
            trading_data = company.trading_stats()
            
            if not trading_data.empty:
                row = trading_data.iloc[0]
                
                def safe_convert(value, convert_type=float):
                    try:
                        if pd.isna(value) or value is None:
                            return 0
                        if hasattr(value, 'item'):
                            value = value.item()
                        return convert_type(value)
                    except (ValueError, TypeError):
                        return 0
                
                result = {
                    'symbol': str(row.get('symbol', '')),
                    'exchange': str(row.get('exchange', '')),
                    'ev': safe_convert(row.get('ev'), float),
                    'ceiling': safe_convert(row.get('ceiling'), int),
                    'floor': safe_convert(row.get('floor'), int),
                    'ref_price': safe_convert(row.get('ref_price'), int),
                    'open': safe_convert(row.get('open'), int),
                    'match_price': safe_convert(row.get('match_price'), int),
                    'close_price': safe_convert(row.get('close_price'), int),
                    'price_change': safe_convert(row.get('price_change'), int),
                    'price_change_pct': safe_convert(row.get('price_change_pct'), float),
                    'highest': safe_convert(row.get('high'), int),
                    'lowest': safe_convert(row.get('low'), int),
                    'volume': safe_convert(row.get('total_volume'), int),
                    'value': 0,
                    'avg_volume_10d': 0,
                    'avg_volume_30d': safe_convert(row.get('avg_match_volume_2w'), int),
                    'market_cap': 0,
                    'free_float': safe_convert(row.get('current_holding_ratio'), float),
                    'beta': 0,
                    'high_price_1y': safe_convert(row.get('high_price_1y'), int),
                    'low_price_1y': safe_convert(row.get('low_price_1y'), int),
                    'foreign_volume': safe_convert(row.get('foreign_volume'), int),
                    'foreign_room': safe_convert(row.get('foreign_room'), int)
                }
        except Exception as e:
            self.logger.error(f"Error getting trading stats for {symbol}: {str(e)}")
        finally:
            self.company_cache.set(cache_key, result)
        return result

    def get_financial_ratios(self, symbol):
        """Get financial ratios for a stock - USE financial_data_raw.db"""
        import time
        start_time = time.time()
        
        try:
            # ✅ USE financial_data_raw.db for ALL data
            conn = sqlite3.connect('financial_data_raw.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    ticker as symbol,
                    yearReport as year,
                    lengthReport as quarter,
                    CASE 
                        WHEN lengthReport = 5 THEN 'year'
                        ELSE 'quarter'
                    END as period_type,
                    pe as pe_ratio,
                    pb as pb_ratio,
                    eps,
                    bvps,
                    roe,
                    roa,
                    epsTTM
                FROM financial_raw
                WHERE ticker = ? 
                ORDER BY yearReport DESC, lengthReport DESC
            ''', (symbol,))
            
            db_rows = cursor.fetchall()
            conn.close()
            
            if db_rows:
                # Convert database rows to list of dictionaries
                columns = [
                    'symbol', 'year', 'quarter', 'period_type',
                    'pe_ratio', 'pb_ratio', 'eps', 'bvps', 'roe', 'roa', 'epsTTM'
                ]
                
                ratios_list = []
                for row in db_rows:
                    ratio_dict = dict(zip(columns, row))
                    
                    # Map database field names to frontend field names
                    if 'pe_ratio' in ratio_dict:
                        ratio_dict['pe'] = ratio_dict.pop('pe_ratio')
                    if 'pb_ratio' in ratio_dict:
                        ratio_dict['pb'] = ratio_dict.pop('pb_ratio')
                    
                    ratios_list.append(ratio_dict)
                
                elapsed = time.time() - start_time
                self.logger.info(f"✅ RAW DB HIT: {symbol} ({len(ratios_list)} records) - {elapsed:.3f}s")
                return ratios_list
            
            # ⚠️ PRIORITY 2: Database miss - fetch from vnstock API (slower)
            elapsed = time.time() - start_time
            self.logger.warning(f"⚠️  DATABASE MISS: {symbol} not in cache ({elapsed:.3f}s), fetching from API...")
            
            # Initialize Finance class with required parameters
            finance = vnstock.Finance(source='VCI', symbol=symbol)
            
            # Get both yearly and quarterly data
            yearly_data = finance.ratio(period='year', lang='vi')
            quarterly_data = finance.ratio(period='quarter', lang='vi')
            
            ratios_list = []
            
            # Process yearly data (last 5 years)
            if yearly_data is not None and not yearly_data.empty:
                yearly_data = yearly_data.head(5)  # Limit to 5 years
                ratios_list.extend(self._process_ratio_data(yearly_data, 'year'))
            
            # Process quarterly data (last 20 quarters)
            if quarterly_data is not None and not quarterly_data.empty:
                quarterly_data = quarterly_data.head(20)  # Limit to 20 quarters
                ratios_list.extend(self._process_ratio_data(quarterly_data, 'quarter'))
            
            if not ratios_list:
                self.logger.warning(f"No financial ratios data for {symbol}")
                return []
            
            # Sort by year and quarter (most recent first)
            ratios_list.sort(key=lambda x: (x.get('year', 0), x.get('quarter', 0)), reverse=True)
            
            # ⚠️ NO LONGER SAVE to fund_data.db - all data is in financial_data_raw.db
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"Fetched financial ratios for {symbol} in {elapsed_time:.2f}s (not saved - use raw db)")
            
            return ratios_list
            
        except Exception as e:
            self.logger.error(f"Error getting financial ratios for {symbol}: {str(e)}")
            return []

    def _save_financial_ratios_to_db(self, symbol, ratios_list):
        """Save financial ratios to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Delete existing data for this symbol
            cursor.execute('DELETE FROM financial_ratios WHERE symbol = ?', (symbol,))
            
            # Insert new data
            for ratio in ratios_list:
                cursor.execute('''
                    INSERT INTO financial_ratios (
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
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol, ratio.get('year'), ratio.get('quarter'), ratio.get('period_type'),
                    ratio.get('pe_ratio'), ratio.get('pb_ratio'), ratio.get('ps_ratio'), 
                    ratio.get('pcf_ratio'), ratio.get('ev_ebitda'),
                    ratio.get('eps'), ratio.get('bvps'), ratio.get('market_cap'), 
                    ratio.get('shares_outstanding'),
                    ratio.get('roe'), ratio.get('roa'), ratio.get('roic'), 
                    ratio.get('gross_margin'), ratio.get('net_margin'), ratio.get('ebit_margin'),
                    ratio.get('ebitda'), ratio.get('ebit'), ratio.get('dividend_yield'),
                    ratio.get('current_ratio'), ratio.get('quick_ratio'), ratio.get('cash_ratio'), 
                    ratio.get('interest_coverage'), ratio.get('financial_leverage'), 
                    ratio.get('total_debt_to_equity'),
                    ratio.get('asset_turnover'), ratio.get('fixed_asset_turnover'), 
                    ratio.get('inventory_turnover'),
                    ratio.get('days_receivable'), ratio.get('days_inventory'), 
                    ratio.get('days_payable'), ratio.get('cash_cycle'),
                    ratio.get('equity_to_charter_capital')
                ))
            
            conn.commit()
            conn.close()
            self.logger.info(f"Saved {len(ratios_list)} financial ratios for {symbol} to database")
            
        except Exception as e:
            self.logger.error(f"Error saving financial ratios to database for {symbol}: {str(e)}")

    def cache_all_financial_ratios(self, resume=False):
        """Cache financial ratios for all stocks in fund holdings"""
        import time
        
        # Get all unique stock symbols from fund holdings
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT stock_code 
            FROM holdings 
            WHERE stock_code IS NOT NULL 
            AND stock_code != '' 
            AND stock_code NOT LIKE '%-%'
            ORDER BY stock_code
        ''')
        
        holdings_stocks = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        self.logger.info(f"Found {len(holdings_stocks)} unique stocks in fund holdings")
        
        # Filter out invalid symbols (keep only Vietnamese stock format)
        valid_stocks = []
        for stock in holdings_stocks:
            if stock and len(stock) >= 3 and stock.isalpha():
                valid_stocks.append(stock.upper())
        
        self.logger.info(f"Filtered to {len(valid_stocks)} valid stock symbols")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Update metadata to track caching progress
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', ('financial_ratios_cache_status', 'in_progress'))
            
            if not resume:
                # Clear existing financial ratios data
                cursor.execute('DELETE FROM financial_ratios')
                self.logger.info("Starting fresh financial ratios caching...")
            else:
                # Get already cached symbols
                cursor.execute('SELECT DISTINCT symbol FROM financial_ratios')
                cached_symbols = [row[0] for row in cursor.fetchall()]
                valid_stocks = [s for s in valid_stocks if s not in cached_symbols]
                self.logger.info(f"Resuming financial ratios caching. {len(valid_stocks)} symbols remaining.")
            
            conn.commit()
            
            total_stocks = len(valid_stocks)
            success_count = 0
            error_count = 0
            
            for i, symbol in enumerate(valid_stocks, 1):
                try:
                    self.logger.info(f"Caching financial ratios for {symbol} ({i}/{total_stocks})...")
                    
                    # Fetch and save data (will automatically save to database)
                    ratios_data = self.get_financial_ratios(symbol)
                    
                    if ratios_data:
                        success_count += 1
                        self.logger.info(f"✅ Successfully cached {len(ratios_data)} financial ratios for {symbol}")
                    else:
                        error_count += 1
                        self.logger.warning(f"⚠️ No financial ratios data for {symbol}")
                    
                    # Rate limiting - wait 2 seconds between requests
                    if i < total_stocks:
                        time.sleep(2)
                        
                except Exception as e:
                    error_count += 1
                    error_msg = str(e)
                    self.logger.error(f"❌ Error caching financial ratios for {symbol}: {error_msg}")
                    
                    # Continue with next symbol
                    continue
            
            # Update metadata with completion status
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', ('financial_ratios_cache_status', 'completed'))
            
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', ('financial_ratios_cache_summary', f"Success: {success_count}, Errors: {error_count}"))
            
            conn.commit()
            
            self.logger.info("Financial ratios caching completed!")
            self.logger.info(f"Summary: Success: {success_count}, Errors: {error_count}")
            
        except Exception as e:
            self.logger.error(f"Error in cache_all_financial_ratios: {str(e)}")
        finally:
            conn.close()
    
    def _process_ratio_data(self, data, period_type):
        """Process ratio data for both yearly and quarterly periods"""
        ratios_list = []
        
        for _, row in data.iterrows():
            # Extract data from multi-level columns
            try:
                year = int(row[('Meta', 'Năm')]) if pd.notna(row[('Meta', 'Năm')]) else None
                quarter = int(row[('Meta', 'Kỳ')]) if pd.notna(row[('Meta', 'Kỳ')]) else None
            except (KeyError, ValueError, TypeError):
                year = None
                quarter = None
                
            ratio_dict = {
                'year': year,
                'quarter': quarter,
                'period_type': period_type,  # Add period type identifier
                
                # Chỉ tiêu định giá
                'pb_ratio': self.safe_float(row.get(('Chỉ tiêu định giá', 'P/B'), 0)),
                'market_cap': self.safe_float(row.get(('Chỉ tiêu định giá', 'Vốn hóa (Tỷ đồng)'), 0)),
                'shares_outstanding': self.safe_float(row.get(('Chỉ tiêu định giá', 'Số CP lưu hành (Triệu CP)'), 0)),
                'pe_ratio': self.safe_float(row.get(('Chỉ tiêu định giá', 'P/E'), 0)),
                'ps_ratio': self.safe_float(row.get(('Chỉ tiêu định giá', 'P/S'), 0)),
                'pcf_ratio': self.safe_float(row.get(('Chỉ tiêu định giá', 'P/Cash Flow'), 0)),
                'eps': self.safe_float(row.get(('Chỉ tiêu định giá', 'EPS (VND)'), 0)),
                'bvps': self.safe_float(row.get(('Chỉ tiêu định giá', 'BVPS (VND)'), 0)),
                'ev_ebitda': self.safe_float(row.get(('Chỉ tiêu định giá', 'EV/EBITDA'), 0)),
                
                # Chỉ tiêu khả năng sinh lợi
                'gross_margin': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'Biên lợi nhuận gộp (%)'), 0)),
                'net_margin': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'Biên lợi nhuận ròng (%)'), 0)),
                'roe': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'ROE (%)'), 0)),
                'roic': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'ROIC (%)'), 0)),
                'roa': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'ROA (%)'), 0)),
                'ebit_margin': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'Biên EBIT (%)'), 0)),
                'ebitda': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'EBITDA (Tỷ đồng)'), 0)),
                'ebit': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'EBIT (Tỷ đồng)'), 0)),
                'dividend_yield': self.safe_float(row.get(('Chỉ tiêu khả năng sinh lợi', 'Tỷ suất cổ tức (%)'), 0)),
                
                # Chỉ tiêu thanh khoản
                'current_ratio': self.safe_float(row.get(('Chỉ tiêu thanh khoản', 'Chỉ số thanh toán hiện thời'), 0)),
                'cash_ratio': self.safe_float(row.get(('Chỉ tiêu thanh khoản', 'Chỉ số thanh toán tiền mặt'), 0)),
                'quick_ratio': self.safe_float(row.get(('Chỉ tiêu thanh khoản', 'Chỉ số thanh toán nhanh'), 0)),
                'interest_coverage': self.safe_float(row.get(('Chỉ tiêu thanh khoản', 'Khả năng chi trả lãi vay'), 0)),
                'financial_leverage': self.safe_float(row.get(('Chỉ tiêu thanh khoản', 'Đòn bẩy tài chính'), 0)),
                
                # Chỉ tiêu hiệu quả hoạt động
                'asset_turnover': self.safe_float(row.get(('Chỉ tiêu hiệu quả hoạt động', 'Vòng quay tài sản'), 0)),
                'fixed_asset_turnover': self.safe_float(row.get(('Chỉ tiêu hiệu quả hoạt động', 'Vòng quay TSCĐ'), 0)),
                'days_receivable': self.safe_float(row.get(('Chỉ tiêu hiệu quả hoạt động', 'Số ngày thu tiền bình quân'), 0)),
                'days_inventory': self.safe_float(row.get(('Chỉ tiêu hiệu quả hoạt động', 'Số ngày tồn kho bình quân'), 0)),
                'days_payable': self.safe_float(row.get(('Chỉ tiêu hiệu quả hoạt động', 'Số ngày thanh toán bình quân'), 0)),
                'cash_cycle': self.safe_float(row.get(('Chỉ tiêu hiệu quả hoạt động', 'Chu kỳ tiền'), 0)),
                'inventory_turnover': self.safe_float(row.get(('Chỉ tiêu hiệu quả hoạt động', 'Vòng quay hàng tồn kho'), 0)),
                
                # Chỉ tiêu cơ cấu nguồn vốn
                'debt_to_equity': self.safe_float(row.get(('Chỉ tiêu cơ cấu nguồn vốn', '(Vay NH+DH)/VCSH'), 0)),
                'total_debt_to_equity': self.safe_float(row.get(('Chỉ tiêu cơ cấu nguồn vốn', 'Nợ/VCSH'), 0)),
                'fixed_asset_to_equity': self.safe_float(row.get(('Chỉ tiêu cơ cấu nguồn vốn', 'TSCĐ / Vốn CSH'), 0)),
                'equity_to_charter_capital': self.safe_float(row.get(('Chỉ tiêu cơ cấu nguồn vốn', 'Vốn CSH/Vốn điều lệ'), 0))
            }
            
            ratios_list.append(ratio_dict)
        
        return ratios_list

    def safe_float(self, value):
        """Safely convert value to float, handling NaN and None"""
        try:
            if pd.isna(value) or value is None or value == 0:
                return None  # Return None instead of 0 for missing data
            if hasattr(value, 'item'):  # numpy types
                value = value.item()
            result = float(value)
            return result if not pd.isna(result) else None
        except (ValueError, TypeError):
            return None

    def cache_all_holdings(self, resume=False):
        """Cache top holdings for all funds with rate limiting and resume capability"""
        import time
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            if not resume:
                # Clear existing holdings only if not resuming
                cursor.execute('DELETE FROM holdings')
            
            # Get all fund codes
            funds = self.get_funds()
            total_funds = len(funds)
            cached_count = 0
            
            # Get already cached funds if resuming
            if resume:
                cursor.execute('SELECT DISTINCT fund_code FROM holdings')
                cached_funds = {row[0] for row in cursor.fetchall()}
                cached_count = len(cached_funds)
                self.logger.info(f"Resuming cache process. Already cached: {cached_count} funds")
            else:
                cached_funds = set()
            
            self.logger.info(f"Starting to cache holdings for {total_funds} funds...")
            
            for i, fund in enumerate(funds, 1):
                fund_code = fund['fund_code']
                
                # Skip if already cached and resuming
                if resume and fund_code in cached_funds:
                    continue
                
                try:
                    self.logger.info(f"Processing {i}/{total_funds}: {fund_code}")
                    holdings = self.get_fund_top_holding(fund_code)
                    
                    if holdings:
                        for holding in holdings:
                            cursor.execute('''
                                INSERT INTO holdings 
                                (fund_code, stock_code, industry, net_asset_percent, type_asset, update_at)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (
                                fund_code,
                                holding.get('stock_code', ''),
                                holding.get('industry', ''),
                                holding.get('net_asset_percent', 0),
                                holding.get('type_asset', ''),
                                holding.get('update_at', '')
                            ))
                        cached_count += 1
                        self.logger.info(f"Cached {len(holdings)} holdings for {fund_code}")
                        
                        # Commit after each fund to save progress
                        conn.commit()
                    else:
                        self.logger.warning(f"No holdings found for {fund_code}")
                    
                    # Add delay to avoid rate limiting
                    time.sleep(5)
                        
                except Exception as e:
                    if "Rate limit exceeded" in str(e) or "Bạn đã gửi quá nhiều request" in str(e):
                        self.logger.warning(f"Rate limit hit, saving progress and stopping...")
                        break
                    else:
                        self.logger.error(f"Error caching holdings for {fund_code}: {str(e)}")
                        continue
            
            # Update metadata
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value, updated_at)
                VALUES (?, ?, ?)
            ''', ('holdings_last_update', datetime.now().isoformat(), datetime.now().isoformat()))
            
            conn.commit()
            self.logger.info(f"Successfully cached holdings for {cached_count}/{total_funds} funds")
            
            return {
                'success': True,
                'cached_funds': cached_count,
                'total_funds': total_funds,
                'timestamp': datetime.now().isoformat(),
                'completed': cached_count == total_funds
            }
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error caching holdings: {str(e)}")
            raise
        finally:
            conn.close()

    def get_all_holdings(self):
        """Get all cached holdings with fund names"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT h.fund_code, f.fund_name, h.stock_code, h.industry, 
                       h.net_asset_percent, h.type_asset, h.update_at
                FROM holdings h
                JOIN funds f ON h.fund_code = f.fund_code
                ORDER BY h.net_asset_percent DESC
            ''')
            
            holdings = []
            for row in cursor.fetchall():
                holdings.append({
                    'fund_code': row[0],
                    'fund_name': row[1],
                    'stock_code': row[2],
                    'industry': row[3],
                    'net_asset_percent': row[4],
                    'type_asset': row[5],
                    'update_at': row[6]
                })
            
            return holdings
            
        except Exception as e:
            self.logger.error(f"Error getting all holdings: {str(e)}")
            return []
        finally:
            conn.close()

    def get_aggregated_holdings(self):
        """Get holdings aggregated by stock code with fund count and total percentage"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT 
                    h.stock_code,
                    h.industry,
                    h.type_asset,
                    SUM(h.net_asset_percent) as total_percent,
                    COUNT(DISTINCT h.fund_code) as fund_count,
                    GROUP_CONCAT(h.fund_code || ':' || f.fund_name, '|') as funds_info,
                    GROUP_CONCAT(h.fund_code || ':' || h.net_asset_percent, '|') as fund_percentages
                FROM holdings h
                JOIN funds f ON h.fund_code = f.fund_code
                WHERE h.stock_code IS NOT NULL AND h.stock_code != ''
                GROUP BY h.stock_code, h.industry, h.type_asset
                ORDER BY total_percent DESC
            ''')
            
            holdings = []
            for row in cursor.fetchall():
                # Parse funds info
                funds_info = []
                fund_percentages = {}
                
                if row[5]:  # funds_info
                    for fund_info in row[5].split('|'):
                        if ':' in fund_info:
                            fund_code, fund_name = fund_info.split(':', 1)
                            funds_info.append({'fund_code': fund_code, 'fund_name': fund_name})
                
                if row[6]:  # fund_percentages
                    for fund_perc in row[6].split('|'):
                        if ':' in fund_perc:
                            fund_code, percentage = fund_perc.split(':', 1)
                            try:
                                fund_percentages[fund_code] = float(percentage)
                            except ValueError:
                                fund_percentages[fund_code] = 0
                
                holdings.append({
                    'stock_code': row[0],
                    'industry': row[1],
                    'type_asset': row[2],
                    'total_percent': round(row[3], 2),
                    'fund_count': row[4],
                    'funds_info': funds_info,
                    'fund_percentages': fund_percentages
                })
            
            return holdings
            
        except Exception as e:
            self.logger.error(f"Error getting aggregated holdings: {str(e)}")
            return []
        finally:
            conn.close()

    def get_cached_holdings(self, fund_code):
        """Get cached holdings for a specific fund"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT stock_code, industry, net_asset_percent, type_asset, update_at
                FROM holdings
                WHERE fund_code = ?
                ORDER BY net_asset_percent DESC
            ''', (fund_code,))
            
            rows = cursor.fetchall()
            holdings = []
            
            for row in rows:
                holdings.append({
                    'stock_code': row[0],
                    'industry': row[1],
                    'net_asset_percent': row[2],
                    'type_asset': row[3],
                    'update_at': row[4]
                })
            
            return holdings
            
        except Exception as e:
            self.logger.error(f"Error getting cached holdings for {fund_code}: {str(e)}")
            return []
        finally:
            conn.close()

    def get_statistics(self):
        """Get fund statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get basic statistics
        cursor.execute('SELECT COUNT(*) FROM funds')
        total_funds = cursor.fetchone()[0]
        
        cursor.execute('SELECT AVG(nav) FROM funds WHERE nav > 0')
        avg_nav = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(DISTINCT fund_type) FROM funds')
        fund_types = cursor.fetchone()[0]
        
        # Get last update time
        cursor.execute('SELECT MAX(updated_at) FROM funds')
        last_update = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_funds': total_funds,
            'avg_nav': round(avg_nav, 2) if avg_nav else 0,
            'fund_types': fund_types,
            'last_update': last_update
        }
    
    def get_last_update_time(self):
        """Get the last update timestamp"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM metadata WHERE key = ?', ('last_update',))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Error retrieving last update time: {str(e)}")
            return None
        finally:
            conn.close()
    
    def is_data_stale(self, hours=24):
        """Check if data is older than specified hours"""
        last_update = self.get_last_update_time()
        if not last_update:
            return True
        
        last_update_dt = datetime.fromisoformat(last_update)
        return datetime.now() - last_update_dt > timedelta(hours=hours)
