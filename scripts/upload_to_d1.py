#!/opt/homebrew/bin/python3.12
"""
Upload technical analysis data to Cloudflare D1 Database
Uses Cloudflare API to execute SQL statements
"""

import requests
import json
import os
import sys
from datetime import datetime

# Cloudflare Configuration
CF_ACCOUNT_ID = os.getenv('CF_ACCOUNT_ID')
CF_API_TOKEN = os.getenv('CF_API_TOKEN')
CF_DATABASE_ID = os.getenv('CF_DATABASE_ID')

BASE_URL = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}"

def execute_sql(sql: str, params: list = None):
    """Execute SQL on D1 database via Cloudflare API."""
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "sql": sql
    }
    
    if params:
        payload["params"] = params
    
    response = requests.post(
        f"{BASE_URL}/query",
        headers=headers,
        json=payload
    )
    
    if response.status_code != 200:
        print(f"❌ SQL Error: {response.text}")
        return None
    
    return response.json()

def batch_insert_prices(ticker: str, candles: list):
    """Batch insert price data for a ticker."""
    if not candles:
        return 0
    
    # Prepare batch insert
    values = []
    for candle in candles:
        values.append(f"('{ticker}', '{candle['date']}', {candle['open']}, {candle['high']}, {candle['low']}, {candle['close']}, {candle.get('volume', 0)})")
    
    # Split into chunks of 100 (D1 limit)
    chunk_size = 100
    total_inserted = 0
    
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i+chunk_size]
        sql = f"""
        INSERT OR REPLACE INTO price_data (ticker, date, open, high, low, close, volume)
        VALUES {','.join(chunk)}
        """
        
        result = execute_sql(sql)
        if result:
            total_inserted += len(chunk)
    
    return total_inserted

def batch_insert_indicators(ticker: str, indicators: list):
    """Batch insert technical indicators for a ticker."""
    if not indicators:
        return 0
    
    total_inserted = 0
    chunk_size = 50  # Smaller chunks for indicators (larger rows)
    
    for i in range(0, len(indicators), chunk_size):
        chunk = indicators[i:i+chunk_size]
        
        values = []
        for ind in chunk:
            # Extract key indicators
            sma_20 = ind.get('SMA_20', 'NULL')
            sma_50 = ind.get('SMA_50', 'NULL')
            sma_200 = ind.get('SMA_200', 'NULL')
            ema_12 = ind.get('EMA_12', 'NULL')
            ema_26 = ind.get('EMA_26', 'NULL')
            rsi_14 = ind.get('RSI_14', 'NULL')
            macd = ind.get('MACD_12_26_9', 'NULL')
            atr = ind.get('ATRr_14', 'NULL')
            
            # Store all indicators as JSON
            indicators_json = json.dumps({k: v for k, v in ind.items() if k != 'date'})
            
            values.append(f"""('{ticker}', '{ind['date']}', {sma_20}, {sma_50}, {sma_200}, 
                          {ema_12}, {ema_26}, {rsi_14}, {macd}, {atr}, '{indicators_json}')""")
        
        sql = f"""
        INSERT OR REPLACE INTO technical_indicators 
        (ticker, date, sma_20, sma_50, sma_200, ema_12, ema_26, rsi_14, macd, atr_14, indicators_json)
        VALUES {','.join(values)}
        """
        
        result = execute_sql(sql)
        if result:
            total_inserted += len(chunk)
    
    return total_inserted

def upload_technical_data():
    """Upload technical analysis data from JSON to D1."""
    
    filepath = 'data/technical_analysis.json'
    
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return
    
    print(f"📁 Loading {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    total_companies = data.get('total_companies', 0)
    successful = data.get('successful_count', 0)
    companies_data = data.get('data', {})
    
    print(f"📊 Found {successful} companies with data")
    print(f"⬆️  Uploading to D1...\n")
    
    total_price_records = 0
    total_indicator_records = 0
    processed = 0
    
    for ticker, ticker_data in companies_data.items():
        processed += 1
        candles = ticker_data.get('indicators', [])
        
        if not candles:
            continue
        
        print(f"[{processed}/{successful}] {ticker} - {len(candles)} candles")
        
        # Insert price data
        price_count = batch_insert_prices(ticker, candles)
        total_price_records += price_count
        
        # Insert indicators
        ind_count = batch_insert_indicators(ticker, candles)
        total_indicator_records += ind_count
        
        print(f"  ✓ Price: {price_count}, Indicators: {ind_count}")
    
    # Update metadata
    execute_sql(f"""
        UPDATE metadata SET value = '{successful}', updated_at = datetime('now') 
        WHERE key = 'total_tickers'
    """)
    
    execute_sql(f"""
        UPDATE metadata SET value = '{total_price_records}', updated_at = datetime('now')
        WHERE key = 'total_records'
    """)
    
    execute_sql(f"""
        UPDATE metadata SET value = datetime('now'), updated_at = datetime('now')
        WHERE key = 'last_update'
    """)
    
    print(f"\n✅ Upload complete!")
    print(f"📊 Total: {successful} tickers, {total_price_records} price records, {total_indicator_records} indicator records")

if __name__ == "__main__":
    upload_technical_data()
