#!/usr/bin/env python3
"""
Fetch price data and calculate technical indicators using pandas-ta
For each company:
- Fetch 200 days of price data
- Calculate 200+ technical indicators
- Save to Arrow file (technical_analysis.arrow)

Uses parallel processing for faster execution.
"""

import requests
import json
import os
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import numpy as np

try:
    import pandas_ta as ta
    HAS_PANDAS_TA = True
except ImportError:
    print("⚠️ pandas_ta not found. Will use manual calculation fallback.")
    HAS_PANDAS_TA = False

# API Configuration - Use environment variables for sensitive URLs
BASE_URL = os.getenv('API_BASE_URL', 'https://iq.vietcap.com.vn/api/iq-insight-service/v1')
LISTINGS_URL = os.getenv('LISTINGS_URL', 'https://screener.lightinvest.vn/screener.json')
HEADERS = {
    "accept": "application/json",
    "origin": os.getenv('API_ORIGIN', 'https://trading.vietcap.com.vn'),
    "referer": os.getenv('API_REFERER', 'https://trading.vietcap.com.vn/'),
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

# Thread-safe counter and lock for progress tracking
progress_lock = Lock()
processed_count = 0
success_count = 0

def fetch_stock_listings() -> list:
    """Fetch stock listings from API."""
    try:
        response = requests.get(LISTINGS_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        tickers = []
        
        # Handle screener.json structure (stocks key)
        if "stocks" in data and isinstance(data["stocks"], list):
            tickers = [item['ticker'] for item in data["stocks"] if 'ticker' in item]
            print(f"✓ Fetched {len(tickers)} tickers from screener.json")
            
        # Handle legacy structure (all_symbols key)
        elif "all_symbols" in data:
            all_symbols = data.get("all_symbols", [])
            if all_symbols and isinstance(all_symbols, list) and isinstance(all_symbols[0], dict):
                tickers = [item['symbol'] for item in all_symbols if 'symbol' in item]
            else:
                tickers = all_symbols if isinstance(all_symbols, list) else []
            print(f"✓ Fetched {len(tickers)} tickers from legacy API")
            
        if not tickers:
            print("⚠️  No tickers found in response, trying manual extraction...")
            # Fallback: Extract from indices and industries
            tickers = set()
            if "stocks_by_index" in data and isinstance(data["stocks_by_index"], dict):
                for index_stocks in data["stocks_by_index"].values():
                    tickers.update(index_stocks)
            if "stocks_by_industry" in data and isinstance(data["stocks_by_industry"], dict):
                for industry_stocks in data["stocks_by_industry"].values():
                    tickers.update(industry_stocks)
            tickers = sorted(list(tickers))
        
        return tickers
    except Exception as e:
        print(f"❌ Error fetching stock listings: {e}")
        import traceback
        traceback.print_exc()
        return []

def fetch_price_data(ticker: str, length: int = 210) -> dict:
    """Fetch price chart data from Vietcap API.
    
    Note: Fetching 210 days to have enough data for SMA200 calculation.
    For VNINDEX, use lengthReport=10 to get full history as lengthReport=200 returns None for indices.
    """
    url = f"{BASE_URL}/company/{ticker}/price-chart"
    
    # Special handling for VNINDEX - use smaller lengthReport to get data
    # Vietcap API quirk: large lengthReport values return None for indices
    if ticker == "VNINDEX":
        params = {"lengthReport": min(length, 10)}
    else:
        params = {"lengthReport": length}
    
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        result = response.json()
        
        # For indices, we get full history regardless of lengthReport
        # So we need to trim to requested length
        if result.get("successful") and result.get("data"):
            data = result["data"]
            if len(data) > length:
                result["data"] = data[-length:]  # Get last N candles
        
        return result
    except Exception as e:
        return None

def process_ticker(ticker: str) -> pd.DataFrame:
    """Fetch price data and calculate technical indicators for a ticker. Returns DataFrame."""
    global processed_count, success_count
    
    # Fetch price data (works for both stocks and indices)
    # Using 210 days to have enough data for SMA200
    price_data = fetch_price_data(ticker, length=210)
    
    if not price_data or not price_data.get("successful"):
        with progress_lock:
            processed_count += 1
        return None
    
    candles = price_data.get("data", [])
    if not candles:
        with progress_lock:
            processed_count += 1
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(candles)
    
    # Rename columns to match ta library expectations
    df = df.rename(columns={
        'openPrice': 'open',
        'highPrice': 'high',
        'lowPrice': 'low',
        'closingPrice': 'close',
        'tradingTime': 'date'
    })
    
    # Add ticker column
    df['ticker'] = ticker
    
    # Add volume (fake data since API doesn't provide it)
    df['volume'] = 1000000  # Default volume
    
    # Convert timestamp to datetime
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df = df.set_index('date')
    
    # Sort by date (oldest first)
    df = df.sort_index()
    
    # Manual calculation function (Fallback)
    def calculate_manual_indicators(df):
        try:
            # SMA
            df['SMA_20'] = df['close'].rolling(window=20).mean()
            df['SMA_50'] = df['close'].rolling(window=50).mean()
            df['SMA_200'] = df['close'].rolling(window=200).mean()
            
            # EMA
            df['EMA_12'] = df['close'].ewm(span=12, adjust=False).mean()
            df['EMA_26'] = df['close'].ewm(span=26, adjust=False).mean()
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI_14'] = 100 - (100 / (1 + rs))
            
            # MACD
            df['MACD_12_26_9'] = df['EMA_12'] - df['EMA_26']
            df['MACDs_12_26_9'] = df['MACD_12_26_9'].ewm(span=9, adjust=False).mean()
            df['MACDh_12_26_9'] = df['MACD_12_26_9'] - df['MACDs_12_26_9']
            
            # Bollinger Bands
            df['BBM_20_2.0'] = df['SMA_20']
            std = df['close'].rolling(window=20).std()
            df['BBU_20_2.0'] = df['SMA_20'] + (std * 2)
            df['BBL_20_2.0'] = df['SMA_20'] - (std * 2)
            
        except Exception as e:
            print(f"❌ Manual calculation failed: {e}")

    # Calculate indicators
    if HAS_PANDAS_TA:
        try:
            # Overlap Studies
            df.ta.sma(length=20, append=True)
            df.ta.sma(length=50, append=True)
            df.ta.sma(length=200, append=True)
            df.ta.ema(length=12, append=True)
            df.ta.ema(length=26, append=True)
            df.ta.wma(length=20, append=True)
            df.ta.vwap(append=True)
            df.ta.bbands(length=20, append=True)
            df.ta.kc(length=20, append=True)
            df.ta.donchian(length=20, append=True)

            # Momentum Indicators
            df.ta.rsi(length=14, append=True)
            df.ta.macd(fast=12, slow=26, signal=9, append=True)
            df.ta.stoch(append=True)
            df.ta.cci(length=20, append=True)
            df.ta.willr(length=14, append=True)
            df.ta.roc(length=12, append=True)
            df.ta.mom(length=10, append=True)
            df.ta.ppo(append=True)
            df.ta.tsi(append=True)
            df.ta.uo(append=True)
            df.ta.kst(append=True)
            df.ta.ao(append=True)
            df.ta.coppock(append=True)
            df.ta.fisher(append=True)
            df.ta.squeeze(append=True)

            # Volatility Indicators
            df.ta.atr(length=14, append=True)
            df.ta.natr(length=14, append=True)
            df.ta.true_range(append=True)
            df.ta.massi(append=True)
            df.ta.ui(append=True)
            df.ta.pdist(append=True)
            df.ta.rvi(append=True)

            # Volume Indicators (with fake volume)
            df.ta.obv(append=True)
            df.ta.ad(append=True)
            df.ta.adosc(append=True)
            df.ta.cmf(append=True)
            df.ta.efi(append=True)
            df.ta.eom(append=True)
            df.ta.mfi(append=True)
            df.ta.nvi(append=True)
            df.ta.pvi(append=True)
            df.ta.pvol(append=True)
            df.ta.pvt(append=True)
            df.ta.vwma(append=True)
            
            # === PSY (Psychological Line) ===
            df['PSY_12'] = 100 * (df['close'].diff() > 0).rolling(12).sum() / 12

            # === Classic Pivot Points (from last candle) ===
            if len(df) > 0:
                last = df.iloc[-1]
                high = last['high']
                low = last['low']
                close = last['close']
                pivot = (high + low + close) / 3
                s1 = 2 * pivot - high
                s2 = pivot - (high - low)
                s3 = low - 2 * (high - pivot)
                r1 = 2 * pivot - low
                r2 = pivot + (high - low)
                r3 = high + 2 * (pivot - low)
                df['pivot'] = pivot
                df['s1'] = s1
                df['s2'] = s2
                df['s3'] = s3
                df['r1'] = r1
                df['r2'] = r2
                df['r3'] = r3

        except Exception as e:
            print(f"⚠️ pandas_ta calculation failed ({e}), using manual calculation...")
            calculate_manual_indicators(df)
    else:
        calculate_manual_indicators(df)

    
    # Rename columns to be more frontend-friendly
    rename_mapping = {
        # Supertrend
        'SUPERT_7_3.0': 'supertrend',
        'SUPERTd_7_3.0': 'supertrend_direction',
        'SUPERTl_7_3.0': 'supertrend_long',
        'SUPERTs_7_3.0': 'supertrend_short',
        
        # PSAR
        'PSARl_0.02_0.2': 'psar_long',
        'PSARs_0.02_0.2': 'psar_short',
        'PSARaf_0.02_0.2': 'psar_af',
        'PSARr_0.02_0.2': 'psar_reverse',
        
        # Bollinger Bands
        # 'BBL_20_2.0': 'bb_lower',
        # 'BBM_20_2.0': 'bb_middle',
        # 'BBU_20_2.0': 'bb_upper',
        # 'BBB_20_2.0': 'bb_bandwidth',
        # 'BBP_20_2.0': 'bb_percent',
        
        # Keltner Channel
        # 'KCLe_20_2': 'kc_lower',
        # 'KCBe_20_2': 'kc_middle',
        # 'KCUe_20_2': 'kc_upper',
        
        # Donchian Channel
        # 'DCL_20_20': 'donchian_lower',
        # 'DCM_20_20': 'donchian_middle',
        # 'DCU_20_20': 'donchian_upper',
        
        # MACD
        # 'MACD_12_26_9': 'macd',
        # 'MACDh_12_26_9': 'macd_histogram',
        # 'MACDs_12_26_9': 'macd_signal',
        
        # Stochastic
        # 'STOCHk_14_3_3': 'stoch_k',
        # 'STOCHd_14_3_3': 'stoch_d',
        
        # ADX
        # 'ADX_14': 'adx',
        # 'DMP_14': 'adx_plus',
        # 'DMN_14': 'adx_minus',
        
        # Aroon
        # 'AROOND_25': 'aroon_down',
        # 'AROONU_25': 'aroon_up',
        # 'AROONOSC_25': 'aroon_osc',
        
        # Ichimoku
        # 'ISA_9': 'ichimoku_conversion',
        # 'ISB_26': 'ichimoku_base',
        # 'ITS_9': 'ichimoku_span_a',
        # 'IKS_26': 'ichimoku_span_b',
        # 'ICS_26': 'ichimoku_chikou',
        
        # Vortex
        # 'VTXP_14': 'vortex_plus',
        # 'VTXM_14': 'vortex_minus',
        
        # KST
        # 'KST_10_15_20_30_10_10_10_15': 'kst',
        # 'KSTs_9': 'kst_signal',
        
        # TSI
        # 'TSI_13_25_13': 'tsi',
        # 'TSIs_13_25_13': 'tsi_signal',
        
        # True Range
        # 'TRUERANGE_1': 'true_range',
        
        # SMA
        # 'SMA_20': 'sma_20',
        # 'SMA_50': 'sma_50',
        # 'SMA_200': 'sma_200',
        
        # EMA
        # 'EMA_12': 'ema_12',
        # 'EMA_26': 'ema_26',
        
        # WMA
        # 'WMA_20': 'wma_20',
        
        # VWAP
        # 'VWAP_D': 'vwap',
        
        # RSI
        # 'RSI_14': 'rsi',
        
        # CCI
        # 'CCI_20_0.015': 'cci',
        
        # Williams %R
        # 'WILLR_14': 'williams_r',
        
        # ROC
        # 'ROC_12': 'roc',
        
        # Momentum
        # 'MOM_10': 'momentum',
        
        # PPO
        # 'PPO_12_26_9': 'ppo',
        # 'PPOh_12_26_9': 'ppo_histogram',
        # 'PPOs_12_26_9': 'ppo_signal',
        
        # Ultimate Oscillator
        # 'UO_7_14_28': 'ultimate_osc',
        
        # Awesome Oscillator
        # 'AO_5_34': 'awesome_osc',
        
        # Coppock Curve
        # 'COPC_11_14_10': 'coppock',
        
        # Fisher Transform
        # 'FISHERT_9_1': 'fisher',
        # 'FISHERTs_9_1': 'fisher_signal',
        
        # Squeeze
        'SQZ_20_2.0_20_1.5': 'squeeze',
        'SQZ_ON': 'squeeze_on',
        'SQZ_OFF': 'squeeze_off',
        'SQZ_NO': 'squeeze_no',
        
        # ATR
        'ATR_14': 'atr',
        'NATR_14': 'atr_normalized',
        
        # MASSI
        'MASSI_9_25': 'mass_index',
        
        # UI
        'UI_14': 'ulcer_index',
        
        # Price Distance
        'PDIST': 'price_distance',
        
        # RVI
        'RVI_14': 'rvi',
        
        # Volume indicators
        'OBV': 'obv',
        'AD': 'ad',
        'ADOSC_3_10': 'adosc',
        'CMF_20': 'cmf',
        'EFI_13': 'efi',
        'EOM_14_100000000': 'eom',
        'MFI_14': 'mfi',
        'NVI_1': 'nvi',
        'PVI_1': 'pvi',
        'PVOL': 'pvol',
        'PVT': 'pvt',
        'VWMA_20': 'vwma',
        
        # DPO
        'DPO_20': 'dpo',
        
        # QSTICK
        'QS_10': 'qstick',
        
        # PSY
        'PSY_12': 'psy',
        
        # Pivot Points
        'pivot': 'pivot',
        's1': 'support_1',
        's2': 'support_2',
        's3': 'support_3',
        'r1': 'resistance_1',
        'r2': 'resistance_2',
        'r3': 'resistance_3'
    }
    
    # Apply renaming
    df = df.rename(columns=rename_mapping)
    
    # Reset index to make date a column
    df = df.reset_index()
    
    # Replace NaN and Infinity with None for JSON compatibility (not strictly needed for Arrow but good practice)
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # Update progress
    with progress_lock:
        processed_count += 1
        success_count += 1
        if processed_count % 50 == 0:
            print(f"Progress: {processed_count} processed, {success_count} successful")
    
    return df

def save_to_arrow(df: pd.DataFrame, output_file: str = "data/technical_analysis.arrow"):
    """Save DataFrame to Arrow file."""
    # Create directory if it doesn't exist
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Save as Arrow IPC (Feather)
    feather.write_feather(df, output_file, compression='lz4')
    
    size_mb = Path(output_file).stat().st_size / (1024 * 1024)
    print(f"\n💾 Saved data to {output_file} ({size_mb:.1f} MB)")

def main():
    global processed_count, success_count
    
    print("=" * 60)
    print("Technical Analysis Data Fetcher (Arrow Version)")
    print("=" * 60)
    
    # Fetch stock listings from API
    print("\n📥 Fetching stock listings from API...")
    tickers = fetch_stock_listings()
    
    if not tickers:
        print("❌ No tickers found. Exiting.")
        return
    
    # Add VNINDEX to the beginning
    tickers = ["VNINDEX"] + tickers
    
    print(f"\n🔄 Processing {len(tickers)} tickers (including VNINDEX)...")
    print(f"   Using parallel processing with 20 workers\n")

    all_dfs = []

    # Process tickers in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=20) as executor:
        # Submit all tasks
        future_to_ticker = {executor.submit(process_ticker, ticker): ticker for ticker in tickers}
        
        # Process completed tasks
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                print(f"❌ Error processing {ticker}: {e}")

    if all_dfs:
        print(f"\nCombining {len(all_dfs)} DataFrames...")
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # Save to Arrow
        save_to_arrow(final_df)
        
        print(f"\n✅ All done! {success_count}/{len(tickers)} tickers processed")
        print(f"   Total rows: {len(final_df)}")
    else:
        print("\n❌ No data collected!")

if __name__ == "__main__":
    main()
