#!/usr/bin/env python3
"""
Convert Ratios JSON files to Apache Arrow IPC format.
Source: frontend/api_data/ratios/*.json
Output: frontend/api_data/ratios_data.arrow

This data contains calculated ratios (P/E, ROE, Market Cap, etc.) 
optimized for Bubble Chart visualization.
"""

import json
import pyarrow as pa
import pyarrow.feather as feather
from pathlib import Path
from tqdm import tqdm
import sys

# Configuration
INPUT_DIR = Path('frontend/api_data/ratios')
OUTPUT_FILE = Path('frontend/api_data/ratios_data.arrow')

def convert_ratios_to_arrow():
    print("=" * 60)
    print("Converting Ratios Data to Arrow IPC")
    print("=" * 60)
    
    json_files = list(INPUT_DIR.glob('*.json'))
    if not json_files:
        print("❌ No JSON files found in frontend/api_data/ratios/")
        return False
        
    print(f"Found {len(json_files)} files to process")
    
    records = []
    
    for json_file in tqdm(json_files, desc="Processing files"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            ticker = data.get('symbol')
            if not ticker:
                continue
                
            stats = data.get('financial_stats', [])
            if not stats or not isinstance(stats, list):
                continue
                
            for item in stats:
                # Base record
                record = {
                    'ticker': ticker,
                    'year': str(item.get('year', '')),
                    'quarter': int(item.get('quarter', 0)),
                    'ratio_type': item.get('ratioType', ''),
                }
                
                # Dynamically add all other numeric fields
                # Exclude keys we already handled or don't want
                exclude_keys = {'year', 'quarter', 'ratioType', 'ticker', 'organCode', 'id'}
                
                for key, value in item.items():
                    if key not in exclude_keys:
                        # Convert CamelCase to snake_case if needed, or keep as is.
                        # For simplicity and frontend compatibility, let's keep original keys 
                        # OR map them if we want consistency.
                        # Given the frontend expects specific snake_case names (roe, market_cap),
                        # we should map known keys and keep others as is.
                        
                        # Mapping for known keys to match frontend expectations
                        key_map = {
                            'marketCap': 'market_cap',
                            'pe': 'pe',
                            'pb': 'pb',
                            'roe': 'roe',
                            'roa': 'roa',
                            'evToEbitda': 'ev_ebitda',
                            'dividendYield': 'dividend_yield',
                            'afterTaxProfitMargin': 'net_margin',
                            'grossMargin': 'gross_margin',
                            'debtToEquity': 'debt_to_equity',
                            'currentRatio': 'current_ratio',
                            'quickRatio': 'quick_ratio',
                            'priceToCashFlow': 'price_to_cash_flow',
                            'roic': 'roic',
                            'assetTurnover': 'asset_turnover',
                            'ebitMargin': 'ebit_margin',
                            'preTaxProfitMargin': 'pre_tax_margin',
                            'inventoryTurnover': 'inventory_turnover',
                            'receivablesTurnover': 'receivables_turnover',
                            # Add more mappings as needed, or just use the original key
                        }
                        
                        final_key = key_map.get(key, key) # Use mapped key or original
                        
                        # Try to convert to float
                        try:
                            if value is None or value == '':
                                record[final_key] = 0.0
                            else:
                                record[final_key] = float(value)
                        except (ValueError, TypeError):
                            # If not a number, skip or store as string? 
                            # For ratios, we mostly care about numbers.
                            continue

                records.append(record)
                
        except Exception as e:
            print(f"⚠️ Error processing {json_file.name}: {e}")
            continue
            
    if not records:
        print("❌ No records extracted")
        return False
        
    print(f"\n📊 Extracted {len(records):,} records")
    
    # Create Arrow Table
    table = pa.Table.from_pylist(records)
    
    # Write to Arrow IPC
    print(f"💾 Writing to {OUTPUT_FILE}...")
    feather.write_feather(
        table, 
        OUTPUT_FILE, 
        compression='uncompressed'
    )
    
    size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print(f"✅ Success! File size: {size_mb:.1f} MB")
    
    return True

if __name__ == "__main__":
    convert_ratios_to_arrow()
