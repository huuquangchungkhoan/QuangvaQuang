#!/usr/bin/env python3
"""
Convert financial statement JSON files to Apache Arrow IPC format.
Approach 3 (Modified): Separate Metadata + 4 Data Tables by Report Type

Output:
- metadata.json (~130KB) - Field definitions and labels
- balance_sheet_data.arrow (~15-25MB) - Arrow IPC format
- income_statement_data.arrow (~8-15MB) - Arrow IPC format
- cash_flow_data.arrow (~10-18MB) - Arrow IPC format
- note_data.arrow (~20-30MB) - Arrow IPC format
"""

import json
import pyarrow as pa
import pyarrow.feather as feather
from pathlib import Path
from tqdm import tqdm
from collections import defaultdict
import sys

# Configuration
INPUT_DIR = Path('frontend/api_data/financial_statements')
OUTPUT_DIR = Path('frontend/api_data')
OUTPUT_METADATA = OUTPUT_DIR / 'metadata.json'

# 4 separate Arrow IPC files by report type
OUTPUT_FILES = {
    'BALANCE_SHEET': OUTPUT_DIR / 'balance_sheet_data.arrow',
    'INCOME_STATEMENT': OUTPUT_DIR / 'income_statement_data.arrow',
    'CASH_FLOW': OUTPUT_DIR / 'cash_flow_data.arrow',
    'NOTE': OUTPUT_DIR / 'note_data.arrow',
}


def extract_metadata():
    """Extract metadata from first available JSON file (all have same structure)"""
    print("📋 Extracting metadata...")
    
    # Find first JSON file
    first_file = next(INPUT_DIR.glob('*.json'), None)
    if not first_file:
        print("❌ No JSON files found!")
        sys.exit(1)
    
    with open(first_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract metadata for all report types
    metadata = {}
    if 'metadata' in data:
        for report_type in ['BALANCE_SHEET', 'INCOME_STATEMENT', 'CASH_FLOW', 'NOTE']:
            if report_type in data['metadata']:
                metadata[report_type] = data['metadata'][report_type]
    
    # Save to JSON
    with open(OUTPUT_METADATA, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    # Get file size
    size_kb = OUTPUT_METADATA.stat().st_size / 1024
    print(f"✅ Metadata saved: {OUTPUT_METADATA.name} ({size_kb:.1f} KB)")
    
    return metadata


def convert_to_parquet_by_type():
    """
    Convert all JSON files to 4 separate Parquet files (one per report type).
    
    Schema for each file:
    - ticker: string
    - period: string (e.g., '2023Q4', '2023')
    - field: string (e.g., 'bsa1', 'isa1', 'cfa1')
    - value: float64
    """
    print("\n🔄 Converting JSON files to Parquet (split by report type)...")
    
    # Separate collections for each report type
    data_by_type = {
        'BALANCE_SHEET': [],
        'INCOME_STATEMENT': [],
        'CASH_FLOW': [],
        'NOTE': [],
    }
    
    json_files = list(INPUT_DIR.glob('*.json'))
    total_files = len(json_files)
    
    if total_files == 0:
        print("❌ No JSON files found!")
        sys.exit(1)
    
    print(f"Found {total_files} JSON files to process\n")
    
    # Track statistics
    stats = defaultdict(int)
    
    for json_file in tqdm(json_files, desc="Processing files"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            ticker = data.get('ticker')
            if not ticker:
                print(f"⚠️  Skipping {json_file.name}: no ticker found")
                continue
            
            stats['total_files'] += 1
            
            # Process each report type
            sections = data.get('sections', {})
            
            for report_type in ['BALANCE_SHEET', 'INCOME_STATEMENT', 'CASH_FLOW', 'NOTE']:
                if report_type not in sections:
                    continue
                
                section = sections[report_type]
                
                # Check if section has data
                if not isinstance(section, dict) or 'data' not in section:
                    continue
                
                section_data = section['data']
                if not isinstance(section_data, dict):
                    continue
                
                records = data_by_type[report_type]
                
                # Process yearly data (list of dicts)
                years = section_data.get('years')
                if years and isinstance(years, list):
                    for year_data in years:
                        if not isinstance(year_data, dict):
                            continue
                        
                        # Get period (year)
                        period = year_data.get('yearReport')
                        if period is None:
                            continue
                        
                        # Extract all metrics from this year's data
                        for field, value in year_data.items():
                            # Skip metadata fields
                            if field in ['organCode', 'ticker', 'createDate', 'updateDate', 'yearReport', 
                                       'serverDateTime', 'status', 'message', 'code', 'msg', 'exception', 'successful']:
                                continue
                            
                            # Only include numeric values
                            if value is not None and isinstance(value, (int, float)):
                                records.append({
                                    'ticker': ticker,
                                    'period': str(period),
                                    'field': field,
                                    'value': float(value)
                                })
                                stats[f'{report_type}_yearly'] += 1
                
                # Process quarterly data (list of dicts)
                quarters = section_data.get('quarters')
                if quarters and isinstance(quarters, list):
                    for quarter_data in quarters:
                        if not isinstance(quarter_data, dict):
                            continue
                        
                        # Get period (quarter)
                        period = quarter_data.get('quarterReport')
                        if period is None:
                            continue
                        
                        # Extract all metrics from this quarter's data
                        for field, value in quarter_data.items():
                            # Skip metadata fields
                            if field in ['organCode', 'ticker', 'createDate', 'updateDate', 'quarterReport',
                                       'serverDateTime', 'status', 'message', 'code', 'msg', 'exception', 'successful']:
                                continue
                            
                            # Only include numeric values
                            if value is not None and isinstance(value, (int, float)):
                                records.append({
                                    'ticker': ticker,
                                    'period': str(period),
                                    'field': field,
                                    'value': float(value)
                                })
                                stats[f'{report_type}_quarterly'] += 1
        
        except Exception as e:
            print(f"\n⚠️  Error processing {json_file.name}: {e}")
            stats['errors'] += 1
            continue
    
    print(f"\n📊 Statistics:")
    print(f"  - Files processed: {stats['total_files']}")
    
    # Write each report type to separate Arrow IPC file
    print(f"\n💾 Writing Arrow IPC files...")
    # Write each report type to separate Arrow IPC file, partitioned by year
    print(f"\n💾 Writing Arrow IPC files (partitioned by year)...")
    
    output_sizes = {} # This will now store total size per report type
    
    # Process each report type
    for report_type, records in data_by_type.items():
        if not records:
            print(f"  ⚠️  {report_type}: No data, skipping")
            continue
            
        print(f"\nProcessing {report_type} ({len(records)} records)...")
        
        # Group by Year
        records_by_year = defaultdict(list)
        for record in records:
            # Extract year from 'period' field (e.g., '2023' or '2023Q4' -> '2023')
            period = record.get('period')
            if period:
                year = period[:4] # Assuming period starts with year
                records_by_year[year].append(record)
        
        # Create output directory for this report type
        report_dir = OUTPUT_DIR / report_type.lower()
        report_dir.mkdir(parents=True, exist_ok=True)
        
        total_report_type_size = 0
        
        # Save each year as a separate Arrow file
        for year, year_records in records_by_year.items():
            if not year_records:
                continue
                
            # Create Arrow Table
            try:
                table = pa.Table.from_pylist(year_records)
            except Exception as e:
                print(f"  ⚠️ Error creating table for {report_type} {year}: {e}")
                continue
            
            # Output file: e.g., frontend/api_data/balance_sheet/2024.arrow
            output_file = report_dir / f"{year}.arrow"
            
            # Write Uncompressed Arrow (Network Compression will handle it)
            feather.write_feather(
                table, 
                output_file, 
                compression='uncompressed'
            )
            
            size_mb = output_file.stat().st_size / (1024 * 1024)
            total_report_type_size += size_mb
            # print(f"  Saved {output_file.name} ({size_mb:.1f} MB)") # Uncomment for verbose output
            
        output_sizes[report_type] = total_report_type_size
        print(f"  ✅ Saved {len(records_by_year)} year files in {report_dir} (Total: {total_report_type_size:.1f} MB)")
    
    if stats['errors'] > 0:
        print(f"\n  ⚠️  Errors: {stats['errors']}")
    
    return output_sizes


def verify_output():
    """Quick verification of output files"""
    print("\n" + "=" * 60)
    print("🔍 VERIFICATION")
    print("=" * 60)
    
    # Check metadata
    if not OUTPUT_METADATA.exists():
        print("❌ Metadata file not found!")
        return False
    
    with open(OUTPUT_METADATA, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    print(f"\n✅ Metadata: {OUTPUT_METADATA.name}")
    for report_type, fields in metadata.items():
        if fields:
            print(f"   - {report_type}: {len(fields)} fields")
    
    # Check each Arrow IPC file
    print(f"\n✅ Arrow IPC files:")
    
    total_rows = 0
    for report_type, output_file in OUTPUT_FILES.items():
        if not output_file.exists():
            print(f"   ⚠️  {report_type}: File not created (no data)")
            continue
        
        table = feather.read_table(output_file)
        total_rows += table.num_rows
        
        size_mb = output_file.stat().st_size / (1024 * 1024)
        unique_tickers = len(table.column('ticker').unique())
        unique_periods = len(table.column('period').unique())
        unique_fields = len(table.column('field').unique())
        
        print(f"\n   📊 {output_file.name}:")
        print(f"      Size: {size_mb:.1f} MB")
        print(f"      Rows: {table.num_rows:,}")
        print(f"      Tickers: {unique_tickers:,}")
        print(f"      Periods: {unique_periods:,}")
        print(f"      Fields: {unique_fields:,}")
        
        # Sample data
        if table.num_rows > 0:
            print(f"      Sample (first 2 rows):")
            df = table.to_pandas().head(2)
            for idx, row in df.iterrows():
                print(f"        {row['ticker']} | {row['period']} | {row['field']} = {row['value']:,.0f}")
    
    print(f"\n   📈 Total rows across all files: {total_rows:,}")
    
    return True


def main():
    print("=" * 60)
    print("Apache Arrow IPC Conversion - Approach 3")
    print("Separate Metadata + 4 Arrow IPC Files by Report Type")
    print("=" * 60)
    
    # Step 1: Extract metadata
    metadata = extract_metadata()
    
    # Step 2: Convert to separate Parquet files
    output_sizes = convert_to_parquet_by_type()
    
    # Step 3: Verify
    success = verify_output()
    
    if success:
        print("\n" + "=" * 60)
        print("✅ CONVERSION COMPLETE!")
        print("=" * 60)
        
        # Calculate total size and compression ratio
        original_size_mb = sum(f.stat().st_size for f in INPUT_DIR.glob('*.json')) / (1024 * 1024)
        total_new_size_mb = sum(output_sizes.values())
        compression_ratio = (1 - total_new_size_mb / original_size_mb) * 100
        
        print(f"\n📦 Compression Summary:")
        print(f"   Original (JSON):  {original_size_mb:,.1f} MB (1344 files)")
        print(f"   Compressed:       {total_new_size_mb:.1f} MB (4 files)")
        print(f"   Reduction:        {compression_ratio:.1f}%")
        
        print(f"\n📁 Output files:")
        print(f"   - {OUTPUT_METADATA.name}")
        for report_type, output_file in OUTPUT_FILES.items():
            if output_file.exists():
                size_mb = output_file.stat().st_size / (1024 * 1024)
                print(f"   - {output_file.name} ({size_mb:.1f} MB)")
        
        print(f"\n🚀 Next steps:")
        print(f"   1. Upload to R2: python scripts/upload_to_r2.py")
        print(f"   2. Update frontend to use Arrow data")
        print(f"   3. Build bubble chart with high performance!")
    else:
        print("\n❌ Verification failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
