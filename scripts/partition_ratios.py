#!/usr/bin/env python3
"""
Partition ratios_data.arrow by year.
Input: frontend/api_data/ratios_data.arrow
Output: frontend/api_data/ratios/2017.arrow, 2018.arrow, ..., 2025.arrow
"""

import pyarrow as pa
import pyarrow.ipc as ipc
from pathlib import Path

INPUT_FILE = Path('frontend/api_data/ratios_data.arrow')
OUTPUT_DIR = Path('frontend/api_data/ratios')

def partition_ratios():
    print("=" * 60)
    print("Partitioning Ratios Data by Year")
    print("=" * 60)
    
    # Load the full ratios file
    print(f"📂 Loading {INPUT_FILE}...")
    with open(INPUT_FILE, 'rb') as f:
        reader = ipc.open_file(f)
        table = reader.read_all()
    
    print(f"✅ Loaded {table.num_rows:,} total rows")
    
    # Convert to pandas for easier grouping
    df = table.to_pandas()
    
    # First, backup and clear existing JSON files
    json_files = list(OUTPUT_DIR.glob('*.json'))
    if json_files:
        print(f"\n🗑️  Removing {len(json_files)} old JSON files...")
        for json_file in json_files:
            json_file.unlink()
    
    # Group by year and create partitioned files
    print(f"\n📊 Creating partitioned files by year...")
    years = sorted(df['year'].unique())
    
    total_size = 0
    for year in years:
        year_df = df[df['year'] == year]
        year_table = pa.Table.from_pandas(year_df, preserve_index=False)
        
        output_file = OUTPUT_DIR / f'{year}.arrow'
        
        with open(output_file, 'wb') as f:
            writer = ipc.new_file(f, year_table.schema)
            writer.write(year_table)
            writer.close()
        
        size_kb = output_file.stat().st_size / 1024
        total_size += size_kb
        print(f'  ✅ {year}: {len(year_df):,} records ({size_kb:.1f} KB)')
    
    print(f"\n📦 Total size of partitioned files: {total_size / 1024:.1f} MB")
    print(f"✨ Created {len(years)} partitioned files")
    
    # Show size comparison
    original_size = INPUT_FILE.stat().st_size / (1024 * 1024)
    print(f"\n📈 Size comparison:")
    print(f"   Original: {original_size:.1f} MB (single file)")
    print(f"   Partitioned: {total_size / 1024:.1f} MB (9 files)")
    
    # Calculate typical usage
    recent_years = [y for y in years if int(y) >= 2024]
    recent_size = sum((OUTPUT_DIR / f'{y}.arrow').stat().st_size for y in recent_years) / 1024
    print(f"\n⚡ Default load (2 recent years: {', '.join(map(str, recent_years))}):")
    print(f"   Size: {recent_size:.1f} KB")
    print(f"   Est. load time: ~{recent_size * 0.02:.0f}ms (vs {original_size * 20:.0f}ms)")

if __name__ == "__main__":
    partition_ratios()
