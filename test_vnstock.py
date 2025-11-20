#!/usr/bin/env python3
"""Quick test to verify vnstock is working"""

from vnstock import Screener, Listing

print("🧪 Testing vnstock installation...")
print("="*50)

# Test 1: Screener
print("\n1️⃣ Testing Screener()...")
screener = Screener()
sample = screener.stock(params={"exchangeName": "HOSE"}, limit=5)
print(f"✅ Fetched {len(sample)} stocks from HOSE")
print(f"Columns: {list(sample.columns)[:10]}...")

# Test 2: Listing
print("\n2️⃣ Testing Listing()...")
lst = Listing()
vn30 = lst.symbols_by_group('VN30')
print(f"✅ VN30 has {len(vn30)} stocks")
print(f"Sample: {list(vn30)[:5]}")

print("\n" + "="*50)
print("✅ All tests passed! vnstock is working correctly.")
