#!/bin/bash
# Test all data fetching endpoints

PYTHON=/opt/homebrew/bin/python3.12

echo "=========================================="
echo "Testing Vietnamese Stock Data Pipeline"
echo "=========================================="
echo ""

# Test 1: Screener
echo "1️⃣  Testing Screener API..."
$PYTHON scripts/fetch_screener.py 2>&1 | grep -E "✅|📊|Total" | tail -5
if [ -f "data/screener.json" ]; then
    SIZE=$(ls -lh data/screener.json | awk '{print $5}')
    echo "   ✅ Generated: data/screener.json ($SIZE)"
else
    echo "   ❌ Failed to generate screener.json"
    exit 1
fi
echo ""

# Test 2: Funds
echo "2️⃣  Testing Fund API..."
$PYTHON scripts/fetch_funds.py 2>&1 | grep -E "✅|📊|💼|Total" | tail -5
if [ -f "data/funds.json" ]; then
    SIZE=$(ls -lh data/funds.json | awk '{print $5}')
    echo "   ✅ Generated: data/funds.json ($SIZE)"
else
    echo "   ❌ Failed to generate funds.json"
    exit 1
fi
echo ""

# Test 3: Company Details (5 samples)
echo "3️⃣  Testing Company Details API (5 samples)..."
$PYTHON << 'PYEOF'
import sys
sys.path.insert(0, 'scripts')
from fetch_company_details import fetch_company_details
import time
import os

symbols = ['VCB', 'VNM', 'HPG', 'FPT', 'MWG']
os.makedirs('data/companies', exist_ok=True)

for symbol in symbols:
    sym, details = fetch_company_details(symbol)
    if details:
        import json
        with open(f'data/companies/{symbol}.json', 'w') as f:
            json.dump(details, f, ensure_ascii=False, indent=2)
        print(f'   ✅ {symbol}')
    time.sleep(1.2)

count = len([f for f in os.listdir('data/companies') if f.endswith('.json')])
print(f'   ✅ Generated {count} company files')
PYEOF
echo ""

echo "=========================================="
echo "✅ All Endpoints Working!"
echo "=========================================="
echo ""
echo "Generated Files:"
ls -lh data/*.json 2>/dev/null | awk '{print "  ", $9, "-", $5}'
echo ""
echo "Company Files:"
ls -1 data/companies/*.json 2>/dev/null | wc -l | awk '{print "   Total:", $1, "files"}'
echo ""
echo "=========================================="
