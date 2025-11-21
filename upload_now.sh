#!/bin/bash
# Upload all data to R2 immediately

# Load credentials from .env file if exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Run upload
/opt/homebrew/bin/python3.12 scripts/upload_to_r2.py

echo ""
echo "✅ Upload complete! Check your data at:"
echo "https://screener.lightinvest.vn/screener.json"
echo "https://screener.lightinvest.vn/funds.json"
echo "https://screener.lightinvest.vn/companies/VCB.json"
