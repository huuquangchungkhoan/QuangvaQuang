# Vnstock Screener - AI Agent Guide

## Project Overview
**Serverless stock screener** for Vietnamese stock market using **vnstock** + **GitHub Actions** + **Cloudflare R2/Workers**. Data refreshes daily via automated pipeline.

## Core Architecture Flow

```
GitHub Actions (daily 8AM UTC+7)
  └─> scripts/fetch_screener.py
      ├─> Fetch from vnstock Screener() API (HOSE/HNX/UPCOM)
      ├─> Add index membership (VN30, VN100, HNX30, etc.)
      └─> Generate: data/screener.json (~1-2MB)
  └─> scripts/upload_to_r2.py
      └─> Upload to Cloudflare R2 bucket
      
Cloudflare R2 Storage
  └─> screener.json (public object)
  
Cloudflare Worker
  └─> GET /api/screener → serve JSON from R2
  
Frontend
  └─> Fetch /api/screener → filter locally (client-side)
```

### Data Sources & Libraries
- **vnstock 3.2.6+**: Primary data source (TCBS provider)
  - `Screener()`: 80+ metrics for all Vietnamese stocks
  - `Listing()`: Index membership data (VN30, VN100, etc.)
  - Exchanges: HOSE, HNX, UPCOM
  
### Storage Strategy (Serverless)
- **screener.json**: Single JSON file (~1-2MB) with ALL stocks
- **R2 Bucket**: Cloudflare object storage (S3-compatible)
- **No database**: Frontend filters locally for instant UX

### Key Components

1. **scripts/fetch_screener.py**: Data fetcher (runs on GitHub Actions)
   - Fetches stocks from all 3 exchanges in parallel
   - Adds index membership for each stock
   - Exports to `data/screener.json`
   - Rate limiting: exponential backoff (1s → 2s → 4s)

2. **scripts/upload_to_r2.py**: R2 uploader
   - Uses boto3 (S3-compatible API)
   - Uploads JSON with public-read ACL
   - Sets cache headers (1 hour)

3. **workers/api.js**: Cloudflare Worker API
   - Routes: `/api/screener`, `/health`
   - Fetches from R2 bucket
   - CORS enabled for frontend
   - CDN caching: 1 hour

4. **.github/workflows/daily-refresh.yml**: Automation
   - Schedule: 8AM UTC+7 daily (1AM UTC)
   - Steps: checkout → install deps → fetch → upload
   - Manual trigger available

## Critical Patterns

### Parallel Exchange Loading
Fetch HOSE, HNX, UPCOM concurrently:
```python
import concurrent.futures

exchanges = ['HOSE', 'HNX', 'UPCOM']
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(fetch_exchange_data, ex) for ex in exchanges]
    for future in concurrent.futures.as_completed(futures):
        df = future.result()
        all_stocks.append(df)
```

### Rate Limit Handling
```python
# Exponential backoff retry
max_retries = 3
for attempt in range(max_retries):
    try:
        df = screener.stock(params={"exchangeName": exchange}, limit=1000)
        break
    except Exception as e:
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # 1s, 2s, 4s
            time.sleep(wait_time)
```

### Index Membership Enrichment
Add VN30, VN100, etc. to each stock:
```python
from vnstock import Listing

indexes = ['VN30', 'VN100', 'HNX30', 'UPCOM']
for index_name in indexes:
    tickers = Listing().symbols_by_group(index_name)
    index_membership[index_name] = tickers

# Add to stocks
screener_df['indexes'] = screener_df['ticker'].apply(
    lambda t: [idx for idx, tickers in index_membership.items() if t in tickers]
)
```

## GitHub Actions Setup

### Workflow Configuration
**File**: `.github/workflows/daily-refresh.yml`
- **Schedule**: `0 1 * * *` (8AM UTC+7 / 1AM UTC)
- **Manual trigger**: `workflow_dispatch` enabled

### Required GitHub Secrets
Add in repository Settings → Secrets and variables → Actions:
```
R2_ACCOUNT_ID         # Cloudflare account ID
R2_ACCESS_KEY_ID      # R2 API access key
R2_SECRET_ACCESS_KEY  # R2 API secret key
R2_BUCKET_NAME        # R2 bucket name (e.g., vietcap-data)
```

### Pipeline Steps
1. Checkout code
2. Setup Python 3.11
3. Install: `vnstock3`, `boto3`, `pandas`
4. Run: `python scripts/fetch_screener.py`
5. Run: `python scripts/upload_to_r2.py`
6. Upload artifact (optional backup)

### R2 Endpoint URLs
```
S3 API: https://{ACCOUNT_ID}.r2.cloudflarestorage.com
Public: https://{BUCKET}.{ACCOUNT_ID}.r2.dev/screener.json
```

## Common Tasks

### Manual Workflow Trigger
```bash
# Via GitHub UI
GitHub → Actions → "Daily Screener Data Refresh" → Run workflow

# Via gh CLI
gh workflow run daily-refresh.yml
```

### Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Fetch data (creates data/screener.json)
python scripts/fetch_screener.py

# Upload to R2 (requires env vars)
export R2_ACCOUNT_ID=...
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export R2_BUCKET_NAME=vietcap-data
python scripts/upload_to_r2.py
```

### Deploy Cloudflare Worker
```bash
cd workers
npm install -g wrangler
wrangler login
wrangler deploy
```

### Verify Data
```bash
# Check JSON structure
cat data/screener.json | jq '.total_stocks'

# Test Worker endpoint
curl https://your-worker.workers.dev/api/screener | jq '.total_stocks'
```

## Data Structure

### screener.json Format
```json
{
  "last_updated": "2025-11-20T08:00:00+07:00",
  "total_stocks": 1654,
  "exchanges": {
    "HOSE": 412,
    "HNX": 384,
    "UPCOM": 858
  },
  "stocks": [
    {
      "ticker": "VNM",
      "company_name": "Vinamilk",
      "exchange": "HOSE",
      "industry": "Thực phẩm và đồ uống",
      "indexes": ["VN30", "VN100", "VNAllShare"],
      "market_cap": 145000,
      "price": 65000,
      "pe": 15.2,
      "pb": 3.8,
      "roe": 25.4,
      "roa": 18.3,
      "gross_margin": 42.5,
      "net_margin": 18.7,
      "revenue_growth_1y": 8.5,
      "eps_growth_1y": 12.3,
      "dividend_yield": 4.5,
      "rsi14": 55.3,
      "price_vs_sma20": "Giá nằm trên SMA(20)",
      "avg_trading_value_20d": 125000000
    }
  ]
}
```

### Filtering Strategy (Frontend)
```javascript
// Frontend filters locally (no backend needed)
const filteredStocks = stocks.filter(stock => {
  return stock.exchange === 'HOSE' &&
         stock.pe > 0 && stock.pe < 20 &&
         stock.roe > 15 &&
         stock.indexes.includes('VN30');
});
```

## Performance Optimizations

### Parallel Exchange Loading
- ThreadPoolExecutor with max_workers=3
- Concurrent fetching: HOSE, HNX, UPCOM
- Total fetch time: ~30-60 seconds

### Index Membership Caching
- Parallel fetch with max_workers=5
- 14 indexes loaded concurrently
- Total time: ~20-30 seconds

### Data Size & Bandwidth
- JSON file: 1-2MB compressed
- ~1600 stocks with 20+ fields each
- Cloudflare CDN: global edge caching
- Frontend download: <1 second

### Cache Strategy
```
GitHub Actions: Daily refresh (8AM)
  ↓
R2 Storage: Immutable JSON file
  ↓
Cloudflare CDN: 1-hour cache
  ↓
Frontend: LocalStorage cache (optional)
```

## Error Handling

### Script Failures
```python
# Exponential backoff for API timeouts
# Max 3 retries per exchange
# Log with emoji prefixes: ✅ ⚠️ ❌
# Exit code 1 on fatal errors (fails workflow)
```

### Missing Data
- Inactive stocks filtered out (no price & no market_cap)
- `null` values for missing metrics
- Frontend should handle gracefully

### Workflow Monitoring
- Check GitHub Actions logs
- Review artifact uploads (7-day retention)
- Monitor Cloudflare Worker analytics

## References
- vnstock docs: https://vnstocks.com/docs/vnstock/bo-loc-co-phieu-vnstock
- Cloudflare R2: S3-compatible API (use boto3)
