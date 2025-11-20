# Vnstock Screener Backend - GitHub Actions + R2

## 🏗️ Architecture

```
GitHub Actions (daily 8AM UTC+7)
  └─> scripts/fetch_screener.py
      └─> Fetch stocks from vnstock (HOSE/HNX/UPCOM)
      └─> Add index membership (VN30, VN100, etc.)
      └─> Save: data/screener.json
  └─> scripts/upload_to_r2.py
      └─> Upload screener.json to Cloudflare R2
      
Cloudflare R2 Bucket
  └─> screener.json (public URL)
  
Cloudflare Worker
  └─> GET /api/screener → serve screener.json
  
Frontend
  └─> Fetch /api/screener
  └─> Filter locally (industry, pe, roe, market_cap, etc.)
```

## 📊 Data Structure

### screener.json
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
      "dividend_yield": 4.5
    }
  ]
}
```

## 🚀 Setup

### 1. GitHub Secrets
Add to repository Settings → Secrets and variables → Actions:
```
R2_ACCOUNT_ID=your-cloudflare-account-id
R2_ACCESS_KEY_ID=your-r2-access-key
R2_SECRET_ACCESS_KEY=your-r2-secret-key
R2_BUCKET_NAME=vietcap-data
```

### 2. Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Fetch data
python scripts/fetch_screener.py

# Upload to R2 (requires env vars)
export R2_ACCOUNT_ID=...
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export R2_BUCKET_NAME=vietcap-data
python scripts/upload_to_r2.py
```

### 3. Manual Trigger Workflow
GitHub → Actions → "Daily Screener Data Refresh" → Run workflow

## 📁 Project Structure

```
.
├── .github/
│   └── workflows/
│       └── daily-refresh.yml    # GitHub Actions workflow
├── scripts/
│   ├── fetch_screener.py        # Fetch vnstock data → JSON
│   └── upload_to_r2.py          # Upload JSON to R2
├── data/
│   └── screener.json            # Generated output (gitignored)
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## 🔧 Cloudflare Worker (Example)

```javascript
// workers/api.js
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // CORS headers
    const headers = {
      'Access-Control-Allow-Origin': '*',
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=3600'
    };
    
    if (url.pathname === '/api/screener') {
      // Fetch screener.json from R2
      const object = await env.R2_BUCKET.get('screener.json');
      
      if (object === null) {
        return new Response('Not found', { status: 404 });
      }
      
      return new Response(await object.text(), { headers });
    }
    
    return new Response('Not found', { status: 404 });
  }
}
```

Deploy:
```bash
npm install -g wrangler
wrangler login
wrangler deploy
```

## 📝 Notes

### Rate Limiting
- vnstock API has rate limits (~100 requests/minute)
- Script uses parallel loading with exponential backoff
- Max 3 retries per exchange

### Data Freshness
- GitHub Actions runs daily at 8AM UTC+7
- Cloudflare Workers cache: 1 hour
- Frontend should cache locally

### Filtering Strategy
- **Backend**: Provide complete dataset (~1-2MB JSON)
- **Frontend**: Filter client-side for instant UX
- Supports complex filters: industry, pe range, roe > x, indexes

### Missing Data Handling
- Inactive stocks filtered out (no price & no market_cap)
- `null` values for missing metrics
- Frontend should handle gracefully

## 🔍 Troubleshooting

### Workflow fails
1. Check GitHub Actions logs
2. Verify secrets are set correctly
3. Test scripts locally first

### Upload fails
1. Verify R2 bucket exists
2. Check R2 API credentials
3. Ensure bucket has public access policy

### Data incomplete
1. Check vnstock API status
2. Increase retry attempts in fetch script
3. Review workflow logs for specific exchange failures

## 📚 References
- vnstock docs: https://vnstocks.com/docs/vnstock/bo-loc-co-phieu-vnstock
- Cloudflare R2: https://developers.cloudflare.com/r2/
- Cloudflare Workers: https://developers.cloudflare.com/workers/
