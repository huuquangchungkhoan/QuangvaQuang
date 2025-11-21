# GitHub Secrets Setup

## Thêm R2 Credentials vào GitHub

1. Truy cập: https://github.com/huuquangchungkhoan/QuangvaQuang/settings/secrets/actions

2. Click **"New repository secret"** và thêm từng secret:

### Secret 1: R2_ACCOUNT_ID
- Name: `R2_ACCOUNT_ID`
- Value: `560fe29dca4a9c9d5d07ad67abdc6fb4`

### Secret 2: R2_ACCESS_KEY_ID
- Name: `R2_ACCESS_KEY_ID`
- Value: `2fa8a93d3e16bea06d96bedb568daed2`

### Secret 3: R2_SECRET_ACCESS_KEY
- Name: `R2_SECRET_ACCESS_KEY`
- Value: `47cb936768bb3043785b20ff53f905c19cd6ffee7ddaa16a81f5c6e52c5876e7`

### Secret 4: R2_BUCKET_NAME
- Name: `R2_BUCKET_NAME`
- Value: `screener`

### Secret 5: R2_CUSTOM_DOMAIN
- Name: `R2_CUSTOM_DOMAIN`
- Value: `screener.lightinvest.vn`

## Test Workflow

Sau khi thêm secrets:

1. Truy cập: https://github.com/huuquangchungkhoan/QuangvaQuang/actions
2. Click workflow "Daily Screener Data Refresh"
3. Click "Run workflow" → chọn branch `main`
4. Click "Run workflow" để test

## Workflow Schedule

- **Chạy tự động:** Mỗi 30 phút
- **Fetch data:**
  - Screener: ~3-5 giây (1344 stocks, 67 fields)
  - Funds: ~10 phút (58 funds, 10s delay mỗi fund)
  - Companies: ~1-2 phút (1344 companies, parallel 20 workers)
- **Upload R2:** ~30-60 giây
- **Tổng thời gian:** ~12-14 phút mỗi lần chạy

## API Endpoints (sau khi workflow chạy)

```bash
# Screener
https://screener.lightinvest.vn/screener.json

# Funds
https://screener.lightinvest.vn/funds.json

# Company details
https://screener.lightinvest.vn/companies/VCB.json
https://screener.lightinvest.vn/companies/HPG.json
https://screener.lightinvest.vn/companies/{TICKER}.json
```

## Kiểm tra cache

```bash
# Check cache headers
curl -I https://screener.lightinvest.vn/screener.json
# Expect: Cache-Control: public, max-age=1800 (30 minutes)

curl -I https://screener.lightinvest.vn/companies/VCB.json
# Expect: Cache-Control: public, max-age=3600 (1 hour)
```
