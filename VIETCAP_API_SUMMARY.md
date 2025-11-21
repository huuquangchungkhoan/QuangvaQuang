# Vietcap API Summary

## Available Endpoints (Verified Working)

### 1. Company Overview
```
GET https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{TICKER}
```
**Data:**
- Basic info: ticker, name (EN/VN), sector
- Pricing: currentPrice, targetPrice, rating (BUY/HOLD/SELL)
- Analyst info: analyst name, rating date
- Market data: marketCap, numberOfShares
- Ownership: foreignerPercentage, statePercentage, maximumForeignPercentage
- Profile: enProfile, profile (HTML formatted)
- Trading: averageMatchValue1Month, averageMatchVolume1Month
- Index membership: inCu (VN30)
- Classification: icbCodeLv2, icbCodeLv4, comTypeCode, comGroupCode
- Free float: freeFloat, freeFloatPercentage
- Listing: listingDate
- Previous insights: prevInsight (previous rating/target)

### 2. Daily Foreign Flow
```
GET https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{TICKER}/daily-info
```
**Data:**
- Foreign net buying/selling by date (1+ year history)
- Format: `[{ "tradingDate": "2025-11-20T00:00:00", "foreignNet": -22106645600.0 }]`

### 3. P/E & P/B History
```
GET https://iq.vietcap.com.vn/api/iq-insight-service/v1/company-ratio-daily/{TICKER}?lengthReport=10
```
**Data:**
- Historical P/E and P/B ratios by trading date
- 10+ years of data
- Format: `[{ "pe": 25.167559251, "pb": 2.8383919911, "tradingDate": "2015-11-23T00:00:00" }]`

### 4. Financial Data (CURRENTLY EMPTY)
```
GET https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{TICKER}/financial-data
```
**Status:** Returns all null values - likely requires authentication or different access

## Non-Existent Endpoints (404)
- `/company/{TICKER}/shareholders` - NOT FOUND
- `/company/{TICKER}/officers` - NOT FOUND
- `/company/{TICKER}/dividends` - NOT FOUND

## Key Advantages
✅ **NO RATE LIMIT** - Can fetch all 1622 stocks quickly
✅ **Fast Response** - ~200ms per request
✅ **High Quality Data** - Analyst ratings, target prices, foreign flow
✅ **Simple REST API** - No authentication needed for public endpoints

## Limitations
❌ No shareholders/officers data
❌ No dividend history
❌ No financial statements (income/balance/cashflow)
❌ No news/events data
❌ Financial-data endpoint returns null

## Recommendation for Project
**Use Vietcap API for:**
1. Company overview (rating, target price, profile)
2. Foreign flow analysis (unique data)
3. P/E/P/B trend analysis

**Missing data that would need vnstock:**
- Shareholders & officers
- Dividends
- Financial statements
- News & events
- Insider deals

## Final Decision
**Option 1: Vietcap Only** (Fast but limited)
- 1622 stocks × 0.2s = ~5-6 minutes
- Data: overview + foreign flow + PE/PB history
- Missing: financial details

**Option 2: Hybrid** (Best of both)
- Vietcap: Overview + foreign flow (ALL stocks, no rate limit)
- vnstock: Only for detailed financials when needed
- Trade-off: More complex code but complete data

**Option 3: vnstock Only** (Complete but slow)
- All data available
- 1622 stocks × 10s = 4.5 hours
- High rate limit risk
