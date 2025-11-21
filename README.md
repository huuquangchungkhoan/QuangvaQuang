# Vietnamese Stock Data API

Automated daily data pipeline for Vietnamese stock market.

## Features

- 📊 Daily screener data for all Vietnamese stocks
- 💼 Open-end fund data with full details  
- 🔄 Auto-refresh daily at 8AM Vietnam time
- ☁️ Serverless architecture

## Endpoints

```
https://screener.lightinvest.vn/screener.json
https://screener.lightinvest.vn/funds.json
```

Cache: 30 minutes

## Tech Stack

- Python 3.12
- vnstock (Vietnamese stock data)
- GitHub Actions (automation)
- Cloudflare R2 + Workers

## License

Private project
