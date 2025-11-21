#!/usr/bin/env python3
"""
Fetch detailed company information for each stock ticker
Creates individual JSON file per ticker
Rate limit: 50 requests per minute
Runs at 5PM daily
"""
import json
import time
import os
from datetime import datetime
from vnstock import Company, Screener
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_company_details(symbol):
    """Fetch all available company information for a symbol"""
    try:
        company = Company(symbol=symbol, source='TCBS')
        details = {
            'symbol': symbol,
            'last_updated': datetime.now().isoformat(),
        }
        
        # Overview
        try:
            overview_df = company.overview()
            if not overview_df.empty:
                details['overview'] = overview_df.to_dict('records')[0]
        except Exception as e:
            logger.warning(f"No overview for {symbol}")
            details['overview'] = None
        
        # Profile
        try:
            profile_df = company.profile()
            if not profile_df.empty:
                details['profile'] = profile_df.to_dict('records')[0]
        except Exception as e:
            logger.warning(f"No profile for {symbol}")
            details['profile'] = None
        
        # Shareholders
        try:
            shareholders_df = company.shareholders()
            if not shareholders_df.empty:
                details['shareholders'] = shareholders_df.to_dict('records')
        except Exception as e:
            details['shareholders'] = []
        
        # Officers
        try:
            officers_df = company.officers()
            if not officers_df.empty:
                details['officers'] = officers_df.to_dict('records')
        except Exception as e:
            details['officers'] = []
        
        # Subsidiaries
        try:
            subsidiaries_df = company.subsidiaries()
            if not subsidiaries_df.empty:
                details['subsidiaries'] = subsidiaries_df.to_dict('records')
        except Exception as e:
            details['subsidiaries'] = []
        
        # Dividends
        try:
            dividends_df = company.dividends()
            if not dividends_df.empty:
                details['dividends'] = dividends_df.to_dict('records')
        except Exception as e:
            details['dividends'] = []
        
        # Events
        try:
            events_df = company.events()
            if not events_df.empty:
                details['events'] = events_df.to_dict('records')
        except Exception as e:
            details['events'] = []
        
        # Insider deals
        try:
            insider_df = company.insider_deals()
            if not insider_df.empty:
                details['insider_deals'] = insider_df.to_dict('records')
        except Exception as e:
            details['insider_deals'] = []
        
        # News
        try:
            news_df = company.news()
            if not news_df.empty:
                # Limit to latest 20 news
                details['news'] = news_df.head(20).to_dict('records')
        except Exception as e:
            details['news'] = []
        
        logger.info(f"✅ {symbol}")
        return symbol, details
        
    except Exception as e:
        logger.error(f"❌ {symbol}: {e}")
        return symbol, None


def main():
    """Main execution function"""
    try:
        start_time = time.time()
        logger.info("🚀 Starting company details fetch...")
        
        # Get list of all stocks from screener
        logger.info("📡 Fetching stock list...")
        screener = Screener()
        
        # Fetch all stocks
        all_stocks = []
        for exchange in ['HOSE', 'HNX', 'UPCOM']:
            logger.info(f"Loading {exchange}...")
            df = screener.stock(params={"exchangeName": exchange}, limit=1000)
            if not df.empty:
                all_stocks.extend(df['ticker'].tolist())
        
        logger.info(f"✅ Found {len(all_stocks)} stocks")
        
        # Create output directory structure
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        companies_dir = os.path.join(project_root, 'data', 'companies')
        os.makedirs(companies_dir, exist_ok=True)
        
        # Rate limiting: 50 requests per minute = 1 request per 1.2 seconds
        request_delay = 1.2
        success_count = 0
        fail_count = 0
        
        logger.info(f"📊 Fetching details for {len(all_stocks)} companies")
        logger.info(f"⏱️  Rate limit: 50 requests/minute ({request_delay}s/request)")
        
        for idx, symbol in enumerate(all_stocks, 1):
            logger.info(f"[{idx}/{len(all_stocks)}] Fetching {symbol}...")
            
            symbol, details = fetch_company_details(symbol)
            
            if details:
                # Save individual file
                output_file = os.path.join(companies_dir, f"{symbol}.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(details, f, ensure_ascii=False, indent=2)
                success_count += 1
            else:
                fail_count += 1
            
            # Rate limiting
            if idx < len(all_stocks):  # Don't sleep after last request
                time.sleep(request_delay)
            
            # Progress update every 50 stocks
            if idx % 50 == 0:
                elapsed = time.time() - start_time
                logger.info(f"Progress: {idx}/{len(all_stocks)} ({success_count} success, {fail_count} failed) - {elapsed:.1f}s elapsed")
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Completed: {success_count} success, {fail_count} failed")
        logger.info(f"⏱️  Total time: {elapsed:.2f}s ({elapsed/60:.1f} minutes)")
        
        # Print summary
        print("\n" + "="*50)
        print(f"✅ Company details fetch completed!")
        print(f"📊 Total companies: {len(all_stocks)}")
        print(f"✅ Success: {success_count}")
        print(f"❌ Failed: {fail_count}")
        print(f"⏱️  Time: {elapsed/60:.1f} minutes")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()
