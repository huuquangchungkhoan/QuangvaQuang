#!/usr/bin/env python3
"""
Fetch all open-end fund data from vnstock and save to JSON
Includes: listing, NAV history, top holdings, industry allocation, asset allocation
Designed to run on GitHub Actions daily
"""
import json
import time
from datetime import datetime
import concurrent.futures
import pandas as pd
from vnstock import Fund
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_fund_details(symbol, fund_id):
    """Fetch detailed data for a single fund"""
    try:
        fund = Fund()
        details = {}
        
        # NAV report (price history)
        try:
            nav_df = fund.details.nav_report(symbol)
            if not nav_df.empty:
                details['nav_history'] = nav_df.to_dict('records')
                details['latest_nav'] = float(nav_df.iloc[-1]['nav_per_unit']) if len(nav_df) > 0 else None
        except Exception as e:
            logger.warning(f"⚠️  No NAV data for {symbol}: {e}")
            details['nav_history'] = []
            details['latest_nav'] = None
        
        # Top holdings (largest positions)
        try:
            holdings_df = fund.details.top_holding(symbol)
            if not holdings_df.empty:
                details['top_holdings'] = holdings_df.to_dict('records')
        except Exception as e:
            logger.warning(f"⚠️  No holdings data for {symbol}: {e}")
            details['top_holdings'] = []
        
        # Industry allocation
        try:
            industry_df = fund.details.industry_holding(symbol)
            if not industry_df.empty:
                details['industry_allocation'] = industry_df.to_dict('records')
        except Exception as e:
            logger.warning(f"⚠️  No industry data for {symbol}: {e}")
            details['industry_allocation'] = []
        
        # Asset allocation
        try:
            asset_df = fund.details.asset_holding(symbol)
            if not asset_df.empty:
                details['asset_allocation'] = asset_df.to_dict('records')
        except Exception as e:
            logger.warning(f"⚠️  No asset data for {symbol}: {e}")
            details['asset_allocation'] = []
        
        logger.info(f"✅ Fetched details for {symbol}")
        return symbol, details
        
    except Exception as e:
        logger.error(f"❌ Error fetching {symbol}: {e}")
        return symbol, None


def convert_to_json_safe(df, fund_details_map):
    """Convert DataFrame to JSON-safe list with all fund details"""
    funds = []
    
    for _, row in df.iterrows():
        symbol = row.get('short_name') or row.get('fund_code')
        fund_id = row.get('fund_id_fmarket') or row.get('id')
        
        fund_data = {
            'symbol': symbol,
            'fund_id': int(fund_id) if pd.notna(fund_id) else None,
            'name': row.get('name'),
            'fund_type': row.get('fund_type'),
            'fund_owner_name': row.get('fund_owner_name'),
            'fund_code': row.get('fund_code'),
            'vsd_fee_id': row.get('vsd_fee_id'),
            'nav_update_at': row.get('nav_update_at'),
            
            # Add detailed data
            'latest_nav': None,
            'nav_history': [],
            'top_holdings': [],
            'industry_allocation': [],
            'asset_allocation': []
        }
        
        # Merge with fetched details
        if symbol in fund_details_map and fund_details_map[symbol]:
            fund_data.update(fund_details_map[symbol])
        
        funds.append(fund_data)
    
    return funds


def main():
    """Main execution function"""
    try:
        start_time = time.time()
        logger.info("🚀 Starting fund data fetch...")
        
        # Initialize Fund API
        fund = Fund()
        
        # Fetch all funds listing
        logger.info("📡 Fetching all funds listing...")
        all_funds_df = fund.listing()
        
        if all_funds_df.empty:
            raise ValueError("No funds data returned from API")
        
        logger.info(f"✅ Fetched {len(all_funds_df)} funds")
        
        # Fetch detailed data for each fund in parallel
        logger.info("📊 Fetching detailed data for all funds...")
        fund_details_map = {}
        
        # Get list of fund symbols
        symbols = []
        fund_ids = []
        for _, row in all_funds_df.iterrows():
            symbol = row.get('short_name') or row.get('fund_code')
            fund_id = row.get('fund_id_fmarket') or row.get('id')
            if symbol:
                symbols.append(symbol)
                fund_ids.append(fund_id)
        
        # Parallel fetch with max 5 workers to avoid rate limits
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_fund_details, symbol, fund_id) 
                      for symbol, fund_id in zip(symbols, fund_ids)]
            
            for future in concurrent.futures.as_completed(futures):
                symbol, details = future.result()
                if details:
                    fund_details_map[symbol] = details
        
        logger.info(f"✅ Fetched details for {len(fund_details_map)} funds")
        
        # Convert to JSON format
        logger.info("📝 Converting to JSON format...")
        funds = convert_to_json_safe(all_funds_df, fund_details_map)
        
        # Prepare output
        output = {
            'last_updated': datetime.now().isoformat(),
            'total_funds': len(funds),
            'fund_types': {
                'STOCK': len([f for f in funds if f.get('fund_type') == 'Quỹ cổ phiếu']),
                'BOND': len([f for f in funds if f.get('fund_type') == 'Quỹ trái phiếu']),
                'BALANCED': len([f for f in funds if f.get('fund_type') == 'Quỹ cân bằng']),
            },
            'funds': funds
        }
        
        # Save to file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        output_file = os.path.join(data_dir, 'funds.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Successfully saved {len(funds)} funds")
        logger.info(f"⏱️  Total time: {elapsed:.2f}s")
        
        # Print summary
        print("\n" + "="*50)
        print(f"✅ Fund data fetch completed!")
        print(f"📊 Total funds: {len(funds)}")
        print(f"💼 Stock funds: {output['fund_types']['STOCK']}")
        print(f"📈 Bond funds: {output['fund_types']['BOND']}")
        print(f"⚖️  Balanced funds: {output['fund_types']['BALANCED']}")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()
