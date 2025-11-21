#!/usr/bin/env python3
"""
Hybrid fund fetcher:
1. Get fund list from vnstock (complete list with codes)
2. Get detailed NAV + info from fmarket API
Best of both worlds!
"""
import json
import os
import time
import requests
from datetime import datetime
from vnstock import Fund
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_vnstock_fund_list():
    """Get complete fund list from vnstock"""
    try:
        fund = Fund()
        df = fund.listing()
        
        if df is not None and not df.empty:
            fund_codes = df['short_name'].tolist()
            logger.info(f"✅ vnstock: Found {len(fund_codes)} funds")
            return fund_codes
        return []
    except Exception as e:
        logger.error(f"Failed to get vnstock fund list: {e}")
        return []


def get_fmarket_fund_details(fund_code):
    """Get detailed fund information from fmarket API"""
    url = f'https://api.fmarket.vn/home/product/{fund_code}'
    headers = {
        'accept': 'application/json, text/plain, */*',
        'f-language': 'vi',
        'origin': 'https://fmarket.vn',
        'referer': 'https://fmarket.vn/',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') == 200 and data.get('data'):
            return data['data']
        return None
    except Exception as e:
        logger.warning(f"fmarket API failed for {fund_code}: {e}")
        return None


def get_vnstock_fund_details(fund_code):
    """Fallback: Get fund details from vnstock if fmarket fails"""
    try:
        fund = Fund()
        # Get NAV history
        nav_df = fund.nav_history(fund_code)
        
        if nav_df is not None and not nav_df.empty:
            latest = nav_df.iloc[0]
            return {
                'source': 'vnstock',
                'nav': latest.get('nav'),
                'nav_change': latest.get('nav_change_percent'),
                'trading_date': latest.get('trading_date')
            }
        return None
    except Exception as e:
        logger.warning(f"vnstock fallback failed for {fund_code}: {e}")
        return None


def convert_fund_data(fund_code, fmarket_data=None, vnstock_data=None):
    """Convert fund data to unified format"""
    result = {
        'code': fund_code,
        'last_updated': datetime.now().isoformat()
    }
    
    # Priority: fmarket > vnstock
    if fmarket_data:
        result.update({
            'name': fmarket_data.get('name'),
            'short_name': fmarket_data.get('shortName'),
            'nav': fmarket_data.get('nav'),
            'nav_change': round((fmarket_data.get('nav', 0) - fmarket_data.get('lastYearNav', 0)) / fmarket_data.get('lastYearNav', 1) * 100, 2) if fmarket_data.get('lastYearNav') else 0,
            'management_fee': fmarket_data.get('managementFee'),
            'performance_fee': fmarket_data.get('performanceFee'),
            'min_investment': fmarket_data.get('buyMinValue'),
            'description': fmarket_data.get('description'),
            'website': fmarket_data.get('website'),
            'manager': {
                'name': fmarket_data.get('owner', {}).get('name'),
                'short_name': fmarket_data.get('owner', {}).get('shortName'),
                'website': fmarket_data.get('owner', {}).get('website'),
                'email': fmarket_data.get('owner', {}).get('email')
            },
            'asset_allocation': [
                model.get('name') for model in fmarket_data.get('productAssetAllocationModelList', [])
            ],
            'holding_volume': fmarket_data.get('holdingVolume'),
            'first_issue_date': datetime.fromtimestamp(fmarket_data.get('firstIssueAt', 0) / 1000).strftime('%Y-%m-%d') if fmarket_data.get('firstIssueAt') else None,
            'data_source': 'fmarket'
        })
    elif vnstock_data:
        result.update({
            'nav': vnstock_data.get('nav'),
            'nav_change': vnstock_data.get('nav_change'),
            'trading_date': vnstock_data.get('trading_date'),
            'data_source': 'vnstock'
        })
    
    return result


def main():
    try:
        start_time = time.time()
        logger.info("🚀 Starting hybrid fund data fetch...")
        
        # Step 1: Get fund list from vnstock
        logger.info("📡 Step 1: Getting fund list from vnstock...")
        fund_codes = get_vnstock_fund_list()
        
        if not fund_codes:
            logger.error("No funds found from vnstock!")
            return
        
        logger.info(f"✅ Found {len(fund_codes)} funds from vnstock")
        
        # Step 2: Get details from fmarket (with vnstock fallback)
        logger.info(f"📊 Step 2: Fetching details from fmarket API...")
        
        all_funds_data = []
        fmarket_success = 0
        vnstock_fallback = 0
        fail_count = 0
        
        for idx, fund_code in enumerate(fund_codes, 1):
            # Try fmarket first
            fmarket_data = get_fmarket_fund_details(fund_code)
            vnstock_data = None
            
            if fmarket_data:
                fund_data = convert_fund_data(fund_code, fmarket_data=fmarket_data)
                logger.info(f"✅ [fmarket] {fund_code} ({idx}/{len(fund_codes)})")
                fmarket_success += 1
            else:
                # Fallback to vnstock
                vnstock_data = get_vnstock_fund_details(fund_code)
                if vnstock_data:
                    fund_data = convert_fund_data(fund_code, vnstock_data=vnstock_data)
                    logger.info(f"⚠️  [vnstock] {fund_code} ({idx}/{len(fund_codes)})")
                    vnstock_fallback += 1
                else:
                    logger.error(f"❌ {fund_code} - both sources failed")
                    fail_count += 1
                    continue
            
            all_funds_data.append(fund_data)
            
            # Small delay to be polite
            time.sleep(0.5)
        
        # Save to file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        output_dir = os.path.join(project_root, 'data')
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, 'funds.json')
        
        output_data = {
            'total_funds': len(all_funds_data),
            'last_updated': datetime.now().isoformat(),
            'data_sources': {
                'fmarket': fmarket_success,
                'vnstock': vnstock_fallback
            },
            'funds': all_funds_data
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        elapsed = time.time() - start_time
        
        logger.info(f"✅ Successfully saved {len(all_funds_data)} funds")
        logger.info(f"📊 fmarket: {fmarket_success}, vnstock: {vnstock_fallback}, failed: {fail_count}")
        logger.info(f"⏱️  Total time: {elapsed:.2f}s ({elapsed/60:.1f} minutes)")
        
        print("\n" + "="*50)
        print(f"✅ Hybrid fund data fetch completed!")
        print(f"📊 Total funds: {len(all_funds_data)}")
        print(f"📡 From fmarket: {fmarket_success}")
        print(f"📡 From vnstock: {vnstock_fallback}")
        print(f"❌ Failed: {fail_count}")
        print(f"⏱️  Time: {elapsed:.2f}s ({elapsed/60:.1f} min)")
        print(f"💾 Saved to: {output_file}")
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
