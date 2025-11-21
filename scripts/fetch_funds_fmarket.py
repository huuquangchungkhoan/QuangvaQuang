#!/usr/bin/env python3
"""
Fetch fund data from fmarket.vn API (alternative to vnstock)
NO RATE LIMIT - much faster than vnstock!
"""
import json
import os
import time
import requests
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_all_funds():
    """Get hardcoded list of all available funds from fmarket.vn"""
    # Complete list of funds from fmarket.vn (as of Nov 2025)
    fund_codes = [
        # Quỹ cổ phiếu
        'BVFED', 'DCDS', 'MAGEF', 'BMFF', 'MBVF', 'UVEEF', 'VCBF-BCF', 
        'VCAMDF', 'TCGF', 'VEOF', 'DCDE', 'VDEF', 'MAFEQI', 'SSISCA', 
        'EVESG', 'PHVSF', 'VLGF', 'VESAF', 'VMEEF', 'TBLF', 'DCAF', 
        'LHCDF', 'VCBF-MGF', 'VNDAF', 'BVPF', 'NTPPF', 'KDEF', 'RVPIF', 
        'VCBF-AIF',
        
        # Quỹ trái phiếu
        'LHBF', 'MBBOND', 'VNDBF', 'DCBF', 'BVBF', 'VCBF-FIF', 'VCAMFI', 
        'VFF', 'MAFF', 'MBAM', 'ABBF', 'HDBOND', 'VNDCF', 'DCIP', 'PVBF', 
        'SSIBF', 'ASBF', 'VLBF', 'DFIX', 'LPBF',
        
        # Quỹ cân bằng
        'VCAMBF', 'VCBF-TBF', 'VIBF', 'MAFBAL', 'MDI', 'ENF', 'UVDIF', 
        'PBIF', 'SSI-EF',
        
        # Quỹ khác
        'MBAM', 'ABBF', 'VNDCF', 'DCIP', 'SSIBF', 'ASBF', 'VLBF'
    ]
    
    # Remove duplicates and sort
    fund_codes = sorted(list(set(fund_codes)))
    
    logger.info(f"✅ Found {len(fund_codes)} funds in hardcoded list")
    
    # Return in same format as API would
    return [{'code': code} for code in fund_codes]


def get_fund_details(fund_code):
    """Get detailed fund information"""
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
        logger.warning(f"Failed to fetch {fund_code}: {e}")
        return None


def convert_fund_data(fund_detail):
    """Convert fmarket fund data to our format"""
    if not fund_detail:
        return None
    
    return {
        'code': fund_detail.get('code'),
        'name': fund_detail.get('name'),
        'short_name': fund_detail.get('shortName'),
        'nav': fund_detail.get('nav'),
        'nav_change': round((fund_detail.get('nav', 0) - fund_detail.get('lastYearNav', 0)) / fund_detail.get('lastYearNav', 1) * 100, 2) if fund_detail.get('lastYearNav') else 0,
        'management_fee': fund_detail.get('managementFee'),
        'performance_fee': fund_detail.get('performanceFee'),
        'min_investment': fund_detail.get('buyMinValue'),
        'description': fund_detail.get('description'),
        'website': fund_detail.get('website'),
        'manager': {
            'name': fund_detail.get('owner', {}).get('name'),
            'short_name': fund_detail.get('owner', {}).get('shortName'),
            'website': fund_detail.get('owner', {}).get('website'),
            'email': fund_detail.get('owner', {}).get('email')
        },
        'asset_allocation': [
            model.get('name') for model in fund_detail.get('productAssetAllocationModelList', [])
        ],
        'holding_volume': fund_detail.get('holdingVolume'),
        'first_issue_date': datetime.fromtimestamp(fund_detail.get('firstIssueAt', 0) / 1000).strftime('%Y-%m-%d') if fund_detail.get('firstIssueAt') else None,
        'last_updated': datetime.now().isoformat()
    }


def main():
    try:
        start_time = time.time()
        logger.info("🚀 Starting fmarket fund data fetch...")
        
        # Get all funds
        funds_list = get_all_funds()
        if not funds_list:
            logger.error("No funds found!")
            return
        
        # Fetch details for each fund
        all_funds_data = []
        success_count = 0
        fail_count = 0
        
        logger.info(f"📊 Fetching details for {len(funds_list)} funds...")
        
        for idx, fund in enumerate(funds_list, 1):
            fund_code = fund.get('code')
            if not fund_code:
                continue
            
            details = get_fund_details(fund_code)
            if details:
                fund_data = convert_fund_data(details)
                if fund_data:
                    all_funds_data.append(fund_data)
                    logger.info(f"✅ {fund_code} ({idx}/{len(funds_list)})")
                    success_count += 1
                else:
                    fail_count += 1
            else:
                fail_count += 1
            
            # Small delay to be polite (not required, no rate limit)
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
            'data_source': 'fmarket.vn',
            'funds': all_funds_data
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        elapsed = time.time() - start_time
        
        logger.info(f"✅ Successfully saved {success_count} funds")
        logger.info(f"❌ Failed: {fail_count}")
        logger.info(f"⏱️  Total time: {elapsed:.2f}s ({elapsed/60:.1f} minutes)")
        
        print("\n" + "="*50)
        print(f"✅ Fund data fetch completed!")
        print(f"📊 Total funds: {len(all_funds_data)}")
        print(f"✅ Success: {success_count}")
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
