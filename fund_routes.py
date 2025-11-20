"""
Fund-related routes
"""
from flask import Blueprint, jsonify, request
import logging
import time

fund_bp = Blueprint('fund', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)


def init_fund_routes(fund_manager):
    """Initialize fund routes with fund_manager dependency"""
    
    @fund_bp.route('/funds')
    def get_funds():
        """API endpoint to get all funds with optional filtering"""
        start_time = time.time()
        logger.info("📥 /api/funds request received")
        
        search_term = request.args.get('search', '')
        fund_type = request.args.get('type', '')
        
        logger.info(f"⏱️  Calling get_funds()...")
        funds = fund_manager.get_funds(search_term=search_term, fund_type=fund_type)
        logger.info(f"✅ get_funds() returned {len(funds)} records in {time.time()-start_time:.2f}s")
        
        logger.info(f"⏱️  Getting last_update_time()...")
        last_updated = fund_manager.get_last_update_time()
        logger.info(f"✅ last_update_time() returned in {time.time()-start_time:.2f}s")
        
        logger.info(f"✅ /api/funds completed in {time.time()-start_time:.2f}s")
        return jsonify({
            'success': True,
            'data': funds,
            'count': len(funds),
            'last_updated': last_updated
        })

    @fund_bp.route('/fund/<fund_code>')
    def get_fund_api_detail(fund_code):
        """API endpoint to get detailed information for a specific fund"""
        try:
            fund = fund_manager.get_fund_by_code(fund_code)
            if fund:
                return jsonify({
                    'success': True,
                    'data': fund
                })
            else:
                return jsonify({
                    'success': False,
                    'message': f'Fund {fund_code} not found'
                }), 404
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Error retrieving fund details: {str(e)}'
            }), 500

    @fund_bp.route('/fund/<fund_code>/nav_history')
    def get_fund_nav_history(fund_code):
        """API endpoint to get NAV history for a specific fund with optional sampling"""
        try:
            sampling = request.args.get('sampling')  # 'monthly' or None
            nav_data = fund_manager.get_fund_nav_report(fund_code, sampling=sampling)
            return jsonify({
                'success': True,
                'data': nav_data,
                'sampling': sampling
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Error getting NAV report: {str(e)}'
            }), 500

    @fund_bp.route('/fund/<fund_code>/holdings')
    def get_fund_holdings(fund_code):
        """API endpoint to get top holdings for a specific fund"""
        try:
            holding_data = fund_manager.get_fund_top_holding(fund_code)
            return jsonify({
                'success': True,
                'data': holding_data
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Error getting top holdings: {str(e)}'
            }), 500

    @fund_bp.route('/fund/<fund_code>/industry')
    def get_fund_industry(fund_code):
        """API endpoint to get industry allocation for a specific fund"""
        try:
            industry_data = fund_manager.get_fund_industry_holding(fund_code)
            return jsonify({
                'success': True,
                'data': industry_data
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Error getting industry holdings: {str(e)}'
            }), 500

    @fund_bp.route('/fund/<fund_code>/assets')
    def get_fund_assets(fund_code):
        """Get asset allocation for a specific fund"""
        try:
            assets = fund_manager.get_fund_asset_holding(fund_code)
            return jsonify({
                'success': True,
                'data': assets
            })
        except Exception as e:
            logger.error(f"Error getting assets for {fund_code}: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @fund_bp.route('/fund/<fund_code>/nav_chart')
    def get_fund_nav_chart(fund_code):
        """Get NAV history for chart with monthly sampling, latest data point, and VN-Index comparison"""
        try:
            # Get monthly sampled data for chart
            monthly_data = fund_manager.get_fund_nav_report(fund_code, sampling='monthly')
            
            # Get latest data point (not sampled) for accurate last update
            latest_data = fund_manager.get_fund_nav_report(fund_code, sampling=None)
            
            # Combine monthly data with latest point if it's different
            chart_data = monthly_data.copy() if monthly_data else []
            latest_point = None
            
            if latest_data and len(latest_data) > 0:
                latest_point = latest_data[-1]
                
                # Check if latest point is different from last monthly point
                if chart_data and len(chart_data) > 0:
                    last_monthly_date = chart_data[-1]['date']
                    latest_date = latest_point['date']
                    
                    # If latest date is different from last monthly date, add it
                    if latest_date != last_monthly_date:
                        chart_data.append(latest_point)
            
            # Get VN-Index data for comparison - match exact dates from fund data
            vnindex_data = []
            if chart_data and len(chart_data) > 0:
                # Get VN-Index data for specific dates to match fund data points
                vnindex_data = fund_manager.get_vnindex_data_for_dates(chart_data)
            
            return jsonify({
                'success': True,
                'data': chart_data,
                'vnindex_data': vnindex_data,
                'latest_date': latest_point['date'] if latest_point else None
            })
        except Exception as e:
            logger.error(f"Error getting NAV chart for {fund_code}: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @fund_bp.route('/refresh')
    def refresh_data():
        """API endpoint to refresh fund data from vnstock"""
        try:
            result = fund_manager.refresh_data()
            return jsonify({
                'success': True,
                'message': 'Data refreshed successfully',
                'updated_count': result['updated_count'],
                'timestamp': result['timestamp']
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Error refreshing data: {str(e)}'
            }), 500

    @fund_bp.route('/statistics')
    def get_statistics():
        """API endpoint to get fund statistics"""
        stats = fund_manager.get_statistics()
        return jsonify({
            'success': True,
            'data': stats
        })

    @fund_bp.route('/all_holdings')
    def get_all_holdings():
        """API endpoint to get all cached holdings"""
        try:
            holdings = fund_manager.get_all_holdings()
            return jsonify({
                'success': True,
                'data': holdings,
                'count': len(holdings)
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Error getting holdings: {str(e)}'
            }), 500

    @fund_bp.route('/aggregated_holdings')
    def get_aggregated_holdings():
        """API endpoint to get aggregated holdings by stock code"""
        try:
            holdings = fund_manager.get_aggregated_holdings()
            return jsonify({
                'success': True,
                'data': holdings,
                'count': len(holdings)
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Error getting aggregated holdings: {str(e)}'
            }), 500
    
    return fund_bp
