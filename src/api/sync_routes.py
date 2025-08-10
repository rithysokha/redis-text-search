from flask import Blueprint, request, jsonify
import logging
from src.services import data_sync_service, postgres_service
sync_bp = Blueprint('sync', __name__)


@sync_bp.route('/postgres', methods=['POST'])
def sync_from_postgres():
    try:
        data = request.get_json() or {}
        batch_size = data.get('batch_size', 100)
        clear_existing = data.get('clear_existing', True)
        
        if not isinstance(batch_size, int) or batch_size < 1 or batch_size > 1000:
            return jsonify({'error': 'batch_size must be an integer between 1 and 1000'}), 400
        
        if not postgres_service.test_connection():
            return jsonify({
                'success': False,
                'error': 'PostgreSQL connection failed. Please check your database configuration.'
            }), 500
        
        result = data_sync_service.sync_all_products(batch_size, clear_existing)
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': f"Successfully synced {result['indexed_products']} products from PostgreSQL",
                'stats': result
            }), 200
        else:
            return jsonify({
                'success': False,
                'message': 'Sync completed with errors',
                'stats': result
            }), 500
            
    except Exception as e:
        logging.error(f"Error in sync_from_postgres: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@sync_bp.route('/postgres/<product_id>', methods=['POST'])
def sync_single_product(product_id):
    try:
        if not product_id:
            return jsonify({'error': 'Product ID is required'}), 400
        
        result = data_sync_service.sync_single_product(product_id)
        
        if result['success']:
            return jsonify(result), 200
        else:
            return jsonify(result), 404
            
    except Exception as e:
        logging.error(f"Error syncing single product: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@sync_bp.route('/status', methods=['GET'])
def get_sync_status():
    try:
        status = data_sync_service.get_sync_status()
        return jsonify(status), 200
        
    except Exception as e:
        logging.error(f"Error getting sync status: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500
