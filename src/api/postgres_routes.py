from flask import Blueprint, request, jsonify
import logging
from src.services.postgres_service import postgres_service

postgres_bp = Blueprint('postgres', __name__)


@postgres_bp.route('/info', methods=['GET'])
def get_postgres_info():
    try:
        if not postgres_service.test_connection():
            return jsonify({
                'error': 'PostgreSQL connection failed',
                'connected': False
            }), 500
        
        info = postgres_service.get_table_info()
        return jsonify(info), 200
        
    except Exception as e:
        logging.error(f"Error getting PostgreSQL info: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@postgres_bp.route('/products', methods=['GET'])
def get_postgres_products():
    try:
        limit = request.args.get('limit', 10, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        if limit < 1 or limit > 100:
            return jsonify({'error': 'Limit must be between 1 and 100'}), 400
            
        if offset < 0:
            return jsonify({'error': 'Offset must be >= 0'}), 400
        
        if not postgres_service.test_connection():
            return jsonify({
                'error': 'PostgreSQL connection failed',
                'connected': False
            }), 500
        
        products = postgres_service.fetch_products(limit, offset)
        total_count = postgres_service.get_products_count()
        
        return jsonify({
            'products': products,
            'total_count': total_count,
            'returned_count': len(products),
            'limit': limit,
            'offset': offset
        }), 200
        
    except Exception as e:
        logging.error(f"Error getting PostgreSQL products: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500
