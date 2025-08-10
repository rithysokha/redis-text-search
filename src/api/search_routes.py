from flask import Blueprint, request, jsonify
import logging
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.core import RediSearchService

search_bp = Blueprint('search', __name__)
redisearch_service = RediSearchService()


@search_bp.route('/index/all', methods=['POST'])
def index_all_documents():
    try:
        from src.services.postgres_service import postgres_service
        
        if not redisearch_service.test_redisearch_availability():
            return jsonify({
                'success': False,
                'error': 'RediSearch module not available. Please install Redis Stack.'
            }), 500
        
        clear_existing = request.json.get('clear_existing', True) if request.is_json else True
        
        if clear_existing:
            redisearch_service.clear_all_data()
            logging.info("Cleared existing search data and RediSearch index")
        
        postgres_products = postgres_service.fetch_products()
        
        if not postgres_products:
            return jsonify({
                'success': False,
                'error': 'No products found in PostgreSQL database'
            }), 404
        
        stats = redisearch_service.bulk_index_from_postgres(postgres_products)
        
        if 'error' in stats:
            return jsonify({
                'success': False,
                'error': stats['error']
            }), 500
        
        return jsonify({
            'success': True,
            'message': f'Successfully indexed {stats["successfully_indexed"]} products using RediSearch',
            'stats': stats
        }), 200
        
    except Exception as e:
        logging.error(f"Error in index_all_documents: {e}")
        return jsonify({'success': False, 'error': f'Internal server error: {str(e)}'}), 500


@search_bp.route('/fulltext', methods=['GET'])
def full_text_search():
    try:
        query = request.args.get('q', '').strip()
        limit = request.args.get('limit', 10, type=int)
        
        if not query:
            return jsonify({'error': 'Query parameter "q" is required'}), 400
        
        if limit < 1 or limit > 100:
            return jsonify({'error': 'Limit must be between 1 and 100'}), 400
        
        results = redisearch_service.full_text_search(query, limit)
        
        return jsonify({
            'query': query,
            'results_count': len(results),
            'results': results
        }), 200
        
    except Exception as e:
        logging.error(f"Error in full_text_search: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@search_bp.route('/fuzzy', methods=['GET'])
def fuzzy_search():
    try:
        query = request.args.get('q', '').strip()
        max_distance = request.args.get('distance', 2, type=int)
        limit = request.args.get('limit', 10, type=int)
        
        if not query:
            return jsonify({'error': 'Query parameter "q" is required'}), 400
        
        if max_distance < 1 or max_distance > 5:
            return jsonify({'error': 'Distance must be between 1 and 5'}), 400
            
        if limit < 1 or limit > 100:
            return jsonify({'error': 'Limit must be between 1 and 100'}), 400
        
        results = redisearch_service.fuzzy_search(query, max_distance, limit)
        
        return jsonify({
            'query': query,
            'max_distance': max_distance,
            'results_count': len(results),
            'results': results
        }), 200
        
    except Exception as e:
        logging.error(f"Error in fuzzy_search: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@search_bp.route('/suggest', methods=['GET'])
def get_suggestions():
    try:
        prefix = request.args.get('prefix', '').strip()
        limit = request.args.get('limit', 10, type=int)
        fuzzy = request.args.get('fuzzy', 'false').lower() == 'true'
        with_scores = request.args.get('with_scores', 'false').lower() == 'true'
        
        if limit < 1 or limit > 50:
            return jsonify({'error': 'Limit must be between 1 and 50'}), 400
        
        suggestions = redisearch_service.get_suggestions(prefix, limit, fuzzy, with_scores)
        
        return jsonify({
            'prefix': prefix,
            'fuzzy_enabled': fuzzy,
            'suggestions_count': len(suggestions),
            'suggestions': suggestions
        }), 200
        
    except Exception as e:
        logging.error(f"Error in get_suggestions: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500


@search_bp.route('/autocomplete', methods=['GET'])
def autocomplete():
    try:
        prefix = request.args.get('prefix', '').strip()
        limit = request.args.get('limit', 10, type=int)
        
        if not prefix:
            return jsonify({'error': 'Prefix parameter is required'}), 400
        
        if limit < 1 or limit > 50:
            return jsonify({'error': 'Limit must be between 1 and 50'}), 400
        
        completions = redisearch_service.get_suggestions(prefix, limit, fuzzy=True, with_scores=False)
        
        return jsonify({
            'prefix': prefix,
            'completions_count': len(completions),
            'completions': completions
        }), 200
        
    except Exception as e:
        logging.error(f"Error in autocomplete: {e}")
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500
