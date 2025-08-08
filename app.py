from flask import Flask, jsonify
from flask_cors import CORS
from config import redis_config
from postgres_service import postgres_service
import logging
import os

from src.api.search_routes import search_bp
from src.api.sync_routes import sync_bp
from src.api.postgres_routes import postgres_bp
from src.core.redisearch_service import RediSearchService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)
CORS(app)

redisearch_service = RediSearchService()

app.register_blueprint(search_bp, url_prefix='/search')
app.register_blueprint(sync_bp, url_prefix='/sync')
app.register_blueprint(postgres_bp, url_prefix='/postgres')


@app.route('/', methods=['GET'])
def health_check():
    try:
        stats = redisearch_service.get_stats()
        return jsonify({
            'status': 'healthy',
            'message': 'Redis Search API is running',
            'stats': stats
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Health check failed: {str(e)}'
        }), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({'error': 'Method not allowed'}), 405


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    try:
        redis_ok = redis_config.test_connection()
        if redis_ok:
            print("Redis connection successful")
        else:
            print("Redis connection failed")
        
        postgres_ok = postgres_service.test_connection()
        if postgres_ok:
            print("PostgreSQL connection successful")
        else:
            print("PostgreSQL connection failed - sync features will not work")
        
        if redis_ok:
            print("Starting Flask server...")
            app.run(
                host='0.0.0.0',
                port=5000,
                debug=os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
            )
        else:
            print("Cannot start without Redis connection.")
    except Exception as e:
        print(f"Failed to start application: {e}")
