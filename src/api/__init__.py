from src.api.postgres_routes import postgres_bp
from src.api.sync_routes import sync_bp
from src.api.search_routes import search_bp

__all__=["search_bp", "sync_bp", "postgres_bp"] 