import logging
from typing import List, Dict
from config import redis_config
from src.services import SuggestionService, DocumentIndexService, SearchService


class RediSearchService:
    def __init__(self):
        self.redis_client = redis_config.get_connection()
        self.suggestions_key = "suggestions"
        self.index_name = "product_index"
        self.documents_key = "search:documents"
        self.inverted_index_key = "search:inverted_index"
        
        self.suggestion_service = SuggestionService(self.redis_client, self.suggestions_key)
        self.document_service = DocumentIndexService(self.redis_client, self.documents_key, self.inverted_index_key)
        self.search_service = SearchService(self.redis_client, self.index_name, self.documents_key, self.inverted_index_key)

    def test_redisearch_availability(self) -> bool:
        """Test if RediSearch module is available"""
        try:
            self.redis_client.execute_command('FT._LIST')
            return True
        except Exception as e:
            logging.error(f"RediSearch not available: {e}")
            return False

    def add_suggestion(self, suggestion: str, score: float = 1.0) -> bool:
        return self.suggestion_service.add_suggestion(suggestion, score)

    def get_suggestions(self, prefix: str, limit: int = 10, fuzzy: bool = False, with_scores: bool = False) -> List:
        return self.suggestion_service.get_suggestions(prefix, limit, fuzzy, with_scores)

    def clear_suggestions(self) -> bool:
        return self.suggestion_service.clear_suggestions()

    def index_document(self, doc_id: str, name: str, price: str, image: str, metadata: Dict = None) -> bool:
        return self.document_service.index_document(doc_id, name, price, image, metadata)

    def full_text_search(self, query: str, limit: int = 10) -> List[Dict]:
        return self.search_service.full_text_search(query, limit)

    def fuzzy_search(self, query: str, max_distance: int = 2, limit: int = 10) -> List[Dict]:
        return self.search_service.fuzzy_search(query, max_distance, limit)

    def bulk_index_from_postgres(self, postgres_products: List[Dict]) -> Dict:
        """Bulk index products using RediSearch and suggestions"""
        try:
            stats = {
                'total_products': len(postgres_products),
                'successfully_indexed': 0,
                'errors': [],
                'suggestions_added': 0
            }
            self.search_service._ensure_index_exists()
            initial_count = self.suggestion_service.get_suggestion_length()
            
            for product in postgres_products:
                price = str(product.get('price', ''))
                name = str(product.get('name', ''))
                image = str(product.get('image', ''))
                doc_id = str(product.get('id'))
                metadata = product.get('metadata')
                if price and name and len(name.strip()) > 0:
                    if self.index_document(doc_id, name, price,image , metadata):
                        weight = 1.0
                        self.suggestion_service.index_document_for_suggestions(price, name, weight)
                        stats['successfully_indexed'] += 1
                    else:
                        stats['errors'].append(f"Failed to index price {price}")
            
            final_count = self.suggestion_service.get_suggestion_length()
            stats['suggestions_added'] = final_count - initial_count
            
            return stats
            
        except Exception as e:
            logging.error(f"Error in bulk indexing: {e}")
            return {'error': str(e)}

    def clear_all_data(self) -> bool:
        doc_cleared = self.document_service.clear_all_data()
        suggestions_cleared = self.suggestion_service.clear_suggestions()
        return doc_cleared and suggestions_cleared

    def get_stats(self) -> Dict:
        try:
            return {
                'total_suggestions': self.suggestion_service.get_suggestion_length(),
                'documents_count': self.document_service.get_document_count(),
                'suggestions_key': self.suggestions_key,
                'service_type': 'RediSearch FT.SUGADD'
            }
        except Exception as e:
            logging.error(f"Error getting stats: {e}")
            return {'error': str(e)}
