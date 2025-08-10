import json
import logging
from datetime import datetime
from typing import List, Dict
from src.utils import TextProcessor


class DocumentIndexService:
    def __init__(self, redis_client, documents_key: str = "search:documents", inverted_index_key: str = "search:inverted_index"):
        self.redis_client = redis_client
        self.documents_key = documents_key
        self.inverted_index_key = inverted_index_key
        self.text_processor = TextProcessor()

    def index_document(self, doc_id: str, name: str, price: str, image:str , metadata: Dict = None) -> bool:
        """Index document using RediSearch native indexing"""
        try:
            document = {
                'id':doc_id,
                'name': name,
                'price': price,
                'image': image,
                'metadata.name': metadata.get('name', name),
                'metadata.tags': ', '.join(metadata.get('tags', [])) if isinstance(metadata.get('tags'), list) else str(metadata.get('tags', '')),
                'metadata.brand': metadata.get('brand', ''),
                'indexed_at': datetime.now().isoformat()
            }
            
            self.redis_client.hset(
                f"{self.documents_key}:{doc_id}",
                mapping=document
            )
            
            logging.info(f"Successfully indexed document with RediSearch: {doc_id}")
            return True
            
        except Exception as e:
            logging.error(f"Error indexing document {doc_id}: {e}")
            return False

    def clear_all_data(self) -> bool:
        """Clear all indexed data including RediSearch index"""
        try:
            keys_to_delete = []
            
            for _ in self.redis_client.scan_iter(match=f"{self.documents_key}:*"):
                keys_to_delete.append(_)
            
            if keys_to_delete:
                self.redis_client.delete(*keys_to_delete)
            
            try:
                self.redis_client.execute_command('FT.DROPINDEX', 'product_index', 'DD')
                logging.info("Dropped RediSearch index")
            except Exception as e:
                logging.warning(f"Could not drop RediSearch index (may not exist): {e}")
            
            logging.info(f"Cleared {len(keys_to_delete)} keys from Redis")
            return True
            
        except Exception as e:
            logging.error(f"Error clearing data: {e}")
            return False

    def get_document_count(self) -> int:
        """Get total document count"""
        try:
            count = 0
            for _ in self.redis_client.scan_iter(match=f"{self.documents_key}:*"):
                count += 1
            return count
        except Exception as e:
            logging.error(f"Error getting document count: {e}")
            return 0
