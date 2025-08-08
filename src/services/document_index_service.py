import json
import logging
from datetime import datetime
from typing import List, Dict
from ..utils.text_processor import TextProcessor


class DocumentIndexService:
    def __init__(self, redis_client, documents_key: str = "search:documents", inverted_index_key: str = "search:inverted_index"):
        self.redis_client = redis_client
        self.documents_key = documents_key
        self.inverted_index_key = inverted_index_key
        self.text_processor = TextProcessor()

    def index_document(self, doc_id: str, title: str, content: str, tags: List[str] = None, metadata: Dict = None) -> bool:
        try:
            document = {
                'id': doc_id,
                'title': title,
                'content': content,
                'tags': ','.join(tags) if tags else '',
                'metadata': json.dumps(metadata) if metadata else '{}',
                'indexed_at': datetime.now().isoformat()
            }
            
            self.redis_client.hset(
                f"{self.documents_key}:{doc_id}",
                mapping=document
            )
            
            self._create_inverted_index(doc_id, title, content, tags)
            
            logging.info(f"Successfully indexed document: {doc_id}")
            return True
            
        except Exception as e:
            logging.error(f"Error indexing document {doc_id}: {e}")
            return False

    def _create_inverted_index(self, doc_id: str, title: str, content: str, tags: List[str] = None):
        words = set()
        
        title_words = self.text_processor.extract_words(title.lower())
        for word in title_words:
            words.add(word)
            self.redis_client.sadd(f"{self.inverted_index_key}:title:{word}", doc_id)
        
        content_words = self.text_processor.extract_words(content.lower())
        for word in content_words:
            words.add(word)
            self.redis_client.sadd(f"{self.inverted_index_key}:content:{word}", doc_id)
        
        if tags:
            for tag in tags:
                tag_words = self.text_processor.extract_words(tag.lower())
                for word in tag_words:
                    words.add(word)
                    self.redis_client.sadd(f"{self.inverted_index_key}:tag:{word}", doc_id)
        
        if words:
            self.redis_client.sadd(f"{self.inverted_index_key}:doc_words:{doc_id}", *words)

    def clear_all_data(self) -> bool:
        try:
            keys_to_delete = []
            
            for key in self.redis_client.scan_iter(match=f"{self.documents_key}:*"):
                keys_to_delete.append(key)
            
            for key in self.redis_client.scan_iter(match=f"{self.inverted_index_key}:*"):
                keys_to_delete.append(key)
            
            if keys_to_delete:
                self.redis_client.delete(*keys_to_delete)
            
            logging.info(f"Cleared {len(keys_to_delete)} keys from Redis")
            return True
            
        except Exception as e:
            logging.error(f"Error clearing data: {e}")
            return False

    def get_document_count(self) -> int:
        try:
            count = 0
            for key in self.redis_client.scan_iter(match=f"{self.documents_key}:*"):
                count += 1
            return count
        except Exception as e:
            logging.error(f"Error getting document count: {e}")
            return 0
