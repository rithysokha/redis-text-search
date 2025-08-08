import json
import logging
from typing import List, Dict
from ..utils.text_processor import TextProcessor


class SearchService:
    def __init__(self, redis_client, documents_key: str = "search:documents", inverted_index_key: str = "search:inverted_index"):
        self.redis_client = redis_client
        self.documents_key = documents_key
        self.inverted_index_key = inverted_index_key
        self.text_processor = TextProcessor()

    def full_text_search(self, query: str, limit: int = 10) -> List[Dict]:
        try:
            query_words = self.text_processor.extract_words(query.lower())
            if not query_words:
                return []
            
            title_matches = set()
            content_matches = set()
            tag_matches = set()
            
            for word in query_words:
                title_docs = self.redis_client.smembers(f"{self.inverted_index_key}:title:{word}")
                title_matches.update(title_docs)
                
                content_docs = self.redis_client.smembers(f"{self.inverted_index_key}:content:{word}")
                content_matches.update(content_docs)
                
                tag_docs = self.redis_client.smembers(f"{self.inverted_index_key}:tag:{word}")
                tag_matches.update(tag_docs)
            
            doc_scores = {}
            for doc_id in title_matches:
                doc_scores[doc_id] = doc_scores.get(doc_id, 0) + 3
            
            for doc_id in content_matches:
                doc_scores[doc_id] = doc_scores.get(doc_id, 0) + 1
                
            for doc_id in tag_matches:
                doc_scores[doc_id] = doc_scores.get(doc_id, 0) + 2
            
            sorted_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)[:limit]
            
            results = []
            for doc_id, score in sorted_docs:
                doc_data = self.redis_client.hgetall(f"{self.documents_key}:{doc_id}")
                if doc_data:
                    doc_data['score'] = score
                    doc_data['tags'] = doc_data.get('tags', '').split(',') if doc_data.get('tags') else []
                    try:
                        doc_data['metadata'] = json.loads(doc_data.get('metadata', '{}'))
                    except:
                        doc_data['metadata'] = {}
                    results.append(doc_data)
            
            return results
            
        except Exception as e:
            logging.error(f"Error in full text search: {e}")
            return []

    def fuzzy_search(self, query: str, max_distance: int = 2, limit: int = 10) -> List[Dict]:
        try:
            query_words = self.text_processor.extract_words(query.lower())
            if not query_words:
                return []
            
            fuzzy_matches = set()
            all_words = set()
            
            for key in self.redis_client.scan_iter(match=f"{self.inverted_index_key}:*:*"):
                word = key.split(':')[-1]
                all_words.add(word)
            
            for query_word in query_words:
                for indexed_word in all_words:
                    if self.text_processor.levenshtein_distance(query_word, indexed_word) <= max_distance:
                        for key_type in ['title', 'content', 'tag']:
                            docs = self.redis_client.smembers(f"{self.inverted_index_key}:{key_type}:{indexed_word}")
                            fuzzy_matches.update(docs)
            
            results = []
            for doc_id in list(fuzzy_matches)[:limit]:
                doc_data = self.redis_client.hgetall(f"{self.documents_key}:{doc_id}")
                if doc_data:
                    doc_data['tags'] = doc_data.get('tags', '').split(',') if doc_data.get('tags') else []
                    try:
                        doc_data['metadata'] = json.loads(doc_data.get('metadata', '{}'))
                    except:
                        doc_data['metadata'] = {}
                    results.append(doc_data)
            
            return results
            
        except Exception as e:
            logging.error(f"Error in fuzzy search: {e}")
            return []
