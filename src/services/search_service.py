import json
import logging
from typing import List, Dict
from ..utils.text_processor import TextProcessor


class SearchService:
    def __init__(self, redis_client, index_name: str = "product_index", documents_key: str = "search:documents", inverted_index_key: str = "search:inverted_index"):
        self.redis_client = redis_client
        self.index_name = index_name
        self.documents_key = documents_key
        self.inverted_index_key = inverted_index_key
        self.text_processor = TextProcessor()
        self._ensure_index_exists()

    def _ensure_index_exists(self):
        """Ensure RediSearch index exists"""
        try:
            self.redis_client.execute_command('FT.INFO', self.index_name)
        except Exception:
            try:
                self.redis_client.execute_command(
                    'FT.CREATE', self.index_name,
                    'ON', 'HASH',
                    'PREFIX', '1', f"{self.documents_key}:",
                    'SCHEMA',
                    'title', 'TEXT', 'WEIGHT', '3.0',
                    'content', 'TEXT', 'WEIGHT', '1.0',
                    'tags', 'TEXT', 'WEIGHT', '2.0',
                    'sku', 'TAG', 'SORTABLE',
                    'names', 'TEXT', 'WEIGHT', '2.0'
                )
                logging.info(f"Created RediSearch index: {self.index_name}")
            except Exception as e:
                logging.error(f"Failed to create RediSearch index: {e}")

    def full_text_search(self, query: str, limit: int = 10) -> List[Dict]:
        """Use RediSearch FT.SEARCH for full-text search"""
        try:
            if not query.strip():
                return []
            
            self._ensure_index_exists()
            
            search_queries = [
                f"@title:({query}) | @content:({query}) | @names:({query})",
                f"({query})",
                f"{query}",
                "*"
            ]
            
            for search_query in search_queries:
                try:
                    result = self.redis_client.execute_command(
                        'FT.SEARCH', self.index_name, 
                        search_query,
                        'LIMIT', '0', str(limit)
                    )
                    
                    if result and len(result) > 1:  # Found results
                        return self._parse_search_results(result)
                        
                except Exception as e:
                    logging.warning(f"Search query '{search_query}' failed: {e}")
                    continue

            return []
            
        except Exception as e:
            logging.error(f"Error in RediSearch full text search: {e}")
            return []

    def _parse_search_results(self, result) -> List[Dict]:
        """Parse RediSearch results into document dictionaries"""
        try:
            if not result or len(result) < 2:
                return []
            
            documents = []
            
            for i in range(1, len(result), 2):
                if i + 1 < len(result):
                    doc_id = result[i]
                    doc_fields = result[i + 1]
                    
                    doc_dict = {}
                    for j in range(0, len(doc_fields), 2):
                        if j + 1 < len(doc_fields):
                            field_name = self._decode_bytes(doc_fields[j])
                            field_value = self._decode_bytes(doc_fields[j + 1])
                            doc_dict[field_name] = field_value
                    
                    doc_dict['id'] = self._decode_bytes(doc_id)
                    
                    doc_dict['tags'] = doc_dict.get('tags', '').split(',') if doc_dict.get('tags') else []
                    
                    try:
                        doc_dict['metadata'] = json.loads(doc_dict.get('metadata', '{}'))
                    except (json.JSONDecodeError, TypeError):
                        doc_dict['metadata'] = {}
                    
                    documents.append(doc_dict)
            
            return documents
            
        except Exception as e:
            logging.error(f"Error parsing search results: {e}")
            return []

    def _decode_bytes(self, value):
        """Helper to decode bytes to string"""
        if isinstance(value, bytes):
            return value.decode('utf-8')
        return value

    def fuzzy_search(self, query: str, max_distance: int = 2, limit: int = 10) -> List[Dict]:
        """Use RediSearch FT.SEARCH with fuzzy matching"""
        try:
            if not query.strip():
                return []
            
            self._ensure_index_exists()
            
            query_words = self.text_processor.extract_words(query.lower())
            if not query_words:
                return []

            fuzzy_terms = []
            for word in query_words:
                if max_distance == 1:
                    fuzzy_terms.append(f"%{word}%")
                elif max_distance == 2:
                    fuzzy_terms.append(f"%%{word}%%")
                else:
                    fuzzy_terms.append(f"%%%{word}%%%")
            
            search_queries = [
                f"@title:({' | '.join(fuzzy_terms)}) | @content:({' | '.join(fuzzy_terms)}) | @names:({' | '.join(fuzzy_terms)})",
                " | ".join(fuzzy_terms),
                f"({' | '.join(fuzzy_terms)})"
            ]
            
            for search_query in search_queries:
                try:
                    result = self.redis_client.execute_command(
                        'FT.SEARCH', self.index_name, 
                        search_query,
                        'LIMIT', '0', str(limit)
                    )
                    
                    if result and len(result) > 1:
                        return self._parse_search_results(result)
                        
                except Exception as e:
                    logging.warning(f"Fuzzy search query '{search_query}' failed: {e}")
                    continue
            
            return []
            
        except Exception as e:
            logging.error(f"Error in RediSearch fuzzy search: {e}")
            return []
