import logging
from typing import List
from src.utils import TextProcessor


class SuggestionService:
    def __init__(self, redis_client, suggestions_key: str = "suggestions"):
        self.redis_client = redis_client
        self.suggestions_key = suggestions_key
        self.text_processor = TextProcessor()

    def add_suggestion(self, suggestion: str, score: float = 1.0) -> bool:
        try:
            self.redis_client.execute_command(
                'FT.SUGADD', 
                self.suggestions_key, 
                suggestion, 
                score
            )
            return True
        except Exception as e:
            logging.error(f"Error adding suggestion '{suggestion}': {e}")
            return False

    def add_suggestion_with_increment(self, suggestion: str, score: float = 1.0) -> bool:
        try:
            self.redis_client.execute_command(
                'FT.SUGADD', 
                self.suggestions_key, 
                suggestion, 
                score,
                'INCR'
            )
            return True
        except Exception as e:
            logging.error(f"Error adding suggestion with increment '{suggestion}': {e}")
            return False

    def get_suggestions(self, prefix: str, limit: int = 10, fuzzy: bool = False, with_scores: bool = False) -> List:
        try:
            cmd = ['FT.SUGGET', self.suggestions_key, prefix, 'MAX', str(limit)]
            
            if fuzzy:
                cmd.append('FUZZY')
            
            if with_scores:
                cmd.append('WITHSCORES')
            
            result = self.redis_client.execute_command(*cmd)
            
            if with_scores:
                suggestions = []
                for i in range(0, len(result), 2):
                    suggestion = result[i].decode('utf-8') if isinstance(result[i], bytes) else result[i]
                    score = float(result[i + 1]) if i + 1 < len(result) else 0.0
                    suggestions.append({'suggestion': suggestion, 'score': score})
                return suggestions
            else:
                return [item.decode('utf-8') if isinstance(item, bytes) else item for item in result]
                
        except Exception as e:
            logging.error(f"Error getting suggestions for '{prefix}': {e}")
            return []

    def delete_suggestion(self, suggestion: str) -> bool:
        try:
            result = self.redis_client.execute_command('FT.SUGDEL', self.suggestions_key, suggestion)
            return bool(result)
        except Exception as e:
            logging.error(f"Error deleting suggestion '{suggestion}': {e}")
            return False

    def get_suggestion_length(self) -> int:
        try:
            result = self.redis_client.execute_command('FT.SUGLEN', self.suggestions_key)
            return int(result)
        except Exception as e:
            logging.error(f"Error getting suggestions length: {e}")
            return 0

    def clear_suggestions(self) -> bool:
        try:
            self.redis_client.delete(self.suggestions_key)
            return True
        except Exception as e:
            logging.error(f"Error clearing suggestions: {e}")
            return False

    def index_document_for_suggestions(self, sku: str, names: str, weight_multiplier: float = 1.0) -> bool:
        try:
            suggestions = self.text_processor.tokenize_for_suggestions(names)
            
            success_count = 0
            total_count = len(suggestions)
            
            for suggestion in suggestions:
                word_count = len(suggestion.split())
                base_score = max(1.0, 5.0 - word_count)
                final_score = base_score * weight_multiplier
                
                if self.add_suggestion_with_increment(suggestion, final_score):
                    success_count += 1
            
            logging.info(f"Added {success_count}/{total_count} suggestions for SKU {sku}")
            return success_count > 0
            
        except Exception as e:
            logging.error(f"Error indexing document for suggestions: {e}")
            return False
