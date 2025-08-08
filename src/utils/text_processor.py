import re
from typing import List, Set


class TextProcessor:
    @staticmethod
    def extract_words(text: str) -> List[str]:
        words = re.findall(r'\b[a-zA-Z0-9]+\b', text)
        return [word for word in words if len(word) >= 2]

    @staticmethod
    def tokenize_for_suggestions(text: str) -> Set[str]:
        if not text:
            return set()
        
        text = text.lower()
        tokens = set()
        words = re.split(r'[-_\s,;.!?()]+', text)
        
        cleaned_words = []
        for word in words:
            word = word.strip()
            if len(word) >= 2 and word.isalpha():
                cleaned_words.append(word)
        
        for word in cleaned_words:
            if len(word) >= 2:
                tokens.add(word)
        
        for i in range(len(cleaned_words) - 1):
            phrase = f"{cleaned_words[i]} {cleaned_words[i + 1]}"
            if len(phrase) <= 30:
                tokens.add(phrase)
        
        for i in range(len(cleaned_words) - 2):
            phrase = f"{cleaned_words[i]} {cleaned_words[i + 1]} {cleaned_words[i + 2]}"
            if len(phrase) <= 40:
                tokens.add(phrase)
        
        return tokens

    @staticmethod
    def levenshtein_distance(s1: str, s2: str) -> int:
        if len(s1) < len(s2):
            return TextProcessor.levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
