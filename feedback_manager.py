import json
import os
import re
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict

class FeedbackManager:
    def __init__(self, feedback_file: str = "feedback_log.json"):
        self.feedback_file = feedback_file
        self.feedback_data = self._load_feedback()
        self.pattern_corrections = defaultdict(list)
        self.confidence_adjustments = {}
        
    def _load_feedback(self) -> Dict:
        """Load existing feedback from file"""
        if os.path.exists(self.feedback_file):
            try:
                with open(self.feedback_file, 'r') as f:
                    return json.load(f)
            except:
                return {'sessions': [], 'patterns': {}, 'corrections': {}}
        return {'sessions': [], 'patterns': {}, 'corrections': {}}
    
    def add_feedback(self, natural_query: str, sql_query: str, 
                    feedback_type: str, correction: Optional[str] = None) -> None:
        """
        Add feedback for a query
        feedback_type: 'positive', 'negative', 'corrected'
        """
        feedback_entry = {
            'timestamp': datetime.now().isoformat(),
            'natural_query': natural_query,
            'sql_query': sql_query,
            'feedback_type': feedback_type,
            'correction': correction
        }
        
        self.feedback_data['sessions'].append(feedback_entry)
        
        # Update patterns based on feedback
        if feedback_type == 'positive':
            self._update_positive_pattern(natural_query, sql_query)
        elif feedback_type == 'corrected' and correction:
            self._update_correction_pattern(natural_query, sql_query, correction)
        
        # Save feedback
        self._save_feedback()
    
    def _update_positive_pattern(self, natural_query: str, sql_query: str) -> None:
        """Update patterns for successful queries"""
        # Extract key phrases from natural query
        key_phrases = self._extract_key_phrases(natural_query)
        
        for phrase in key_phrases:
            if phrase not in self.feedback_data['patterns']:
                self.feedback_data['patterns'][phrase] = {
                    'success_count': 0,
                    'fail_count': 0,
                    'common_sql_patterns': []
                }
            
            self.feedback_data['patterns'][phrase]['success_count'] += 1
            
            # Store SQL pattern
            sql_pattern = self._generalize_sql_pattern(sql_query)
            if sql_pattern not in self.feedback_data['patterns'][phrase]['common_sql_patterns']:
                self.feedback_data['patterns'][phrase]['common_sql_patterns'].append(sql_pattern)
    
    def _update_correction_pattern(self, natural_query: str, 
                                  wrong_sql: str, correct_sql: str) -> None:
        """Update patterns for corrected queries"""
        # Store correction mapping
        query_hash = hash(natural_query)
        
        if str(query_hash) not in self.feedback_data['corrections']:
            self.feedback_data['corrections'][str(query_hash)] = []
        
        self.feedback_data['corrections'][str(query_hash)].append({
            'wrong': wrong_sql,
            'correct': correct_sql,
            'timestamp': datetime.now().isoformat()
        })
        
        # Update fail count for patterns
        key_phrases = self._extract_key_phrases(natural_query)
        for phrase in key_phrases:
            if phrase in self.feedback_data['patterns']:
                self.feedback_data['patterns'][phrase]['fail_count'] += 1
    
    def get_confidence_adjustment(self, natural_query: str) -> float:
        """Get confidence adjustment based on past feedback"""
        key_phrases = self._extract_key_phrases(natural_query)
        
        total_adjustment = 1.0
        for phrase in key_phrases:
            if phrase in self.feedback_data['patterns']:
                pattern = self.feedback_data['patterns'][phrase]
                success_rate = pattern['success_count'] / (pattern['success_count'] + pattern['fail_count'] + 1)
                
                # Adjust confidence based on success rate
                if success_rate > 0.8:
                    total_adjustment *= 1.1
                elif success_rate < 0.5:
                    total_adjustment *= 0.8
        
        return min(max(total_adjustment, 0.5), 1.5)  # Clamp between 0.5 and 1.5
    
    def get_similar_corrections(self, natural_query: str) -> List[Dict]:
        """Get similar queries that were corrected"""
        query_hash = hash(natural_query)
        
        if str(query_hash) in self.feedback_data['corrections']:
            return self.feedback_data['corrections'][str(query_hash)]
        
        # Look for similar queries
        similar_corrections = []
        key_phrases = set(self._extract_key_phrases(natural_query))
        
        for session in self.feedback_data['sessions']:
            if session['feedback_type'] == 'corrected' and session['correction']:
                session_phrases = set(self._extract_key_phrases(session['natural_query']))
                
                # Calculate similarity
                similarity = len(key_phrases.intersection(session_phrases)) / len(key_phrases.union(session_phrases))
                
                if similarity > 0.6:  # 60% similarity threshold
                    similar_corrections.append({
                        'natural_query': session['natural_query'],
                        'original_sql': session['sql_query'],
                        'corrected_sql': session['correction'],
                        'similarity': similarity
                    })
        
        # Sort by similarity
        similar_corrections.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_corrections[:3]  # Return top 3
    
    def _extract_key_phrases(self, query: str) -> List[str]:
        """Extract key phrases from natural language query"""
        # Simple implementation - extract significant words and bigrams
        words = query.lower().split()
        
        # Remove common words
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        words = [w for w in words if w not in stop_words]
        
        # Extract single words and bigrams
        phrases = words.copy()
        
        # Add bigrams
        for i in range(len(words) - 1):
            phrases.append(f"{words[i]} {words[i+1]}")
        
        return phrases
    
    def _generalize_sql_pattern(self, sql_query: str) -> str:
        """Generalize SQL query to extract pattern"""
        # Replace specific values with placeholders
        pattern = sql_query
        
        # Replace string literals
        pattern = re.sub(r"'[^']*'", "'<STRING>'", pattern)
        pattern = re.sub(r'"[^"]*"', '"<STRING>"', pattern)
        
        # Replace numbers
        pattern = re.sub(r'\b\d+\b', '<NUMBER>', pattern)
        
        # Replace table/column names with generic versions (simplified)
        pattern = pattern.lower()
        
        return pattern
    
    def _save_feedback(self) -> None:
        """Save feedback data to file"""
        try:
            with open(self.feedback_file, 'w') as f:
                json.dump(self.feedback_data, f, indent=2)
        except Exception as e:
            print(f"Error saving feedback: {e}")
    
    def get_statistics(self) -> Dict:
        """Get feedback statistics"""
        total_queries = len(self.feedback_data['sessions'])
        positive = sum(1 for s in self.feedback_data['sessions'] if s['feedback_type'] == 'positive')
        negative = sum(1 for s in self.feedback_data['sessions'] if s['feedback_type'] == 'negative')
        corrected = sum(1 for s in self.feedback_data['sessions'] if s['feedback_type'] == 'corrected')
        
        return {
            'total_queries': total_queries,
            'positive_feedback': positive,
            'negative_feedback': negative,
            'corrections': corrected,
            'success_rate': (positive / total_queries * 100) if total_queries > 0 else 0,
            'pattern_count': len(self.feedback_data['patterns']),
            'correction_mappings': len(self.feedback_data['corrections'])
        }
    
    def export_learning_data(self) -> Dict:
        """Export learning data for analysis"""
        return {
            'statistics': self.get_statistics(),
            'top_patterns': self._get_top_patterns(),
            'common_corrections': self._get_common_corrections(),
            'learning_timeline': self._get_learning_timeline()
        }
    
    def _get_top_patterns(self, limit: int = 10) -> List[Dict]:
        """Get most successful patterns"""
        patterns_with_stats = []
        
        for phrase, data in self.feedback_data['patterns'].items():
            total = data['success_count'] + data['fail_count']
            if total > 0:
                patterns_with_stats.append({
                    'phrase': phrase,
                    'success_rate': data['success_count'] / total,
                    'total_uses': total,
                    'sql_patterns': data['common_sql_patterns'][:3]
                })
        
        # Sort by total uses and success rate
        patterns_with_stats.sort(key=lambda x: (x['total_uses'], x['success_rate']), reverse=True)
        return patterns_with_stats[:limit]
    
    def _get_common_corrections(self, limit: int = 5) -> List[Dict]:
        """Get most common corrections"""
        correction_counts = defaultdict(int)
        
        for corrections in self.feedback_data['corrections'].values():
            for correction in corrections:
                key = (correction['wrong'], correction['correct'])
                correction_counts[key] += 1
        
        # Sort by frequency
        sorted_corrections = sorted(correction_counts.items(), key=lambda x: x[1], reverse=True)
        
        return [
            {
                'wrong_pattern': wrong,
                'correct_pattern': correct,
                'frequency': count
            }
            for (wrong, correct), count in sorted_corrections[:limit]
        ]
    
    def _get_learning_timeline(self) -> List[Dict]:
        """Get learning progress over time"""
        # Group by day
        daily_stats = defaultdict(lambda: {'positive': 0, 'negative': 0, 'corrected': 0})
        
        for session in self.feedback_data['sessions']:
            date = session['timestamp'][:10]  # Extract date part
            daily_stats[date][session['feedback_type']] += 1
        
        # Calculate success rate per day
        timeline = []
        for date, stats in sorted(daily_stats.items()):
            total = sum(stats.values())
            success_rate = (stats['positive'] / total * 100) if total > 0 else 0
            
            timeline.append({
                'date': date,
                'total_queries': total,
                'success_rate': success_rate,
                **stats
            })
        
        return timeline
