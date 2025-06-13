import sqlite3
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

class QueryExecutor:
    def __init__(self):
        self.dangerous_keywords = [
            'DROP', 'TRUNCATE', 'EXEC', 'EXECUTE', 
            'SCRIPT', 'SHUTDOWN', 'GRANT', 'REVOKE'
        ]
        
    def execute(self, sql_query: str, connection: Optional[sqlite3.Connection]) -> Dict[str, Any]:
        """
        Execute SQL query with protection against SQL injection
        Returns dict with success status, data/error, and metadata
        """
        # Validate query safety
        validation = self._validate_query(sql_query)
        if not validation['safe']:
            return {
                'success': False,
                'error': validation['reason'],
                'data': None
            }
        
        # If no connection provided, return error
        if not connection:
            return {
                'success': False,
                'error': 'No database connection available',
                'data': None
            }
        
        try:
            # Use parameterized query execution
            cursor = connection.cursor()
            
            # Extract parameters from query (for this demo, we'll use placeholders)
            # In real implementation, parameters would be passed separately
            params = self._extract_parameters(sql_query)
            
            # Execute query
            if params:
                cursor.execute(sql_query, params)
            else:
                cursor.execute(sql_query)
            
            # Handle different query types
            if sql_query.strip().upper().startswith('SELECT'):
                # Fetch results for SELECT queries
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                
                # Convert to list of dicts
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row)))
                
                return {
                    'success': True,
                    'data': data,
                    'error': None,
                    'rows_affected': len(data)
                }
            
            else:
                # For INSERT, UPDATE, DELETE
                connection.commit()
                return {
                    'success': True,
                    'data': None,
                    'error': None,
                    'rows_affected': cursor.rowcount
                }
                
        except sqlite3.Error as e:
            # Rollback on error
            if connection:
                connection.rollback()
            
            return {
                'success': False,
                'error': f"Database error: {str(e)}",
                'data': None
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}",
                'data': None
            }
        finally:
            cursor.close()
    
    def _validate_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Validate query for safety against SQL injection
        """
        query_upper = sql_query.upper()
        
        # Check for dangerous keywords
        for keyword in self.dangerous_keywords:
            if keyword in query_upper:
                return {
                    'safe': False,
                    'reason': f"Query contains potentially dangerous keyword: {keyword}"
                }
        
        # Check for multiple statements (semicolon not in string)
        if self._has_multiple_statements(sql_query):
            return {
                'safe': False,
                'reason': "Multiple SQL statements detected. Only single statements are allowed."
            }
        
        # Check for comments that might hide malicious code
        if '--' in sql_query or '/*' in sql_query:
            return {
                'safe': False,
                'reason': "SQL comments detected. Comments are not allowed for security."
            }
        
        # Validate balanced quotes
        if not self._balanced_quotes(sql_query):
            return {
                'safe': False,
                'reason': "Unbalanced quotes detected. Possible SQL injection attempt."
            }
        
        return {
            'safe': True,
            'reason': None
        }
    
    def _has_multiple_statements(self, sql_query: str) -> bool:
        """
        Check if query contains multiple statements
        """
        # Remove string literals to avoid false positives
        cleaned = self._remove_string_literals(sql_query)
        
        # Check for semicolons outside of strings
        return ';' in cleaned
    
    def _balanced_quotes(self, sql_query: str) -> bool:
        """
        Check if quotes are balanced in the query
        """
        single_quotes = sql_query.count("'") - sql_query.count("\\'")
        double_quotes = sql_query.count('"') - sql_query.count('\\"')
        
        return single_quotes % 2 == 0 and double_quotes % 2 == 0
    
    def _remove_string_literals(self, sql_query: str) -> str:
        """
        Remove string literals from query for analysis
        """
        # Replace string literals with placeholder
        # This is a simplified version - production would need more robust parsing
        result = re.sub(r"'[^']*'", "''", sql_query)
        result = re.sub(r'"[^"]*"', '""', result)
        return result
    
    def _extract_parameters(self, sql_query: str) -> List[Any]:
        """
        Extract parameters for parameterized query
        In a real implementation, these would be passed separately
        """
        # For this demo, we'll return empty list
        # In production, parameters would be extracted from the parsed query
        return []
    
    def validate_connection(self, connection: sqlite3.Connection) -> bool:
        """
        Validate database connection
        """
        try:
            connection.execute("SELECT 1")
            return True
        except:
            return False
