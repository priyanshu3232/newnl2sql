import re
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

@dataclass
class ParsedQuery:
    """Structured representation of a parsed natural language query"""
    action: str  # SELECT, INSERT, UPDATE, DELETE
    tables: List[str]
    columns: List[str]
    conditions: List[Dict[str, any]]
    joins: List[Dict[str, str]]
    aggregations: List[Dict[str, str]]
    group_by: List[str]
    order_by: List[Dict[str, str]]
    limit: Optional[int]
    
class NaturalLanguageParser:
    def __init__(self):
        # Action keywords
        self.action_patterns = {
            'SELECT': [
                'show', 'display', 'get', 'find', 'list', 'fetch', 
                'retrieve', 'select', 'give me', 'what are', 'which'
            ],
            'INSERT': ['insert', 'add', 'create', 'new'],
            'UPDATE': ['update', 'modify', 'change', 'set'],
            'DELETE': ['delete', 'remove', 'drop']
        }
        
        # Aggregation keywords
        self.aggregation_keywords = {
            'count': ['count', 'how many', 'number of', 'total'],
            'sum': ['sum', 'total', 'aggregate', 'add up'],
            'avg': ['average', 'avg', 'mean'],
            'max': ['maximum', 'max', 'highest', 'largest'],
            'min': ['minimum', 'min', 'lowest', 'smallest']
        }
        
        # Condition keywords
        self.condition_keywords = {
            '=': ['is', 'equals', 'equal to', '='],
            '>': ['greater than', 'more than', 'over', 'above', '>'],
            '<': ['less than', 'below', 'under', '<'],
            '>=': ['at least', 'greater than or equal', '>='],
            '<=': ['at most', 'less than or equal', '<='],
            '!=': ['not equal', 'not', 'different from', '!='],
            'LIKE': ['contains', 'includes', 'has', 'with', 'like'],
            'IN': ['in', 'among', 'one of'],
            'BETWEEN': ['between', 'from ... to', 'range']
        }
        
        # Join keywords
        self.join_keywords = {
            'INNER': ['with', 'and', 'having', 'that have'],
            'LEFT': ['including all', 'even if no'],
            'RIGHT': ['all from right', 'must have'],
            'FULL': ['all records', 'everything']
        }
        
        # Order keywords
        self.order_keywords = {
            'ASC': ['ascending', 'asc', 'lowest first', 'smallest first'],
            'DESC': ['descending', 'desc', 'highest first', 'largest first', 'top']
        }
        
    def parse(self, query: str, schema: Dict) -> ParsedQuery:
        """Parse natural language query into structured format"""
        query_lower = query.lower().strip()
        
        # Detect action
        action = self._detect_action(query_lower)
        
        # Extract components based on action
        if action == 'SELECT':
            return self._parse_select_query(query_lower, schema)
        elif action == 'INSERT':
            return self._parse_insert_query(query_lower, schema)
        elif action == 'UPDATE':
            return self._parse_update_query(query_lower, schema)
        elif action == 'DELETE':
            return self._parse_delete_query(query_lower, schema)
        else:
            # Default to SELECT
            return self._parse_select_query(query_lower, schema)
    
    def _detect_action(self, query: str) -> str:
        """Detect the SQL action from the query"""
        for action, keywords in self.action_patterns.items():
            for keyword in keywords:
                if keyword in query:
                    return action
        return 'SELECT'  # Default
    
    def _parse_select_query(self, query: str, schema: Dict) -> ParsedQuery:
        """Parse a SELECT query"""
        tables = self._extract_tables(query, schema)
        columns = self._extract_columns(query, schema, tables)
        conditions = self._extract_conditions(query, schema, tables)
        joins = self._extract_joins(query, tables, schema)
        aggregations = self._extract_aggregations(query, columns)
        group_by = self._extract_group_by(query, columns)
        order_by = self._extract_order_by(query, columns)
        limit = self._extract_limit(query)
        
        return ParsedQuery(
            action='SELECT',
            tables=tables,
            columns=columns,
            conditions=conditions,
            joins=joins,
            aggregations=aggregations,
            group_by=group_by,
            order_by=order_by,
            limit=limit
        )
    
    def _extract_tables(self, query: str, schema: Dict) -> List[str]:
        """Extract table names from query"""
        tables = []
        
        # Check each table in schema
        for table_name in schema.keys():
            # Check exact match or plural forms
            if table_name.lower() in query or f"{table_name.lower()}s" in query:
                tables.append(table_name)
            # Check singular form if table name ends with 's'
            elif table_name.lower().endswith('s') and table_name.lower()[:-1] in query:
                tables.append(table_name)
        
        return tables
    
    def _extract_columns(self, query: str, schema: Dict, tables: List[str]) -> List[str]:
        """Extract column names from query"""
        columns = []
        
        # Check for "all" or "*" keywords
        if any(word in query for word in ['all', 'everything', 'all columns', '*']):
            return ['*']
        
        # Check columns from identified tables
        for table in tables:
            if table in schema:
                for col_info in schema[table]['columns']:
                    col_name = col_info['name'].lower()
                    if col_name in query:
                        columns.append(col_info['name'])
        
        # If no columns found, default to all
        if not columns and tables:
            columns = ['*']
        
        return columns
    
    def _extract_conditions(self, query: str, schema: Dict, tables: List[str]) -> List[Dict]:
        """Extract WHERE conditions from query"""
        conditions = []
        
        # Look for condition patterns
        for operator, keywords in self.condition_keywords.items():
            for keyword in keywords:
                pattern = rf"(\w+)\s+{re.escape(keyword)}\s+(['\"]?)([^'\"]+)\2"
                matches = re.finditer(pattern, query, re.IGNORECASE)
                
                for match in matches:
                    field = match.group(1)
                    value = match.group(3)
                    
                    # Validate field exists in schema
                    valid_field = self._validate_field(field, tables, schema)
                    if valid_field:
                        conditions.append({
                            'field': valid_field,
                            'operator': operator,
                            'value': value,
                            'parameterized': True
                        })
        
        # Extract date conditions
        date_patterns = [
            (r'in the last (\d+) (day|month|year)s?', 'date_range'),
            (r'since (\d{4}-\d{2}-\d{2})', 'since_date'),
            (r'before (\d{4}-\d{2}-\d{2})', 'before_date'),
            (r'on (\d{4}-\d{2}-\d{2})', 'on_date')
        ]
        
        for pattern, condition_type in date_patterns:
            matches = re.finditer(pattern, query, re.IGNORECASE)
            for match in matches:
                # Find date columns in schema
                date_columns = self._find_date_columns(tables, schema)
                if date_columns:
                    conditions.append({
                        'field': date_columns[0],
                        'operator': 'date_condition',
                        'value': match.group(0),
                        'type': condition_type,
                        'parameterized': True
                    })
        
        return conditions
    
    def _extract_joins(self, query: str, tables: List[str], schema: Dict) -> List[Dict]:
        """Extract JOIN information from query"""
        joins = []
        
        if len(tables) > 1:
            # Look for relationships in schema
            for i, table1 in enumerate(tables):
                for table2 in tables[i+1:]:
                    relationship = self._find_relationship(table1, table2, schema)
                    if relationship:
                        joins.append({
                            'type': 'INNER',  # Default to INNER JOIN
                            'table1': table1,
                            'table2': table2,
                            'on': relationship
                        })
        
        return joins
    
    def _extract_aggregations(self, query: str, columns: List[str]) -> List[Dict]:
        """Extract aggregation functions from query"""
        aggregations = []
        
        for func, keywords in self.aggregation_keywords.items():
            for keyword in keywords:
                if keyword in query:
                    # Determine which column to aggregate
                    if func == 'count':
                        aggregations.append({
                            'function': func.upper(),
                            'column': '*'
                        })
                    else:
                        # Try to find numeric columns
                        for col in columns:
                            if col != '*' and self._is_numeric_column(col):
                                aggregations.append({
                                    'function': func.upper(),
                                    'column': col
                                })
                                break
        
        return aggregations
    
    def _extract_group_by(self, query: str, columns: List[str]) -> List[str]:
        """Extract GROUP BY columns from query"""
        group_by = []
        
        # Look for grouping keywords
        group_keywords = ['by', 'per', 'for each', 'group by']
        for keyword in group_keywords:
            if keyword in query:
                # Extract the word after the keyword
                pattern = rf"{keyword}\s+(\w+)"
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    field = match.group(1)
                    if field in columns or field == '*':
                        group_by.append(field)
        
        return group_by
    
    def _extract_order_by(self, query: str, columns: List[str]) -> List[Dict]:
        """Extract ORDER BY information from query"""
        order_by = []
        
        # Look for ordering keywords
        order_patterns = [
            'sort by', 'order by', 'sorted', 'ordered'
        ]
        
        for pattern in order_patterns:
            if pattern in query:
                # Determine direction
                direction = 'ASC'  # Default
                for dir_key, dir_keywords in self.order_keywords.items():
                    if any(kw in query for kw in dir_keywords):
                        direction = dir_key
                        break
                
                # Find column to order by
                for col in columns:
                    if col != '*' and col.lower() in query:
                        order_by.append({
                            'column': col,
                            'direction': direction
                        })
                        break
        
        return order_by
    
    def _extract_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT value from query"""
        limit_patterns = [
            r'top (\d+)',
            r'first (\d+)',
            r'limit (\d+)',
            r'up to (\d+)'
        ]
        
        for pattern in limit_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        return None
    
    def _validate_field(self, field: str, tables: List[str], schema: Dict) -> Optional[str]:
        """Validate if a field exists in the schema"""
        for table in tables:
            if table in schema:
                for col_info in schema[table]['columns']:
                    if col_info['name'].lower() == field.lower():
                        return col_info['name']
        return None
    
    def _find_date_columns(self, tables: List[str], schema: Dict) -> List[str]:
        """Find date/datetime columns in the schema"""
        date_columns = []
        date_types = ['date', 'datetime', 'timestamp']
        
        for table in tables:
            if table in schema:
                for col_info in schema[table]['columns']:
                    if any(dt in col_info['type'].lower() for dt in date_types):
                        date_columns.append(col_info['name'])
        
        return date_columns
    
    def _find_relationship(self, table1: str, table2: str, schema: Dict) -> Optional[str]:
        """Find relationship between two tables"""
        # Check if there are defined relationships
        if table1 in schema and 'relationships' in schema[table1]:
            for rel in schema[table1]['relationships']:
                if table2.lower() in rel.lower():
                    return rel
        
        if table2 in schema and 'relationships' in schema[table2]:
            for rel in schema[table2]['relationships']:
                if table1.lower() in rel.lower():
                    return rel
        
        # Try to find common column names (foreign keys)
        if table1 in schema and table2 in schema:
            table1_cols = {col['name'].lower() for col in schema[table1]['columns']}
            table2_cols = {col['name'].lower() for col in schema[table2]['columns']}
            
            # Look for table_id pattern
            if f"{table1.lower()}_id" in table2_cols:
                return f"{table2}.{table1}_id = {table1}.id"
            elif f"{table2.lower()}_id" in table1_cols:
                return f"{table1}.{table2}_id = {table2}.id"
        
        return None
    
    def _is_numeric_column(self, column: str) -> bool:
        """Check if column name suggests numeric data"""
        numeric_indicators = [
            'amount', 'price', 'cost', 'total', 'quantity',
            'count', 'sum', 'value', 'balance', 'salary'
        ]
        return any(indicator in column.lower() for indicator in numeric_indicators)
    
    def _parse_insert_query(self, query: str, schema: Dict) -> ParsedQuery:
        """Parse an INSERT query"""
        # Basic implementation for INSERT
        tables = self._extract_tables(query, schema)
        
        return ParsedQuery(
            action='INSERT',
            tables=tables[:1],  # INSERT into one table
            columns=[],
            conditions=[],
            joins=[],
            aggregations=[],
            group_by=[],
            order_by=[],
            limit=None
        )
    
    def _parse_update_query(self, query: str, schema: Dict) -> ParsedQuery:
        """Parse an UPDATE query"""
        # Basic implementation for UPDATE
        tables = self._extract_tables(query, schema)
        conditions = self._extract_conditions(query, schema, tables)
        
        return ParsedQuery(
            action='UPDATE',
            tables=tables[:1],  # UPDATE one table
            columns=[],
            conditions=conditions,
            joins=[],
            aggregations=[],
            group_by=[],
            order_by=[],
            limit=None
        )
    
    def _parse_delete_query(self, query: str, schema: Dict) -> ParsedQuery:
        """Parse a DELETE query"""
        # Basic implementation for DELETE
        tables = self._extract_tables(query, schema)
        conditions = self._extract_conditions(query, schema, tables)
        
        return ParsedQuery(
            action='DELETE',
            tables=tables[:1],  # DELETE from one table
            columns=[],
            conditions=conditions,
            joins=[],
            aggregations=[],
            group_by=[],
            order_by=[],
            limit=None
        )
