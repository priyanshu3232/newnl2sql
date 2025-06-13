"""
Natural Language Query Parser - Custom Rule-Based Parsing
Extracts action, tables, columns, filters, joins from natural language queries
"""
import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from schema_manager import get_schema_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ParsedQuery:
    """Structured representation of parsed natural language query"""
    action: str  # SELECT, COUNT, SUM, etc.
    tables: List[str]
    columns: List[str]
    filters: List[Dict[str, Any]]
    joins: List[Dict[str, str]]
    groupby: List[str]
    orderby: List[Dict[str, str]]
    having: List[Dict[str, Any]]
    limit: Optional[int]
    aggregations: List[Dict[str, str]]
    confidence: float
    assumptions: List[str]
    original_query: str

class NaturalLanguageParser:
    """Rule-based natural language query parser"""
    
    def __init__(self):
        self.schema_manager = get_schema_manager()
        self._initialize_patterns()
        self._initialize_keywords()
    
    def _initialize_patterns(self):
        """Initialize regex patterns for parsing"""
        
        # Action patterns
        self.action_patterns = {
            'SELECT': [
                r'\b(show|display|list|get|find|retrieve|see|view)\b',
                r'\b(what|which|who)\b.*\b(are|is)\b',
                r'\b(give me|tell me)\b'
            ],
            'COUNT': [
                r'\b(count|number of|how many|total)\b',
                r'\b(count|total|sum)(?:\s+of)?\s+(?:all\s+)?(?:the\s+)?\w+',
            ],
            'SUM': [
                r'\b(sum|total|add up)\b.*\b(amount|price|cost|value|revenue|sales)\b',
                r'\b(total|sum)\s+(?:of\s+)?(?:all\s+)?\w+(?:\s+amounts?|\s+values?|\s+prices?)?',
            ],
            'AVG': [
                r'\b(average|avg|mean)\b',
                r'\b(what\'s the average|average of)\b'
            ],
            'MAX': [
                r'\b(maximum|max|highest|largest|biggest|most|top)\b',
                r'\b(which.*highest|what.*maximum)\b'
            ],
            'MIN': [
                r'\b(minimum|min|lowest|smallest|least|bottom)\b',
                r'\b(which.*lowest|what.*minimum)\b'
            ]
        }
        
        # Filter patterns
        self.filter_patterns = {
            'EQUALS': [
                r'(\w+)\s+(?:is|equals?|=)\s+["\']?([^"\']+)["\']?',
                r'(\w+)\s+["\']([^"\']+)["\']',
                r'where\s+(\w+)\s*=\s*["\']?([^"\']+)["\']?'
            ],
            'GREATER_THAN': [
                r'(\w+)\s+(?:greater than|more than|above|>)\s+(\d+(?:\.\d+)?)',
                r'(\w+)\s*>\s*(\d+(?:\.\d+)?)',
                r'where\s+(\w+)\s*>\s*(\d+(?:\.\d+)?)'
            ],
            'LESS_THAN': [
                r'(\w+)\s+(?:less than|below|under|<)\s+(\d+(?:\.\d+)?)',
                r'(\w+)\s*<\s*(\d+(?:\.\d+)?)',
                r'where\s+(\w+)\s*<\s*(\d+(?:\.\d+)?)'
            ],
            'BETWEEN': [
                r'(\w+)\s+between\s+(\d+(?:\.\d+)?)\s+and\s+(\d+(?:\.\d+)?)',
                r'(\w+)\s+from\s+(\d+(?:\.\d+)?)\s+to\s+(\d+(?:\.\d+)?)'
            ],
            'LIKE': [
                r'(\w+)\s+(?:contains?|includes?|like)\s+["\']([^"\']+)["\']',
                r'(\w+)\s+with\s+["\']([^"\']+)["\']',
                r'(\w+)\s+starting with\s+["\']([^"\']+)["\']',
                r'(\w+)\s+ending with\s+["\']([^"\']+)["\']'
            ],
            'IN': [
                r'(\w+)\s+(?:in|among)\s+\(([^)]+)\)',
                r'(\w+)\s+(?:is one of|among)\s+([^,]+(?:,\s*[^,]+)*)'
            ]
        }
        
        # Ordering patterns
        self.order_patterns = {
            'ASC': [
                r'order(?:ed)?\s+by\s+(\w+)(?:\s+(?:asc|ascending))?',
                r'sort(?:ed)?\s+by\s+(\w+)(?:\s+(?:asc|ascending))?',
                r'arranged by\s+(\w+)',
                r'(\w+)\s+in ascending order'
            ],
            'DESC': [
                r'order(?:ed)?\s+by\s+(\w+)\s+(?:desc|descending)',
                r'sort(?:ed)?\s+by\s+(\w+)\s+(?:desc|descending)',
                r'(\w+)\s+in descending order',
                r'highest\s+(\w+)',
                r'top.*by\s+(\w+)'
            ]
        }
        
        # Grouping patterns
        self.group_patterns = [
            r'group(?:ed)?\s+by\s+(\w+)',
            r'by\s+(\w+)(?:\s+group)?',
            r'for each\s+(\w+)',
            r'per\s+(\w+)'
        ]
        
        # Limit patterns
        self.limit_patterns = [
            r'(?:top|first|limit)\s+(\d+)',
            r'(\d+)\s+(?:records?|rows?|items?|results?)',
            r'only\s+(\d+)',
            r'limit\s+to\s+(\d+)'
        ]
    
    def _initialize_keywords(self):
        """Initialize keyword mappings"""
        
        # Common column aliases
        self.column_aliases = {
            'name': ['name', 'title', 'full_name', 'first_name', 'last_name', 'product_name', 'dept_name'],
            'id': ['id', 'customer_id', 'product_id', 'order_id', 'emp_id', 'dept_id'],
            'price': ['price', 'cost', 'amount', 'total_amount', 'unit_price', 'salary'],
            'date': ['date', 'created_date', 'order_date', 'hire_date', 'registration_date'],
            'email': ['email', 'email_address'],
            'phone': ['phone', 'phone_number'],
            'address': ['address', 'shipping_address', 'location'],
            'quantity': ['quantity', 'stock_quantity', 'amount'],
            'status': ['status', 'order_status'],
            'category': ['category', 'type', 'department', 'position']
        }
        
        # Table aliases
        self.table_aliases = {
            'customer': ['customer', 'customers', 'client', 'clients', 'user', 'users'],
            'product': ['product', 'products', 'item', 'items'],
            'order': ['order', 'orders', 'purchase', 'purchases'],
            'employee': ['employee', 'employees', 'staff', 'worker', 'workers'],
            'department': ['department', 'departments', 'dept', 'depts', 'division', 'divisions']
        }
    
    def parse_query(self, query: str) -> ParsedQuery:
        """Parse natural language query into structured format"""
        logger.info(f"Parsing query: {query}")
        
        query_lower = query.lower().strip()
        
        # Initialize parsed query structure
        parsed = ParsedQuery(
            action='SELECT',
            tables=[],
            columns=[],
            filters=[],
            joins=[],
            groupby=[],
            orderby=[],
            having=[],
            limit=None,
            aggregations=[],
            confidence=0.0,
            assumptions=[],
            original_query=query
        )
        
        # Parse action
        parsed.action = self._parse_action(query_lower)
        
        # Parse tables
        parsed.tables = self._parse_tables(query_lower)
        
        # Parse columns
        parsed.columns = self._parse_columns(query_lower, parsed.tables)
        
        # Parse filters
        parsed.filters = self._parse_filters(query_lower, parsed.tables)
        
        # Parse joins (if multiple tables)
        if len(parsed.tables) > 1:
            parsed.joins = self._generate_joins(parsed.tables)
        
        # Parse grouping
        parsed.groupby = self._parse_groupby(query_lower, parsed.tables)
        
        # Parse ordering
        parsed.orderby = self._parse_orderby(query_lower, parsed.tables)
        
        # Parse limit
        parsed.limit = self._parse_limit(query_lower)
        
        # Parse aggregations
        parsed.aggregations = self._parse_aggregations(query_lower, parsed.action)
        
        # Calculate confidence and assumptions
        parsed.confidence = self._calculate_confidence(parsed, query_lower)
        parsed.assumptions = self._generate_assumptions(parsed, query_lower)
        
        logger.info(f"Parsed successfully. Confidence: {parsed.confidence:.2f}")
        return parsed
    
    def _parse_action(self, query: str) -> str:
        """Determine the main action from query"""
        for action, patterns in self.action_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    return action
        return 'SELECT'  # Default action
    
    def _parse_tables(self, query: str) -> List[str]:
        """Extract table names from query"""
        tables = []
        schema_tables = self.schema_manager.get_table_names()
        
        # Direct table name matching
        for table in schema_tables:
            if table in query:
                tables.append(table)
        
        # Alias matching
        for canonical_name, aliases in self.table_aliases.items():
            for alias in aliases:
                if re.search(rf'\b{alias}\b', query, re.IGNORECASE):
                    # Find actual table name
                    for schema_table in schema_tables:
                        if canonical_name in schema_table.lower():
                            if schema_table not in tables:
                                tables.append(schema_table)
                            break
        
        # If no tables found, try to infer from context
        if not tables:
            tables = self._infer_tables_from_context(query)
        
        return tables
    
    def _parse_columns(self, query: str, tables: List[str]) -> List[str]:
        """Extract column names from query"""
        columns = []
        
        if not tables:
            return columns
        
        # Get all available columns from identified tables
        all_columns = []
        for table in tables:
            table_columns = self.schema_manager.get_column_names(table)
            all_columns.extend([(col, table) for col in table_columns])
        
        # Direct column matching
        for col_name, table in all_columns:
            if re.search(rf'\b{col_name}\b', query, re.IGNORECASE):
                columns.append(f"{table}.{col_name}")
        
        # Alias matching
        for canonical_name, aliases in self.column_aliases.items():
            for alias in aliases:
                if re.search(rf'\b{alias}\b', query, re.IGNORECASE):
                    # Find matching columns
                    for col_name, table in all_columns:
                        if canonical_name in col_name.lower():
                            col_ref = f"{table}.{col_name}"
                            if col_ref not in columns:
                                columns.append(col_ref)
        
        # If no specific columns found, use * for SELECT
        if not columns and query.startswith(('show', 'display', 'list', 'get')):
            columns = ['*']
        
        return columns
    
    def _parse_filters(self, query: str, tables: List[str]) -> List[Dict[str, Any]]:
        """Extract filter conditions from query"""
        filters = []
        
        for filter_type, patterns in self.filter_patterns.items():
            for pattern in patterns:
                matches = re.finditer(pattern, query, re.IGNORECASE)
                for match in matches:
                    if filter_type == 'BETWEEN':
                        column, value1, value2 = match.groups()
                        resolved_column = self._resolve_column_name(column, tables)
                        if resolved_column:
                            filters.append({
                                'type': filter_type,
                                'column': resolved_column,
                                'value1': self._convert_value(value1),
                                'value2': self._convert_value(value2)
                            })
                    elif filter_type == 'IN':
                        column, values_str = match.groups()
                        resolved_column = self._resolve_column_name(column, tables)
                        if resolved_column:
                            values = [v.strip().strip('\'"') for v in values_str.split(',')]
                            filters.append({
                                'type': filter_type,
                                'column': resolved_column,
                                'values': values
                            })
                    else:
                        column, value = match.groups()
                        resolved_column = self._resolve_column_name(column, tables)
                        if resolved_column:
                            filters.append({
                                'type': filter_type,
                                'column': resolved_column,
                                'value': self._convert_value(value)
                            })
        
        return filters
    
    def _parse_groupby(self, query: str, tables: List[str]) -> List[str]:
        """Extract GROUP BY columns from query"""
        groupby = []
        
        for pattern in self.group_patterns:
            matches = re.finditer(pattern, query, re.IGNORECASE)
            for match in matches:
                column = match.group(1)
                resolved_column = self._resolve_column_name(column, tables)
                if resolved_column and resolved_column not in groupby:
                    groupby.append(resolved_column)
        
        return groupby
    
    def _parse_orderby(self, query: str, tables: List[str]) -> List[Dict[str, str]]:
        """Extract ORDER BY clauses from query"""
        orderby = []
        
        for direction, patterns in self.order_patterns.items():
            for pattern in patterns:
                matches = re.finditer(pattern, query, re.IGNORECASE)
                for match in matches:
                    column = match.group(1)
                    resolved_column = self._resolve_column_name(column, tables)
                    if resolved_column:
                        orderby.append({
                            'column': resolved_column,
                            'direction': direction
                        })
        
        return orderby
    
    def _parse_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT value from query"""
        for pattern in self.limit_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        return None
    
    def _parse_aggregations(self, query: str, action: str) -> List[Dict[str, str]]:
        """Extract aggregation functions from query"""
        aggregations = []
        
        if action in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']:
            aggregations.append({
                'function': action,
                'column': '*' if action == 'COUNT' else 'auto'
            })
        
        # Look for explicit aggregation mentions
        agg_patterns = {
            'COUNT': r'\bcount\s*\(\s*(\*|\w+)\s*\)',
            'SUM': r'\bsum\s*\(\s*(\w+)\s*\)',
            'AVG': r'\b(?:avg|average)\s*\(\s*(\w+)\s*\)',
            'MAX': r'\bmax\s*\(\s*(\w+)\s*\)',
            'MIN': r'\bmin\s*\(\s*(\w+)\s*\)'
        }
        
        for func, pattern in agg_patterns.items():
            matches = re.finditer(pattern, query, re.IGNORECASE)
            for match in matches:
                column = match.group(1)
                aggregations.append({
                    'function': func,
                    'column': column
                })
        
        return aggregations
    
    def _resolve_column_name(self, column_hint: str, tables: List[str]) -> Optional[str]:
        """Resolve column hint to actual table.column reference"""
        if not tables:
            return None
        
        # Direct match
        for table in tables:
            table_columns = self.schema_manager.get_column_names(table)
            if column_hint.lower() in [col.lower() for col in table_columns]:
                actual_column = next(col for col in table_columns if col.lower() == column_hint.lower())
                return f"{table}.{actual_column}"
        
        # Alias match
        for canonical_name, aliases in self.column_aliases.items():
            if column_hint.lower() in [alias.lower() for alias in aliases]:
                for table in tables:
                    table_columns = self.schema_manager.get_column_names(table)
                    for col in table_columns:
                        if canonical_name in col.lower():
                            return f"{table}.{col}"
        
        # Fuzzy match (contains)
        for table in tables:
            table_columns = self.schema_manager.get_column_names(table)
            for col in table_columns:
                if column_hint.lower() in col.lower() or col.lower() in column_hint.lower():
                    return f"{table}.{col}"
        
        return None
    
    def _generate_joins(self, tables: List[str]) -> List[Dict[str, str]]:
        """Generate necessary joins between tables"""
        joins = []
        
        if len(tables) < 2:
            return joins
        
        relationships = self.schema_manager.get_relationships()
        
        # Find relationships between the tables
        for i, table1 in enumerate(tables):
            for table2 in tables[i+1:]:
                # Look for direct relationship
                for rel in relationships:
                    if ((table1 in rel["from"] and table2 in rel["to"]) or
                        (table2 in rel["from"] and table1 in rel["to"])):
                        joins.append({
                            'type': 'INNER',
                            'table1': table1,
                            'table2': table2,
                            'condition': f"{rel['from']} = {rel['to']}"
                        })
        
        return joins
    
    def _infer_tables_from_context(self, query: str) -> List[str]:
        """Infer tables when direct matching fails"""
        schema_tables = self.schema_manager.get_table_names()
        
        # Look for context clues
        context_hints = {
            'customer': ['buy', 'purchase', 'client', 'customer'],
            'product': ['product', 'item', 'inventory', 'stock'],
            'order': ['order', 'purchase', 'buy', 'sale'],
            'employee': ['employee', 'staff', 'work', 'salary', 'hire'],
            'department': ['department', 'division', 'team']
        }
        
        inferred_tables = []
        for table_hint, keywords in context_hints.items():
            if any(keyword in query.lower() for keyword in keywords):
                for table in schema_tables:
                    if table_hint in table.lower():
                        inferred_tables.append(table)
        
        # If still no tables, default to the first table (fallback)
        if not inferred_tables and schema_tables:
            inferred_tables = [schema_tables[0]]
        
        return inferred_tables
    
    def _convert_value(self, value: str) -> Any:
        """Convert string value to appropriate type"""
        value = value.strip().strip('\'"')
        
        # Try to convert to number
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # Try to convert to boolean
        if value.lower() in ['true', 'yes', '1']:
            return True
        elif value.lower() in ['false', 'no', '0']:
            return False
        
        # Return as string
        return value
    
    def _calculate_confidence(self, parsed: ParsedQuery, query: str) -> float:
        """Calculate confidence score for the parsed query"""
        confidence = 0.0
        
        # Base confidence
        confidence += 0.3
        
        # Table identification confidence
        if parsed.tables:
            confidence += 0.2
        
        # Column identification confidence
        if parsed.columns:
            confidence += 0.2
        
        # Action identification confidence
        if parsed.action != 'SELECT' or any(kw in query for kw in ['show', 'list', 'get']):
            confidence += 0.1
        
        # Filter confidence
        if parsed.filters:
            confidence += 0.1
        
        # Join confidence (if multiple tables)
        if len(parsed.tables) > 1 and parsed.joins:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _generate_assumptions(self, parsed: ParsedQuery, query: str) -> List[str]:
        """Generate list of assumptions made during parsing"""
        assumptions = []
        
        if not parsed.tables:
            assumptions.append("No specific tables identified - using default table")
        
        if not parsed.columns or parsed.columns == ['*']:
            assumptions.append("No specific columns mentioned - selecting all columns")
        
        if len(parsed.tables) > 1 and not parsed.joins:
            assumptions.append("Multiple tables detected but no explicit join conditions found")
        
        if parsed.action == 'SELECT' and any(kw in query for kw in ['count', 'total', 'number']):
            assumptions.append("Interpreted as SELECT but COUNT might be intended")
        
        if not parsed.filters and any(kw in query for kw in ['where', 'with', 'having']):
            assumptions.append("Filter keywords detected but conditions not clearly identified")
        
        return assumptions

# Global parser instance
nl_parser = NaturalLanguageParser()

def parse_natural_language(query: str) -> ParsedQuery:
    """Parse natural language query - main entry point"""
    return nl_parser.parse_query(query)
