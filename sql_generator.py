"""
SQL Query Generator - Step-by-step SQL Builder Engine
Converts parsed natural language input into executable SQL
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from nl_parser import ParsedQuery
from schema_manager import get_schema_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class GeneratedSQL:
    """Container for generated SQL query and metadata"""
    sql: str
    parameters: Dict[str, Any]
    explanation: str
    confidence: float
    assumptions: List[str]
    warnings: List[str]

class SQLGenerator:
    """Step-by-step SQL query generator"""
    
    def __init__(self):
        self.schema_manager = get_schema_manager()
    
    def generate_sql(self, parsed_query: ParsedQuery) -> GeneratedSQL:
        """Generate SQL from parsed natural language query"""
        logger.info(f"Generating SQL for action: {parsed_query.action}")
        
        try:
            # Validate parsed query
            validation_result = self._validate_parsed_query(parsed_query)
            if not validation_result[0]:
                return GeneratedSQL(
                    sql="",
                    parameters={},
                    explanation=f"Validation failed: {validation_result[1]}",
                    confidence=0.0,
                    assumptions=[],
                    warnings=[validation_result[1]]
                )
            
            # Generate SQL based on action type
            if parsed_query.action == 'SELECT':
                return self._generate_select_query(parsed_query)
            elif parsed_query.action == 'COUNT':
                return self._generate_count_query(parsed_query)
            elif parsed_query.action in ['SUM', 'AVG', 'MAX', 'MIN']:
                return self._generate_aggregate_query(parsed_query)
            else:
                return self._generate_select_query(parsed_query)  # Default fallback
        
        except Exception as e:
            logger.error(f"SQL generation failed: {e}")
            return GeneratedSQL(
                sql="",
                parameters={},
                explanation=f"SQL generation error: {str(e)}",
                confidence=0.0,
                assumptions=[],
                warnings=[f"Generation failed: {str(e)}"]
            )
    
    def _validate_parsed_query(self, parsed_query: ParsedQuery) -> Tuple[bool, str]:
        """Validate the parsed query structure"""
        
        if not parsed_query.tables:
            return False, "No tables identified in the query"
        
        # Validate table names exist
        schema_tables = self.schema_manager.get_table_names()
        for table in parsed_query.tables:
            if table not in schema_tables:
                return False, f"Table '{table}' not found in schema"
        
        # Validate column references
        for column_ref in parsed_query.columns:
            if column_ref != '*' and '.' in column_ref:
                table, column = column_ref.split('.', 1)
                if not self.schema_manager.validate_table_column(table, column):
                    return False, f"Column '{column}' not found in table '{table}'"
        
        return True, "Validation passed"
    
    def _generate_select_query(self, parsed_query: ParsedQuery) -> GeneratedSQL:
        """Generate SELECT query"""
        
        # Build SELECT clause
        select_clause = self._build_select_clause(parsed_query)
        
        # Build FROM clause
        from_clause = self._build_from_clause(parsed_query)
        
        # Build JOIN clauses
        join_clauses = self._build_join_clauses(parsed_query)
        
        # Build WHERE clause
        where_clause, parameters = self._build_where_clause(parsed_query)
        
        # Build GROUP BY clause
        group_by_clause = self._build_group_by_clause(parsed_query)
        
        # Build HAVING clause
        having_clause, having_params = self._build_having_clause(parsed_query)
        parameters.update(having_params)
        
        # Build ORDER BY clause
        order_by_clause = self._build_order_by_clause(parsed_query)
        
        # Build LIMIT clause
        limit_clause = self._build_limit_clause(parsed_query)
        
        # Combine all parts
        sql_parts = [select_clause, from_clause]
        sql_parts.extend(join_clauses)
        
        if where_clause:
            sql_parts.append(where_clause)
        
        if group_by_clause:
            sql_parts.append(group_by_clause)
        
        if having_clause:
            sql_parts.append(having_clause)
        
        if order_by_clause:
            sql_parts.append(order_by_clause)
        
        if limit_clause:
            sql_parts.append(limit_clause)
        
        sql = '\n'.join(sql_parts) + ';'
        
        # Generate explanation
        explanation = self._generate_explanation(parsed_query, sql)
        
        # Calculate confidence
        confidence = self._calculate_sql_confidence(parsed_query, sql)
        
        # Generate assumptions and warnings
        assumptions = parsed_query.assumptions.copy()
        warnings = self._generate_warnings(parsed_query)
        
        return GeneratedSQL(
            sql=sql,
            parameters=parameters,
            explanation=explanation,
            confidence=confidence,
            assumptions=assumptions,
            warnings=warnings
        )
    
    def _generate_count_query(self, parsed_query: ParsedQuery) -> GeneratedSQL:
        """Generate COUNT query"""
        
        # Modify parsed query for count
        count_query = ParsedQuery(
            action='SELECT',
            tables=parsed_query.tables,
            columns=['COUNT(*)'],
            filters=parsed_query.filters,
            joins=parsed_query.joins,
            groupby=parsed_query.groupby,
            orderby=[],  # Usually no ordering for count
            having=parsed_query.having,
            limit=None,  # Usually no limit for count
            aggregations=[{'function': 'COUNT', 'column': '*'}],
            confidence=parsed_query.confidence,
            assumptions=parsed_query.assumptions + ["Converted to COUNT query"],
            original_query=parsed_query.original_query
        )
        
        return self._generate_select_query(count_query)
    
    def _generate_aggregate_query(self, parsed_query: ParsedQuery) -> GeneratedSQL:
        """Generate aggregate query (SUM, AVG, MAX, MIN)"""
        
        # Determine the column to aggregate
        agg_column = self._determine_aggregate_column(parsed_query)
        
        if not agg_column:
            return GeneratedSQL(
                sql="",
                parameters={},
                explanation="Could not determine column for aggregation",
                confidence=0.0,
                assumptions=[],
                warnings=["No suitable column found for aggregation"]
            )
        
        # Build aggregate function
        agg_function = f"{parsed_query.action}({agg_column})"
        
        # Modify parsed query for aggregation
        agg_query = ParsedQuery(
            action='SELECT',
            tables=parsed_query.tables,
            columns=[agg_function],
            filters=parsed_query.filters,
            joins=parsed_query.joins,
            groupby=parsed_query.groupby,
            orderby=parsed_query.orderby,
            having=parsed_query.having,
            limit=parsed_query.limit,
            aggregations=parsed_query.aggregations,
            confidence=parsed_query.confidence,
            assumptions=parsed_query.assumptions + [f"Using {agg_column} for {parsed_query.action} aggregation"],
            original_query=parsed_query.original_query
        )
        
        return self._generate_select_query(agg_query)
    
    def _build_select_clause(self, parsed_query: ParsedQuery) -> str:
        """Build SELECT clause"""
        
        if not parsed_query.columns or parsed_query.columns == ['*']:
            return "SELECT *"
        
        # Handle aggregations
        if parsed_query.aggregations:
            select_items = []
            for agg in parsed_query.aggregations:
                if agg['column'] == '*':
                    select_items.append(f"{agg['function']}(*)")
                elif agg['column'] == 'auto':
                    # Auto-determine column for aggregation
                    auto_column = self._determine_aggregate_column(parsed_query)
                    if auto_column:
                        select_items.append(f"{agg['function']}({auto_column})")
                    else:
                        select_items.append(f"{agg['function']}(*)")
                else:
                    select_items.append(f"{agg['function']}({agg['column']})")
            
            # Add GROUP BY columns to SELECT if present
            for group_col in parsed_query.groupby:
                if group_col not in select_items:
                    select_items.append(group_col)
            
            return f"SELECT {', '.join(select_items)}"
        
        # Regular column selection
        return f"SELECT {', '.join(parsed_query.columns)}"
    
    def _build_from_clause(self, parsed_query: ParsedQuery) -> str:
        """Build FROM clause"""
        main_table = parsed_query.tables[0]
        return f"FROM {main_table}"
    
    def _build_join_clauses(self, parsed_query: ParsedQuery) -> List[str]:
        """Build JOIN clauses"""
        join_clauses = []
        
        for join in parsed_query.joins:
            join_type = join.get('type', 'INNER')
            table2 = join['table2']
            condition = join['condition']
            
            join_clauses.append(f"{join_type} JOIN {table2} ON {condition}")
        
        return join_clauses
    
    def _build_where_clause(self, parsed_query: ParsedQuery) -> Tuple[str, Dict[str, Any]]:
        """Build WHERE clause with parameterized queries"""
        
        if not parsed_query.filters:
            return "", {}
        
        conditions = []
        parameters = {}
        param_counter = 0
        
        for filter_item in parsed_query.filters:
            filter_type = filter_item['type']
            column = filter_item['column']
            
            if filter_type == 'EQUALS':
                param_name = f"param_{param_counter}"
                conditions.append(f"{column} = :{param_name}")
                parameters[param_name] = filter_item['value']
                param_counter += 1
            
            elif filter_type == 'GREATER_THAN':
                param_name = f"param_{param_counter}"
                conditions.append(f"{column} > :{param_name}")
                parameters[param_name] = filter_item['value']
                param_counter += 1
            
            elif filter_type == 'LESS_THAN':
                param_name = f"param_{param_counter}"
                conditions.append(f"{column} < :{param_name}")
                parameters[param_name] = filter_item['value']
                param_counter += 1
            
            elif filter_type == 'BETWEEN':
                param_name1 = f"param_{param_counter}"
                param_name2 = f"param_{param_counter + 1}"
                conditions.append(f"{column} BETWEEN :{param_name1} AND :{param_name2}")
                parameters[param_name1] = filter_item['value1']
                parameters[param_name2] = filter_item['value2']
                param_counter += 2
            
            elif filter_type == 'LIKE':
                param_name = f"param_{param_counter}"
                conditions.append(f"{column} LIKE :{param_name}")
                parameters[param_name] = f"%{filter_item['value']}%"
                param_counter += 1
            
            elif filter_type == 'IN':
                param_names = []
                for i, value in enumerate(filter_item['values']):
                    param_name = f"param_{param_counter}"
                    param_names.append(f":{param_name}")
                    parameters[param_name] = value
                    param_counter += 1
                
                conditions.append(f"{column} IN ({', '.join(param_names)})")
        
        if conditions:
            return f"WHERE {' AND '.join(conditions)}", parameters
        
        return "", parameters
    
    def _build_group_by_clause(self, parsed_query: ParsedQuery) -> str:
        """Build GROUP BY clause"""
        
        if not parsed_query.groupby:
            return ""
        
        return f"GROUP BY {', '.join(parsed_query.groupby)}"
    
    def _build_having_clause(self, parsed_query: ParsedQuery) -> Tuple[str, Dict[str, Any]]:
        """Build HAVING clause"""
        
        if not parsed_query.having:
            return "", {}
        
        # Similar to WHERE clause but for HAVING
        # Implementation would be similar to _build_where_clause
        return "", {}
    
    def _build_order_by_clause(self, parsed_query: ParsedQuery) -> str:
        """Build ORDER BY clause"""
        
        if not parsed_query.orderby:
            return ""
        
        order_items = []
        for order in parsed_query.orderby:
            column = order['column']
            direction = order['direction']
            order_items.append(f"{column} {direction}")
        
        return f"ORDER BY {', '.join(order_items)}"
    
    def _build_limit_clause(self, parsed_query: ParsedQuery) -> str:
        """Build LIMIT clause"""
        
        if parsed_query.limit is None:
            return ""
        
        return f"LIMIT {parsed_query.limit}"
    
    def _determine_aggregate_column(self, parsed_query: ParsedQuery) -> Optional[str]:
        """Determine which column to use for aggregation"""
        
        # Look for numeric columns in the tables
        numeric_columns = []
        for table in parsed_query.tables:
            table_columns = self.schema_manager.get_column_names(table)
            for col in table_columns:
                col_info = self.schema_manager.get_column_info(table, col)
                col_type = col_info.get('type', '').upper()
                if any(t in col_type for t in ['INT', 'DECIMAL', 'FLOAT', 'NUMERIC', 'MONEY']):
                    numeric_columns.append(f"{table}.{col}")
        
        # Prefer columns with common aggregate names
        preference_keywords = ['price', 'amount', 'cost', 'salary', 'total', 'value', 'quantity']
        
        for keyword in preference_keywords:
            for col in numeric_columns:
                if keyword in col.lower():
                    return col
        
        # Return first numeric column if available
        if numeric_columns:
            return numeric_columns[0]
        
        # Fallback for COUNT
        if parsed_query.action == 'COUNT':
            return '*'
        
        return None
    
    def _generate_explanation(self, parsed_query: ParsedQuery, sql: str) -> str:
        """Generate human-readable explanation of the SQL query"""
        
        explanation_parts = []
        
        # Action explanation
        action_explanations = {
            'SELECT': "Retrieving data",
            'COUNT': "Counting records",
            'SUM': "Calculating sum",
            'AVG': "Calculating average",
            'MAX': "Finding maximum value",
            'MIN': "Finding minimum value"
        }
        
        explanation_parts.append(action_explanations.get(parsed_query.action, "Processing"))
        
        # Tables explanation
        if len(parsed_query.tables) == 1:
            explanation_parts.append(f"from the {parsed_query.tables[0]} table")
        else:
            explanation_parts.append(f"from {len(parsed_query.tables)} related tables")
        
        # Filters explanation
        if parsed_query.filters:
            filter_count = len(parsed_query.filters)
            explanation_parts.append(f"with {filter_count} filter condition{'s' if filter_count > 1 else ''}")
        
        # Grouping explanation
        if parsed_query.groupby:
            explanation_parts.append(f"grouped by {', '.join(parsed_query.groupby)}")
        
        # Ordering explanation
        if parsed_query.orderby:
            explanation_parts.append(f"ordered by {', '.join([o['column'] for o in parsed_query.orderby])}")
        
        # Limit explanation
        if parsed_query.limit:
            explanation_parts.append(f"limited to {parsed_query.limit} results")
        
        return " ".join(explanation_parts).capitalize() + "."
    
    def _calculate_sql_confidence(self, parsed_query: ParsedQuery, sql: str) -> float:
        """Calculate confidence score for generated SQL"""
        
        base_confidence = parsed_query.confidence
        
        # Boost confidence for successful SQL generation
        if sql and not sql.strip().startswith("--"):
            base_confidence += 0.1
        
        # Boost for parameterized queries (safer)
        if ":" in sql:
            base_confidence += 0.05
        
        # Reduce confidence for complex assumptions
        assumption_penalty = len(parsed_query.assumptions) * 0.02
        base_confidence -= assumption_penalty
        
        return max(0.0, min(1.0, base_confidence))
    
    def _generate_warnings(self, parsed_query: ParsedQuery) -> List[str]:
        """Generate warnings about the generated SQL"""
        warnings = []
        
        if len(parsed_query.tables) > 1 and not parsed_query.joins:
            warnings.append("Multiple tables detected but no explicit joins - may produce Cartesian product")
        
        if not parsed_query.filters and parsed_query.limit is None:
            warnings.append("No filters or limits - query may return large result set")
        
        if parsed_query.confidence < 0.6:
            warnings.append("Low confidence in query interpretation - please review assumptions")
        
        return warnings

# Global SQL generator instance
sql_generator = SQLGenerator()

def generate_sql_query(parsed_query: ParsedQuery) -> GeneratedSQL:
    """Generate SQL query from parsed natural language - main entry point"""
    return sql_generator.generate_sql(parsed_query)
