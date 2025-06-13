from typing import Dict, List, Tuple
from query_parser import ParsedQuery
import re
from datetime import datetime, timedelta

class SQLGenerator:
    def __init__(self):
        self.assumptions = []
        self.confidence = 0.0
        
    def generate(self, parsed_query: ParsedQuery, schema: Dict) -> Dict:
        self.assumptions = []
        self.confidence = 1.0
        
        if parsed_query.action == 'SELECT':
            query = self._generate_select(parsed_query, schema)
        elif parsed_query.action == 'INSERT':
            query = self._generate_insert(parsed_query, schema)
        elif parsed_query.action == 'UPDATE':
            query = self._generate_update(parsed_query, schema)
        elif parsed_query.action == 'DELETE':
            query = self._generate_delete(parsed_query, schema)
        else:
            query = self._generate_select(parsed_query, schema)
        
        return {
            'query': query,
            'assumptions': self.assumptions,
            'confidence': self.confidence,
            'parsed': parsed_query
        }
    
    def _generate_select(self, parsed: ParsedQuery, schema: Dict) -> str:
        query_parts = []
        
        select_clause = self._build_select_clause(parsed)
        query_parts.append(select_clause)
        
        from_clause = self._build_from_clause(parsed)
        query_parts.append(from_clause)
        
        if parsed.joins:
            join_clauses = self._build_join_clauses(parsed)
            query_parts.extend(join_clauses)
        
        if parsed.conditions:
            where_clause = self._build_where_clause(parsed)
            query_parts.append(where_clause)
        
        if parsed.group_by or parsed.aggregations:
            group_by_clause = self._build_group_by_clause(parsed)
            if group_by_clause:
                query_parts.append(group_by_clause)
        
        if parsed.order_by:
            order_by_clause = self._build_order_by_clause(parsed)
            query_parts.append(order_by_clause)
        
        if parsed.limit:
            query_parts.append(f"LIMIT {parsed.limit}")
        
        return '\n'.join(query_parts)
    
    def _build_select_clause(self, parsed: ParsedQuery) -> str:
        if parsed.aggregations:
            select_items = []
            
            for agg in parsed.aggregations:
                if agg['function'] == 'COUNT' and agg['column'] == '*':
                    select_items.append("COUNT(*)")
                else:
                    select_items.append(f"{agg['function']}({agg['column']})")
            
            if parsed.group_by:
                for col in parsed.group_by:
                    if col not in [agg['column'] for agg in parsed.aggregations]:
                        select_items.append(col)
            
            return f"SELECT {', '.join(select_items)}"
        else:
            if parsed.columns:
                if parsed.columns == ['*']:
                    return "SELECT *"
                else:
                    return f"SELECT {', '.join(parsed.columns)}"
            else:
                self.assumptions.append("No specific columns mentioned, selecting all columns")
                self.confidence *= 0.8
                return "SELECT *"
    
    def _build_from_clause(self, parsed: ParsedQuery) -> str:
        if not parsed.tables:
            self.assumptions.append("No tables identified, query may fail")
            self.confidence *= 0.1
            return "FROM unknown_table"
        
        primary_table = parsed.tables[0]
        return f"FROM {primary_table}"
    
    def _build_join_clauses(self, parsed: ParsedQuery) -> List[str]:
        join_clauses = []
        
        for join in parsed.joins:
            join_type = join.get('type', 'INNER')
            table2 = join['table2']
            on_condition = join['on']
            
            join_clause = f"{join_type} JOIN {table2} ON {on_condition}"
            join_clauses.append(join_clause)
            
            self.assumptions.append(f"Assumed {join_type} JOIN between tables based on relationship")
            self.confidence *= 0.9
        
        return join_clauses
    
    def _build_where_clause(self, parsed: ParsedQuery) -> str:
        conditions = []
        
        for condition in parsed.conditions:
            field = condition['field']
            operator = condition['operator']
            value = condition['value']
            
            if operator == 'date_condition':
                date_condition = self._build_date_condition(condition)
                conditions.append(date_condition)
            elif operator == 'LIKE':
                conditions.append(f"{field} LIKE ?")
                self.assumptions.append(f"Searching for partial match in {field}")
            elif operator == 'IN':
                placeholders = ', '.join(['?' for _ in value.split(',')])
                conditions.append(f"{field} IN ({placeholders})")
            elif operator == 'BETWEEN':
                conditions.append(f"{field} BETWEEN ? AND ?")
            else:
                conditions.append(f"{field} {operator} ?")
        
        if conditions:
            return f"WHERE {' AND '.join(conditions)}"
        return ""
    
    def _build_date_condition(self, condition: Dict) -> str:
        field = condition['field']
        value = condition['value']
        condition_type = condition.get('type', '')
        
        if 'last' in value:
            match = re.search(r'(\d+)\s+(day|month|year)', value)
            if match:
                number = int(match.group(1))
                unit = match.group(2)
                
                self.assumptions.append(f"Interpreted '{value}' as date range from today")
                
                if unit == 'day':
                    return f"{field} >= date('now', '-{number} days')"
                elif unit == 'month':
                    return f"{field} >= date('now', '-{number} months')"
                elif unit == 'year':
                    return f"{field} >= date('now', '-{number} years')"
        elif condition_type == 'since_date':
            return f"{field} >= ?"
        elif condition_type == 'before_date':
            return f"{field} < ?"
        elif condition_type == 'on_date':
            return f"{field} = ?"
        
        return f"{field} = ?"
    
    def _build_group_by_clause(self, parsed: ParsedQuery) -> str:
        if parsed.group_by:
            return f"GROUP BY {', '.join(parsed.group_by)}"
        elif parsed.aggregations and parsed.columns:
            non_agg_columns = [col for col in parsed.columns 
                             if col != '*' and col not in [agg['column'] for agg in parsed.aggregations]]
            if non_agg_columns:
                self.assumptions.append("Grouping by non-aggregated columns")
                self.confidence *= 0.9
                return f"GROUP BY {', '.join(non_agg_columns)}"
        return ""
    
    def _build_order_by_clause(self, parsed: ParsedQuery) -> str:
        order_items = []
        
        for order in parsed.order_by:
            column = order['column']
            direction = order.get('direction', 'ASC')
            order_items.append(f"{column} {direction}")
        
        return f"ORDER BY {', '.join(order_items)}"
        
    def _generate_insert(self, parsed: ParsedQuery, schema: Dict) -> str:
        if not parsed.tables:
            self.confidence = 0.1
            return "-- Unable to generate INSERT query: no table specified"
        
        table = parsed.tables[0]
        self.assumptions.append("INSERT query generation requires values to be provided separately")
        self.confidence = 0.7
        
        if table in schema:
            columns = [col['name'] for col in schema[table]['columns'] if col['name'] != 'id']
            placeholders = ', '.join(['?' for _ in columns])
            return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        return f"INSERT INTO {table} VALUES (?)"
    
    def _generate_update(self, parsed: ParsedQuery, schema: Dict) -> str:
        if not parsed.tables:
            self.confidence = 0.1
            return "-- Unable to generate UPDATE query: no table specified"
        
        table = parsed.tables[0]
        self.assumptions.append("UPDATE query requires SET values to be provided")
        self.confidence = 0.7
        
        set_clause = "SET column_name = ?"
        where_clause = ""
        if parsed.conditions:
            where_clause = self._build_where_clause(parsed)
        else:
            self.assumptions.append("No WHERE clause specified - this will update ALL rows")
            self.confidence *= 0.5
        
        query_parts = [f"UPDATE {table}", set_clause]
        if where_clause:
            query_parts.append(where_clause)
        
        return '\n'.join(query_parts)
    
    def _generate_delete(self, parsed: ParsedQuery, schema: Dict) -> str:
        if not parsed.tables:
            self.confidence = 0.1
            return "-- Unable to generate DELETE query: no table specified"
        
        table = parsed.tables[0]
        where_clause = ""
        if parsed.conditions:
            where_clause = self._build_where_clause(parsed)
        else:
            self.assumptions.append("No WHERE clause specified - this will delete ALL rows")
            self.confidence *= 0.3
        
        query_parts = [f"DELETE FROM {table}"]
        if where_clause:
            query_parts.append(where_clause)
        
        return '\n'.join(query_parts)
