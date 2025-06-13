"""
Database Schema Manager - Preloaded Schema Management
Handles predefined database schemas and table structures
"""
import json
import logging
from typing import Dict, List, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SchemaManager:
    """Manages preloaded database schemas"""
    
    def __init__(self):
        self.schemas = {}
        self.current_schema = None
        self._load_default_schemas()
    
    def _load_default_schemas(self):
        """Load predefined schemas"""
        
        # E-commerce Schema
        ecommerce_schema = {
            "name": "ecommerce",
            "description": "E-commerce database with customers, products, and orders",
            "tables": {
                "customers": {
                    "description": "Customer information",
                    "columns": {
                        "customer_id": {"type": "INTEGER", "primary_key": True, "description": "Unique customer identifier"},
                        "first_name": {"type": "VARCHAR(50)", "nullable": False, "description": "Customer first name"},
                        "last_name": {"type": "VARCHAR(50)", "nullable": False, "description": "Customer last name"},
                        "email": {"type": "VARCHAR(100)", "unique": True, "description": "Customer email address"},
                        "phone": {"type": "VARCHAR(20)", "nullable": True, "description": "Customer phone number"},
                        "address": {"type": "TEXT", "nullable": True, "description": "Customer address"},
                        "city": {"type": "VARCHAR(50)", "nullable": True, "description": "Customer city"},
                        "country": {"type": "VARCHAR(50)", "nullable": True, "description": "Customer country"},
                        "registration_date": {"type": "DATE", "nullable": False, "description": "Customer registration date"}
                    },
                    "sample_data": [
                        (1, "John", "Doe", "john.doe@email.com", "+1234567890", "123 Main St", "New York", "USA", "2023-01-15"),
                        (2, "Jane", "Smith", "jane.smith@email.com", "+1234567891", "456 Oak Ave", "Los Angeles", "USA", "2023-02-20"),
                        (3, "Bob", "Johnson", "bob.johnson@email.com", "+1234567892", "789 Pine Rd", "Chicago", "USA", "2023-03-10")
                    ]
                },
                "products": {
                    "description": "Product catalog",
                    "columns": {
                        "product_id": {"type": "INTEGER", "primary_key": True, "description": "Unique product identifier"},
                        "product_name": {"type": "VARCHAR(100)", "nullable": False, "description": "Product name"},
                        "category": {"type": "VARCHAR(50)", "nullable": False, "description": "Product category"},
                        "price": {"type": "DECIMAL(10,2)", "nullable": False, "description": "Product price"},
                        "stock_quantity": {"type": "INTEGER", "nullable": False, "description": "Available stock quantity"},
                        "description": {"type": "TEXT", "nullable": True, "description": "Product description"},
                        "brand": {"type": "VARCHAR(50)", "nullable": True, "description": "Product brand"},
                        "created_date": {"type": "DATE", "nullable": False, "description": "Product creation date"}
                    },
                    "sample_data": [
                        (1, "Laptop", "Electronics", 999.99, 50, "High-performance laptop", "TechBrand", "2023-01-01"),
                        (2, "Smartphone", "Electronics", 699.99, 100, "Latest smartphone", "PhoneCorp", "2023-01-05"),
                        (3, "Headphones", "Electronics", 199.99, 75, "Noise-cancelling headphones", "AudioTech", "2023-01-10"),
                        (4, "T-Shirt", "Clothing", 29.99, 200, "Cotton t-shirt", "FashionCo", "2023-01-15"),
                        (5, "Jeans", "Clothing", 79.99, 150, "Denim jeans", "FashionCo", "2023-01-20")
                    ]
                },
                "orders": {
                    "description": "Customer orders",
                    "columns": {
                        "order_id": {"type": "INTEGER", "primary_key": True, "description": "Unique order identifier"},
                        "customer_id": {"type": "INTEGER", "foreign_key": "customers.customer_id", "description": "Customer who placed the order"},
                        "order_date": {"type": "DATE", "nullable": False, "description": "Order placement date"},
                        "status": {"type": "VARCHAR(20)", "nullable": False, "description": "Order status (pending, shipped, delivered, cancelled)"},
                        "total_amount": {"type": "DECIMAL(10,2)", "nullable": False, "description": "Total order amount"},
                        "shipping_address": {"type": "TEXT", "nullable": True, "description": "Shipping address"}
                    },
                    "sample_data": [
                        (1, 1, "2023-04-01", "delivered", 1199.98, "123 Main St, New York, USA"),
                        (2, 2, "2023-04-05", "shipped", 699.99, "456 Oak Ave, Los Angeles, USA"),
                        (3, 1, "2023-04-10", "pending", 229.98, "123 Main St, New York, USA"),
                        (4, 3, "2023-04-15", "delivered", 109.98, "789 Pine Rd, Chicago, USA")
                    ]
                },
                "order_items": {
                    "description": "Individual items in each order",
                    "columns": {
                        "order_item_id": {"type": "INTEGER", "primary_key": True, "description": "Unique order item identifier"},
                        "order_id": {"type": "INTEGER", "foreign_key": "orders.order_id", "description": "Order this item belongs to"},
                        "product_id": {"type": "INTEGER", "foreign_key": "products.product_id", "description": "Product being ordered"},
                        "quantity": {"type": "INTEGER", "nullable": False, "description": "Quantity of product ordered"},
                        "unit_price": {"type": "DECIMAL(10,2)", "nullable": False, "description": "Price per unit at time of order"}
                    },
                    "sample_data": [
                        (1, 1, 1, 1, 999.99),
                        (2, 1, 3, 1, 199.99),
                        (3, 2, 2, 1, 699.99),
                        (4, 3, 3, 1, 199.99),
                        (5, 3, 4, 1, 29.99),
                        (6, 4, 4, 2, 29.99),
                        (7, 4, 5, 1, 79.99)
                    ]
                }
            },
            "relationships": [
                {"from": "orders.customer_id", "to": "customers.customer_id", "type": "many_to_one"},
                {"from": "order_items.order_id", "to": "orders.order_id", "type": "many_to_one"},
                {"from": "order_items.product_id", "to": "products.product_id", "type": "many_to_one"}
            ]
        }
        
        # HR Schema
        hr_schema = {
            "name": "hr",
            "description": "Human Resources database with employees and departments",
            "tables": {
                "departments": {
                    "description": "Company departments",
                    "columns": {
                        "dept_id": {"type": "INTEGER", "primary_key": True, "description": "Unique department identifier"},
                        "dept_name": {"type": "VARCHAR(50)", "nullable": False, "description": "Department name"},
                        "manager_id": {"type": "INTEGER", "nullable": True, "description": "Department manager employee ID"},
                        "budget": {"type": "DECIMAL(12,2)", "nullable": True, "description": "Department budget"},
                        "location": {"type": "VARCHAR(100)", "nullable": True, "description": "Department location"}
                    },
                    "sample_data": [
                        (1, "Engineering", 101, 500000.00, "Building A"),
                        (2, "Marketing", 102, 200000.00, "Building B"),
                        (3, "Sales", 103, 300000.00, "Building C"),
                        (4, "HR", 104, 150000.00, "Building B")
                    ]
                },
                "employees": {
                    "description": "Employee information",
                    "columns": {
                        "emp_id": {"type": "INTEGER", "primary_key": True, "description": "Unique employee identifier"},
                        "first_name": {"type": "VARCHAR(50)", "nullable": False, "description": "Employee first name"},
                        "last_name": {"type": "VARCHAR(50)", "nullable": False, "description": "Employee last name"},
                        "email": {"type": "VARCHAR(100)", "unique": True, "description": "Employee email"},
                        "dept_id": {"type": "INTEGER", "foreign_key": "departments.dept_id", "description": "Department ID"},
                        "position": {"type": "VARCHAR(100)", "nullable": False, "description": "Job position"},
                        "salary": {"type": "DECIMAL(10,2)", "nullable": False, "description": "Employee salary"},
                        "hire_date": {"type": "DATE", "nullable": False, "description": "Employee hire date"},
                        "manager_id": {"type": "INTEGER", "nullable": True, "description": "Manager employee ID"}
                    },
                    "sample_data": [
                        (101, "Alice", "Johnson", "alice.johnson@company.com", 1, "Engineering Manager", 120000.00, "2020-01-15", None),
                        (102, "Bob", "Smith", "bob.smith@company.com", 2, "Marketing Manager", 95000.00, "2020-02-01", None),
                        (103, "Carol", "Davis", "carol.davis@company.com", 3, "Sales Manager", 110000.00, "2020-03-01", None),
                        (104, "David", "Wilson", "david.wilson@company.com", 4, "HR Manager", 85000.00, "2020-04-01", None),
                        (105, "Emma", "Brown", "emma.brown@company.com", 1, "Senior Developer", 95000.00, "2021-01-15", 101),
                        (106, "Frank", "Miller", "frank.miller@company.com", 1, "Developer", 75000.00, "2021-06-01", 101),
                        (107, "Grace", "Taylor", "grace.taylor@company.com", 2, "Marketing Specialist", 65000.00, "2021-08-15", 102)
                    ]
                }
            },
            "relationships": [
                {"from": "employees.dept_id", "to": "departments.dept_id", "type": "many_to_one"},
                {"from": "employees.manager_id", "to": "employees.emp_id", "type": "many_to_one"},
                {"from": "departments.manager_id", "to": "employees.emp_id", "type": "one_to_one"}
            ]
        }
        
        self.schemas["ecommerce"] = ecommerce_schema
        self.schemas["hr"] = hr_schema
        self.current_schema = "ecommerce"  # Default schema
        
        logger.info(f"Loaded {len(self.schemas)} predefined schemas")
    
    def get_current_schema(self) -> Dict[str, Any]:
        """Get the currently active schema"""
        return self.schemas.get(self.current_schema, {})
    
    def set_current_schema(self, schema_name: str) -> bool:
        """Set the current active schema"""
        if schema_name in self.schemas:
            self.current_schema = schema_name
            logger.info(f"Switched to schema: {schema_name}")
            return True
        return False
    
    def get_available_schemas(self) -> List[str]:
        """Get list of available schema names"""
        return list(self.schemas.keys())
    
    def get_table_names(self) -> List[str]:
        """Get all table names in current schema"""
        schema = self.get_current_schema()
        return list(schema.get("tables", {}).keys())
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific table"""
        schema = self.get_current_schema()
        return schema.get("tables", {}).get(table_name, {})
    
    def get_column_names(self, table_name: str) -> List[str]:
        """Get column names for a specific table"""
        table_info = self.get_table_info(table_name)
        return list(table_info.get("columns", {}).keys())
    
    def get_column_info(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific column"""
        table_info = self.get_table_info(table_name)
        return table_info.get("columns", {}).get(column_name, {})
    
    def get_relationships(self) -> List[Dict[str, str]]:
        """Get all table relationships in current schema"""
        schema = self.get_current_schema()
        return schema.get("relationships", [])
    
    def find_related_tables(self, table_name: str) -> List[Dict[str, str]]:
        """Find tables related to the given table"""
        relationships = self.get_relationships()
        related = []
        
        for rel in relationships:
            if table_name in rel["from"] or table_name in rel["to"]:
                related.append(rel)
        
        return related
    
    def get_schema_summary(self) -> str:
        """Get a formatted summary of the current schema"""
        schema = self.get_current_schema()
        if not schema:
            return "No schema loaded"
        
        summary = [f"Schema: {schema['name']}"]
        summary.append(f"Description: {schema['description']}")
        summary.append("\nTables:")
        
        for table_name, table_info in schema["tables"].items():
            summary.append(f"\n  {table_name}: {table_info['description']}")
            summary.append("    Columns:")
            
            for col_name, col_info in table_info["columns"].items():
                pk_marker = " (PK)" if col_info.get("primary_key") else ""
                fk_marker = f" (FK -> {col_info['foreign_key']})" if col_info.get("foreign_key") else ""
                summary.append(f"      - {col_name}: {col_info['type']}{pk_marker}{fk_marker}")
        
        return "\n".join(summary)
    
    def search_columns(self, search_term: str) -> List[Dict[str, str]]:
        """Search for columns containing the search term"""
        results = []
        schema = self.get_current_schema()
        
        for table_name, table_info in schema.get("tables", {}).items():
            for col_name, col_info in table_info.get("columns", {}).items():
                if (search_term.lower() in col_name.lower() or 
                    search_term.lower() in col_info.get("description", "").lower()):
                    results.append({
                        "table": table_name,
                        "column": col_name,
                        "type": col_info.get("type", ""),
                        "description": col_info.get("description", "")
                    })
        
        return results
    
    def validate_table_column(self, table_name: str, column_name: str) -> bool:
        """Validate if table and column exist in current schema"""
        table_info = self.get_table_info(table_name)
        if not table_info:
            return False
        return column_name in table_info.get("columns", {})
    
    def export_schema(self) -> Dict[str, Any]:
        """Export current schema as dictionary"""
        return self.get_current_schema()

# Global schema manager instance
schema_manager = SchemaManager()

def get_schema_manager() -> SchemaManager:
    """Get the global schema manager instance"""
    return schema_manager
