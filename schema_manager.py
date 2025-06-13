import sqlite3
import json
import os
from typing import Dict, List, Optional

class SchemaManager:
    def __init__(self, db_path: str = "sample_database.db"):
        self.db_path = db_path
        self.schema = {}
        self.connection = None
        
    def load_sample_schema(self) -> None:
        """Load a sample e-commerce schema for demonstration"""
        # Define sample schema
        self.schema = {
            "customers": {
                "columns": [
                    {"name": "id", "type": "INTEGER PRIMARY KEY"},
                    {"name": "name", "type": "TEXT"},
                    {"name": "email", "type": "TEXT"},
                    {"name": "phone", "type": "TEXT"},
                    {"name": "created_at", "type": "DATETIME"},
                    {"name": "city", "type": "TEXT"},
                    {"name": "country", "type": "TEXT"}
                ],
                "relationships": [
                    "orders.customer_id = customers.id"
                ]
            },
            "products": {
                "columns": [
                    {"name": "id", "type": "INTEGER PRIMARY KEY"},
                    {"name": "name", "type": "TEXT"},
                    {"name": "category", "type": "TEXT"},
                    {"name": "price", "type": "DECIMAL(10,2)"},
                    {"name": "stock_quantity", "type": "INTEGER"},
                    {"name": "description", "type": "TEXT"}
                ],
                "relationships": [
                    "order_items.product_id = products.id"
                ]
            },
            "orders": {
                "columns": [
                    {"name": "id", "type": "INTEGER PRIMARY KEY"},
                    {"name": "customer_id", "type": "INTEGER"},
                    {"name": "order_date", "type": "DATETIME"},
                    {"name": "total_amount", "type": "DECIMAL(10,2)"},
                    {"name": "status", "type": "TEXT"},
                    {"name": "shipping_address", "type": "TEXT"}
                ],
                "relationships": [
                    "orders.customer_id = customers.id",
                    "order_items.order_id = orders.id"
                ]
            },
            "order_items": {
                "columns": [
                    {"name": "id", "type": "INTEGER PRIMARY KEY"},
                    {"name": "order_id", "type": "INTEGER"},
                    {"name": "product_id", "type": "INTEGER"},
                    {"name": "quantity", "type": "INTEGER"},
                    {"name": "unit_price", "type": "DECIMAL(10,2)"},
                    {"name": "subtotal", "type": "DECIMAL(10,2)"}
                ],
                "relationships": [
                    "order_items.order_id = orders.id",
                    "order_items.product_id = products.id"
                ]
            }
        }
        
        # Create actual database with sample data
        self._create_sample_database()
    def load_tally_schema(self) -> None:
        """
        Load the full Tally-ERP schema from tally_schema.sql.
        Drops any existing tables, then runs the DDL and
        builds self.schema for your parser.
        """
        # 1) Read the SQL file
        ddl_path = os.path.join(os.path.dirname(__file__), "tally_schema.sql")
        with open(ddl_path, "r") as f:
            ddl_sql = f.read()

        # 2) Build the in-memory schema map
        self.schema = {
            "config": {
                "columns": [
                    {"name":"name","type":"VARCHAR(64)"},
                    {"name":"value","type":"VARCHAR(1024)"},
                    {"name":"user_id","type":"VARCHAR(255)"},
                    {"name":"company_name","type":"VARCHAR(255)"}
                ],
                "relationships": []
            },
            "mst_group": {
                "columns": [
                    {"name":"guid","type":"VARCHAR(64)"},
                    {"name":"name","type":"VARCHAR(1024)"},
                    # … all the columns …
                ],
                "relationships": []
            },
            # … repeat for every mst_… and trn_… table …
        }

        # 3) (Re)create the SQLite DB
        if self.connection:
            self.connection.close()
        self.connection = sqlite3.connect(self.db_path)
        cursor = self.connection.cursor()

        # 4) Drop tables if they exist
        for tbl in self.schema:
            cursor.execute(f"DROP TABLE IF EXISTS {tbl}")

        # 5) Execute the full DDL
        cursor.executescript(ddl_sql)
        self.connection.commit()
        cursor.close()
    
    def _create_sample_database(self) -> None:
        """Create SQLite database with sample data"""
        # Close existing connection if any
        if self.connection:
            self.connection.close()
        
        # Create new connection
        self.connection = sqlite3.connect(self.db_path)
        cursor = self.connection.cursor()
        
        # Drop existing tables
        for table in ['order_items', 'orders', 'products', 'customers']:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Create tables
        cursor.execute("""
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                phone TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                city TEXT,
                country TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT,
                price DECIMAL(10,2),
                stock_quantity INTEGER DEFAULT 0,
                description TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER,
                order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_amount DECIMAL(10,2),
                status TEXT DEFAULT 'pending',
                shipping_address TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                subtotal DECIMAL(10,2),
                FOREIGN KEY (order_id) REFERENCES orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # Insert sample data
        # Customers
        customers = [
            ("John Doe", "john@email.com", "123-456-7890", "New York", "USA"),
            ("Jane Smith", "jane@email.com", "098-765-4321", "London", "UK"),
            ("Bob Johnson", "bob@email.com", "555-123-4567", "Toronto", "Canada"),
            ("Alice Brown", "alice@email.com", "555-987-6543", "Sydney", "Australia"),
            ("Charlie Wilson", "charlie@email.com", "555-246-8135", "Paris", "France")
        ]
        cursor.executemany(
            "INSERT INTO customers (name, email, phone, city, country) VALUES (?, ?, ?, ?, ?)", 
            customers
        )
        
        # Products
        products = [
            ("Laptop", "Electronics", 999.99, 50, "High-performance laptop"),
            ("Mouse", "Electronics", 29.99, 200, "Wireless mouse"),
            ("Keyboard", "Electronics", 79.99, 150, "Mechanical keyboard"),
            ("Monitor", "Electronics", 299.99, 75, "27-inch LED monitor"),
            ("Desk Chair", "Furniture", 199.99, 30, "Ergonomic office chair"),
            ("Standing Desk", "Furniture", 499.99, 20, "Adjustable height desk"),
            ("Notebook", "Stationery", 4.99, 500, "Spiral notebook"),
            ("Pen Set", "Stationery", 12.99, 300, "Professional pen set"),
            ("Headphones", "Electronics", 149.99, 100, "Noise-cancelling headphones"),
            ("Webcam", "Electronics", 89.99, 80, "HD webcam")
        ]
        cursor.executemany(
            "INSERT INTO products (name, category, price, stock_quantity, description) VALUES (?, ?, ?, ?, ?)", 
            products
        )
        
        # Orders
        orders = [
            (1, "2024-12-15", 1079.97, "completed", "123 Main St, New York, USA"),
            (2, "2024-12-20", 379.98, "completed", "456 High St, London, UK"),
            (3, "2025-01-05", 229.97, "shipped", "789 King St, Toronto, Canada"),
            (1, "2025-01-10", 149.99, "processing", "123 Main St, New York, USA"),
            (4, "2025-01-12", 699.98, "pending", "321 George St, Sydney, Australia")
        ]
        cursor.executemany(
            "INSERT INTO orders (customer_id, order_date, total_amount, status, shipping_address) VALUES (?, ?, ?, ?, ?)", 
            orders
        )
        
        # Order Items
        order_items = [
            (1, 1, 1, 999.99, 999.99),      # Order 1: Laptop
            (1, 3, 1, 79.99, 79.99),        # Order 1: Keyboard
            (2, 2, 2, 29.99, 59.98),        # Order 2: 2 Mice
            (2, 4, 1, 299.99, 299.99),      # Order 2: Monitor
            (3, 7, 10, 4.99, 49.90),        # Order 3: 10 Notebooks
            (3, 8, 15, 12.99, 194.85),      # Order 3: 15 Pen Sets
            (4, 9, 1, 149.99, 149.99),      # Order 4: Headphones
            (5, 5, 2, 199.99, 399.98),      # Order 5: 2 Desk Chairs
            (5, 6, 1, 499.99, 499.99)       # Order 5: Standing Desk
        ]
        cursor.executemany(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES (?, ?, ?, ?, ?)", 
            order_items
        )
        
        self.connection.commit()
    
    def get_schema(self) -> Dict:
        """Get current schema"""
        return self.schema
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        if not self.connection:
            self.connection = sqlite3.connect(self.db_path)
        return self.connection
    
    def load_schema_from_database(self, connection: sqlite3.Connection) -> Dict:
        """Load schema from an existing database"""
        cursor = connection.cursor()
        schema = {}
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            if table_name.startswith('sqlite_'):
                continue
                
            schema[table_name] = {
                'columns': [],
                'relationships': []
            }
            
            # Get column information
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for col in columns:
                schema[table_name]['columns'].append({
                    'name': col[1],
                    'type': col[2],
                    'nullable': not col[3],
                    'default': col[4],
                    'primary_key': bool(col[5])
                })
            
            # Get foreign key information
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            
            for fk in foreign_keys:
                relationship = f"{table_name}.{fk[3]} = {fk[2]}.{fk[4]}"
                schema[table_name]['relationships'].append(relationship)
        
        self.schema = schema
        return schema
    
    def validate_schema(self) -> Dict[str, List[str]]:
        """Validate schema for consistency"""
        issues = {
            'warnings': [],
            'errors': []
        }
        
        # Check for tables without primary keys
        for table, info in self.schema.items():
            has_pk = any(col.get('primary_key', False) for col in info['columns'])
            if not has_pk:
                issues['warnings'].append(f"Table '{table}' has no primary key")
        
        # Check for orphaned relationships
        for table, info in self.schema.items():
            for relationship in info.get('relationships', []):
                # Parse relationship
                parts = relationship.split(' = ')
                if len(parts) == 2:
                    # Check if referenced tables exist
                    for part in parts:
                        ref_table = part.split('.')[0]
                        if ref_table not in self.schema:
                            issues['errors'].append(
                                f"Relationship '{relationship}' references non-existent table '{ref_table}'"
                            )
        
        return issues
    
    def export_schema(self, format: str = 'json') -> str:
        """Export schema in specified format"""
        if format == 'json':
            return json.dumps(self.schema, indent=2)
        elif format == 'sql':
            return self._generate_create_statements()
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _generate_create_statements(self) -> str:
        """Generate SQL CREATE statements from schema"""
        statements = []
        
        for table, info in self.schema.items():
            columns_sql = []
            
            for col in info['columns']:
                col_def = f"{col['name']} {col['type']}"
                if col.get('primary_key'):
                    col_def += " PRIMARY KEY"
                elif not col.get('nullable', True):
                    col_def += " NOT NULL"
                if col.get('default'):
                    col_def += f" DEFAULT {col['default']}"
                
                columns_sql.append(col_def)
            
            create_stmt = f"CREATE TABLE {table} (\n  " + ",\n  ".join(columns_sql) + "\n);"
            statements.append(create_stmt)
        
        return "\n\n".join(statements)
    
    def get_table_statistics(self) -> Dict[str, Dict]:
        """Get statistics for each table"""
        if not self.connection:
            return {}
        
        cursor = self.connection.cursor()
        stats = {}
        
        for table in self.schema.keys():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            
            stats[table] = {
                'row_count': row_count,
                'column_count': len(self.schema[table]['columns']),
                'relationship_count': len(self.schema[table].get('relationships', []))
            }
        
        return stats
