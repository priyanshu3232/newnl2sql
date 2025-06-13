import streamlit as st
import json
import sqlite3
import re
from datetime import datetime
import pandas as pd
from typing import Dict, List, Tuple, Optional
import os

# Import our custom modules
from query_parser import NaturalLanguageParser
from sql_generator import SQLGenerator
from query_executor import QueryExecutor
from feedback_manager import FeedbackManager
from schema_manager import SchemaManager

# Initialize session state
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'feedback_manager' not in st.session_state:
    st.session_state.feedback_manager = FeedbackManager()
if 'schema_manager' not in st.session_state:
    st.session_state.schema_manager = SchemaManager()
if 'parser' not in st.session_state:
    st.session_state.parser = NaturalLanguageParser()
if 'sql_generator' not in st.session_state:
    st.session_state.sql_generator = SQLGenerator()
if 'executor' not in st.session_state:
    st.session_state.executor = QueryExecutor()
if 'current_user' not in st.session_state:
    st.session_state.current_user = "demo_user"
if 'current_company' not in st.session_state:
    st.session_state.current_company = "Demo Company Ltd"

# Page config
st.set_page_config(
    page_title="Tally ERP Text-to-SQL Agent",
    page_icon="💼",
    layout="wide"
)

# Title and description
st.title("💼 Tally ERP Text-to-SQL Agent")
st.markdown("### Convert natural language queries to SQL for Tally ERP database")

# User and Company Selection
with st.sidebar:
    st.header("👤 User Context")
    
    # User selection (in real app, this would be authentication)
    st.session_state.current_user = st.text_input(
        "User ID", 
        value=st.session_state.current_user,
        help="User ID for data isolation"
    )
    
    st.session_state.current_company = st.text_input(
        "Company Name", 
        value=st.session_state.current_company,
        help="Company name for data filtering"
    )
    
    st.markdown("---")
    
    st.header("📊 Tally ERP Database")
    
    # Load Tally schema
    if st.button("Load Tally ERP Schema"):
        with st.spinner("Loading Tally ERP database structure..."):
            st.session_state.schema_manager.load_tally_schema()
            st.success("Tally ERP schema loaded successfully!")
    
    # Display current schema
    schema = st.session_state.schema_manager.get_schema()
    if schema:
        # Show database statistics
        stats = st.session_state.schema_manager.get_table_statistics()
        
        st.subheader("📈 Database Statistics")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Tables", len(schema))
        with col2:
            total_records = sum(stat.get('row_count', 0) for stat in stats.values())
            st.metric("Total Records", total_records)
        
        # Schema browser
        st.subheader("🗂️ Schema Browser")
        
        # Group tables by type
        master_tables = {k: v for k, v in schema.items() if k.startswith('mst_')}
        transaction_tables = {k: v for k, v in schema.items() if k.startswith('trn_')}
        other_tables = {k: v for k, v in schema.items() if not k.startswith(('mst_', 'trn_'))}
        
        # Master Tables
        if master_tables:
            with st.expander("📋 Master Tables", expanded=False):
                for table_name, table_info in master_tables.items():
                    st.markdown(f"**{table_name}**")
                    st.write(f"Description: {table_info.get('description', 'No description')}")
                    st.write(f"Records: {stats.get(table_name, {}).get('row_count', 0)}")
                    
                    st.markdown("Key Columns:")
                    key_columns = [col for col in table_info['columns'][:5]]  # Show first 5
                    for col in key_columns:
                        st.text(f"• {col['name']} ({col['type']})")
                    
                    if len(table_info['columns']) > 5:
                        st.text(f"... and {len(table_info['columns'])-5} more columns")
                    st.markdown("---")
        
        # Transaction Tables
        if transaction_tables:
            with st.expander("💱 Transaction Tables", expanded=False):
                for table_name, table_info in transaction_tables.items():
                    st.markdown(f"**{table_name}**")
                    st.write(f"Description: {table_info.get('description', 'No description')}")
                    st.write(f"Records: {stats.get(table_name, {}).get('row_count', 0)}")
                    
                    st.markdown("Key Columns:")
                    key_columns = [col for col in table_info['columns'][:5]]
                    for col in key_columns:
                        st.text(f"• {col['name']} ({col['type']})")
                    st.markdown("---")
        
        # Other Tables  
        if other_tables:
            with st.expander("⚙️ Configuration Tables", expanded=False):
                for table_name, table_info in other_tables.items():
                    st.markdown(f"**{table_name}**")
                    st.write(f"Records: {stats.get(table_name, {}).get('row_count', 0)}")
                    
                    st.markdown("Key Columns:")
                    for col in table_info['columns'][:3]:
                        st.text(f"• {col['name']} ({col['type']})")
                    st.markdown("---")

# Main content area
col1, col2 = st.columns([2, 1])

with col1:
    # Sample queries section
    st.header("🎯 Try These Sample Queries")
    
    if schema:
        sample_queries = st.session_state.schema_manager.get_sample_queries()
        
        # Create columns for sample query buttons
        query_cols = st.columns(3)
        for i, query in enumerate(sample_queries[:6]):  # Show first 6
            with query_cols[i % 3]:
                if st.button(query, key=f"sample_{i}", help="Click to use this query"):
                    st.session_state.sample_query = query
    
    # Query input
    st.header("💬 Enter Your Query")
    
    # Use sample query if selected
    default_query = st.session_state.get('sample_query', '')
    if default_query:
        # Clear the sample query after using it
        del st.session_state.sample_query
    
    user_query = st.text_area(
        "Natural Language Query",
        value=default_query,
        placeholder="e.g., Show me all employees with their salary details for January 2024",
        height=100,
        help="Ask questions about employees, ledgers, stock items, vouchers, etc."
    )
    
    # Query processing
    col_btn1, col_btn2, col_btn3 = st.columns(3)
    
    with col_btn1:
        generate_clicked = st.button("🔮 Generate SQL", type="primary")
    
    with col_btn2:
        if st.button("📊 Generate Report"):
            if user_query:
                # Detect report type from query
                query_lower = user_query.lower()
                if 'trial balance' in query_lower:
                    report_type = 'trial_balance'
                elif 'stock summary' in query_lower or 'inventory report' in query_lower:
                    report_type = 'stock_summary'
                elif 'payroll' in query_lower or 'salary' in query_lower:
                    report_type = 'payroll_summary'
                elif 'gst' in query_lower:
                    report_type = 'gst_report'
                else:
                    report_type = 'custom'
                
                with st.spinner("Generating specialized report..."):
                    parsed_result = st.session_state.parser.parse(
                        user_query, 
                        st.session_state.schema_manager.get_schema(),
                        st.session_state.current_user,
                        st.session_state.current_company
                    )
                    
                    if report_type != 'custom':
                        sql_query = st.session_state.sql_generator.generate_tally_report_query(
                            report_type, parsed_result, st.session_state.schema_manager.get_schema()
                        )
                        sql_result = {
                            'query': sql_query,
                            'parameters': st.session_state.sql_generator.parameters,
                            'assumptions': st.session_state.sql_generator.assumptions,
                            'confidence': 0.95
                        }
                    else:
                        sql_result = st.session_state.sql_generator.generate(
                            parsed_result,
                            st.session_state.schema_manager.get_schema()
                        )
                    
                    st.session_state.current_sql = sql_result
                    st.session_state.current_query = user_query
                    st.session_state.current_parsed = parsed_result
    
    with col_btn3:
        if st.button("🧹 Clear"):
            if 'current_sql' in st.session_state:
                del st.session_state.current_sql
            if 'current_query' in st.session_state:
                del st.session_state.current_query
            st.rerun()
    
    # Process generate SQL button
    if generate_clicked and user_query:
        with st.spinner("Parsing your query..."):
            # Parse the natural language query
            parsed_result = st.session_state.parser.parse(
                user_query, 
                st.session_state.schema_manager.get_schema(),
                st.session_state.current_user,
                st.session_state.current_company
            )
            
            # Generate SQL
            sql_result = st.session_state.sql_generator.generate(
                parsed_result,
                st.session_state.schema_manager.get_schema()
            )
            
            # Store in session state for confirmation
            st.session_state.current_sql = sql_result
            st.session_state.current_query = user_query
            st.session_state.current_parsed = parsed_result
    
    # Display generated SQL and assumptions
    if hasattr(st.session_state, 'current_sql'):
        st.header("🔧 Generated SQL Query")
        
        # Show confidence and assumptions
        col_conf, col_ass = st.columns(2)
        
        with col_conf:
            confidence = st.session_state.current_sql.get('confidence', 0.0)
            st.metric("Confidence Score", f"{confidence:.0%}")
            
            # Color code based on confidence
            if confidence >= 0.8:
                st.success("High confidence")
            elif confidence >= 0.6:
                st.warning("Medium confidence") 
            else:
                st.error("Low confidence - please review carefully")
        
        with col_ass:
            assumptions = st.session_state.current_sql.get('assumptions', [])
            if assumptions:
                with st.expander("📋 Assumptions Made", expanded=True):
                    for i, assumption in enumerate(assumptions):
                        st.info(f"{i+1}. {assumption}")
            else:
                st.info("No specific assumptions were made.")
        
        # SQL Query display with editing capability
        st.subheader("📝 SQL Query")
        edited_sql = st.text_area(
            "You can edit the SQL if needed:",
            value=st.session_state.current_sql['query'],
            height=200,
            help="Review and modify the generated SQL query before execution"
        )
        
        # Show parameters if any
        parameters = st.session_state.current_sql.get('parameters', [])
        if parameters:
            with st.expander("🔒 Query Parameters", expanded=False):
                st.write("The following parameters will be used for secure execution:")
                for i, param in enumerate(parameters):
                    st.code(f"Parameter {i+1}: {param}")
        
        # Confirmation buttons
        col1_1, col1_2, col1_3 = st.columns(3)
        
        with col1_1:
            if st.button("✅ Execute Query", type="primary"):
                # Execute the query with parameters
                result = st.session_state.executor.execute(
                    edited_sql,
                    st.session_state.schema_manager.get_connection(),
                    parameters
                )
                
                if result['success']:
                    st.success("Query executed successfully!")
                    
                    # Display results
                    if result['data']:
                        st.subheader("📊 Query Results")
                        df = pd.DataFrame(result['data'])
                        
                        # Format numeric columns
                        for col in df.columns:
                            if df[col].dtype in ['float64', 'int64']:
                                if 'amount' in col.lower() or 'balance' in col.lower() or 'value' in col.lower():
                                    df[col] = df[col].apply(lambda x: f"₹{x:,.2f}" if pd.notnull(x) else "")
                        
                        st.dataframe(df, use_container_width=True)
                        
                        # Show export options
                        col_exp1, col_exp2 = st.columns(2)
                        with col_exp1:
                            csv = df.to_csv(index=False)
                            st.download_button(
                                label="📥 Download CSV",
                                data=csv,
                                file_name=f"tally_query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                        
                        with col_exp2:
                            st.info(f"Found {len(df)} records")
                        
                        # Show parameter info if any were used
                        if parameters:
                            with st.expander("🔒 Security Info"):
                                st.success(f"Query executed securely with {len(parameters)} parameters")
                        
                        # Log successful query
                        st.session_state.query_history.append({
                            'timestamp': datetime.now().isoformat(),
                            'natural_query': st.session_state.current_query,
                            'sql_query': edited_sql,
                            'parameters': parameters,
                            'success': True,
                            'result_count': len(df),
                            'feedback': 'positive'
                        })
                        
                        # Update feedback manager
                        st.session_state.feedback_manager.add_feedback(
                            st.session_state.current_query,
                            edited_sql,
                            'positive'
                        )
                    else:
                        st.info("Query executed successfully but returned no results.")
                        st.info("💡 Try adjusting your search criteria or date ranges.")
                else:
                    st.error(f"❌ Error: {result['error']}")
                    
                    # Log failed query
                    st.session_state.query_history.append({
                        'timestamp': datetime.now().isoformat(),
                        'natural_query': st.session_state.current_query,
                        'sql_query': edited_sql,
                        'parameters': parameters,
                        'success': False,
                        'error': result['error']
                    })
        
        with col1_2:
            if st.button("🔄 Regenerate"):
                # Clear current SQL to regenerate
                del st.session_state.current_sql
                st.rerun()
        
        with col1_3:
            if st.button("❌ Cancel"):
                # Clear current SQL
                del st.session_state.current_sql
                st.rerun()

with col2:
    # Query history
    st.header("📜 Query History")
    
    if st.session_state.query_history:
        # Show last 5 queries
        for i, query in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"Query {len(st.session_state.query_history) - i}"):
                st.text(f"⏰ {query['timestamp'][:19]}")
                st.text(f"💬 {query['natural_query'][:50]}...")
                
                if query['success']:
                    st.success(f"✅ Success - {query.get('result_count', 0)} records")
                else:
                    st.error("❌ Failed")
                
                # Show SQL in expandable section
                with st.expander("View SQL"):
                    st.code(query['sql_query'], language='sql')
                
                # Feedback buttons
                col_fb1, col_fb2 = st.columns(2)
                with col_fb1:
                    if st.button("👍", key=f"thumb_up_{i}"):
                        st.success("Thanks for the feedback!")
                with col_fb2:
                    if st.button("👎", key=f"thumb_down_{i}"):
                        st.info("Feedback noted for improvement!")
    else:
        st.info("No queries executed yet. Try the sample queries above!")
    
    # Quick actions
    st.markdown("---")
    st.header("⚡ Quick Actions")
    
    quick_queries = [
        "Show all employees",
        "Total sales this month",
        "Stock items below 10 units",
        "GST registered customers",
        "Recent vouchers"
    ]
    
    for query in quick_queries:
        if st.button(query, key=f"quick_{query}"):
            st.session_state.sample_query = query
            st.rerun()

# Footer with enhanced statistics
st.markdown("---")
col_f1, col_f2, col_f3, col_f4 = st.columns(4)

with col_f1:
    total_queries = len(st.session_state.query_history)
    st.metric("Total Queries", total_queries)

with col_f2:
    successful_queries = sum(1 for q in st.session_state.query_history if q['success'])
    st.metric("Successful Queries", successful_queries)

with col_f3:
    if total_queries > 0:
        success_rate = (successful_queries / total_queries) * 100
        st.metric("Success Rate", f"{success_rate:.1f}%")
    else:
        st.metric("Success Rate", "N/A")

with col_f4:
    if st.session_state.query_history:
        total_records = sum(q.get('result_count', 0) for q in st.session_state.query_history if q['success'])
        st.metric("Records Fetched", total_records)
    else:
        st.metric("Records Fetched", 0)

# Help section
with st.expander("❓ Help & Tips", expanded=False):
    st.markdown("""
    ### 🚀 How to use this Tally ERP Text-to-SQL Agent:
    
    **1. Load the Database:**
    - Click "Load Tally ERP Schema" in the sidebar to initialize the database
    
    **2. Ask Natural Language Questions:**
    - Use business language: "Show me all employees with salary details"
    - Specify time periods: "Sales vouchers from last month"
    - Ask for summaries: "Total stock value by item category"
    
    **3. Sample Query Types:**
    - **Employee queries:** "Show employees in Mumbai", "Payroll summary for John"
    - **Ledger queries:** "Customers with outstanding balance", "GST registered parties"
    - **Stock queries:** "Items with low stock", "Inventory movements this month"
    - **Voucher queries:** "Recent sales transactions", "Purchase vouchers from Supplier B"
    - **Reports:** "Trial balance", "Stock summary", "Payroll report"
    
    **4. Review & Execute:**
    - Check the generated SQL and assumptions
    - Edit if needed before execution
    - Parameters are automatically handled for security
    
    **5. Tally-Specific Features:**
    - Multi-tenant support (user_id and company_name filters)
    - GST compliance queries
    - Standard Tally reports
    - Date range filtering
    - Balance and stock level queries
    
    **⚠️ Important Notes:**
    - All queries are automatically filtered by user and company for data security
    - The system uses parameterized queries to prevent SQL injection
    - Generated SQL can be edited before execution
    - Complex reports may take longer to execute
    """)

# Developer info
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; font-size: 12px;'>
    🔧 Tally ERP Text-to-SQL Agent | Built with Streamlit | 
    Secure • Multi-tenant • Production-ready
    </div>
    """, 
    unsafe_allow_html=True
)
