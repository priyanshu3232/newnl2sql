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
from llm_judge import GroqLLMJudge  # New import

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

# Initialize LLM Judge (only if API key is available)
if 'llm_judge' not in st.session_state:
    try:
        # Try to initialize LLM Judge with API key
        groq_api_key = os.getenv('GROQ_API_KEY') or st.secrets.get('GROQ_API_KEY', None)
        if groq_api_key:
            st.session_state.llm_judge = GroqLLMJudge(api_key=groq_api_key)
            st.session_state.llm_enabled = True
        else:
            st.session_state.llm_judge = None
            st.session_state.llm_enabled = False
    except Exception as e:
        st.session_state.llm_judge = None
        st.session_state.llm_enabled = False

# Page config
st.set_page_config(
    page_title="Tally ERP Text-to-SQL Agent with AI Judge",
    page_icon="💼",
    layout="wide"
)

# Title and description
st.title("💼 Tally ERP Text-to-SQL Agent with AI Judge")
st.markdown("### Convert natural language queries to SQL for Tally ERP database with LLM-powered quality assessment")

# Sidebar with API Key configuration
with st.sidebar:
    st.header("🔧 Configuration")
    
    # API Key input
    api_key_input = st.text_input(
        "Groq API Key", 
        type="password",
        value="",
        help="Enter your Groq API key to enable LLM judge functionality"
    )
    
    if api_key_input and not st.session_state.llm_enabled:
        try:
            st.session_state.llm_judge = GroqLLMJudge(api_key=api_key_input)
            st.session_state.llm_enabled = True
            st.success("✅ LLM Judge enabled!")
        except Exception as e:
            st.error(f"❌ Failed to initialize LLM Judge: {str(e)}")
    
    # LLM Status
    if st.session_state.llm_enabled:
        st.success("🤖 AI Judge: ACTIVE")
        
        # Show LLM statistics
        if st.button("📊 View Learning Report"):
            if st.session_state.llm_judge:
                report = st.session_state.llm_judge.generate_learning_report()
                with st.expander("📈 Learning Report", expanded=True):
                    if 'message' in report:
                        st.info(report['message'])
                    else:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Total Evaluations", report.get('total_evaluations', 0))
                            st.metric("Success Rate", f"{report.get('success_rate', 0):.1%}")
                        with col2:
                            st.metric("Avg LLM Score", f"{report.get('average_llm_score', 0):.2f}")
                            
                        trends = report.get('improvement_trends', {})
                        if 'trend' in trends:
                            trend_emoji = "📈" if trends['trend'] == 'improving' else "📉" if trends['trend'] == 'declining' else "➡️"
                            st.info(f"{trend_emoji} Trend: {trends['trend'].title()}")
    else:
        st.warning("🤖 AI Judge: DISABLED")
        st.info("💡 Add your Groq API key above to enable AI-powered query evaluation")
    
    st.markdown("---")
    
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
        
        # Schema browser (keeping original implementation)
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
    # Sample queries section (keeping original implementation)
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
    col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)  # Added one more column
    
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
