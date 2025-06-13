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

# Page config
st.set_page_config(
    page_title="Text-to-SQL Agent",
    page_icon="🔍",
    layout="wide"
)

# Title and description
st.title("🔍 Text-to-SQL Agent")
st.markdown("### Convert natural language queries to SQL with interactive confirmation")

# Sidebar for schema display
with st.sidebar:
    st.header("📊 Database Schema")
    
    # Load sample schema
    if st.button("Load Sample Schema"):
        st.session_state.schema_manager.load_sample_schema()
        st.success("Sample schema loaded!")
    
    # Display current schema
    schema = st.session_state.schema_manager.get_schema()
    if schema:
        for table_name, table_info in schema.items():
            with st.expander(f"Table: {table_name}"):
                st.markdown("**Columns:**")
                for col in table_info['columns']:
                    st.text(f"• {col['name']} ({col['type']})")
                if table_info.get('relationships'):
                    st.markdown("**Relationships:**")
                    for rel in table_info['relationships']:
                        st.text(f"• {rel}")

# Main content area
col1, col2 = st.columns([2, 1])

with col1:
    # Query input
    st.header("🎯 Enter Your Query")
    user_query = st.text_area(
        "Natural Language Query",
        placeholder="e.g., Show me all customers who made purchases over $100 in the last month",
        height=100
    )
    
    # Query processing
    if st.button("Generate SQL", type="primary"):
        if user_query:
            with st.spinner("Parsing your query..."):
                # Parse the natural language query
                parsed_result = st.session_state.parser.parse(
                    user_query, 
                    st.session_state.schema_manager.get_schema()
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
        st.header("🔧 Generated SQL")
        
        # Show assumptions
        with st.expander("📋 Assumptions Made", expanded=True):
            assumptions = st.session_state.current_sql.get('assumptions', [])
            if assumptions:
                for i, assumption in enumerate(assumptions):
                    st.info(f"{i+1}. {assumption}")
            else:
                st.info("No specific assumptions were made.")
        
        # Show confidence score
        confidence = st.session_state.current_sql.get('confidence', 0.0)
        st.metric("Confidence Score", f"{confidence:.0%}")
        
        # SQL Query display with editing capability
        st.subheader("SQL Query")
        edited_sql = st.text_area(
            "You can edit the SQL if needed:",
            value=st.session_state.current_sql['query'],
            height=150
        )
        
        # Confirmation buttons
        col1_1, col1_2, col1_3 = st.columns(3)
        
        with col1_1:
            if st.button("✅ Execute Query", type="primary"):
                # Execute the query
                result = st.session_state.executor.execute(
                    edited_sql,
                    st.session_state.schema_manager.get_connection()
                )
                
                if result['success']:
                    st.success("Query executed successfully!")
                    
                    # Display results
                    if result['data']:
                        st.subheader("Query Results")
                        df = pd.DataFrame(result['data'])
                        st.dataframe(df, use_container_width=True)
                        
                        # Log successful query
                        st.session_state.query_history.append({
                            'timestamp': datetime.now().isoformat(),
                            'natural_query': st.session_state.current_query,
                            'sql_query': edited_sql,
                            'success': True,
                            'feedback': 'positive'
                        })
                        
                        # Update feedback manager
                        st.session_state.feedback_manager.add_feedback(
                            st.session_state.current_query,
                            edited_sql,
                            'positive'
                        )
                    else:
                        st.info("Query executed but returned no results.")
                else:
                    st.error(f"Error: {result['error']}")
        
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
        for i, query in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"Query {len(st.session_state.query_history) - i}"):
                st.text(f"Time: {query['timestamp']}")
                st.text(f"Natural: {query['natural_query']}")
                st.code(query['sql_query'], language='sql')
                if query['success']:
                    st.success("✓ Successful")
                else:
                    st.error("✗ Failed")
    else:
        st.info("No queries yet")

# Footer with statistics
st.markdown("---")
col_f1, col_f2, col_f3 = st.columns(3)

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
