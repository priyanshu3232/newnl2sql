import streamlit as st
import json
import sqlite3
import re
from datetime import datetime
import pandas as pd
from typing import Dict, List, Tuple, Optional
import os

from query_parser import NaturalLanguageParser
from sql_generator import SQLGenerator
from query_executor import QueryExecutor
from feedback_manager import FeedbackManager
from schema_manager import SchemaManager

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

st.set_page_config(
    page_title="Text-to-SQL Agent",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Text-to-SQL Agent")
st.markdown("### Convert natural language queries to SQL with interactive confirmation")

with st.sidebar:
    st.header("📊 Database Schema")
    if st.button("Load Sample Schema"):
        st.session_state.schema_manager.load_sample_schema()
        st.success("Sample schema loaded!")
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

col1, col2 = st.columns([2, 1])

with col1:
    st.header("🎯 Enter Your Query")
    user_query = st.text_area(
        "Natural Language Query",
        placeholder="e.g., Show me all customers who made purchases over $100 in the last month",
        height=100
    )
    if st.button("Generate SQL", type="primary"):
        if user_query:
            with st.spinner("Parsing your query..."):
                parsed_result = st.session_state.parser.parse(
                    user_query,
                    st.session_state.schema_manager.get_schema()
                )
                sql_result = st.session_state.sql_generator.generate(
                    parsed_result,
                    st.session_state.schema_manager.get_schema()
                )
                st.session_state.current_sql = sql_result
                st.session_state.current_query = user_query
                st.session_state.current_parsed = parsed_result

    if hasattr(st.session_state, 'current_sql'):
        st.header("🔧 Generated SQL")
        with st.expander("📋 Assumptions Made", expanded=True):
            assumptions = st.session_state.current_sql.get('assumptions', [])
            if assumptions:
                for i, assumption in enumerate(assumptions):
                    st.info(f"{i+1}. {assumption}")
            else:
                st.info("No specific assumptions were made.")
        confidence = st.session_state.current_sql.get('confidence', 0.0)
        st.metric("Confidence Score", f"{confidence:.0%}")
        st.subheader("SQL Query")
        edited_sql = st.text_area(
            "You can edit the SQL if needed:",
            value=st.session_state.current_sql['query'],
            height=150
        )
        col1_1, col1_2, col1_3 = st.columns(3)
        with col1_1:
            if st.button("✅ Execute Query", type="primary"):
                result = st.session_state.executor.execute(
                    edited_sql,
                    st.session_state.schema_manager.get_connection()
                )
                if result['success']:
                    st.success("Query executed successfully!")
                    if result['data']:
                        st.subheader("Query Results")
                        df = pd.DataFrame(result['data'])
                        st.dataframe(df, use_container_width=True)
                        st.session_state.query_history.append({
                            'timestamp': datetime.now().isoformat(),
                            'natural_query': st.session_state.current_query,
                            'sql_query': edited_sql,
                            'success': True,
                            'feedback': 'positive'
                        })
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
                del st.session_state.current_sql
                st.rerun()
        with col1_3:
            if st.button("❌ Cancel"):
                del st.session_state.current_sql
                st.rerun()

with col2:
    st.header("📜 Query History")
    if st.session_state.query_history:
        for i, query in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"Query {len(st.session_state.query_history) - i}"):
                st.text(f"Time: {query['timestamp']}")
                st.text(f"Natural: {query['natural_query']}")
                st.code(query['sql_query'], language='sql')
                if query['success']:
