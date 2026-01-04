"""Query parsing utilities for extracting patterns and features from Athena queries."""

import re
from typing import Dict, Any, Optional

try:
    import pandas as pd
except ImportError:
    pd = None


def extract_query_pattern(query_text: str) -> str:
    """
    Extract a high-level pattern from a query text.
    
    Args:
        query_text: The SQL query text
        
    Returns:
        A string pattern identifier
    """
    if not query_text or (pd is not None and pd.isna(query_text)):
        return "EMPTY"
    
    query_upper = str(query_text).upper()
    
    if 'UNLOAD' in query_upper:
        return "UNLOAD"
    elif 'INSERT' in query_upper or 'CREATE TABLE' in query_upper:
        # Try to identify specific INSERT patterns
        if 'PARQUET__ALL_CRM_USERS' in query_upper:
            return "INSERT: parquet__all_crm_users"
        elif 'PARQUET__HAS_STREAM' in query_upper:
            return "INSERT: parquet__has_stream"
        return "INSERT/CREATE"
    elif 'SELECT' in query_upper and 'FROM' in query_upper:
        match = re.search(r'FROM\s+([a-zA-Z0-9_\.]+)', query_upper)
        if match:
            table = match.group(1)
            return f"SELECT from {table}"
        return "SELECT"
    else:
        return "OTHER"


def extract_query_features(query_text: str) -> Dict[str, Any]:
    """
    Extract detailed features from a query text.
    
    Args:
        query_text: The SQL query text
        
    Returns:
        Dictionary of extracted features
    """
    if not query_text:
        return {}
    
    query_str = str(query_text).upper()
    features = {}
    
    # Extract date ranges
    date_pattern = r"DATE\('(\d{4}-\d{2}-\d{2})'\)"
    dates = re.findall(date_pattern, query_str)
    if dates:
        features['date_range'] = f"{dates[0]} to {dates[-1]}" if len(dates) >= 2 else dates[0]
        features['start_date'] = dates[0]
        features['end_date'] = dates[-1] if len(dates) >= 2 else dates[0]
    
    # Check for CROSS JOIN UNNEST
    if 'CROSS JOIN UNNEST' in query_str:
        features['has_cross_join_unnest'] = True
        # Try to extract publisher count
        if 'SET_PUBLISHERS' in query_str:
            features['uses_set_publishers'] = True
    
    # Check for specific table sources
    if 'DISTINCT_USERS_WITH_PUBLISHERS_DAILY' in query_str:
        features['source_table'] = 'distinct_users_with_publishers_daily'
    elif 'PARQUET_DMP_RAW_V3' in query_str:
        features['source_table'] = 'parquet_dmp_raw_v3'
    
    # Check for country filter
    if "LIKE '%US%'" in query_str or "LIKE 'US%'" in query_str:
        features['country_filter'] = 'US'
    
    # Check for publisher filter type
    if 'LOWER(PUBLISHER) IN' in query_str:
        features['publisher_filter_type'] = 'IN list'
        # Try to count publishers
        match = re.search(r"IN\s*\(([^)]+)\)", query_str)
        if match:
            publishers = match.group(1).split(',')
            features['publisher_count'] = len([p for p in publishers if p.strip()])
    elif 'ARRAY_OF_APPIDS' in query_str or 'SPLIT(' in query_str:
        features['publisher_filter_type'] = 'array/split'
    
    # Check query length
    features['query_length'] = len(query_str)
    
    return features


def extract_primary_database(query_text: str) -> Optional[str]:
    """
    Extract the primary database name from a query text.
    
    Looks for database.table patterns in FROM, JOIN, INSERT INTO, CREATE TABLE, etc.
    Returns the first database found, which is typically the primary database.
    Handles both quoted (backticks) and unquoted identifiers.
    
    Filters out invalid matches like table aliases, single characters, and numbers.
    
    Args:
        query_text: The SQL query text
        
    Returns:
        Primary database name, or None if not found
        
    Examples:
        >>> extract_primary_database("SELECT * FROM db1.table1")
        'db1'
        >>> extract_primary_database("SELECT * FROM `db1`.`table1`")
        'db1'
        >>> extract_primary_database("CREATE TABLE db1.schema1.table1")
        'db1'
    """
    if not query_text or (pd is not None and pd.isna(query_text)):
        return None
    
    query_str = str(query_text)
    
    # Minimum database name length (exclude single chars, numbers, short aliases)
    MIN_DB_LENGTH = 2
    
    # Pattern to match database.table or database.schema.table
    # Handles both quoted (backticks) and unquoted identifiers
    # Matches: db.table, `db`.`table`, db.schema.table, `db`.`schema`.`table`
    # Uses word boundaries for unquoted identifiers
    unquoted_pattern = r'\b([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)(?:\.[a-zA-Z0-9_]+)?'
    quoted_pattern = r'`([^`]+)`\.`([^`]+)`(?:\.`[^`]+`)?'
    
    # Look for database.table patterns in common SQL contexts
    # Priority order: FROM, INSERT INTO, CREATE TABLE, JOIN, UPDATE, DELETE
    # Avoid matching after closing parentheses (likely aliases) or after AS keyword
    contexts = [
        (r'FROM\s+(?!\()', unquoted_pattern),           # FROM db.table (but not FROM (subquery))
        (r'FROM\s+(?!\()', quoted_pattern),             # FROM `db`.`table`
        (r'INSERT\s+INTO\s+', unquoted_pattern),        # INSERT INTO db.table
        (r'INSERT\s+INTO\s+', quoted_pattern),          # INSERT INTO `db`.`table`
        (r'CREATE\s+TABLE\s+', unquoted_pattern),       # CREATE TABLE db.table
        (r'CREATE\s+TABLE\s+', quoted_pattern),         # CREATE TABLE `db`.`table`
        (r'CREATE\s+EXTERNAL\s+TABLE\s+', unquoted_pattern),  # CREATE EXTERNAL TABLE db.table
        (r'CREATE\s+EXTERNAL\s+TABLE\s+', quoted_pattern),     # CREATE EXTERNAL TABLE `db`.`table`
        (r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+', unquoted_pattern),  # JOIN db.table
        (r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+', quoted_pattern),    # JOIN `db`.`table`
        (r'UPDATE\s+', unquoted_pattern),                # UPDATE db.table
        (r'UPDATE\s+', quoted_pattern),                 # UPDATE `db`.`table`
        (r'DELETE\s+FROM\s+', unquoted_pattern),        # DELETE FROM db.table
        (r'DELETE\s+FROM\s+', quoted_pattern),          # DELETE FROM `db`.`table`
    ]
    
    for prefix, pattern in contexts:
        full_pattern = prefix + pattern if prefix else pattern
        match = re.search(full_pattern, query_str, re.IGNORECASE)
        if match:
            database = match.group(1)
            # Remove backticks if present
            database = database.strip('`')
            
            # Skip if database name is too short (likely an alias)
            if len(database) < MIN_DB_LENGTH:
                continue
            
            # Skip if database is purely numeric (likely not a database name)
            if database.isdigit():
                continue
            
            # Skip common SQL keywords that might be matched incorrectly
            sql_keywords = {'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 
                          'FULL', 'OUTER', 'ON', 'GROUP', 'ORDER', 'HAVING', 'INSERT',
                          'INTO', 'UPDATE', 'DELETE', 'CREATE', 'TABLE', 'EXTERNAL',
                          'UNION', 'EXCEPT', 'INTERSECT', 'WITH', 'AS', 'CASE', 'WHEN',
                          'IF', 'NOT', 'EXISTS', 'SET', 'VALUES', 'ALTER', 'DROP',
                          'UNLOAD', 'PARTITION', 'PARTITIONED', 'ROW', 'FORMAT', 'STORED',
                          'LOCATION', 'TBLPROPERTIES'}
            
            if database and database.upper() not in sql_keywords:
                # Additional validation: database name should be a valid identifier
                # Must start with a letter or underscore, not a number
                if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database):
                    return database
    
    return None


def normalize_query(query: str) -> str:
    """
    Normalize a query by removing date-specific parts to identify same query patterns.
    
    Args:
        query: The SQL query text
        
    Returns:
        Normalized query string (first 500 chars)
    """
    if not query:
        return ""
    
    # Remove date-specific parts to identify same query pattern
    normalized = re.sub(r"DATE\('2025-\d{2}-\d{2}'\)", "DATE('YYYY-MM-DD')", query)
    normalized = re.sub(r"'2025'", "'YYYY'", normalized)
    normalized = re.sub(r"'11'", "'MM'", normalized)
    normalized = re.sub(r"'\d+'", "'DD'", normalized)
    return normalized[:500]  # First 500 chars for comparison

