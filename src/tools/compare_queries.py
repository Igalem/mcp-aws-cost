"""Tool for comparing expensive queries and extracting patterns."""

import pandas as pd
from datetime import date, datetime
from typing import Dict, Any, Optional
from ..utils.query_parser import extract_query_features
from ..utils.database import query_database


def compare_expensive_queries(
    csv_file: Optional[str] = None,
    query_pattern: Optional[str] = None,
    query_id: Optional[str] = None,
    baseline_start: Optional[str] = None,
    baseline_end: Optional[str] = None,
    target_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    workgroup: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare expensive queries and extract patterns.
    
    Can query from PostgreSQL database or read from CSV file.
    If csv_file is provided, it will be used. Otherwise, PostgreSQL will be queried
    using start_date, end_date, and optional workgroup parameters.
    
    Args:
        csv_file: Path to CSV file with query data (optional - if not provided, queries PostgreSQL)
        query_pattern: Optional pattern to filter queries (e.g., table name)
        query_id: Optional specific query execution ID to analyze
        baseline_start: Optional baseline start date for comparison (YYYY-MM-DD)
        baseline_end: Optional baseline end date for comparison (YYYY-MM-DD)
        target_date: Optional target date for comparison (YYYY-MM-DD)
        start_date: Start date for PostgreSQL query (YYYY-MM-DD, required if csv_file not provided)
        end_date: End date for PostgreSQL query (YYYY-MM-DD, required if csv_file not provided)
        workgroup: Optional workgroup filter for PostgreSQL query
        
    Returns:
        Dictionary containing comparison results with:
            - query_details: Details about the query(ies) analyzed
            - statistics: Statistical comparisons
            - patterns: Pattern analysis
            - success: Boolean indicating success
            - error: Error message if failed
    """
    try:
        # Load data from CSV or PostgreSQL
        if csv_file:
            df = pd.read_csv(csv_file)
        else:
            if not start_date or not end_date:
                return {
                    "success": False,
                    "error": "Either csv_file or both start_date and end_date must be provided",
                    "query_details": None,
                    "statistics": None,
                    "patterns": None
                }
            
            # Query from PostgreSQL
            if workgroup:
                sql = """
                    SELECT 
                        query_execution_id,
                        start_time,
                        state,
                        data_scanned_bytes,
                        engine_version,
                        query_text,
                        status_reason,
                        workgroup
                    FROM queries
                    WHERE DATE(start_time) BETWEEN %s AND %s
                        AND workgroup = %s
                    ORDER BY start_time
                """
                params = (start_date, end_date, workgroup)
            else:
                sql = """
                    SELECT 
                        query_execution_id,
                        start_time,
                        state,
                        data_scanned_bytes,
                        engine_version,
                        query_text,
                        status_reason,
                        workgroup
                    FROM queries
                    WHERE DATE(start_time) BETWEEN %s AND %s
                    ORDER BY start_time, workgroup NULLS LAST
                """
                params = (start_date, end_date)
            
            df = query_database(sql, params=params)
        
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['date'] = df['start_time'].dt.date
        
        # Filter for SUCCEEDED queries
        df_succeeded = df[df['state'] == 'SUCCEEDED'].copy()
        df_succeeded['data_scanned_gb'] = df_succeeded['data_scanned_bytes'] / (1024**3)
        
        # Apply filters
        df_filtered = df_succeeded.copy()
        
        if query_pattern:
            df_filtered = df_filtered[
                df_filtered['query_text'].str.contains(query_pattern, case=False, na=False)
            ].copy()
        
        if query_id:
            df_filtered = df_filtered[
                df_filtered['query_execution_id'] == query_id
            ].copy()
        
        if len(df_filtered) == 0:
            return {
                "success": False,
                "error": "No queries found matching the specified criteria",
                "query_details": None,
                "statistics": None,
                "patterns": None
            }
        
        # Extract features for all filtered queries
        df_filtered['features'] = df_filtered['query_text'].apply(extract_query_features)
        df_filtered = df_filtered.sort_values('data_scanned_bytes', ascending=False)
        
        # Get query details
        query_details = []
        if query_id:
            # Specific query analysis
            query_row = df_filtered[df_filtered['query_execution_id'] == query_id]
            if len(query_row) > 0:
                row = query_row.iloc[0]
                query_details.append({
                    "query_id": query_id,
                    "date": str(row['date']),
                    "start_time": str(row['start_time']),
                    "data_scanned_gb": row['data_scanned_gb'],
                    "features": row['features']
                })
        else:
            # Top queries
            top_queries = df_filtered.head(10)
            for idx, row in top_queries.iterrows():
                query_details.append({
                    "query_id": row['query_execution_id'],
                    "date": str(row['date']),
                    "start_time": str(row['start_time']),
                    "data_scanned_gb": row['data_scanned_gb'],
                    "features": row['features']
                })
        
        # Statistics
        statistics = {
            "total_queries": len(df_filtered),
            "total_data_scanned_gb": df_filtered['data_scanned_gb'].sum(),
            "avg_data_scanned_gb": df_filtered['data_scanned_gb'].mean(),
            "median_data_scanned_gb": df_filtered['data_scanned_gb'].median(),
            "max_data_scanned_gb": df_filtered['data_scanned_gb'].max(),
            "min_data_scanned_gb": df_filtered['data_scanned_gb'].min()
        }
        
        # Date-based comparisons
        if baseline_start and baseline_end and target_date:
            baseline_start_date = datetime.strptime(baseline_start, "%Y-%m-%d").date()
            baseline_end_date = datetime.strptime(baseline_end, "%Y-%m-%d").date()
            target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
            
            baseline_queries = df_filtered[
                (df_filtered['date'] >= baseline_start_date) & 
                (df_filtered['date'] <= baseline_end_date)
            ].copy()
            
            target_queries = df_filtered[df_filtered['date'] == target_date_obj].copy()
            
            if len(baseline_queries) > 0 and len(target_queries) > 0:
                statistics["baseline"] = {
                    "start_date": baseline_start,
                    "end_date": baseline_end,
                    "total_queries": len(baseline_queries),
                    "avg_data_scanned_gb": baseline_queries['data_scanned_gb'].mean(),
                    "median_data_scanned_gb": baseline_queries['data_scanned_gb'].median(),
                    "max_data_scanned_gb": baseline_queries['data_scanned_gb'].max(),
                    "min_data_scanned_gb": baseline_queries['data_scanned_gb'].min()
                }
                
                statistics["target_date"] = {
                    "date": target_date,
                    "total_queries": len(target_queries),
                    "avg_data_scanned_gb": target_queries['data_scanned_gb'].mean(),
                    "median_data_scanned_gb": target_queries['data_scanned_gb'].median(),
                    "max_data_scanned_gb": target_queries['data_scanned_gb'].max(),
                    "min_data_scanned_gb": target_queries['data_scanned_gb'].min()
                }
                
                # Calculate changes
                if baseline_queries['data_scanned_gb'].mean() > 0:
                    avg_change_pct = (
                        (target_queries['data_scanned_gb'].mean() - baseline_queries['data_scanned_gb'].mean()) /
                        baseline_queries['data_scanned_gb'].mean() * 100
                    )
                    statistics["change"] = {
                        "avg_data_scanned_pct": avg_change_pct
                    }
        
        # Pattern analysis
        patterns = {}
        
        # Group by source table
        if 'source_table' in df_filtered.columns or any('source_table' in f for f in df_filtered['features']):
            df_filtered['source_table'] = df_filtered['features'].apply(
                lambda x: x.get('source_table', 'unknown') if isinstance(x, dict) else 'unknown'
            )
            source_table_stats = df_filtered.groupby('source_table')['data_scanned_gb'].agg(['count', 'mean', 'sum']).to_dict()
            patterns['by_source_table'] = {
                k: {
                    'count': int(source_table_stats['count'][k]),
                    'mean_gb': float(source_table_stats['mean'][k]),
                    'sum_gb': float(source_table_stats['sum'][k])
                }
                for k in source_table_stats['count'].keys()
            }
        
        # Group by end date
        df_filtered['end_date'] = df_filtered['features'].apply(
            lambda x: x.get('end_date', 'unknown') if isinstance(x, dict) else 'unknown'
        )
        end_date_stats = df_filtered.groupby('end_date')['data_scanned_gb'].agg(['count', 'mean', 'sum']).to_dict()
        patterns['by_end_date'] = {
            k: {
                'count': int(end_date_stats['count'][k]),
                'mean_gb': float(end_date_stats['mean'][k]),
                'sum_gb': float(end_date_stats['sum'][k])
            }
            for k in end_date_stats['count'].keys()
        }
        
        # Date-based pattern comparison
        if baseline_start and baseline_end and target_date:
            baseline_queries = df_filtered[
                (df_filtered['date'] >= baseline_start_date) & 
                (df_filtered['date'] <= baseline_end_date)
            ].copy()
            target_queries = df_filtered[df_filtered['date'] == target_date_obj].copy()
            
            if len(baseline_queries) > 0 and len(target_queries) > 0:
                baseline_queries['source_table'] = baseline_queries['features'].apply(
                    lambda x: x.get('source_table', 'unknown') if isinstance(x, dict) else 'unknown'
                )
                target_queries['source_table'] = target_queries['features'].apply(
                    lambda x: x.get('source_table', 'unknown') if isinstance(x, dict) else 'unknown'
                )
                
                baseline_source = baseline_queries.groupby('source_table')['data_scanned_gb'].agg(['count', 'mean', 'sum'])
                target_source = target_queries.groupby('source_table')['data_scanned_gb'].agg(['count', 'mean', 'sum'])
                
                patterns['baseline_by_source_table'] = {
                    k: {
                        'count': int(baseline_source.loc[k, 'count']),
                        'mean_gb': float(baseline_source.loc[k, 'mean']),
                        'sum_gb': float(baseline_source.loc[k, 'sum'])
                    }
                    for k in baseline_source.index
                }
                
                patterns['target_by_source_table'] = {
                    k: {
                        'count': int(target_source.loc[k, 'count']),
                        'mean_gb': float(target_source.loc[k, 'mean']),
                        'sum_gb': float(target_source.loc[k, 'sum'])
                    }
                    for k in target_source.index
                }
                
                baseline_queries['end_date'] = baseline_queries['features'].apply(
                    lambda x: x.get('end_date', 'unknown') if isinstance(x, dict) else 'unknown'
                )
                target_queries['end_date'] = target_queries['features'].apply(
                    lambda x: x.get('end_date', 'unknown') if isinstance(x, dict) else 'unknown'
                )
                
                baseline_end_date_stats = baseline_queries.groupby('end_date')['data_scanned_gb'].agg(['count', 'mean', 'sum'])
                target_end_date_stats = target_queries.groupby('end_date')['data_scanned_gb'].agg(['count', 'mean', 'sum'])
                
                patterns['baseline_by_end_date'] = {
                    k: {
                        'count': int(baseline_end_date_stats.loc[k, 'count']),
                        'mean_gb': float(baseline_end_date_stats.loc[k, 'mean']),
                        'sum_gb': float(baseline_end_date_stats.loc[k, 'sum'])
                    }
                    for k in baseline_end_date_stats.index
                }
                
                patterns['target_by_end_date'] = {
                    k: {
                        'count': int(target_end_date_stats.loc[k, 'count']),
                        'mean_gb': float(target_end_date_stats.loc[k, 'mean']),
                        'sum_gb': float(target_end_date_stats.loc[k, 'sum'])
                    }
                    for k in target_end_date_stats.index
                }
        
        return {
            "success": True,
            "query_details": query_details,
            "statistics": statistics,
            "patterns": patterns,
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "query_details": None,
            "statistics": None,
            "patterns": None
        }



