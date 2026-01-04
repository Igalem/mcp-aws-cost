"""Tool for analyzing cost increases by comparing baseline vs spike periods."""

import pandas as pd
from datetime import date, datetime
from typing import Dict, Any, Optional
from ..utils.query_parser import extract_query_pattern, normalize_query
from ..utils.database import query_database


def analyze_cost_increase(
    csv_file: Optional[str] = None,
    baseline_start: str = None,
    baseline_end: str = None,
    spike_start: str = None,
    spike_end: str = None,
    workgroup: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze cost increases by comparing baseline vs spike periods.
    
    Can query from PostgreSQL database or read from CSV file.
    If csv_file is provided, it will be used. Otherwise, PostgreSQL will be queried
    using baseline_start, baseline_end, spike_start, spike_end, and optional workgroup parameters.
    
    Args:
        csv_file: Path to CSV file with query data (optional - if not provided, queries PostgreSQL)
        baseline_start: Baseline period start date (YYYY-MM-DD, required if csv_file not provided)
        baseline_end: Baseline period end date (YYYY-MM-DD, required if csv_file not provided)
        spike_start: Spike period start date (YYYY-MM-DD, required if csv_file not provided)
        spike_end: Spike period end date (YYYY-MM-DD, required if csv_file not provided)
        workgroup: Optional workgroup filter for PostgreSQL query
        
    Returns:
        Dictionary containing analysis results with:
            - summary: Overall statistics
            - daily_metrics: Daily breakdown
            - period_comparison: Baseline vs spike comparison
            - query_patterns: Pattern analysis
            - top_queries: Most expensive queries
            - new_patterns: New patterns in spike period
            - success: Boolean indicating success
            - error: Error message if failed
    """
    try:
        # Load data from CSV or PostgreSQL
        if csv_file:
            df = pd.read_csv(csv_file)
        else:
            if not baseline_start or not baseline_end or not spike_start or not spike_end:
                return {
                    "success": False,
                    "error": "Either csv_file or all date parameters (baseline_start, baseline_end, spike_start, spike_end) must be provided",
                    "summary": None,
                    "daily_metrics": None,
                    "period_comparison": None,
                    "query_patterns": None,
                    "top_queries": None,
                    "new_patterns": None,
                    "insert_analysis": None,
                    "query_changes": None
                }
            
            # Determine date range to query (from earliest baseline to latest spike)
            all_dates = [baseline_start, baseline_end, spike_start, spike_end]
            query_start = min(all_dates)
            query_end = max(all_dates)
            
            # Query from PostgreSQL
            if workgroup:
                sql = """
                    SELECT 
                        query_execution_id,
                        start_time,
                        end_time,
                        runtime,
                        state,
                        data_scanned_bytes,
                        engine_version,
                        query_text,
                        status_reason,
                        workgroup,
                        database,
                        cost
                    FROM queries
                    WHERE DATE(start_time) BETWEEN %s AND %s
                        AND workgroup = %s
                    ORDER BY start_time
                """
                params = (query_start, query_end, workgroup)
            else:
                sql = """
                    SELECT 
                        query_execution_id,
                        start_time,
                        end_time,
                        runtime,
                        state,
                        data_scanned_bytes,
                        engine_version,
                        query_text,
                        status_reason,
                        workgroup,
                        database,
                        cost
                    FROM queries
                    WHERE DATE(start_time) BETWEEN %s AND %s
                    ORDER BY start_time, workgroup NULLS LAST
                """
                params = (query_start, query_end)
            
            df = query_database(sql, params=params)
        
        # Convert start_time to datetime
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['date'] = df['start_time'].dt.date
        df['hour'] = df['start_time'].dt.hour
        
        # Filter only SUCCEEDED queries for accurate cost analysis
        df_succeeded = df[df['state'] == 'SUCCEEDED'].copy()
        
        total_queries = len(df)
        succeeded_queries = len(df_succeeded)
        failed_queries = total_queries - succeeded_queries
        
        # Parse dates
        baseline_start_date = datetime.strptime(baseline_start, "%Y-%m-%d").date()
        baseline_end_date = datetime.strptime(baseline_end, "%Y-%m-%d").date()
        spike_start_date = datetime.strptime(spike_start, "%Y-%m-%d").date()
        spike_end_date = datetime.strptime(spike_end, "%Y-%m-%d").date()
        
        df_baseline = df_succeeded[
            (df_succeeded['date'] >= baseline_start_date) & 
            (df_succeeded['date'] <= baseline_end_date)
        ].copy()
        df_spike = df_succeeded[
            (df_succeeded['date'] >= spike_start_date) & 
            (df_succeeded['date'] <= spike_end_date)
        ].copy()
        
        # Calculate daily metrics
        daily_stats = df_succeeded.groupby('date').agg({
            'data_scanned_bytes': ['sum', 'mean', 'count', 'max'],
            'query_execution_id': 'count'
        }).reset_index()
        
        daily_stats.columns = ['date', 'total_bytes_scanned', 'avg_bytes_per_query', 'query_count', 'max_bytes_single_query', 'total_queries']
        daily_stats['total_gb_scanned'] = daily_stats['total_bytes_scanned'] / (1024**3)
        daily_stats['avg_gb_per_query'] = daily_stats['avg_bytes_per_query'] / (1024**3)
        daily_stats['max_gb_single_query'] = daily_stats['max_bytes_single_query'] / (1024**3)
        
        # Period comparison
        baseline_total_gb = df_baseline['data_scanned_bytes'].sum() / (1024**3)
        baseline_avg_gb = df_baseline['data_scanned_bytes'].mean() / (1024**3)
        baseline_query_count = len(df_baseline)
        baseline_days = (baseline_end_date - baseline_start_date).days + 1
        baseline_daily_avg = baseline_total_gb / baseline_days if baseline_days > 0 else 0
        
        spike_total_gb = df_spike['data_scanned_bytes'].sum() / (1024**3)
        spike_avg_gb = df_spike['data_scanned_bytes'].mean() / (1024**3)
        spike_query_count = len(df_spike)
        spike_days = (spike_end_date - spike_start_date).days + 1
        spike_daily_avg = spike_total_gb / spike_days if spike_days > 0 else 0
        
        daily_change_pct = ((spike_daily_avg - baseline_daily_avg) / baseline_daily_avg * 100) if baseline_daily_avg > 0 else 0
        avg_query_change_pct = ((spike_avg_gb - baseline_avg_gb) / baseline_avg_gb * 100) if baseline_avg_gb > 0 else 0
        query_count_change_pct = ((spike_query_count / spike_days - baseline_query_count / baseline_days) / (baseline_query_count / baseline_days) * 100) if baseline_query_count > 0 and baseline_days > 0 else 0
        
        # Extract query patterns
        df_baseline['query_pattern'] = df_baseline['query_text'].apply(extract_query_pattern)
        df_spike['query_pattern'] = df_spike['query_text'].apply(extract_query_pattern)
        
        # Compare query patterns
        baseline_patterns = df_baseline.groupby('query_pattern').agg({
            'data_scanned_bytes': ['sum', 'mean', 'count']
        }).reset_index()
        baseline_patterns.columns = ['pattern', 'total_bytes', 'avg_bytes', 'count']
        baseline_patterns['total_gb'] = baseline_patterns['total_bytes'] / (1024**3)
        baseline_patterns['avg_gb'] = baseline_patterns['avg_bytes'] / (1024**3)
        baseline_patterns = baseline_patterns.sort_values('total_gb', ascending=False)
        
        spike_patterns = df_spike.groupby('query_pattern').agg({
            'data_scanned_bytes': ['sum', 'mean', 'count']
        }).reset_index()
        spike_patterns.columns = ['pattern', 'total_bytes', 'avg_bytes', 'count']
        spike_patterns['total_gb'] = spike_patterns['total_bytes'] / (1024**3)
        spike_patterns['avg_gb'] = spike_patterns['avg_bytes'] / (1024**3)
        spike_patterns = spike_patterns.sort_values('total_gb', ascending=False)
        
        # Merge for comparison
        pattern_comparison = pd.merge(
            baseline_patterns[['pattern', 'total_gb', 'avg_gb', 'count']],
            spike_patterns[['pattern', 'total_gb', 'avg_gb', 'count']],
            on='pattern',
            how='outer',
            suffixes=('_baseline', '_spike')
        ).fillna(0)
        
        pattern_comparison['gb_change'] = pattern_comparison['total_gb_spike'] - pattern_comparison['total_gb_baseline']
        pattern_comparison['gb_change_pct'] = (
            (pattern_comparison['total_gb_spike'] - pattern_comparison['total_gb_baseline']) / 
            pattern_comparison['total_gb_baseline'].replace(0, 1) * 100
        )
        pattern_comparison = pattern_comparison.sort_values('total_gb_spike', ascending=False)
        
        # Find new patterns in spike period
        new_patterns = set(df_spike['query_pattern'].unique()) - set(df_baseline['query_pattern'].unique())
        new_patterns_data = []
        if new_patterns:
            for pattern in new_patterns:
                pattern_data = df_spike[df_spike['query_pattern'] == pattern]
                total_gb = pattern_data['data_scanned_bytes'].sum() / (1024**3)
                count = len(pattern_data)
                new_patterns_data.append({
                    'pattern': pattern,
                    'total_gb': total_gb,
                    'count': count
                })
        
        # Analyze INSERT queries specifically
        insert_baseline = df_baseline[df_baseline['query_pattern'].str.contains('INSERT', na=False)]
        insert_spike = df_spike[df_spike['query_pattern'].str.contains('INSERT', na=False)]
        
        insert_analysis = {}
        if len(insert_baseline) > 0 and len(insert_spike) > 0:
            baseline_insert_gb = insert_baseline['data_scanned_bytes'].sum() / (1024**3)
            spike_insert_gb = insert_spike['data_scanned_bytes'].sum() / (1024**3)
            baseline_insert_avg = insert_baseline['data_scanned_bytes'].mean() / (1024**3)
            spike_insert_avg = insert_spike['data_scanned_bytes'].mean() / (1024**3)
            insert_daily_change = ((spike_insert_gb / spike_days - baseline_insert_gb / baseline_days) / (baseline_insert_gb / baseline_days) * 100) if baseline_insert_gb > 0 and baseline_days > 0 else 0
            
            insert_analysis = {
                'baseline_total_gb': baseline_insert_gb,
                'baseline_avg_gb': baseline_insert_avg,
                'baseline_count': len(insert_baseline),
                'spike_total_gb': spike_insert_gb,
                'spike_avg_gb': spike_insert_avg,
                'spike_count': len(insert_spike),
                'daily_change_pct': insert_daily_change
            }
        
        # Top expensive queries
        top_baseline = df_baseline.nlargest(10, 'data_scanned_bytes')[
            ['date', 'data_scanned_bytes', 'query_pattern', 'query_execution_id']
        ].copy()
        top_baseline['gb'] = top_baseline['data_scanned_bytes'] / (1024**3)
        
        top_spike = df_spike.nlargest(10, 'data_scanned_bytes')[
            ['date', 'data_scanned_bytes', 'query_pattern', 'query_execution_id']
        ].copy()
        top_spike['gb'] = top_spike['data_scanned_bytes'] / (1024**3)
        
        # Query execution frequency analysis
        df_baseline['query_normalized'] = df_baseline['query_text'].apply(normalize_query)
        df_spike['query_normalized'] = df_spike['query_text'].apply(normalize_query)
        
        baseline_query_types = df_baseline['query_normalized'].value_counts()
        spike_query_types = df_spike['query_normalized'].value_counts()
        
        # Find queries that increased significantly
        common_queries = set(baseline_query_types.index) & set(spike_query_types.index)
        query_changes = []
        if common_queries:
            for q in common_queries:
                baseline_count = baseline_query_types.get(q, 0)
                spike_count = spike_query_types.get(q, 0)
                baseline_avg_gb = df_baseline[df_baseline['query_normalized'] == q]['data_scanned_bytes'].mean() / (1024**3)
                spike_avg_gb = df_spike[df_spike['query_normalized'] == q]['data_scanned_bytes'].mean() / (1024**3)
                
                if baseline_avg_gb > 0:
                    change_pct = ((spike_avg_gb - baseline_avg_gb) / baseline_avg_gb * 100)
                    if abs(change_pct) > 10:  # Significant change
                        query_changes.append({
                            'query_preview': q[:100],
                            'baseline_avg_gb': baseline_avg_gb,
                            'spike_avg_gb': spike_avg_gb,
                            'change_pct': change_pct,
                            'baseline_count': baseline_count,
                            'spike_count': spike_count
                        })
        
        return {
            "success": True,
            "summary": {
                "total_queries": total_queries,
                "succeeded_queries": succeeded_queries,
                "failed_queries": failed_queries,
                "baseline_queries": baseline_query_count,
                "spike_queries": spike_query_count
            },
            "daily_metrics": daily_stats.to_dict('records'),
            "period_comparison": {
                "baseline": {
                    "start_date": baseline_start,
                    "end_date": baseline_end,
                    "days": baseline_days,
                    "total_gb": baseline_total_gb,
                    "daily_avg_gb": baseline_daily_avg,
                    "query_count": baseline_query_count,
                    "avg_gb_per_query": baseline_avg_gb
                },
                "spike": {
                    "start_date": spike_start,
                    "end_date": spike_end,
                    "days": spike_days,
                    "total_gb": spike_total_gb,
                    "daily_avg_gb": spike_daily_avg,
                    "query_count": spike_query_count,
                    "avg_gb_per_query": spike_avg_gb
                },
                "changes": {
                    "daily_data_scanned_pct": daily_change_pct,
                    "avg_per_query_pct": avg_query_change_pct,
                    "query_count_pct": query_count_change_pct
                }
            },
            "query_patterns": pattern_comparison.head(15).to_dict('records'),
            "top_queries": {
                "baseline": top_baseline.to_dict('records'),
                "spike": top_spike.to_dict('records')
            },
            "new_patterns": new_patterns_data,
            "insert_analysis": insert_analysis,
            "query_changes": query_changes,
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "summary": None,
            "daily_metrics": None,
            "period_comparison": None,
            "query_patterns": None,
            "top_queries": None,
            "new_patterns": None,
            "insert_analysis": None,
            "query_changes": None
        }



