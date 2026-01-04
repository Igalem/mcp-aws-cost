"""Tool for querying Athena query execution data from PostgreSQL database and exporting to CSV.

Note: This module also contains helper functions for fetching from AWS Athena API,
which are used by the daily fetch script (scripts/daily_fetch_queries.py).
"""

import boto3
import csv
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Generator, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..utils.database import get_sqlalchemy_engine, query_database, init_database, get_db_connection, calculate_athena_cost
from ..utils.query_parser import extract_primary_database
import psycopg2


def list_query_ids(athena_client, workgroup: str):
    """Generator that yields query execution IDs from a workgroup."""
    next_token = None
    while True:
        kwargs = {"WorkGroup": workgroup, "MaxResults": 50}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = athena_client.list_query_executions(**kwargs)
        for qid in resp.get("QueryExecutionIds", []):
            yield qid
        next_token = resp.get("NextToken")
        if not next_token:
            break


def list_workgroups(athena_client) -> List[str]:
    """List all Athena workgroups."""
    workgroups = []
    next_token = None
    try:
        while True:
            kwargs = {"MaxResults": 50}
            if next_token:
                kwargs["NextToken"] = next_token
            resp = athena_client.list_work_groups(**kwargs)
            for wg in resp.get("WorkGroups", []):
                workgroups.append(wg["Name"])
            next_token = resp.get("NextToken")
            if not next_token:
                break
    except Exception as e:
        # If list_work_groups fails, try common default workgroups
        # Try common workgroup names
        for default_wg in ["primary", "ETLs"]:
            try:
                # Test if workgroup exists by trying to list queries
                list(athena_client.list_query_executions(WorkGroup=default_wg, MaxResults=1).get("QueryExecutionIds", []))
                workgroups.append(default_wg)
            except:
                pass
    
    return workgroups if workgroups else ["primary"]  # Default to primary if listing fails


def _process_single_workgroup(
    workgroup: str,
    start_dt: datetime,
    end_dt: datetime,
    progress_callback: Optional[Callable[[str, int, int], None]] = None
) -> List[Dict[str, Any]]:
    """
    Process a single workgroup and return all matching queries.
    
    Creates its own boto3 client to avoid connection pool exhaustion when running in parallel.
    
    Args:
        workgroup: Workgroup name to process
        start_dt: Start datetime for filtering
        end_dt: End datetime for filtering
        progress_callback: Optional callback function(workgroup, query_ids_count, matched_count)
        
    Returns:
        List of query execution dictionaries matching the date range
    """
    # Create a new client for this thread to avoid connection pool exhaustion
    # Each thread gets its own connection pool (default size: 10 connections)
    athena_client = boto3.client('athena')
    
    all_queries = []
    query_ids_batch = []
    workgroup_query_ids_count = 0
    workgroup_matched_count = 0
    
    try:
        for query_id in list_query_ids(athena_client, workgroup):
            query_ids_batch.append((query_id, workgroup))
            workgroup_query_ids_count += 1
            
            # Process in batches of 50 (AWS limit)
            if len(query_ids_batch) >= 50:
                queries = _get_query_execution_details(athena_client, query_ids_batch, start_dt, end_dt)
                matched_in_batch = len(queries)
                workgroup_matched_count += matched_in_batch
                all_queries.extend(queries)
                query_ids_batch = []
        
        # Process remaining queries
        if query_ids_batch:
            queries = _get_query_execution_details(athena_client, query_ids_batch, start_dt, end_dt)
            matched_in_batch = len(queries)
            workgroup_matched_count += matched_in_batch
            all_queries.extend(queries)
            
    except Exception as e:
        # Log error will be handled by caller
        pass
    finally:
        # Report progress for this workgroup
        if progress_callback and workgroup_query_ids_count > 0:
            progress_callback(workgroup, workgroup_query_ids_count, workgroup_matched_count)
    
    return all_queries


def fetch_query_executions_from_aws(
    athena_client,
    workgroups: List[str],
    start_date: str,
    end_date: str,
    batch_size: int = 1000,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
    max_workers: Optional[int] = None
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Fetch query execution details from AWS Athena API for given workgroups and date range.
    Yields batches of queries as they are fetched, allowing incremental database insertion.
    
    Uses parallel processing to fetch from multiple workgroups simultaneously.
    Each worker thread creates its own boto3 client to avoid connection pool exhaustion.
    
    Args:
        athena_client: Boto3 Athena client (kept for backward compatibility, but not used in parallel mode)
        workgroups: List of workgroup names to fetch from
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        batch_size: Number of queries to yield per batch (default: 1000)
        progress_callback: Optional callback function(workgroup, query_ids_count, matched_count) for progress updates
        max_workers: Maximum number of parallel workers (default: min(32, len(workgroups) + 4))
        
    Yields:
        Batches of query execution dictionaries
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    
    if max_workers is None:
        # Default to reasonable number of workers (AWS API rate limits are per-account, not per-workgroup)
        max_workers = min(32, len(workgroups) + 4)
    
    min_yield_size = min(50, batch_size // 10)  # Yield more frequently - every 50 queries or 10% of batch_size
    batch = []
    
    # Use ThreadPoolExecutor to process workgroups in parallel
    # Each thread creates its own boto3 client to avoid connection pool exhaustion
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all workgroup processing tasks
        # Note: We don't pass athena_client - each thread creates its own
        future_to_workgroup = {
            executor.submit(
                _process_single_workgroup,
                workgroup,
                start_dt,
                end_dt,
                progress_callback
            ): workgroup
            for workgroup in workgroups
        }
        
        # Process results as they complete (yield batches incrementally)
        for future in as_completed(future_to_workgroup):
            workgroup = future_to_workgroup[future]
            try:
                queries = future.result()
                batch.extend(queries)
                
                # Yield batch whenever we have enough queries
                # This ensures data is available quickly even if some workgroups complete faster
                if len(batch) >= min_yield_size:
                    yield batch
                    batch = []
                    
            except Exception as e:
                # Log error (will be handled by daily script's logging)
                # Continue processing other workgroups
                continue
    
    # Yield remaining queries
    if batch:
        yield batch


def _get_query_execution_details(
    athena_client,
    query_ids_batch: List[tuple],
    start_dt: datetime,
    end_dt: datetime
) -> List[Dict[str, Any]]:
    """Get query execution details for a batch of query IDs."""
    queries = []
    
    # Use batch_get_query_execution for efficiency
    query_ids_only = [qid for qid, _ in query_ids_batch]
    workgroup_map = {qid: wg for qid, wg in query_ids_batch}
    
    try:
        resp = athena_client.batch_get_query_execution(QueryExecutionIds=query_ids_only)
        
        for execution in resp.get("QueryExecutions", []):
            status = execution.get("Status", {})
            start_time = status.get("SubmissionDateTime")
            
            if not start_time:
                continue
                
            # Ensure timezone-aware datetime
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            
            # Filter by date range
            if start_time < start_dt or start_time > end_dt:
                continue
            
            query_execution_id = execution.get("QueryExecutionId")
            workgroup = workgroup_map.get(query_execution_id, execution.get("WorkGroup", "primary"))
            
            # Extract status_reason (StateChangeReason)
            status_reason = status.get("StateChangeReason")
            state = status.get("State", "UNKNOWN")
            
            # Extract query statistics
            stats = execution.get("Statistics", {})
            data_scanned_bytes = stats.get("DataScannedInBytes", 0)
            
            # Extract end_time (CompletionDateTime)
            end_time = status.get("CompletionDateTime")
            if end_time and end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            
            # Extract runtime in milliseconds and convert to minutes
            total_execution_time_ms = stats.get("TotalExecutionTimeInMillis")
            runtime_minutes = None
            if total_execution_time_ms is not None:
                runtime_minutes = total_execution_time_ms / 60000.0  # Convert milliseconds to minutes
            
            # Extract engine version
            engine_version = execution.get("EngineVersion", {}).get("SelectedEngineVersion", "AUTO")
            if not engine_version:
                engine_version = "AUTO"
            
            # Extract query text
            query_text = execution.get("Query", "")
            
            queries.append({
                "query_execution_id": query_execution_id,
                "start_time": start_time,
                "end_time": end_time,
                "runtime_minutes": runtime_minutes,
                "state": state,
                "data_scanned_bytes": data_scanned_bytes,
                "engine_version": engine_version,
                "query_text": query_text,
                "status_reason": status_reason,
                "workgroup": workgroup
            })
    
    except Exception as e:
        # Fallback to individual get_query_execution calls
        pass  # batch_get_query_execution failed, will use individual calls
        for query_id, workgroup in query_ids_batch:
            try:
                resp = athena_client.get_query_execution(QueryExecutionId=query_id)
                execution = resp.get("QueryExecution", {})
                status = execution.get("Status", {})
                start_time = status.get("SubmissionDateTime")
                
                if not start_time:
                    continue
                    
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                
                # Filter by date range
                if start_time < start_dt or start_time > end_dt:
                    continue
                
                status_reason = status.get("StateChangeReason")
                state = status.get("State", "UNKNOWN")
                stats = execution.get("Statistics", {})
                data_scanned_bytes = stats.get("DataScannedInBytes", 0)
                
                # Extract end_time (CompletionDateTime)
                end_time = status.get("CompletionDateTime")
                if end_time and end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                
                # Extract runtime in milliseconds and convert to minutes
                total_execution_time_ms = stats.get("TotalExecutionTimeInMillis")
                runtime_minutes = None
                if total_execution_time_ms is not None:
                    runtime_minutes = total_execution_time_ms / 60000.0  # Convert milliseconds to minutes
                
                engine_version = execution.get("EngineVersion", {}).get("SelectedEngineVersion", "AUTO")
                if not engine_version:
                    engine_version = "AUTO"
                query_text = execution.get("Query", "")
                
                queries.append({
                    "query_execution_id": query_id,
                    "start_time": start_time,
                    "end_time": end_time,
                    "runtime_minutes": runtime_minutes,
                    "state": state,
                    "data_scanned_bytes": data_scanned_bytes,
                    "engine_version": engine_version,
                    "query_text": query_text,
                    "status_reason": status_reason,
                    "workgroup": workgroup
                })
            except Exception as e2:
                # Skip queries that fail to fetch
                continue
    
    return queries


def delete_queries_for_date_range(
    start_date: str,
    end_date: str,
    workgroup: Optional[str] = None
) -> int:
    """
    Delete existing queries for a date range from PostgreSQL database.
    Used to rewrite data when rerunning the daily fetch for the same date.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        workgroup: Optional workgroup filter. If None, deletes for all workgroups.
        
    Returns:
        Number of queries deleted
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if workgroup:
            sql = """
                DELETE FROM queries
                WHERE DATE(start_time) BETWEEN %s AND %s
                    AND workgroup = %s
            """
            params = (start_date, end_date, workgroup)
        else:
            sql = """
                DELETE FROM queries
                WHERE DATE(start_time) BETWEEN %s AND %s
            """
            params = (start_date, end_date)
        
        cursor.execute(sql, params)
        deleted_count = cursor.rowcount
        conn.commit()
        return deleted_count
    
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def _strip_null_bytes(value: Any) -> Any:
    """
    Strip null bytes (0x00) from string values.
    PostgreSQL doesn't allow null bytes in string literals.
    
    Args:
        value: Value to clean (string, None, or other type)
        
    Returns:
        Cleaned value with null bytes removed, or original value if not a string
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace('\x00', '')
    return value


def insert_queries_to_database(queries: List[Dict[str, Any]], commit: bool = True) -> int:
    """
    Insert query executions into PostgreSQL database.
    
    Args:
        queries: List of query execution dictionaries
        commit: Whether to commit the transaction (default: True)
               Set to False if you want to batch multiple inserts before committing
        
    Returns:
        Number of queries inserted
    """
    if not queries:
        return 0
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Set lock timeout to prevent long waits (5 seconds)
        cursor.execute("SET lock_timeout = '5s'")
        
        values = []
        for q in queries:
            # Extract database from query_text
            query_text = _strip_null_bytes(str(q.get("query_text", "")))
            database = None
            if query_text:
                database = extract_primary_database(query_text)
                if database:
                    database = _strip_null_bytes(database)
            
            # Calculate cost from data_scanned_bytes
            data_scanned_bytes = int(q["data_scanned_bytes"]) if q["data_scanned_bytes"] else 0
            cost = calculate_athena_cost(data_scanned_bytes)
            
            # Extract end_time and runtime
            end_time = q.get("end_time")
            runtime_minutes = q.get("runtime_minutes")
            
            values.append((
                _strip_null_bytes(str(q["query_execution_id"])),
                q["start_time"],
                end_time,
                runtime_minutes,
                _strip_null_bytes(str(q["state"])),
                data_scanned_bytes,
                _strip_null_bytes(str(q.get("engine_version", "AUTO"))),
                query_text,
                _strip_null_bytes(str(q.get("status_reason"))) if q.get("status_reason") else None,
                _strip_null_bytes(str(q.get("workgroup"))) if q.get("workgroup") else None,
                database,
                cost
            ))
        
        cursor.executemany("""
            INSERT INTO queries (
                query_execution_id, start_time, end_time, runtime, state, 
                data_scanned_bytes, engine_version, query_text, status_reason, workgroup, database, cost
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (query_execution_id) DO UPDATE SET
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                runtime = EXCLUDED.runtime,
                state = EXCLUDED.state,
                data_scanned_bytes = EXCLUDED.data_scanned_bytes,
                engine_version = EXCLUDED.engine_version,
                query_text = EXCLUDED.query_text,
                status_reason = EXCLUDED.status_reason,
                workgroup = EXCLUDED.workgroup,
                database = EXCLUDED.database,
                cost = EXCLUDED.cost
        """, values)
        
        inserted_count = cursor.rowcount
        if commit:
            conn.commit()
        return inserted_count
    
    except (psycopg2.errors.LockNotAvailable, psycopg2.OperationalError) as e:
        # Handle lock timeout and other operational errors
        error_msg = str(e)
        if 'lock' in error_msg.lower() or 'timeout' in error_msg.lower() or isinstance(e, psycopg2.errors.LockNotAvailable):
            conn.rollback()
            raise Exception(f"Lock timeout: Could not acquire lock for batch insert. This may indicate concurrent access. Error: {error_msg}")
        # Re-raise other operational errors
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def fetch_athena_queries(
    workgroup: Optional[str],
    start_date: str,
    end_date: str,
    output_dir: Optional[str] = None,
    batch_size: int = 50,
    query_batch_size: int = 50,
    source_csv: Optional[str] = None
) -> Dict[str, Any]:
    """
    Query Athena query execution data from PostgreSQL database and export to CSV.
    
    This function only queries the PostgreSQL database - it does not fetch from AWS Athena API.
    Use scripts/daily_fetch_queries.py to fetch data from AWS and store in PostgreSQL.
    
    Args:
        workgroup: Athena workgroup name (optional - if None, queries all workgroups)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        output_dir: Output directory for CSV (default: ./reports)
        batch_size: Number of rows to write to CSV per batch (unused)
        query_batch_size: Batch size for batch_get_query_execution (unused)
        source_csv: Deprecated - kept for backward compatibility
        
    Returns:
        Dictionary with:
            - file_path: Path to generated CSV file
            - total_processed: Total queries in database
            - matched_count: Number of queries matching date range
            - success: Boolean indicating success
            - error: Error message if failed
    """
    try:
        # Ensure database is initialized
        try:
            init_database()
        except Exception as e:
            # Database might already exist, continue
            pass
        
        # Create reports directory
        if output_dir is None:
            script_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            output_dir = os.path.join(script_dir, "reports")
        os.makedirs(output_dir, exist_ok=True)
        
        # Build filename with workgroup if specified
        if workgroup:
            filename = os.path.join(output_dir, f"athena_{workgroup}_{start_date}_to_{end_date}.csv")
        else:
            filename = os.path.join(output_dir, f"athena_all_workgroups_{start_date}_to_{end_date}.csv")
        
        # Query database for date range, optionally filtered by workgroup
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
            params = (start_date, end_date, workgroup)
        else:
            # Fetch ALL queries for ALL workgroups (includes queries with workgroup values AND NULL workgroups)
            # This query does not filter by workgroup, so it returns everything matching the date range
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
            params = (start_date, end_date)
        
        # Execute query
        df_filtered = query_database(sql, params=params)
        
        # Get total count in database
        total_df = query_database("SELECT COUNT(*) as total FROM queries")
        total_processed = int(total_df.iloc[0]['total']) if len(total_df) > 0 else 0
        
        # Write filtered results to CSV
        if len(df_filtered) > 0:
            df_filtered.to_csv(filename, index=False)
        else:
            # Create empty CSV with headers
            df_empty = pd.DataFrame(columns=[
                'query_execution_id', 'start_time', 'end_time', 'runtime', 'state', 
                'data_scanned_bytes', 'engine_version', 'query_text', 'status_reason', 'workgroup', 'database', 'cost'
            ])
            df_empty.to_csv(filename, index=False)
        
        return {
            "success": True,
            "file_path": filename,
            "total_processed": total_processed,
            "matched_count": len(df_filtered),
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "file_path": None,
            "total_processed": 0,
            "matched_count": 0
        }

