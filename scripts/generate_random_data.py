#!/usr/bin/env python3
"""Script to generate random data for the queries table."""

import sys
import os
import uuid
import random
import argparse
from datetime import datetime, timedelta
import time

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.database import get_db_connection, calculate_athena_cost

def generate_random_query(start_date, end_date):
    """Generate a single random query record."""
    
    # Random timestamps
    time_range = (end_date - start_date).total_seconds()
    random_seconds = random.randint(0, int(time_range))
    start_time = start_date + timedelta(seconds=random_seconds)
    
    # Runtime: Mixture of distributions for realism
    rand_val = random.random()
    if rand_val < 0.5:
        # Fast queries: 0.1s to 10s
        duration_seconds = random.uniform(0.1, 10.0)
    elif rand_val < 0.8:
        # Medium queries: 10s to 2 mins
        duration_seconds = random.uniform(10.0, 120.0)
    elif rand_val < 0.95:
        # Long queries: 2 mins to 15 mins
        duration_seconds = random.uniform(120.0, 900.0)
    else:
        # Very long/stuck queries: 15 mins to 2 hours
        duration_seconds = random.uniform(900.0, 7200.0)
        
    end_time = start_time + timedelta(seconds=duration_seconds)
    runtime_minutes = duration_seconds / 60.0
    
    # Workgroup
    workgroups = ['primary', 'analytics', 'reporting', 'etl', 'dashboard', 'ad-hoc', 'data-science', 'ml-training', 'marketing-campaigns']
    workgroup = random.choice(workgroups)
    
    # State
    states = ['SUCCEEDED'] * 85 + ['FAILED'] * 10 + ['CANCELLED'] * 5
    state = random.choice(states)
    
    status_reason = None
    if state == 'FAILED':
        reasons = [
            'SYNTAX_ERROR: line 1:15: Column "x" cannot be resolved', 
            'HIVE_CURSOR_ERROR: Row is too large to fit in cursor', 
            'GENERIC_INTERNAL_ERROR: ConnectTimeoutException', 
            'RESOURCE_LIMIT_EXCEEDED: Query exhausted resources',
            'HIVE_PARTITION_SCHEMA_MISMATCH: Partition schema mismatch',
            'ACCESS_DENIED: User is not authorized to read path'
        ]
        status_reason = random.choice(reasons)
    elif state == 'CANCELLED':
        status_reason = 'Query cancelled by user'
    
    # Data scanned & Cost
    if state == 'SUCCEEDED':
        # More variety in data sizes
        rand_size = random.random()
        if rand_size < 0.4:
            # Small: 1KB - 100MB
             data_scanned_bytes = random.randint(1024, 1024 * 1024 * 100)
        elif rand_size < 0.7:
            # Medium: 100MB - 10GB
             data_scanned_bytes = random.randint(1024 * 1024 * 100, 1024 * 1024 * 1024 * 10)
        elif rand_size < 0.9:
            # Large: 10GB - 500GB
             data_scanned_bytes = random.randint(1024 * 1024 * 1024 * 10, 1024 * 1024 * 1024 * 500)
        else:
            # Huge: 500GB - 5TB
             data_scanned_bytes = random.randint(1024 * 1024 * 1024 * 500, 1024 * 1024 * 1024 * 1024 * 5)
    else:
        # Failed queries can still scan data
        if random.random() < 0.5:
            data_scanned_bytes = random.randint(1024, 1024 * 1024 * 100)
        else:
            data_scanned_bytes = 0
        
    cost = calculate_athena_cost(data_scanned_bytes)
    
    # Query Text Generation
    tables = [
        'users_raw', 'users_processed', 'orders_daily', 'orders_historical', 
        'products_catalog', 'clickstream_logs', 'app_impressions', 'transactions_ledger', 
        'system_logs', 'inventory_snapshot', 'customer_profiles', 'ad_campaign_metrics'
    ]
    
    # Complex query patterns
    query_types = [
        "simple_select", "aggregation", "complex_join", "window_function", "cte_query", "insert_into"
    ]
    query_type = random.choice(query_types)
    
    if query_type == "simple_select":
        table = random.choice(tables)
        cols = random.choice(["*", "id, created_at, status", "count(*)", "distinct user_id"])
        query_text = f"SELECT {cols} FROM {table} WHERE dt = '{start_time.strftime('%Y-%m-%d')}'"
        if random.random() < 0.5:
            query_text += f" LIMIT {random.randint(10, 1000)}"
            
    elif query_type == "aggregation":
        table = random.choice(tables)
        agg = random.choice(["count(*)", "sum(amount)", "avg(latency)", "max(price)", "min(creation_date)"])
        group_by = random.choice(["category", "region, country", "date_trunc('hour', timestamp)", "status"])
        query_text = f"SELECT {group_by}, {agg} FROM {table} WHERE dt = '{start_time.strftime('%Y-%m-%d')}' GROUP BY {group_by}"
        if random.random() < 0.3:
            query_text += f" HAVING {agg.split('(')[0]}(...) > 100"
            
    elif query_type == "complex_join":
        t1, t2 = random.sample(tables, 2)
        query_text = f"""
        SELECT t1.id, t1.name, t2.total 
        FROM {t1} t1 
        JOIN {t2} t2 ON t1.id = t2.foreign_id 
        WHERE t1.dt = '{start_time.strftime('%Y-%m-%d')}' 
        AND t2.amount > 100
        """
        
    elif query_type == "window_function":
        table = random.choice(tables)
        query_text = f"""
        SELECT id, category, amount,
        RANK() OVER (PARTITION BY category ORDER BY amount DESC) as rank_val,
        AVG(amount) OVER (PARTITION BY category) as avg_cat
        FROM {table}
        WHERE dt = '{start_time.strftime('%Y-%m-%d')}'
        """
        
    elif query_type == "cte_query":
        t1 = random.choice(tables)
        query_text = f"""
        WITH daily_stats AS (
            SELECT region, sum(views) as total_views
            FROM {t1}
            WHERE dt = '{start_time.strftime('%Y-%m-%d')}'
            GROUP BY region
        )
        SELECT * FROM daily_stats WHERE total_views > 1000
        """
    
    elif query_type == "insert_into":
        target = f"{random.choice(tables)}_aggregated"
        source = random.choice(tables)
        query_text = f"""
        INSERT INTO {target}
        SELECT * FROM {source}
        WHERE dt = '{start_time.strftime('%Y-%m-%d')}'
        """

    # Add comments sometimes
    if random.random() < 0.2:
        query_text = f"-- Query generated by automated report\n{query_text}"

    database = 'analytics_db'
    engine_version = f'Athena engine version {random.choice([2, 3])}'
    
    return (
        str(uuid.uuid4()),
        start_time,
        end_time,
        runtime_minutes,
        state,
        status_reason,
        data_scanned_bytes,
        cost,
        workgroup,
        database,
        engine_version,
        query_text
    )

def generate_data(count, clear=False, days=30):
    """Generate and insert random data."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if clear:
            print("Clearing existing data...")
            cursor.execute("TRUNCATE TABLE queries")
            # Also clear staging if it exists, though typically ephemeral
            cursor.execute("TRUNCATE TABLE queries_staging") 
            conn.commit()
            print("✓ Table cleared.")
            
        print(f"Generating {count} random queries over the last {days} days...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        batch_size = 1000
        total_inserted = 0
        
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            values = []
            
            for _ in range(current_batch_size):
                values.append(generate_random_query(start_date, end_date))
                
            cursor.executemany("""
                INSERT INTO queries (
                    query_execution_id, start_time, end_time, runtime, state, status_reason,
                    data_scanned_bytes, cost, workgroup, database, engine_version, query_text
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)
            
            conn.commit()
            total_inserted += current_batch_size
            print(f"  Generated {total_inserted}/{count} queries")
            
        print(f"\n✓ Successfully generated {count} queries.")
        
    except Exception as e:
        conn.rollback()
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random data for queries table")
    parser.add_argument("--count", type=int, default=10000, help="Number of queries to generate (default: 10000)")
    parser.add_argument("--days", type=int, default=30, help="Number of days to span(default: 30)")
    parser.add_argument("--no-clear", action="store_true", help="Do not clear existing data")
    
    args = parser.parse_args()
    
    generate_data(args.count, clear=not args.no_clear, days=args.days)
