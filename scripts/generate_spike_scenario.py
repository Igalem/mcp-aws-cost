#!/usr/bin/env python3
"""Script to generate a specific cost spike scenario."""

import sys
import os
import uuid
import random
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.database import get_db_connection, calculate_athena_cost

# Scenario Configuration
TARGET_WORKGROUP = 'reporting'
INCREASE_FACTOR = 2.5 # 250% increase
START_DATE_SPIKE = datetime(2026, 1, 2) # Jan 2nd 2026 (assuming user met "2026-02" as typo or day 2)
END_DATE = datetime.now() # Today
START_DATE_BASELINE = START_DATE_SPIKE - timedelta(days=7) # One week before

def generate_query(timestamp, is_spike=False):
    """Generate a query, potentially with inflated cost if it's a spike."""
    
    workgroups = ['staging', 'analytics', 'reporting', 'etl', 'dashboard', 'ad-hoc', 'data-science']
    workgroup = random.choice(workgroups)
    
    # If we are in the spike period and this is the target workgroup, assume high activity
    # But to simulate "increasing", we simply make this workgroup generate HUGE queries more often
    # OR we make it appear more frequently.
    # The prompt asked for "increasing of scanned data... with like 250%"
    
    # Logic:
    # 1. Base probability of large query
    # 2. If is_spike and workgroup == TARGET_WORKGROUP, multiply prob by 2.5 OR multiply size by 2.5
    
    is_target = (workgroup == TARGET_WORKGROUP)
    
    state = 'SUCCEEDED'
    status_reason = None
    
    # Default size logic from original script
    rand_size = random.random()
    if rand_size < 0.4:
        data_scanned_bytes = random.randint(1024, 1024 * 1024 * 100) # Small
    elif rand_size < 0.7:
        data_scanned_bytes = random.randint(1024 * 1024 * 100, 1024 * 1024 * 1024 * 10) # Medium
    elif rand_size < 0.9:
        data_scanned_bytes = random.randint(1024 * 1024 * 1024 * 10, 1024 * 1024 * 1024 * 500) # Large
    else:
        data_scanned_bytes = random.randint(1024 * 1024 * 1024 * 500, 1024 * 1024 * 1024 * 1024 * 5) # Huge

    # APPLY SPIKE
    if is_spike and is_target:
        # Increase data scanned by factor
        data_scanned_bytes = int(data_scanned_bytes * INCREASE_FACTOR)
        # Also maybe increase frequency? For now just size per query is a simple way to drive cost up.
    
    cost = calculate_athena_cost(data_scanned_bytes)
    
    # Other fields
    duration_seconds = random.uniform(1.0, 60.0)
    start_time = timestamp
    end_time = start_time + timedelta(seconds=duration_seconds)
    runtime = duration_seconds / 60.0
    
    query_text = f"SELECT * FROM {workgroup}_logs WHERE date = '{start_time.date()}'"
    
    return (
        str(uuid.uuid4()),
        start_time,
        end_time,
        runtime,
        state,
        status_reason,
        data_scanned_bytes,
        cost,
        workgroup,
        'analytics_db',
        'Athena engine version 3',
        query_text
    )

def run_simulation():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("Clearing data...")
        cursor.execute("TRUNCATE TABLE queries")
        conn.commit()
        
        # Generate Baseline (Dec 25 - Jan 1)
        current_date = datetime(2025, 12, 25)
        print(f"Generating baseline from {current_date.date()} to {START_DATE_SPIKE.date()}...")
        
        while current_date < START_DATE_SPIKE:
            # Random queries per day
            daily_count = random.randint(300, 800)
            for _ in range(daily_count):
                # Random time in that day
                t = current_date + timedelta(seconds=random.randint(0, 86399))
                q = generate_query(t, is_spike=False)
                cursor.execute("""
                    INSERT INTO queries (
                        query_execution_id, start_time, end_time, runtime, state, status_reason,
                        data_scanned_bytes, cost, workgroup, database, engine_version, query_text
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, q)
            current_date += timedelta(days=1)
            print(f"  Processed {current_date.date()} (Count: {daily_count})")
            
        # Generate Spike (Jan 2 - Today)
        print(f"Generating SPIKE from {START_DATE_SPIKE.date()} to {END_DATE.date()}...")
        
        while current_date <= END_DATE:
            # Random queries per day
            daily_count = random.randint(300, 800)
            for _ in range(daily_count):
                t = current_date + timedelta(seconds=random.randint(0, 86399))
                # Enable spike logic
                q = generate_query(t, is_spike=True)
                cursor.execute("""
                    INSERT INTO queries (
                        query_execution_id, start_time, end_time, runtime, state, status_reason,
                        data_scanned_bytes, cost, workgroup, database, engine_version, query_text
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, q)
            current_date += timedelta(days=1)
            print(f"  Processed {current_date.date()} (SPIKE ACTIVE, Count: {daily_count})")
            
        conn.commit()
        print("Done!")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    run_simulation()
