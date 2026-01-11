import sys
import os
sys.path.insert(0, os.getcwd())
try:
    from backend.agent import AthenaAnalyticsAgent
    from src.utils.database import query_database
    
    # Check date distribution
    print("Checking database content...")
    df = query_database("SELECT MIN(start_time), MAX(start_time), COUNT(*) FROM queries")
    print("\nDate Range:")
    print(df)
    
    # Check specific range user asked about
    print("\nChecking Jan 1-11 2026:")
    df_range = query_database("SELECT COUNT(*) FROM queries WHERE start_time >= '2026-01-01' AND start_time <= '2026-01-11 23:59:59'")
    print(df_range)
    
    # Check query states
    print("\nQuery States:")
    df_states = query_database("SELECT state, COUNT(*) as c FROM queries GROUP BY state")
    print(df_states)

    # Check workgroups for Jan 1-11
    print("\nWorkgroups for Jan 1-11:")
    df_wg = query_database("SELECT workgroup, COUNT(*) as c FROM queries WHERE start_time >= '2026-01-01' AND start_time <= '2026-01-11 23:59:59' GROUP BY workgroup")
    print(df_wg)
    
except Exception as e:
    print(f"Error: {e}")
