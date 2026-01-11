#!/usr/bin/env python3
"""Script to drop and recreate the queries table with proper column ordering."""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.database import get_db_connection
import psycopg2


def recreate_database():
    """Drop and recreate the queries table with proper column ordering."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("Dropping existing queries table...")
        cursor.execute("DROP TABLE IF EXISTS queries CASCADE")
        conn.commit()
        print("✓ Table dropped successfully")
        
        print("\nCreating new queries table with proper column ordering...")
        # Create table with logical column order:
        # identifiers -> timestamps -> status -> metrics -> metadata -> content -> system
        cursor.execute("""
            CREATE TABLE queries (
                query_execution_id VARCHAR(255) PRIMARY KEY,
                start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                end_time TIMESTAMP WITH TIME ZONE,
                runtime NUMERIC(12, 4),
                state VARCHAR(50) NOT NULL,
                status_reason TEXT,
                data_scanned_bytes BIGINT NOT NULL,
                cost NUMERIC(15, 6),
                workgroup VARCHAR(100),
                database VARCHAR(255),
                engine_version VARCHAR(50),
                query_text TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("✓ Table created successfully")
        
        # Create indexes
        print("\nCreating indexes...")
        indexes = [
            ("idx_start_time", "ON queries (start_time)"),
            ("idx_end_time", "ON queries (end_time)"),
            ("idx_runtime", "ON queries (runtime DESC)"),
            ("idx_state", "ON queries (state)"),
            ("idx_data_scanned_bytes", "ON queries (data_scanned_bytes DESC)"),
            ("idx_cost", "ON queries (cost DESC)"),
            ("idx_workgroup", "ON queries (workgroup)"),
            ("idx_database", "ON queries (database)"),
        ]
        
        for idx_name, idx_def in indexes:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} {idx_def}")
            print(f"  ✓ Created index: {idx_name}")
        
        conn.commit()
        print("\n✓ Database recreated successfully!")
        print("\nColumn order:")
        print("  1. query_execution_id (PRIMARY KEY)")
        print("  2. start_time")
        print("  3. end_time")
        print("  4. runtime")
        print("  5. state")
        print("  6. status_reason")
        print("  7. data_scanned_bytes")
        print("  8. cost")
        print("  9. workgroup")
        print("  10. database")
        print("  11. engine_version")
        print("  12. query_text")
        print("  13. created_at")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("Recreating queries table with proper column ordering")
    print("=" * 60)
    print("\n⚠ WARNING: This will DELETE all existing data!")
    response = input("\nAre you sure you want to continue? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        recreate_database()
    else:
        print("\nOperation cancelled.")
        sys.exit(0)


