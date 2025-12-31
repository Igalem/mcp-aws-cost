"""Database utilities for PostgreSQL connection and operations."""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
from typing import Optional, Dict, Any
import pandas as pd

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, skip loading .env file


def get_db_connection_string() -> str:
    """
    Get PostgreSQL connection string from environment variables or defaults.
    
    Environment variables:
        POSTGRES_HOST: Database host (default: localhost)
        POSTGRES_PORT: Database port (default: 5432)
        POSTGRES_DB: Database name (default: athena_queries)
        POSTGRES_USER: Database user (default: current OS user)
        POSTGRES_PASSWORD: Database password (default: empty, uses peer authentication)
    """
    import getpass
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "athena_queries")
    # On macOS with Homebrew PostgreSQL, default user is the current OS user
    user = os.getenv("POSTGRES_USER", getpass.getuser())
    password = os.getenv("POSTGRES_PASSWORD", "")
    
    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    else:
        # No password - use peer authentication (default on macOS Homebrew)
        return f"postgresql://{user}@{host}:{port}/{dbname}"


def get_db_connection():
    """Get a PostgreSQL connection using psycopg2."""
    import getpass
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    dbname = os.getenv("POSTGRES_DB", "athena_queries")
    # On macOS with Homebrew PostgreSQL, default user is the current OS user
    user = os.getenv("POSTGRES_USER", getpass.getuser())
    password = os.getenv("POSTGRES_PASSWORD", "")
    
    # Build connection parameters
    conn_params = {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user
    }
    
    # Only add password if provided (peer authentication if not)
    if password:
        conn_params["password"] = password
    
    return psycopg2.connect(**conn_params)


def get_sqlalchemy_engine():
    """Get a SQLAlchemy engine for pandas operations."""
    connection_string = get_db_connection_string()
    return create_engine(connection_string)


def init_database():
    """Initialize the database schema if it doesn't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS queries (
            query_execution_id VARCHAR(255) PRIMARY KEY,
            start_time TIMESTAMP WITH TIME ZONE NOT NULL,
            state VARCHAR(50) NOT NULL,
            data_scanned_bytes BIGINT NOT NULL,
            engine_version VARCHAR(50),
            query_text TEXT,
            status_reason TEXT,
            workgroup VARCHAR(100),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Add status_reason column if it doesn't exist (for existing databases)
    cursor.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='queries' AND column_name='status_reason'
            ) THEN
                ALTER TABLE queries ADD COLUMN status_reason TEXT;
            END IF;
        END $$;
    """)
    
    # Add workgroup column if it doesn't exist (for existing databases)
    cursor.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='queries' AND column_name='workgroup'
            ) THEN
                ALTER TABLE queries ADD COLUMN workgroup VARCHAR(100);
            END IF;
        END $$;
    """)
    
    # Create indexes for fast queries
    # Note: We can't use DATE() function in index, so we index on start_time directly
    # Queries will use DATE(start_time) in WHERE clause which can still use the index
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_start_time 
        ON queries (start_time)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_data_scanned_bytes 
        ON queries (data_scanned_bytes DESC)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_state 
        ON queries (state)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_workgroup 
        ON queries (workgroup)
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return True


def import_csv_to_database(csv_file: str, table_name: str = "queries", chunk_size: int = 10000) -> Dict[str, Any]:
    """
    Import CSV file into PostgreSQL database.
    
    Args:
        csv_file: Path to CSV file
        table_name: Table name to import into (default: queries)
        chunk_size: Number of rows to process per batch
        
    Returns:
        Dictionary with import statistics
    """
    try:
        engine = get_sqlalchemy_engine()
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Initialize database
        init_database()
        
        total_rows = 0
        imported_rows = 0
        skipped_rows = 0
        
        # Read CSV in chunks and import
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            total_rows += len(chunk)
            
            # Convert start_time to datetime - handle various formats
            chunk['start_time'] = pd.to_datetime(chunk['start_time'], format='mixed', errors='coerce')
            
            # Drop rows with invalid datetime (NaT)
            chunk = chunk.dropna(subset=['start_time'])
            
            # Prepare data for batch insert
            values = []
            for _, row in chunk.iterrows():
                try:
                    # Handle status_reason - check if column exists and has a value
                    status_reason = None
                    if 'status_reason' in row.index and pd.notna(row.get('status_reason')):
                        status_reason_val = row.get('status_reason')
                        if status_reason_val and str(status_reason_val).strip():
                            status_reason = str(status_reason_val)
                    
                    # Handle workgroup - check if column exists and has a value
                    workgroup = None
                    if 'workgroup' in row.index and pd.notna(row.get('workgroup')):
                        workgroup_val = row.get('workgroup')
                        if workgroup_val and str(workgroup_val).strip():
                            workgroup = str(workgroup_val)
                    
                    values.append((
                        str(row['query_execution_id']),
                        row['start_time'],
                        str(row['state']),
                        int(row['data_scanned_bytes']) if pd.notna(row['data_scanned_bytes']) else 0,
                        str(row.get('engine_version', 'AUTO')) if pd.notna(row.get('engine_version')) else 'AUTO',
                        str(row.get('query_text', '')) if pd.notna(row.get('query_text')) else '',
                        status_reason,
                        workgroup
                    ))
                except Exception:
                    skipped_rows += 1
                    continue
            
            if values:
                # Batch insert using executemany
                cursor.executemany("""
                    INSERT INTO queries (
                        query_execution_id, start_time, state, 
                        data_scanned_bytes, engine_version, query_text, status_reason, workgroup
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (query_execution_id) DO UPDATE SET
                        start_time = EXCLUDED.start_time,
                        state = EXCLUDED.state,
                        data_scanned_bytes = EXCLUDED.data_scanned_bytes,
                        engine_version = EXCLUDED.engine_version,
                        query_text = EXCLUDED.query_text,
                        status_reason = EXCLUDED.status_reason,
                        workgroup = EXCLUDED.workgroup
                """, values)
                
                imported_rows += cursor.rowcount
            
            # Commit after each chunk
            conn.commit()
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "total_rows": total_rows,
            "imported_rows": imported_rows,
            "skipped_rows": skipped_rows,
            "error": None
        }
        
    except Exception as e:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        return {
            "success": False,
            "total_rows": total_rows if 'total_rows' in locals() else 0,
            "imported_rows": imported_rows if 'imported_rows' in locals() else 0,
            "skipped_rows": skipped_rows if 'skipped_rows' in locals() else 0,
            "error": str(e)
        }


def query_database(sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """
    Execute a SQL query and return results as pandas DataFrame.
    
    Args:
        sql: SQL query string
        params: Optional parameters for parameterized query
        
    Returns:
        pandas DataFrame with query results
    """
    engine = get_sqlalchemy_engine()
    
    if params:
        return pd.read_sql_query(sql, engine, params=params)
    else:
        return pd.read_sql_query(sql, engine)

