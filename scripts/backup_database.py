#!/usr/bin/env python3
"""Script to backup the queries table to a CSV file."""

import sys
import os
import datetime
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.database import get_sqlalchemy_engine, query_database

def backup_database():
    """Backup the queries table to a CSV file."""
    try:
        # Create backups directory if it doesn't exist
        backup_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backups")
        os.makedirs(backup_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"queries_backup_{timestamp}.csv")
        
        print(f"Backing up 'queries' table...")
        
        # Fetch all data
        df = query_database("SELECT * FROM queries ORDER BY start_time")
        
        if df.empty:
            print("Warning: Table 'queries' is empty. Nothing to backup.")
            return
            
        # Save to CSV
        df.to_csv(backup_file, index=False)
        print(f"✓ Backup successful! Saved {len(df)} rows to:")
        print(f"  {backup_file}")
        
    except Exception as e:
        print(f"✗ Error during backup: {e}")
        sys.exit(1)

if __name__ == "__main__":
    backup_database()
