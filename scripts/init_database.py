#!/usr/bin/env python3
"""Script to initialize PostgreSQL database and import CSV data."""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.database import init_database, import_csv_to_database
import argparse


def main():
    parser = argparse.ArgumentParser(description="Initialize PostgreSQL database and import CSV data")
    parser.add_argument(
        "--csv",
        type=str,
        help="Path to CSV file to import (optional)",
        default=None
    )
    parser.add_argument(
        "--source-csv",
        type=str,
        help="Path to source CSV file (default: reports/athena_etls_2025-12-10_to_2025-12-16.csv)",
        default=None
    )
    
    args = parser.parse_args()
    
    print("Initializing database...")
    init_database()
    print("✓ Database initialized successfully")
    
    # Determine CSV file to import
    csv_file = args.csv or args.source_csv
    if csv_file is None:
        # Default to the sample report file
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_file = os.path.join(script_dir, "reports", "athena_etls_2025-12-10_to_2025-12-16.csv")
    
    if not os.path.exists(csv_file):
        print(f"⚠ Warning: CSV file not found: {csv_file}")
        print("Database initialized but no data imported.")
        print("You can import data later using:")
        print(f"  python scripts/init_database.py --csv <path_to_csv>")
        return
    
    print(f"\nImporting data from: {csv_file}")
    print("This may take a few minutes for large files...")
    
    result = import_csv_to_database(csv_file)
    
    if result["success"]:
        print(f"✓ Successfully imported {result['imported_rows']:,} rows")
        print(f"  Total rows processed: {result['total_rows']:,}")
    else:
        print(f"✗ Error importing data: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()





