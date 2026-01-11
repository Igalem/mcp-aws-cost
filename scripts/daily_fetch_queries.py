#!/usr/bin/env python3
"""Daily script to fetch Athena query execution data from AWS and store in PostgreSQL.

This script is designed to run as a cron job to automatically fetch and store
query execution data from AWS Athena API for all workgroups.

Usage:
    python scripts/daily_fetch_queries.py [--date YYYY-MM-DD] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--workgroup WORKGROUP] [--batch-size SIZE] [--max-workers WORKERS]

Examples:
    # Fetch yesterday's data for all workgroups (default)
    python scripts/daily_fetch_queries.py

    # Fetch specific date for all workgroups
    python scripts/daily_fetch_queries.py --date 2025-12-29

    # Fetch date range (from start date to end date)
    python scripts/daily_fetch_queries.py --start-date 2025-12-01 --end-date 2025-12-31

    # Fetch date range from start date to yesterday (if end-date omitted)
    python scripts/daily_fetch_queries.py --start-date 2025-12-01

    # Fetch specific date for specific workgroup
    python scripts/daily_fetch_queries.py --date 2025-12-29 --workgroup ETLs

    # Fetch with custom batch size (inserts every 500 queries)
    python scripts/daily_fetch_queries.py --date 2025-12-29 --batch-size 500

    # Fetch with custom number of parallel workers (for faster processing of multiple workgroups)
    python scripts/daily_fetch_queries.py --start-date 2025-12-01 --max-workers 16
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.tools.fetch_queries import (
    list_workgroups,
    fetch_query_executions_from_aws,
    insert_queries_to_database
)
from src.utils.database import init_database, init_staging_table, merge_staging_to_main, clear_staging_table
import boto3


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def main():
    """Main function to fetch and store Athena queries."""
    parser = argparse.ArgumentParser(
        description='Fetch Athena query execution data from AWS and store in PostgreSQL'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Date to fetch (YYYY-MM-DD format). Defaults to yesterday. Mutually exclusive with --start-date/--end-date.',
        default=None
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for date range (YYYY-MM-DD format). Use with --end-date. If --end-date is omitted, defaults to yesterday.',
        default=None
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for date range (YYYY-MM-DD format). Defaults to yesterday if --start-date is provided.',
        default=None
    )
    parser.add_argument(
        '--workgroup',
        type=str,
        help='Specific workgroup to fetch from. If not provided, fetches from all workgroups.',
        default=None
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Number of queries to insert per batch (default: 1000)',
        default=1000
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        help='Maximum number of parallel workers for processing workgroups (default: auto-detect based on number of workgroups)',
        default=None
    )
    
    args = parser.parse_args()
    logger = setup_logging()
    
    try:
        # Determine date range to fetch
        if args.date:
            if args.start_date or args.end_date:
                logger.error("Cannot use --date with --start-date/--end-date. Use one or the other.")
                sys.exit(1)
            try:
                fetch_start_date = datetime.strptime(args.date, "%Y-%m-%d").date()
                fetch_end_date = fetch_start_date
            except ValueError:
                logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD format.")
                sys.exit(1)
        elif args.start_date:
            try:
                fetch_start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
                if args.end_date:
                    fetch_end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
                else:
                    # Default end date to yesterday if start-date provided
                    fetch_end_date = (datetime.now() - timedelta(days=1)).date()
            except ValueError as e:
                logger.error(f"Invalid date format. Use YYYY-MM-DD format. Error: {e}")
                sys.exit(1)
        else:
            # Default to yesterday
            fetch_start_date = (datetime.now() - timedelta(days=1)).date()
            fetch_end_date = fetch_start_date
        
        start_date_str = fetch_start_date.strftime("%Y-%m-%d")
        end_date_str = fetch_end_date.strftime("%Y-%m-%d")
        
        if fetch_start_date > fetch_end_date:
            logger.error(f"Start date ({start_date_str}) must be <= end date ({end_date_str})")
            sys.exit(1)
        
        if fetch_start_date == fetch_end_date:
            logger.info(f"Starting fetch for date: {start_date_str}")
        else:
            logger.info(f"Starting fetch for date range: {start_date_str} to {end_date_str}")
        
        # Initialize database and staging table
        try:
            init_database()
            init_staging_table()
            logger.info("Database and staging table initialized")
        except Exception as e:
            logger.warning(f"Database initialization warning: {e}")
        
        # Initialize AWS Athena client
        try:
            athena_client = boto3.client('athena')
            logger.info("AWS Athena client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize AWS Athena client: {e}")
            sys.exit(1)
        
        # Determine workgroups to fetch from
        if args.workgroup:
            workgroups_to_fetch = [args.workgroup]
            logger.info(f"Fetching from workgroup: {args.workgroup}")
        else:
            logger.info("Fetching from all workgroups...")
            workgroups_to_fetch = list_workgroups(athena_client)
            logger.info(f"Found {len(workgroups_to_fetch)} workgroups: {', '.join(workgroups_to_fetch[:10])}{'...' if len(workgroups_to_fetch) > 10 else ''}")
        
        # Clear staging table before starting
        logger.info("Clearing staging table before fetch...")
        try:
            cleared_count = clear_staging_table()
            logger.info(f"Cleared {cleared_count} rows from staging table")
        except Exception as e:
            logger.warning(f"Error clearing staging table (may be empty): {e}")
        
        # Progress callback for logging workgroup processing
        def log_progress(workgroup, query_ids_count, matched_count):
            if query_ids_count > 0:
                logger.info(f"Processed workgroup '{workgroup}': {query_ids_count} query IDs checked, {matched_count} matched date filter")
        
        # Fetch query executions from AWS and insert in batches
        if len(workgroups_to_fetch) > 1:
            max_workers_display = args.max_workers if args.max_workers else f"auto (min(32, {len(workgroups_to_fetch)} + 4))"
            logger.info(f"Fetching query executions from AWS Athena using parallel processing ({max_workers_display} workers)...")
        else:
            logger.info("Fetching query executions from AWS Athena...")
        total_fetched = 0
        total_inserted = 0
        batch_number = 0
        
        for batch in fetch_query_executions_from_aws(
            athena_client,
            workgroups_to_fetch,
            start_date_str,
            end_date_str,
            batch_size=args.batch_size,
            progress_callback=log_progress,
            max_workers=args.max_workers
        ):
            batch_number += 1
            total_fetched += len(batch)
            
            logger.info(f"Received batch {batch_number} from generator ({len(batch)} queries)")
            
            if batch:
                logger.info(f"Inserting batch {batch_number} ({len(batch)} queries) into staging table...")
                try:
                    # Insert into staging table instead of main table
                    inserted_count = insert_queries_to_database(batch, commit=True, use_staging=True)
                    total_inserted += inserted_count
                    logger.info(f"Batch {batch_number} inserted into staging: {inserted_count} queries (Total: {total_inserted})")
                except Exception as e:
                    logger.error(f"Error inserting batch {batch_number} into staging: {e}")
                    raise
            else:
                logger.info(f"Received empty batch {batch_number}")
        
        if total_fetched == 0:
            if fetch_start_date == fetch_end_date:
                logger.info("No queries found for the specified date")
            else:
                logger.info(f"No queries found for the specified date range ({start_date_str} to {end_date_str})")
        else:
            # Merge staging table to main table
            logger.info(f"Merging {total_inserted} queries from staging table to main table...")
            try:
                merge_result = merge_staging_to_main()
                if merge_result.get("success"):
                    logger.info(
                        f"Merge completed successfully: "
                        f"{merge_result.get('inserted', 0)} inserted, "
                        f"{merge_result.get('updated', 0)} updated, "
                        f"{merge_result.get('total', 0)} total affected"
                    )
                else:
                    logger.error(f"Merge failed: {merge_result.get('error', 'Unknown error')}")
                    raise Exception(f"Merge failed: {merge_result.get('error', 'Unknown error')}")
            except Exception as e:
                logger.error(f"Error merging staging to main table: {e}")
                raise
            
            # Clear staging table after successful merge
            logger.info("Clearing staging table after successful merge...")
            try:
                cleared_count = clear_staging_table()
                logger.info(f"Cleared {cleared_count} rows from staging table")
            except Exception as e:
                logger.warning(f"Error clearing staging table: {e}")
        
        logger.info(f"Daily fetch completed successfully. Fetched {total_fetched} queries, inserted {total_inserted} queries into staging, merged to main table.")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Error during daily fetch: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

