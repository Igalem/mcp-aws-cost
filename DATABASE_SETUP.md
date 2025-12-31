# PostgreSQL Database Setup

This project uses PostgreSQL to store and query Athena query execution data for fast performance.

## Prerequisites

1. **PostgreSQL installed and running**
   - macOS: `brew install postgresql@14` or use Postgres.app
   - Linux: `sudo apt-get install postgresql` or `sudo yum install postgresql`
   - Windows: Download from [postgresql.org](https://www.postgresql.org/download/)

2. **Create database** (if not using default):
   ```bash
   createdb athena_queries
   ```

## Quick Setup

### 1. Install Dependencies

```bash
# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# or
# venv\Scripts\activate  # On Windows

# Install PostgreSQL dependencies
pip install -r requirements.txt
```

### 2. Configure Database Connection

Copy the example environment file and update with your database credentials:

```bash
cp .env.example .env
```

Edit `.env` with your PostgreSQL settings:
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=athena_queries
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

### 3. Initialize Database and Import Data

```bash
# Initialize database schema and import CSV data
python scripts/init_database.py

# Or import a specific CSV file:
python scripts/init_database.py --csv /path/to/your/file.csv
```

This will:
- Create the `queries` table with proper schema
- Create indexes for fast date filtering and sorting
- Import data from the CSV file (default: `reports/athena_etls_2025-12-10_to_2025-12-16.csv`)

## Usage

Once the database is set up, the MCP tool will automatically use PostgreSQL instead of CSV files.

### Query Performance

- **Date filtering**: ~0.02-0.1 seconds (with indexes)
- **Aggregations**: ~0.1-0.3 seconds
- **Complex analytics**: ~0.2-0.5 seconds

Much faster than CSV processing!

## Troubleshooting

### Connection Error

If you get a connection error:
```bash
# Check if PostgreSQL is running
pg_isready

# Or check with psql
psql -U postgres -d athena_queries -c "SELECT 1;"
```

### Permission Error

If you get permission errors:
```bash
# Grant permissions (adjust as needed)
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE athena_queries TO postgres;"
```

### Import Takes Too Long

For very large CSV files (>1M rows), the import may take several minutes. This is normal and only needs to be done once.

## Daily Data Collection

The daily process automatically fetches query execution data from AWS Athena API and stores it in PostgreSQL.

### Setup Daily Fetch Process

1. **Test the script manually:**
```bash
# Fetch yesterday's data for all workgroups
python scripts/daily_fetch_queries.py

# Fetch specific date
python scripts/daily_fetch_queries.py --date 2025-12-29

# Fetch for specific workgroup
python scripts/daily_fetch_queries.py --date 2025-12-29 --workgroup ETLs
```

2. **Set up cron job:**
```bash
# Edit crontab
crontab -e

# Add entry (runs daily at 1:00 AM UTC)
# Update paths to match your installation
0 1 * * * /path/to/mcp-aws-cost/venv/bin/python /path/to/mcp-aws-cost/scripts/daily_fetch_queries.py >> /path/to/mcp-aws-cost/logs/daily_fetch.log 2>&1
```

See `scripts/cron.example` for more examples and setup instructions.

### Manual Data Import

To manually import data from CSV file:

```bash
# Import a new CSV file (will append to existing data)
python scripts/init_database.py --csv /path/to/new_data.csv
```

Note: Duplicate `query_execution_id` values will be skipped (primary key constraint).

## Database Schema

The `queries` table structure:
- `query_execution_id` (VARCHAR, PRIMARY KEY)
- `start_time` (TIMESTAMP WITH TIME ZONE, indexed)
- `state` (VARCHAR, indexed)
- `data_scanned_bytes` (BIGINT, indexed)
- `engine_version` (VARCHAR)
- `query_text` (TEXT)
- `status_reason` (TEXT) - Error message for failed queries (from Athena's StateChangeReason)
- `workgroup` (VARCHAR, indexed) - Athena workgroup name
- `created_at` (TIMESTAMP WITH TIME ZONE)

Indexes:
- `idx_start_time_date` - Fast date filtering
- `idx_data_scanned_bytes` - Fast sorting by cost
- `idx_state` - Fast state filtering
- `idx_workgroup` - Fast workgroup filtering

### Status Reason Field

The `status_reason` field contains error messages from failed Athena queries. This field is populated from AWS Athena's `StateChangeReason` field and includes detailed error information such as:
- Syntax errors (e.g., "SYNTAX_ERROR: line 1:8: Column 'invalid_column' cannot be resolved")
- Resource limitations (e.g., "EXCEEDED_MEMORY_LIMIT")
- Access errors (e.g., "Access Denied")
- Internal errors (e.g., "INTERNAL_ERROR_QUERY_ENGINE")
- Data/path errors (e.g., "Table not found: my_database.my_table")

For successful queries (`state = 'SUCCEEDED'`), this field is typically `NULL`.

