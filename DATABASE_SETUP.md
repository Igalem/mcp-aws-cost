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

The `queries` table structure (columns ordered logically):

### Identifiers
- `query_execution_id` (VARCHAR(255), PRIMARY KEY) - Unique query execution ID from AWS Athena

### Timestamps
- `start_time` (TIMESTAMP WITH TIME ZONE, indexed, NOT NULL) - Query submission time
- `end_time` (TIMESTAMP WITH TIME ZONE, indexed) - Query completion time
- `runtime` (NUMERIC(12, 4), indexed) - Query execution time in minutes

### Status
- `state` (VARCHAR(50), indexed, NOT NULL) - Query state (SUCCEEDED, FAILED, CANCELLED, etc.)
- `status_reason` (TEXT) - Error message for failed queries (from Athena's StateChangeReason)

### Performance Metrics
- `data_scanned_bytes` (BIGINT, indexed, NOT NULL) - Amount of data scanned in bytes
- `cost` (NUMERIC(15, 6), indexed) - Calculated cost in USD (based on $5 per TB scanned)

### Metadata
- `workgroup` (VARCHAR(100), indexed) - Athena workgroup name
- `database` (VARCHAR(255), indexed) - Primary database name extracted from query text
- `engine_version` (VARCHAR(50)) - Athena engine version used

### Content
- `query_text` (TEXT) - The SQL query text

### System
- `created_at` (TIMESTAMP WITH TIME ZONE) - Record creation timestamp (auto-generated)

### Indexes
- `idx_start_time` - Fast date filtering
- `idx_end_time` - Fast completion time filtering
- `idx_runtime` - Fast sorting by execution time
- `idx_state` - Fast state filtering
- `idx_data_scanned_bytes` - Fast sorting by data scanned
- `idx_cost` - Fast sorting by cost
- `idx_workgroup` - Fast workgroup filtering
- `idx_database` - Fast database filtering

### Database Field

The `database` field contains the primary database name extracted from the query text. The extraction looks for `database.table` patterns in common SQL contexts (FROM, INSERT INTO, CREATE TABLE, JOIN, etc.) and returns the first database found. This field is automatically populated when queries are inserted or imported.

### Timestamp Fields

The `end_time` and `runtime` fields provide query execution timing information:

- **end_time**: Query completion timestamp extracted from AWS Athena's `CompletionDateTime` field. This field is `NULL` for queries that haven't completed yet (e.g., RUNNING, QUEUED states).

- **runtime**: Query execution duration in minutes, calculated from AWS Athena's `TotalExecutionTimeInMillis` field. The value is converted from milliseconds to minutes (ms / 60000). This field is `NULL` for queries that don't have execution time data available.

Both fields are automatically populated when queries are fetched from AWS Athena API.

### Cost Field

The `cost` field contains the calculated cost in USD based on AWS Athena pricing:
- **Formula**: `cost = (data_scanned_bytes / 1_000_000_000_000) * 5`
- **Pricing**: $5 per TB scanned
- **Precision**: NUMERIC(15, 6) supports costs up to $999,999,999.999999

The cost is automatically calculated when queries are inserted or imported. For existing rows, cost is calculated during database initialization if the column is newly added.

### Status Reason Field

The `status_reason` field contains error messages from failed Athena queries. This field is populated from AWS Athena's `StateChangeReason` field and includes detailed error information such as:
- Syntax errors (e.g., "SYNTAX_ERROR: line 1:8: Column 'invalid_column' cannot be resolved")
- Resource limitations (e.g., "EXCEEDED_MEMORY_LIMIT")
- Access errors (e.g., "Access Denied")
- Internal errors (e.g., "INTERNAL_ERROR_QUERY_ENGINE")
- Data/path errors (e.g., "Table not found: my_database.my_table")

For successful queries (`state = 'SUCCEEDED'`), this field is typically `NULL`.

