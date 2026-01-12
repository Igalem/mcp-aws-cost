# Cursor MCP Configuration Guide

This guide shows you how to configure the AWS Athena Cost MCP server in Cursor.

## Step 1: Ensure Virtual Environment is Set Up

Make sure you've created and activated the virtual environment:

```bash
cd /Users/igal.emona/mcp-aws-cost
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Step 2: Configure Cursor

Cursor uses an MCP configuration file. Create or edit the configuration file:

**Location:** `~/.cursor/mcp.json` (or `~/.config/cursor/mcp.json` depending on your Cursor version)

### Configuration Option 1: Using run_server.py (Recommended)

```json
{
  "mcpServers": {
    "aws-athena-cost": {
      "command": "/Users/igal.emona/mcp-aws-cost/venv/bin/python",
      "args": [
        "/Users/igal.emona/mcp-aws-cost/run_server.py"
      ],
      "cwd": "/Users/igal.emona/mcp-aws-cost",
      "env": {
        "AWS_DEFAULT_REGION": "us-east-1",
        "PYTHONPATH": "/Users/igal.emona/mcp-aws-cost",
        "POSTGRES_PASSWORD": "postgres"
      }
    }
  }
}
```

### Configuration Option 2: Using Python 3.10 Directly with Module

```json
{
  "mcpServers": {
    "aws-athena-cost": {
      "command": "/opt/homebrew/bin/python3.10",
      "args": [
        "-m",
        "src.server"
      ],
      "cwd": "/Users/igal.emona/mcp-aws-cost",
      "env": {
        "AWS_DEFAULT_REGION": "us-east-1",
        "PYTHONPATH": "/Users/igal.emona/mcp-aws-cost",
        "POSTGRES_PASSWORD": "postgres"
      }
    }
  }
}
```

**Note:** Make sure `PYTHONPATH` includes the project root directory so Python can find the `src` module.

### Configuration Option 3: Using Virtual Environment with Module (Alternative)

If you prefer using `python -m src.server`, make sure PYTHONPATH is set:

```json
{
  "mcpServers": {
    "aws-athena-cost": {
      "command": "/Users/igal.emona/mcp-aws-cost/venv/bin/python",
      "args": [
        "-m",
        "src.server"
      ],
      "cwd": "/Users/igal.emona/mcp-aws-cost",
      "env": {
        "AWS_DEFAULT_REGION": "us-east-1",
        "PYTHONPATH": "/Users/igal.emona/mcp-aws-cost",
        "POSTGRES_PASSWORD": "postgres"
      }
    }
  }
}
```

## Step 3: Configure AWS Credentials

Make sure your AWS credentials are configured. You can do this in one of these ways:

**Option A: AWS CLI**
```bash
aws configure
```

**Option B: Environment Variables in Cursor Config**
Add to the `env` section in your `mcp.json`:
```json
"env": {
  "AWS_DEFAULT_REGION": "us-east-1",
  "AWS_ACCESS_KEY_ID": "your_access_key",
  "AWS_SECRET_ACCESS_KEY": "your_secret_key"
}
```

**Option C: Use IAM Roles** (if running on AWS infrastructure)

## Step 4: Restart Cursor

After saving the configuration file, restart Cursor to load the MCP server.

## Step 5: Verify the Configuration

Once Cursor restarts, you should be able to use the MCP tools. You can test by asking Cursor to:

- "Use the fetch_athena_queries tool to get queries from the ETL workgroup between 2025-12-10 and 2025-12-16"
- "Analyze cost increases using the analyze_cost_increase tool"
- "Compare expensive queries using the compare_expensive_queries tool"

## Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'src'"

This error occurs when Python can't find the `src` module. Solutions:

1. **Use `run_server.py` instead** (recommended):
   - Change the `args` to use `run_server.py` instead of `-m src.server`
   - The `run_server.py` script handles the Python path automatically

2. **Add PYTHONPATH to environment**:
   - Make sure `PYTHONPATH` is set to the project root directory in the `env` section
   - Example: `"PYTHONPATH": "/Users/igal.emona/mcp-aws-cost"`

3. **Verify working directory**:
   - Ensure `cwd` is set to the project root: `/Users/igal.emona/mcp-aws-cost`

### Issue: "MCP server not found" or "Python version error"

- Make sure you're using Python 3.10 or higher
- Verify the path to Python in the configuration is correct
- Check that the virtual environment has all dependencies installed

### Issue: "AWS credentials not found"

- Verify AWS credentials are configured (run `aws configure` or set environment variables)
- Check that the AWS region is set correctly
- Ensure your AWS credentials have the necessary permissions:
  - `athena:ListQueryExecutions`
  - `athena:BatchGetQueryExecution`
  - `athena:GetQueryExecution`

### Issue: "Module not found" errors

- Make sure the `cwd` parameter points to the project root directory
- Verify that `PYTHONPATH` is set correctly if not using a virtual environment
- Check that all dependencies are installed in the virtual environment

### Finding Your Python Path

To find the correct path to Python 3.10:

```bash
which python3.10
# or
which python3.13
```

For the virtual environment:

```bash
ls -la /Users/igal.emona/mcp-aws-cost/venv/bin/python
```

