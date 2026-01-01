# Starting All Services

This guide shows you how to start all services for the AWS Athena Cost MCP Server.

## Quick Start

### Option 1: Start All Services (Recommended)

Run the main startup script from the project root:

```bash
cd /Users/igal.emona/mcp-aws-cost
./start_all.sh
```

This will:
- ✅ Start Backend API on `http://localhost:8000`
- ✅ Start Frontend Dashboard on `http://localhost:5173`
- ✅ Check dependencies and install if needed
- ✅ Handle cleanup on exit (Ctrl+C)
- ✅ Log output to `backend.log` and `frontend.log`

### Option 2: Start Dashboard Only

Run the dashboard-specific script:

```bash
cd /Users/igal.emona/mcp-aws-cost
./scripts/start_dashboard.sh
```

### Option 3: Manual Start

**Terminal 1 - Backend:**
```bash
cd /Users/igal.emona/mcp-aws-cost
source venv/bin/activate
python -m backend.main
```

**Terminal 2 - Frontend:**
```bash
cd /Users/igal.emona/mcp-aws-cost/frontend
npm run dev
```

## Access Points

Once services are running:

- **Frontend Dashboard**: http://localhost:5173
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs (Swagger UI)

## Stopping Services

Press `Ctrl+C` in the terminal where you ran the startup script. The script will automatically:
- Stop all background processes
- Clean up PID files
- Kill any remaining processes

## Log Files

- `backend.log` - Backend API logs
- `frontend.log` - Frontend development server logs

## Troubleshooting

### Port Already in Use

If you see warnings about ports being in use:

```bash
# Check what's using port 8000
lsof -i :8000

# Check what's using port 5173
lsof -i :5173

# Kill processes if needed
kill -9 <PID>
```

### Services Not Starting

1. **Check virtual environment:**
   ```bash
   source venv/bin/activate
   python --version  # Should be 3.10+
   ```

2. **Check dependencies:**
   ```bash
   pip install -r requirements.txt
   cd frontend && npm install && cd ..
   ```

3. **Check logs:**
   ```bash
   tail -f backend.log
   tail -f frontend.log
   ```

### Database Connection Issues

Make sure PostgreSQL is running and configured:
- Check `.env` file for database credentials
- Ensure `queries` table exists (run `scripts/init_database.py` if needed)

## Features

The startup script (`start_all.sh`) includes:
- ✅ Automatic dependency checking
- ✅ Port availability checking
- ✅ Service health checking
- ✅ Proper cleanup on exit
- ✅ Colored output for better readability
- ✅ PID tracking for reliable shutdown
- ✅ Log file management


