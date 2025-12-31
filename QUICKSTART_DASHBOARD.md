# Quick Start: AWS Athena Analytics Dashboard

## Prerequisites

- PostgreSQL database with Athena queries data (see main README.md)
- Node.js 18+ and npm
- Python 3.10+ with virtual environment

## Quick Start (3 Steps)

### 1. Install Dependencies

**Backend:**
```bash
source venv/bin/activate
pip install -r requirements.txt
```

**Frontend:**
```bash
cd frontend
npm install
cd ..
```

### 2. Start the Backend

```bash
source venv/bin/activate
python -m backend.main
```

Backend runs on `http://localhost:8000`

### 3. Start the Frontend

In a new terminal:
```bash
cd frontend
npm run dev
```

Frontend runs on `http://localhost:5173`

## Or Use the Startup Script

```bash
./scripts/start_dashboard.sh
```

This will start both backend and frontend automatically.

## Access the Dashboard

Open your browser: `http://localhost:5173`

## Features

- 📊 Real-time dashboard with charts and metrics
- 💬 AI chat interface (basic responses, ready for MCP integration)
- 📈 Query trends and workgroup analysis
- 💰 Cost monitoring and optimization insights

## Troubleshooting

**Backend won't start:**
- Check PostgreSQL is running
- Verify database credentials in environment variables
- Ensure `queries` table exists

**Frontend shows loading forever:**
- Verify backend is running on port 8000
- Check browser console for errors
- Ensure CORS is enabled (should be automatic)

**No data showing:**
- Check database has data in `queries` table
- Verify date range (defaults to last 30 days)
- Check backend logs for errors

## Next Steps

- Integrate MCP server for advanced chat functionality
- Add date range picker for custom time periods
- Add query detail views and drill-downs
- Implement export functionality

