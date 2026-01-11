# AWS Athena Analytics Dashboard

A modern React dashboard for visualizing and analyzing AWS Athena query performance and costs.

## Features

- 📊 **Dashboard View**
  - Key Metrics Cards: Total queries, data scanned (GB), average execution time, and workgroup count
  - Query Trend Chart: 14-day line chart showing query volume over time
  - Data Scanned Chart: Bar chart showing daily data consumption
  - Workgroup Distribution: Pie chart showing query distribution across workgroups
  - Workgroup Usage: Horizontal bar chart for data usage by workgroup

- 💬 **AI Chat Interface**
  - Floating chat window for querying your Athena data
  - Integration with MCP server for intelligent analysis
  - Examples: "Show me the most expensive queries" or "Which workgroup uses the most data?"
  - Real-time AI assistance for cost optimization and query analysis

- 🎨 **Design**
  - Modern dark theme with glassmorphism effects
  - Responsive layout that works on desktop and mobile
  - Smooth animations and transitions
  - Color-coded visualizations for easy interpretation

## Architecture

```
┌─────────────────────┐
│   React Frontend    │
│   (Vite + React)    │
└──────────┬──────────┘
           │ HTTP API
           ▼
┌─────────────────────┐
│   FastAPI Backend   │
│   (Python)          │
└──────────┬──────────┘
           │ SQL Queries
           ▼
┌─────────────────────┐
│  PostgreSQL Database│
│   (queries table)   │
└─────────────────────┘
```

## Prerequisites

- Node.js 18+ and npm
- Python 3.10+
- PostgreSQL database with Athena queries data (see main README.md for setup)

## Installation

### Frontend Setup

```bash
cd frontend
npm install
```

### Backend Setup

The backend uses the same Python environment as the MCP server:

```bash
# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# or
# venv\Scripts\activate  # On Windows

# Install dependencies (if not already installed)
pip install -r requirements.txt
```

## Running the Application

### 1. Start the Backend API

```bash
# Activate virtual environment
source venv/bin/activate

# Run the FastAPI server
python -m backend.main
# or
python backend/run_server.py
```

The backend will start on `http://localhost:8000`

### 2. Start the Frontend

In a new terminal:

```bash
cd frontend
npm run dev
```

The frontend will start on `http://localhost:5173` (Vite default port)

### 3. Access the Dashboard

Open your browser and navigate to `http://localhost:5173`

## Configuration

### Backend Configuration

The backend uses the same database configuration as the MCP server. Set these environment variables:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=athena_queries
export POSTGRES_USER=your_username
export POSTGRES_PASSWORD=your_password  # Optional, uses peer auth if not set
```

Or create a `.env` file in the project root:

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=athena_queries
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

### Frontend Configuration

The frontend is configured to connect to `http://localhost:8000` by default. To change this, edit `src/components/AthenaQueryDashboard.jsx` and update the API URLs.

## API Endpoints

### `GET /api/dashboard/stats`
Returns aggregated query statistics for the last 30 days.

**Response:**
```json
{
  "queries": [
    {
      "date": "2025-12-01",
      "workgroup": "analytics",
      "query_count": 150,
      "scanned_size_mb": 5000.5,
      "avg_execution_time": 12.3
    }
  ]
}
```

### `POST /api/chat`
Handle chat messages (currently returns simple responses, can be extended with MCP integration).

**Request:**
```json
{
  "message": "Show me the most expensive queries",
  "chat_history": []
}
```

**Response:**
```json
{
  "response": "To find expensive queries, I can help you analyze query patterns..."
}
```

### `GET /api/workgroups`
Get list of all workgroups in the database.

### `GET /api/queries/expensive?limit=10`
Get the most expensive queries by data scanned.

## Development

### Frontend Development

```bash
cd frontend
npm run dev      # Start dev server with hot reload
npm run build    # Build for production
npm run preview  # Preview production build
```

### Backend Development

The FastAPI backend supports hot reload when run with `uvicorn --reload`:

```bash
python -m backend.main
```

## MCP Integration

The chat interface is designed to integrate with your MCP server. To enable full MCP integration:

1. Update `backend/main.py` to connect to your MCP server
2. Use MCP tools like `fetch_athena_queries`, `analyze_cost_increase`, and `compare_expensive_queries`
3. Process chat messages and call appropriate MCP tools based on user queries

Example integration:

```python
# In backend/main.py chat endpoint
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def chat_with_mcp(message: str):
    # Connect to MCP server
    # Call appropriate tools based on message
    # Return formatted response
    pass
```

## Troubleshooting

### Backend can't connect to database
- Verify PostgreSQL is running
- Check environment variables are set correctly
- Ensure database exists and has the `queries` table

### Frontend shows "Loading..." indefinitely
- Check backend is running on port 8000
- Check browser console for CORS errors
- Verify API endpoint URLs in the component

### Charts not displaying
- Check browser console for errors
- Verify data format matches expected structure
- Ensure Recharts is properly installed

## License

[Same as main project]




