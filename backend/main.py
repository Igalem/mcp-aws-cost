"""FastAPI backend for AWS Athena Analytics Dashboard."""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to import from src
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.database import query_database, get_db_connection
import psycopg2
import traceback

app = FastAPI(title="AWS Athena Analytics API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],  # Vite default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    message: str
    chat_history: List[Dict[str, str]] = []


class ChatResponse(BaseModel):
    response: str


@app.get("/")
async def root():
    return {"message": "AWS Athena Analytics API"}


@app.get("/api/dashboard/stats")
async def get_dashboard_stats():
    """Get dashboard statistics from PostgreSQL database."""
    try:
        # Get data from last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        # Query database for aggregated stats
        query = """
        SELECT 
            DATE(start_time) as date,
            workgroup,
            COUNT(*) as query_count,
            SUM(data_scanned_bytes) / (1024 * 1024) as scanned_size_mb,
            AVG(EXTRACT(EPOCH FROM (created_at - start_time))) as avg_execution_time
        FROM queries
        WHERE start_time >= %s AND start_time <= %s
        GROUP BY DATE(start_time), workgroup
        ORDER BY date DESC, workgroup
        """
        
        df = query_database(query, params=(start_date, end_date))
        
        # Transform to match frontend format
        queries = []
        if not df.empty:
            for _, row in df.iterrows():
                queries.append({
                    'date': row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date']),
                    'workgroup': row['workgroup'] or 'unknown',
                    'query_count': int(row['query_count']),
                    'scanned_size_mb': float(row['scanned_size_mb']) if row['scanned_size_mb'] is not None else 0,
                    'avg_execution_time': float(row['avg_execution_time']) if row['avg_execution_time'] is not None else 0
                })
        
        return {"queries": queries}
        
    except Exception as e:
        # Return empty data on error (frontend will use sample data)
        print(f"Error fetching dashboard stats: {e}")
        traceback.print_exc()
        return {"queries": []}


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Handle chat requests with MCP integration."""
    try:
        # For now, return a simple response
        # TODO: Integrate with MCP server or Claude API
        message = request.message.lower()
        
        # Simple keyword-based responses (can be replaced with MCP integration)
        if 'expensive' in message or 'cost' in message:
            response = "To find expensive queries, I can help you analyze query patterns. Would you like me to check queries with high data scan volumes?"
        elif 'workgroup' in message and 'data' in message:
            response = "I can analyze workgroup data usage. Let me query the database for workgroup statistics..."
        elif 'trend' in message or 'pattern' in message:
            response = "I can help you identify query trends and patterns. Would you like to see daily query volumes or data scan trends?"
        else:
            response = "I'm here to help you analyze your AWS Athena queries. You can ask me about:\n- Expensive queries\n- Workgroup data usage\n- Query trends and patterns\n- Cost optimization suggestions"
        
        return ChatResponse(response=response)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chat error: {str(e)}")


@app.get("/api/workgroups")
async def get_workgroups():
    """Get list of all workgroups."""
    try:
        query = """
        SELECT DISTINCT workgroup
        FROM queries
        WHERE workgroup IS NOT NULL
        ORDER BY workgroup
        """
        df = query_database(query)
        workgroups = df['workgroup'].tolist()
        return {"workgroups": workgroups}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching workgroups: {str(e)}")


@app.get("/api/queries/expensive")
async def get_expensive_queries(limit: int = 10):
    """Get most expensive queries by data scanned."""
    try:
        query = """
        SELECT 
            query_execution_id,
            start_time,
            workgroup,
            data_scanned_bytes / (1024 * 1024 * 1024) as data_scanned_gb,
            LEFT(query_text, 200) as query_preview
        FROM queries
        WHERE state = 'SUCCEEDED'
        ORDER BY data_scanned_bytes DESC
        LIMIT %s
        """
        df = query_database(query, params=(limit,))
        
        queries = []
        for _, row in df.iterrows():
            queries.append({
                'query_execution_id': row['query_execution_id'],
                'start_time': row['start_time'].isoformat() if hasattr(row['start_time'], 'isoformat') else str(row['start_time']),
                'workgroup': row['workgroup'],
                'data_scanned_gb': float(row['data_scanned_gb']) if row['data_scanned_gb'] else 0,
                'query_preview': row['query_preview'] if row['query_preview'] else ''
            })
        
        return {"queries": queries}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching expensive queries: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

