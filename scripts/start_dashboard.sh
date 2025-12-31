#!/bin/bash
# Start script for AWS Athena Analytics Dashboard

echo "Starting AWS Athena Analytics Dashboard..."
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Error: Virtual environment not found. Please run: python3 -m venv venv"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check if backend dependencies are installed
if ! python -c "import fastapi" 2>/dev/null; then
    echo "Installing backend dependencies..."
    pip install -r requirements.txt
fi

# Start backend in background
echo "Starting backend API server on http://localhost:8000..."
python -m backend.main &
BACKEND_PID=$!

# Wait for backend to start
sleep 3

# Check if frontend node_modules exists
if [ ! -d "frontend/node_modules" ]; then
    echo "Installing frontend dependencies..."
    cd frontend
    npm install
    cd ..
fi

# Start frontend
echo "Starting frontend development server..."
echo "Dashboard will be available at http://localhost:5173"
echo ""
echo "Press Ctrl+C to stop both servers"
cd frontend
npm run dev &
FRONTEND_PID=$!

# Wait for user interrupt
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM
wait

