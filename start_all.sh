#!/bin/bash
# Startup script for AWS Athena Cost MCP Server - All Services
# Starts backend API, frontend dashboard, and optionally MCP server

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Define Model Name (default to llama3.1)
MODEL_NAME=${LLM_MODEL_NAME:-llama3.1}

# PID file to track processes
PID_FILE="$SCRIPT_DIR/.start_all.pids"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Shutting down services...${NC}"
    
    if [ -f "$PID_FILE" ]; then
        while read pid; do
            if ps -p $pid > /dev/null 2>&1; then
                echo "Stopping process $pid..."
                kill $pid 2>/dev/null || true
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    
    # Also kill any processes we might have missed
    pkill -f "backend.main" 2>/dev/null || true
    pkill -f "npm run dev" 2>/dev/null || true
    pkill -f "src.server" 2>/dev/null || true
    pkill -f "ollama serve" 2>/dev/null || true
    
    echo -e "${GREEN}All services stopped.${NC}"
    exit 0
}

# Set up trap to cleanup on exit
trap cleanup INT TERM EXIT

# Function to check if port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        return 0
    else
        return 1
    fi
}

# Function to wait for service to be ready
wait_for_service() {
    local url=$1
    local max_attempts=30
    local attempt=0
    
    echo -n "Waiting for service at $url"
    while [ $attempt -lt $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done
    echo -e " ${RED}✗${NC}"
    return 1
}

# Initialize PID file
> "$PID_FILE"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  AWS Athena Cost - Starting Services${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${RED}Error: Virtual environment not found.${NC}"
    echo "Please run: python3.10 -m venv venv"
    exit 1
fi

# Activate virtual environment
echo -e "${GREEN}Activating virtual environment...${NC}"
source venv/bin/activate

# Check backend dependencies
echo -e "${YELLOW}Checking dependencies...${NC}"
pip install -r requirements.txt > /dev/null 2>&1

# Check ports
if check_port 8000; then
    echo -e "${YELLOW}Warning: Port 8000 is already in use. Backend may not start.${NC}"
fi

if check_port 5173; then
    echo -e "${YELLOW}Warning: Port 5173 is already in use. Frontend may not start.${NC}"
fi

# Start Backend API
echo -e "${GREEN}Starting Backend API server...${NC}"
# Explicitly pass model name to backend
LLM_MODEL_NAME=$MODEL_NAME python -m backend.main > backend.log 2>&1 &
BACKEND_PID=$!
echo $BACKEND_PID >> "$PID_FILE"
echo "  Backend PID: $BACKEND_PID"
echo "  Logs: backend.log"
echo "  URL: http://localhost:8000"
echo ""

# Wait for backend to be ready
sleep 2
if wait_for_service "http://localhost:8000" > /dev/null 2>&1; then
    echo -e "${GREEN}Backend API is ready!${NC}"
else
    echo -e "${YELLOW}Backend may still be starting...${NC}"
fi
echo ""

# Check if frontend node_modules exists
if [ ! -d "frontend/node_modules" ]; then
    echo -e "${YELLOW}Installing frontend dependencies...${NC}"
    cd frontend
    npm install > /dev/null 2>&1
    cd ..
fi


# Check if Ollama is running
if check_port 11434; then
    echo -e "${GREEN}Ollama is already running.${NC}"
else
    echo -e "${YELLOW}Starting Ollama server...${NC}"
    if command -v ollama &> /dev/null; then
        ollama serve > ollama.log 2>&1 &
        OLLAMA_PID=$!
        echo $OLLAMA_PID >> "$PID_FILE"
        echo "  Ollama PID: $OLLAMA_PID"
        echo "  Logs: ollama.log"
        
        # Pull llama3.1 if not present (background)
        # Pull model (background)
        # Pull model (background)
        # Check if we need to pull the model
        if ! ollama list | grep -q "$MODEL_NAME"; then
             echo -e "${YELLOW}Pulling $MODEL_NAME model...${NC}"
             ollama pull $MODEL_NAME > /dev/null 2>&1 &
        fi
    else
        echo -e "${RED}Error: 'ollama' command not found. Please install Ollama first.${NC}"
    fi
fi
echo ""

# Start Frontend
echo -e "${GREEN}Starting Frontend development server...${NC}"
cd frontend
npm run dev > ../frontend.log 2>&1 &
FRONTEND_PID=$!
echo $FRONTEND_PID >> "$PID_FILE"
cd ..
echo "  Frontend PID: $FRONTEND_PID"
echo "  Logs: frontend.log"
echo "  URL: http://localhost:5173"
echo ""

# Wait for frontend to be ready
sleep 3
if wait_for_service "http://localhost:5173" > /dev/null 2>&1; then
    echo -e "${GREEN}Frontend is ready!${NC}"
else
    echo -e "${YELLOW}Frontend may still be starting...${NC}"
fi
echo ""

# Optional: Start MCP Server (commented out by default)
# Uncomment the following lines if you want to start the MCP server too
# echo -e "${GREEN}Starting MCP Server...${NC}"
# python -m src.server > mcp_server.log 2>&1 &
# MCP_PID=$!
# echo $MCP_PID >> "$PID_FILE"
# echo "  MCP Server PID: $MCP_PID"
# echo "  Logs: mcp_server.log"
# echo ""

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}All services started successfully!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Services running:"
echo -e "  ${GREEN}✓${NC} Backend API:    http://localhost:8000"
echo -e "  ${GREEN}✓${NC} Frontend:       http://localhost:5173"
echo -e "  ${GREEN}✓${NC} API Docs:       http://localhost:8000/docs"
echo -e "  ${GREEN}✓${NC} Ollama Local:   http://localhost:11434"
echo ""
echo -e "Log files:"
echo -e "  - Backend:  backend.log"
echo -e "  - Frontend: frontend.log"
echo -e "  - Ollama:   ollama.log"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop all services${NC}"
echo ""

# Wait for user interrupt
wait



