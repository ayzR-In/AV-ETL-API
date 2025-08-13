#!/bin/bash

echo "🚀 Starting Stock Data Intraday API..."
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install -r requirements.txt

# Check if database is running
echo "🔍 Checking database connection..."
if ! python -c "
import sys
sys.path.append('..')
from alpha_vantage_intraday.DB.database import DatabaseManager
try:
    db = DatabaseManager()
    db.init_connection_pool()
    print('✅ Database connection successful')
    db.close()
except Exception as e:
    print(f'❌ Database connection failed: {e}')
    sys.exit(1)
" 2>/dev/null; then
    echo "❌ Database connection failed. Please ensure PostgreSQL is running."
    echo "   Run: docker-compose up -d postgres"
    exit 1
fi

echo ""
echo "🌐 Starting FastAPI server..."
echo "   📖 API Documentation: http://localhost:8000/docs"
echo "   📚 ReDoc Documentation: http://localhost:8000/redoc"
echo "   🏥 Health Check: http://localhost:8000/health"
echo "   🚀 API Root: http://localhost:8000/"
echo ""

# Start the API server
cd "$(dirname "$0")"
python main.py
