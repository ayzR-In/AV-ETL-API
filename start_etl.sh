#!/bin/bash

# Stock Data Intraday ETL System - Startup Script

echo "🚀 Starting Stock Data Intraday ETL System (PostgreSQL Only)..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ .env file not found!"
    echo "📝 Please create .env file with the following configuration:"
    echo ""
    echo "   # Alpha Vantage API Configuration"
    echo "   ALPHA_VANTAGE_API_KEY=your_api_key_here"
    echo ""
    echo "   # Database Configuration"
    echo "   POSTGRES_HOST=localhost"
    echo "   POSTGRES_PORT=5432"
    echo "   POSTGRES_DB=stock_data"
    echo "   POSTGRES_USER=postgres"
    echo "   POSTGRES_PASSWORD=postgres"
    echo ""
    echo "   # Application Configuration"
    echo "   LOG_LEVEL=INFO"
    echo "   DEFAULT_INTERVAL=5min"
    echo "   DEFAULT_POLLING_INTERVAL=5"
    echo "   BATCH_SIZE=10"
    echo ""
    echo "   Then run this script again."
    exit 1
fi

# Check if ALPHA_VANTAGE_API_KEY is set
if ! grep -q "ALPHA_VANTAGE_API_KEY=your_api_key_here" .env; then
    echo "✅ Environment configuration looks good"
    echo ""
    echo "📋 Current Configuration:"
    echo "  API Key: $(grep 'ALPHA_VANTAGE_API_KEY=' .env | cut -d'=' -f2 | head -c 10)..."
    echo "  Database: $(grep 'POSTGRES_HOST=' .env | cut -d'=' -f2):$(grep 'POSTGRES_PORT=' .env | cut -d'=' -f2)"
    echo "  Log Level: $(grep 'LOG_LEVEL=' .env | cut -d'=' -f2)"
    echo "  Default Interval: $(grep 'DEFAULT_INTERVAL=' .env | cut -d'=' -f2)"
    echo "  Polling Interval: $(grep 'DEFAULT_POLLING_INTERVAL=' .env | cut -d'=' -f2) minutes"
    echo "  Batch Size: $(grep 'BATCH_SIZE=' .env | cut -d'=' -f2)"
else
    echo "❌ Please set your Alpha Vantage API key in .env file"
    exit 1
fi

echo "🐳 Starting PostgreSQL service..."
docker-compose up -d postgres

echo "⏳ Waiting for PostgreSQL to start..."
sleep 10

# Check service status
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "📈 PostgreSQL Database is running!"
echo "🚀 ETL Pipeline ready to be triggered manually!"
echo ""
echo "🏗️  System Architecture:"
echo "  📁 Alpha Vantage Intraday Package: alpha_vantage_intraday/"
echo "    📁 ETL Package: alpha_vantage_intraday/ETL/"
echo "      📥 Extract: alpha_vantage_intraday/ETL/extract.py - API data extraction"
echo "      🔄 Transform: alpha_vantage_intraday/ETL/transform.py - Data transformation"
echo "      📤 Load: alpha_vantage_intraday/ETL/load.py - Database loading"
echo "    📁 DB Package: alpha_vantage_intraday/DB/"
echo "      🗄️  Database: alpha_vantage_intraday/DB/database.py - Database management"
echo "    📁 MODELS Package: alpha_vantage_intraday/MODELS/"
echo "      🏗️  Models: alpha_vantage_intraday/MODELS/models.py - Data structures"
echo "    📁 DATA_STREAMING Package: alpha_vantage_intraday/DATA_STREAMING/"
echo "      🌊 Streaming: alpha_vantage_intraday/DATA_STREAMING/streaming_service.py - Real-time data streaming"
echo "      🔄 Polling: alpha_vantage_intraday/DATA_STREAMING/polling_manager.py - Advanced polling strategies"
echo "      ⏰ Scheduler: alpha_vantage_intraday/DATA_STREAMING/market_scheduler.py - Market-aware scheduling"
echo "    🎯 Intraday Pipeline: alpha_vantage_intraday/intraday_pipeline.py - Pipeline orchestration"
echo ""
echo "🚀 Advanced Streaming & Polling Examples:"
echo "  # Real-time data streaming (every 5 minutes)"
echo "  python trigger_pipeline.py --stream --stream-interval 5"
echo ""
echo "  # Market hours polling (only during trading hours)"
echo "  python trigger_pipeline.py --market-hours-poll --poll-interval 5"
echo ""
echo "  # Adaptive polling (adjusts frequency based on data availability)"
echo "  python trigger_pipeline.py --adaptive-poll --poll-interval 5"
echo ""
echo "  # Schedule market-aware jobs"
echo "  python trigger_pipeline.py --schedule-market-job 'daily_trading' --job-symbols AAPL MSFT GOOGL --job-interval 5"
echo "  python trigger_pipeline.py --run-scheduled-jobs"
echo ""
echo "  # Check streaming status and scheduled jobs"
echo "  python trigger_pipeline.py --streaming-status"
echo "  python trigger_pipeline.py --show-scheduled-jobs"
echo ""
echo "📝 View logs: docker-compose logs -f postgres"
echo "🛑 Stop services: docker-compose down"
echo ""
echo "🔧 Manual ETL commands:"
echo "  - Single stock: python trigger_pipeline.py --single AAPL --interval 5min"
echo "  - Batch stocks: python trigger_pipeline.py --batch AAPL MSFT GOOGL --interval 5min"

echo "  - Check status: python trigger_pipeline.py --status"
echo "  - Start streaming: python trigger_pipeline.py --stream --stream-interval 5"
echo "  - Single cycle: python trigger_pipeline.py --cycle"
echo ""
echo "🔌 Programmatic ETL control:"
echo "  - Import trigger_pipeline.py ETLRunner class in your scripts"
echo "  - Use methods like runner.process_single_stock('AAPL')"
echo "  - Run complete pipeline: runner.run_etl_pipeline(['AAPL', 'MSFT', 'GOOGL'])"
echo "  - Control ETL operations from other applications"
echo ""
echo "📊 Available intervals: 1min, 5min, 15min, 30min, 60min"
echo "⏰ ETL pipeline triggered manually via Python commands"
echo ""
echo "🎉 Stock Data Intraday ETL System is ready!"
