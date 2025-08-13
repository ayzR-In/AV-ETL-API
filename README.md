# Stock Data ETL System

A pure ETL (Extract, Transform, Load) system for stock market data built with Python and PostgreSQL. This system retrieves stock price data from Alpha Vantage API, transforms it, and loads it into a scalable relational database using raw SQL for maximum performance and control.

**Now with RESTful API Support!** 🚀 The system includes a FastAPI application that provides full CRUD operations on your stored market data.

## 🚀 Features

- **Pure ETL Pipeline**: No ORM overhead, direct SQL operations for maximum performance
- **Real-time Data Ingestion**: Automated ETL jobs with configurable scheduling
- **Scalable Architecture**: Optimized database schema with proper indexing
- **Docker Support**: Easy deployment with Docker Compose
- **Raw SQL Operations**: Direct database control without abstraction layers
- **Comprehensive Logging**: ETL job monitoring and debugging
- **Rate Limiting**: Respects API limits and implements proper delays
- **Background Processing**: Non-blocking ETL job execution
- **RESTful API**: Full CRUD operations via FastAPI with interactive documentation
- **Advanced Analytics**: Stock summaries, market overviews, and ETL monitoring

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Alpha Vantage │    │   Python ETL    │    │   PostgreSQL    │
│      API        │───▶│   Service       │───▶│   Database      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                                             │ STREAM_N_POLLING│
                      │  (Real-time     │
                      │   Ingestion)    │
                       └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   FastAPI       │
                       │  (REST API)     │
                       │ • CRUD Ops      │
                       │ • Analytics     │
                       │ • ETL Monitor   │
                       └─────────────────┘
```

## 📋 Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Alpha Vantage API key (free tier available)

## 🛠️ Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd TA-120802
```

### 2. Set Up Environment Variables

Create a `.env` file with the following configuration:

```bash
# Alpha Vantage API Configuration
ALPHA_VANTAGE_API_KEY=your_actual_api_key_here

# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=stock_data
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Application Configuration
LOG_LEVEL=INFO
DEFAULT_INTERVAL=5min
DEFAULT_POLLING_INTERVAL=5
BATCH_SIZE=10

# API Configuration (optional)
API_HOST=0.0.0.0
API_PORT=8000
```

### 3. Start with Docker (Recommended)

```bash
# Start PostgreSQL database
./start_etl.sh

# Or manually:
docker-compose up -d postgres
```

### 4. Manual Setup (Alternative)

If you prefer to run without Docker:

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies (now includes FastAPI)
pip install -r requirements.txt

# Start PostgreSQL (ensure it's running on port 5432)
# Update .env with your database connection details

# Initialize database
python trigger_pipeline.py --init-db

# Run ETL jobs
python trigger_pipeline.py --batch AAPL MSFT GOOGL
```

## 🌐 FastAPI REST API

The system now includes a comprehensive REST API built with FastAPI that provides full CRUD operations on your stored market data.

### Quick Start API

```bash
# Start the API from root directory
./start_api.sh
```

The API will be available at:
- **API**: http://localhost:8000
- **Interactive Documentation**: http://localhost:8000/docs
- **ReDoc Documentation**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

### API Endpoints

#### �� Stocks
- `GET /api/stocks` - List stocks with pagination and filtering
- `GET /api/stocks/{symbol}` - Get specific stock
- `POST /api/stocks` - Create new stock
- `PUT /api/stocks/{symbol}` - Update stock
- `DELETE /api/stocks/{symbol}` - Soft delete stock

#### 💰 Stock Prices
- `GET /api/stocks/{symbol}/prices` - Get stock prices with filtering
- `GET /api/stocks/{symbol}/prices/latest` - Get latest price
- `GET /api/prices` - Get all prices with filtering

#### 🔄 ETL Jobs
- `GET /api/etl/jobs` - Get ETL job logs
- `GET /api/etl/status` - Get ETL system status

#### 📊 Analytics
- `GET /api/analytics/stocks/{symbol}/summary` - Stock summary and statistics
- `GET /api/analytics/market/overview` - Market overview and statistics

### API Usage Examples

```bash
# Get all stocks
curl "http://localhost:8000/api/stocks?limit=10&skip=0"

# Get stock prices with filtering
curl "http://localhost:8000/api/stocks/AAPL/prices?interval=5min&limit=5"

# Get latest price
curl "http://localhost:8000/api/stocks/AAPL/prices/latest?interval=5min"

# Get stock summary
curl "http://localhost:8000/api/analytics/stocks/AAPL/summary?interval=5min"

# Get market overview
curl "http://localhost:8000/api/analytics/market/overview?interval=5min"
```

### API Features

- **Full CRUD Operations**: Create, read, update, delete stocks and prices
- **Advanced Filtering**: By symbol, exchange, date ranges, intervals
- **Pagination**: Efficient handling of large datasets
- **Real-time Analytics**: Stock summaries, market overviews, volatility stats
- **ETL Monitoring**: Job status, execution logs, performance metrics
- **Interactive Documentation**: Auto-generated Swagger UI and ReDoc
- **Input Validation**: Pydantic models for request/response validation
- **Error Handling**: Comprehensive HTTP status codes and error messages

## 🗄️ Database Setup

### Automatic Setup (Docker)

The PostgreSQL database is automatically created when using Docker Compose. The ETL pipeline is triggered manually via Python commands.

### Manual Setup

1. Create PostgreSQL database:
```sql
CREATE DATABASE stock_data;
```

2. Initialize tables:
```bash
python trigger_pipeline.py --init-db
```

## 📊 Database Schema

The system includes several optimized tables created with raw SQL:

- **`stocks`**: Master stock information (symbol, company name, sector, etc.)
- **`stock_prices`**: Historical daily price data (OHLCV)
- **`stock_prices_intraday`**: Intraday price data for real-time analysis
- **`etl_job_logs`**: ETL job execution logs and monitoring
- **`market_indices`**: Market index information
- **`market_index_prices`**: Market index price data

### Key Design Features

- **Raw SQL**: No ORM overhead, direct database control
- **Proper Indexing**: Optimized for time-series queries
- **Connection Pooling**: Efficient database connection management
- **Batch Operations**: High-performance bulk data loading
- **Soft Deletes**: Stocks are deactivated rather than deleted

## 🔄 ETL Jobs

### Automated Scheduling

The system includes a built-in scheduler that runs:

- **Daily Updates**: Market data refresh at 9 AM UTC
- **Intraday Updates**: Hourly updates during market hours (9 AM - 4 PM UTC)
- **Weekly Refresh**: Full historical data refresh on Sundays
- **Health Checks**: System monitoring every 30 minutes

### Manual Execution

You can manually run ETL jobs using the command-line interface:

```bash
# Single stock
python trigger_pipeline.py --single AAPL

# Batch processing
python trigger_pipeline.py --batch AAPL MSFT GOOGL

# Batch processing with pipeline stats
python trigger_pipeline.py --batch AAPL MSFT GOOGL --pipeline

# Check system status
python trigger_pipeline.py --status

# Start real-time streaming
python trigger_pipeline.py --stream --stream-interval 5
```

## 🐳 Docker Commands

```bash
# Start PostgreSQL only
docker-compose up -d postgres

# Start all services (if any)
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f postgres

# Restart PostgreSQL
docker-compose restart postgres

# Clean up volumes
docker-compose down -v
```

## 📈 Monitoring and Logs

### ETL Job Monitoring

```bash
# Check ETL status
python trigger_pipeline.py --status

# View logs
# Logs are displayed in the console (no separate log file)

# Docker logs
docker-compose logs -f postgres
```

### Database Monitoring

```bash
# Connect to database
docker-compose exec postgres psql -U postgres -d stock_data

# Check table sizes
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### API Monitoring

```bash
# Health check
curl http://localhost:8000/health

# API status
curl http://localhost:8000/api/etl/status

# View API logs
# API logs are displayed in the console when running
```

## 🚀 Performance Optimization

### Database Optimization

- **Raw SQL**: No ORM overhead, direct database control
- **Connection Pooling**: Efficient connection management
- **Indexes**: Optimized for time-series queries
- **Batch Operations**: High-performance bulk data loading
- **Query Optimization**: Direct SQL control for performance tuning

### ETL Optimization

- **Rate Limiting**: Respects API limits
- **Background Processing**: Non-blocking job execution
- **Error Handling**: Comprehensive retry logic
- **Monitoring**: Real-time job status tracking

### API Optimization

- **Async Operations**: FastAPI's async support for high concurrency
- **Connection Reuse**: Efficient database connection management
- **Response Caching**: Consider implementing Redis for production
- **Rate Limiting**: Built-in protection against abuse

## 🔒 Security Considerations

- **Input Validation**: Direct SQL parameterization
- **SQL Injection Protection**: Proper parameter binding
- **Rate Limiting**: API call throttling
- **Environment Variables**: Secure configuration management
- **CORS Configuration**: Configurable cross-origin settings
- **Authentication**: Ready for JWT or OAuth2 integration

## 🧪 Testing

```bash
# Test database connection
python trigger_pipeline.py --init-db

# Test single stock ETL
python trigger_pipeline.py --single AAPL

# Test batch ETL
python trigger_pipeline.py --batch AAPL MSFT

# Test real-time streaming
python trigger_pipeline.py --stream --stream-interval 5

# Test API (after starting it)
curl http://localhost:8000/health
curl "http://localhost:8000/api/stocks?limit=5"
```

## 📝 Development

### Project Structure

```
TA-120802/
├── alpha_vantage_intraday/
│   ├── __init__.py
│   ├── DB/
│   │   ├── __init__.py
│   │   └── database.py     # Raw SQL database operations
│   ├── MODELS/
│   │   ├── __init__.py
│   │   └── models.py       # Data structures (no ORM)
│   ├── ETL/
│   │   ├── __init__.py
│   │   ├── extract.py      # Data extraction from API
│   │   ├── transform.py    # Data transformation
│   │   └── load.py         # Data loading to database
│   ├── STREAM_N_POLLING/
│   │   ├── __init__.py
│   │   ├── streaming_service.py  # Real-time streaming
│   │   ├── polling_manager.py    # Advanced polling strategies

│   └── intraday_pipeline.py      # ETL pipeline orchestration
├── api/                        # 🆕 FastAPI Application
│   ├── main.py                 # FastAPI app with all endpoints
│   ├── database.py             # Database connection layer
│   ├── models.py               # Pydantic models
│   └── services.py             # Business logic
├── start_api.sh                # 🆕 API startup script
├── trigger_pipeline.py         # Command-line ETL runner
├── docker-compose.yml          # PostgreSQL service
├── requirements.txt            # Python dependencies (merged)
├── start_etl.sh                # Startup script
└── README.md                   # This file
```

### Adding New Features

1. **New Data Models**: Add to `alpha_vantage_intraday/MODELS/models.py`
2. **New ETL Logic**: Extend `alpha_vantage_intraday/intraday_pipeline.py`
3. **New Streaming Strategies**: Add to `alpha_vantage_intraday/STREAM_N_POLLING/`
4. **Database Changes**: Modify `alpha_vantage_intraday/DB/database.py` create_tables method
5. **New API Endpoints**: Add to `api/main.py` and `api/services.py`

## 🚀 Production Deployment

### ETL System

```bash
# Using systemd
sudo systemctl enable stock-etl
sudo systemctl start stock-etl

# Or using supervisor
# Configure supervisor to run trigger_pipeline.py
```

### API System

```bash
# Using systemd
sudo systemctl enable stock-api
sudo systemctl start stock-api

# Or using supervisor
# Configure supervisor to run the FastAPI app
```

### Reverse Proxy (Nginx)

```nginx
# ETL endpoints (if needed)
server {
    listen 80;
    server_name etl.yourdomain.com;
    
    location / {
        proxy_pass http://localhost:8001;  # ETL service
    }
}

# API endpoints
server {
    listen 80;
    server_name api.yourdomain.com;
    
    location / {
        proxy_pass http://localhost:8000;  # FastAPI
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## 🐛 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check PostgreSQL is running
   - Verify connection string in `.env`
   - Check Docker container status

2. **API Key Issues**
   - Verify Alpha Vantage API key
   - Check rate limits
   - Ensure key is in `.env` file

3. **ETL Jobs Not Running**
   - Check scheduler status
   - View ETL job logs
   - Verify database connectivity

4. **API Not Starting**
   - Check if port 8000 is available
   - Verify FastAPI dependencies are installed
   - Check database connectivity from API

### Logs

```bash
# Application logs
# Logs are now displayed in the console (no separate log file)

# Docker logs
docker-compose logs -f postgres

# API logs
# Run API in foreground to see logs: python api/main.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly (both ETL and API)
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- [Alpha Vantage](https://www.alphavantage.co/) - Market data API
- [PostgreSQL](https://www.postgresql.org/) - Database system
- [psycopg2](https://www.psycopg.org/) - PostgreSQL adapter for Python
- [FastAPI](https://fastapi.tiangolo.com/) - Modern, fast web framework
- [Pydantic](https://pydantic-docs.helpmanual.io/) - Data validation using Python type annotations

## 📞 Support

For questions or issues:

1. Check the troubleshooting section
2. Review the logs for error details
3. Check the command-line help: `python trigger_pipeline.py --help`
4. For API issues, check the interactive docs at `/docs`
5. Open an issue on GitHub

---

**Happy Trading! 📈**

**Your ETL system now has a powerful REST API for easy data access and management! 🚀**
