# Stock Data ETL System

A pure ETL (Extract, Transform, Load) system for stock market data built with Python and PostgreSQL. This system retrieves stock price data from Alpha Vantage API, transforms it, and loads it into a scalable relational database using raw SQL for maximum performance and control.

## 🚀 Features

- **Pure ETL Pipeline**: No ORM overhead, direct SQL operations for maximum performance
- **Real-time Data Ingestion**: Automated ETL jobs with configurable scheduling
- **Scalable Architecture**: Optimized database schema with proper indexing
- **Docker Support**: Easy deployment with Docker Compose
- **Raw SQL Operations**: Direct database control without abstraction layers
- **Comprehensive Logging**: ETL job monitoring and debugging
- **Rate Limiting**: Respects API limits and implements proper delays
- **Background Processing**: Non-blocking ETL job execution

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Alpha Vantage │    │   Python ETL    │    │   PostgreSQL    │
│      API        │───▶│   Service       │───▶│   Database      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │ DATA_STREAMING  │
                       │  (Real-time     │
                       │   Ingestion)    │
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

# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL (ensure it's running on port 5432)
# Update .env with your database connection details

# Initialize database
python trigger_pipeline.py --init-db

# Run ETL jobs
python trigger_pipeline.py --batch AAPL MSFT GOOGL
```

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
python main.py --init-db
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

## 🔒 Security Considerations

- **Input Validation**: Direct SQL parameterization
- **SQL Injection Protection**: Proper parameter binding
- **Rate Limiting**: API call throttling
- **Environment Variables**: Secure configuration management

## 🧪 Testing

```bash
# Test database connection
python run_etl.py --init-db

# Test single stock ETL
python trigger_pipeline.py --single AAPL

# Test batch ETL
python trigger_pipeline.py --batch AAPL MSFT

# Test real-time streaming
python trigger_pipeline.py --stream --stream-interval 5
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
│   ├── DATA_STREAMING/
│   │   ├── __init__.py
│   │   ├── streaming_service.py  # Real-time streaming
│   │   ├── polling_manager.py    # Advanced polling strategies
│   │   └── market_scheduler.py   # Market-aware scheduling
│   └── intraday_pipeline.py      # ETL pipeline orchestration
├── trigger_pipeline.py     # Command-line ETL runner
├── docker-compose.yml      # PostgreSQL service
├── requirements.txt        # Python dependencies
├── start_etl.sh           # Startup script
└── README.md              # This file
```

### Adding New Features

1. **New Data Models**: Add to `alpha_vantage_intraday/MODELS/models.py`
2. **New ETL Logic**: Extend `alpha_vantage_intraday/intraday_pipeline.py`
3. **New Streaming Strategies**: Add to `alpha_vantage_intraday/DATA_STREAMING/`
4. **Database Changes**: Modify `alpha_vantage_intraday/DB/database.py` create_tables method

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

### Logs

```bash
# Application logs
# Logs are now displayed in the console (no separate log file)

# Docker logs
docker-compose logs -f etl-app

# Database logs
docker-compose logs -f postgres
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- [Alpha Vantage](https://www.alphavantage.co/) - Market data API
- [PostgreSQL](https://www.postgresql.org/) - Database system
- [psycopg2](https://www.psycopg.org/) - PostgreSQL adapter for Python

## 📞 Support

For questions or issues:

1. Check the troubleshooting section
2. Review the logs for error details
3. Check the command-line help: `python trigger_pipeline.py --help`
4. Open an issue on GitHub

---

**Happy Trading! 📈**
