# DATA_STREAMING Package

## Overview

The `DATA_STREAMING` package provides comprehensive real-time data ingestion simulation for stock market data. It includes multiple streaming strategies, polling mechanisms, and market-aware scheduling to simulate real-time data ingestion from the Alpha Vantage API.

## 🏗️ Architecture

```
alpha_vantage_intraday/DATA_STREAMING/
├── __init__.py              # Package initialization and exports
├── streaming_service.py     # Main streaming service with threading
├── polling_manager.py       # Advanced polling strategies
├── market_scheduler.py      # Market-aware job scheduling
└── README.md               # This documentation
```

## 🚀 Core Components

### 1. DataStreamingService (`streaming_service.py`)

**Purpose**: Main service for real-time data streaming simulation using threading.

**Key Features**:
- **Threaded Streaming**: Runs streaming in background threads
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals
- **Configurable Intervals**: Adjustable polling frequency
- **Iteration Limits**: Optional maximum iteration limits
- **Callback Support**: Custom functions after each cycle
- **Status Monitoring**: Real-time streaming status

**Usage Example**:
```python
from alpha_vantage_intraday.DATA_STREAMING import DataStreamingService

# Initialize service
streaming = DataStreamingService()

# Start streaming (every 5 minutes, max 100 iterations)
streaming.start_streaming(
    symbols=["AAPL", "MSFT", "GOOGL"],
    interval_minutes=5,
    max_iterations=100
)

# Check status
status = streaming.get_streaming_status()
print(f"Streaming running: {status['is_running']}")

# Stop streaming
streaming.stop_streaming()
```

### 2. PollingManager (`polling_manager.py`)

**Purpose**: Manages different polling strategies for data ingestion.

**Polling Strategies**:

#### a) Continuous Polling
- **Description**: Polls continuously at fixed intervals
- **Use Case**: 24/7 data collection
- **Features**: Batch processing, retry logic, statistics

#### b) Market Hours Polling
- **Description**: Polls only during market hours (9 AM - 4 PM UTC)
- **Use Case**: Trading day data collection
- **Features**: Market time awareness, extended hours support

#### c) Adaptive Polling
- **Description**: Adjusts polling frequency based on data availability
- **Use Case**: Dynamic data collection
- **Features**: Frequency adjustment, performance optimization

**Usage Example**:
```python
from alpha_vantage_intraday.DATA_STREAMING import PollingManager, PollingConfig

# Initialize manager
polling = PollingManager()

# Configure continuous polling
config = PollingConfig(
    interval_minutes=5,
    max_iterations=50,
    symbols=["AAPL", "MSFT", "GOOGL"],
    batch_size=5
)

# Start different polling strategies
results = polling.continuous_polling(config)
# OR
results = polling.market_hours_polling(config)
# OR
results = polling.adaptive_polling(config)
```

### 3. MarketScheduler (`market_scheduler.py`)

**Purpose**: Market-aware job scheduling and execution.

**Key Features**:
- **Market Hours Detection**: Automatically detects market open/close
- **Pre-Market Support**: 4 AM - 9 AM UTC
- **After-Hours Support**: 4 PM - 8 PM UTC
- **Trading Days**: Monday-Friday (configurable)
- **Job Scheduling**: Schedule jobs for specific market periods
- **Automatic Execution**: Run jobs based on market conditions

**Market Schedule**:
- **Market Hours**: 9:00 AM - 4:00 PM UTC (Monday-Friday)
- **Pre-Market**: 4:00 AM - 9:00 AM UTC (Monday-Friday)
- **After Hours**: 4:00 PM - 8:00 PM UTC (Monday-Friday)

**Usage Example**:
```python
from alpha_vantage_intraday.DATA_STREAMING import MarketScheduler

# Initialize scheduler
scheduler = MarketScheduler()

# Schedule market hours job
scheduler.schedule_market_hours_job(
    job_name="daily_trading",
    symbols=["AAPL", "MSFT", "GOOGL"],
    interval_minutes=5
)

# Schedule pre-market job
scheduler.schedule_pre_market_job(
    job_name="pre_market_analysis",
    symbols=["SPY", "QQQ"],
    interval_minutes=15
)

# Check market status
if scheduler.is_market_open():
    print("Market is open!")
elif scheduler.is_pre_market():
    print("Pre-market trading")
elif scheduler.is_after_hours():
    print("After-hours trading")
else:
    print("Market is closed")

# Run scheduled jobs
results = scheduler.run_scheduled_jobs()
```

## 🎯 Command Line Interface

The `main.py` script provides comprehensive CLI access to all streaming functionality:

### Basic Streaming
```bash
# Start real-time streaming (every 5 minutes)
python main.py --stream --stream-interval 5

# Start streaming with max iterations
python main.py --stream --stream-interval 5 --stream-max-iterations 100

# Stop streaming
python main.py --stop-streaming

# Check streaming status
python main.py --streaming-status
```

### Advanced Polling
```bash
# Continuous polling
python main.py --continuous-poll --poll-interval 5

# Market hours polling
python main.py --market-hours-poll --poll-interval 5

# Adaptive polling
python main.py --adaptive-poll --poll-interval 5

# Polling with max iterations
python main.py --market-hours-poll --poll-interval 5 --poll-max-iterations 50
```

### Market Scheduling
```bash
# Schedule market hours job
python main.py --schedule-market-job "daily_trading" --job-symbols AAPL MSFT GOOGL --job-interval 5

# Schedule pre-market job
python main.py --schedule-pre-market-job "pre_market" --job-symbols SPY QQQ --job-interval 15

# Schedule after-hours job
python main.py --schedule-after-hours-job "after_hours" --job-symbols AAPL MSFT --job-interval 15

# Run all scheduled jobs
python main.py --run-scheduled-jobs

# Show scheduled jobs
python main.py --show-scheduled-jobs
```

## 🔧 Configuration

### Environment Variables
```bash
# Alpha Vantage API Key
ALPHA_VANTAGE_API_KEY=your_api_key_here

# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=stock_data
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

### Polling Configuration
```python
@dataclass
class PollingConfig:
    interval_minutes: int = 5          # Polling interval
    max_iterations: Optional[int] = None  # Max cycles (None = infinite)
    symbols: Optional[List[str]] = None   # Stock symbols
    interval: str = "5min"             # Alpha Vantage interval
    batch_size: int = 10               # Batch size for processing
    retry_on_failure: bool = True      # Enable retry logic
    max_retries: int = 3               # Maximum retry attempts
```

### Market Schedule Configuration
```python
@dataclass
class MarketSchedule:
    open_hour: int = 9                # Market open (9 AM UTC)
    close_hour: int = 16              # Market close (4 PM UTC)
    pre_market_start: int = 4         # Pre-market start (4 AM UTC)
    after_hours_end: int = 20         # After-hours end (8 PM UTC)
    timezone: str = "UTC"             # Timezone
    trading_days: List[int] = [0,1,2,3,4]  # Monday-Friday
```

## 📊 Monitoring and Statistics

### Streaming Status
```python
status = streaming_service.get_streaming_status()
# Returns:
{
    'is_running': True,
    'stop_event_set': False,
    'thread_alive': True,
    'timestamp': '2024-01-15T10:30:00'
}
```

### Polling Results
```python
results = polling_manager.continuous_polling(config)
# Returns:
{
    'total_iterations': 25,
    'successful_cycles': 23,
    'failed_cycles': 2,
    'total_records_processed': 1150,
    'start_time': '2024-01-15T09:00:00',
    'end_time': '2024-01-15T10:30:00',
    'total_duration': '1:30:00',
    'success_rate': 92.0
}
```

### Scheduled Jobs Status
```python
jobs_info = market_scheduler.get_scheduled_jobs()
# Returns:
{
    'total_jobs': 3,
    'jobs': {...},
    'current_time': '2024-01-15T10:30:00',
    'market_status': {
        'is_market_open': True,
        'is_pre_market': False,
        'is_after_hours': False,
        'next_market_open': '2024-01-15T09:00:00',
        'next_market_close': '2024-01-15T16:00:00'
    }
}
```

## 🚨 Error Handling

### Graceful Shutdown
- **Signal Handling**: SIGINT (Ctrl+C) and SIGTERM
- **Thread Cleanup**: Proper thread termination
- **Resource Cleanup**: Database connections and file handles

### Error Recovery
- **Retry Logic**: Automatic retry on failures
- **Batch Processing**: Continue processing other symbols on individual failures
- **Logging**: Comprehensive error logging and monitoring

### Rate Limiting
- **API Respect**: Built-in delays between API calls
- **Batch Delays**: Small delays between symbol batches
- **Configurable**: Adjustable timing parameters

## 🔄 Integration with ETL Pipeline

The DATA_STREAMING package seamlessly integrates with the main ETL pipeline:

```python
# The streaming services use the ETLService internally
from alpha_vantage_intraday.intraday_pipeline import ETLService

# All streaming operations go through the ETL pipeline
# - Extract: Alpha Vantage API calls
# - Transform: Data structure conversion
# - Load: PostgreSQL database insertion
# - Logging: ETL job tracking and monitoring
```

## 📈 Performance Considerations

### Threading
- **Background Processing**: Non-blocking streaming operations
- **Resource Management**: Efficient thread lifecycle management
- **Scalability**: Can handle multiple streaming operations

### Batching
- **Symbol Batching**: Process symbols in configurable batches
- **API Optimization**: Minimize API rate limit issues
- **Memory Management**: Efficient memory usage for large datasets

### Database
- **Connection Pooling**: Reuse database connections
- **Batch Inserts**: Efficient bulk data insertion
- **Transaction Management**: Proper transaction handling

## 🧪 Testing and Development

### Import Testing
```bash
# Test package imports
python -c "from alpha_vantage_intraday.DATA_STREAMING import DataStreamingService, PollingManager, MarketScheduler; print('✅ Imports successful!')"
```

### Functionality Testing
```bash
# Test streaming status
python main.py --streaming-status

# Test scheduled jobs
python main.py --show-scheduled-jobs

# Test market awareness
python main.py --schedule-market-job "test" --job-symbols AAPL --job-interval 1
```

## 🚀 Best Practices

### 1. **Start Small**
- Begin with a few symbols and short intervals
- Gradually increase complexity and volume

### 2. **Monitor Resources**
- Watch memory usage during long-running streams
- Monitor database connection pool usage

### 3. **Use Appropriate Strategies**
- **Market Hours**: For trading day data
- **Continuous**: For 24/7 monitoring
- **Adaptive**: For dynamic data requirements

### 4. **Handle Errors Gracefully**
- Implement proper error handling in callbacks
- Use retry logic for transient failures
- Monitor and log all errors

### 5. **Respect API Limits**
- Alpha Vantage free tier: 5 calls per minute
- Use appropriate delays between calls
- Implement rate limiting if needed

## 🔮 Future Enhancements

### Planned Features
- **WebSocket Support**: Real-time data streaming
- **Multiple API Sources**: Support for other data providers
- **Advanced Scheduling**: Cron-like job scheduling
- **Metrics Dashboard**: Real-time performance monitoring
- **Alert System**: Notifications for failures or anomalies

### Extensibility
- **Plugin Architecture**: Custom streaming strategies
- **Custom Callbacks**: Advanced post-processing hooks
- **Configuration Files**: YAML/JSON configuration support
- **API Rate Limiting**: Advanced rate limiting strategies

---

## 📚 Additional Resources

- **Main Documentation**: See the root README.md
- **ETL Pipeline**: `alpha_vantage_intraday/intraday_pipeline.py`
- **Database Management**: `alpha_vantage_intraday/DB/database.py`
- **Data Models**: `alpha_vantage_intraday/MODELS/models.py`
- **CLI Interface**: `main.py`

For questions or issues, please refer to the main project documentation or create an issue in the project repository.
