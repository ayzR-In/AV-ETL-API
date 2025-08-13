from datetime import datetime
from typing import Optional

class Stock:
    """Stock data structure for intraday trading"""
    
    def __init__(self, symbol: str, company_name: str, exchange: str, 
                 currency: str = "USD", is_active: bool = True):
        self.symbol = symbol
        self.company_name = company_name
        self.exchange = exchange
        self.currency = currency
        self.is_active = is_active
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

class StockPriceIntraday:
    """Intraday stock price data structure with OHLCV"""
    
    def __init__(self, stock_symbol: str, timestamp: datetime, open_price: float,
                 high_price: float, low_price: float, close_price: float,
                 volume: int, interval: str = "5min"):
        self.stock_symbol = stock_symbol
        self.timestamp = timestamp
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        self.interval = interval

class ETLJobLog:
    """ETL job execution log structure"""
    
    def __init__(self, job_name: str, status: str, start_time: datetime,
                 end_time: Optional[datetime] = None, records_processed: int = 0,
                 error_message: Optional[str] = None):
        self.job_name = job_name
        self.status = status  # SUCCESS, FAILED, RUNNING
        self.start_time = start_time
        self.end_time = end_time
        self.records_processed = records_processed
        self.error_message = error_message
        self.created_at = datetime.utcnow()
