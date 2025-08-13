#!/usr/bin/env python3
"""
Pydantic models for API request/response validation
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Generic, TypeVar
from datetime import datetime

# Generic type for paginated responses
T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response model"""
    items: List[T]
    total: int
    skip: int
    limit: int
    has_more: bool

class StockResponse(BaseModel):
    """Stock response model"""
    id: Optional[int] = None
    symbol: str = Field(..., description="Stock symbol (e.g., AAPL)")
    company_name: str = Field(..., description="Company name")
    exchange: str = Field(..., description="Stock exchange (e.g., NYSE, NASDAQ)")
    is_active: bool = Field(True, description="Whether the stock is active")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class StockPriceResponse(BaseModel):
    """Stock price response model"""
    id: Optional[int] = None
    stock_symbol: str = Field(..., description="Stock symbol")
    timestamp: datetime = Field(..., description="Price timestamp")
    open_price: float = Field(..., description="Opening price")
    high_price: float = Field(..., description="Highest price")
    low_price: float = Field(..., description="Lowest price")
    close_price: float = Field(..., description="Closing price")
    volume: int = Field(..., description="Trading volume")
    interval: str = Field(..., description="Time interval (e.g., 5min)")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ETLJobResponse(BaseModel):
    """ETL job response model"""
    id: Optional[int] = None
    job_name: str = Field(..., description="ETL job name")
    status: str = Field(..., description="Job status (SUCCESS, FAILED, RUNNING)")
    start_time: datetime = Field(..., description="Job start time")
    end_time: Optional[datetime] = Field(None, description="Job end time")
    records_processed: int = Field(0, description="Number of records processed")
    total_records: int = Field(0, description="Total number of records")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class StockCreateRequest(BaseModel):
    """Request model for creating a stock"""
    symbol: str = Field(..., description="Stock symbol")
    company_name: str = Field(..., description="Company name")
    exchange: str = Field(..., description="Stock exchange")
    is_active: bool = Field(True, description="Whether the stock is active")

class StockUpdateRequest(BaseModel):
    """Request model for updating a stock"""
    company_name: Optional[str] = Field(None, description="Company name")
    exchange: Optional[str] = Field(None, description="Stock exchange")
    is_active: Optional[bool] = Field(None, description="Whether the stock is active")

class StockSummaryResponse(BaseModel):
    """Stock summary and statistics response"""
    symbol: str
    interval: str
    start_date: datetime
    end_date: datetime
    total_records: int
    price_stats: dict
    volume_stats: dict
    volatility_stats: dict

class MarketOverviewResponse(BaseModel):
    """Market overview response"""
    total_stocks: int
    total_price_records: int
    date_range: dict
    interval: str
    market_stats: dict
    top_gainers: List[dict]
    top_losers: List[dict]
    most_active: List[dict]
