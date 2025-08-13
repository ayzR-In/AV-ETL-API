#!/usr/bin/env python3
"""
FastAPI RESTful API for Stock Data Intraday ETL System
Provides CRUD operations on stored market data
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import uvicorn
import os
from dotenv import load_dotenv

from database import get_db, DatabaseManager
from models import StockResponse, StockPriceResponse, ETLJobResponse, PaginatedResponse
from services import StockService, PriceService, ETLService

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(
    title="Stock Data Intraday API",
    description="RESTful API for Stock Data Intraday ETL System",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
stock_service = StockService()
price_service = PriceService()
etl_service = ETLService()

@app.get("/", tags=["Root"])
async def root():
    """API root endpoint"""
    return {
        "message": "Stock Data Intraday API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# Stock endpoints
@app.get("/api/stocks", response_model=PaginatedResponse[StockResponse], tags=["Stocks"])
async def get_stocks(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    symbol: Optional[str] = Query(None, description="Filter by stock symbol"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    db: DatabaseManager = Depends(get_db)
):
    """Get stocks with pagination and filtering"""
    try:
        stocks = await stock_service.get_stocks(
            db, skip=skip, limit=limit, 
            symbol=symbol, exchange=exchange, is_active=is_active
        )
        return stocks
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks/{symbol}", response_model=StockResponse, tags=["Stocks"])
async def get_stock(
    symbol: str,
    db: DatabaseManager = Depends(get_db)
):
    """Get a specific stock by symbol"""
    try:
        stock = await stock_service.get_stock_by_symbol(db, symbol.upper())
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")
        return stock
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/stocks", response_model=StockResponse, tags=["Stocks"])
async def create_stock(
    stock_data: StockResponse,
    db: DatabaseManager = Depends(get_db)
):
    """Create a new stock"""
    try:
        stock = await stock_service.create_stock(db, stock_data)
        return stock
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/stocks/{symbol}", response_model=StockResponse, tags=["Stocks"])
async def update_stock(
    symbol: str,
    stock_data: StockResponse,
    db: DatabaseManager = Depends(get_db)
):
    """Update an existing stock"""
    try:
        stock = await stock_service.update_stock(db, symbol.upper(), stock_data)
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")
        return stock
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/stocks/{symbol}", tags=["Stocks"])
async def delete_stock(
    symbol: str,
    db: DatabaseManager = Depends(get_db)
):
    """Delete a stock (soft delete - sets is_active to False)"""
    try:
        success = await stock_service.delete_stock(db, symbol.upper())
        if not success:
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")
        return {"message": f"Stock {symbol} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Stock Price endpoints
@app.get("/api/stocks/{symbol}/prices", response_model=PaginatedResponse[StockPriceResponse], tags=["Prices"])
async def get_stock_prices(
    symbol: str,
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    start_date: Optional[datetime] = Query(None, description="Start date for price range"),
    end_date: Optional[datetime] = Query(None, description="End date for price range"),
    interval: Optional[str] = Query(None, description="Filter by time interval"),
    db: DatabaseManager = Depends(get_db)
):
    """Get stock prices with pagination and filtering"""
    try:
        prices = await price_service.get_stock_prices(
            db, symbol.upper(), skip=skip, limit=limit,
            start_date=start_date, end_date=end_date, interval=interval
        )
        return prices
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks/{symbol}/prices/latest", response_model=StockPriceResponse, tags=["Prices"])
async def get_latest_price(
    symbol: str,
    interval: str = Query("5min", description="Time interval for latest price"),
    db: DatabaseManager = Depends(get_db)
):
    """Get the latest price for a stock"""
    try:
        price = await price_service.get_latest_price(db, symbol.upper(), interval)
        if not price:
            raise HTTPException(status_code=404, detail=f"No price data found for {symbol}")
        return price
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/prices", response_model=PaginatedResponse[StockPriceResponse], tags=["Prices"])
async def get_all_prices(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    start_date: Optional[datetime] = Query(None, description="Start date for price range"),
    end_date: Optional[datetime] = Query(None, description="End date for price range"),
    interval: Optional[str] = Query(None, description="Filter by time interval"),
    symbols: Optional[List[str]] = Query(None, description="Filter by stock symbols"),
    db: DatabaseManager = Depends(get_db)
):
    """Get all stock prices with pagination and filtering"""
    try:
        prices = await price_service.get_all_prices(
            db, skip=skip, limit=limit,
            start_date=start_date, end_date=end_date, 
            interval=interval, symbols=symbols
        )
        return prices
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ETL Job endpoints
@app.get("/api/etl/jobs", response_model=PaginatedResponse[ETLJobResponse], tags=["ETL"])
async def get_etl_jobs(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    status: Optional[str] = Query(None, description="Filter by job status"),
    job_name: Optional[str] = Query(None, description="Filter by job name"),
    start_date: Optional[datetime] = Query(None, description="Start date for job range"),
    end_date: Optional[datetime] = Query(None, description="End date for job range"),
    db: DatabaseManager = Depends(get_db)
):
    """Get ETL job logs with pagination and filtering"""
    try:
        jobs = await etl_service.get_etl_jobs(
            db, skip=skip, limit=limit,
            status=status, job_name=job_name,
            start_date=start_date, end_date=end_date
        )
        return jobs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/status", tags=["ETL"])
async def get_etl_status(db: DatabaseManager = Depends(get_db)):
    """Get current ETL system status"""
    try:
        status = await etl_service.get_etl_status(db)
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Analytics endpoints
@app.get("/api/analytics/stocks/{symbol}/summary", tags=["Analytics"])
async def get_stock_summary(
    symbol: str,
    start_date: Optional[datetime] = Query(None, description="Start date for analysis"),
    end_date: Optional[datetime] = Query(None, description="End date for analysis"),
    interval: str = Query("5min", description="Time interval for analysis"),
    db: DatabaseManager = Depends(get_db)
):
    """Get stock price summary and statistics"""
    try:
        summary = await price_service.get_stock_summary(
            db, symbol.upper(), start_date, end_date, interval
        )
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/market/overview", tags=["Analytics"])
async def get_market_overview(
    start_date: Optional[datetime] = Query(None, description="Start date for overview"),
    end_date: Optional[datetime] = Query(None, description="End date for overview"),
    interval: str = Query("5min", description="Time interval for overview"),
    db: DatabaseManager = Depends(get_db)
):
    """Get market overview and statistics"""
    try:
        overview = await price_service.get_market_overview(
            db, start_date, end_date, interval
        )
        return overview
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 8000))
    host = os.getenv("API_HOST", "0.0.0.0")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True,
        log_level="info"
    )
