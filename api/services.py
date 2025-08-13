#!/usr/bin/env python3
"""
Service layer for API business logic
Handles data operations and business rules
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
from alpha_vantage_intraday.DB.database import DatabaseManager
from models import StockResponse, StockPriceResponse, ETLJobResponse, PaginatedResponse

logger = logging.getLogger(__name__)

class StockService:
    """Service for stock-related operations"""
    
    async def get_stocks(
        self, 
        db: DatabaseManager, 
        skip: int = 0, 
        limit: int = 100,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> PaginatedResponse[StockResponse]:
        """Get stocks with pagination and filtering"""
        try:
            # Build query conditions
            conditions = []
            params = []
            
            if symbol:
                conditions.append("symbol ILIKE %s")
                params.append(f"%{symbol}%")
            
            if exchange:
                conditions.append("exchange ILIKE %s")
                params.append(f"%{exchange}%")
            
            if is_active is not None:
                conditions.append("is_active = %s")
                params.append(is_active)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Get total count
            count_sql = f"SELECT COUNT(*) FROM stocks WHERE {where_clause}"
            total_result = db.execute_query(count_sql, tuple(params), fetch_one=True)
            total = total_result['count'] if total_result else 0
            
            # Get paginated results
            query_sql = f"""
                SELECT id, symbol, company_name, exchange, is_active, created_at, updated_at
                FROM stocks 
                WHERE {where_clause}
                ORDER BY symbol
                LIMIT %s OFFSET %s
            """
            params.extend([limit, skip])
            
            results = db.execute_query(query_sql, tuple(params), fetch=True)
            
            # Convert to response models
            stocks = [StockResponse(**dict(row)) for row in results] if results else []
            
            return PaginatedResponse(
                items=stocks,
                total=total,
                skip=skip,
                limit=limit,
                has_more=(skip + limit) < total
            )
            
        except Exception as e:
            logger.error(f"Error getting stocks: {e}")
            raise
    
    async def get_stock_by_symbol(self, db: DatabaseManager, symbol: str) -> Optional[StockResponse]:
        """Get a specific stock by symbol"""
        try:
            query_sql = """
                SELECT id, symbol, company_name, exchange, is_active, created_at, updated_at
                FROM stocks 
                WHERE symbol = %s
            """
            result = db.execute_query(query_sql, (symbol,), fetch_one=True)
            
            if result:
                return StockResponse(**dict(result))
            return None
            
        except Exception as e:
            logger.error(f"Error getting stock {symbol}: {e}")
            raise
    
    async def create_stock(self, db: DatabaseManager, stock_data: StockResponse) -> StockResponse:
        """Create a new stock"""
        try:
            # Check if stock already exists
            existing = await self.get_stock_by_symbol(db, stock_data.symbol)
            if existing:
                raise ValueError(f"Stock {stock_data.symbol} already exists")
            
            insert_sql = """
                INSERT INTO stocks (symbol, company_name, exchange, is_active)
                VALUES (%s, %s, %s, %s)
                RETURNING id, symbol, company_name, exchange, is_active, created_at, updated_at
            """
            
            params = (
                stock_data.symbol,
                stock_data.company_name,
                stock_data.exchange,
                stock_data.is_active
            )
            
            result = db.execute_query(insert_sql, params, fetch_one=True)
            
            if result:
                return StockResponse(**dict(result))
            else:
                raise ValueError("Failed to create stock")
                
        except Exception as e:
            logger.error(f"Error creating stock: {e}")
            raise
    
    async def update_stock(self, db: DatabaseManager, symbol: str, stock_data: StockResponse) -> Optional[StockResponse]:
        """Update an existing stock"""
        try:
            # Check if stock exists
            existing = await self.get_stock_by_symbol(db, symbol)
            if not existing:
                return None
            
            # Build update query
            update_fields = []
            params = []
            
            if stock_data.company_name is not None:
                update_fields.append("company_name = %s")
                params.append(stock_data.company_name)
            
            if stock_data.exchange is not None:
                update_fields.append("exchange = %s")
                params.append(stock_data.exchange)
            
            if stock_data.is_active is not None:
                update_fields.append("is_active = %s")
                params.append(stock_data.is_active)
            
            if not update_fields:
                return existing  # No changes
            
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            
            update_sql = f"""
                UPDATE stocks 
                SET {', '.join(update_fields)}
                WHERE symbol = %s
                RETURNING id, symbol, company_name, exchange, is_active, created_at, updated_at
            """
            params.append(symbol)
            
            result = db.execute_query(update_sql, tuple(params), fetch_one=True)
            
            if result:
                return StockResponse(**dict(result))
            else:
                raise ValueError("Failed to update stock")
                
        except Exception as e:
            logger.error(f"Error updating stock {symbol}: {e}")
            raise
    
    async def delete_stock(self, db: DatabaseManager, symbol: str) -> bool:
        """Soft delete a stock (sets is_active to False)"""
        try:
            update_sql = """
                UPDATE stocks 
                SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
                WHERE symbol = %s
            """
            
            result = db.execute_query(update_sql, (symbol,))
            return True
            
        except Exception as e:
            logger.error(f"Error deleting stock {symbol}: {e}")
            raise

class PriceService:
    """Service for stock price-related operations"""
    
    async def get_stock_prices(
        self, 
        db: DatabaseManager, 
        symbol: str,
        skip: int = 0, 
        limit: int = 100,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        interval: Optional[str] = None
    ) -> PaginatedResponse[StockPriceResponse]:
        """Get stock prices with pagination and filtering"""
        try:
            # Build query conditions
            conditions = ["stock_symbol = %s"]
            params = [symbol]
            
            if start_date:
                conditions.append("timestamp >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("timestamp <= %s")
                params.append(end_date)
            
            if interval:
                conditions.append("interval = %s")
                params.append(interval)
            
            where_clause = " AND ".join(conditions)
            
            # Get total count
            count_sql = f"SELECT COUNT(*) FROM stock_prices_intraday WHERE {where_clause}"
            total_result = db.execute_query(count_sql, tuple(params), fetch_one=True)
            total = total_result['count'] if total_result else 0
            
            # Get paginated results
            query_sql = f"""
                SELECT id, stock_symbol, timestamp, open_price, high_price, low_price, 
                       close_price, volume, interval, created_at, updated_at
                FROM stock_prices_intraday 
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limit, skip])
            
            results = db.execute_query(query_sql, tuple(params), fetch=True)
            
            # Convert to response models
            prices = [StockPriceResponse(**dict(row)) for row in results] if results else []
            
            return PaginatedResponse(
                items=prices,
                total=total,
                skip=skip,
                limit=limit,
                has_more=(skip + limit) < total
            )
            
        except Exception as e:
            logger.error(f"Error getting stock prices for {symbol}: {e}")
            raise
    
    async def get_latest_price(self, db: DatabaseManager, symbol: str, interval: str = "5min") -> Optional[StockPriceResponse]:
        """Get the latest price for a stock"""
        try:
            query_sql = """
                SELECT id, stock_symbol, timestamp, open_price, high_price, low_price, 
                       close_price, volume, interval, created_at, updated_at
                FROM stock_prices_intraday 
                WHERE stock_symbol = %s AND interval = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """
            
            result = db.execute_query(query_sql, (symbol, interval), fetch_one=True)
            
            if result:
                return StockPriceResponse(**dict(result))
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest price for {symbol}: {e}")
            raise
    
    async def get_all_prices(
        self, 
        db: DatabaseManager, 
        skip: int = 0, 
        limit: int = 100,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        interval: Optional[str] = None,
        symbols: Optional[List[str]] = None
    ) -> PaginatedResponse[StockPriceResponse]:
        """Get all stock prices with pagination and filtering"""
        try:
            # Build query conditions
            conditions = []
            params = []
            
            if start_date:
                conditions.append("timestamp >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("timestamp <= %s")
                params.append(end_date)
            
            if interval:
                conditions.append("interval = %s")
                params.append(interval)
            
            if symbols:
                placeholders = ','.join(['%s'] * len(symbols))
                conditions.append(f"stock_symbol IN ({placeholders})")
                params.extend(symbols)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Get total count
            count_sql = f"SELECT COUNT(*) FROM stock_prices_intraday WHERE {where_clause}"
            total_result = db.execute_query(count_sql, tuple(params), fetch_one=True)
            total = total_result['count'] if total_result else 0
            
            # Get paginated results
            query_sql = f"""
                SELECT id, stock_symbol, timestamp, open_price, high_price, low_price, 
                       close_price, volume, interval, created_at, updated_at
                FROM stock_prices_intraday 
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limit, skip])
            
            results = db.execute_query(query_sql, tuple(params), fetch=True)
            
            # Convert to response models
            prices = [StockPriceResponse(**dict(row)) for row in results] if results else []
            
            return PaginatedResponse(
                items=prices,
                total=total,
                skip=skip,
                limit=limit,
                has_more=(skip + limit) < total
            )
            
        except Exception as e:
            logger.error(f"Error getting all prices: {e}")
            raise
    
    async def get_stock_summary(
        self, 
        db: DatabaseManager, 
        symbol: str, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        interval: str = "5min"
    ) -> Dict[str, Any]:
        """Get stock price summary and statistics"""
        try:
            # Set default date range if not provided
            if not end_date:
                end_date = datetime.utcnow()
            if not start_date:
                start_date = end_date - timedelta(days=30)
            
            # Get price data for the period
            conditions = ["stock_symbol = %s", "interval = %s", "timestamp >= %s", "timestamp <= %s"]
            params = [symbol, interval, start_date, end_date]
            
            query_sql = f"""
                SELECT open_price, high_price, low_price, close_price, volume, timestamp
                FROM stock_prices_intraday 
                WHERE {' AND '.join(conditions)}
                ORDER BY timestamp
            """
            
            results = db.execute_query(query_sql, tuple(params), fetch=True)
            
            if not results:
                return {
                    "symbol": symbol,
                    "interval": interval,
                    "start_date": start_date,
                    "end_date": end_date,
                    "total_records": 0,
                    "price_stats": {},
                    "volume_stats": {},
                    "volatility_stats": {}
                }
            
            # Calculate statistics
            prices = [dict(row) for row in results]
            
            # Price statistics
            all_prices = []
            for price in prices:
                all_prices.extend([
                    price['open_price'], 
                    price['high_price'], 
                    price['low_price'], 
                    price['close_price']
                ])
            
            price_stats = {
                "min": min(all_prices),
                "max": max(all_prices),
                "avg": sum(all_prices) / len(all_prices),
                "latest": prices[-1]['close_price'] if prices else None
            }
            
            # Volume statistics
            volumes = [price['volume'] for price in prices]
            volume_stats = {
                "min": min(volumes),
                "max": max(volumes),
                "avg": sum(volumes) / len(volumes),
                "total": sum(volumes)
            }
            
            # Volatility statistics (price changes)
            price_changes = []
            for i in range(1, len(prices)):
                change = ((prices[i]['close_price'] - prices[i-1]['close_price']) / prices[i-1]['close_price']) * 100
                price_changes.append(change)
            
            # Calculate volatility (standard deviation of price changes)
            if price_changes:
                avg_change = sum(price_changes) / len(price_changes)
                variance = sum([(float(c - avg_change)) ** 2 for c in price_changes]) / len(price_changes)
                volatility = float(variance) ** 0.5
            else:
                avg_change = 0
                volatility = 0
            
            volatility_stats = {
                "avg_change": avg_change,
                "max_gain": max(price_changes) if price_changes else 0,
                "max_loss": min(price_changes) if price_changes else 0,
                "volatility": volatility
            }
            
            return {
                "symbol": symbol,
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date,
                "total_records": len(prices),
                "price_stats": price_stats,
                "volume_stats": volume_stats,
                "volatility_stats": volatility_stats
            }
            
        except Exception as e:
            logger.error(f"Error getting stock summary for {symbol}: {e}")
            raise
    
    async def get_market_overview(
        self, 
        db: DatabaseManager, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        interval: str = "5min"
    ) -> Dict[str, Any]:
        """Get market overview and statistics"""
        try:
            # Set default date range if not provided
            if not end_date:
                end_date = datetime.utcnow()
            if not start_date:
                start_date = end_date - timedelta(days=7)
            
            # Get market statistics
            conditions = ["interval = %s", "timestamp >= %s", "timestamp <= %s"]
            params = [interval, start_date, end_date]
            
            # Count total stocks and price records
            stocks_count_sql = "SELECT COUNT(*) FROM stocks WHERE is_active = TRUE"
            stocks_result = db.execute_query(stocks_count_sql, fetch_one=True)
            total_stocks = stocks_result['count'] if stocks_result else 0
            
            prices_count_sql = f"SELECT COUNT(*) FROM stock_prices_intraday WHERE {' AND '.join(conditions)}"
            prices_result = db.execute_query(prices_count_sql, tuple(params), fetch_one=True)
            total_price_records = prices_result['count'] if prices_result else 0
            
            # Get top gainers and losers
            gainers_sql = f"""
                SELECT stock_symbol, 
                       (MAX(close_price) - MIN(open_price)) / MIN(open_price) * 100 as gain_percent
                FROM stock_prices_intraday 
                WHERE {' AND '.join(conditions)}
                GROUP BY stock_symbol
                HAVING COUNT(*) > 1
                ORDER BY gain_percent DESC
                LIMIT 5
            """
            top_gainers = db.execute_query(gainers_sql, tuple(params), fetch=True)
            
            losers_sql = f"""
                SELECT stock_symbol, 
                       (MIN(close_price) - MAX(open_price)) / MAX(open_price) * 100 as loss_percent
                FROM stock_prices_intraday 
                WHERE {' AND '.join(conditions)}
                GROUP BY stock_symbol
                HAVING COUNT(*) > 1
                ORDER BY loss_percent ASC
                LIMIT 5
            """
            top_losers = db.execute_query(losers_sql, tuple(params), fetch=True)
            
            # Get most active stocks by volume
            most_active_sql = f"""
                SELECT stock_symbol, SUM(volume) as total_volume
                FROM stock_prices_intraday 
                WHERE {' AND '.join(conditions)}
                GROUP BY stock_symbol
                ORDER BY total_volume DESC
                LIMIT 5
            """
            most_active = db.execute_query(most_active_sql, tuple(params), fetch=True)
            
            return {
                "total_stocks": total_stocks,
                "total_price_records": total_price_records,
                "date_range": {
                    "start": start_date,
                    "end": end_date
                },
                "interval": interval,
                "market_stats": {
                    "avg_records_per_stock": total_price_records / total_stocks if total_stocks > 0 else 0
                },
                "top_gainers": [dict(row) for row in top_gainers] if top_gainers else [],
                "top_losers": [dict(row) for row in top_losers] if top_losers else [],
                "most_active": [dict(row) for row in most_active] if most_active else []
            }
            
        except Exception as e:
            logger.error(f"Error getting market overview: {e}")
            raise

class ETLService:
    """Service for ETL job-related operations"""
    
    async def get_etl_jobs(
        self, 
        db: DatabaseManager, 
        skip: int = 0, 
        limit: int = 100,
        status: Optional[str] = None,
        job_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> PaginatedResponse[ETLJobResponse]:
        """Get ETL job logs with pagination and filtering"""
        try:
            # Build query conditions
            conditions = []
            params = []
            
            if status:
                conditions.append("status = %s")
                params.append(status)
            
            if job_name:
                conditions.append("job_name ILIKE %s")
                params.append(f"%{job_name}%")
            
            if start_date:
                conditions.append("start_time >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("start_time <= %s")
                params.append(end_date)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Get total count
            count_sql = f"SELECT COUNT(*) FROM etl_job_logs WHERE {where_clause}"
            total_result = db.execute_query(count_sql, tuple(params), fetch_one=True)
            total = total_result['count'] if total_result else 0
            
            # Get paginated results
            query_sql = f"""
                SELECT id, job_name, status, start_time, end_time, records_processed, 
                       total_records, error_message, created_at
                FROM etl_job_logs 
                WHERE {where_clause}
                ORDER BY start_time DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limit, skip])
            
            results = db.execute_query(query_sql, tuple(params), fetch=True)
            
            # Convert to response models
            jobs = [ETLJobResponse(**dict(row)) for row in results] if results else []
            
            return PaginatedResponse(
                items=jobs,
                total=total,
                skip=skip,
                limit=limit,
                has_more=(skip + limit) < total
            )
            
        except Exception as e:
            logger.error(f"Error getting ETL jobs: {e}")
            raise
    
    async def get_etl_status(self, db: DatabaseManager) -> Dict[str, Any]:
        """Get current ETL system status"""
        try:
            # Get recent job statistics
            status_sql = """
                SELECT 
                    COUNT(*) as total_jobs,
                    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_jobs,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_jobs,
                    COUNT(CASE WHEN status = 'RUNNING' THEN 1 END) as running_jobs,
                    MAX(start_time) as last_run
                FROM etl_job_logs 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            """
            
            result = db.execute_query(status_sql, fetch_one=True)
            
            if result:
                return {
                    'is_running': result['running_jobs'] > 0,
                    'last_run': result['last_run'].isoformat() if result['last_run'] else None,
                    'total_jobs': result['total_jobs'],
                    'successful_jobs': result['successful_jobs'],
                    'failed_jobs': result['failed_jobs'],
                    'running_jobs': result['running_jobs']
                }
            else:
                return {
                    'is_running': False,
                    'last_run': None,
                    'total_jobs': 0,
                    'successful_jobs': 0,
                    'failed_jobs': 0,
                    'running_jobs': 0
                }
                
        except Exception as e:
            logger.error(f"Failed to get ETL status: {e}")
            return {
                'is_running': False,
                'last_run': None,
                'total_jobs': 0,
                'successful_jobs': 0,
                'failed_jobs': 0,
                'running_jobs': 0,
                'error': str(e)
            }
