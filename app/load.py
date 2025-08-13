#!/usr/bin/env python3
"""
Data Loading Module for Stock Data ETL
Handles loading transformed data into PostgreSQL database
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from app.database import DatabaseManager
from app.models import Stock, StockPriceIntraday

logger = logging.getLogger(__name__)

class DataLoader:
    """Loads transformed data into the database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def load_stock(self, stock: Stock) -> bool:
        """
        Load stock information into database
        
        Args:
            stock: Stock object to load
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if stock already exists
            check_sql = "SELECT id FROM stocks WHERE symbol = %s"
            existing_stock = self.db_manager.execute_query(check_sql, (stock.symbol,), fetch_one=True)
            
            if existing_stock:
                logger.info(f"Stock {stock.symbol} already exists, skipping insert")
                return True
            
            # Insert new stock
            insert_sql = """
                INSERT INTO stocks (symbol, name, exchange, is_active, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
            """
            
            params = (
                stock.symbol,
                stock.name,
                stock.exchange,
                stock.is_active,
                stock.created_at
            )
            
            self.db_manager.execute_query(insert_sql, params)
            logger.info(f"Successfully loaded stock {stock.symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load stock {stock.symbol}: {e}")
            return False
    
    def load_intraday_prices(self, symbol: str, price_records: List[StockPriceIntraday]) -> int:
        """
        Load intraday price records into database
        
        Args:
            symbol: Stock symbol
            price_records: List of StockPriceIntraday objects
        
        Returns:
            Number of records successfully loaded
        """
        if not price_records:
            logger.warning(f"No price records to load for {symbol}")
            return 0
        
        try:
            loaded_count = 0
            
            for price_record in price_records:
                if self._load_single_price_record(price_record):
                    loaded_count += 1
            
            logger.info(f"Successfully loaded {loaded_count}/{len(price_records)} price records for {symbol}")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Failed to load price records for {symbol}: {e}")
            return 0
    
    def _load_single_price_record(self, price_record: StockPriceIntraday) -> bool:
        """Load a single price record into the database"""
        try:
            # Check for duplicate (same symbol, timestamp, and interval)
            check_sql = """
                SELECT id FROM stock_prices_intraday 
                WHERE stock_symbol = %s AND timestamp = %s AND interval = %s
            """
            
            params = (price_record.stock_symbol, price_record.timestamp, price_record.interval)
            existing_record = self.db_manager.execute_query(check_sql, params, fetch_one=True)
            
            if existing_record:
                # Update existing record
                update_sql = """
                    UPDATE stock_prices_intraday 
                    SET open_price = %s, high_price = %s, low_price = %s, 
                        close_price = %s, volume = %s, updated_at = %s
                    WHERE stock_symbol = %s AND timestamp = %s AND interval = %s
                """
                
                update_params = (
                    price_record.open_price,
                    price_record.high_price,
                    price_record.low_price,
                    price_record.close_price,
                    price_record.volume,
                    datetime.utcnow(),
                    price_record.stock_symbol,
                    price_record.timestamp,
                    price_record.interval
                )
                
                self.db_manager.execute_query(update_sql, update_params)
                logger.debug(f"Updated existing price record for {price_record.stock_symbol} at {price_record.timestamp}")
                
            else:
                # Insert new record
                insert_sql = """
                    INSERT INTO stock_prices_intraday 
                    (stock_symbol, timestamp, open_price, high_price, low_price, 
                     close_price, volume, interval, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                insert_params = (
                    price_record.stock_symbol,
                    price_record.timestamp,
                    price_record.open_price,
                    price_record.high_price,
                    price_record.low_price,
                    price_record.close_price,
                    price_record.volume,
                    price_record.interval,
                    datetime.utcnow()
                )
                
                self.db_manager.execute_query(insert_sql, insert_params)
                logger.debug(f"Inserted new price record for {price_record.stock_symbol} at {price_record.timestamp}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load price record for {price_record.stock_symbol} at {price_record.timestamp}: {e}")
            return False
    
    def load_transformed_data(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load complete transformed data (stock + prices) into database
        
        Args:
            transformed_data: Transformed data from DataTransformer
        
        Returns:
            Loading results with counts and status
        """
        if not transformed_data:
            logger.error("No transformed data to load")
            return {'success': False, 'error': 'No transformed data'}
        
        try:
            symbol = transformed_data['metadata']['symbol']
            stock = transformed_data['stock']
            price_records = transformed_data['price_records']
            
            logger.info(f"Loading transformed data for {symbol}")
            
            # Load stock information
            stock_loaded = self.load_stock(stock)
            if not stock_loaded:
                return {
                    'success': False,
                    'symbol': symbol,
                    'error': 'Failed to load stock information'
                }
            
            # Load price records
            prices_loaded = self.load_intraday_prices(symbol, price_records)
            
            # Log ETL job
            self._log_etl_job(symbol, len(price_records), prices_loaded, 'SUCCESS')
            
            return {
                'success': True,
                'symbol': symbol,
                'stock_loaded': stock_loaded,
                'prices_loaded': prices_loaded,
                'total_price_records': len(price_records),
                'loading_timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to load transformed data: {e}")
            
            # Log failed ETL job
            symbol = transformed_data.get('metadata', {}).get('symbol', 'unknown')
            self._log_etl_job(symbol, 0, 0, 'FAILED', str(e))
            
            return {
                'success': False,
                'symbol': symbol,
                'error': str(e)
            }
    
    def load_multiple_stocks(self, transformed_data_dict: Dict[str, Optional[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
        """
        Load data for multiple stocks
        
        Args:
            transformed_data_dict: Dictionary mapping symbols to transformed data
        
        Returns:
            Dictionary mapping symbols to loading results
        """
        loading_results = {}
        
        logger.info(f"Starting data loading for {len(transformed_data_dict)} stocks")
        
        for symbol, transformed_data in transformed_data_dict.items():
            if transformed_data is None:
                logger.warning(f"Skipping loading for {symbol} - no transformed data")
                loading_results[symbol] = {
                    'success': False,
                    'error': 'No transformed data available'
                }
                continue
            
            logger.info(f"Loading data for {symbol}...")
            result = self.load_transformed_data(transformed_data)
            loading_results[symbol] = result
        
        successful_loads = sum(1 for result in loading_results.values() if result.get('success', False))
        logger.info(f"Loading completed. {successful_loads}/{len(transformed_data_dict)} stocks successful")
        
        return loading_results
    
    def _log_etl_job(self, symbol: str, total_records: int, processed_records: int, 
                     status: str, error_message: str = None):
        """Log ETL job execution"""
        try:
            log_sql = """
                INSERT INTO etl_job_logs 
                (job_name, status, start_time, end_time, records_processed, 
                 total_records, error_message, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            now = datetime.utcnow()
            params = (
                f"intraday_etl_{symbol}",
                status,
                now,  # Using same time for start/end in this case
                now,
                processed_records,
                total_records,
                error_message,
                now
            )
            
            self.db_manager.execute_query(log_sql, params)
            
        except Exception as e:
            logger.error(f"Failed to log ETL job: {e}")
    
    def get_loading_stats(self, loading_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Get statistics about the loading process"""
        total_symbols = len(loading_results)
        successful_symbols = sum(1 for result in loading_results.values() if result.get('success', False))
        failed_symbols = total_symbols - successful_symbols
        
        total_price_records = 0
        total_loaded_records = 0
        
        for result in loading_results.values():
            if result.get('success', False):
                total_price_records += result.get('total_price_records', 0)
                total_loaded_records += result.get('prices_loaded', 0)
        
        return {
            'total_symbols': total_symbols,
            'successful_symbols': successful_symbols,
            'failed_symbols': failed_symbols,
            'total_price_records': total_price_records,
            'total_loaded_records': total_loaded_records,
            'success_rate': (successful_symbols / total_symbols * 100) if total_symbols > 0 else 0,
            'loading_efficiency': (total_loaded_records / total_price_records * 100) if total_price_records > 0 else 0
        }
