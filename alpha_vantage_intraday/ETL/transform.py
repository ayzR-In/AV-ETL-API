#!/usr/bin/env python3
"""
Data Transformation Module for Stock Data ETL
Transforms raw API data into structured internal data models
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from alpha_vantage_intraday.MODELS import Stock, StockPriceIntraday

logger = logging.getLogger(__name__)

class DataTransformer:
    """Transforms raw API data into structured data models"""
    
    def __init__(self):
        self.supported_intervals = ['1min', '5min', '15min', '30min', '60min']
    
    def transform_intraday_data(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform raw intraday data from Alpha Vantage API
        
        Args:
            raw_data: Raw data from DataExtractor
        
        Returns:
            Transformed data with Stock and StockPriceIntraday objects
        """
        if not raw_data or 'time_series' not in raw_data:
            logger.error("Invalid raw data for transformation")
            return None
        
        try:
            symbol = raw_data['symbol']
            interval = raw_data['interval']
            time_series = raw_data['time_series']
            metadata = raw_data.get('metadata', {})
            
            logger.info(f"Transforming intraday data for {symbol} with {interval} interval")
            
            # Transform stock information
            stock = self._create_stock_object(symbol, metadata)
            
            # Transform price data
            price_records = self._create_price_objects(symbol, time_series, interval)
            
            if not price_records:
                logger.warning(f"No valid price records found for {symbol}")
                return None
            
            logger.info(f"Successfully transformed {len(price_records)} price records for {symbol}")
            
            return {
                'stock': stock,
                'price_records': price_records,
                'metadata': {
                    'symbol': symbol,
                    'interval': interval,
                    'record_count': len(price_records),
                    'transformation_timestamp': datetime.utcnow().isoformat(),
                    'api_metadata': metadata
                }
            }
            
        except Exception as e:
            logger.error(f"Error transforming data for {raw_data.get('symbol', 'unknown')}: {e}")
            return None
    
    def _create_stock_object(self, symbol: str, metadata: Dict[str, Any]) -> Stock:
        """Create a Stock object from symbol and metadata"""
        return Stock(
            symbol=symbol,
            company_name=f"{symbol} Stock",  # Default company name, could be enhanced with company info API
            exchange="NYSE",  # Default exchange, could be enhanced with lookup
            is_active=True
        )
    
    def _create_price_objects(self, symbol: str, time_series: Dict[str, Any], interval: str) -> List[StockPriceIntraday]:
        """Create StockPriceIntraday objects from time series data"""
        price_records = []
        
        for timestamp_str, price_data in time_series.items():
            try:
                # Parse timestamp (Alpha Vantage format: "2024-01-15 16:00:00")
                timestamp = self._parse_timestamp(timestamp_str)
                
                # Extract OHLCV data
                open_price = float(price_data.get('1. open', 0))
                high_price = float(price_data.get('2. high', 0))
                low_price = float(price_data.get('3. low', 0))
                close_price = float(price_data.get('4. close', 0))
                volume = int(price_data.get('5. volume', 0))
                
                # Validate data
                if not self._validate_price_data(open_price, high_price, low_price, close_price, volume):
                    logger.warning(f"Invalid price data for {symbol} at {timestamp_str}, skipping")
                    continue
                
                # Create price record
                price_record = StockPriceIntraday(
                    stock_symbol=symbol,
                    timestamp=timestamp,
                    open_price=open_price,
                    high_price=high_price,
                    low_price=low_price,
                    close_price=close_price,
                    volume=volume,
                    interval=interval
                )
                
                price_records.append(price_record)
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Error parsing price data for {symbol} at {timestamp_str}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error creating price object for {symbol} at {timestamp_str}: {e}")
                continue
        
        return price_records
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse Alpha Vantage timestamp string to datetime object"""
        try:
            # Handle different timestamp formats
            timestamp_formats = [
                "%Y-%m-%d %H:%M:%S",  # "2024-01-15 16:00:00"
                "%Y-%m-%d %H:%M",     # "2024-01-15 16:00"
                "%Y-%m-%d"            # "2024-01-15"
            ]
            
            for fmt in timestamp_formats:
                try:
                    return datetime.strptime(timestamp_str, fmt)
                except ValueError:
                    continue
            
            # If no format matches, try parsing with dateutil (more flexible)
            from dateutil import parser
            return parser.parse(timestamp_str)
            
        except Exception as e:
            logger.error(f"Failed to parse timestamp '{timestamp_str}': {e}")
            raise ValueError(f"Invalid timestamp format: {timestamp_str}")
    
    def _validate_price_data(self, open_price: float, high_price: float, 
                           low_price: float, close_price: float, volume: int) -> bool:
        """Validate OHLCV data for consistency"""
        # Check for positive values
        if any(price <= 0 for price in [open_price, high_price, low_price, close_price]):
            return False
        
        if volume < 0:
            return False
        
        # Check OHLC relationships
        if high_price < max(open_price, close_price):
            return False
        
        if low_price > min(open_price, close_price):
            return False
        
        # Check for reasonable price ranges (e.g., no prices over $1M)
        if any(price > 1000000 for price in [open_price, high_price, low_price, close_price]):
            return False
        
        return True
    
    def transform_multiple_stocks(self, raw_data_dict: Dict[str, Optional[Dict[str, Any]]]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Transform data for multiple stocks
        
        Args:
            raw_data_dict: Dictionary mapping symbols to raw data
        
        Returns:
            Dictionary mapping symbols to transformed data
        """
        transformed_results = {}
        
        logger.info(f"Starting transformation for {len(raw_data_dict)} stocks")
        
        for symbol, raw_data in raw_data_dict.items():
            if raw_data is None:
                logger.warning(f"Skipping transformation for {symbol} - no raw data")
                transformed_results[symbol] = None
                continue
            
            logger.info(f"Transforming data for {symbol}...")
            transformed_data = self.transform_intraday_data(raw_data)
            transformed_results[symbol] = transformed_data
        
        successful_transformations = sum(1 for data in transformed_results.values() if data is not None)
        logger.info(f"Transformation completed. {successful_transformations}/{len(raw_data_dict)} stocks successful")
        
        return transformed_results
    
    def get_transformation_stats(self, transformed_data: Dict[str, Optional[Dict[str, Any]]]) -> Dict[str, Any]:
        """Get statistics about the transformation process"""
        total_symbols = len(transformed_data)
        successful_symbols = sum(1 for data in transformed_data.values() if data is not None)
        failed_symbols = total_symbols - successful_symbols
        
        total_records = 0
        for data in transformed_data.values():
            if data and 'price_records' in data:
                total_records += len(data['price_records'])
        
        return {
            'total_symbols': total_symbols,
            'successful_symbols': successful_symbols,
            'failed_symbols': failed_symbols,
            'total_price_records': total_records,
            'success_rate': (successful_symbols / total_symbols * 100) if total_symbols > 0 else 0
        }
