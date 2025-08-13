#!/usr/bin/env python3
"""
Data Extraction Module for Stock Data ETL
Handles API calls to Alpha Vantage for intraday stock data
"""

import requests
import logging
import time
from typing import Dict, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class DataExtractor:
    """Extracts stock data from Alpha Vantage API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.session = requests.Session()
        
        # Rate limiting: Alpha Vantage allows 5 API calls per minute for free tier
        self.rate_limit_delay = 12  # seconds between calls (60/5 = 12)
        self.last_call_time = 0
    
    def _rate_limit(self):
        """Implement rate limiting for API calls"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_call
            logger.info(f"Rate limiting: waiting {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()
    
    def extract_intraday_data(self, symbol: str, interval: str = "5min") -> Optional[Dict[str, Any]]:
        """
        Extract intraday stock data from Alpha Vantage API
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            interval: Time interval ('1min', '5min', '15min', '30min', '60min')
        
        Returns:
            Dictionary containing intraday data or None if failed
        """
        try:
            self._rate_limit()
            
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': symbol.upper(),
                'interval': interval,
                'apikey': self.api_key,
                'outputsize': 'compact'  # Get last 100 data points
            }
            
            logger.info(f"Extracting intraday data for {symbol} with {interval} interval")
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"API Error for {symbol}: {data['Error Message']}")
                return None
            
            if 'Note' in data:
                logger.warning(f"API Rate Limit Note for {symbol}: {data['Note']}")
                return None
            
            # Check if we have time series data
            time_series_key = f'Time Series ({interval})'
            if time_series_key not in data:
                logger.error(f"No time series data found for {symbol}. Response: {data}")
                return None
            
            time_series_data = data[time_series_key]
            
            if not time_series_data:
                logger.warning(f"No intraday data available for {symbol}")
                return None
            
            logger.info(f"Successfully extracted {len(time_series_data)} data points for {symbol}")
            
            return {
                'symbol': symbol.upper(),
                'interval': interval,
                'time_series': time_series_data,
                'metadata': {
                    'last_refreshed': data.get('Meta Data', {}).get('3. Last Refreshed', ''),
                    'timezone': data.get('Meta Data', {}).get('6. Time Zone', ''),
                    'extraction_timestamp': datetime.utcnow().isoformat()
                }
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol}: {e}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {symbol}")
            return None
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Invalid JSON response for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error extracting data for {symbol}: {e}")
            return None
    
    def extract_multiple_stocks(self, symbols: list, interval: str = "5min") -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Extract intraday data for multiple stocks
        
        Args:
            symbols: List of stock symbols
            interval: Time interval for data
        
        Returns:
            Dictionary mapping symbols to their extracted data
        """
        results = {}
        
        logger.info(f"Starting extraction for {len(symbols)} stocks with {interval} interval")
        
        for symbol in symbols:
            logger.info(f"Processing {symbol}...")
            data = self.extract_intraday_data(symbol, interval)
            results[symbol] = data
            
            # Add small delay between different symbols to be respectful to API
            if len(symbols) > 1:
                time.sleep(1)
        
        successful_extractions = sum(1 for data in results.values() if data is not None)
        logger.info(f"Extraction completed. {successful_extractions}/{len(symbols)} stocks successful")
        
        return results
    
    def get_api_status(self) -> Dict[str, Any]:
        """Get API connection status and rate limit info"""
        return {
            'api_key_configured': bool(self.api_key),
            'base_url': self.base_url,
            'rate_limit_delay': self.rate_limit_delay,
            'last_call_time': self.last_call_time,
            'session_active': self.session is not None
        }
    
    def close(self):
        """Close the session"""
        if self.session:
            self.session.close()
            logger.info("Data extractor session closed")
