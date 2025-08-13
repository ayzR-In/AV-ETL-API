#!/usr/bin/env python3
"""
Intraday Pipeline Module for Stock Data Processing
Orchestrates the Extract, Transform, and Load operations
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

from alpha_vantage_intraday.ETL import DataExtractor, DataTransformer, DataLoader
from alpha_vantage_intraday.DB import DatabaseManager

logger = logging.getLogger(__name__)

class ETLService:
    """Main ETL service that orchestrates the ETL pipeline"""
    
    def __init__(self):
        # Initialize database manager
        self.db_manager = DatabaseManager()
        
        # Initialize ETL components
        api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not api_key:
            logger.warning("ALPHA_VANTAGE_API_KEY not found in environment variables")
            api_key = "demo"  # Fallback to demo key for testing
        
        self.extractor = DataExtractor(api_key)
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.db_manager)
    
    def _ensure_db_connection(self):
        """Ensure database connection pool is initialized"""
        if not self.db_manager.connection_pool:
            self.db_manager.init_connection_pool()
    
    def process_stock_intraday(self, symbol: str, interval: str = "5min") -> int:
        """
        Process a single stock through the complete ETL pipeline
        
        Args:
            symbol: Stock symbol to process
            interval: Intraday interval
        
        Returns:
            Number of price records processed
        """
        try:
            logger.info(f"Starting ETL pipeline for {symbol} with {interval} interval")
            
            # Ensure database connection is available
            self._ensure_db_connection()
            
            # Step 1: Extract data from API
            logger.info(f"Step 1: Extracting data for {symbol}")
            raw_data = self.extractor.extract_intraday_data(symbol, interval)
            
            if not raw_data:
                logger.error(f"Extraction failed for {symbol}")
                self._log_etl_job(symbol, 0, 0, 'FAILED', 'Extraction failed')
                return 0
            
            # Step 2: Transform raw data
            logger.info(f"Step 2: Transforming data for {symbol}")
            transformed_data = self.transformer.transform_intraday_data(raw_data)
            
            if not transformed_data:
                logger.error(f"Transformation failed for {symbol}")
                self._log_etl_job(symbol, 0, 0, 'FAILED', 'Transformation failed')
                return 0
            
            # Step 3: Load data into database
            logger.info(f"Step 3: Loading data for {symbol}")
            loading_result = self.loader.load_transformed_data(transformed_data)
            
            if not loading_result.get('success', False):
                logger.error(f"Loading failed for {symbol}: {loading_result.get('error', 'Unknown error')}")
                return 0
            
            records_processed = loading_result.get('prices_loaded', 0)
            logger.info(f"ETL pipeline completed for {symbol}. Records processed: {records_processed}")
            
            return records_processed
            
        except Exception as e:
            logger.error(f"ETL pipeline failed for {symbol}: {e}")
            self._log_etl_job(symbol, 0, 0, 'FAILED', str(e))
            return 0
    
    def process_multiple_stocks_intraday(self, symbols: List[str], interval: str = "5min") -> Dict[str, int]:
        """
        Process multiple stocks through the ETL pipeline
        
        Args:
            symbols: List of stock symbols
            interval: Intraday interval
        
        Returns:
            Dictionary mapping symbols to records processed
        """
        results = {}
        
        logger.info(f"Starting batch ETL processing for {len(symbols)} stocks")
        
        for symbol in symbols:
            try:
                records_processed = self.process_stock_intraday(symbol, interval)
                results[symbol] = records_processed
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
                results[symbol] = 0
        
        total_processed = sum(results.values())
        logger.info(f"Batch ETL processing completed. Total records processed: {total_processed}")
        
        return results
    
    def run_etl_pipeline(self, symbols: List[str], interval: str = "5min") -> Dict[str, Any]:
        """
        Run the complete ETL pipeline with detailed statistics
        
        Args:
            symbols: List of stock symbols
            interval: Intraday interval
        
        Returns:
            Comprehensive ETL pipeline results
        """
        pipeline_start = datetime.utcnow()
        
        logger.info(f"Starting ETL pipeline for {len(symbols)} stocks with {interval} interval")
        
        # Ensure database connection is available
        self._ensure_db_connection()
        
        # Step 1: Extract all data
        logger.info("=== EXTRACTION PHASE ===")
        raw_data_dict = self.extractor.extract_multiple_stocks(symbols, interval)
        
        # Step 2: Transform all data
        logger.info("=== TRANSFORMATION PHASE ===")
        transformed_data_dict = self.transformer.transform_multiple_stocks(raw_data_dict)
        
        # Step 3: Load all data
        logger.info("=== LOADING PHASE ===")
        loading_results = self.loader.load_multiple_stocks(transformed_data_dict)
        
        # Calculate pipeline statistics
        pipeline_end = datetime.utcnow()
        pipeline_duration = pipeline_end - pipeline_start
        
        # Get detailed stats from each phase
        extraction_stats = self._get_extraction_stats(raw_data_dict)
        transformation_stats = self.transformer.get_transformation_stats(transformed_data_dict)
        loading_stats = self.loader.get_loading_stats(loading_results)
        
        pipeline_results = {
            'pipeline_metadata': {
                'start_time': pipeline_start.isoformat(),
                'end_time': pipeline_end.isoformat(),
                'duration_seconds': pipeline_duration.total_seconds(),
                'symbols_processed': len(symbols),
                'interval': interval
            },
            'extraction': extraction_stats,
            'transformation': transformation_stats,
            'loading': loading_stats,
            'overall_success_rate': loading_stats['success_rate'],
            'total_records_processed': loading_stats['total_loaded_records']
        }
        
        logger.info(f"ETL pipeline completed in {pipeline_duration.total_seconds():.2f} seconds")
        logger.info(f"Overall success rate: {pipeline_results['overall_success_rate']:.1f}%")
        logger.info(f"Total records processed: {pipeline_results['total_records_processed']}")
        
        return pipeline_results
    
    def _get_extraction_stats(self, raw_data_dict: Dict[str, Optional[Dict[str, Any]]]) -> Dict[str, Any]:
        """Get statistics about the extraction phase"""
        total_symbols = len(raw_data_dict)
        successful_extractions = sum(1 for data in raw_data_dict.values() if data is not None)
        failed_extractions = total_symbols - successful_extractions
        
        total_data_points = 0
        for data in raw_data_dict.values():
            if data and 'time_series' in data:
                total_data_points += len(data['time_series'])
        
        return {
            'total_symbols': total_symbols,
            'successful_extractions': successful_extractions,
            'failed_extractions': failed_extractions,
            'total_data_points': total_data_points,
            'success_rate': (successful_extractions / total_symbols * 100) if total_symbols > 0 else 0
        }
    
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
                now,
                now,
                processed_records,
                total_records,
                error_message,
                now
            )
            
            self.db_manager.execute_query(log_sql, params)
            
        except Exception as e:
            logger.error(f"Failed to log ETL job: {e}")
    
    def get_etl_status(self) -> Dict[str, Any]:
        """Get current ETL system status"""
        try:
            # Ensure database connection is available
            self._ensure_db_connection()
            
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
            
            result = self.db_manager.execute_query(status_sql, fetch_one=True)
            
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
    
    def cleanup(self):
        """Clean up resources"""
        try:
            self.extractor.close()
            logger.info("ETL service cleanup completed")
        except Exception as e:
            logger.error(f"Error during ETL service cleanup: {e}")
