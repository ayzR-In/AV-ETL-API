#!/usr/bin/env python3
"""
ETL Pipeline Trigger for Stock Data Intraday Processing
Main entry point for triggering ETL operations and real-time data streaming
Simulates "real-time" ingestion through periodic polling
"""

import logging
import signal
import sys
import os
from datetime import datetime
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv

from alpha_vantage_intraday.DB import init_db
from alpha_vantage_intraday.intraday_pipeline import ETLService
from alpha_vantage_intraday.STREAM_N_POLLING import DataStreamingService, PollingManager

# Load environment variables
load_dotenv()

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class ETLRunner:
    """ETL Pipeline Trigger for periodic intraday data processing and real-time streaming"""
    
    def __init__(self):
        self.is_running = False
        self.etl_service = ETLService()
        
        # Load configuration from environment variables
        self.default_interval = os.getenv('DEFAULT_INTERVAL', '5min')
        self.default_polling_interval = int(os.getenv('DEFAULT_POLLING_INTERVAL', '5'))
        self.batch_size = int(os.getenv('BATCH_SIZE', '10'))
        
        # Initialize streaming services
        self.streaming_service = DataStreamingService()
        self.polling_manager = PollingManager()
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)
    
    def process_single_stock(self, symbol: str, interval: str = None) -> int:
        if interval is None:
            interval = self.default_interval
        """Process a single stock for intraday data"""
        try:
            logger.info(f"Processing intraday data for {symbol} with {interval} interval")
            records_processed = self.etl_service.process_stock_intraday(symbol, interval)
            logger.info(f"Completed processing {symbol}. Records processed: {records_processed}")
            return records_processed
        except Exception as e:
            logger.error(f"Failed to process {symbol}: {e}")
            return 0
    
    def process_batch_stocks(self, symbols: List[str], interval: str = None) -> Dict[str, int]:
        if interval is None:
            interval = self.default_interval
        """Process multiple stocks for intraday data"""
        try:
            logger.info(f"Processing batch intraday data for {len(symbols)} stocks with {interval} interval")
            results = self.etl_service.process_multiple_stocks_intraday(symbols, interval)
            
            total_processed = sum(results.values())
            logger.info(f"Batch processing completed. Total records processed: {total_processed}")
            
            # Log individual results
            for symbol, count in results.items():
                logger.info(f"  {symbol}: {count}")
            
            return results
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return {}
    
    def run_etl_pipeline(self, symbols: List[str], interval: str = None) -> Dict[str, Any]:
        if interval is None:
            interval = self.default_interval
        """Run the complete ETL pipeline with detailed statistics"""
        try:
            logger.info(f"Running complete ETL pipeline for {len(symbols)} stocks with {interval} interval")
            pipeline_results = self.etl_service.run_etl_pipeline(symbols, interval)
            
            logger.info(f"ETL pipeline completed successfully!")
            logger.info(f"Overall success rate: {pipeline_results['overall_success_rate']:.1f}%")
            logger.info(f"Total records processed: {pipeline_results['total_records_processed']}")
            
            return pipeline_results
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return {}
    

    
    # start_periodic_polling method removed - replaced by streaming functionality
    
    def run_single_cycle(self, symbols: Optional[List[str]] = None, interval: str = None):
        if interval is None:
            interval = self.default_interval
        """Run a single ETL cycle"""
        if symbols is None:
            symbols = ["AAPL", "MSFT", "GOOGL"]  # Default symbols for single cycle
        
        logger.info(f"Running single ETL cycle for {len(symbols)} symbols")
        return self.process_batch_stocks(symbols, interval)
    
    def show_status(self):
        """Show current ETL system status"""
        try:
            status = self.etl_service.get_etl_status()
            
            print("\n=== Intraday ETL System Status ===")
            print(f"Running: {status['is_running']}")
            print(f"Last Run: {status['last_run']}")
            print(f"Total Jobs (7 days): {status['total_jobs']}")
            print(f"Successful: {status['successful_jobs']}")
            print(f"Failed: {status['failed_jobs']}")
            print(f"Running: {status['running_jobs']}")
            
            return status
        except Exception as e:
            logger.error(f"Failed to get ETL status: {e}")
            return None
    
    def stop(self):
        """Stop the ETL runner"""
        self.is_running = False
        logger.info("ETL runner stopped")
    
    def start_streaming(self, 
                       symbols: Optional[List[str]] = None,
                       interval_minutes: int = 5,
                       max_iterations: Optional[int] = None) -> bool:
        """Start real-time data streaming"""
        try:
            if symbols is None:
                symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Default symbols for streaming
            
            logger.info(f"Starting real-time data streaming for {len(symbols)} symbols every {interval_minutes} minutes")
            
            success = self.streaming_service.start_streaming(
                symbols=symbols,
                interval_minutes=interval_minutes,
                max_iterations=max_iterations
            )
            
            if success:
                logger.info("Data streaming started successfully")
                return True
            else:
                logger.error("Failed to start data streaming")
                return False
                
        except Exception as e:
            logger.error(f"Error starting streaming: {e}")
            return False
    
    def stop_streaming(self) -> bool:
        """Stop real-time data streaming"""
        try:
            success = self.streaming_service.stop_streaming()
            if success:
                logger.info("Data streaming stopped successfully")
                return True
            else:
                logger.error("Failed to stop data streaming")
                return False
        except Exception as e:
            logger.error(f"Error stopping streaming: {e}")
            return False
    
    def get_streaming_status(self) -> Dict[str, Any]:
        """Get current streaming service status"""
        try:
            return self.streaming_service.get_streaming_status()
        except Exception as e:
            logger.error(f"Error getting streaming status: {e}")
            # Return a basic status even if database is not available
            return {
                'is_running': False,
                'stop_event_set': True,
                'thread_alive': False,
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }
    
    def start_continuous_polling(self, 
                               interval_minutes: int = 5,
                               symbols: Optional[List[str]] = None,
                               max_iterations: Optional[int] = None) -> Dict[str, Any]:
        """Start continuous polling with the polling manager"""
        try:
            if symbols is None:
                symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Default symbols for continuous polling
            
            logger.info(f"Starting continuous polling for {len(symbols)} symbols every {interval_minutes} minutes")
            
            from alpha_vantage_intraday.STREAM_N_POLLING.polling_manager import PollingConfig
            
            config = PollingConfig(
                interval_minutes=interval_minutes,
                max_iterations=max_iterations,
                symbols=symbols,
                interval="5min",
                batch_size=10
            )
            
            results = self.polling_manager.continuous_polling(config)
            return results
            
        except Exception as e:
            logger.error(f"Error in continuous polling: {e}")
            return {}
    
    def start_market_hours_polling(self, 
                                 interval_minutes: int = 5,
                                 symbols: Optional[List[str]] = None,
                                 max_iterations: Optional[int] = None) -> Dict[str, Any]:
        """Start market hours polling"""
        try:
            if symbols is None:
                symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]  # Default symbols for market hours polling
            
            logger.info(f"Starting market hours polling for {len(symbols)} symbols every {interval_minutes} minutes")
            
            from alpha_vantage_intraday.STREAM_N_POLLING.polling_manager import PollingConfig
            
            config = PollingConfig(
                interval_minutes=interval_minutes,
                max_iterations=max_iterations,
                symbols=symbols,
                interval="5min",
                batch_size=10
            )
            
            results = self.polling_manager.market_hours_polling(config)
            return results
            
        except Exception as e:
            logger.error(f"Error in market hours polling: {e}")
            return {}
    

    


def main():
    """Main function for triggering ETL pipeline operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Intraday Stock Data ETL Pipeline Trigger')
    parser.add_argument('--init-db', action='store_true', help='Initialize database tables')
    parser.add_argument('--single', type=str, help='Process single stock intraday data by symbol')
    parser.add_argument('--batch', nargs='+', help='Process multiple stocks intraday data by symbols')

    parser.add_argument('--interval', type=str, default=None, 
                       choices=['1min', '5min', '15min', '30min', '60min'],
                       help='Intraday interval (default: from environment or 5min)')
    parser.add_argument('--status', action='store_true', help='Show ETL system status')
    # --poll and --poll-interval removed - replaced by streaming functionality
    parser.add_argument('--cycle', action='store_true', help='Run a single ETL cycle')
    parser.add_argument('--pipeline', action='store_true', help='Run complete ETL pipeline with detailed stats')
    
    # Streaming and advanced polling arguments
    parser.add_argument('--stream', action='store_true', help='Start real-time data streaming')
    parser.add_argument('--stream-interval', type=int, default=None, 
                       help='Streaming interval in minutes (default: from environment or 5)')
    parser.add_argument('--stream-max-iterations', type=int, 
                       help='Maximum streaming iterations (default: infinite)')
    parser.add_argument('--continuous-poll', action='store_true', help='Start continuous polling')
    parser.add_argument('--market-hours-poll', action='store_true', help='Start market hours polling')

    parser.add_argument('--poll-max-iterations', type=int, 
                       help='Maximum polling iterations (default: infinite)')
    

    
    # Status and control arguments
    parser.add_argument('--streaming-status', action='store_true', help='Show streaming service status')
    parser.add_argument('--stop-streaming', action='store_true', help='Stop real-time streaming')
    
    args = parser.parse_args()
    
    try:
        if args.init_db:
            logger.info("Initializing database...")
            init_db()
            logger.info("Database initialized successfully!")
            return
        
        runner = ETLRunner()
        
        if args.single:
            runner.process_single_stock(args.single, args.interval)
            return
        
        if args.batch:
            if args.pipeline:
                runner.run_etl_pipeline(args.batch, args.interval)
            else:
                runner.process_batch_stocks(args.batch, args.interval)
            return
        

        
        if args.status:
            runner.show_status()
            return
        
        if args.cycle:
            runner.run_single_cycle(interval=args.interval)
            return
        
        # Old --poll argument handling removed - replaced by streaming functionality
        
        # Handle streaming and advanced polling
        if args.stream:
            stream_interval = args.stream_interval or runner.default_polling_interval
            logger.info(f"Starting real-time data streaming every {stream_interval} minutes...")
            runner.start_streaming(
                interval_minutes=stream_interval,
                max_iterations=args.stream_max_iterations
            )
            return
        
        if args.continuous_poll:
            logger.info(f"Starting continuous polling every {args.poll_interval} minutes...")
            runner.start_continuous_polling(
                interval_minutes=args.poll_interval,
                max_iterations=args.poll_max_iterations
            )
            return
        
        if args.market_hours_poll:
            logger.info(f"Starting market hours polling every {args.poll_interval} minutes...")
            runner.start_market_hours_polling(
                interval_minutes=args.poll_interval,
                max_iterations=args.poll_max_iterations
            )
            return
        

        

        
        # Handle status and control
        if args.streaming_status:
            status = runner.get_streaming_status()
            print("\n=== Streaming Service Status ===")
            print(f"Running: {status.get('is_running', 'Unknown')}")
            print(f"Stop Event Set: {status.get('stop_event_set', 'Unknown')}")
            print(f"Thread Alive: {status.get('thread_alive', 'Unknown')}")
            print(f"Timestamp: {status.get('timestamp', 'Unknown')}")
            return
        
        if args.stop_streaming:
            logger.info("Stopping real-time streaming...")
            success = runner.stop_streaming()
            if success:
                logger.info("Streaming stopped successfully")
            else:
                logger.error("Failed to stop streaming")
            return
        
        # If no arguments provided, show help
        if len(sys.argv) == 1:
            parser.print_help()
            return
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
