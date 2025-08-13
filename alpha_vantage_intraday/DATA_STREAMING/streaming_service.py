#!/usr/bin/env python3
"""
Data Streaming Service for Real-time Stock Data Ingestion
Simulates real-time ingestion through periodic polling
"""

import time
import logging
import signal
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Callable
from threading import Thread, Event

from alpha_vantage_intraday.intraday_pipeline import ETLService

logger = logging.getLogger(__name__)

class DataStreamingService:
    """Main service for real-time data streaming simulation"""
    
    def __init__(self):
        self.etl_service = ETLService()
        self.is_running = False
        self.stop_event = Event()
        self.streaming_thread = None
        

        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop_streaming()
        sys.exit(0)
    
    def start_streaming(self, 
                       symbols: Optional[List[str]] = None,
                       interval_minutes: int = 5,
                       max_iterations: Optional[int] = None,
                       callback: Optional[Callable] = None) -> bool:
        """
        Start real-time data streaming simulation
        
        Args:
            symbols: List of stock symbols to stream (default: popular symbols)
            interval_minutes: Polling interval in minutes
            max_iterations: Maximum number of polling cycles (None for infinite)
            callback: Optional callback function after each cycle
        
        Returns:
            True if streaming started successfully
        """
        if self.is_running:
            logger.warning("Streaming service is already running")
            return False
        
        if symbols is None:
            symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Default symbols for streaming
        
        self.is_running = True
        self.stop_event.clear()
        
        # Start streaming in a separate thread
        self.streaming_thread = Thread(
            target=self._streaming_worker,
            args=(symbols, interval_minutes, max_iterations, callback),
            daemon=True
        )
        self.streaming_thread.start()
        
        logger.info(f"Started data streaming for {len(symbols)} symbols every {interval_minutes} minutes")
        return True
    
    def _streaming_worker(self, 
                         symbols: List[str], 
                         interval_minutes: int, 
                         max_iterations: Optional[int],
                         callback: Optional[Callable]):
        """Worker thread for data streaming"""
        iteration = 0
        
        try:
            while not self.stop_event.is_set():
                if max_iterations and iteration >= max_iterations:
                    logger.info(f"Reached maximum iterations ({max_iterations}), stopping streaming")
                    break
                
                start_time = datetime.utcnow()
                iteration += 1
                
                logger.info(f"=== Streaming Cycle {iteration} at {start_time} ===")
                
                # Process all symbols in the current cycle
                try:
                    results = self.etl_service.process_multiple_stocks_intraday(symbols, "5min")
                    total_processed = sum(results.values())
                    
                    cycle_time = datetime.utcnow() - start_time
                    logger.info(f"Cycle {iteration} completed in {cycle_time}. Records processed: {total_processed}")
                    
                    # Log individual results
                    for symbol, count in results.items():
                        logger.info(f"  {symbol}: {count} records")
                    
                    # Execute callback if provided
                    if callback:
                        try:
                            callback(iteration, results, cycle_time)
                        except Exception as e:
                            logger.error(f"Callback execution failed: {e}")
                    
                except Exception as e:
                    logger.error(f"Error in streaming cycle {iteration}: {e}")
                
                # Calculate next run time
                next_run = start_time + timedelta(minutes=interval_minutes)
                logger.info(f"Next streaming cycle scheduled for: {next_run}")
                
                # Wait for next cycle (with early termination check)
                wait_seconds = interval_minutes * 60
                for _ in range(wait_seconds):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)
                
        except Exception as e:
            logger.error(f"Unexpected error in streaming worker: {e}")
        finally:
            self.is_running = False
            logger.info("Streaming worker stopped")
    
    def stop_streaming(self) -> bool:
        """Stop the streaming service"""
        if not self.is_running:
            logger.info("Streaming service is not running")
            return True
        
        logger.info("Stopping streaming service...")
        self.stop_event.set()
        self.is_running = False
        
        # Wait for streaming thread to finish
        if self.streaming_thread and self.streaming_thread.is_alive():
            self.streaming_thread.join(timeout=10)
        
        logger.info("Streaming service stopped")
        return True
    
    def get_streaming_status(self) -> Dict[str, Any]:
        """Get current streaming service status"""
        return {
            'is_running': self.is_running,
            'stop_event_set': self.stop_event.is_set(),
            'thread_alive': self.streaming_thread.is_alive() if self.streaming_thread else False,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def run_single_cycle(self, 
                         symbols: Optional[List[str]] = None,
                         callback: Optional[Callable] = None) -> Dict[str, int]:
        """Run a single streaming cycle"""
        if symbols is None:
            symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Default symbols for single cycle
        
        logger.info(f"Running single streaming cycle for {len(symbols)} symbols")
        
        start_time = datetime.utcnow()
        results = self.etl_service.process_multiple_stocks_intraday(symbols, "5min")
        cycle_time = datetime.utcnow() - start_time
        
        total_processed = sum(results.values())
        logger.info(f"Single cycle completed in {cycle_time}. Records processed: {total_processed}")
        
        # Execute callback if provided
        if callback:
            try:
                callback(1, results, cycle_time)
            except Exception as e:
                logger.error(f"Callback execution failed: {e}")
        
        return results
    
    def cleanup(self):
        """Clean up resources"""
        try:
            self.stop_streaming()
            self.etl_service.cleanup()
            logger.info("Data streaming service cleanup completed")
        except Exception as e:
            logger.error(f"Error during streaming service cleanup: {e}")
