#!/usr/bin/env python3
"""
Polling Manager for Data Ingestion
Manages different polling strategies and configurations
"""

import logging
import time
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from alpha_vantage_intraday.intraday_pipeline import ETLService

logger = logging.getLogger(__name__)

@dataclass
class PollingConfig:
    """Configuration for polling operations"""
    interval_minutes: int = 5
    max_iterations: Optional[int] = None
    symbols: Optional[List[str]] = None
    interval: str = "5min"
    batch_size: int = 10
    retry_on_failure: bool = True
    max_retries: int = 3

class PollingManager:
    """Manages different polling strategies for data ingestion"""
    
    def __init__(self):
        self.etl_service = ETLService()

    
    def continuous_polling(self, config: PollingConfig) -> Dict[str, Any]:
        """
        Start continuous polling with the specified configuration
        
        Args:
            config: Polling configuration
        
        Returns:
            Polling results and statistics
        """
        logger.info(f"Starting continuous polling with interval: {config.interval_minutes} minutes")
        
        if config.symbols is None:
            config.symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Default symbols for continuous polling
        
        iteration = 0
        total_records = 0
        successful_cycles = 0
        failed_cycles = 0
        
        start_time = datetime.utcnow()
        
        try:
            while True:
                if config.max_iterations and iteration >= config.max_iterations:
                    logger.info(f"Reached maximum iterations ({config.max_iterations})")
                    break
                
                iteration += 1
                cycle_start = datetime.utcnow()
                
                logger.info(f"=== Polling Cycle {iteration} at {cycle_start} ===")
                
                # Process symbols in batches
                batch_results = self._process_symbols_in_batches(
                    config.symbols, 
                    config.interval, 
                    config.batch_size
                )
                
                # Calculate cycle statistics
                cycle_records = sum(batch_results.values())
                total_records += cycle_records
                cycle_time = datetime.utcnow() - cycle_start
                
                if cycle_records > 0:
                    successful_cycles += 1
                    logger.info(f"Cycle {iteration} successful: {cycle_records} records in {cycle_time}")
                else:
                    failed_cycles += 1
                    logger.warning(f"Cycle {iteration} failed: no records processed")
                
                # Log individual symbol results
                for symbol, count in batch_results.items():
                    if count > 0:
                        logger.info(f"  {symbol}: {count} records")
                
                # Calculate next cycle time
                next_cycle = cycle_start + timedelta(minutes=config.interval_minutes)
                logger.info(f"Next cycle scheduled for: {next_cycle}")
                
                # Wait for next cycle
                time.sleep(config.interval_minutes * 60)
                
        except KeyboardInterrupt:
            logger.info("Continuous polling interrupted by user")
        except Exception as e:
            logger.error(f"Error in continuous polling: {e}")
            failed_cycles += 1
        
        end_time = datetime.utcnow()
        total_duration = end_time - start_time
        
        return {
            'total_iterations': iteration,
            'successful_cycles': successful_cycles,
            'failed_cycles': failed_cycles,
            'total_records_processed': total_records,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'total_duration': str(total_duration),
            'success_rate': (successful_cycles / iteration * 100) if iteration > 0 else 0
        }
    
    def _process_symbols_in_batches(self, 
                                   symbols: List[str], 
                                   interval: str, 
                                   batch_size: int) -> Dict[str, int]:
        """Process symbols in batches to avoid overwhelming the API"""
        all_results = {}
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: {batch}")
            
            try:
                batch_results = self.etl_service.process_multiple_stocks_intraday(batch, interval)
                all_results.update(batch_results)
                
                # Small delay between batches to be respectful to API
                if i + batch_size < len(symbols):
                    time.sleep(2)
                    
            except Exception as e:
                logger.error(f"Error processing batch {batch}: {e}")
                # Mark failed symbols as 0 records
                for symbol in batch:
                    all_results[symbol] = 0
        
        return all_results
    
    def market_hours_polling(self, 
                           config: PollingConfig,
                           market_open_hour: int = 9,
                           market_close_hour: int = 16) -> Dict[str, Any]:
        """
        Poll only during market hours
        
        Args:
            config: Polling configuration
            market_open_hour: Market open hour (24-hour format)
            market_close_hour: Market close hour (24-hour format)
        
        Returns:
            Polling results and statistics
        """
        logger.info(f"Starting market hours polling ({market_open_hour}:00 - {market_close_hour}:00)")
        
        if config.symbols is None:
            config.symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Default symbols for market hours polling
        
        iteration = 0
        total_records = 0
        successful_cycles = 0
        failed_cycles = 0
        
        start_time = datetime.utcnow()
        
        try:
            while True:
                if config.max_iterations and iteration >= config.max_iterations:
                    logger.info(f"Reached maximum iterations ({config.max_iterations})")
                    break
                
                current_hour = datetime.utcnow().hour
                
                # Check if market is open
                if market_open_hour <= current_hour < market_close_hour:
                    iteration += 1
                    cycle_start = datetime.utcnow()
                    
                    logger.info(f"=== Market Hours Polling Cycle {iteration} at {cycle_start} ===")
                    
                    # Process symbols
                    batch_results = self._process_symbols_in_batches(
                        config.symbols, 
                        config.interval, 
                        config.batch_size
                    )
                    
                    cycle_records = sum(batch_results.values())
                    total_records += cycle_records
                    cycle_time = datetime.utcnow() - cycle_start
                    
                    if cycle_records > 0:
                        successful_cycles += 1
                        logger.info(f"Cycle {iteration} successful: {cycle_records} records in {cycle_time}")
                    else:
                        failed_cycles += 1
                        logger.warning(f"Cycle {iteration} failed: no records processed")
                    
                    # Wait for next cycle
                    time.sleep(config.interval_minutes * 60)
                else:
                    # Market is closed, wait longer
                    logger.info(f"Market closed (current hour: {current_hour}), waiting...")
                    time.sleep(30 * 60)  # Wait 30 minutes
                    
        except KeyboardInterrupt:
            logger.info("Market hours polling interrupted by user")
        except Exception as e:
            logger.error(f"Error in market hours polling: {e}")
            failed_cycles += 1
        
        end_time = datetime.utcnow()
        total_duration = end_time - start_time
        
        return {
            'total_iterations': iteration,
            'successful_cycles': successful_cycles,
            'failed_cycles': failed_cycles,
            'total_records_processed': total_records,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'total_duration': str(total_duration),
            'success_rate': (successful_cycles / iteration * 100) if iteration > 0 else 0,
            'market_hours': f"{market_open_hour}:00-{market_close_hour}:00"
        }
    

