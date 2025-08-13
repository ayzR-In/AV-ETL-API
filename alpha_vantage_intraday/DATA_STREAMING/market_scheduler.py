#!/usr/bin/env python3
"""
Market Scheduler for Data Ingestion
Handles market-aware scheduling and timing
"""

import logging
import time
from typing import List, Dict, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass

from alpha_vantage_intraday.intraday_pipeline import ETLService

logger = logging.getLogger(__name__)

@dataclass
class MarketSchedule:
    """Market schedule configuration"""
    open_hour: int = 9  # 9 AM
    close_hour: int = 16  # 4 PM
    pre_market_start: int = 4  # 4 AM
    after_hours_end: int = 20  # 8 PM
    timezone: str = "UTC"
    trading_days: List[int] = None  # Monday=0, Sunday=6
    
    def __post_init__(self):
        if self.trading_days is None:
            self.trading_days = [0, 1, 2, 3, 4]  # Monday to Friday

class MarketScheduler:
    """Market-aware scheduler for data ingestion"""
    
    def __init__(self, market_schedule: Optional[MarketSchedule] = None):
        self.etl_service = ETLService()
        self.market_schedule = market_schedule or MarketSchedule()
        self.is_running = False
        self.scheduled_jobs = {}
        

    
    def is_market_open(self, current_time: Optional[datetime] = None) -> bool:
        """Check if market is currently open"""
        if current_time is None:
            current_time = datetime.utcnow()
        
        # Check if it's a trading day
        if current_time.weekday() not in self.market_schedule.trading_days:
            return False
        
        current_hour = current_time.hour
        
        # Check if within market hours
        return self.market_schedule.open_hour <= current_hour < self.market_schedule.close_hour
    
    def is_pre_market(self, current_time: Optional[datetime] = None) -> bool:
        """Check if it's pre-market hours"""
        if current_time is None:
            current_time = datetime.utcnow()
        
        if current_time.weekday() not in self.market_schedule.trading_days:
            return False
        
        current_hour = current_time.hour
        return self.market_schedule.pre_market_start <= current_hour < self.market_schedule.open_hour
    
    def is_after_hours(self, current_time: Optional[datetime] = None) -> bool:
        """Check if it's after-hours trading"""
        if current_time is None:
            current_time = datetime.utcnow()
        
        if current_time.weekday() not in self.market_schedule.trading_days:
            return False
        
        current_hour = current_time.hour
        return self.market_schedule.close_hour <= current_hour < self.market_schedule.after_hours_end
    
    def get_next_market_open(self, current_time: Optional[datetime] = None) -> datetime:
        """Get the next market open time"""
        if current_time is None:
            current_time = datetime.utcnow()
        
        # If market is open today, return today's open
        if current_time.weekday() in self.market_schedule.trading_days:
            today_open = current_time.replace(
                hour=self.market_schedule.open_hour, 
                minute=0, 
                second=0, 
                microsecond=0
            )
            
            if current_time < today_open:
                return today_open
        
        # Find next trading day
        next_trading_day = current_time
        while True:
            next_trading_day += timedelta(days=1)
            if next_trading_day.weekday() in self.market_schedule.trading_days:
                break
        
        return next_trading_day.replace(
            hour=self.market_schedule.open_hour, 
            minute=0, 
            second=0, 
            microsecond=0
        )
    
    def get_next_market_close(self, current_time: Optional[datetime] = None) -> datetime:
        """Get the next market close time"""
        if current_time is None:
            current_time = datetime.utcnow()
        
        # If market is open today, return today's close
        if current_time.weekday() in self.market_schedule.trading_days:
            today_close = current_time.replace(
                hour=self.market_schedule.close_hour, 
                minute=0, 
                second=0, 
                microsecond=0
            )
            
            if current_time < today_close:
                return today_close
        
        # Find next trading day
        next_trading_day = current_time
        while True:
            next_trading_day += timedelta(days=1)
            if next_trading_day.weekday() in self.market_schedule.trading_days:
                break
        
        return next_trading_day.replace(
            hour=self.market_schedule.close_hour, 
            minute=0, 
            second=0, 
            microsecond=0
        )
    
    def schedule_market_hours_job(self, 
                                job_name: str,
                                symbols: List[str],
                                interval_minutes: int = 5,
                                callback: Optional[Callable] = None) -> bool:
        """
        Schedule a job to run during market hours
        
        Args:
            job_name: Name of the scheduled job
            symbols: List of stock symbols to process
            interval_minutes: Polling interval in minutes
            callback: Optional callback function after each cycle
        
        Returns:
            True if job scheduled successfully
        """
        if job_name in self.scheduled_jobs:
            logger.warning(f"Job '{job_name}' already scheduled")
            return False
        
        job_config = {
            'name': job_name,
            'symbols': symbols,
            'interval_minutes': interval_minutes,
            'callback': callback,
            'type': 'market_hours',
            'created_at': datetime.utcnow()
        }
        
        self.scheduled_jobs[job_name] = job_config
        logger.info(f"Scheduled market hours job '{job_name}' for {len(symbols)} symbols every {interval_minutes} minutes")
        
        return True
    
    def schedule_pre_market_job(self, 
                              job_name: str,
                              symbols: List[str],
                              interval_minutes: int = 15,
                              callback: Optional[Callable] = None) -> bool:
        """Schedule a job to run during pre-market hours"""
        if job_name in self.scheduled_jobs:
            logger.warning(f"Job '{job_name}' already scheduled")
            return False
        
        job_config = {
            'name': job_name,
            'symbols': symbols,
            'interval_minutes': interval_minutes,
            'callback': callback,
            'type': 'pre_market',
            'created_at': datetime.utcnow()
        }
        
        self.scheduled_jobs[job_name] = job_config
        logger.info(f"Scheduled pre-market job '{job_name}' for {len(symbols)} symbols every {interval_minutes} minutes")
        
        return True
    
    def schedule_after_hours_job(self, 
                               job_name: str,
                               symbols: List[str],
                               interval_minutes: int = 15,
                               callback: Optional[Callable] = None) -> bool:
        """Schedule a job to run during after-hours trading"""
        if job_name in self.scheduled_jobs:
            logger.warning(f"Job '{job_name}' already scheduled")
            return False
        
        job_config = {
            'name': job_name,
            'symbols': symbols,
            'interval_minutes': interval_minutes,
            'callback': callback,
            'type': 'after_hours',
            'created_at': datetime.utcnow()
        }
        
        self.scheduled_jobs[job_name] = job_config
        logger.info(f"Scheduled after-hours job '{job_name}' for {len(symbols)} symbols every {interval_minutes} minutes")
        
        return True
    
    def run_scheduled_jobs(self) -> Dict[str, Any]:
        """Run all scheduled jobs according to their schedules"""
        if not self.scheduled_jobs:
            logger.info("No scheduled jobs to run")
            return {}
        
        logger.info(f"Running {len(self.scheduled_jobs)} scheduled jobs")
        results = {}
        
        for job_name, job_config in self.scheduled_jobs.items():
            try:
                logger.info(f"Running scheduled job: {job_name}")
                
                # Check if job should run based on current time
                current_time = datetime.utcnow()
                should_run = False
                
                if job_config['type'] == 'market_hours':
                    should_run = self.is_market_open(current_time)
                elif job_config['type'] == 'pre_market':
                    should_run = self.is_pre_market(current_time)
                elif job_config['type'] == 'after_hours':
                    should_run = self.is_after_hours(current_time)
                
                if should_run:
                    # Run the job
                    job_results = self.etl_service.process_multiple_stocks_intraday(
                        job_config['symbols'], 
                        "5min"
                    )
                    
                    total_records = sum(job_results.values())
                    results[job_name] = {
                        'status': 'success',
                        'records_processed': total_records,
                        'symbols_processed': len(job_config['symbols']),
                        'execution_time': datetime.utcnow().isoformat()
                    }
                    
                    logger.info(f"Job '{job_name}' completed: {total_records} records processed")
                    
                    # Execute callback if provided
                    if job_config['callback']:
                        try:
                            job_config['callback'](job_name, job_results)
                        except Exception as e:
                            logger.error(f"Callback execution failed for job '{job_name}': {e}")
                else:
                    logger.info(f"Job '{job_name}' not scheduled to run at current time")
                    results[job_name] = {
                        'status': 'skipped',
                        'reason': 'outside scheduled hours',
                        'execution_time': datetime.utcnow().isoformat()
                    }
                    
            except Exception as e:
                logger.error(f"Error running scheduled job '{job_name}': {e}")
                results[job_name] = {
                    'status': 'error',
                    'error': str(e),
                    'execution_time': datetime.utcnow().isoformat()
                }
        
        return results
    
    def remove_scheduled_job(self, job_name: str) -> bool:
        """Remove a scheduled job"""
        if job_name in self.scheduled_jobs:
            del self.scheduled_jobs[job_name]
            logger.info(f"Removed scheduled job '{job_name}'")
            return True
        else:
            logger.warning(f"Job '{job_name}' not found in scheduled jobs")
            return False
    
    def get_scheduled_jobs(self) -> Dict[str, Any]:
        """Get information about all scheduled jobs"""
        return {
            'total_jobs': len(self.scheduled_jobs),
            'jobs': self.scheduled_jobs,
            'current_time': datetime.utcnow().isoformat(),
            'market_status': {
                'is_market_open': self.is_market_open(),
                'is_pre_market': self.is_pre_market(),
                'is_after_hours': self.is_after_hours(),
                'next_market_open': self.get_next_market_open().isoformat(),
                'next_market_close': self.get_next_market_close().isoformat()
            }
        }
    
    def cleanup(self):
        """Clean up resources"""
        try:
            self.scheduled_jobs.clear()
            self.etl_service.cleanup()
            logger.info("Market scheduler cleanup completed")
        except Exception as e:
            logger.error(f"Error during market scheduler cleanup: {e}")
