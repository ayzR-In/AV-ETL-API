#!/usr/bin/env python3
"""
DATA_STREAMING Package for Real-time Data Ingestion
Contains streaming, polling, and real-time simulation functionality
"""

from .streaming_service import DataStreamingService
from .polling_manager import PollingManager
from .market_scheduler import MarketScheduler

__all__ = ['DataStreamingService', 'PollingManager', 'MarketScheduler']

# Version information
__version__ = '1.0.0'
__author__ = 'Stock Data ETL System'
__description__ = 'Real-time data streaming and periodic polling for stock data'
