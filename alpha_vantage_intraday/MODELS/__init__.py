#!/usr/bin/env python3
"""
MODELS Package for Data Structures
Contains data models and structures for the ETL system
"""

from .models import Stock, StockPriceIntraday

__all__ = ['Stock', 'StockPriceIntraday']

# Version information
__version__ = '1.0.0'
__author__ = 'Stock Data ETL System'
__description__ = 'Data models and structures for stock data processing'
