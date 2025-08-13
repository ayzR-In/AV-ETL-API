#!/usr/bin/env python3
"""
ETL Package for Stock Data Processing
Contains Extract, Transform, and Load modules
"""

from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader

__all__ = ['DataExtractor', 'DataTransformer', 'DataLoader']

# Version information
__version__ = '1.0.0'
__author__ = 'Stock Data ETL System'
__description__ = 'Modular ETL system for stock data processing'
