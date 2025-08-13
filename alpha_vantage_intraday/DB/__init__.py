#!/usr/bin/env python3
"""
DB Package for Database Management
Contains database connection and management functionality
"""

from .database import DatabaseManager, init_db

__all__ = ['DatabaseManager', 'init_db']

# Version information
__version__ = '1.0.0'
__author__ = 'Stock Data ETL System'
__description__ = 'Database management and connection handling'
