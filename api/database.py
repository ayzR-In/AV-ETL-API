#!/usr/bin/env python3
"""
Database connection and management for the FastAPI
Reuses the existing database manager from the ETL system
"""

import os
import sys
from typing import Generator
from dotenv import load_dotenv

# Add the parent directory to the path to import from alpha_vantage_intraday
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from alpha_vantage_intraday.DB.database import DatabaseManager

load_dotenv()

def get_db() -> Generator[DatabaseManager, None, None]:
    """
    Dependency to get database connection
    Initializes connection pool if needed
    """
    db_manager = DatabaseManager()
    try:
        # Initialize connection pool if not already done
        if not db_manager.connection_pool:
            db_manager.init_connection_pool()
        yield db_manager
    finally:
        # Don't close the connection pool here as it might be used by other requests
        # The pool will be managed by the ETL system
        pass
