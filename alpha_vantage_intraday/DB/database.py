from __future__ import annotations
import psycopg2
import psycopg2.extras
import psycopg2.pool
import os
import logging
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""
    
    def __init__(self):
        self.connection_pool = None
        # Don't initialize connection pool on import to avoid connection errors
        # Call init_connection_pool() explicitly when needed
    
    def init_connection_pool(self):
        """Initialize the database connection pool"""
        try:
            # Get database connection parameters from environment variables
            host = os.getenv('POSTGRES_HOST', 'localhost')
            port = int(os.getenv('POSTGRES_PORT', '5432'))
            database = os.getenv('POSTGRES_DB', 'stock_data')
            user = os.getenv('POSTGRES_USER', 'postgres')
            password = os.getenv('POSTGRES_PASSWORD', 'postgres')
            
            # Create connection pool
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            logger.info(f"Database connection pool initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        if self.connection_pool:
            return self.connection_pool.getconn()
        else:
            raise Exception("Database connection pool not initialized")
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if self.connection_pool and conn:
            self.connection_pool.putconn(conn)
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = False, fetch_one: bool = False):
        """Execute a SQL query"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            cursor.execute(query, params)
            
            if fetch:
                result = cursor.fetchall()
            elif fetch_one:
                result = cursor.fetchone()
            else:
                result = None
            
            conn.commit()
            cursor.close()
            
            return result
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute a query with multiple parameter sets"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.executemany(query, params_list)
            rows_affected = cursor.rowcount
            
            conn.commit()
            cursor.close()
            
            return rows_affected
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Batch query execution failed: {e}")
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    def create_tables(self):
        """Create all necessary database tables"""
        try:
            logger.info("Creating intraday-focused database tables...")
            
            # Create stocks table
            stocks_table_sql = """
            CREATE TABLE IF NOT EXISTS stocks (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) UNIQUE NOT NULL,
                company_name VARCHAR(255) NOT NULL,
                exchange VARCHAR(50) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            self.execute_query(stocks_table_sql)
            logger.info("Stocks table created/verified")
            
            # Create intraday stock prices table 
            intraday_table_sql = """
            CREATE TABLE IF NOT EXISTS stock_prices_intraday (
                id SERIAL PRIMARY KEY,
                stock_symbol VARCHAR(10) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open_price DECIMAL(10,4) NOT NULL,
                high_price DECIMAL(10,4) NOT NULL,
                low_price DECIMAL(10,4) NOT NULL,
                close_price DECIMAL(10,4) NOT NULL,
                volume BIGINT NOT NULL,
                interval VARCHAR(10) NOT NULL DEFAULT '5min',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stock_symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
            );
            """
            
            self.execute_query(intraday_table_sql)
            logger.info("Stock prices intraday table created/verified")
            
            # Create ETL job logs table
            etl_logs_table_sql = """
            CREATE TABLE IF NOT EXISTS etl_job_logs (
                id SERIAL PRIMARY KEY,
                job_name VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                records_processed INTEGER DEFAULT 0,
                total_records INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            self.execute_query(etl_logs_table_sql)
            logger.info("ETL job logs table created/verified")
            
            # Create indexes for performance optimization
            logger.info("Creating database indexes...")
            
            # Indexes for stocks table
            self.execute_query("""
                -- Index on symbol for fast stock lookups by symbol (most common query)
                CREATE INDEX IF NOT EXISTS idx_stock_symbol ON stocks(symbol);
                -- Index on exchange for filtering stocks by exchange (e.g., NYSE, NASDAQ)
                CREATE INDEX IF NOT EXISTS idx_stock_exchange ON stocks(exchange);
            """)
            
            # Indexes for stock_prices_intraday table
            self.execute_query("""
                -- Composite index for fast queries by stock symbol and timestamp (most common query pattern)
                -- This enables efficient time-series queries for specific stocks
                CREATE INDEX IF NOT EXISTS idx_intraday_symbol_timestamp ON stock_prices_intraday(stock_symbol, timestamp);
                
                -- Index on timestamp for global time-based queries across all stocks
                -- Useful for market-wide analysis and time-range filtering
                CREATE INDEX IF NOT EXISTS idx_intraday_timestamp ON stock_prices_intraday(timestamp);
                
                -- Index on interval for filtering by time interval (1min, 5min, 15min, etc.)
                -- Enables efficient queries for specific intraday intervals
                CREATE INDEX IF NOT EXISTS idx_intraday_interval ON stock_prices_intraday(interval);
                
                -- Composite index for queries filtering by both stock and interval
                -- Optimizes queries like "get all 5min data for AAPL"
                CREATE INDEX IF NOT EXISTS idx_intraday_symbol_interval ON stock_prices_intraday(stock_symbol, interval);
            """)
            
            # Indexes for etl_job_logs table
            self.execute_query("""
                -- Index on status for filtering jobs by status (SUCCESS, FAILED, RUNNING)
                -- Enables quick status-based queries for monitoring and debugging
                CREATE INDEX IF NOT EXISTS idx_etl_job_status ON etl_job_logs(status);
                
                -- Index on start_time for time-based job queries and recent job analysis
                -- Useful for performance monitoring and job history analysis
                CREATE INDEX IF NOT EXISTS idx_etl_job_start_time ON etl_job_logs(start_time);
                
                -- Index on job_name for filtering specific job types
                -- Enables analysis of specific ETL job performance over time
                CREATE INDEX IF NOT EXISTS idx_etl_job_name ON etl_job_logs(job_name);
            """)
            
            logger.info("All database indexes created/verified")
            logger.info("Intraday-focused database tables and indexes setup completed successfully!")
            
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise
    
    def close(self):
        """Close the connection pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connection pool closed")

# Function to initialize database (lazy initialization)
def init_db():
    """Initialize the database with tables and indexes"""
    db_manager = DatabaseManager()
    db_manager.init_connection_pool()
    db_manager.create_tables()
    db_manager.close()
    return db_manager
