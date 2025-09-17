import duckdb
from datetime import datetime
import os
import pandas as pd

class StockDatabase:
    def __init__(self, db_path='stock_data.duckdb'):
        self.db_path = db_path
        self.connection = None
        self._create_tables()

    def _create_tables(self):
        """Create necessary tables if they don't exist"""
        with duckdb.connect(self.db_path) as conn:
            # Table for storing stock price data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    symbol VARCHAR,
                    timestamp TIMESTAMP,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    volume BIGINT,
                    PRIMARY KEY (symbol, timestamp)
                )
            """)

            # Table for storing latest prices
            conn.execute("""
                CREATE TABLE IF NOT EXISTS latest_prices (
                    symbol VARCHAR PRIMARY KEY,
                    price DOUBLE,
                    change_percent DOUBLE,
                    volume BIGINT,
                    last_updated TIMESTAMP
                )
            """)

    def insert_historic_data(self, symbol, data):
        """Insert historic stock data"""
        with duckdb.connect(self.db_path) as conn:
            # Convert data to the format expected
            records = []
            for index, row in data.iterrows():
                # Use datetime object directly for TIMESTAMP column
                records.append((
                    symbol,
                    index.to_pydatetime(),  # Convert pandas Timestamp to Python datetime
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    int(row['Volume'])
                ))

            # Use INSERT OR REPLACE for DuckDB
            conn.executemany("""
                INSERT OR REPLACE INTO stock_prices
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, records)

    def update_latest_price(self, symbol, price, change_percent, volume):
        """Update the latest price for a symbol"""
        with duckdb.connect(self.db_path) as conn:
            current_time = datetime.now()
            conn.execute("""
                INSERT OR REPLACE INTO latest_prices
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, price, change_percent, volume, current_time))

    def get_historic_data(self, symbol, start_date=None, end_date=None):
        """Get historic data for a symbol"""
        with duckdb.connect(self.db_path) as conn:
            query = "SELECT * FROM stock_prices WHERE symbol = ?"
            params = [symbol]

            # Convert date strings to datetime objects for filtering
            if start_date:
                start_datetime = pd.to_datetime(start_date)
                query += " AND timestamp >= ?"
                params.append(start_datetime)

            if end_date:
                end_datetime = pd.to_datetime(end_date)
                query += " AND timestamp <= ?"
                params.append(end_datetime)

            query += " ORDER BY timestamp"

            df = conn.execute(query, params).fetchdf()

            # Set timestamp as index directly since it's already a datetime
            if not df.empty and 'timestamp' in df.columns:
                df.set_index('timestamp', inplace=True)
                df.index.name = 'Date'  # Match the original format

            return df

    def get_latest_price(self, symbol):
        """Get the latest price for a symbol"""
        with duckdb.connect(self.db_path) as conn:
            result = conn.execute("""
                SELECT * FROM latest_prices WHERE symbol = ?
            """, (symbol,)).fetchone()

            if result:
                # last_updated is already a datetime object from TIMESTAMP column
                return {
                    'symbol': result[0],
                    'price': result[1],
                    'change_percent': result[2],
                    'volume': result[3],
                    'last_updated': result[4]
                }
            return None

    def get_all_symbols(self):
        """Get all unique symbols in the database"""
        with duckdb.connect(self.db_path) as conn:
            result = conn.execute("""
                SELECT DISTINCT symbol FROM stock_prices
            """).fetchall()
            return [row[0] for row in result]
