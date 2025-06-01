#!/usr/bin/env python3
"""
Stablecoin Market Cap Data Pipeline

Fetches historical market cap data for major stablecoins from CoinGecko API
and stores it in a PostgreSQL database hosted on Supabase.

Author: Gabriel
Created: 2025
"""

import requests
import time
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os
import logging
from typing import List, Dict, Optional, Tuple, Union
from dotenv import load_dotenv
import json
from decimal import Decimal, ROUND_HALF_UP


class StablecoinDataPipeline:
    """
    A robust pipeline for fetching stablecoin market data from CoinGecko API
    and storing it in PostgreSQL database.
    
    Free tier limitation: Only the last 365 days of data are available.
    """
    
    # Target stablecoins with their CoinGecko IDs and metadata
    STABLECOINS = {
        'tether': {
            'name': 'Tether',
            'symbol': 'USDT',
            'expected_price_range': (0.90, 1.10)
        },
        'usd-coin': {
            'name': 'USD Coin',
            'symbol': 'USDC',
            'expected_price_range': (0.90, 1.10)
        },
        'dai': {
            'name': 'Dai',
            'symbol': 'DAI',
            'expected_price_range': (0.90, 1.10)
        },
        'binance-usd': {
            'name': 'Binance USD',
            'symbol': 'BUSD',
            'expected_price_range': (0.90, 1.10)
        },
        'frax': {
            'name': 'Frax',
            'symbol': 'FRAX',
            'expected_price_range': (0.90, 1.10)
        },
        'true-usd': {
            'name': 'TrueUSD',
            'symbol': 'TUSD',
            'expected_price_range': (0.90, 1.10)
        }
    }
    
    def __init__(self, config: Dict[str, str]):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config: Dictionary containing database and API configuration
        """
        self.config = config
        self.api_key = config.get('COINGECKO_API_KEY')
        self.base_url = "https://api.coingecko.com/api/v3"
        self.rate_limit_delay = 2.1  # Slightly over 2 seconds to respect 30 calls/minute
        self.max_retries = 3
        self.db_connection = None
        
        # Setup logging
        self._setup_logging()
        
        # Initialize database connection
        self._connect_to_database()
    
    def _setup_logging(self) -> None:
        """Configure logging for the pipeline."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)
    
    def _connect_to_database(self) -> None:
        """Establish connection to PostgreSQL database."""
        try:
            # Try connection string first, then individual parameters
            if 'SUPABASE_DB_URL' in self.config:
                self.db_connection = psycopg2.connect(self.config['SUPABASE_DB_URL'])
            else:
                self.db_connection = psycopg2.connect(
                    host=self.config['SUPABASE_DB_HOST'],
                    database=self.config['SUPABASE_DB_NAME'],
                    user=self.config['SUPABASE_DB_USER'],
                    password=self.config['SUPABASE_DB_PASSWORD'],
                    port=self.config.get('SUPABASE_DB_PORT', 5432)
                )
            
            self.db_connection.autocommit = False
            self.logger.info("✅ Successfully connected to PostgreSQL database")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to database: {e}")
            raise
    
    def create_table_if_not_exists(self) -> None:
        """Create the stablecoin market caps table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stablecoin_market_caps (
            id BIGSERIAL PRIMARY KEY,
            coin_id VARCHAR(50) NOT NULL,
            coin_name VARCHAR(100) NOT NULL,
            coin_symbol VARCHAR(10) NOT NULL,
            timestamp_utc TIMESTAMPTZ NOT NULL,
            market_cap_usd NUMERIC(20,2),
            price_usd NUMERIC(12,6),
            volume_24h_usd NUMERIC(20,2),
            data_granularity VARCHAR(20) DEFAULT 'daily',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            
            CONSTRAINT unique_coin_timestamp UNIQUE(coin_id, timestamp_utc)
        );
        
        CREATE INDEX IF NOT EXISTS idx_coin_timestamp 
        ON stablecoin_market_caps (coin_id, timestamp_utc);
        
        CREATE INDEX IF NOT EXISTS idx_timestamp 
        ON stablecoin_market_caps (timestamp_utc);
        
        CREATE INDEX IF NOT EXISTS idx_coin_id 
        ON stablecoin_market_caps (coin_id);
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.db_connection.commit()
                self.logger.info("✅ Database table and indexes created/verified (daily data only)")
        except Exception as e:
            self.logger.error(f"❌ Failed to create table: {e}")
            self.db_connection.rollback()
            raise
    
    def _make_api_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """
        Make a rate-limited request to CoinGecko API with retry logic.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response data or None if failed
        """
        if params is None:
            params = {}
        
        # Add API key if available
        if self.api_key:
            params['x_cg_demo_api_key'] = self.api_key
        
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"🔄 API Request: {url} (attempt {attempt + 1})")
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limited
                    wait_time = min(60 * (2 ** attempt), 300)  # Exponential backoff, max 5 minutes
                    self.logger.warning(f"⏳ Rate limited, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.warning(f"⚠️ API request failed with status {response.status_code}: {response.text}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"⚠️ Request exception (attempt {attempt + 1}): {e}")
                
            if attempt < self.max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        self.logger.error(f"❌ Failed to fetch data from {endpoint} after {self.max_retries} attempts")
        return None
    
    def fetch_market_data(self, coin_id: str, days: str = "365") -> Optional[Dict]:
        """
        Fetch market cap data from CoinGecko API.
        Free tier is limited to 365 days of historical data.
        
        Args:
            coin_id: CoinGecko coin identifier
            days: Number of days to fetch (max 365 for free tier)
            
        Returns:
            Market data dictionary or None if failed
        """
        endpoint = f"coins/{coin_id}/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': days,
            'interval': 'daily'  # Force daily interval only
        }
        
        self.logger.info(f"📊 Fetching daily market data for {coin_id} (last {days} days)")
        
        # Respect rate limit
        time.sleep(self.rate_limit_delay)
        
        data = self._make_api_request(endpoint, params)
        
        if data and 'market_caps' in data:
            self.logger.info(f"✅ Successfully fetched {len(data['market_caps'])} daily data points for {coin_id}")
            return data
        else:
            self.logger.error(f"❌ No market data received for {coin_id}")
            return None
    
    def _determine_granularity(self, timestamps: List[int]) -> str:
        """
        Determine the data granularity based on timestamp intervals.
        Since we only request daily data, this should always return 'daily'.
        
        Args:
            timestamps: List of UNIX timestamps
            
        Returns:
            Granularity string (should always be 'daily')
        """
        if len(timestamps) < 2:
            return 'daily'  # Default to daily
        
        # Calculate average interval in seconds
        intervals = [timestamps[i] - timestamps[i-1] for i in range(1, min(10, len(timestamps)))]
        avg_interval = sum(intervals) / len(intervals)
        
        # Convert to hours for easier understanding
        avg_interval_hours = avg_interval / 3600
        
        # We expect daily data (24 hours), but allow some tolerance
        if 20 <= avg_interval_hours <= 30:  # Between 20-30 hours (daily with some tolerance)
            return 'daily'
        else:
            # Log unexpected granularity but still process as daily
            self.logger.warning(f"⚠️ Unexpected data interval: ~{avg_interval_hours:.1f} hours. Expected daily data.")
            return 'daily'  # Force to daily since that's what we requested
    
    def _validate_price(self, price: float, coin_id: str) -> bool:
        """
        Validate if price is within expected range for stablecoin.
        
        Args:
            price: Price to validate
            coin_id: Coin identifier
            
        Returns:
            True if price is valid, False otherwise
        """
        if coin_id not in self.STABLECOINS:
            return True  # Skip validation for unknown coins
        
        min_price, max_price = self.STABLECOINS[coin_id]['expected_price_range']
        return min_price <= price <= max_price
    
    def process_market_data(self, raw_data: Dict, coin_id: str) -> List[Dict]:
        """
        Process and flatten API response data.
        Only processes daily granularity data.
        
        Args:
            raw_data: Raw API response data
            coin_id: CoinGecko coin identifier
            
        Returns:
            List of processed data records (daily granularity only)
        """
        coin_info = self.STABLECOINS.get(coin_id, {
            'name': coin_id.title(),
            'symbol': coin_id.upper()
        })
        
        processed_records = []
        
        # Extract data arrays
        market_caps = raw_data.get('market_caps', [])
        prices = raw_data.get('prices', [])
        volumes = raw_data.get('total_volumes', [])
        
        # Determine granularity - should always be daily
        timestamps = [item[0] for item in market_caps]
        granularity = self._determine_granularity(timestamps)
        
        # Validate that we have daily data
        if granularity != 'daily':
            self.logger.error(f"❌ Expected daily data for {coin_id}, got {granularity}. Skipping...")
            return []
        
        self.logger.info(f"📈 Processing {len(market_caps)} daily records for {coin_id}")
        
        # Create lookup dictionaries for efficient matching
        price_dict = {item[0]: item[1] for item in prices} if prices else {}
        volume_dict = {item[0]: item[1] for item in volumes} if volumes else {}
        
        anomaly_count = 0
        
        for timestamp_ms, market_cap in market_caps:
            try:
                # Convert timestamp to UTC datetime
                timestamp_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                
                # Get corresponding price and volume
                price = price_dict.get(timestamp_ms)
                volume_24h = volume_dict.get(timestamp_ms)
                
                # Data quality checks
                if market_cap is not None and market_cap < 0:
                    self.logger.warning(f"⚠️ Negative market cap detected: {market_cap}")
                    continue
                
                if price is not None and not self._validate_price(price, coin_id):
                    anomaly_count += 1
                    if anomaly_count <= 5:  # Log first 5 anomalies
                        self.logger.warning(f"⚠️ Price anomaly for {coin_id}: ${price:.4f} at {timestamp_utc}")
                
                # Convert to Decimal for precision
                record = {
                    'coin_id': coin_id,
                    'coin_name': coin_info.get('name', coin_id.title()),
                    'coin_symbol': coin_info.get('symbol', coin_id.upper()),
                    'timestamp_utc': timestamp_utc,
                    'market_cap_usd': Decimal(str(market_cap)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if market_cap is not None else None,
                    'price_usd': Decimal(str(price)).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP) if price is not None else None,
                    'volume_24h_usd': Decimal(str(volume_24h)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if volume_24h is not None else None,
                    'data_granularity': 'daily'  # Always daily as validated above
                }
                
                processed_records.append(record)
                
            except Exception as e:
                self.logger.warning(f"⚠️ Error processing record for {coin_id}: {e}")
                continue
        
        if anomaly_count > 5:
            self.logger.warning(f"⚠️ Total price anomalies for {coin_id}: {anomaly_count}")
        
        self.logger.info(f"✅ Processed {len(processed_records)} valid daily records for {coin_id}")
        return processed_records
    
    def upsert_market_data(self, processed_data: List[Dict]) -> None:
        """
        Insert or update market data in database using batch operations.
        
        Args:
            processed_data: List of processed data records
        """
        if not processed_data:
            self.logger.warning("⚠️ No data to insert")
            return
        
        upsert_sql = """
        INSERT INTO stablecoin_market_caps (
            coin_id, coin_name, coin_symbol, timestamp_utc, 
            market_cap_usd, price_usd, volume_24h_usd, data_granularity
        ) VALUES (
            %(coin_id)s, %(coin_name)s, %(coin_symbol)s, %(timestamp_utc)s,
            %(market_cap_usd)s, %(price_usd)s, %(volume_24h_usd)s, %(data_granularity)s
        )
        ON CONFLICT (coin_id, timestamp_utc) 
        DO UPDATE SET
            coin_name = EXCLUDED.coin_name,
            coin_symbol = EXCLUDED.coin_symbol,
            market_cap_usd = EXCLUDED.market_cap_usd,
            price_usd = EXCLUDED.price_usd,
            volume_24h_usd = EXCLUDED.volume_24h_usd,
            data_granularity = EXCLUDED.data_granularity,
            updated_at = NOW()
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                # Use batch execution for efficiency
                execute_batch(cursor, upsert_sql, processed_data, page_size=1000)
                self.db_connection.commit()
                
                self.logger.info(f"✅ Successfully upserted {len(processed_data)} records")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to upsert data: {e}")
            self.db_connection.rollback()
            raise
    
    def get_data_stats(self) -> Dict[str, int]:
        """
        Get statistics about the data in the database.
        
        Returns:
            Dictionary with data statistics
        """
        stats_sql = """
        SELECT 
            coin_id,
            COUNT(*) as record_count,
            MIN(timestamp_utc) as earliest_date,
            MAX(timestamp_utc) as latest_date
        FROM stablecoin_market_caps 
        GROUP BY coin_id
        ORDER BY coin_id
        """
        
        try:
            with self.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(stats_sql)
                results = cursor.fetchall()
                
                stats = {}
                total_records = 0
                
                for row in results:
                    coin_id = row['coin_id']
                    record_count = row['record_count']
                    total_records += record_count
                    
                    stats[coin_id] = {
                        'records': record_count,
                        'earliest': row['earliest_date'],
                        'latest': row['latest_date']
                    }
                
                stats['total_records'] = total_records
                return stats
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get data statistics: {e}")
            return {}
    
    def run_pipeline(self) -> None:
        """Main pipeline execution for all stablecoins - daily data only (last 365 days)."""
        self.logger.info("🚀 Starting Stablecoin Data Pipeline (Daily Data Collection - Last 365 Days)")
        
        # Create table if needed
        self.create_table_if_not_exists()
        
        successful_coins = []
        failed_coins = []
        
        for coin_id in self.STABLECOINS.keys():
            try:
                self.logger.info(f"\n📍 Processing daily data for {coin_id} (last 365 days)...")
                
                # Fetch market data (daily only, last 365 days)
                raw_data = self.fetch_market_data(coin_id, days="365")
                
                if raw_data is None:
                    failed_coins.append(coin_id)
                    continue
                
                # Process the data (daily only)
                processed_data = self.process_market_data(raw_data, coin_id)
                
                if not processed_data:
                    failed_coins.append(coin_id)
                    continue
                
                # Store in database
                self.upsert_market_data(processed_data)
                successful_coins.append(coin_id)
                
                self.logger.info(f"✅ Completed daily data processing for {coin_id}")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to process {coin_id}: {e}")
                failed_coins.append(coin_id)
                continue
        
        # Final summary
        self.logger.info(f"\n📊 DAILY DATA PIPELINE SUMMARY (LAST 365 DAYS)")
        self.logger.info(f"✅ Successful: {len(successful_coins)} coins")
        self.logger.info(f"❌ Failed: {len(failed_coins)} coins")
        
        if successful_coins:
            self.logger.info(f"🎉 Successfully processed daily data: {', '.join(successful_coins)}")
        
        if failed_coins:
            self.logger.warning(f"⚠️ Failed to process: {', '.join(failed_coins)}")
        
        # Display data statistics
        stats = self.get_data_stats()
        if stats:
            self.logger.info(f"\n📈 DAILY DATA STATISTICS (LAST 365 DAYS)")
            for coin_id, coin_stats in stats.items():
                if coin_id != 'total_records':
                    self.logger.info(f"{coin_id}: {coin_stats['records']:,} daily records "
                                   f"({coin_stats['earliest'].date()} to {coin_stats['latest'].date()})")
            self.logger.info(f"📊 Total daily records in database: {stats.get('total_records', 0):,}")
    
    def __del__(self):
        """Cleanup database connection."""
        if hasattr(self, 'db_connection') and self.db_connection:
            self.db_connection.close()


def load_configuration() -> Dict[str, str]:
    """Load configuration from environment variables."""
    load_dotenv()
    
    required_vars = [
        # 'SUPABASE_DB_HOST',
        # 'SUPABASE_DB_NAME', 
        # 'SUPABASE_DB_USER',
        # 'SUPABASE_DB_PASSWORD'
    ]
    
    config = {}
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            config[var] = value
        else:
            missing_vars.append(var)
    
    # Optional variables
    optional_vars = ['COINGECKO_API_KEY', 'SUPABASE_DB_PORT', 'SUPABASE_DB_URL']
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            config[var] = value
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return config


def main():
    """Main execution function."""
    try:
        # Load configuration
        config = load_configuration()
        
        # Initialize and run pipeline
        pipeline = StablecoinDataPipeline(config)
        pipeline.run_pipeline()
        
        print("\n🎯 Pipeline completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⏹️ Pipeline interrupted by user")
    except Exception as e:
        print(f"\n💥 Pipeline failed with error: {e}")
        raise


if __name__ == "__main__":
    main() 