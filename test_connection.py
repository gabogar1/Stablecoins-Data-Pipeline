#!/usr/bin/env python3
"""
Test Database Connection and API Access

Simple script to verify that:
1. Database connection works
2. CoinGecko API is accessible
3. Environment variables are configured correctly

Run this before executing the main pipeline.
"""

import os
import sys
import requests
import psycopg2
from dotenv import load_dotenv


def test_environment_variables():
    """Test if all required environment variables are set."""
    print("🔍 Testing environment variables...")
    
    load_dotenv()
    
    required_vars = [
        'SUPABASE_DB_HOST',
        'SUPABASE_DB_NAME', 
        'SUPABASE_DB_USER',
        'SUPABASE_DB_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("💡 Please check your .env file")
        return False
    
    print("✅ All required environment variables are set")
    return True


def test_database_connection():
    """Test PostgreSQL database connection."""
    print("\n🔍 Testing database connection...")
    
    try:
        # Try connection string first, then individual parameters
        db_url = os.getenv('SUPABASE_DB_URL')
        
        if db_url:
            connection = psycopg2.connect(db_url)
        else:
            connection = psycopg2.connect(
                host=os.getenv('SUPABASE_DB_HOST'),
                database=os.getenv('SUPABASE_DB_NAME'),
                user=os.getenv('SUPABASE_DB_USER'),
                password=os.getenv('SUPABASE_DB_PASSWORD'),
                port=os.getenv('SUPABASE_DB_PORT', 5432)
            )
        
        # Test a simple query
        with connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            result = cursor.fetchone()
            print(f"✅ Database connection successful")
            print(f"📊 PostgreSQL version: {result[0][:50]}...")
        
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Please check your Supabase credentials in .env file")
        return False


def test_coingecko_api():
    """Test CoinGecko API access."""
    print("\n🔍 Testing CoinGecko API access...")
    
    try:
        # Test basic API access
        response = requests.get(
            "https://api.coingecko.com/api/v3/ping",
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ CoinGecko API is accessible")
            
            # Test data endpoint with a simple call (last 7 days)
            test_response = requests.get(
                "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
                params={'vs_currency': 'usd', 'days': '7', 'interval': 'daily'},
                timeout=10
            )
            
            if test_response.status_code == 200:
                data = test_response.json()
                if 'market_caps' in data:
                    print(f"✅ Market data endpoint working (got {len(data['market_caps'])} data points)")
                    print("📅 Free tier provides up to 365 days of historical data")
                    return True
                else:
                    print("⚠️ Market data endpoint returned unexpected format")
                    return False
            else:
                print(f"⚠️ Market data endpoint returned status {test_response.status_code}")
                return False
        else:
            print(f"❌ CoinGecko API ping failed with status {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ CoinGecko API test failed: {e}")
        print("💡 Please check your internet connection")
        return False


def test_table_creation():
    """Test if we can create the required table."""
    print("\n🔍 Testing table creation...")
    
    try:
        # Connect to database
        db_url = os.getenv('SUPABASE_DB_URL')
        
        if db_url:
            connection = psycopg2.connect(db_url)
        else:
            connection = psycopg2.connect(
                host=os.getenv('SUPABASE_DB_HOST'),
                database=os.getenv('SUPABASE_DB_NAME'),
                user=os.getenv('SUPABASE_DB_USER'),
                password=os.getenv('SUPABASE_DB_PASSWORD'),
                port=os.getenv('SUPABASE_DB_PORT', 5432)
            )
        
        # Test table creation (without actually creating it in main pipeline)
        test_table_sql = """
        CREATE TABLE IF NOT EXISTS test_stablecoin_connection (
            id SERIAL PRIMARY KEY,
            test_timestamp TIMESTAMPTZ DEFAULT NOW()
        );
        
        INSERT INTO test_stablecoin_connection DEFAULT VALUES;
        
        SELECT COUNT(*) FROM test_stablecoin_connection;
        
        DROP TABLE test_stablecoin_connection;
        """
        
        with connection.cursor() as cursor:
            cursor.execute(test_table_sql)
            result = cursor.fetchone()
            connection.commit()
            
            print("✅ Table creation and operations successful")
            print(f"📊 Test record count: {result[0]}")
        
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Table creation test failed: {e}")
        print("💡 Please check your database permissions")
        return False


def main():
    """Run all connection tests."""
    print("🧪 STABLECOIN PIPELINE CONNECTION TESTS")
    print("=" * 50)
    
    all_tests_passed = True
    
    # Test 1: Environment Variables
    if not test_environment_variables():
        all_tests_passed = False
    
    # Test 2: Database Connection
    if not test_database_connection():
        all_tests_passed = False
    
    # Test 3: CoinGecko API
    if not test_coingecko_api():
        all_tests_passed = False
    
    # Test 4: Table Creation
    if not test_table_creation():
        all_tests_passed = False
    
    # Summary
    print("\n" + "=" * 50)
    if all_tests_passed:
        print("🎉 ALL TESTS PASSED!")
        print("✅ Your environment is ready to run the stablecoin pipeline")
        print("\n🚀 Next step: python stablecoin_data_pipeline.py")
    else:
        print("❌ SOME TESTS FAILED!")
        print("💡 Please fix the issues above before running the pipeline")
        sys.exit(1)


if __name__ == "__main__":
    main() 