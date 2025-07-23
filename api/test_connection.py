#!/usr/bin/env python3
"""
Test script to verify Snowflake connection and basic functionality
"""

import os
import sys
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

def test_snowflake_connection():
    """Test basic Snowflake connection"""
    print("🔍 Testing Snowflake Connection...")
    
    # Get connection parameters from environment
    config = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA')
    }
    
    # Check if all required parameters are set
    missing_params = [key for key, value in config.items() if not value]
    if missing_params:
        print(f"❌ Missing environment variables: {missing_params}")
        print("Please set these in your .env file:")
        for param in missing_params:
            print(f"  {param.upper()}=your_value")
        return False
    
    try:
        # Attempt connection
        print(f"📡 Connecting to Snowflake account: {config['account']}")
        conn = snowflake.connector.connect(**config)
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"✅ Connected successfully! Snowflake version: {version}")
        
        # Test database access
        cursor.execute(f"USE DATABASE {config['database']}")
        print(f"✅ Database access confirmed: {config['database']}")
        
        # Test schema access
        cursor.execute(f"USE SCHEMA {config['schema']}")
        print(f"✅ Schema access confirmed: {config['schema']}")
        
        # Test table existence
        tables_to_check = ['GOLD_NEWS_SENTIMENT', 'GOLD_STOCK_SENTIMENT_CORRELATION']
        for table in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table} LIMIT 1")
                count = cursor.fetchone()[0]
                print(f"✅ Table {table} exists with {count} rows")
            except Exception as e:
                print(f"⚠️  Table {table} not found or not accessible: {e}")
        
        cursor.close()
        conn.close()
        print("✅ All connection tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def test_api_endpoints():
    """Test API endpoints if Flask app is running"""
    print("\n🔍 Testing API Endpoints...")
    
    import requests
    
    base_url = "http://localhost:5001"
    endpoints = [
        "/api/health",
        "/api/sentiment/headlines?limit=5",
        "/api/sentiment/summary",
        "/api/correlation/stock-sentiment"
    ]
    
    for endpoint in endpoints:
        try:
            response = requests.get(f"{base_url}{endpoint}", timeout=5)
            if response.status_code == 200:
                print(f"✅ {endpoint} - OK")
            else:
                print(f"⚠️  {endpoint} - Status: {response.status_code}")
        except requests.exceptions.ConnectionError:
            print(f"❌ {endpoint} - Connection failed (API not running)")
        except Exception as e:
            print(f"❌ {endpoint} - Error: {e}")

if __name__ == "__main__":
    print("🚀 Financial Sentiment API - Connection Test")
    print("=" * 50)
    
    # Test Snowflake connection
    snowflake_ok = test_snowflake_connection()
    
    # Test API endpoints
    test_api_endpoints()
    
    print("\n" + "=" * 50)
    if snowflake_ok:
        print("🎉 Connection test completed successfully!")
        print("You can now run the API with: python api/sentiment_api.py")
    else:
        print("❌ Connection test failed. Please check your configuration.")
        sys.exit(1) 