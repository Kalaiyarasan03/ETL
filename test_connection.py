#!/usr/bin/env python3
"""
Test MySQL connections for ETL system
"""

import pymysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def test_direct_connection():
    """Test direct pymysql connection"""
    print("🔍 Testing direct pymysql connection...")
    
    try:
        # Test chit_db connection
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='root@2001',
            database='chit_db',
            port=3306
        )
        print("✅ Direct connection to chit_db: SUCCESS")
        conn.close()
        
        # Test test database connection
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='root@2001',
            database='test',
            port=3306
        )
        print("✅ Direct connection to test: SUCCESS")
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Direct connection failed: {e}")
        return False

def test_sqlalchemy_connection():
    """Test SQLAlchemy engine connection"""
    print("\n🔍 Testing SQLAlchemy engine connection...")
    
    try:
        from sqlalchemy import text
        # URL encode the password
        encoded_password = quote_plus('root@2001')
        
        # Test chit_db
        chit_conn_str = f"mysql+pymysql://root:{encoded_password}@localhost:3306/chit_db"
        chit_engine = create_engine(chit_conn_str)
        
        with chit_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ SQLAlchemy connection to chit_db: SUCCESS")
        
        # Test test database
        test_conn_str = f"mysql+pymysql://root:{encoded_password}@localhost:3306/test"
        test_engine = create_engine(test_conn_str)
        
        with test_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ SQLAlchemy connection to test: SUCCESS")
        
        chit_engine.dispose()
        test_engine.dispose()
        return True
        
    except Exception as e:
        print(f"❌ SQLAlchemy connection failed: {e}")
        return False

def test_credentials_from_db():
    """Test fetching and using credentials from database"""
    print("\n🔍 Testing credentials from ETL database...")
    
    try:
        # Connect to metadata database
        encoded_password = quote_plus('root@2001')
        metadata_conn_str = f"mysql+pymysql://root:{encoded_password}@localhost:3306/etl_config"
        metadata_engine = create_engine(metadata_conn_str)
        
        # Fetch credentials
        import pandas as pd
        
        # Get chit_db credentials
        query = """
            SELECT db_type, host1, port1, database1, username1, password1 
            FROM ETL_DATABASE_CRED
            WHERE LOWER(db_type) = 'chit_db'
            LIMIT 1
        """
        df = pd.read_sql(query, metadata_engine)
        
        if df.empty:
            print("❌ No credentials found for chit_db")
            return False
        
        cred = df.iloc[0].to_dict()
        print(f"✅ Found credentials for chit_db")
        print(f"   Host: {cred['host1']}")
        print(f"   Port: {cred['port1']}")
        print(f"   Database: {cred['database1']}")
        print(f"   Username: {cred['username1']}")
        
        # Test connection with fetched credentials
        from sqlalchemy import text
        encoded_cred_password = quote_plus(str(cred['password1']))
        test_conn_str = f"mysql+pymysql://{cred['username1']}:{encoded_cred_password}@{cred['host1']}:{cred['port1']}/{cred['database1']}"
        test_engine = create_engine(test_conn_str)
        
        with test_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM loans_loan"))
            count = result.fetchone()[0]
            print(f"✅ Connection using stored credentials: SUCCESS")
            print(f"   Found {count} rows in loans_loan table")
        
        test_engine.dispose()
        metadata_engine.dispose()
        return True
        
    except Exception as e:
        print(f"❌ Credential test failed: {e}")
        return False

def main():
    print("🧪 MySQL Connection Test for ETL System")
    print("=" * 50)
    
    # Test 1: Direct connection
    direct_ok = test_direct_connection()
    
    # Test 2: SQLAlchemy connection
    sqlalchemy_ok = test_sqlalchemy_connection()
    
    # Test 3: Credentials from database
    cred_ok = test_credentials_from_db()
    
    print("\n" + "=" * 50)
    print("📋 CONNECTION TEST SUMMARY")
    print("=" * 50)
    print(f"Direct PyMySQL: {'✅ PASS' if direct_ok else '❌ FAIL'}")
    print(f"SQLAlchemy: {'✅ PASS' if sqlalchemy_ok else '❌ FAIL'}")
    print(f"Stored Credentials: {'✅ PASS' if cred_ok else '❌ FAIL'}")
    
    if all([direct_ok, sqlalchemy_ok, cred_ok]):
        print("\n🎉 All connection tests passed! ETL should work now.")
        print("Run: python automation_script.py")
    else:
        print("\n⚠️ Some connection tests failed. Check your MySQL setup.")

if __name__ == "__main__":
    main()