# debug_connection.py
import pymysql
from sqlalchemy import create_engine, text
import sys
import urllib.parse

def debug_connection():
    """Debug the connection step by step"""
    
    print("🔍 Debugging MySQL Connection...")
    print("=" * 50)
    
    # Connection parameters
    host = '192.168.2.76'
    port = 3306
    user = 'RRPLDATA'
    password = 'Jerish'
    
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"User: {user}")
    print(f"Password: {'*' * len(password)}")
    print("-" * 50)
    
    # Step 1: Test basic connection without database
    print("Step 1: Testing basic connection (no database specified)...")
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset='utf8mb4'
        )
        print("✅ Basic connection successful!")
        
        # Check what databases exist
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"📋 Available databases: {[db[0] for db in databases]}")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Basic connection failed: {e}")
        print("\n💡 Troubleshooting tips:")
        print("1. Check if the server IP is correct: 192.168.2.76")
        print("2. Verify the username: RRPLDATA")
        print("3. Verify the password: $t06ngPa$$w0rd")
        print("4. Check if MySQL is running on the server")
        print("5. Check firewall settings")
        return False
    
    # Step 2: Try to create etl database
    print("\nStep 2: Creating etl database...")
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset='utf8mb4'
        )
        
        cursor = connection.cursor()
        
        # Check if etl exists
        cursor.execute("SHOW DATABASES LIKE 'etl'")
        result = cursor.fetchone()
        
        if result:
            print("✅ Database 'etl' already exists!")
        else:
            print("Creating database 'etl'...")
            cursor.execute("CREATE DATABASE etl CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            print("✅ Database 'etl' created successfully!")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Database creation failed: {e}")
        return False
    
    # Step 3: Test connection to etl
    print("\nStep 3: Testing connection to etl...")
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database='etl',
            charset='utf8mb4'
        )
        print("✅ Connection to etl successful!")
        
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()
        print(f"📍 Currently connected to: {current_db[0]}")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Connection to etl failed: {e}")
        return False
    
    # Step 4: Test SQLAlchemy connection
    print("\nStep 4: Testing SQLAlchemy connection...")
    try:
        # URL encode the password to handle special characters
        encoded_password = urllib.parse.quote_plus(password)
        conn_str = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/etl"
        
        print(f"Connection string: mysql+pymysql://{user}:***@{host}:{port}/etl")
        
        engine = create_engine(conn_str)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DATABASE(), USER(), VERSION()"))
            row = result.fetchone()
            print("✅ SQLAlchemy connection successful!")
            print(f"   Database: {row[0]}")
            print(f"   User: {row[1]}")
            print(f"   MySQL Version: {row[2]}")
            
    except Exception as e:
        print(f"❌ SQLAlchemy connection failed: {e}")
        print(f"   Full error: {str(e)}")
        return False
    
    # Step 5: Create EXECUTION_TRACK table
    print("\nStep 5: Setting up EXECUTION_TRACK table...")
    try:
        encoded_password = urllib.parse.quote_plus(password)
        conn_str = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/etl"
        engine = create_engine(conn_str)
        
        with engine.connect() as conn:
            # Check if table exists
            result = conn.execute(text("SHOW TABLES LIKE 'EXECUTION_TRACK'"))
            table_exists = result.fetchone()
            
            if not table_exists:
                print("Creating EXECUTION_TRACK table...")
                create_table_sql = """
                CREATE TABLE EXECUTION_TRACK (
                    EXECUTION_DT DATE NOT NULL,
                    SRCTBL_ID INT NOT NULL,
                    COMPLETE_TRACK CHAR(1) DEFAULT 'N',
                    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (EXECUTION_DT, SRCTBL_ID),
                    INDEX idx_execution_dt (EXECUTION_DT),
                    INDEX idx_srctbl_id (SRCTBL_ID)
                )
                """
                conn.execute(text(create_table_sql))
                print("✅ EXECUTION_TRACK table created!")
            else:
                print("✅ EXECUTION_TRACK table already exists!")
                
    except Exception as e:
        print(f"❌ Table creation failed: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 All connection tests passed!")
    print("✅ Server: 192.168.2.76:3306")
    print("✅ User: RRPLDATA")
    print("✅ Database: etl")
    print("✅ Tables: Ready")
    print("=" * 50)
    
    return True

def test_env_file():
    """Test reading from .env file"""
    print("\n🔍 Testing .env file configuration...")
    
    try:
        import os
        from pathlib import Path
        
        # Check if .env file exists
        env_path = Path('.env')
        if not env_path.exists():
            print("❌ .env file not found!")
            print("Create .env file with:")
            print("DB_HOST=192.168.2.76")
            print("DB_PORT=3306")
            print("DB_NAME=etl")
            print("DB_USER=RRPLDATA")
            print("DB_PASSWORD=$t06ngPa$$w0rd")
            return False
        
        # Load .env file
        with open(env_path) as f:
            env_vars = {}
            for line in f:
                if line.strip() and not line.startswith('#') and '=' in line:
                    key, value = line.strip().split('=', 1)
                    env_vars[key] = value
                    os.environ[key] = value
        
        print("✅ .env file loaded successfully!")
        print(f"   DB_HOST: {env_vars.get('DB_HOST', 'Not set')}")
        print(f"   DB_PORT: {env_vars.get('DB_PORT', 'Not set')}")
        print(f"   DB_NAME: {env_vars.get('DB_NAME', 'Not set')}")
        print(f"   DB_USER: {env_vars.get('DB_USER', 'Not set')}")
        print(f"   DB_PASSWORD: {'*' * len(env_vars.get('DB_PASSWORD', ''))}")
        
        # Test connection using env vars
        import urllib.parse
        password = env_vars.get('DB_PASSWORD', '')
        encoded_password = urllib.parse.quote_plus(password)
        conn_str = f"mysql+pymysql://{env_vars.get('DB_USER')}:{encoded_password}@{env_vars.get('DB_HOST')}:{env_vars.get('DB_PORT')}/{env_vars.get('DB_NAME')}"
        
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Connection using .env variables successful!")
            
        return True
        
    except Exception as e:
        print(f"❌ .env file test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 ETL System Connection Debugger")
    print("=" * 60)
    
    # Test direct connection
    success = debug_connection()
    
    if success:
        # Test .env file
        test_env_file()
        
        print("\n🎯 Next Steps:")
        print("1. Ensure .env file is created with correct values")
        print("2. Run: python manage.py migrate")
        print("3. Run: python manage.py init_etl_system")
        print("4. Run: python manage.py runserver")
        
    else:
        print("\n❌ Connection failed. Please check your server configuration.")
        sys.exit(1)