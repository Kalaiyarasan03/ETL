# correct_password_test.py
import pymysql
import urllib.parse
from sqlalchemy import create_engine, text
import sys

def test_connection_with_password(password):
    """Test connection with the correct password"""
    
    print("🔍 Testing Connection with Correct Password")
    print("=" * 60)
    
    host = '192.168.2.76'
    port = 3306
    user = 'RRPLDATA'
    database = 'etl'
    
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"User: {user}")
    print(f"Password: {'*' * len(password)}")
    print(f"Database: {database}")
    print("-" * 60)
    
    # Test 1: Basic PyMySQL connection
    print("\n1️⃣ Testing PyMySQL connection...")
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4'
        )
        
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE(), USER(), NOW()")
        result = cursor.fetchone()
        print(f"✅ PyMySQL connection successful!")
        print(f"   Database: {result[0]}")
        print(f"   User: {result[1]}")
        print(f"   Server time: {result[2]}")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ PyMySQL connection failed: {e}")
        return False
    
    # Test 2: SQLAlchemy with URL encoding
    print("\n2️⃣ Testing SQLAlchemy connection...")
    try:
        # URL encode the password to handle special characters
        encoded_password = urllib.parse.quote_plus(password)
        conn_str = f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}"
        
        engine = create_engine(conn_str)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DATABASE(), USER()"))
            row = result.fetchone()
            print(f"✅ SQLAlchemy connection successful!")
            print(f"   Database: {row[0]}")
            print(f"   User: {row[1]}")
        
    except Exception as e:
        print(f"❌ SQLAlchemy connection failed: {e}")
        return False
    
    # Test 3: Check/Create EXECUTION_TRACK table
    print("\n3️⃣ Checking EXECUTION_TRACK table...")
    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=True
        )
        
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute("SHOW TABLES LIKE 'EXECUTION_TRACK'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            print("✅ EXECUTION_TRACK table already exists!")
        else:
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
            cursor.execute(create_table_sql)
            print("✅ EXECUTION_TRACK table created!")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Table operation failed: {e}")
        return False
    
    return True

def create_env_file_with_password(password):
    """Create .env file with the correct password"""
    print("\n4️⃣ Creating .env file...")
    
    env_content = f"""# Database Configuration
DB_HOST=192.168.2.76
DB_PORT=3306
DB_NAME=etl
DB_USER=RRPLDATA
DB_PASSWORD={password}

# Django Configuration
SECRET_KEY=django-insecure-i7y$g!om4$gg70d((kdpruam7z4^rn8uxrpd&0!b^1&0%z2b=@
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1,192.168.2.76,192.168.250.185
"""
    
    try:
        with open('.env', 'w') as f:
            f.write(env_content)
        print("✅ .env file created with correct password")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False

if __name__ == "__main__":
    print("🚀 ETL System - Connection Test with Correct Password")
    print("=" * 70)
    
    # Get the correct password from user
    print("Please enter the correct password for user RRPLDATA:")
    password = input("Password: ").strip()
    
    if not password:
        print("❌ No password entered!")
        sys.exit(1)
    
    print(f"\nTesting with password of length: {len(password)}")
    
    # Test connection
    if test_connection_with_password(password):
        # Create .env file
        create_env_file_with_password(password)
        
        print("\n" + "=" * 70)
        print("🎉 ALL TESTS PASSED!")
        print("✅ Database connection working")
        print("✅ EXECUTION_TRACK table ready")
        print("✅ .env file created")
        print("=" * 70)
        
        print("\n🎯 Next steps:")
        print("1. Run: python manage.py migrate")
        print("2. Run: python manage.py init_etl_system")
        print("3. Run: python manage.py runserver")
        
    else:
        print("\n❌ Connection test failed!")
        print("Please verify:")
        print("1. The password is correct")
        print("2. The user RRPLDATA exists")
        print("3. The user has access to the 'etl' database")
        sys.exit(1)