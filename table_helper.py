#!/usr/bin/env python3
"""
Helper script to show available tables in chit_db
and provide guidance on creating table mappings
"""

import pymysql

def show_chit_db_tables():
    """Display all tables in chit_db"""
    try:
        # Connect to chit_db
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='root@2001',
            database='chit_db'
        )
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        if tables:
            print("📋 Tables found in chit_db:")
            print("=" * 40)
            for i, table in enumerate(tables, 1):
                table_name = table[0]
                print(f"{i}. {table_name}")
                
                # Get row count for each table
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    print(f"   └── Rows: {count}")
                except:
                    print(f"   └── Rows: Unable to count")
            
            print("\n" + "=" * 40)
            print("📝 To create table mappings:")
            print("1. Go to your web interface: http://localhost:8000")
            print("2. Navigate to 'Tables' → 'Add Table Mapping'")
            print("3. For each table above, create a mapping:")
            print("   - Data Source ID: 1")
            print("   - Source Database: chit_db")
            print("   - Source Table Name: [table_name_from_above]")
            print("   - Target Database: test")
            print("   - Target Table Name: [same_or_different_name]")
            print("   - Source Layout ID: 1")
            print("   - Target Layout ID: 1")
            
        else:
            print("❌ No tables found in chit_db")
            print("Make sure chit_db exists and has tables")
        
        conn.close()
        
    except pymysql.Error as e:
        print(f"❌ Error connecting to chit_db: {e}")
        print("Possible issues:")
        print("1. chit_db database doesn't exist")
        print("2. MySQL credentials are incorrect")
        print("3. MySQL server is not running")

def check_existing_mappings():
    """Check if any table mappings already exist"""
    try:
        # Connect to etl_config database
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='root@2001',
            database='etl_config'
        )
        cursor = conn.cursor()
        
        # Check existing mappings
        cursor.execute("SELECT COUNT(*) FROM ETL_TABLE_INFO WHERE DATASRC_ID = 1")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"\n✅ Found {count} existing table mapping(s) for Data Source ID 1")
            cursor.execute("""
                SELECT SRCTBL_ID, SRC_TABLENAME, TGT_TABLENAME 
                FROM ETL_TABLE_INFO 
                WHERE DATASRC_ID = 1
            """)
            mappings = cursor.fetchall()
            
            print("Current mappings:")
            for mapping in mappings:
                print(f"  - {mapping[1]} → {mapping[2]} (ID: {mapping[0]})")
        else:
            print("\n❌ No table mappings found for Data Source ID 1")
            print("You need to create table mappings before running ETL")
        
        conn.close()
        
    except pymysql.Error as e:
        print(f"❌ Error checking mappings: {e}")

if __name__ == "__main__":
    print("🔍 ETL Configuration Helper")
    print("=" * 50)
    
    # Show tables in chit_db
    show_chit_db_tables()
    
    # Check existing mappings
    check_existing_mappings()
    
    print("\n🚀 Next steps:")
    print("1. Create table mappings via web interface")
    print("2. Run: python load_table.py 1")