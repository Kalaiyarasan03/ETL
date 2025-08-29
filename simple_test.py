#!/usr/bin/env python3
"""
Enhanced debug script for ETL Data Viewer issues
"""

import os
import sys
import django
from pathlib import Path

# Add the project directory to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'etl_manager.settings')
django.setup()

from django.db import connection
from etl_system.models import TableInfo, DatabaseCred, TableSchema
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

def check_credential_mapping():
    """Check if all target databases have corresponding credentials"""
    print("\n🔍 Checking credential mapping...")
    
    try:
        tables = TableInfo.objects.all()
        creds = DatabaseCred.objects.all()
        
        # Get unique target databases
        target_dbs = set()
        for table in tables:
            if table.TGT_DATABASE:
                target_dbs.add(table.TGT_DATABASE.lower())
        
        # Get available credential types
        cred_types = set()
        for cred in creds:
            if cred.db_type:
                cred_types.add(cred.db_type.lower())
        
        print(f"📋 Target databases needed: {sorted(target_dbs)}")
        print(f"🗝️  Available credential types: {sorted(cred_types)}")
        
        missing_creds = target_dbs - cred_types
        if missing_creds:
            print(f"❌ Missing credentials for: {sorted(missing_creds)}")
            print("\n💡 Solutions:")
            for missing in missing_creds:
                print(f"   1. Add credential entry for '{missing}' database type")
                print(f"   2. Or update table mappings to use existing credential type")
            return False
        else:
            print("✅ All target databases have matching credentials")
            return True
            
    except Exception as e:
        print(f"❌ Error checking credential mapping: {str(e)}")
        return False

def show_table_credential_pairs():
    """Show which tables map to which credentials"""
    print("\n🔍 Table to Credential mapping:")
    
    try:
        tables = TableInfo.objects.all()
        
        for table in tables:
            cred = DatabaseCred.objects.filter(db_type__iexact=table.TGT_DATABASE).first()
            status = "✅" if cred else "❌"
            
            print(f"   {status} Table {table.SRCTBL_ID}: {table.TGT_DATABASE} -> ", end="")
            if cred:
                print(f"Found: {cred.db_type} ({cred.host1}:{cred.port1}/{cred.database1})")
            else:
                print("No matching credential found")
                
    except Exception as e:
        print(f"❌ Error showing mappings: {str(e)}")

def suggest_fixes():
    """Suggest specific fixes for the current setup"""
    print("\n🔧 Suggested fixes for your setup:")
    
    try:
        tables = TableInfo.objects.all()
        creds = DatabaseCred.objects.all()
        
        # Check each table
        for table in tables:
            cred = DatabaseCred.objects.filter(db_type__iexact=table.TGT_DATABASE).first()
            if not cred:
                print(f"\n❌ Table {table.SRCTBL_ID} ({table.TGT_TABLENAME}):")
                print(f"   Target DB: {table.TGT_DATABASE}")
                print(f"   Problem: No credential found for '{table.TGT_DATABASE}'")
                
                # Check if there's a credential for the actual database name
                matching_db_cred = DatabaseCred.objects.filter(database1__iexact=table.TGT_DATABASE).first()
                if matching_db_cred:
                    print(f"   💡 Found credential with database name '{table.TGT_DATABASE}' but type '{matching_db_cred.db_type}'")
                    print(f"   🔧 Fix 1: Change table's TGT_DATABASE from '{table.TGT_DATABASE}' to '{matching_db_cred.db_type}'")
                    print(f"      SQL: UPDATE ETL_TABLE_INFO SET TGT_DATABASE = '{matching_db_cred.db_type}' WHERE SRCTBL_ID = {table.SRCTBL_ID};")
                
                print(f"   🔧 Fix 2: Add new credential entry:")
                print(f"      SQL: INSERT INTO ETL_DATABASE_CRED (db_type, db_role, host1, port1, database1, username1, password1)")
                print(f"           VALUES ('{table.TGT_DATABASE}', 'target', 'localhost', 3306, '{table.TGT_DATABASE}', 'root', 'root@2001');")
                
    except Exception as e:
        print(f"❌ Error generating suggestions: {str(e)}")

def test_manual_connection():
    """Test manual connection with known good credentials"""
    print("\n🔍 Testing manual connection to 'test' database...")
    
    try:
        # Try to connect to test database directly
        from urllib.parse import quote_plus
        encoded_password = quote_plus('root@2001')
        conn_str = f"mysql+pymysql://root:{encoded_password}@localhost:3306/test"
        
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        print("✅ Direct connection to 'test' database: OK")
        
        # Test table access
        with engine.connect() as conn:
            # Check if loans_loan table exists
            check_table = "SHOW TABLES LIKE 'loans_loan'"
            result = conn.execute(text(check_table))
            tables = result.fetchall()
            
            if tables:
                print("✅ loans_loan table exists in test database")
                
                # Try to count rows
                count_result = conn.execute(text("SELECT COUNT(*) FROM loans_loan"))
                count = count_result.fetchone()[0]
                print(f"✅ Table has {count} rows")
            else:
                print("❌ loans_loan table does not exist in test database")
                
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"❌ Manual connection failed: {str(e)}")
        return False

def main():
    """Enhanced diagnostic with credential focus"""
    print("🔧 Enhanced ETL Data Viewer Diagnostic Tool")
    print("=" * 50)
    
    # Focus on credential issues
    check_credential_mapping()
    show_table_credential_pairs()
    suggest_fixes()
    test_manual_connection()
    
    print("\n" + "=" * 50)
    print("🎯 QUICK FIX for your setup:")
    print("Run this SQL to fix the credential mapping:")
    print("INSERT INTO ETL_DATABASE_CRED (db_type, db_role, host1, port1, database1, username1, password1)")
    print("VALUES ('test', 'target', 'localhost', 3306, 'test', 'root', 'root@2001');")

if __name__ == "__main__":
    main()