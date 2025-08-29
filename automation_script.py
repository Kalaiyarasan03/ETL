#!/usr/bin/env python3
"""
Multi-Database ETL Automation Script
Automatically executes ETL for all configured data sources
"""

import subprocess
import sys
import time
import pymysql
from sqlalchemy import create_engine, text

def get_all_data_sources():
    """Get all configured data sources from the metadata database"""
    try:
        # Connect to metadata database
        metadata_conn_str = "mysql+pymysql://root:root%402001@localhost:3306/etl_config"
        metadata_engine = create_engine(metadata_conn_str)
        
        # Get all data sources
        query = "SELECT SOURCE_ID, SOURCE_NM FROM ETL_SOURCE_INFO ORDER BY SOURCE_ID"
        
        import pandas as pd
        df = pd.read_sql(query, metadata_engine)
        metadata_engine.dispose()
        
        return df.to_dict('records')
        
    except Exception as e:
        print(f"Error fetching data sources: {str(e)}")
        return []

def execute_etl_for_source(source_id, source_name):
    """Execute ETL for a specific data source"""
    print(f"\n🚀 Starting ETL for: {source_name} (ID: {source_id})")
    print("=" * 50)
    
    try:
        result = subprocess.run([
            sys.executable, 'load_table.py', str(source_id)
        ], capture_output=True, text=True, timeout=600)
        
        if result.returncode == 0:
            print(f"✅ ETL completed successfully for {source_name}")
            if result.stdout:
                print("Output:", result.stdout)
            return True
        else:
            print(f"❌ ETL failed for {source_name}")
            if result.stderr:
                print("Error:", result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ ETL timed out for {source_name}")
        return False
    except Exception as e:
        print(f"💥 Unexpected error for {source_name}: {str(e)}")
        return False

def main():
    """Main automation function"""
    print("🔄 Multi-Database ETL Automation Started")
    print("=" * 50)
    
    # Get all configured data sources
    sources = get_all_data_sources()
    
    if not sources:
        print("❌ No data sources found. Please configure data sources first.")
        return
    
    print(f"📋 Found {len(sources)} data source(s) to process:")
    for source in sources:
        print(f"   - {source['SOURCE_NM']} (ID: {source['SOURCE_ID']})")
    
    print("\n🎯 Starting ETL execution for all sources...")
    
    success_count = 0
    failure_count = 0
    start_time = time.time()
    
    # Execute ETL for each data source
    for source in sources:
        source_id = source['SOURCE_ID']
        source_name = source['SOURCE_NM']
        
        success = execute_etl_for_source(source_id, source_name)
        
        if success:
            success_count += 1
        else:
            failure_count += 1
        
        # Add delay between executions
        if source != sources[-1]:  # Not the last source
            print("\n⏳ Waiting 5 seconds before next execution...")
            time.sleep(5)
    
    # Final summary
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "=" * 50)
    print("📊 ETL AUTOMATION SUMMARY")
    print("=" * 50)
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {failure_count}")
    print(f"📋 Total Sources: {len(sources)}")
    print(f"⏱️  Total Duration: {duration:.2f} seconds")
    
    if failure_count == 0:
        print("\n🎉 All ETL processes completed successfully!")
    else:
        print(f"\n⚠️  {failure_count} ETL process(es) failed. Check logs above.")

if __name__ == "__main__":
    main()