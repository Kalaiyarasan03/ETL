import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import sys
import os
from datetime import date
import logging

# Import Django settings to get database name
import django
from django.conf import settings

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'etl_manager.settings')  # Replace 'ETL_1' with your project name
django.setup()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_actual_db_engine_type(db_type):
    """
    Map custom database type names to actual database engine types
    This allows you to use custom names like 'mysql_wh', 'mysql_prod', etc.
    """
    # Convert to lowercase for comparison
    db_type_lower = db_type.lower().strip()
    
    # Define mapping of custom names to actual engine types
    CUSTOM_DB_TYPE_MAPPING = {
        # MySQL variants
        'mysql_wh': 'mysql',
        'mysql_warehouse': 'mysql',
        'mysql_prod': 'mysql',
        'mysql_staging': 'mysql',
        'mysql_dev': 'mysql',
        'vms': 'mysql',
        'chit_db': 'mysql',
        'warehouse_mysql': 'mysql',
        'prod_mysql': 'mysql',
        
        # PostgreSQL variants
        'postgres_wh': 'postgresql',
        'postgres_warehouse': 'postgresql',
        'postgres_prod': 'postgresql',
        'postgres_staging': 'postgresql',
        'pg_warehouse': 'postgresql',
        
        # Add more custom mappings as needed
        # 'your_custom_name': 'actual_engine_type',
    }
    
    # Check if it's a custom type that needs mapping
    if db_type_lower in CUSTOM_DB_TYPE_MAPPING:
        actual_type = CUSTOM_DB_TYPE_MAPPING[db_type_lower]
        logger.info(f"Mapped custom database type '{db_type}' to engine type '{actual_type}'")
        return actual_type
    
    # If not in custom mapping, return original (for standard types)
    return db_type_lower

def get_db_engine(credential_info):
    """Create database engine based on proper database type"""
    try:
        logger.info(f"Creating database engine for db_type: {credential_info.get('db_type', 'Unknown')}")
        
        # Get the actual engine type (handles custom names)
        actual_db_type = get_actual_db_engine_type(credential_info['db_type'])
        
        # Define valid database types and their aliases
        MYSQL_TYPES = ['mysql', 'mariadb']
        POSTGRESQL_TYPES = ['postgresql', 'postgres', 'postgre']
        SQLITE_TYPES = ['sqlite', 'sqlite3']
        
        if actual_db_type in POSTGRESQL_TYPES:
            try:
                import psycopg2
                conn_str = f"postgresql+psycopg2://{credential_info['username']}:{credential_info['password']}@{credential_info['host']}/{credential_info['database']}"
            except ImportError:
                raise ImportError("psycopg2 is required for PostgreSQL connections. Install with: pip install psycopg2-binary")
                
        elif actual_db_type in MYSQL_TYPES:
            try:
                import pymysql
                from urllib.parse import quote_plus
                port = credential_info.get('port', 3306)
                # URL encode the password to handle special characters like @
                encoded_password = quote_plus(str(credential_info['password']))
                conn_str = f"mysql+pymysql://{credential_info['username']}:{encoded_password}@{credential_info['host']}:{port}/{credential_info['database']}"
            except ImportError:
                raise ImportError("pymysql is required for MySQL connections. Install with: pip install pymysql")
                
        elif actual_db_type in SQLITE_TYPES:
            conn_str = f"sqlite:///{credential_info['database']}"
            
        else:
            # Create helpful error message with valid options
            valid_types = MYSQL_TYPES + POSTGRESQL_TYPES + SQLITE_TYPES
            raise ValueError(
                f"Unsupported database type: '{actual_db_type}' (mapped from '{credential_info['db_type']}'). "
                f"Valid engine types are: {', '.join(valid_types)}. "
                f"Add your custom type '{credential_info['db_type']}' to CUSTOM_DB_TYPE_MAPPING in get_actual_db_engine_type()."
            )
            
        logger.info(f"Created {actual_db_type} engine for database '{credential_info['database']}'")
        return create_engine(conn_str)
        
    except Exception as e:
        logger.error(f"Failed to create engine: {str(e)}")
        logger.error(f"Credential info: {credential_info}")
        raise

def fetch_credentials(metadata_engine, db_type, datasrc_id=None):
    """
    Fetch credentials using correct table and column names
    """
    try:
        # Using correct lowercase table name from your database
        query = f"""
            SELECT `db_type`, `db_role`, `host`, `port`, `database`, `username`, `password` 
            FROM `database_cred`
            WHERE LOWER(`db_type`) = LOWER('{db_type}')
        """
        logger.info(f"Executing credential query: {query}")
        df = pd.read_sql(query, metadata_engine)
        
        if df.empty:
            raise ValueError(f"No credentials found for db_type: {db_type}")
        
        logger.info(f"Found {len(df)} credential(s) for db_type: {db_type}")
        
        # If only one credential, use it
        if len(df) == 1:
            logger.info(f"Found single credential for db_type: {db_type}")
            return df.iloc[0].to_dict()
        
        # If datasrc_id provided, try to build expected role name
        if datasrc_id:
            try:
                # Using correct lowercase table name from your database
                source_query = f"""
                    SELECT SOURCE_NM 
                    FROM source_info 
                    WHERE SOURCE_ID = {datasrc_id}
                """
                source_df = pd.read_sql(source_query, metadata_engine)
                
                if not source_df.empty:
                    datasrc_name = source_df.iloc[0]['SOURCE_NM'].lower()
                    
                    # Clean the datasource name (remove spaces, special chars)
                    clean_name = ''.join(c for c in datasrc_name if c.isalnum() or c == '_')
                    
                    # Expected role names to try
                    expected_roles = [
                        f"source_{clean_name}",
                        f"{clean_name}_source",
                        f"source_{clean_name.replace('_', '')}",
                        clean_name,
                        "source"  # fallback to generic source
                    ]
                    
                    # Try each expected role
                    for expected_role in expected_roles:
                        matching_creds = df[df['db_role'].str.lower() == expected_role.lower()]
                        if not matching_creds.empty:
                            logger.info(f"Found credential with expected role: {expected_role} for db_type: {db_type}")
                            return matching_creds.iloc[0].to_dict()
                    
                    logger.info(f"No credential found with expected roles: {expected_roles} for db_type: {db_type}")
                    
            except Exception as e:
                logger.warning(f"Could not determine expected role for datasrc_id {datasrc_id}: {str(e)}")
        
        # Fallback: Use any credential with 'source' or 'target' or 'destination' in the role
        role_patterns = ['source', 'target', 'destination']
        for pattern in role_patterns:
            matching_creds = df[df['db_role'].str.contains(pattern, case=False, na=False)]
            if not matching_creds.empty:
                cred_dict = matching_creds.iloc[0].to_dict()
                logger.info(f"Using fallback credential with role: {cred_dict['db_role']} for db_type: {db_type}")
                return cred_dict
        
        # Final fallback: First credential
        logger.warning(f"Using first available credential for {db_type}")
        return df.iloc[0].to_dict()
        
    except Exception as e:
        logger.error(f"Failed to fetch credentials for {db_type}: {str(e)}")
        raise

def is_already_loaded(metadata_engine, srctbl_id):
    """Check if ETL has already been executed today for this source table"""
    try:
        today = date.today().isoformat()
        # Using existing uppercase table name 'EXECUTION_TRACK' as seen in your database
        query = text("""
            SELECT 1 FROM EXECUTION_TRACK
            WHERE EXECUTION_DT = :today AND SRCTBL_ID = :srctbl_id AND COMPLETE_TRACK = 'Y'
            LIMIT 1
        """)
        with metadata_engine.connect() as conn:
            result = conn.execute(query, {"today": today, "srctbl_id": srctbl_id}).fetchone()
        return result is not None
    except Exception as e:
        logger.warning(f"Could not check execution status for SRCTBL_ID {srctbl_id}: {str(e)}")
        return False

def mark_as_loaded(metadata_engine, srctbl_id, record_count=0):
    """Mark the ETL process as completed for this source table with record count"""
    try:
        today = date.today().isoformat()
        
        # Using existing uppercase table name 'EXECUTION_TRACK' as seen in your database
        create_table_query = text("""
            CREATE TABLE IF NOT EXISTS EXECUTION_TRACK (
                EXECUTION_DT DATE,
                SRCTBL_ID INT,
                COMPLETE_TRACK CHAR(1) DEFAULT 'N',
                REC_LOAD_COUNT BIGINT DEFAULT NULL,
                LAST_EXEC_DT DATE DEFAULT '2000-01-01',
                NEXT_EXEC_DT DATE DEFAULT '2099-12-31',
                PRIMARY KEY (EXECUTION_DT, SRCTBL_ID)
            )
        """)
        
        check_query = text("""
            SELECT 1 FROM EXECUTION_TRACK
            WHERE EXECUTION_DT = :today AND SRCTBL_ID = :srctbl_id
        """)
        
        with metadata_engine.begin() as conn:
            # Create table if not exists
            conn.execute(create_table_query)
            
            existing = conn.execute(check_query, {"today": today, "srctbl_id": srctbl_id}).fetchone()
            
            if existing:
                # Update existing record
                update_query = text("""
                    UPDATE EXECUTION_TRACK 
                    SET COMPLETE_TRACK = 'Y', REC_LOAD_COUNT = :record_count, LAST_EXEC_DT = :today
                    WHERE EXECUTION_DT = :today AND SRCTBL_ID = :srctbl_id
                """)
                conn.execute(update_query, {"today": today, "srctbl_id": srctbl_id, "record_count": record_count})
            else:
                # Insert new record
                insert_query = text("""
                    INSERT INTO EXECUTION_TRACK (EXECUTION_DT, SRCTBL_ID, COMPLETE_TRACK, REC_LOAD_COUNT, LAST_EXEC_DT)
                    VALUES (:today, :srctbl_id, 'Y', :record_count, :today)
                """)
                conn.execute(insert_query, {"today": today, "srctbl_id": srctbl_id, "record_count": record_count})
                
        logger.info(f"Marked SRCTBL_ID {srctbl_id} as completed for {today} with {record_count} records")
        
    except Exception as e:
        logger.error(f"Failed to mark SRCTBL_ID {srctbl_id} as loaded: {str(e)}")

def mark_as_failed(metadata_engine, srctbl_id, error_message=""):
    """Mark the ETL process as failed for this source table"""
    try:
        today = date.today().isoformat()
        
        # Using existing uppercase table name 'EXECUTION_TRACK' as seen in your database
        create_table_query = text("""
            CREATE TABLE IF NOT EXISTS EXECUTION_TRACK (
                EXECUTION_DT DATE,
                SRCTBL_ID INT,
                COMPLETE_TRACK CHAR(1) DEFAULT 'N',
                REC_LOAD_COUNT BIGINT DEFAULT NULL,
                LAST_EXEC_DT DATE DEFAULT '2000-01-01',
                NEXT_EXEC_DT DATE DEFAULT '2099-12-31',
                PRIMARY KEY (EXECUTION_DT, SRCTBL_ID)
            )
        """)
        
        with metadata_engine.begin() as conn:
            conn.execute(create_table_query)
            
            check_query = text("""
                SELECT 1 FROM EXECUTION_TRACK
                WHERE EXECUTION_DT = :today AND SRCTBL_ID = :srctbl_id
            """)
            existing = conn.execute(check_query, {"today": today, "srctbl_id": srctbl_id}).fetchone()
            
            if existing:
                # Update existing record
                update_query = text("""
                    UPDATE EXECUTION_TRACK 
                    SET COMPLETE_TRACK = 'N', REC_LOAD_COUNT = 0, LAST_EXEC_DT = :today
                    WHERE EXECUTION_DT = :today AND SRCTBL_ID = :srctbl_id
                """)
                conn.execute(update_query, {"today": today, "srctbl_id": srctbl_id})
            else:
                # Insert new record with all required fields
                insert_query = text("""
                    INSERT INTO EXECUTION_TRACK (EXECUTION_DT, SRCTBL_ID, COMPLETE_TRACK, REC_LOAD_COUNT, LAST_EXEC_DT, NEXT_EXEC_DT)
                    VALUES (:today, :srctbl_id, 'N', 0, :today, '2099-12-31')
                """)
                conn.execute(insert_query, {"today": today, "srctbl_id": srctbl_id})
                
        logger.error(f"Marked SRCTBL_ID {srctbl_id} as failed for {today}: {error_message}")
        
    except Exception as e:
        logger.error(f"Failed to mark SRCTBL_ID {srctbl_id} as failed: {str(e)}")

def check_dependencies():
    """Check if required dependencies are available"""
    missing_deps = []
    
    try:
        import pymysql
    except ImportError:
        missing_deps.append("pymysql (for MySQL connections)")
    
    if missing_deps:
        logger.warning(f"Missing optional dependencies: {', '.join(missing_deps)}")
        logger.info("Install missing dependencies as needed for your database connections")

def main(datasrc_id):
    """Main ETL execution function - Using actual table names from your database"""
    logger.info(f"Starting ETL process for DATASRC_ID: {datasrc_id}")
    
    # Check dependencies
    check_dependencies()
    
    try:
        # Connect to metadata database - using Django database name from settings
        try:
            import pymysql
            from urllib.parse import quote_plus
            
            # Get database name from Django settings
            db_name = settings.DATABASES['default']['NAME']
            
            # URL encode the password to handle special characters like @
            encoded_password = quote_plus('root@2001')
            metadata_conn_str = f"mysql+pymysql://root:{encoded_password}@localhost:3306/{db_name}"
            metadata_engine = create_engine(metadata_conn_str)
            
            logger.info(f"Using database from Django settings: {db_name}")
            
        except ImportError:
            logger.error("pymysql is required for metadata database connection. Install with: pip install pymysql")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to get database name from Django settings: {str(e)}")
            sys.exit(1)
            
        logger.info(f"Connected to metadata database: {settings.DATABASES['default']['NAME']}")

        # Test metadata connection
        try:
            with metadata_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            logger.error(f"Failed to connect to metadata database: {str(e)}")
            logger.error("Please check your database connection settings")
            sys.exit(1)

        # Using correct lowercase table name from your database
        query = f"""
            SELECT * FROM srctbl_info 
            WHERE DATASRC_ID = {datasrc_id}
        """
        srctbl_info_df = pd.read_sql(query, metadata_engine)

        if srctbl_info_df.empty:
            logger.warning(f"No tables found for DATASRC_ID = {datasrc_id}")
            print(f"No tables found for DATASRC_ID = {datasrc_id}")
            return

        logger.info(f"Found {len(srctbl_info_df)} table(s) to process")
        success_count = 0
        error_count = 0

        for _, row in srctbl_info_df.iterrows():
            srctbl_id = row['SRCTBL_ID']
            
            try:
                # Check if already loaded today
                if is_already_loaded(metadata_engine, srctbl_id):
                    logger.info(f"Skipping SRCTBL_ID {srctbl_id} (already loaded today)")
                    print(f"⏭️  Skipping SRCTBL_ID {srctbl_id} (already loaded today)")
                    continue

                src_db_type = row['SRC_DATABASE']
                tgt_db_type = row['TGT_DATABASE']
                src_schema = row['SRC_SCHEMA']
                tgt_schema = row['TGT_SCHEMA']
                src_table = row['SRC_TABLENAME']
                tgt_table = row['TGT_TABLENAME']
                ref_frequency = row.get('REF_FRQNCY', 'DAILY')

                logger.info(f"Processing table: {src_schema}.{src_table} -> {tgt_schema}.{tgt_table} (Frequency: {ref_frequency})")
                logger.info(f"Using source db_type: {src_db_type}, target db_type: {tgt_db_type}")

                # Get database credentials
                try:
                    src_cred = fetch_credentials(metadata_engine, src_db_type, datasrc_id)
                    tgt_cred = fetch_credentials(metadata_engine, tgt_db_type)
                    logger.info(f"Successfully fetched credentials for source: {src_db_type}, target: {tgt_db_type}")
                except Exception as e:
                    logger.error(f"Failed to fetch credentials: {str(e)}")
                    print(f"❌ Failed to fetch credentials for {src_db_type} or {tgt_db_type}: {str(e)}")
                    mark_as_failed(metadata_engine, srctbl_id, f"Credential error: {str(e)}")
                    error_count += 1
                    continue

                # Create database engines (now handles custom database type names)
                try:
                    logger.info(f"Creating source engine with credentials: {src_cred}")
                    src_engine = get_db_engine(src_cred)
                    logger.info(f"Creating target engine with credentials: {tgt_cred}")
                    tgt_engine = get_db_engine(tgt_cred)
                    logger.info("Successfully created database engines")
                except Exception as e:
                    logger.error(f"Failed to create database engines: {str(e)}")
                    print(f"❌ Database connection error: {str(e)}")
                    mark_as_failed(metadata_engine, srctbl_id, f"Connection error: {str(e)}")
                    error_count += 1
                    continue

                # Handle schema and table name inference
                if tgt_schema and tgt_schema.upper() == 'INFER_SRC':
                    tgt_schema = src_schema.strip() if src_schema else None
                if tgt_table and tgt_table.upper() == 'INFER_SRC':
                    tgt_table = src_table.strip()

                # Build source query with proper table name handling
                if src_schema and src_schema.strip():
                    src_query = f"SELECT * FROM `{src_schema}`.`{src_table}`"
                else:
                    src_query = f"SELECT * FROM `{src_table}`"

                logger.info(f"Executing source query: {src_query}")
                
                # Extract data from source
                try:
                    df = pd.read_sql(src_query, src_engine)
                    logger.info(f"Extracted {len(df)} rows from source")
                except Exception as e:
                    logger.error(f"Failed to extract data from source: {str(e)}")
                    print(f"❌ Source data extraction failed: {str(e)}")
                    
                    # Additional debugging for table existence
                    try:
                        if src_schema and src_schema.strip():
                            check_query = f"SHOW TABLES FROM `{src_cred['database']}` LIKE '{src_table}'"
                        else:
                            check_query = f"SHOW TABLES FROM `{src_cred['database']}` LIKE '{src_table}'"
                        
                        with src_engine.connect() as conn:
                            result = conn.execute(text(check_query))
                            tables = result.fetchall()
                        
                        if not tables:
                            logger.error(f"Table '{src_table}' does not exist in database '{src_cred['database']}'")
                            print(f"❌ Table '{src_table}' not found in database '{src_cred['database']}'")
                        else:
                            logger.info(f"Table '{src_table}' exists but query failed")
                            
                    except Exception as debug_e:
                        logger.error(f"Debug check failed: {str(debug_e)}")
                    
                    mark_as_failed(metadata_engine, srctbl_id, f"Source extraction error: {str(e)}")
                    error_count += 1
                    continue

                if df.empty:
                    logger.warning(f"No data found in source table {src_schema}.{src_table}")
                    print(f"⚠️  No data found in {src_schema}.{src_table}")
                    # Mark as successful with 0 records
                    mark_as_loaded(metadata_engine, srctbl_id, 0)
                    success_count += 1
                    continue

                # Load data to target
                try:
                    df.to_sql(
                        name=tgt_table, 
                        con=tgt_engine, 
                        schema=tgt_schema, 
                        if_exists='replace', 
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    record_count = len(df)
                    success_count += 1
                    logger.info(f"Successfully loaded {record_count} rows to {tgt_schema}.{tgt_table}")
                    print(f"✅ Loaded {record_count} rows from {src_db_type}.{src_table} to {tgt_db_type}.{tgt_table}")

                    # Mark as completed with record count
                    mark_as_loaded(metadata_engine, srctbl_id, record_count)
                    
                except Exception as e:
                    logger.error(f"Failed to load data to target: {str(e)}")
                    print(f"❌ Target data loading failed: {str(e)}")
                    mark_as_failed(metadata_engine, srctbl_id, f"Target load error: {str(e)}")
                    error_count += 1
                    continue

                # Close connections for this iteration
                src_engine.dispose()
                tgt_engine.dispose()

            except Exception as e:
                error_count += 1
                error_msg = f"Error processing SRCTBL_ID {srctbl_id}: {str(e)}"
                logger.error(error_msg)
                print(f"❌ {error_msg}")
                mark_as_failed(metadata_engine, srctbl_id, str(e))
                continue

        # Summary
        total_tables = len(srctbl_info_df)
        logger.info(f"ETL completed. Success: {success_count}, Errors: {error_count}, Total: {total_tables}")
        print(f"\n📊 ETL Summary for DATASRC_ID {datasrc_id}:")
        print(f"   ✅ Successful: {success_count}")
        print(f"   ❌ Errors: {error_count}")
        print(f"   📋 Total: {total_tables}")

    except Exception as e:
        error_msg = f"Fatal error in ETL process: {str(e)}"
        logger.error(error_msg)
        print(f"💥 {error_msg}")
        sys.exit(1)
    
    finally:
        # Clean up metadata connection
        try:
            metadata_engine.dispose()
        except:
            pass

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load_table.py <DATASRC_ID>")
        print("Example: python load_table.py 1")
        sys.exit(1)

    try:
        datasrc_id = int(sys.argv[1])
    except ValueError:
        print("❌ Error: DATASRC_ID must be an integer.")
        sys.exit(1)

    if datasrc_id <= 0:
        print("❌ Error: DATASRC_ID must be a positive integer.")
        sys.exit(1)

    main(datasrc_id)