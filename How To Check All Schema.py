import pandas as pd
from hdbcli import dbapi
import os
import logging
from datetime import datetime, date

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"sap_hana_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('sap_hana_extract')

def connect_to_hana():
    """Connect to SAP HANA database using hardcoded connection parameters"""
    try:
        # Hardcoded connection parameters - UPDATE THESE WITH YOUR ACTUAL VALUES
        host = "15.6.21.246"          # Your SAP HANA server IP
        port = 30015                   # Your SAP HANA port
        user = "POWERBI3"              # Your username - confirmed working
        password = "Som@2025"     # Your actual password - replace this!
        
        print(f"Connecting to SAP HANA at {host}:{port} with user {user}")
        
        # Use the connection method that worked in your test
        connection = dbapi.connect(
            address=host,
            port=port,
            user=user,
            password=password
        )
        logger.info("Successfully connected to SAP HANA")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to SAP HANA: {str(e)}")
        if hasattr(e, 'errorcode'):
            logger.error(f"Error code: {e.errorcode}")
        if hasattr(e, 'errortext'):
            logger.error(f"Error text: {e.errortext}")
        raise

def list_schemas(connection):
    """List all available schemas that the user can access"""
    try:
        cursor = connection.cursor()
        query = "SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE HAS_PRIVILEGES = 'TRUE' ORDER BY SCHEMA_NAME"
        cursor.execute(query)
        schemas = [row[0] for row in cursor.fetchall()]
        return schemas
    except Exception as e:
        print(f"Error listing schemas: {str(e)}")
        return []
    finally:
        if cursor:
            cursor.close()

def list_tables_in_schema(connection, schema):
    """List all tables in a specific schema"""
    try:
        cursor = connection.cursor()
        query = f"SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = '{schema}' ORDER BY TABLE_NAME"
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    except Exception as e:
        print(f"Error listing tables in schema {schema}: {str(e)}")
        return []
    finally:
        if cursor:
            cursor.close()

def find_mard_table(connection):
    """Find the MARD table in all accessible schemas"""
    schemas = list_schemas(connection)
    print(f"Found {len(schemas)} accessible schemas.")
    
    for schema in schemas:
        print(f"Checking schema: {schema}")
        tables = list_tables_in_schema(connection, schema)
        
        # Look for MARD table variants (case insensitive)
        mard_tables = [table for table in tables if 'MARD' in table.upper()]
        
        if mard_tables:
            print(f"Found potential MARD tables in schema {schema}: {mard_tables}")
            return schema, mard_tables
    
    return None, []

def check_all_sap_tables(connection):
    """Try to find common SAP table names across all schemas"""
    schemas = list_schemas(connection)
    sap_tables = ['MARD', 'MAKT', 'MSEG', 'MKPF', 'MARC', 'MARA']
    
    found_tables = {}
    
    for schema in schemas:
        tables = list_tables_in_schema(connection, schema)
        for sap_table in sap_tables:
            matches = [table for table in tables if sap_table.upper() in table.upper()]
            if matches:
                if schema not in found_tables:
                    found_tables[schema] = {}
                found_tables[schema][sap_table] = matches
    
    return found_tables

def main():
    """Main function"""
    connection = None  # Initialize connection to None to avoid UnboundLocalError
    try:
        # Connect to SAP HANA using hardcoded parameters
        connection = connect_to_hana()
        
        print("\nStep 1: Listing all schemas you have access to...")
        schemas = list_schemas(connection)
        print(f"You have access to {len(schemas)} schemas:")
        for i, schema in enumerate(schemas[:10], 1):  # Show first 10 schemas
            print(f"  {i}. {schema}")
        if len(schemas) > 10:
            print(f"  ... and {len(schemas) - 10} more")
        
        print("\nStep 2: Looking for MARD table in your schemas...")
        schema, mard_tables = find_mard_table(connection)
        
        if schema:
            print(f"\nFound MARD table in schema: {schema}")
            print(f"MARD table variants: {mard_tables}")
        else:
            print("\nCould not find MARD table in any accessible schema.")
            
        print("\nStep 3: Checking for all common SAP tables...")
        found_tables = check_all_sap_tables(connection)
        
        if found_tables:
            print("\nFound SAP tables in the following schemas:")
            for schema, tables in found_tables.items():
                print(f"\nSchema: {schema}")
                for sap_table, matches in tables.items():
                    print(f"  {sap_table}: {matches}")
        else:
            print("\nCould not find any common SAP tables in your accessible schemas.")
        
        print("\nPlease use this information to update your script with the correct schema and table names.")
        print("Example usage: SELECT * FROM SCHEMA_NAME.TABLE_NAME")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        print(f"Error: {str(e)}")
    finally:
        if connection:
            connection.close()
            logger.info("Connection closed")

if __name__ == "__main__":
    main()