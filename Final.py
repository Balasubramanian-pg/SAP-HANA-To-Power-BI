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

def extract_mard_data_with_material_name(connection, start_date, end_date, plant_code, language_key='E', additional_filters=None):
    """
    Extract data from MARD table with material descriptions from MAKT and MSEG for date filtering
    
    Parameters:
    - connection: SAP HANA connection object
    - start_date: Start date for filtering (format: YYYY-MM-DD)
    - end_date: End date for filtering (format: YYYY-MM-DD)
    - plant_code: Plant code to filter by (e.g., 'S004')
    - language_key: Language key for material descriptions (default: 'E' for English)
    - additional_filters: Dictionary of additional column-value pairs to filter the data
    
    Returns:
    - DataFrame containing MARD data with material descriptions
    """
    try:
        cursor = connection.cursor()
        
        # Convert dates to SAP format (YYYYMMDD)
        start_date_sap = start_date.strftime('%Y%m%d')
        end_date_sap = end_date.strftime('%Y%m%d')
        
        print(f"Extracting data for plant {plant_code} from {start_date_sap} to {end_date_sap}")
        
        # First, test a simple query to make sure we can access the MARD table
        try:
            test_query = "SELECT COUNT(*) FROM SAPABAP1.MARD WHERE WERKS = ?"
            cursor.execute(test_query, [plant_code])
            count_result = cursor.fetchone()[0]
            print(f"Found {count_result} records in MARD table for plant {plant_code}")
            
            if count_result == 0:
                print(f"WARNING: No records found in MARD for plant {plant_code}. Check if plant code is correct.")
        except Exception as e:
            print(f"Error testing MARD table access: {str(e)}")
            raise
        
        # Try to check if MAKT table exists and is accessible
        try:
            test_query = "SELECT COUNT(*) FROM SAPABAP1.MAKT WHERE SPRAS = ?"
            cursor.execute(test_query, [language_key])
            count_result = cursor.fetchone()[0]
            print(f"Found {count_result} records in MAKT table for language {language_key}")
        except Exception as e:
            print(f"Error testing MAKT table access: {str(e)}")
            print("Simplifying query to only use MARD table...")
            
            # Simplified query using only MARD if MAKT is not accessible
            query = """
            SELECT 
                MANDT,           -- Client
                MATNR,           -- Material Number
                WERKS,           -- Plant
                LGORT,           -- Storage Location
                LABST,           -- Valuated Unrestricted-Use Stock
                UMLME,           -- Stock in Transfer (Plant to Plant)
                INSME,           -- Quality Inspection Stock
                EINME,           -- Restricted-Use Stock
                SPEME,           -- Blocked Stock
                RETME,           -- Returns Blocked Stock
                VMLAB,           -- Stock Posting to Previous Period
                LWMKB,           -- Total stock managed by warehouse management system
                PSTAT            -- Maintenance status
            FROM SAPABAP1.MARD
            WHERE WERKS = ?
            """
            params = [plant_code]
            cursor.execute(query, params)
            
            # Fetch column names
            column_names = [description[0] for description in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            logger.info(f"Retrieved {len(rows)} rows from MARD table for plant {plant_code}")
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=column_names)
            return df
        
        # Full query with JOIN to MAKT for material descriptions and MSEG for date filtering
        # Using the correct schema prefixes
        query = """
        SELECT DISTINCT
            m.MANDT,           -- Client
            m.MATNR,           -- Material Number
            mk.MAKTX,          -- Material Description
            m.WERKS,           -- Plant
            m.LGORT,           -- Storage Location
            m.LABST,           -- Valuated Unrestricted-Use Stock
            m.UMLME,           -- Stock in Transfer (Plant to Plant)
            m.INSME,           -- Quality Inspection Stock
            m.EINME,           -- Restricted-Use Stock
            m.SPEME,           -- Blocked Stock
            m.RETME,           -- Returns Blocked Stock
            m.VMLAB,           -- Stock Posting to Previous Period
            m.LWMKB,           -- Total stock managed by warehouse management system
            m.PSTAT            -- Maintenance status
        FROM SAPABAP1.MARD m
        JOIN SAPABAP1.MAKT mk ON m.MATNR = mk.MATNR AND mk.SPRAS = ?
        JOIN (
            SELECT DISTINCT 
                ms.MATNR, 
                ms.WERKS, 
                ms.LGORT
            FROM SAPABAP1.MSEG ms
            JOIN SAPABAP1.MKPF h ON ms.MBLNR = h.MBLNR AND ms.MJAHR = h.MJAHR
            WHERE h.BUDAT BETWEEN ? AND ?
            AND ms.WERKS = ?
        ) ms ON m.MATNR = ms.MATNR AND m.WERKS = ms.WERKS AND m.LGORT = ms.LGORT
        WHERE m.WERKS = ?
        """
        
        # Add additional filters if provided
        params = [language_key, start_date_sap, end_date_sap, plant_code, plant_code]
        where_clauses = []
        
        if additional_filters:
            for column, value in additional_filters.items():
                where_clauses.append(f"m.{column} = ?")
                params.append(value)
        
        if where_clauses:
            query += " AND " + " AND ".join(where_clauses)
        
        logger.info(f"Executing query: {query}")
        logger.info(f"Parameters: Language={language_key}, Start Date={start_date_sap}, End Date={end_date_sap}, Plant={plant_code}")
        
        # Execute with parameters
        cursor.execute(query, params)
        
        # Fetch column names
        column_names = [description[0] for description in cursor.description]
        
        # Fetch all rows
        rows = cursor.fetchall()
        logger.info(f"Retrieved {len(rows)} rows from MARD table with material descriptions for plant {plant_code}")
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=column_names)
        return df
    
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        
        # Fall back to simplified query if the full one fails
        try:
            print("Attempting simplified query with just MARD table...")
            query = """
            SELECT 
                MANDT,           -- Client
                MATNR,           -- Material Number
                WERKS,           -- Plant
                LGORT,           -- Storage Location
                LABST,           -- Valuated Unrestricted-Use Stock
                UMLME,           -- Stock in Transfer (Plant to Plant)
                INSME,           -- Quality Inspection Stock
                EINME,           -- Restricted-Use Stock
                SPEME,           -- Blocked Stock
                RETME,           -- Returns Blocked Stock
                VMLAB,           -- Stock Posting to Previous Period
                LWMKB,           -- Total stock managed by warehouse management system
                PSTAT            -- Maintenance status
            FROM SAPABAP1.MARD
            WHERE WERKS = ?
            """
            params = [plant_code]
            cursor.execute(query, params)
            
            # Fetch column names
            column_names = [description[0] for description in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            logger.info(f"Retrieved {len(rows)} rows from MARD table for plant {plant_code}")
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=column_names)
            return df
        except Exception as e2:
            logger.error(f"Error with simplified query: {str(e2)}")
            raise e  # Raise the original error
    finally:
        if cursor:
            cursor.close()

def save_to_file(df, output_file='mard_data.csv'):
    """Save data to file"""
    try:
        # Choose format based on file extension
        file_extension = output_file.split('.')[-1].lower()
        
        if file_extension == 'csv':
            df.to_csv(output_file, index=False)
        elif file_extension in ['xlsx', 'xls']:
            df.to_excel(output_file, index=False)
        elif file_extension == 'json':
            df.to_json(output_file, orient='records')
        else:
            logger.warning(f"Unsupported file extension '{file_extension}'. Defaulting to CSV.")
            df.to_csv(output_file, index=False)
            
        logger.info(f"Data successfully saved to {output_file}")
    except Exception as e:
        logger.error(f"Error saving data to {output_file}: {str(e)}")
        raise

def main():
    """Main function"""
    connection = None  # Initialize connection to None to avoid UnboundLocalError
    try:
        # Connect to SAP HANA using hardcoded parameters
        connection = connect_to_hana()
        
        # Set date range: April 1, 2025 to today (May 20, 2025)
        start_date = date(2025, 4, 1)
        end_date = date.today()  # May 20, 2025
        
        # Plant code is S004
        plant_code = 'S004'
        
        # Additional filters if needed
        additional_filters = {
            # 'LGORT': '0001'  # Example for storage location filter (uncomment if needed)
        }
        
        # Extract data from MARD table with material descriptions and date filtering
        df = extract_mard_data_with_material_name(
            connection, 
            start_date, 
            end_date, 
            plant_code,
            language_key='E',  # 'E' for English, change as needed (e.g., 'D' for German)
            additional_filters=additional_filters
        )
        
        # Display basic information about the extracted data
        print(f"Extracted {len(df)} rows for plant {plant_code}")
        print("\nData sample:")
        print(df.head())
        
        # Check if dataframe has data
        if len(df) > 0:
            print("\nData statistics for numeric columns:")
            print(df.describe())
        else:
            print("\nNo data found. Check your query parameters.")
        
        # Save data to file
        output_file = f'mard_data_plant_{plant_code}_{start_date.strftime("%Y%m%d")}_to_{end_date.strftime("%Y%m%d")}.xlsx'
        save_to_file(df, output_file)
        
        print(f"\nData successfully saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        print(f"Error: {str(e)}")
    finally:
        if connection:
            connection.close()
            logger.info("Connection closed")

if __name__ == "__main__":
    main()