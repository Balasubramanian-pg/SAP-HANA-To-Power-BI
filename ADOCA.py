import pandas as pd
from hdbcli import dbapi
from datetime import datetime

def connect_to_hana():
    """Connect to SAP HANA database"""
    host = "15.6.21.246"
    port = 30015
    user = "POWERBI3"
    password = "Som@2025"  # Replace with your actual password
    
    print(f"Connecting to SAP HANA at {host}:{port} with user {user}")
    
    connection = dbapi.connect(
        address=host,
        port=port,
        user=user,
        password=password
    )
    print("Successfully connected to SAP HANA")
    return connection

def extract_asset_data(connection, table_name, fiscal_year_filter=None):
    """Extract data from a specified Asset Accounting table"""
    try:
        cursor = connection.cursor()
        
        # Prepare base query
        if fiscal_year_filter:
            query = f"SELECT * FROM SAPABAP1.{table_name} WHERE GJAHR BETWEEN '2024' AND '2025'"
            print(f"Executing query: {query}")
        else:
            query = f"SELECT * FROM SAPABAP1.{table_name}"
            print(f"Executing query: {query}")
        
        cursor.execute(query)
        
        column_names = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        print(f"Retrieved {len(rows)} rows from {table_name} table")
        
        df = pd.DataFrame(rows, columns=column_names)
        return df
    
    except Exception as e:
        print(f"Error extracting data from {table_name}: {str(e)}")
        # Check if table exists
        try:
            cursor.execute(f"SELECT COUNT(*) FROM SAPABAP1.{table_name}")
            count = cursor.fetchone()[0]
            print(f"Table {table_name} exists with {count} rows.")
            return pd.DataFrame()
        except:
            print(f"Table {table_name} does not exist or is not accessible.")
            return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()

def main():
    """Main function to extract asset accounting data"""
    connection = connect_to_hana()
    
    # Define asset accounting tables to extract
    # Tables with fiscal year field
    tables_with_year = [
        'ANEP',  # Asset Line Items
        'ANEA',  # Asset Document Header
        'ANLP',  # Asset Period Totals
        'BSEG',  # Accounting Document Segment
        'BKPF',  # Accounting Document Header
        'ANEK',  # Asset Class Totals
        'ANLC'   # Asset Class
    ]
    
    # Tables without fiscal year field
    tables_without_year = [
        'ANLA',  # Asset Master General Data
        'ANLZ',  # Asset Time-Dependent Data
        'ANLB',  # Asset Depreciation Terms
        'T093',  # Depreciation Areas
        'T093C', # Depreciation Area Texts
        'T001A'  # Company Code Assignments
    ]
    
    # Create a timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d")
    
    # Extract data for tables with fiscal year field
    for table in tables_with_year:
        print(f"\nExtracting data from {table} for fiscal years 2024-2025...")
        df = extract_asset_data(connection, table, fiscal_year_filter=True)
        
        if not df.empty:
            output_file = f'{table}_2024_2025_{timestamp}.xlsx'
            df.to_excel(output_file, index=False)
            print(f"Data saved to {output_file}")
    
    # Extract data for tables without fiscal year field
    for table in tables_without_year:
        print(f"\nExtracting data from {table} (master data)...")
        df = extract_asset_data(connection, table)
        
        if not df.empty:
            output_file = f'{table}_master_{timestamp}.xlsx'
            df.to_excel(output_file, index=False)
            print(f"Data saved to {output_file}")
    
    # Close connection
    connection.close()
    print("\nExtraction complete!")

if __name__ == "__main__":
    main()