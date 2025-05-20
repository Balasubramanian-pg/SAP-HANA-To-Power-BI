import pandas as pd
from hdbcli import dbapi

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

def extract_mard_data():
    """Extract MARD data"""
    connection = connect_to_hana()
    cursor = connection.cursor()
    
    plant_code = 'S004'
    query = f"SELECT * FROM SAPABAP1.MARD WHERE WERKS = '{plant_code}'"
    
    print(f"Executing query: {query}")
    cursor.execute(query)
    
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    print(f"Retrieved {len(rows)} rows from MARD table for plant {plant_code}")
    
    df = pd.DataFrame(rows, columns=column_names)
    
    output_file = 'MARD_data.xlsx'
    df.to_excel(output_file, index=False)
    print(f"Data saved to {output_file}")
    
    connection.close()
    return df

def extract_makt_data():
    """Extract MAKT data"""
    connection = connect_to_hana()
    cursor = connection.cursor()
    
    language_key = 'E'
    query = f"SELECT * FROM SAPABAP1.MAKT WHERE SPRAS = '{language_key}'"
    
    print(f"Executing query: {query}")
    cursor.execute(query)
    
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    print(f"Retrieved {len(rows)} rows from MAKT table for language {language_key}")
    
    df = pd.DataFrame(rows, columns=column_names)
    
    output_file = 'MAKT_data.xlsx'
    df.to_excel(output_file, index=False)
    print(f"Data saved to {output_file}")
    
    connection.close()
    return df

if __name__ == "__main__":
    print("Extracting MARD data...")
    mard_df = extract_mard_data()
    
    print("\nExtracting MAKT data...")
    makt_df = extract_makt_data()
    
    print("\nExtraction complete!")