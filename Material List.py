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

def create_linked_dataset(mard_df, makt_df):
    """Create a linked dataset joining MARD and MAKT on MATNR"""
    print("\nCreating linked dataset between MARD and MAKT...")
    
    # Make sure MATNR is treated as string for joining
    mard_df['MATNR'] = mard_df['MATNR'].astype(str)
    makt_df['MATNR'] = makt_df['MATNR'].astype(str)
    
    # Merge/Join the dataframes on MATNR
    # Left join to keep all MARD records even if there's no matching MAKT record
    linked_df = pd.merge(
        mard_df,
        makt_df[['MATNR', 'MAKTX']],  # Only take the material number and description from MAKT
        on='MATNR',
        how='left'
    )
    
    print(f"Created linked dataset with {len(linked_df)} rows")
    
    # Create a clean list of materials with descriptions
    material_list_df = linked_df[['MATNR', 'MAKTX']].drop_duplicates()
    print(f"Found {len(material_list_df)} unique materials with descriptions")
    
    # Save to Excel
    linked_file = 'MARD_with_descriptions.xlsx'
    linked_df.to_excel(linked_file, index=False)
    print(f"Linked data saved to {linked_file}")
    
    material_list_file = 'Material_List.xlsx'
    material_list_df.to_excel(material_list_file, index=False)
    print(f"Material list saved to {material_list_file}")
    
    return linked_df

if __name__ == "__main__":
    print("Extracting MARD data...")
    mard_df = extract_mard_data()
    
    print("\nExtracting MAKT data...")
    makt_df = extract_makt_data()
    
    # Create linked dataset
    linked_df = create_linked_dataset(mard_df, makt_df)
    
    print("\nExtraction and linking complete!")