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

def get_all_schemas(connection):
    """Get all accessible schemas"""
    try:
        cursor = connection.cursor()
        query = "SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE HAS_PRIVILEGES = 'TRUE' ORDER BY SCHEMA_NAME"
        cursor.execute(query)
        schemas = [row[0] for row in cursor.fetchall()]
        return schemas
    except Exception as e:
        print(f"Error getting schemas: {str(e)}")
        return []
    finally:
        if cursor:
            cursor.close()

def extract_infocubes(connection):
    """Extract SAP BW InfoCubes"""
    try:
        cursor = connection.cursor()
        
        # First, try RSDCUBE which contains InfoCube metadata in SAP BW
        print("Searching for InfoCubes in SAP BW...")
        try:
            query = """
            SELECT 
                * 
            FROM SAPABAP1.RSDCUBE
            ORDER BY INFOCUBE
            """
            cursor.execute(query)
            column_names = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            print(f"Retrieved {len(rows)} InfoCubes from RSDCUBE table")
            
            infocubes_df = pd.DataFrame(rows, columns=column_names)
            
            # Add cube texts if available
            try:
                query_texts = """
                SELECT 
                    *
                FROM SAPABAP1.RSDCUBETXT
                WHERE LANGU = 'E'
                """
                cursor.execute(query_texts)
                text_cols = [description[0] for description in cursor.description]
                text_rows = cursor.fetchall()
                texts_df = pd.DataFrame(text_rows, columns=text_cols)
                
                if not texts_df.empty and not infocubes_df.empty:
                    # Join with cube names if both dataframes have data
                    infocubes_df = pd.merge(
                        infocubes_df, 
                        texts_df[['INFOCUBE', 'TXTLG']], 
                        on='INFOCUBE', 
                        how='left'
                    )
                    print(f"Added descriptions to {len(texts_df)} InfoCubes")
            except Exception as e:
                print(f"Could not retrieve InfoCube texts: {str(e)}")
            
            return infocubes_df
        
        except Exception as e:
            print(f"Could not access RSDCUBE: {str(e)}")
            
            # Search for cubes in catalog
            print("Searching for cubes in HANA catalog...")
            schemas = get_all_schemas(connection)
            cube_tables = []
            
            for schema in schemas:
                try:
                    query = f"""
                    SELECT
                        '{schema}' as SCHEMA_NAME,
                        TABLE_NAME
                    FROM SYS.TABLES
                    WHERE SCHEMA_NAME = '{schema}'
                    AND (
                        TABLE_NAME LIKE '%CUBE%' OR
                        TABLE_NAME LIKE '%FACT%' OR
                        TABLE_NAME LIKE '/BIC/%'
                    )
                    ORDER BY TABLE_NAME
                    """
                    cursor.execute(query)
                    schema_tables = cursor.fetchall()
                    if schema_tables:
                        print(f"Found {len(schema_tables)} potential cube tables in schema {schema}")
                        cube_tables.extend(schema_tables)
                except Exception as e:
                    print(f"Error searching schema {schema}: {str(e)}")
            
            if cube_tables:
                df = pd.DataFrame(cube_tables, columns=['SCHEMA_NAME', 'TABLE_NAME'])
                return df
            else:
                return pd.DataFrame(columns=['SCHEMA_NAME', 'TABLE_NAME'])
    
    except Exception as e:
        print(f"Error extracting InfoCubes: {str(e)}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()

def extract_parameters(connection):
    """Extract SAP BW Parameters"""
    try:
        cursor = connection.cursor()
        
        # Try to access RSPARAMS which contains parameters in SAP BW
        print("Searching for Parameters in SAP BW...")
        try:
            query = """
            SELECT 
                * 
            FROM SAPABAP1.RSPARAMS
            ORDER BY PARAMTYPE, PARAM
            """
            cursor.execute(query)
            column_names = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            print(f"Retrieved {len(rows)} Parameters from RSPARAMS table")
            
            return pd.DataFrame(rows, columns=column_names)
        
        except Exception as e:
            print(f"Could not access RSPARAMS: {str(e)}")
            
            # Try TVARVC which contains system variables
            try:
                print("Searching for Parameters in TVARVC...")
                query = """
                SELECT 
                    * 
                FROM SAPABAP1.TVARVC
                ORDER BY NAME
                """
                cursor.execute(query)
                column_names = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                print(f"Retrieved {len(rows)} Parameters from TVARVC table")
                
                return pd.DataFrame(rows, columns=column_names)
            except Exception as e:
                print(f"Could not access TVARVC: {str(e)}")
                
                # Try TPARA table for more parameters
                try:
                    print("Searching for Parameters in TPARA...")
                    query = """
                    SELECT 
                        * 
                    FROM SAPABAP1.TPARA
                    """
                    cursor.execute(query)
                    column_names = [description[0] for description in cursor.description]
                    rows = cursor.fetchall()
                    print(f"Retrieved {len(rows)} Parameters from TPARA table")
                    
                    return pd.DataFrame(rows, columns=column_names)
                except Exception as e:
                    print(f"Could not access TPARA: {str(e)}")
                    return pd.DataFrame()
    
    except Exception as e:
        print(f"Error extracting Parameters: {str(e)}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()

def extract_bw_objects(connection):
    """Extract various SAP BW objects"""
    try:
        cursor = connection.cursor()
        results = {}
        
        # List of SAP BW metadata tables to check
        bw_tables = [
            # InfoObjects
            ('RSDIOBJT', 'InfoObjects'),
            # InfoSets
            ('RSDCLIST', 'InfoSets'),
            # DataSources
            ('ROOSOURCE', 'DataSources'),
            # DSOs
            ('RSDODSO', 'DSOs'),
            # MultiProviders
            ('RSDMULTIP', 'MultiProviders'),
            # Process Chains
            ('RSPCCHAIN', 'ProcessChains'),
            # Transformations
            ('RSTRAN', 'Transformations'),
            # InfoPackages
            ('RSBASIDOC', 'InfoPackages')
        ]
        
        for table, description in bw_tables:
            try:
                query = f"SELECT COUNT(*) FROM SAPABAP1.{table}"
                cursor.execute(query)
                count = cursor.fetchone()[0]
                
                if count > 0:
                    query = f"SELECT * FROM SAPABAP1.{table}"
                    cursor.execute(query)
                    column_names = [description[0] for description in cursor.description]
                    rows = cursor.fetchall()
                    results[table] = pd.DataFrame(rows, columns=column_names)
                    print(f"Retrieved {len(rows)} rows from {table} ({description})")
            except Exception as e:
                print(f"Could not access {table}: {str(e)}")
        
        return results
    
    except Exception as e:
        print(f"Error extracting BW objects: {str(e)}")
        return {}
    finally:
        if cursor:
            cursor.close()

def main():
    """Main function to extract SAP BW parameters and cubes"""
    try:
        connection = connect_to_hana()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Extract InfoCubes
        print("\n===== EXTRACTING INFOCUBES =====")
        infocubes_df = extract_infocubes(connection)
        if not infocubes_df.empty:
            output_file = f'SAP_InfoCubes_{timestamp}.xlsx'
            infocubes_df.to_excel(output_file, index=False)
            print(f"InfoCubes data saved to {output_file}")
        
        # Extract Parameters
        print("\n===== EXTRACTING PARAMETERS =====")
        parameters_df = extract_parameters(connection)
        if not parameters_df.empty:
            output_file = f'SAP_Parameters_{timestamp}.xlsx'
            parameters_df.to_excel(output_file, index=False)
            print(f"Parameters data saved to {output_file}")
        
        # Extract other BW objects
        print("\n===== EXTRACTING OTHER BW OBJECTS =====")
        bw_objects = extract_bw_objects(connection)
        if bw_objects:
            with pd.ExcelWriter(f'SAP_BW_Objects_{timestamp}.xlsx') as writer:
                for table_name, df in bw_objects.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=table_name[:31], index=False)  # Excel sheet names max 31 chars
            print(f"BW Objects data saved to SAP_BW_Objects_{timestamp}.xlsx")
        
        # Close connection
        connection.close()
        print("\nExtraction complete!")
    
    except Exception as e:
        print(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    main()