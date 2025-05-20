import getpass
from hdbcli import dbapi
import pandas as pd # Optional: for easy CSV export and data handling
import argparse

# --- Configuration ---
# These can be hardcoded for testing, but it's better to use
# command-line arguments, environment variables, or a config file for production.

# Default values (can be overridden by command-line arguments)
DEFAULT_HANA_HOST = "15.6.21.246"
DEFAULT_HANA_PORT = 30015 # Replace with your HANA SQL port (e.g., 3<instance_number>15)
DEFAULT_HANA_USER = "POWERBI3"
# Password will be prompted securely

DEFAULT_OUTPUT_FILE = "mard_extract.csv"
DEFAULT_SCHEMA_NAME = "SAPABAP1" # Or your specific schema for MARD, e.g., SAPSR3, SAPHANADB

def connect_to_hana(host, port, user, password):
    """Establishes a connection to the SAP HANA database."""
    try:
        print(f"Attempting to connect to HANA: {host}:{port} as user: {user}...")
        connection = dbapi.connect(
            address=host,
            port=port,
            user=user,
            password=password,
            encrypt='true',  # Recommended for security
            sslValidateCertificate='false' # For self-signed certs; set to 'true' with proper CA
        )
        print("Successfully connected to SAP HANA!")
        return connection
    except dbapi.Error as e:
        print(f"Error connecting to SAP HANA: {e}")
        return None

def fetch_mard_data(connection, schema_name, limit=None, plant_filter=None, material_filter=None):
    """Fetches data from the MARD table."""
    if not connection:
        return None

    cursor = None
    try:
        cursor = connection.cursor()
        print(f"Executing query on {schema_name}.MARD...")

        # --- Customize your SQL Query ---
        # Select specific columns for better performance if you don't need all (*)
        # Common MARD fields: MATNR, WERKS, LGORT, LABST, UMLME, INSME, SPEME, LFGJA, LFMON
        query = f"""
            SELECT
                MATNR,  -- Material Number
                WERKS,  -- Plant
                LGORT,  -- Storage Location
                LABST,  -- Valuated Unrestricted-Use Stock
                UMLME,  -- Stock in transfer (plant to plant)
                INSME,  -- Stock in quality inspection
                SPEME,  -- Blocked stock
                LFGJA,  -- Fiscal Year of Current Period
                LFMON   -- Current period (posting period)
                -- Add more fields as needed, e.g., PSTAT (Maintenance status)
            FROM {schema_name}.MARD
        """

        conditions = []
        params = []

        if plant_filter:
            conditions.append("WERKS = ?")
            params.append(plant_filter)
        if material_filter:
            conditions.append("MATNR = ?") # Or use LIKE for partial matches: MATNR LIKE ?
            params.append(material_filter) # If using LIKE: f"%{material_filter}%"

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        if limit:
            query += f" LIMIT {int(limit)}" # LIMIT clause might vary slightly by DB version

        print(f"SQL Query: {query}")
        if params:
            print(f"Query Parameters: {params}")
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Fetch column names for Pandas DataFrame (optional)
        column_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        print(f"Fetched {len(rows)} rows from MARD.")
        return rows, column_names

    except dbapi.Error as e:
        print(f"Error executing query or fetching data: {e}")
        return None, None
    finally:
        if cursor:
            cursor.close()

def save_to_csv(data, column_names, filename):
    """Saves the fetched data to a CSV file using Pandas."""
    if not data:
        print("No data to save.")
        return

    try:
        df = pd.DataFrame(data, columns=column_names)
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

def main():
    parser = argparse.ArgumentParser(description="Extract data from SAP HANA MARD table.")
    parser.add_argument("--host", default=DEFAULT_HANA_HOST, help="SAP HANA host address.")
    parser.add_argument("--port", type=int, default=DEFAULT_HANA_PORT, help="SAP HANA port number.")
    parser.add_argument("--user", default=DEFAULT_HANA_USER, help="SAP HANA username.")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA_NAME, help="Schema name where MARD table resides.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE, help="Output CSV file name.")
    parser.add_argument("--limit", type=int, help="Limit the number of rows fetched (e.g., for testing).")
    parser.add_argument("--plant", help="Filter by specific plant (WERKS).")
    parser.add_argument("--material", help="Filter by specific material number (MATNR).")

    args = parser.parse_args()

    # Securely get password
    hana_password = getpass.getpass(f"Enter password for HANA user {args.user}: ")

    connection = connect_to_hana(args.host, args.port, args.user, hana_password)

    if connection:
        mard_data, columns = fetch_mard_data(connection, args.schema, args.limit, args.plant, args.material)

        if mard_data:
            # Option 1: Print to console (for small datasets)
            # print("\n--- MARD Data ---")
            # for col_name in columns:
            #     print(f"{col_name:<15}", end="")
            # print()
            # for row in mard_data[:10]: # Print first 10 rows
            #     for item in row:
            #         print(f"{str(item):<15}", end="")
            #     print()

            # Option 2: Save to CSV
            save_to_csv(mard_data, columns, args.output)

        # Close the connection
        print("Closing HANA connection...")
        connection.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()