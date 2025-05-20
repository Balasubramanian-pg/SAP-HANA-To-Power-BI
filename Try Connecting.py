from hdbcli import dbapi
import sys

def test_sap_connection():
    # Connection parameters - UPDATE THESE
    host = "15.6.21.246"    # Your SAP HANA server IP
    port = 30015            # Your SAP HANA port
    user = "POWERBI3"     # Your username - update with correct format
    password = "Som@2025"  # Your actual password
    
    print(f"Attempting to connect to SAP HANA at {host}:{port} with user {user}")
    
    # Method 1: Direct connection
    try:
        print("\nTrying connection method 1: Direct parameters")
        connection = dbapi.connect(
            address=host,
            port=port,
            user=user,
            password=password
        )
        print("SUCCESS: Connected using method 1!")
        try:
            print("Connection details:", connection.getConnectionInfo())
        except:
            print("Connected, but couldn't retrieve connection details.")
        connection.close()
        return True
    except Exception as e:
        print(f"Method 1 FAILED: {str(e)}\n")
    
    # Method 2: With user without space
    try:
        print("\nTrying connection method 2: Username without space")
        user_no_space = user.replace(" ", "")
        connection = dbapi.connect(
            address=host,
            port=port,
            user=user_no_space,
            password=password
        )
        print(f"SUCCESS: Connected using method 2 with user '{user_no_space}'!")
        try:
            print("Connection details:", connection.getConnectionInfo())
        except:
            print("Connected, but couldn't retrieve connection details.")
        connection.close()
        return True
    except Exception as e:
        print(f"Method 2 FAILED: {str(e)}\n")
    
    # Method 3: With user with space
    try:
        print("\nTrying connection method 3: Username with space")
        user_with_space = "FLIP CARBON"  # Explicitly add space
        connection = dbapi.connect(
            address=host,
            port=port,
            user=user_with_space,
            password=password
        )
        print(f"SUCCESS: Connected using method 3 with user '{user_with_space}'!")
        try:
            print("Connection details:", connection.getConnectionInfo())
        except:
            print("Connected, but couldn't retrieve connection details.")
        connection.close()
        return True
    except Exception as e:
        print(f"Method 3 FAILED: {str(e)}\n")
    
    # Method 4: Using connection string
    try:
        print("\nTrying connection method 4: Connection string")
        conn_str = f"SERVERNODE={host}:{port};UID={user};PWD={password}"
        connection = dbapi.connect(conn_str)
        print("SUCCESS: Connected using method 4 with connection string!")
        try:
            print("Connection details:", connection.getConnectionInfo())
        except:
            print("Connected, but couldn't retrieve connection details.")
        connection.close()
        return True
    except Exception as e:
        print(f"Method 4 FAILED: {str(e)}\n")
        
    print("\nALL CONNECTION METHODS FAILED")
    return False

if __name__ == "__main__":
    print("SAP HANA Connection Test\n" + "="*25)
    
    # Verify hdbcli module is available without checking version
    try:
        module_info = str(dbapi)
        print(f"hdbcli.dbapi module loaded: {module_info}")
    except Exception as e:
        print(f"Error with hdbcli module: {str(e)}")
        sys.exit(1)
    
    # Try to connect
    result = test_sap_connection()
    
    if result:
        print("\nTEST PASSED: Successfully connected to SAP HANA!")
        print("\nNext steps:")
        print("1. Use the successful connection method in your full script")
        print("2. Proceed with extracting data from the MARD table")
    else:
        print("\nTEST FAILED: Could not connect to SAP HANA.")
        print("\nTroubleshooting tips:")
        print("1. Verify username and password")
        print("2. Check if SAP HANA is running and accessible from this machine")
        print("3. Ensure no firewall is blocking the connection")
        print("4. Try connecting with SAP HANA Studio or another client to verify credentials")
        print("5. Contact your SAP administrator to confirm connection details")