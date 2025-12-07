from conn import read_table, get_neo4j_driver, execute_neo4j_query



# -----------------------------
# Read Dim_Circuits from Postgres
# -----------------------------

def read_dim_circuits():
    
    query = "SELECT * FROM f1_dimension.dim_circuits;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    print(f"[INFO] Retrieved {len(df)} length from Dim_Circuits.")

    constraint_circuit = """
    CREATE CONSTRAINT circuit_id_unique IF NOT EXISTS FOR 
    (c:Circuit) REQUIRE c.circuit_id IS UNIQUE
    """
    
    print("[INFO] Creating constraint for Circuit nodes.")
    execute_neo4j_query(constraint_circuit)

    for row in df.itertuples():
        params = {
            'circuit_id': row.circuit_id,
            'circuit_name': row.name,
            'circuit_ref': row.circuit_ref,
            'name': row.name,
            'location': row.location,
            'country': row.country,
            'lat': row.lat,
            'lng': row.lng,
            'alt': row.altitude,
            'url': row.url
        }

        queryCircuit = """ 
        MERGE (c:Circuit {circuit_id: $circuit_id})
        SET c.circuit_name = $name,
        c.circuit_ref = $circuit_ref,
        c.location = $location,
        c.country = $country,
        c.lat = $lat,
        c.lng = $lng,
        c.alt = $alt,
        c.url = $url
        """

        execute_neo4j_query(queryCircuit, params)
    print(f"[INFO] Loaded/Updated {len(df)} Circuit nodes.")


# -----------------------------
# Read Dim_Constructors from Postgres
# -----------------------------

def read_dim_constructors():

    query = "SELECT * FROM f1_dimension.dim_constructors;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    print(f"[INFO] Retrieved {len(df)} length from Dim_Constructors.")

# -----------------------------
# Read Dim_Drivers from Postgres
# -----------------------------

def read_dim_drivers():
    query = "SELECT * FROM f1_dimension.dim_drivers;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    print(f"[INFO] Retrieved {len(df)} length from Dim_Drivers.")

    for row in df.itertuples():
        print(row.driver_id, row.driver_name)   

# -----------------------------
# Read Dim_Races from Postgres
# -----------------------------

def read_dim_races():
    query = "SELECT * FROM f1_dimension.dim_races;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    print(f"[INFO] Retrieved {len(df)} length from Dim_Races.") 




# -----------------------------
# Read Dim_Seasons from Postgres
# -----------------------------

def read_dim_seasons():
    query = "SELECT * FROM f1_dimension.dim_seasons;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    print(f"[INFO] Retrieved {len(df)} length from Dim_Seasons.")

# -----------------------------
# Read Dim_Status from Postgres
# -----------------------------

def read_dim_status():
    query = "SELECT * FROM f1_dimension.dim_status;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    print(f"[INFO] Retrieved {len(df)} length from Dim_Status.")    

if __name__ == "__main__":
    read_dim_circuits()
    # read_dim_constructors()
    # read_dim_drivers()
    # read_dim_races()
    # read_dim_seasons()
    # read_dim_status()
