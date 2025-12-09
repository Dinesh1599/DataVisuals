from conn import read_table, get_neo4j_driver, execute_neo4j_query



# -----------------------------
# Read Dim_Circuits from Postgres
# -----------------------------

def read_dim_circuits():

    query = "SELECT * FROM f1_dimension.dim_circuits;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    print(f"[INFO] Retrieved {len(df)} length from Dim_Circuits.")

    record = df.to_dict(orient='records')

    constraint_circuit = """
    CREATE CONSTRAINT circuit_id_unique IF NOT EXISTS FOR 
    (c:Circuit) REQUIRE c.circuit_id IS UNIQUE
    """
    
    print("[INFO] Creating constraint for Circuit nodes.")
    execute_neo4j_query(constraint_circuit)



    queryCircuit = """ 
    UNWIND $rows AS row
    MERGE (c:Circuit {circuit_id: row.circuit_id})
    SET c.circuit_name = row.name,
    c.circuit_ref = row.circuit_ref,
    c.location = row.location,
    c.country = row.country,
    c.lat = row.lat,
    c.lng = row.lng,
    c.alt = row.altitude,
    c.url = row.url
    """

    execute_neo4j_query(queryCircuit, {"rows": record})
    print(f"[INFO] Loaded/Updated {len(df)} Circuit nodes.")


# -----------------------------
# Read Dim_Constructors from Postgres
# -----------------------------

def read_dim_constructors():

    query = "SELECT * FROM f1_dimension.dim_constructors;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    record = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Dim_Constructors.")

    constraint_constructor = """
    CREATE CONSTRAINT constructor_id_unique IF NOT EXISTS FOR
    (con:Constructor) REQUIRE con.constructor_id IS UNIQUE
    """
    print("[INFO] Creating constraint for Constructor nodes.")
    execute_neo4j_query(constraint_constructor)

    queryConstructor = """ 
    UNWIND $rows AS row
    MERGE (con:Constructor {constructor_id: row.constructor_id})
    SET con.constructor_name = row.name,
    con.constructor_ref = row.constructor_ref,
    con.nationality = row.nationality,
    con.url = row.url
    """

    execute_neo4j_query(queryConstructor, {"rows": record})
    print(f"[INFO] Loaded/Updated {len(df)} Constructor nodes.")


# -----------------------------
# Read Dim_Drivers from Postgres
# -----------------------------

def read_dim_drivers():
    query = "SELECT * FROM f1_dimension.dim_drivers;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    record = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Dim_Drivers.")

    constraint_driver = """
    CREATE CONSTRAINT driver_id_unique IF NOT EXISTS FOR
    (d:Driver) REQUIRE d.driver_id IS UNIQUE
    """

    print("[INFO] Creating constraint for Driver nodes.")
    execute_neo4j_query(constraint_driver)



    queryDriver = """ 
    UNWIND $rows AS row
    MERGE (d:Driver {driver_id: row.driver_id})
    SET d.driver_forename = row.driver_forename,
    d.driver_surname = row.driver_surname,
    d.driver_ref = row.driver_ref,
    d.code = row.code,
    d.number = row.driver_number,
    d.dob = row.dob,
    d.nationality = row.nationality,
    d.url = row.url
    """
    execute_neo4j_query(queryDriver, {"rows": record})
    print(f"[INFO] Loaded/Updated {len(df)} Driver nodes.")


# -----------------------------
# Read Dim_Races from Postgres
# -----------------------------

def read_dim_races():
    query = "SELECT * FROM f1_dimension.dim_races;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    print(f"[INFO] Retrieved {len(df)} rows from dim_races.")

    # Constraint
    constraint_race = """
    CREATE CONSTRAINT race_id_unique IF NOT EXISTS
    FOR (r:Race)
    REQUIRE r.race_id IS UNIQUE
    """
    execute_neo4j_query(constraint_race)

    # Convert df → list of dicts (Neo4j UNWIND expects this format)
    records = df.to_dict("records")

    # UNWIND Cypher Query
    queryRace = """
    UNWIND $rows AS row  
    MERGE (r:Race {race_id: row.race_id})
    SET r.race_year       = row.race_year,
        r.race_round      = row.round,
        r.race_circuit_id = row.circuit_id,
        r.race_name       = row.race_name,
        r.race_date       = row.race_date,
        r.race_time       = row.race_time,
        r.fp1_date        = row.fp1_date,
        r.fp1_time        = row.fp1_time,
        r.fp2_date        = row.fp2_date,
        r.fp2_time        = row.fp2_time,
        r.fp3_date        = row.fp3_date,
        r.fp3_time        = row.fp3_time,
        r.quali_date       = row.quali_date,
        r.quali_time       = row.quali_time,
        r.sprint_date     = row.sprint_date,
        r.sprint_time     = row.sprint_time,
        r.race_url        = row.url
    """

    # Execute once (FAST)
    execute_neo4j_query(queryRace, {"rows": records})
    print(f"[INFO] Loaded/Updated {len(df)} Race nodes")


# -----------------------------
# Read Dim_Seasons from Postgres
# -----------------------------

def read_dim_seasons():
    query = "SELECT * FROM f1_dimension.dim_seasons;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    record = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Dim_Seasons.")

    constraint_season = """
    CREATE CONSTRAINT season_year_unique IF NOT EXISTS FOR
    (s:Season) REQUIRE s.season_year IS UNIQUE
    """
    execute_neo4j_query(constraint_season)

    querySeason = """ 
    UNWIND $rows AS row 
    MERGE (s:Season {season_year: row.season_year})
    SET s.url = row.url
    """
    execute_neo4j_query(querySeason, {"rows": record})
    print(f"[INFO] Loaded/Updated {len(df)} Season nodes.")

# -----------------------------
# Read Dim_Status from Postgres
# -----------------------------

def read_dim_status():
    query = "SELECT * FROM f1_dimension.dim_status;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    record = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Dim_Status.")    

    constraint_status = """
    CREATE CONSTRAINT status_id_unique IF NOT EXISTS FOR
    (s:Status) REQUIRE s.status_id IS UNIQUE
    """
    execute_neo4j_query(constraint_status)

    queryStatus = """
    UNWIND $rows AS row
    MERGE (s:Status {status_id: row.status_id})
    SET s.status = row.status
    """
    execute_neo4j_query(queryStatus, {"rows": record})
    print(f"[INFO] Loaded/Updated {len(df)} Status nodes.")

# -----------------------------
# All Node Relationships done
# -----------------------------

def match_dim_nodes():
    circuitRaceQuery = """
    MATCH (r:Race)
    MATCH(c:Circuit{c.circuit_id: r.race_circuit_id})
    MERGE (r)-[:HELD_AT]->(c)
    """
    execute_neo4j_query(circuitRaceQuery)
    print(f"[INFO] Created HELD_AT relationships between Race and Circuit nodes.")

    seasonRaceQuery = """
        MATCH (r:Race)
        MATCH (s:Season {season_year: r.race_year})
        MERGE (r)-[:PART_OF_SEASON]->(s)
    """
    execute_neo4j_query(seasonRaceQuery)
    print(f"[INFO] Created PART_OF_SEASON relationships between Race and Season nodes.")


def run():
    read_dim_circuits()
    read_dim_constructors()
    read_dim_drivers()
    read_dim_races()
    read_dim_seasons()
    read_dim_status()
    match_dim_nodes()

if __name__ == "__main__":
    run()

