from conn import read_table, get_neo4j_driver, execute_neo4j_query

# -----------------------------
# Read Fact_Results from Postgres
# -----------------------------

def read_fact_results():
    query = "SELECT * FROM f1_fact.fact_results;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    record = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Fact_Results.")

    constraint_result = """
    CREATE CONSTRAINT result_id_unique IF NOT EXISTS FOR
    (res:Result) REQUIRE res.result_id IS UNIQUE
    """

    print("[INFO] Creating constraint for Result nodes.")
    execute_neo4j_query(constraint_result)

    queryResult = """ 
    UNWIND $rows AS row
    MERGE (res:Result {result_id: row.result_id})
    SET res.race_id = row.race_id,
    res.driver_id = row.driver_id,
    res.constructor_id = row.constructor_id,
    res.status_id = row.status_id,
    res.grid_position = row.grid_position,
    res.position = row.position,
    res.position_order = row.position_order,
    res.points = row.points,
    res.laps = row.laps,
    res.time = row.result_time,
    res.milliseconds = row.milliseconds,
    res.fastest_lap = row.fastest_lap,
    res.rank = row.rank,
    res.fastest_lap_time = row.fastest_lap_time,
    res.fastest_lap_speed = row.fastest_lap_speed
    """
    execute_neo4j_query(queryResult, {"rows": record})
    print(f"[INFO] Loaded/Updated {len(df)} Result nodes.")


def read_fact_sprint_results():
    query = "SELECT * FROM f1_fact.fact_sprint_results;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    records = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Fact_Sprint_Results.")

    constraint_sprint = """
    CREATE CONSTRAINT sprint_result_id_unique IF NOT EXISTS FOR
    (sr:SprintResult) REQUIRE sr.result_id IS UNIQUE
    """

    print("[INFO] Creating constraint for SprintResult nodes.")
    execute_neo4j_query(constraint_sprint)

    querySprintResult = """ 
    UNWIND $rows AS row
    MERGE (r:SprintResult {sprint_result_id: row.result_id})
    SET r.race_id       = row.race_id,
        r.driver_id     = row.driver_id,
        r.constructor_id= row.constructor_id,
        r.status_id     = row.status_id,
        r.grid_position = row.grid_position,
        r.position      = row.position,
        r.position_order= row.position_order,
        r.points        = row.points,
        r.laps          = row.laps,
        r.time          = row.time,
        r.milliseconds  = row.milliseconds,
        r.fastest_lap   = row.fastest_lap,
        r.fastest_lap_time= row.fastest_lap_time
    """ 
    execute_neo4j_query(querySprintResult, {"rows": records})
    print(f"[INFO] Loaded/Updated {len(df)} SprintResult nodes.")


def read_fact_qualifying():
    query = "SELECT * FROM f1_fact.fact_qualifying;"
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    records = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Fact_Qualifying.")

    constraint_qualifying = """
    CREATE CONSTRAINT qualifying_id_unique IF NOT EXISTS FOR
    (q:Qualifying) REQUIRE q.qualifying_id IS UNIQUE
    """

    print("[INFO] Creating constraint for Qualifying nodes.")
    execute_neo4j_query(constraint_qualifying)

    queryQualifying = """ 
    UNWIND $rows AS row 
    MERGE (r:Qualifying {qualify_id: row.qualify_id})
    sET r.race_id        = row.race_id,
        r.driver_id      = row.driver_id,   
        r.constructor_id = row.constructor_id,
        r.car_number    = row.car_number,
        r.position       = row.position,
        r.q1             = row.q1,
        r.q2             = row.q2,
        r.q3             = row.q3
    """
    execute_neo4j_query(queryQualifying, {"rows": records})
    print(f"[INFO] Loaded/Updated {len(df)} Qualifying nodes.")


# -----------------------------
# All Fact Relationships done
# -----------------------------

def driverRace_relationship():
    query = """
    SELECT 
        race_id,
        driver_id,
        ARRAY_AGG(lap ORDER BY lap) AS laps,
        ARRAY_AGG(lap_time ORDER BY lap) AS lap_times,
        ARRAY_AGG(milliseconds ORDER BY lap) AS ms_values,
        ARRAY_AGG(position ORDER BY lap) AS positions
    FROM f1_fact.fact_lap_times
    GROUP BY race_id, driver_id;
    """
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    records = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Fact_Lap_Times.")

    driverRaceQuery = """ 
    UNWIND $rows AS row
    MATCH (d:Driver {driver_id: row.driver_id})
    MATCH (r:Race {race_id: row.race_id})
    MERGE (d)-[ri:RACED_IN]->(r)
    SET ri.laps = row.laps,
        ri.lap_times = row.lap_times,
        ri.ms_values = row.ms_values,
        ri.positions = row.positions
    """
    
    execute_neo4j_query(driverRaceQuery, {"rows": records})
    print(f"[INFO] Created/Updated RACED_IN relationships between Driver and Race nodes.")

def driverRace_pitstop_relationship():
    query = """
    SELECT 
        race_id,
        driver_id,
        ARRAY_AGG(stop_number ORDER BY stop_number) AS stop_number,
        ARRAY_AGG(lap ORDER BY stop_number) AS lap,
        ARRAY_AGG(pit_time ORDER BY stop_number) AS pit_time,
        ARRAY_AGG(duration ORDER BY stop_number) AS duration,
	    ARRAY_AGG(milliseconds ORDER BY stop_number) AS ms_values
    FROM f1_fact.fact_pit_stops
    GROUP BY race_id, driver_id;
    """
    print(f"[INFO] Executing query: {query}")
    df = read_table(query)
    records = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Fact_Pit_Stops.")
    driverRacePitstopQuery = """
    UNWIND $rows AS row
    MATCH (d:Driver {driver_id: row.driver_id})
    MATCH (r:Race {race_id: row.race_id})
    MERGE (d)-[pi:PITSTOP_IN]->(r)
    SET pi.stop_number = row.stop_number,
        pi.lap = row.lap,
        pi.pit_time = row.pit_time,
        pi.duration = row.duration,
        pi.ms_values = row.ms_values
    """
    execute_neo4j_query(driverRacePitstopQuery, {"rows": records})
    print(f"[INFO] Created/Updated PITSTOP_IN relationships between Driver and Race nodes.")

def driverRace_standing_relationship():
    query = """
    SELECT * from f1_fact.fact_driver_standings
    """
    df = read_table(query)
    records = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Fact_Driver_Standings.")
    driverRaceStandingQuery = """
    UNWIND $rows AS row
    MATCH (d:Driver {driver_id: row.driver_id})
    MATCH (r:Race {race_id: row.race_id})
    MERGE (d)-[ds:DRIVER_STANDING_IN]->(r)
    SET ds.position = row.position,
        ds.Total_race_points = row.points,
        ds.Total_season_wins = row.wins
    """
    execute_neo4j_query(driverRaceStandingQuery, {"rows": records})
    print(f"[INFO] Created/Updated DRIVER_STANDING_IN relationships between Driver and Race nodes.")

def constructorRace_standing_relationship():
    query = """
    SELECT * from f1_fact.fact_constructor_standings
    """
    df = read_table(query)
    records = df.to_dict(orient='records')
    print(f"[INFO] Retrieved {len(df)} length from Fact_Constructor_Standings.")
    constructorRaceStandingQuery = """
    UNWIND $rows AS row
    MATCH (c:Constructor {constructor_id: row.constructor_id})
    MATCH (r:Race {race_id: row.race_id})
    MERGE (c)-[ds:CONSTRUCTOR_STANDING_IN]->(r)
    SET ds.position = row.position,
        ds.Total_race_points = row.points,
        ds.Total_season_wins = row.wins
    """
    execute_neo4j_query(constructorRaceStandingQuery, {"rows": records})
    print(f"[INFO] Created/Updated CONSTRUCTOR_STANDING_IN relationships between Constructor and Race nodes.")

def statusRace_result_relationship():
    cypher = """
    MATCH (r:Result)
    match (st:Status {status_id: r.status_id})
    MERGE (r)-[:HAS_STATUS]->(st)
    """
    execute_neo4j_query(cypher)
    print(f"[INFO] Created HAS_STATUS relationships between Result and Status nodes.")

def raceRace_result_relationship():
    cypher = """
    MATCH (r:Result)
    match (ra:Race {race_id: r.race_id})
    MERGE (r)-[:BELONGS_TO_RACE]->(ra)
    """
    execute_neo4j_query(cypher)
    print(f"[INFO] Created BELONGS_TO_RACE relationships between Result and Race nodes.")    

def sprintRace_relationship():
    cypher = """
    MATCH (sr:SprintResult)
    MATCH (ra:Race {race_id: sr.race_id})
    MERGE (sr)-[:BELONGS_TO_RACE]->(ra)
    """
    execute_neo4j_query(cypher)
    print(f"[INFO] Created BELONGS_TO_RACE relationships between SprintResult and   Race nodes.")   

def qualifyingRace_relationship():
    cypher = """
    MATCH (q:Qualifying)
    MATCH (ra:Race {race_id: q.race_id})
    MERGE (q)-[:BELONGS_TO_RACE]->(ra)
    """
    execute_neo4j_query(cypher)
    print(f"[INFO] Created BELONGS_TO_RACE relationships between Qualifying and Race nodes.")    

def sprintStatus_relationship():
    cypher = """
    MATCH (sr:SprintResult)
    MATCH (st:Status {status_id: sr.status_id})
    MERGE (sr)-[:HAS_STATUS]->(st)
    """
    execute_neo4j_query(cypher)
    print(f"[INFO] Created HAS_STATUS relationships between SprintResult and Status nodes.")

def run():
    read_fact_results()
    read_fact_sprint_results()
    read_fact_qualifying()
    driverRace_relationship()
    driverRace_pitstop_relationship()
    driverRace_standing_relationship()
    constructorRace_standing_relationship()
    statusRace_result_relationship()
    raceRace_result_relationship()
    sprintRace_relationship()
    qualifyingRace_relationship()    
    sprintStatus_relationship()


if __name__ == "__main__":
    run()


