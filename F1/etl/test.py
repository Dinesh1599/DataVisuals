from conn import read_table, get_neo4j_driver, execute_neo4j_query



query = "SELECT * FROM f1_dimension.dim_circuits;"
print(f"[INFO] Executing query: {query}")
df = read_table(query)

record = df.to_dict(orient='records')

params = {"rows": record}



for row in params:
    print(row[0])