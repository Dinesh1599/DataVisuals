import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from dataCleaning import clean_dataframe

# -----------------------------
# DATABASE CONFIG
# -----------------------------
DB_USER = "postgres"
DB_PASS = "DRAGON10"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "F1"

RAW_SCHEMA = 'RAW'
CLEAN_SCHEMA = 'CLEANED'

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# -------------------------------------------------------------
# GET ALL TABLE NAMES THAT EXIST IN RAW SCHEMA (DYNAMIC)
# -------------------------------------------------------------

def get_raw_tables():
    sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{RAW_SCHEMA}';
    """

    with engine.connect() as conn:
        result = conn.execute(text(sql)).fetchall()
    # Convert list of tuples → list of strings
    return [row[0] for row in result]

# -------------------------------------------------------------
# EXTRACT RAW DATA
# -------------------------------------------------------------
def extract_raw(table_name):
    df = pd.read_sql(f'SELECT * FROM "{RAW_SCHEMA}"."{table_name}"', engine)
    return df

# -------------------------------------------------------------
# MAIN ETL FOR A SINGLE TABLE
# -------------------------------------------------------------
def run_etl_for_table(table_name):
    # print(f"[ETL] Starting: {table_name}") ---------

    df_raw = extract_raw(table_name)
    df_clean = clean_dataframe(df_raw, table_name)
    # load_clean(df_clean, table_name)

    # print(f"[ETL] Finished: {table_name}") ---------


# -------------------------------------------------------------
# MASTER PIPELINE: RUN ETL FOR EVERY RAW TABLE
# -------------------------------------------------------------

def run_all_etl():
    tables = get_raw_tables()

    print(f"[INFO] Found RAW tables: {tables}")

    for table in tables:
        run_etl_for_table(table)



if __name__ == "__main__":
    run_all_etl()
