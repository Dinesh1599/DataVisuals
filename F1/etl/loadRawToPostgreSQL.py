import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from conn import get_engine

load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------
FOLDER_PATH = "D:\codes\Data Engineer\Data Visualizations\F1\Dataset"  
SCHEMA_NAME = "RAW"

# Create SQLAlchemy engine
engine = get_engine()

# -----------------------------
# FUNCTION TO CREATE TABLE
# -----------------------------
def create_table_if_not_exists(df, table_name):
    dtype_mapping = {
        "int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "object": "TEXT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP"
    }

    col_defs = []
    for col, dtype in df.dtypes.items():
        pg_type = dtype_mapping.get(str(dtype), "TEXT")
        col_defs.append(f'"{col}" {pg_type}')

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "RAW_{table_name}" (
            {", ".join(col_defs)}
        );
    """

    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        print(f"[INFO] Table '{table_name}' ready.")

# -----------------------------
# LOAD CSV FILE INTO POSTGRES
# -----------------------------

def load_csv_to_postgres(file_path):
    table_name = os.path.splitext(os.path.basename(file_path))[0].lower()

    print(f"\n[INFO] Processing: {file_path}")

    df = pd.read_csv(file_path)

    # Create table if not exists
    create_table_if_not_exists(df, table_name)

    # Load the data
    df.to_sql(f"RAW_{table_name}", engine, schema=SCHEMA_NAME, if_exists='append', index=False)
    print(f"[SUCCESS] Loaded {len(df)} rows into '{table_name}'.")


# -----------------------------
# MAIN LOOP
# -----------------------------
def load_all_csvs(folder):
    for file in os.listdir(folder):
        if file.endswith(".csv"):
            file_path = os.path.join(folder, file)
            load_csv_to_postgres(file_path)


if __name__ == "__main__":
    load_all_csvs(FOLDER_PATH)
    print("\n[COMPLETE] All CSV Files Loaded Successfully!")
