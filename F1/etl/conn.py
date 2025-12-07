import pandas as pd
from sqlalchemy import create_engine
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

# -----------------------------
# POSTGRES_CONFIG
# -----------------------------
POSTGRES_URI = os.getenv('POSTGRES_URI')
engine = create_engine(POSTGRES_URI)

# -----------------------------
# NEO4J_CONFIG
# -----------------------------
NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
DATABASE = os.getenv('NEO4J_DATABASE')


# -----------------------------
# GET ENGINE AND DRIVER
# -----------------------------
def get_engine():
    """Get the SQLAlchemy engine"""
    return engine

def get_neo4j_driver():
    """Get the Neo4j driver"""
    return driver


# -----------------------------
# READ TABLE AND EXECUTE NEO4J QUERY
# -----------------------------

def read_table(query: str):
    """Read table/query into pandas DataFrame"""
    print(query)
    return pd.read_sql(query, engine)

def execute_neo4j_query(query: str, params: dict = None): 
    print(f"[INFO] Executing Neo4j queries.")
    with driver.session(database=DATABASE) as session:
        if params:
            session.run(query, params)
        else:
            session.run(query)
    print(f"[INFO] Neo4j query executed successfully.")
