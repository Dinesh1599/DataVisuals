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
print(f"Using POSTGRES_URI: {POSTGRES_URI}" )
engine = create_engine(POSTGRES_URI)

# -----------------------------
# NEO4J_CONFIG
# -----------------------------
NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))



def read_table(query: str):
    """Read table/query into pandas DataFrame"""
    return pd.read_sql(query, engine)

def get_engine():
    """Get the SQLAlchemy engine"""
    return engine


