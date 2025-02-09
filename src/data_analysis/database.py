import psycopg2
from .. import config
import pandas as pd

def connect_to_db():
    """Connects to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        print("Connected to PostgreSQL successfully!")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def execute_query(conn, query, params=None):
    """Executes an SQL query and returns the results as a Pandas DataFrame."""
    try:
        df = pd.read_sql_query(query, conn, params=params)
        print("Query executed successfully!")
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None