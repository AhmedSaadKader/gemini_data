import pandas as pd
from psycopg2.extensions import connection
import psycopg2
from io import StringIO
from .data_analysis import database

def import_active_ingredients(conn: connection, csv_path: str):
    """
    Imports active ingredients from CSV file into the database using bulk insert
    and SQL-based deduplication.
    
    Args:
        conn (connection): PostgreSQL database connection
        csv_path (str): Path to the CSV file containing active ingredients
    
    Returns:
        tuple: (total_rows, unique_rows) - Count of total rows and unique rows after deduplication
    """
    try:
        # Create temporary table for initial bulk insert
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE temp_ingredients (
                    active_ingredient VARCHAR(255)
                );
            """)
            
            # Read CSV file, skip index column
            df = pd.read_csv(csv_path, usecols=['active_ingredient'])
            
            # Create string buffer
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            # Bulk insert all data
            cur.copy_from(buffer, 'temp_ingredients', sep=',')
            
            # Get total count
            cur.execute("SELECT COUNT(*) FROM temp_ingredients")
            total_count = cur.fetchone()[0]
            
            # Create final table with unique values
            cur.execute("""
                CREATE TABLE IF NOT EXISTS active_ingredients AS
                SELECT DISTINCT active_ingredient as ingredient_name
                FROM temp_ingredients
                ORDER BY ingredient_name;
                
                ALTER TABLE active_ingredients 
                ADD COLUMN id SERIAL PRIMARY KEY;
            """)
            
            # Get unique count
            cur.execute("SELECT COUNT(*) FROM active_ingredients")
            unique_count = cur.fetchone()[0]
            
            # Clean up
            cur.execute("DROP TABLE temp_ingredients")
            
            conn.commit()
            
            print(f"\nImport Summary:")
            print(f"Total rows in CSV: {total_count}")
            print(f"Unique ingredients: {unique_count}")
            print(f"Duplicates removed: {total_count - unique_count}")
            
            return total_count, unique_count
    
    except Exception as e:
        print(f"Error importing active ingredients: {e}")
        conn.rollback()
        return 0, 0

# Usage example:
if __name__ == "__main__":
    from .data_analysis import database
    
    conn = database.connect_to_db()
    if conn:
        try:
            import_active_ingredients(conn, "src\\active.csv")
        finally:
            conn.close()