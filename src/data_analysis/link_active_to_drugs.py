# migration.py
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Tuple, Optional
import logging
from datetime import datetime

logging.basicConfig(
    filename=f'database_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def add_proper_id_column(conn) -> None:
    """Adds a proper primary key column to the drug_database table."""
    with conn.cursor() as cur:
        try:
            # First, check if drug_id column exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name = 'drug_database' 
                    AND column_name = 'drug_id'
                );
            """)
            column_exists = cur.fetchone()[0]

            if not column_exists:
                print("Adding proper primary key column to drug_database table...")
                
                # Add new drug_id column
                cur.execute("""
                    ALTER TABLE drug_database 
                    ADD COLUMN drug_id SERIAL PRIMARY KEY;
                """)
                
                # Create an index on the old id column for reference
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_old_id 
                    ON drug_database(id);
                """)
                
                conn.commit()
                logging.info("Added proper primary key column to drug_database table")
            else:
                print("Proper primary key column already exists.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error adding primary key column: {e}")
            raise

def create_bridge_table(conn) -> None:
    """Creates a bridge table to handle many-to-many relationship between drugs and ingredients."""
    with conn.cursor() as cur:
        # Drop existing table if it exists
        cur.execute("""
            DROP TABLE IF EXISTS drug_ingredients CASCADE;
        """)
        
        cur.execute("""
        CREATE TABLE drug_ingredients (
            id SERIAL PRIMARY KEY,
            drug_id INTEGER,
            ingredient_id INTEGER,
            UNIQUE(drug_id, ingredient_id),
            FOREIGN KEY (drug_id) REFERENCES drug_database(drug_id),
            FOREIGN KEY (ingredient_id) REFERENCES active_ingredients_extended(id)
        );
        """)
        
        # Create indexes for better query performance
        cur.execute("""
            CREATE INDEX idx_drug_ingredients_drug_id ON drug_ingredients(drug_id);
            CREATE INDEX idx_drug_ingredients_ingredient_id ON drug_ingredients(ingredient_id);
        """)
        
        conn.commit()
        logging.info("Bridge table created successfully")

def clean_ingredient_name(ingredient: str) -> str:
    """Cleans and normalizes ingredient names."""
    import re
    
    # Convert to lowercase
    ingredient = ingredient.lower()
    
    # Remove parentheses and their contents
    ingredient = re.sub(r'\([^)]*\)', '', ingredient)
    
    # Remove specific patterns
    patterns_to_remove = [
        r'\d+\s*mg',  # Remove mg amounts
        r'\d+\s*mcg', # Remove mcg amounts
        r'\d+\s*i\.u\.', # Remove i.u. amounts
        r'\d+\s*u', # Remove unit amounts
        r'\d+\s*ml', # Remove ml amounts
        r'\.\d+', # Remove decimal numbers
        r'\d+%', # Remove percentages
        r'vitamin\s+', # Remove vitamin prefix
        r'vit\s+', # Remove vit prefix
    ]
    
    for pattern in patterns_to_remove:
        ingredient = re.sub(pattern, '', ingredient)
    
    # Remove special characters and extra whitespace
    ingredient = re.sub(r'[^a-z\s]', ' ', ingredient)
    ingredient = ' '.join(ingredient.split())
    
    return ingredient.strip()

def parse_compound_ingredients(ingredient_str: Optional[str]) -> List[str]:
    """Splits compound ingredients into individual ingredients, handling NULL values."""
    if not ingredient_str:
        return []
    
    try:
        # Split by multiple possible delimiters
        delimiters = ['+', ',', '/', '-']
        ingredients = [ingredient_str]
        
        for delimiter in delimiters:
            new_ingredients = []
            for ing in ingredients:
                new_ingredients.extend([i.strip() for i in ing.split(delimiter)])
            ingredients = new_ingredients
        
        # Clean each ingredient name
        cleaned_ingredients = [clean_ingredient_name(ing) for ing in ingredients]
        
        # Remove empty strings and duplicates while preserving order
        seen = set()
        return [ing for ing in cleaned_ingredients if ing and ing not in seen and not seen.add(ing)]
    except Exception as e:
        logging.error(f"Error parsing ingredient string '{ingredient_str}': {e}")
        return []

def analyze_data_quality(conn) -> None:
    """Analyzes data quality issues in the active ingredients."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN activeingredient IS NULL THEN 1 END) as null_ingredients,
                COUNT(CASE WHEN activeingredient = '' THEN 1 END) as empty_ingredients
            FROM drug_database;
        """)
        stats = cur.fetchone()
        
        print("\nData Quality Analysis:")
        print(f"Total rows: {stats[0]}")
        print(f"Null ingredients: {stats[1]}")
        print(f"Empty ingredients: {stats[2]}")
        
        # Sample of problematic rows
        cur.execute("""
            SELECT id, activeingredient
            FROM drug_database
            WHERE activeingredient IS NULL OR activeingredient = ''
            LIMIT 5;
        """)
        problem_rows = cur.fetchall()
        
        if problem_rows:
            print("\nSample of problematic rows:")
            for row in problem_rows:
                print(f"ID: {row[0]}, Ingredient: {row[1]}")

def get_ingredient_mappings(conn) -> dict:
    """Creates a mapping of ingredient names to their IDs, including normalized versions."""
    mappings = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, ingredient_name FROM active_ingredients_extended;")
        for row in cur.fetchall():
            ingredient_id = row[0]
            ingredient_name = row[1]
            
            # Add original lowercase version
            mappings[ingredient_name.lower()] = ingredient_id
            
            # Add cleaned version
            clean_name = clean_ingredient_name(ingredient_name)
            if clean_name:
                mappings[clean_name] = ingredient_id
    
    return mappings

def prepare_bridge_records(conn) -> List[Tuple[int, int]]:
    """Prepares records for the bridge table."""
    ingredient_map = get_ingredient_mappings(conn)
    bridge_records = []
    unmapped_ingredients = set()
    skipped_rows = 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT drug_id, activeingredient 
            FROM drug_database 
            WHERE activeingredient IS NOT NULL 
            AND activeingredient != '';
        """)
        
        for drug_id, compound_ingredient in cur:
            ingredients = parse_compound_ingredients(compound_ingredient)
            if not ingredients:
                skipped_rows += 1
                continue
                
            for ingredient in ingredients:
                ingredient_lower = ingredient.lower()
                if ingredient_lower in ingredient_map:
                    bridge_records.append((drug_id, ingredient_map[ingredient_lower]))
                else:
                    unmapped_ingredients.add(ingredient)

    if unmapped_ingredients:
        logging.warning(f"Unmapped ingredients: {unmapped_ingredients}")
        print(f"\nWarning: Found {len(unmapped_ingredients)} unmapped ingredients:")
        for ingredient in sorted(unmapped_ingredients):
            print(f"  - {ingredient}")
    
    if skipped_rows:
        print(f"\nSkipped {skipped_rows} rows due to null or empty ingredients")

    return bridge_records

def migrate_data(conn) -> None:
    """Performs the data migration."""
    try:
        # Analyze data quality first
        print("Analyzing data quality...")
        analyze_data_quality(conn)

        # Add proper primary key
        add_proper_id_column(conn)

        # Create bridge table
        create_bridge_table(conn)

        # Prepare and insert bridge records
        print("\nPreparing ingredient relationships...")
        bridge_records = prepare_bridge_records(conn)
        
        if bridge_records:
            print(f"Inserting {len(bridge_records)} ingredient relationships...")
            with conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO drug_ingredients (drug_id, ingredient_id)
                    VALUES (%s, %s)
                    ON CONFLICT (drug_id, ingredient_id) DO NOTHING;
                """, bridge_records)
            
            conn.commit()
            logging.info(f"Successfully inserted {len(bridge_records)} bridge records")
            print(f"Successfully migrated {len(bridge_records)} ingredient relationships")
        else:
            print("No valid ingredient relationships found to migrate")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error during migration: {e}")
        raise

def verify_migration(conn) -> None:
    """Verifies the migration was successful."""
    with conn.cursor() as cur:
        # Check total number of relationships
        cur.execute("SELECT COUNT(*) FROM drug_ingredients;")
        total_relationships = cur.fetchone()[0]
        
        # Check for any drugs without ingredients
        cur.execute("""
            SELECT COUNT(*) FROM drug_database d
            WHERE NOT EXISTS (
                SELECT 1 FROM drug_ingredients di
                WHERE di.drug_id = d.drug_id
            );
        """)
        orphaned_drugs = cur.fetchone()[0]
        
        # Get sample of successful relationships
        cur.execute("""
            SELECT d.id as old_id, d.drug_id, d.activeingredient, 
                   string_agg(ae.ingredient_name, ', ') as mapped_ingredients
            FROM drug_database d
            JOIN drug_ingredients di ON d.drug_id = di.drug_id
            JOIN active_ingredients_extended ae ON di.ingredient_id = ae.id
            GROUP BY d.id, d.drug_id, d.activeingredient
            LIMIT 5;
        """)
        sample_relationships = cur.fetchall()

        print(f"\nMigration Verification:")
        print(f"Total ingredient relationships: {total_relationships}")
        print(f"Drugs without ingredients: {orphaned_drugs}")
        
        if sample_relationships:
            print("\nSample of mapped relationships:")
            for old_id, drug_id, active_ing, mapped_ing in sample_relationships:
                print(f"\nDrug ID: {drug_id} (Old ID: {old_id})")
                print(f"Original ingredients: {active_ing}")
                print(f"Mapped ingredients: {mapped_ing}")

def main():
    from .. import config
    
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        print("Starting database migration...")
        migrate_data(conn)
        verify_migration(conn)
        
    except Exception as e:
        print(f"Error: {e}")
        logging.error(f"Fatal error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()