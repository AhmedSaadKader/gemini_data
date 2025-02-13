import psycopg2
from typing import List, Tuple
import logging
from datetime import datetime
from .. import config

class IngredientPairChecker:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging with both file and console output."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f'ingredient_pair_check_{timestamp}.log'
        
        # Create formatters
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        
        # Setup root logger
        self.logger = logging.getLogger('IngredientPairChecker')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

    def check_ingredient_pairs(self, conn, pairs: List[Tuple[str, str]]) -> None:
        """
        Check if both ingredients in each pair exist in the database
        and print their details including related drugs.
        """
        try:
            with conn.cursor() as cur:
                for primary, variant in pairs:
                    print(f"\n{'='*80}")
                    print(f"Checking pair: {primary} ↔ {variant}")
                    print(f"{'='*80}")
                    
                    # Get ingredient details
                    cur.execute("""
                        SELECT 
                            id, 
                            ingredient_name, 
                            processing_status, 
                            error_message,
                            last_updated,
                            (
                                SELECT COUNT(*)
                                FROM drug_ingredients
                                WHERE ingredient_id = ae.id
                            ) as drug_count
                        FROM active_ingredients_extended ae
                        WHERE LOWER(ingredient_name) LIKE LOWER(%s)
                           OR LOWER(ingredient_name) LIKE LOWER(%s);
                    """, (f"%{primary}%", f"%{variant}%"))
                    
                    results = cur.fetchall()
                    
                    if not results:
                        print(f"⚠ No matches found for either '{primary}' or '{variant}'")
                        continue
                        
                    print(f"\nFound {len(results)} matching ingredients:")
                    for row in results:
                        ing_id, name, status, error, updated, drug_count = row
                        print(f"\n📋 Ingredient Details:")
                        print(f"  ID: {ing_id}")
                        print(f"  Name: {name}")
                        print(f"  Status: {status}")
                        print(f"  Last Updated: {updated}")
                        print(f"  Used in {drug_count} drugs")
                        if error:
                            print(f"  Error Message: {error}")
                        
                        # Get related drugs
                        if drug_count > 0:
                            cur.execute("""
                                SELECT DISTINCT
                                    d.tradename,
                                    d.company,
                                    d.form,
                                    d."group" as drug_group
                                FROM drug_ingredients di
                                JOIN drug_database d ON di.drug_id = d.drug_id
                                WHERE di.ingredient_id = %s
                                LIMIT 5;
                            """, (ing_id,))
                            
                            drugs = cur.fetchall()
                            print(f"\n  📦 Example Drugs (showing up to 5 of {drug_count}):")
                            for drug in drugs:
                                print(f"    - {drug[0]} ({drug[1]}, {drug[2]}, {drug[3]})")
                        
                        print(f"\n  {'-'*60}")
                    
                    # # Check if they're already linked in ingredient_duplicates
                    # cur.execute("""
                    #     WITH ingredients AS (
                    #         SELECT id, ingredient_name
                    #         FROM active_ingredients_extended
                    #         WHERE LOWER(ingredient_name) LIKE LOWER(%s)
                    #            OR LOWER(ingredient_name) LIKE LOWER(%s)
                    #     )
                    #     SELECT 
                    #         i1.ingredient_name as duplicate_name,
                    #         i2.ingredient_name as primary_name,
                    #         id.confidence,
                    #         id.notes
                    #     FROM ingredient_duplicates id
                    #     JOIN ingredients i1 ON id.duplicate_id = i1.id
                    #     JOIN ingredients i2 ON id.primary_id = i2.id;
                    # """, (f"%{primary}%", f"%{variant}%"))
                    
                    # duplicate_relations = cur.fetchall()
                    # if duplicate_relations:
                    #     print("\n🔗 Existing Duplicate Relations:")
                    #     for rel in duplicate_relations:
                    #         print(f"  {rel[0]} → {rel[1]}")
                    #         print(f"  Confidence: {rel[2]}")
                    #         print(f"  Notes: {rel[3]}")
                    
        except Exception as e:
            self.logger.error(f"Error checking ingredient pairs: {e}")
            raise

def main():
    # Example pairs to check
    pairs_to_check = [
        ('Wool fat', 'lanolin'),
        ('Zingiber officinale', 'ginger'),
        ('Zolpidem', 'Zolpidem tartrate'),
        ('Triticum aestivum', 'Fiber derived from wheat'),
        ('Vanillyl', 'Vanyl')
    ]
    
    checker = IngredientPairChecker()
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        # Check the pairs
        checker.check_ingredient_pairs(conn, pairs_to_check)
        
    except Exception as e:
        checker.logger.error(f"Fatal error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            checker.logger.info("Database connection closed")

if __name__ == "__main__":
    main()