from datetime import datetime
import re
from typing import Dict, Set, List, Tuple
import psycopg2
from psycopg2.extras import execute_batch
import logging
from .. import config

class IngredientCleaner:
    def __init__(self):
        # Set up logging
        self.setup_logging()
        
        # Chemical nomenclature standardization
        self.chemical_standardization = {
            'n acetyl cysteine': 'n acetylcysteine',
            'n acetyl l cysteine': 'n acetylcysteine',
            'l methyltetrahydrofolate': 'methyltetrahydrofolate',
            'l ascorbic acid': 'ascorbic acid',
            'd alpha tocopherol': 'alpha tocopherol',
            'dl alpha tocopherol': 'alpha tocopherol',
            'd alpha tocopherol acetate': 'alpha tocopherol acetate',
            'beta sitosterol': 'b sitosterol'
        }
        
        # Plant name standardization
        self.plant_standardization = {
            'pepper mint': 'peppermint',
            'golden seal': 'goldenseal',
            'wheat germ': 'wheatgerm',
            'rose hip': 'rosehip',
            'eucalptus': 'eucalyptus',
            'eucalyptus globus': 'eucalyptus globulus',
            'valerian': 'valeriana',
            'gaultheria': 'gaultheria',  # standard spelling
            'glatheria': 'gaultheria',
            'glautheria': 'gaultheria',
            'thyme vulgaris': 'thymus vulgaris',
            'anis': 'anise',
            'prunus amygdalus dulcis kernel': 'prunus amygdalus dulcis'
        }
        
        # Pharmaceutical standardization (keep separate variants)
        self.keep_separate = {
            ('l cysteine', 'l cystine'),
            ('l theanine', 'l threonine'),
            ('l leucine', 'l isoleucine'),
            ('silicone', 'silicon'),
            ('rapeseed', 'grapeseed'),
            ('grape', 'rape'),
            ('melaleuca alternifolia', 'melaleuca ericifolia'),
            ('interferon alfa a', 'interferon alfa b'),
            ('interferon beta a', 'interferon beta b'),
            ('peginterferon alfa a', 'peginterferon alfa b'),
            ('epa', 'cepa')
        }
        
        # Plural standardization (always use singular)
        self.plural_forms = {
            'omega fatty acids': 'omega fatty acid',
            'psyllium husks': 'psyllium husk',
            'grapes': 'grape',
            'oats': 'oat',
            'soy beans': 'soybean',
            'grape seeds': 'grapeseed'
        }
        
        # Polymer standardization
        self.polymer_standardization = {
            'polquaternium': 'polyquaternium',
            'poliquaternium': 'polyquaternium'
        }
        
        # Combined equivalences
        self.equivalences = {
            **self.chemical_standardization,
            **self.plant_standardization,
            **self.plural_forms,
            **self.polymer_standardization
        }
        
        # Words to remove completely (now correctly referenced as remove_words)
        self.remove_words = {'extract', 'ext.', 'ext', 'oil', 'powder', 'leaf', 'leaves',
                           'root', 'seed', 'fruit', 'flower', 'gel', 'bulb'}
    
    def setup_logging(self):
        """Set up logging configuration."""
        log_filename = f'ingredient_cleaner_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def basic_clean(self, name: str) -> str:
        """Basic cleaning of ingredient names."""
        if not isinstance(name, str):
            self.logger.warning(f"Non-string input received: {name}")
            return ""
            
        # Handle potential encoding issues
        # Normalize unicode characters
        import unicodedata
        name = unicodedata.normalize('NFKD', name)
        # Remove non-ASCII characters but keep basic punctuation
        name = ''.join(c for c in name if ord(c) < 128)
            
        try:
            # Convert to lowercase
            name = name.lower()
            
            # Remove parentheses and their contents
            name = re.sub(r'\([^)]*\)', '', name)
            
            # Remove specific patterns
            patterns_to_remove = [
                r'\d+%',               # Percentages
                r'\d+(\.\d+)?\s*(mg|g|ml|l|mcg|µg)',  # Measurements
                r'\s+\d+\s*$',         # Numbers at the end
                r'^\d+\s+',            # Numbers at the start
            ]
            
            for pattern in patterns_to_remove:
                name = re.sub(pattern, '', name)
            
            # Remove special characters and extra whitespace
            name = re.sub(r'[^a-z\s]', ' ', name)
            name = ' '.join(name.split())
            
            return name.strip()
        except Exception as e:
            self.logger.error(f"Error in basic_clean for '{name}': {e}")
            return name
    
    def remove_form_words(self, name: str) -> str:
        """Remove words indicating form (extract, oil, etc.)."""
        try:
            words = name.split()
            # Fixed: Use self.remove_words instead of undefined form_words
            words = [w for w in words if w not in self.remove_words]
            return ' '.join(words)
        except Exception as e:
            self.logger.error(f"Error in remove_form_words for '{name}': {e}")
            return name
    
    def standardize_name(self, name: str) -> str:
        """Standardize ingredient name using known equivalences and rules."""
        try:
            if not name:
                return ""
                
            name = self.basic_clean(name)
            name = self.remove_form_words(name)
            
            # Use the combined equivalences dictionary
            if name in self.equivalences:
                return self.equivalences[name]
            
            # Handle amino acid compounds
            if name.startswith('alpha keto analog of l '):
                return name
            
            # Handle interferons and similar biologics
            if any(x in name for x in ['interferon', 'peginterferon']):
                return name
            
            # Handle enzyme and protein variants
            if name.startswith('micronized '):
                return name
            
            return name
            
        except Exception as e:
            self.logger.error(f"Error in standardize_name for '{name}': {e}")
            return name
    
    def find_similar_ingredients(self, names: List[str], threshold: float = 0.85) -> List[Tuple[str, str, float]]:
        """Find similar ingredient names using string similarity."""
        from difflib import SequenceMatcher
        
        similar_pairs = []
        processed = set()
        
        try:
            total_comparisons = len(names) * (len(names) - 1) // 2
            comparison_count = 0
            last_progress_log = 0
            
            for i, name1 in enumerate(names):
                for j, name2 in enumerate(names[i+1:], i+1):
                    comparison_count += 1
                    
                    # Log progress every 5%
                    progress = (comparison_count / total_comparisons) * 100
                    if int(progress) // 5 > last_progress_log:
                        last_progress_log = int(progress) // 5
                        self.logger.debug(f"Similarity comparison progress: {progress:.1f}%")
                    
                    if not name1 or not name2:
                        continue
                        
                    if name1 == name2 or (name1, name2) in processed:
                        continue
                    
                    # Skip if these are known to be separate compounds
                    if (name1, name2) in self.keep_separate or (name2, name1) in self.keep_separate:
                        continue
                    
                    # Special handling for vitamins
                    if ('vitamin' in name1 and 'vitamin' in name2 and 
                        any(c in name1 for c in 'abcdefhk') and 
                        any(c in name2 for c in 'abcdefhk')):
                        continue
                    
                    similarity = SequenceMatcher(None, name1, name2).ratio()
                    if similarity > threshold:
                        similar_pairs.append((name1, name2, similarity))
                        processed.add((name1, name2))
                        processed.add((name2, name1))
            
            return sorted(similar_pairs, key=lambda x: x[2], reverse=True)
            
        except Exception as e:
            self.logger.error(f"Error in find_similar_ingredients: {e}")
            return []

def generate_standardization_report(conn, cleaner: IngredientCleaner) -> None:
    """Generate a report of suggested standardizations without making changes."""
    try:
        # Create a temporary table for analysis
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE suggested_changes (
                    id SERIAL PRIMARY KEY,
                    old_name VARCHAR(255),
                    new_name VARCHAR(255),
                    change_type VARCHAR(50),
                    reason TEXT
                );
            """)
            
            # Get all current ingredients
            cur.execute("""
                SELECT id, ingredient_name 
                FROM active_ingredients_extended 
                WHERE ingredient_name IS NOT NULL
                ORDER BY ingredient_name;
            """)
            ingredients = cur.fetchall()
            
            cleaner.logger.info(f"Analyzing {len(ingredients)} ingredients")
            
            # Analyze standard name changes with progress tracking
            standard_changes = []
            total_ingredients = len(ingredients)
            
            cleaner.logger.info("Starting ingredient standardization...")
            for idx, (id, ing) in enumerate(ingredients, 1):
                if idx % 100 == 0:  # Log progress every 100 items
                    progress = (idx / total_ingredients) * 100
                    cleaner.logger.info(f"Progress: {progress:.1f}% ({idx}/{total_ingredients})")
                
                if not ing:  # Skip NULL or empty ingredients
                    continue
                    
                try:
                    cleaned = cleaner.standardize_name(ing)
                    if cleaned and cleaned != ing.lower():
                        standard_changes.append((
                            ing, cleaned, 'standardization',
                            'Standardized naming convention'
                        ))
                except Exception as e:
                    cleaner.logger.error(f"Error processing ingredient {ing}: {e}")
                    continue
            
            # Insert suggested standard changes
            if standard_changes:
                execute_batch(cur, """
                    INSERT INTO suggested_changes 
                    (old_name, new_name, change_type, reason)
                    VALUES (%s, %s, %s, %s);
                """, standard_changes)
            
            # Find similar ingredients with batching
            cleaner.logger.info("Starting similarity analysis...")
            all_names = [ing[1] for ing in ingredients if ing[1]]
            
            # Process in batches of 1000 ingredients
            BATCH_SIZE = 1000
            similar = []
            
            for i in range(0, len(all_names), BATCH_SIZE):
                batch = all_names[i:i + BATCH_SIZE]
                cleaner.logger.info(f"Processing similarity batch {i//BATCH_SIZE + 1}/{(len(all_names) + BATCH_SIZE - 1)//BATCH_SIZE}")
                batch_similar = cleaner.find_similar_ingredients(batch)
                similar.extend(batch_similar)
                
                # Log progress
                progress = (i + len(batch)) / len(all_names) * 100
                cleaner.logger.info(f"Similarity analysis progress: {progress:.1f}%")
            
            # Create SQL file with suggested changes
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            sql_filename = f'suggested_changes_{timestamp}.sql'
            
            # Use UTF-8 encoding for file writing
            with open(sql_filename, 'w', encoding='utf-8') as f:
                f.write("-- Suggested ingredient standardization changes\n")
                f.write("-- Generated on: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n")
                
                if standard_changes:
                    f.write("\n-- Standard name changes:\n")
                    for old, new, _, _ in standard_changes:
                        # Escape special characters in SQL strings
                        old_escaped = old.replace("'", "''")
                        new_escaped = new.replace("'", "''")
                        f.write(f"-- UPDATE active_ingredients_extended\n")
                        f.write(f"-- SET ingredient_name = '{new_escaped}'\n")
                        f.write(f"-- WHERE ingredient_name = '{old_escaped}';\n\n")
                
                if similar:
                    f.write("\n-- Similar ingredients to review and delete duplicates:\n")
                    for name1, name2, sim in similar:
                        # Escape special characters in SQL strings
                        name1_escaped = name1.replace("'", "''")
                        name2_escaped = name2.replace("'", "''")
                        f.write(f"-- Possible duplicate ({sim:.2f} similarity):\n")
                        f.write(f"--   Keep: '{name1_escaped}'\n")
                        f.write(f"--   Delete: '{name2_escaped}'\n")
                        f.write("-- To remove the duplicate:\n")
                        # First update drug_ingredients to point to the kept ingredient
                        f.write("BEGIN;\n")
                        f.write("-- Update drug_ingredients to point to the kept ingredient\n")
                        f.write("UPDATE drug_ingredients di\n")
                        f.write("SET ingredient_id = (\n")
                        f.write("    SELECT id FROM active_ingredients_extended\n")
                        f.write(f"    WHERE ingredient_name = '{name1_escaped}'\n")
                        f.write(")\n")
                        f.write("WHERE ingredient_id IN (\n")
                        f.write("    SELECT id FROM active_ingredients_extended\n")
                        f.write(f"    WHERE ingredient_name = '{name2_escaped}'\n")
                        f.write(");\n\n")
                        # Then delete the duplicate ingredient
                        f.write("-- Delete the duplicate ingredient\n")
                        f.write("DELETE FROM active_ingredients_extended\n")
                        f.write(f"WHERE ingredient_name = '{name2_escaped}';\n")
                        f.write("COMMIT;\n\n")
            
            cleaner.logger.info(f"SQL suggestions saved to {sql_filename}")
            
            # Generate summary report
            print("\nStandardization Report:")
            print(f"Total ingredients analyzed: {len(ingredients)}")
            print(f"Suggested standard name changes: {len(standard_changes)}")
            print(f"Similar ingredient pairs found: {len(similar)}")
            print(f"\nDetailed report saved to: {sql_filename}")
            
            if standard_changes:
                print("\nSample of Suggested Standard Name Changes:")
                for old, new, _, _ in standard_changes[:5]:  # Show only first 5
                    print(f"  {old} → {new}")
                if len(standard_changes) > 5:
                    print(f"  ... and {len(standard_changes) - 5} more")
            
            if similar:
                print("\nSample of Similar Ingredients to Review (Keep → Delete):")
                for name1, name2, sim in similar[:5]:  # Show only first 5
                    print(f"  {name1} → {name2} (similarity: {sim:.2f})")
                if len(similar) > 5:
                    print(f"  ... and {len(similar) - 5} more")
            
            conn.commit()
            
    except Exception as e:
        conn.rollback()
        cleaner.logger.error(f"Error generating standardization report: {e}")
        raise

def main():
    """Main function to analyze ingredients and generate standardization report."""
    try:
        # Initialize the cleaner
        cleaner = IngredientCleaner()
        cleaner.logger.info("Starting ingredient standardization analysis")
        
        # Connect to database using config
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        # Generate standardization report
        generate_standardization_report(conn, cleaner)
        
        cleaner.logger.info("Standardization analysis completed successfully!")
        
    except Exception as e:
        cleaner.logger.error(f"Fatal error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()