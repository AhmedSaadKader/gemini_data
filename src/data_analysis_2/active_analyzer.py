import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
import time
from typing import List, Dict, Set
import json
from .. import gemini_api
from .. import config

class IngredientCleaner:
    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging with both file and console output."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Main log file
        log_filename = f'ingredient_cleaning_{timestamp}.log'
        # Changes log file
        self.changes_log = f'ingredient_changes_{timestamp}.log'
        
        # Create formatters
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        
        # Setup root logger
        self.logger = logging.getLogger('IngredientCleaner')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # Set up changes logger
        self.changes_logger = logging.getLogger('IngredientChanges')
        self.changes_logger.setLevel(logging.INFO)
        changes_handler = logging.FileHandler(self.changes_log)
        changes_handler.setFormatter(file_formatter)
        self.changes_logger.addHandler(changes_handler)
        
        self.logger.info(f"Main log file: {log_filename}")
        self.logger.info(f"Changes log file: {self.changes_log}")

    def create_cleaning_prompt(self, ingredients: List[Dict]) -> str:
        """Create a prompt for the Gemini API to clean ingredient names."""
        ingredient_names = [ing['name'] for ing in ingredients]
        names_list = '", "'.join(ingredient_names)
        
        return f'''You are a pharmaceutical database expert. Analyze these drug ingredient names and return ONLY a JSON array.

Input ingredients: "{names_list}"

Required JSON structure for each ingredient:
{{
    "original": "original name exactly as provided",
    "cleaned": "standardized name",
    "confidence": number between 0 and 1,
    "notes": "explanation of changes",
    "duplicate_of": "exact name from input list if this is a duplicate, otherwise null",
    "is_duplicate": true/false
}}

Cleaning and duplicate detection rules:
1. Remove words like 'extract', 'powder', 'oil' unless part of chemical name
2. Standardize chemical nomenclature and spelling
3. Identify duplicates that are:
   - Same ingredient name
   - Same ingredient with different spellings (e.g., "Vitamin B1" and "Thiamine")
   - Same chemical with different forms (e.g., "Magnesium oxide" and "Magnesium oxide powder")
   - Different capitalizations or spacings
4. Keep distinction between truly different compounds
5. Mark confidence lower (0.7-0.8) if unsure
6. For duplicates, choose the more standard/formal name as primary

Example:
[
    {{
        "original": "vitamin b1 powder",
        "cleaned": "thiamine",
        "confidence": 0.95,
        "notes": "Standardized to chemical name",
        "duplicate_of": null,
        "is_duplicate": false
    }},
    {{
        "original": "thiamine hcl",
        "cleaned": "thiamine",
        "confidence": 0.95,
        "notes": "Standardized form, duplicate of vitamin b1",
        "duplicate_of": "vitamin b1 powder",
        "is_duplicate": true
    }}
]

IMPORTANT:
1. Response must be ONLY the JSON array
2. ALL strings must use double quotes
3. Keep original names EXACTLY as provided
4. Include ALL input ingredients in response
5. Mark duplicates consistently within the batch
6. No explanation or additional text'''

    def clean_api_response(self, response: str) -> str:
        """Clean and validate the API response."""
        try:
            # Log raw response for debugging
            self.logger.debug(f"Raw API response: {response}")
            
            # Remove any markdown code blocks
            if '```json' in response:
                response = response.split('```json', 1)[1]
            elif '```' in response:
                response = response.split('```', 1)[1]
            if response.endswith('```'):
                response = response.rsplit('```', 1)[0]
            
            # Handle potential YAML-style markers
            if response.startswith('---'):
                response = response.split('---', 1)[1]
            
            # Clean the response
            response = response.strip()
            
            # Log cleaned response
            self.logger.debug(f"Cleaned response: {response}")
            
            return response
            
        except Exception as e:
            self.logger.error(f"Error cleaning API response: {e}")
            self.logger.debug(f"Problematic response: {response}")
            raise

    def log_change(self, original: Dict, cleaned: Dict):
        """Log detailed information about each ingredient change."""
        changes = []
        
        # Check for name changes
        if original['name'].lower() != cleaned['cleaned'].lower():
            changes.append(f"Name changed: '{original['name']}' → '{cleaned['cleaned']}'")
            
        # Log the changes with additional metadata
        log_entry = (
            f"\n{'='*80}\n"
            f"Timestamp: {datetime.now().isoformat()}\n"
            f"Original ID: {original['id']}\n"
            f"Original Name: {original['name']}\n"
            f"Cleaned Name: {cleaned['cleaned']}\n"
            f"Confidence: {cleaned['confidence']}\n"
            f"Notes: {cleaned['notes']}\n"
            f"Changes Detected: {'; '.join(changes) if changes else 'No changes needed'}\n"
            f"{'='*80}"
        )
        
        self.changes_logger.info(log_entry)

    def create_duplicates_table(self, conn):
        """Create a table to track duplicate ingredients."""
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ingredient_duplicates (
                        id SERIAL PRIMARY KEY,
                        duplicate_id INTEGER,
                        primary_id INTEGER,
                        confidence FLOAT,
                        notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (duplicate_id) REFERENCES active_ingredients_extended(id),
                        FOREIGN KEY (primary_id) REFERENCES active_ingredients_extended(id),
                        UNIQUE(duplicate_id, primary_id)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_duplicate_id 
                    ON ingredient_duplicates(duplicate_id);
                    
                    CREATE INDEX IF NOT EXISTS idx_primary_id 
                    ON ingredient_duplicates(primary_id);
                """)
                conn.commit()
                self.logger.info("Duplicates table structure verified")
        except Exception as e:
            self.logger.error(f"Error creating duplicates table: {e}")
            conn.rollback()
            raise
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS active_ingredients_cleaned (
                        id SERIAL PRIMARY KEY,
                        original_id INTEGER,
                        original_name VARCHAR(255),
                        cleaned_name VARCHAR(255),
                        confidence FLOAT,
                        cleaning_notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (original_id) REFERENCES active_ingredients_extended(id)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_cleaned_original_id 
                    ON active_ingredients_cleaned(original_id);
                    
                    CREATE INDEX IF NOT EXISTS idx_cleaned_name 
                    ON active_ingredients_cleaned(cleaned_name);
                """)
                conn.commit()
                self.logger.info("Cleaned ingredients table structure verified")
        except Exception as e:
            self.logger.error(f"Error creating cleaned table: {e}")
            conn.rollback()
            raise

    def get_ingredients_by_letter(self, conn, letter: str) -> List[Dict]:
        """Get all ingredients starting with a specific letter."""
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, ingredient_name 
                    FROM active_ingredients_extended
                    WHERE ingredient_name ILIKE %s
                    AND id NOT IN (
                        SELECT original_id FROM active_ingredients_cleaned
                    )
                    ORDER BY ingredient_name;
                """, (f'{letter}%',))
                
                results = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]
                self.logger.info(f"Found {len(results)} ingredients starting with '{letter}'")
                return results
        except Exception as e:
            self.logger.error(f"Error fetching ingredients for letter {letter}: {e}")
            raise
    def update_active_ingredients(self, conn, cleaned_data: List[Dict]) -> int:
        """Update active_ingredients_extended table with conflict handling."""
        updates_applied = 0
        try:
            with conn.cursor() as cur:
                # First, check which updates would cause conflicts
                for item in cleaned_data:
                    if item['confidence'] < 0.85:
                        self.logger.info(
                            f"Skipping low confidence update for {item['original']} "
                            f"(confidence: {item['confidence']})"
                        )
                        continue

                    try:
                        # Check if the cleaned name already exists
                        cur.execute("""
                            SELECT id, ingredient_name 
                            FROM active_ingredients_extended 
                            WHERE LOWER(ingredient_name) = LOWER(%s);
                        """, (item['cleaned'],))
                        existing = cur.fetchone()

                        if existing:
                            # If exists, log as potential duplicate instead of trying to update
                            self.logger.info(
                                f"Found existing ingredient '{existing[1]}' for cleaned name "
                                f"'{item['cleaned']}' - marking as duplicate relation"
                            )
                            
                            # Get the original ingredient's ID
                            cur.execute("""
                                SELECT id FROM active_ingredients_extended 
                                WHERE ingredient_name = %s;
                            """, (item['original'],))
                            original_id = cur.fetchone()
                            
                            if original_id:
                                # Add to ingredient_duplicates table instead
                                cur.execute("""
                                    INSERT INTO ingredient_duplicates 
                                    (duplicate_id, primary_id, confidence, notes)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (duplicate_id, primary_id) DO NOTHING;
                                """, (
                                    original_id[0],
                                    existing[0],
                                    item['confidence'],
                                    f"Automatically detected during cleaning: {item['notes']}"
                                ))
                                updates_applied += 1
                        else:
                            # Safe to update - no conflict
                            cur.execute("""
                                UPDATE active_ingredients_extended
                                SET 
                                    ingredient_name = %s,
                                    processing_status = 'updated_by_gemini',
                                    last_updated = CURRENT_TIMESTAMP
                                WHERE ingredient_name = %s
                                RETURNING id;
                            """, (item['cleaned'], item['original']))
                            
                            if cur.fetchone():
                                updates_applied += 1

                    except Exception as e:
                        self.logger.error(
                            f"Error processing update for {item['original']}: {str(e)}"
                        )
                        continue

                conn.commit()
                self.logger.info(f"Successfully applied {updates_applied} updates/duplicate relations")
                return updates_applied

        except Exception as e:
            self.logger.error(f"Error in batch update: {str(e)}")
            conn.rollback()
            raise

    def handle_duplicates(self, conn, cleaned_data: List[Dict]) -> int:
        """Handle duplicate ingredients with improved error handling."""
        duplicates_handled = 0
        try:
            with conn.cursor() as cur:
                # Process duplicates
                for item in cleaned_data:
                    # Skip if not marked as duplicate
                    if not item.get('is_duplicate', False) or not item.get('duplicate_of'):
                        continue
                        
                    if item['confidence'] < 0.90:
                        self.logger.info(
                            f"Skipping low confidence duplicate: {item['original']} → "
                            f"{item['duplicate_of']} (confidence: {item['confidence']})"
                        )
                        continue

                    try:
                        # Get IDs for both ingredients using a single query
                        cur.execute("""
                            WITH names AS (
                                SELECT id, ingredient_name, 
                                    CASE 
                                        WHEN ingredient_name = %s THEN 'duplicate'
                                        WHEN ingredient_name = %s THEN 'primary'
                                    END as role
                                FROM active_ingredients_extended
                                WHERE ingredient_name IN (%s, %s)
                            )
                            SELECT 
                                MAX(CASE WHEN role = 'duplicate' THEN id END) as duplicate_id,
                                MAX(CASE WHEN role = 'primary' THEN id END) as primary_id
                            FROM names;
                        """, (
                            item['original'], item['duplicate_of'],
                            item['original'], item['duplicate_of']
                        ))
                        
                        result = cur.fetchone()
                        if not result or None in result:
                            self.logger.warning(
                                f"Could not find both ingredients for duplicate pair: "
                                f"{item['original']} → {item['duplicate_of']}"
                            )
                            continue
                            
                        duplicate_id, primary_id = result
                        
                        # Begin a subtransaction
                        cur.execute("SAVEPOINT duplicate_merge;")
                        
                        try:
                            # Update drug_ingredients to point to primary ingredient
                            cur.execute("""
                                UPDATE drug_ingredients 
                                SET ingredient_id = %s
                                WHERE ingredient_id = %s;
                            """, (primary_id, duplicate_id))
                            
                            # Log the duplicate relation
                            cur.execute("""
                                INSERT INTO ingredient_duplicates 
                                (duplicate_id, primary_id, confidence, notes)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (duplicate_id, primary_id) DO NOTHING;
                            """, (duplicate_id, primary_id, item['confidence'], item['notes']))
                            
                            # Mark the duplicate record
                            cur.execute("""
                                UPDATE active_ingredients_extended
                                SET 
                                    processing_status = 'duplicate',
                                    error_message = %s
                                WHERE id = %s;
                            """, (
                                f"Merged into {item['duplicate_of']} (ID: {primary_id}) "
                                f"with confidence {item['confidence']}",
                                duplicate_id
                            ))
                            
                            cur.execute("RELEASE SAVEPOINT duplicate_merge;")
                            duplicates_handled += 1
                            
                        except Exception as e:
                            cur.execute("ROLLBACK TO SAVEPOINT duplicate_merge;")
                            self.logger.error(f"Error processing duplicate: {str(e)}")
                            continue
                            
                    except Exception as e:
                        self.logger.error(f"Error getting ingredient IDs: {str(e)}")
                        continue

                conn.commit()
                self.logger.info(f"Successfully handled {duplicates_handled} duplicates")
                return duplicates_handled
                
        except Exception as e:
            self.logger.error(f"Error handling duplicates: {str(e)}")
            conn.rollback()
            raise

    def process_ingredients(self):
        """Main processing function with improved error handling and rate limiting."""
        start_time = datetime.now()
        try:
            # Initialize Gemini API
            gemini_api.initialize_gemini()
            self.logger.info("Gemini API initialized")
            
            # Connect to database with retry logic
            max_retries = 3
            retry_count = 0
            conn = None
            
            while retry_count < max_retries:
                try:
                    conn = psycopg2.connect(
                        dbname=config.DB_NAME,
                        user=config.DB_USER,
                        password=config.DB_PASSWORD,
                        host=config.DB_HOST,
                        connect_timeout=10
                    )
                    break
                except psycopg2.Error as e:
                    retry_count += 1
                    self.logger.error(f"Database connection attempt {retry_count} failed: {e}")
                    if retry_count == max_retries:
                        raise
                    time.sleep(5)
            
            # Create necessary tables
            self.create_duplicates_table(conn)
            
            # Initialize counters
            stats = {
                'total_processed': 0,
                'total_updated': 0,
                'total_duplicates': 0,
                'errors': 0
            }
            
            # Process each letter
            letters = 'abcdefghijklmnopqrstuvwxyz'
            for letter in letters:
                letter_start = datetime.now()
                self.logger.info(f"\nProcessing ingredients starting with '{letter}'")
                
                try:
                    ingredients = self.get_ingredients_by_letter(conn, letter)
                    if not ingredients:
                        continue
                    
                    # Process in batches with rate limiting
                    for i in range(0, len(ingredients), self.batch_size):
                        batch = ingredients[i:i + self.batch_size]
                        batch_num = i // self.batch_size + 1
                        total_batches = (len(ingredients) + self.batch_size - 1) // self.batch_size
                        
                        batch_start = datetime.now()
                        self.logger.info(
                            f"Processing batch {batch_num}/{total_batches} "
                            f"for letter '{letter}'"
                        )
                        
                        try:
                            # Implement rate limiting
                            time.sleep(4)  # Ensure we stay under 15 RPM
                            
                            # Process batch
                            prompt = self.create_cleaning_prompt(batch)
                            response, _ = gemini_api.generate_content(prompt)
                            
                            if not response:
                                raise ValueError("Empty response from Gemini API")
                            
                            # Process response
                            cleaned_response = self.clean_api_response(response)
                            cleaned_data = json.loads(cleaned_response)
                            
                            # Handle duplicates first
                            duplicates = self.handle_duplicates(conn, cleaned_data)
                            stats['total_duplicates'] += duplicates
                            
                            # Handle updates for non-duplicates
                            non_duplicates = [
                                item for item in cleaned_data 
                                if not item.get('is_duplicate')
                            ]
                            updated = self.update_active_ingredients(conn, non_duplicates)
                            stats['total_updated'] += updated if updated else 0
                            
                            stats['total_processed'] += len(cleaned_data)
                            
                            # Log batch results
                            batch_time = datetime.now() - batch_start
                            self.logger.info(
                                f"Batch {batch_num} completed in {batch_time}:\n"
                                f"- Processed: {len(cleaned_data)}\n"
                                f"- Updated: {updated if updated else 0}\n"
                                f"- Duplicates: {duplicates}"
                            )
                            
                        except Exception as e:
                            stats['errors'] += 1
                            self.logger.error(f"Error processing batch: {str(e)}")
                            continue
                    
                    # Log letter completion
                    letter_time = datetime.now() - letter_start
                    self.logger.info(
                        f"Completed letter '{letter}' in {letter_time}\n"
                        f"Current totals:\n"
                        f"- Processed: {stats['total_processed']}\n"
                        f"- Updated: {stats['total_updated']}\n"
                        f"- Duplicates: {stats['total_duplicates']}\n"
                        f"- Errors: {stats['errors']}"
                    )
                    
                except Exception as e:
                    self.logger.error(f"Error processing letter '{letter}': {str(e)}")
                    continue
            
            # Log final statistics
            total_time = datetime.now() - start_time
            self.logger.info(
                f"\nProcessing completed in {total_time}:\n"
                f"- Total processed: {stats['total_processed']}\n"
                f"- Total updated: {stats['total_updated']}\n"
                f"- Total duplicates: {stats['total_duplicates']}\n"
                f"- Total errors: {stats['errors']}"
            )
            
        except Exception as e:
            self.logger.error(f"Fatal error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                self.logger.info("Database connection closed")


def main():
    cleaner = IngredientCleaner(batch_size=100)
    cleaner.process_ingredients()

if __name__ == "__main__":
    main()