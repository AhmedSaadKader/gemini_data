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
    "notes": "explanation of changes"
}}

Cleaning rules:
1. Remove words like 'extract', 'powder', 'oil' unless part of chemical name
2. Standardize chemical nomenclature
3. Fix spelling errors
4. Remove extra spaces
5. Keep chemical formulas in standard format
6. Keep distinction between similar but different compounds
7. Set confidence 0.7-0.8 if unsure
8. Preserve scientific accuracy

Example:
[
    {{
        "original": "vitamin b12 powder",
        "cleaned": "cyanocobalamin",
        "confidence": 0.95,
        "notes": "Standardized to chemical name"
    }}
]

IMPORTANT:
1. Response must be ONLY the JSON array
2. ALL strings must use double quotes
3. Keep original names EXACTLY as provided
4. Include ALL input ingredients in response
5. No explanation or additional text'''

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

    def create_cleaned_table(self, conn):
        """Create a new table for cleaned ingredients."""
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

    def process_ingredients(self):
        """Main processing function."""
        try:
            # Initialize Gemini API
            gemini_api.initialize_gemini()
            self.logger.info("Gemini API initialized")
            
            # Connect to database
            conn = psycopg2.connect(
                dbname=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST
            )
            
            # Create cleaned table
            self.create_cleaned_table(conn)
            
            # Process each letter of the alphabet
            letters = 'abcdefghijklmnopqrstuvwxyz'
            total_processed = 0
            
            for letter in letters:
                self.logger.info(f"\nProcessing ingredients starting with '{letter}'")
                ingredients = self.get_ingredients_by_letter(conn, letter)
                
                if not ingredients:
                    continue
                
                # Process in batches
                for i in range(0, len(ingredients), self.batch_size):
                    batch = ingredients[i:i + self.batch_size]
                    batch_num = i // self.batch_size + 1
                    total_batches = (len(ingredients) + self.batch_size - 1) // self.batch_size
                    
                    self.logger.info(f"Processing batch {batch_num}/{total_batches} for letter '{letter}'")
                    
                    try:
                        # Create and send prompt
                        prompt = self.create_cleaning_prompt(batch)
                        
                        # Rate limiting - sleep for 4 seconds between batches (15 RPM limit)
                        time.sleep(4)
                        
                        response, _ = gemini_api.generate_content(prompt)
                        
                        if not response:
                            raise ValueError("Empty response from Gemini API")
                            
                        # Clean and parse response
                        cleaned_response = self.clean_api_response(response)
                        
                        try:
                            cleaned_data = json.loads(cleaned_response)
                        except json.JSONDecodeError as e:
                            self.logger.error(f"JSON parsing error: {e}")
                            self.logger.error(f"Failed to parse: {cleaned_response}")
                            raise
                        
                        # Validate response structure
                        if not isinstance(cleaned_data, list):
                            raise ValueError("API response is not a list")
                            
                        required_keys = {'original', 'cleaned', 'confidence', 'notes'}
                        for item in cleaned_data:
                            if not isinstance(item, dict):
                                raise ValueError("Response item is not a dictionary")
                            if not all(key in item for key in required_keys):
                                missing = required_keys - set(item.keys())
                                raise ValueError(f"Response item missing required keys: {missing}")
                            if not isinstance(item['confidence'], (int, float)):
                                raise ValueError("Confidence score is not a number")
                            if not 0 <= item['confidence'] <= 1:
                                raise ValueError("Confidence score out of range [0,1]")
                            
                        # Insert cleaned data
                        with conn.cursor() as cur:
                            for item in cleaned_data:
                                # Find original ingredient
                                original = next(
                                    ing for ing in batch 
                                    if ing['name'] == item['original']
                                )
                                
                                # Log the change
                                self.log_change(original, item)
                                
                                cur.execute("""
                                    INSERT INTO active_ingredients_cleaned
                                    (original_id, original_name, cleaned_name, 
                                     confidence, cleaning_notes)
                                    VALUES (%s, %s, %s, %s, %s);
                                """, (
                                    original['id'],
                                    item['original'],
                                    item['cleaned'],
                                    item['confidence'],
                                    item['notes']
                                ))
                            
                            conn.commit()
                            total_processed += len(cleaned_data)
                            self.logger.info(f"Successfully processed {len(cleaned_data)} ingredients")
                            
                    except Exception as e:
                        self.logger.error(f"Error processing batch: {e}")
                        conn.rollback()
                        continue
                
                self.logger.info(f"Completed processing letter '{letter}'")
                self.logger.info(f"Total processed so far: {total_processed}")
                
                # Add a longer delay between letters to respect rate limits
                time.sleep(10)
            
            self.logger.info(f"\nProcessing completed. Total ingredients processed: {total_processed}")
            
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()
                self.logger.info("Database connection closed")

def main():
    cleaner = IngredientCleaner(batch_size=25)  # Small batch size to respect rate limits
    cleaner.process_ingredients()

if __name__ == "__main__":
    main()