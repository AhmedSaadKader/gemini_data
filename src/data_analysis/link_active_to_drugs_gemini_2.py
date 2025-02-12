import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Set
import json
import time

from .. import gemini_api
from . import database
from .. import config

class BatchProcessor:
    def __init__(self, batch_size: int = 5, max_retries: int = 3):
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.rate_limiter = RateLimiter()
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging with both file and console output."""
        log_filename = f'ingredient_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        # Create formatters
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        
        # Setup root logger
        self.logger = logging.getLogger('BatchProcessor')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"Logging initialized - writing to {log_filename}")

    def initialize_gemini(self):
        """Initialize the Gemini API with proper error handling."""
        try:
            gemini_api.initialize_gemini()
            self.logger.info("Gemini API initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize Gemini API: {e}")
            return False

    def create_tables(self, conn):
        """Create or verify necessary database tables."""
        try:
            with conn.cursor() as cur:
                # Create bridge table if not exists
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS drug_ingredients (
                        id SERIAL PRIMARY KEY,
                        drug_id INTEGER REFERENCES drug_database(drug_id),
                        ingredient_id INTEGER REFERENCES active_ingredients_extended(id),
                        confidence FLOAT,
                        mapping_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(drug_id, ingredient_id)
                    );
                """)
                
                # Create necessary indexes
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_drug_ingredients_drug_id 
                    ON drug_ingredients(drug_id);
                    
                    CREATE INDEX IF NOT EXISTS idx_drug_ingredients_ingredient_id 
                    ON drug_ingredients(ingredient_id);
                """)
                
                conn.commit()
                self.logger.info("Database tables and indexes verified/created successfully")
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            conn.rollback()
            raise

    def get_ingredients_to_process(self, conn) -> List[Tuple[int, str]]:
        """Get list of ingredients that need processing."""
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT d.drug_id, d.activeingredient 
                    FROM drug_database d
                    WHERE d.activeingredient IS NOT NULL 
                    AND d.activeingredient != ''
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM drug_ingredients di 
                        WHERE di.drug_id = d.drug_id
                    )
                    ORDER BY d.drug_id;
                """)
                return cur.fetchall()
        except Exception as e:
            self.logger.error(f"Error fetching ingredients: {e}")
            raise

    def process_batch(self, conn, batch: List[Tuple[int, str]], known_ingredients: Set[str]) -> int:
        """Process a single batch of ingredients."""
        successful_mappings = 0
        
        try:
            for drug_id, compound in batch:
                retry_count = 0
                while retry_count < self.max_retries:
                    try:
                        self.rate_limiter.wait_if_needed()
                        
                        # Generate and process content
                        prompt = self.create_prompt(compound, known_ingredients)
                        response, _ = gemini_api.generate_content(prompt)
                        
                        if not response:
                            raise ValueError("Empty response from Gemini API")
                            
                        # Parse and validate response
                        mappings = self.parse_response(response)
                        if not mappings:
                            raise ValueError("No valid mappings found in response")
                            
                        # Insert valid mappings
                        successful_mappings += self.insert_mappings(conn, drug_id, mappings)
                        break
                        
                    except Exception as e:
                        retry_count += 1
                        self.logger.warning(f"Retry {retry_count}/{self.max_retries} for drug_id {drug_id}: {e}")
                        time.sleep(2 ** retry_count)  # Exponential backoff
                        
                if retry_count == self.max_retries:
                    self.logger.error(f"Failed to process drug_id {drug_id} after {self.max_retries} retries")
                    
            return successful_mappings
            
        except Exception as e:
            self.logger.error(f"Batch processing error: {e}")
            conn.rollback()
            raise

    def create_prompt(self, compound: str, known_ingredients: Set[str]) -> str:
        """Create a prompt for the Gemini API."""
        known_sample = list(known_ingredients)[:50]  # Limit sample size
        return f'''Analyze this drug compound: "{compound}"

Known active ingredients: {", ".join(known_sample)}

Return a JSON array where each object has:
- "original": original ingredient text
- "mapped_ingredient": matched known ingredient
- "confidence": confidence score (0-1)
- "notes": explanation of mapping

Format:
[
    {{
        "original": "acetaminophen",
        "mapped_ingredient": "Paracetamol",
        "confidence": 0.95,
        "notes": "Common alternative name"
    }}
]

Rules:
1. Return ONLY valid JSON
2. Always Split compound ingredients
3. Match to known ingredients as I sure all ingredients should be present, if you can't find any please warn me
4. Use high confidence (>0.9) for exact matches
5. Include mapping logic in notes'''

    def parse_response(self, response: str) -> List[Dict]:
        """Parse and validate the API response."""
        try:
            # Clean response
            response = response.strip()
            if response.startswith('```'):
                response = response.split('```')[1]
            if response.endswith('```'):
                response = response[:-3]
            
            mappings = json.loads(response)
            
            # Validate mappings
            valid_mappings = []
            for mapping in mappings:
                if all(key in mapping for key in ['original', 'mapped_ingredient', 'confidence']):
                    if 0 <= mapping['confidence'] <= 1:
                        valid_mappings.append(mapping)
                        
            return valid_mappings
            
        except Exception as e:
            self.logger.error(f"Error parsing response: {e}")
            return []

    def insert_mappings(self, conn, drug_id: int, mappings: List[Dict]) -> int:
        """Insert valid mappings into the database."""
        successful_inserts = 0
        
        try:
            with conn.cursor() as cur:
                for mapping in mappings:
                    if mapping['confidence'] >= 0.7:  # Confidence threshold
                        cur.execute("""
                            INSERT INTO drug_ingredients (drug_id, ingredient_id, confidence)
                            SELECT %s, id, %s
                            FROM active_ingredients_extended
                            WHERE ingredient_name = %s
                            ON CONFLICT (drug_id, ingredient_id) DO NOTHING
                            RETURNING id;
                        """, (drug_id, mapping['confidence'], mapping['mapped_ingredient']))
                        
                        if cur.fetchone():
                            successful_inserts += 1
                            
            conn.commit()
            return successful_inserts
            
        except Exception as e:
            self.logger.error(f"Error inserting mappings: {e}")
            conn.rollback()
            return 0

    def process_all_ingredients(self):
        """Main processing function."""
        start_time = datetime.now()
        self.logger.info(f"Starting ingredient processing at {start_time}")
        
        try:
            # Initialize API
            if not self.initialize_gemini():
                return
            
            # Connect to database
            conn = database.connect_to_db()
            if not conn:
                self.logger.error("Failed to connect to database")
                return
                
            try:
                # Create necessary tables
                self.create_tables(conn)
                
                # Get known ingredients
                with conn.cursor() as cur:
                    cur.execute("SELECT ingredient_name FROM active_ingredients_extended")
                    known_ingredients = {row[0] for row in cur.fetchall()}
                
                # Get ingredients to process
                ingredients = self.get_ingredients_to_process(conn)
                total_ingredients = len(ingredients)
                self.logger.info(f"Found {total_ingredients} ingredients to process")
                
                # Process in batches
                total_processed = 0
                total_successful = 0
                
                for i in range(0, total_ingredients, self.batch_size):
                    batch = ingredients[i:i + self.batch_size]
                    batch_num = i // self.batch_size + 1
                    total_batches = (total_ingredients + self.batch_size - 1) // self.batch_size
                    
                    self.logger.info(f"Processing batch {batch_num}/{total_batches}")
                    
                    successful_mappings = self.process_batch(conn, batch, known_ingredients)
                    total_processed += len(batch)
                    total_successful += successful_mappings
                    
                    # Log progress
                    progress = (total_processed / total_ingredients) * 100
                    self.logger.info(f"Progress: {progress:.1f}% ({total_successful} successful mappings)")
                    
                end_time = datetime.now()
                processing_time = end_time - start_time
                self.logger.info(f"Processing completed in {processing_time}")
                self.logger.info(f"Total successful mappings: {total_successful}")
                
            finally:
                conn.close()
                self.logger.info("Database connection closed")
                
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            raise

class RateLimiter:
    """Rate limiting for API calls."""
    def __init__(self, max_requests_per_minute=14):
        self.minute_requests = []
        self.MAX_REQUESTS_PER_MINUTE = max_requests_per_minute
    
    def wait_if_needed(self):
        """Implement rate limiting with a sliding window."""
        now = time.time()
        self.minute_requests = [t for t in self.minute_requests if now - t < 60]
        
        if len(self.minute_requests) >= self.MAX_REQUESTS_PER_MINUTE:
            sleep_time = 61 - (now - self.minute_requests[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.minute_requests.append(now)

def main():
    """Entry point for the batch processing system."""
    processor = BatchProcessor(batch_size=5)
    processor.process_all_ingredients()

if __name__ == "__main__":
    main()