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

# Setup logging with both file and console handlers
def setup_logging():
    log_filename = f'gemini_ingredient_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    logging.info(f"Logging initialized - writing to {log_filename}")

class RateLimiter:
    def __init__(self):
        self.minute_requests = []
        self.MAX_REQUESTS_PER_MINUTE = 14
        
    def wait_if_needed(self):
        now = time.time()
        self.minute_requests = [t for t in self.minute_requests if now - t < 60]
        
        if len(self.minute_requests) >= self.MAX_REQUESTS_PER_MINUTE:
            sleep_time = 61 - (now - self.minute_requests[0])
            if sleep_time > 0:
                logging.info(f"Rate limit reached. Waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
        
        self.minute_requests.append(now)

def get_existing_ingredients(conn) -> Set[str]:
    """Get all existing ingredients from active_ingredients_extended."""
    try:
        logging.info("Fetching existing ingredients...")
        with conn.cursor() as cur:
            cur.execute("SELECT ingredient_name FROM active_ingredients_extended;")
            ingredients = {row[0].lower() for row in cur.fetchall()}
        logging.info(f"Found {len(ingredients)} existing ingredients")
        return ingredients
    except Exception as e:
        logging.error(f"Error fetching existing ingredients: {e}")
        raise

def batch_ingredients(ingredients: List[str], batch_size: int = 5) -> List[List[str]]:
    """Split ingredients into smaller batches for processing."""
    return [ingredients[i:i + batch_size] for i in range(0, len(ingredients), batch_size)]

def create_gemini_prompt(ingredients: List[str], known_ingredients: Set[str]) -> str:
    ingredients_str = '", "'.join(ingredients)
    known_ingredients_str = '", "'.join(list(known_ingredients)[:100])
    
    return f'''Analyze these drug ingredients: "{ingredients_str}"

These are known valid ingredient names: "{known_ingredients_str}"

Return ONLY a valid JSON array of objects. Each object must have exactly these fields, with no additional fields:
- "original": input ingredient name (string)
- "normalized": standardized ingredient name (string)
- "confidence": number between 0 and 1
- "type": one of "exact_match", "close_match", or "new_ingredient"
- "notes": optional explanation (string)

Example of valid response format:
[
    {{
        "original": "example_drug",
        "normalized": "standardized_name",
        "confidence": 0.95,
        "type": "exact_match",
        "notes": "Direct match found"
    }}
]

Rules:
1. Response must be ONLY the JSON array, no markdown, no explanations
2. All string values must be in double quotes
3. Use exact matches when available
4. Split compound ingredients into separate entries
5. If unsure, use type "new_ingredient" with lower confidence'''

def clean_json_response(response: str) -> str:
    """Clean and validate JSON response from Gemini API."""
    # Remove any markdown code blocks
    if response.startswith('```json'):
        response = response.split('```json', 1)[1]
    if response.startswith('```'):
        response = response.split('```', 1)[1]
    if response.endswith('```'):
        response = response.rsplit('```', 1)[0]
    
    # Clean the response
    response = response.strip()
    
    # Handle potential YAML-style dashes
    if response.startswith('---'):
        response = response.split('---', 1)[1]
    
    return response

def process_ingredient_batch(batch: List[str], known_ingredients: Set[str], rate_limiter: RateLimiter) -> List[Dict]:
    """Process a batch of ingredients using Gemini API with rate limiting."""
    try:
        prompt = create_gemini_prompt(batch, known_ingredients)
        logging.info(f"Processing batch of {len(batch)} ingredients")
        
        rate_limiter.wait_if_needed()
        response, _ = gemini_api.generate_content(prompt)
        
        if not response:
            raise ValueError("Empty response from Gemini")
        
        # Log raw response for debugging
        logging.debug(f"Raw API response: {response}")
        
        # Clean and parse JSON response
        cleaned_response = clean_json_response(response)
        
        try:
            mappings = json.loads(cleaned_response)
        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error: {e}")
            logging.error(f"Cleaned response that failed to parse: {cleaned_response}")
            return []
        
        # Validate mappings structure
        if not isinstance(mappings, list):
            logging.error("API response is not a list")
            return []
            
        valid_mappings = []
        for mapping in mappings:
            if not isinstance(mapping, dict):
                continue
            if all(key in mapping for key in ['original', 'normalized', 'confidence', 'type']):
                valid_mappings.append(mapping)
                
        logging.info(f"Successfully processed {len(valid_mappings)} valid mappings")
        return valid_mappings
        
    except Exception as e:
        logging.error(f"Error processing batch: {e}")
        return []

def update_ingredients_table(conn, mappings: List[Dict]) -> None:
    """Update the ingredients table with new standardized names."""
    try:
        new_ingredients = [
            (m['normalized'], 'pending')
            for m in mappings
            if m['type'] == 'new_ingredient' and m['confidence'] > 0.7
        ]
        
        if new_ingredients:
            logging.info(f"Adding {len(new_ingredients)} new ingredients to database")
            with conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO active_ingredients_extended 
                    (ingredient_name, processing_status)
                    VALUES (%s, %s)
                    ON CONFLICT (ingredient_name) DO NOTHING;
                """, new_ingredients)
            conn.commit()
            logging.info("New ingredients added successfully")
            
    except Exception as e:
        logging.error(f"Error updating ingredients table: {e}")
        conn.rollback()
        raise

def create_bridge_records(conn, mappings: List[Dict]) -> List[Tuple[int, int]]:
    """Create bridge table records from the mappings."""
    try:
        bridge_records = []
        
        with conn.cursor() as cur:
            cur.execute("SELECT id, ingredient_name FROM active_ingredients_extended;")
            ingredient_ids = {name.lower(): id for id, name in cur.fetchall()}
        
        with conn.cursor() as cur:
            cur.execute("SELECT drug_id, activeingredient FROM drug_database;")
            for drug_id, ingredients_str in cur:
                if not ingredients_str:
                    continue
                    
                drug_mappings = [
                    m for m in mappings 
                    if m['original'].lower() in ingredients_str.lower()
                    and m['confidence'] > 0.7
                ]
                
                for mapping in drug_mappings:
                    normalized_name = mapping['normalized'].lower()
                    if normalized_name in ingredient_ids:
                        bridge_records.append((drug_id, ingredient_ids[normalized_name]))
        
        logging.info(f"Created {len(bridge_records)} bridge records")
        return bridge_records
        
    except Exception as e:
        logging.error(f"Error creating bridge records: {e}")
        raise

def verify_mappings(conn) -> Dict:
    """Verify the quality of ingredient mappings."""
    verification_results = {
        'suspicious_mappings': [],
        'sample_mappings': [],
        'unmapped_count': 0,
        'total_mappings': 0,
        'success': False
    }
    
    try:
        logging.info("Starting mapping verification")
        
        with conn.cursor() as cur:
            # Check suspicious patterns
            cur.execute("""
                SELECT d.activeingredient, 
                       string_agg(DISTINCT ae.ingredient_name, ', ') as mapped_ingredients,
                       COUNT(DISTINCT ae.id) as mapping_count
                FROM drug_database d
                JOIN drug_ingredients di ON d.drug_id = di.drug_id
                JOIN active_ingredients_extended ae ON di.ingredient_id = ae.id
                GROUP BY d.activeingredient
                HAVING COUNT(DISTINCT ae.id) > 5
                LIMIT 5;
            """)
            verification_results['suspicious_mappings'] = cur.fetchall()
            
            # Get random samples
            cur.execute("""
                SELECT d.activeingredient, 
                       string_agg(DISTINCT ae.ingredient_name, ', ') as mapped_ingredients
                FROM drug_database d
                JOIN drug_ingredients di ON d.drug_id = di.drug_id
                JOIN active_ingredients_extended ae ON di.ingredient_id = ae.id
                GROUP BY d.activeingredient
                ORDER BY RANDOM()
                LIMIT 5;
            """)
            verification_results['sample_mappings'] = cur.fetchall()
            
            # Check unmapped ingredients
            cur.execute("""
                SELECT COUNT(*) 
                FROM drug_database d
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM drug_ingredients di 
                    WHERE di.drug_id = d.drug_id
                ) AND activeingredient IS NOT NULL 
                AND activeingredient != '';
            """)
            verification_results['unmapped_count'] = cur.fetchone()[0]
            
            # Get total mappings
            cur.execute("SELECT COUNT(*) FROM drug_ingredients;")
            verification_results['total_mappings'] = cur.fetchone()[0]
        
        verification_results['success'] = True
        logging.info("Verification completed successfully")
        
        # Print verification summary
        print("\nVerification Summary:")
        print(f"Total mappings: {verification_results['total_mappings']}")
        print(f"Unmapped ingredients: {verification_results['unmapped_count']}")
        print(f"Suspicious mappings found: {len(verification_results['suspicious_mappings'])}")
        
        if verification_results['suspicious_mappings']:
            print("\nSuspicious Mappings (>5 splits):")
            for orig, mapped, count in verification_results['suspicious_mappings']:
                print(f"\nOriginal: {orig}")
                print(f"Mapped to {count} ingredients: {mapped}")
        
        if verification_results['sample_mappings']:
            print("\nRandom Mapping Samples:")
            for orig, mapped in verification_results['sample_mappings']:
                print(f"\nOriginal: {orig}")
                print(f"Mapped to: {mapped}")
        
        return verification_results
        
    except Exception as e:
        logging.error(f"Error during verification: {e}")
        verification_results['error'] = str(e)
        return verification_results

def migrate_with_gemini(batch_size: int = 50, verify: bool = True):
    """Main function to perform the migration using Gemini."""
    start_time = datetime.now()
    setup_logging()
    logging.info(f"Starting migration process at {start_time}")
    
    conn = None
    try:
        # Initialize connection and APIs
        conn = database.connect_to_db()
        if not conn:
            logging.error("Failed to establish database connection")
            return
        
        gemini_api.initialize_gemini()
        rate_limiter = RateLimiter()
        
        # Get existing ingredients
        known_ingredients = get_existing_ingredients(conn)
        
        # Get unmapped ingredients
        logging.info("Fetching unmapped ingredients from database")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT activeingredient 
                FROM drug_database 
                WHERE activeingredient IS NOT NULL 
                AND activeingredient != '';
            """)
            all_ingredients = [row[0] for row in cur.fetchall()]
        
        logging.info(f"Found {len(all_ingredients)} total ingredients to process")
        
        # Process in batches
        all_mappings = []
        total_batches = len(all_ingredients) // batch_size + (1 if len(all_ingredients) % batch_size else 0)
        
        for batch_num, batch in enumerate(batch_ingredients(all_ingredients, batch_size), 1):
            logging.info(f"Processing batch {batch_num}/{total_batches}")
            
            mappings = process_ingredient_batch(batch, known_ingredients, rate_limiter)
            if mappings:
                all_mappings.extend(mappings)
                update_ingredients_table(conn, mappings)
                known_ingredients.update(
                    m['normalized'].lower() 
                    for m in mappings 
                    if m['type'] == 'new_ingredient' and m['confidence'] > 0.7
                )
        
        # Create and insert bridge records
        bridge_records = create_bridge_records(conn, all_mappings)
        if bridge_records:
            logging.info(f"Inserting {len(bridge_records)} bridge records")
            with conn.cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO drug_ingredients (drug_id, ingredient_id)
                    VALUES (%s, %s)
                    ON CONFLICT (drug_id, ingredient_id) DO NOTHING;
                """, bridge_records)
            conn.commit()
        
        # Run verification if requested
        if verify:
            logging.info("Starting verification process")
            verification_results = verify_mappings(conn)
            if verification_results['success']:
                logging.info("Verification completed successfully")
            else:
                logging.warning("Verification completed with issues")
        else:
            logging.info("Verification skipped (verify=False)")
        
        end_time = datetime.now()
        processing_time = end_time - start_time
        logging.info(f"Migration completed. Total processing time: {processing_time}")
        
    except Exception as e:
        logging.error(f"Fatal error during migration: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    migrate_with_gemini(batch_size=10, verify=True)