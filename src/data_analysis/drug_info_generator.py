import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from . import database
from .. import gemini_api
import json
import time
from datetime import datetime
import logging


# Setup logging
logging.basicConfig(
    filename=f'drug_info_generator_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
                print(f"Rate limit reached. Waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
        
        self.minute_requests.append(now)

def safe_execute(cur, query, params=None):
    """Execute a database query with proper error handling."""
    try:
        cur.execute(query, params)
        return True
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        if cur.connection:
            cur.connection.rollback()
        return False

def create_extended_table(conn):
    """Creates the extended active_ingredients table with additional columns."""
    try:
        with conn.cursor() as cur:
            safe_execute(cur, """
                CREATE TABLE IF NOT EXISTS active_ingredients_extended (
                    id SERIAL PRIMARY KEY,
                    ingredient_name VARCHAR(255) NOT NULL UNIQUE,
                    short_description TEXT,
                    common_uses TEXT,
                    side_effects TEXT,
                    contraindications TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processing_status VARCHAR(50) DEFAULT 'pending',
                    error_message TEXT
                );
            """)
            conn.commit()
            print("Table structure verified successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
        raise
    
def get_batch_drug_info_prompt(ingredients):
    """Generates a prompt for multiple ingredients at once."""
    ingredients_list = '", "'.join(ingredients)
    logging.info(f"Preparing prompt for ingredients: {ingredients_list}")
    return f'''Analyze these active ingredients: "{ingredients_list}"

For each ingredient, create a JSON object. Return an array of objects with this structure:
[
    {{
        "ingredient_name": "name of the ingredient",
        "short_description": "Brief description of what this ingredient is",
        "common_uses": "List main medical uses, separated by semicolons",
        "side_effects": "List common side effects, separated by semicolons",
        "contraindications": "List main contraindications, separated by semicolons"
    }}
]

Rules:
1. Respond ONLY with the JSON array
2. Keep descriptions factual and brief
3. Include ALL ingredients in the response
4. If information is uncertain for an ingredient, use "Information not available"
5. Make sure ingredient_name matches exactly with the input names
'''

def process_ingredient_batch(conn, ingredients, rate_limiter):
    """Process a batch of ingredients with a single API call."""
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.info(f"Starting batch {batch_id} with {len(ingredients)} ingredients")
    print(f"\nProcessing batch {batch_id} with {len(ingredients)} ingredients...")
    
    try:
        # Wait for rate limiting if needed
        rate_limiter.wait_if_needed()
        logging.info(f"Batch {batch_id} - Rate limiter check passed")
        
        # Get information for all ingredients in one API call
        prompt = get_batch_drug_info_prompt(ingredients)
        response, _ = gemini_api.generate_content(prompt)
        
        if not response:
            error_msg = "Empty response from API"
            logging.error(f"Batch {batch_id} - {error_msg}")
            raise Exception(error_msg)
        
        logging.info(f"Batch {batch_id} - Received API response")
        
        # Clean response and parse JSON
        response = response.strip()
        if response.startswith('```'):
            response = response.split('\n', 1)[1]
        if response.endswith('```'):
            response = response.rsplit('\n', 1)[0]
        response = response.strip()
        
        info_list = json.loads(response)
        logging.info(f"Batch {batch_id} - Successfully parsed JSON response with {len(info_list)} ingredients")
        
        # Verify all ingredients are present in response
        received_ingredients = {info['ingredient_name'] for info in info_list}
        missing_ingredients = set(ingredients) - received_ingredients
        if missing_ingredients:
            logging.warning(f"Batch {batch_id} - Missing ingredients in response: {missing_ingredients}")
        
        # Update database in a single transaction
        with conn.cursor() as cur:
            successful_updates = []
            for info in info_list:
                ingredient = info['ingredient_name']
                logging.info(f"Batch {batch_id} - Processing ingredient: {ingredient}")
                
                success = safe_execute(cur, """
                    INSERT INTO active_ingredients_extended 
                    (ingredient_name, short_description, common_uses, 
                     side_effects, contraindications, processing_status)
                    VALUES (%s, %s, %s, %s, %s, 'completed')
                    ON CONFLICT (ingredient_name) 
                    DO UPDATE SET 
                        short_description = EXCLUDED.short_description,
                        common_uses = EXCLUDED.common_uses,
                        side_effects = EXCLUDED.side_effects,
                        contraindications = EXCLUDED.contraindications,
                        processing_status = 'completed',
                        error_message = NULL,
                        last_updated = CURRENT_TIMESTAMP;
                """, (
                    ingredient,
                    info.get('short_description', 'Information not available'),
                    info.get('common_uses', 'Information not available'),
                    info.get('side_effects', 'Information not available'),
                    info.get('contraindications', 'Information not available')
                ))
                
                if success:
                    successful_updates.append(ingredient)
                    logging.info(f"Batch {batch_id} - Successfully updated {ingredient}")
                else:
                    logging.error(f"Batch {batch_id} - Failed to update {ingredient}")
            
            conn.commit()
            logging.info(f"Batch {batch_id} - Transaction committed successfully")
            print(f"✓ Successfully processed batch of {len(successful_updates)} ingredients")
            
            # Log detailed success information
            logging.info(f"Batch {batch_id} - Successfully processed ingredients: {', '.join(successful_updates)}")
            return len(successful_updates)
            
    except Exception as e:
        error_msg = str(e)
        logging.error(f"Batch {batch_id} - Error processing batch: {error_msg}")
        print(f"× Error processing batch: {error_msg}")
        
        # Log errors for all ingredients in the batch
        try:
            with conn.cursor() as cur:
                for ingredient in ingredients:
                    logging.info(f"Batch {batch_id} - Recording error for {ingredient}")
                    safe_execute(cur, """
                        INSERT INTO active_ingredients_extended 
                        (ingredient_name, processing_status, error_message)
                        VALUES (%s, 'error', %s)
                        ON CONFLICT (ingredient_name) 
                        DO UPDATE SET 
                            processing_status = 'error',
                            error_message = EXCLUDED.error_message,
                            last_updated = CURRENT_TIMESTAMP;
                    """, (ingredient, error_msg[:500]))
                conn.commit()
                logging.info(f"Batch {batch_id} - Error status recorded for all ingredients")
        except Exception as db_error:
            logging.error(f"Batch {batch_id} - Error logging failures: {db_error}")
            print(f"Error logging failures: {db_error}")
            conn.rollback()
    
    return 0

def populate_extended_table(conn, batch_size=5):
    """Populates the extended table with information from Gemini API using optimized batch processing."""
    start_time = datetime.now()
    logging.info(f"Starting population process at {start_time} with batch size {batch_size}")
    
    try:
        processed_count = 0
        rate_limiter = RateLimiter()
        
        # Get total count for progress tracking
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(DISTINCT ingredient_name) 
                FROM active_ingredients 
                WHERE ingredient_name NOT IN (
                    SELECT ingredient_name 
                    FROM active_ingredients_extended 
                    WHERE processing_status IN ('completed', 'error')
                );
            """)
            total_ingredients = cur.fetchone()[0]
            logging.info(f"Total ingredients to process: {total_ingredients}")
        
        while True:
            # Get next batch of ingredients
            query = """
                SELECT DISTINCT ingredient_name 
                FROM active_ingredients 
                WHERE ingredient_name NOT IN (
                    SELECT ingredient_name 
                    FROM active_ingredients_extended 
                    WHERE processing_status IN ('completed', 'error')
                )
                LIMIT %s;
            """
            df = database.execute_query(conn, query, params=(batch_size,))
            
            if df is None or df.empty:
                logging.info("No more ingredients to process")
                print("\nAll ingredients have been processed.")
                break
                
            ingredients = df['ingredient_name'].tolist()
            newly_processed = process_ingredient_batch(conn, ingredients, rate_limiter)
            processed_count += newly_processed
            
            # Calculate and log progress
            progress_percent = (processed_count / total_ingredients) * 100 if total_ingredients > 0 else 0
            elapsed_time = datetime.now() - start_time
            avg_time_per_batch = elapsed_time / ((processed_count / batch_size) if processed_count > 0 else 1)
            
            status_msg = f"""
            Progress: {processed_count}/{total_ingredients} ({progress_percent:.1f}%)
            Elapsed time: {elapsed_time}
            Average time per batch: {avg_time_per_batch}
            """
            logging.info(status_msg)
            print(status_msg)
            
            # Optional: Add a delay between batches
            time.sleep(2)
            
    except Exception as e:
        logging.error(f"Error in batch processing: {e}")
        print(f"Error in batch processing: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        # Print final summary
        try:
            with conn.cursor() as cur:
                safe_execute(cur, """
                    SELECT processing_status, COUNT(*) 
                    FROM active_ingredients_extended 
                    GROUP BY processing_status;
                """)
                results = cur.fetchall()
                
                summary = "\nFinal Processing Summary:"
                for status, count in results:
                    summary += f"\n{status}: {count}"
                
                end_time = datetime.now()
                total_time = end_time - start_time
                summary += f"\nTotal processing time: {total_time}"
                
                logging.info(summary)
                print(summary)
        except Exception as e:
            logging.error(f"Error getting final summary: {e}")
            print(f"Error getting final summary: {e}")

def main():
    start_time = datetime.now()
    logging.info(f"Starting drug information generation process at {start_time}")
    print("Starting drug information generation process...")
    
    # Initialize Gemini API
    gemini_api.initialize_gemini()
    logging.info("Gemini API initialized")
    
    # Connect to database
    conn = database.connect_to_db()
    if not conn:
        logging.error("Failed to connect to database")
        return
    logging.info("Database connection established")
    
    try:
        # Create/verify table structure
        create_extended_table(conn)
        logging.info("Table structure verified")
        
        # Process ingredients with optimized batch size
        populate_extended_table(conn, batch_size=5)
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        print(f"Fatal error: {e}")
    finally:
        conn.close()
        end_time = datetime.now()
        processing_time = end_time - start_time
        logging.info(f"Process completed at {end_time}. Total time: {processing_time}")
        print(f"\nProcess completed. Total time: {processing_time}")

if __name__ == "__main__":
    main()