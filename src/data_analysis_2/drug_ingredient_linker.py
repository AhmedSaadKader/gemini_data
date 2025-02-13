import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
from typing import List, Dict, Set, Tuple
import json
import time
from .. import gemini_api
from .. import config

class DrugIngredientLinker:
    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging with both file and console output."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f'drug_ingredient_linking_{timestamp}.log'
        report_filename = f'linking_report_{timestamp}.txt'
        
        # Main logger
        self.logger = logging.getLogger('DrugIngredientLinker')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        self.logger.addHandler(console_handler)
        
        self.report_file = report_filename
        
    def write_report(self, content: str):
        """Append content to the report file."""
        with open(self.report_file, 'a', encoding='utf-8') as f:
            f.write(content + '\n')

    def clean_ingredient_text(self, text: str) -> str:
        """Clean and standardize ingredient text for better matching."""
        if not text:
            return ""
        # Remove common prefixes/suffixes that might interfere with matching
        text = text.lower()
        text = text.replace(".", "")
        text = text.replace("(", " ")
        text = text.replace(")", " ")
        # Standardize common separators
        text = text.replace("/", "+")
        text = text.replace("&", "+")
        text = text.replace(" and ", "+")
        # Normalize whitespace
        text = " ".join(text.split())
        return text

    def create_gemini_prompt(self, drug_entry: Dict, known_ingredients: List[str]) -> str:
        """Create a prompt for Gemini API to analyze drug ingredients."""
        # Clean and prepare ingredient text
        cleaned_ingredient = self.clean_ingredient_text(drug_entry['activeingredient'])
        ingredients_list = '", "'.join(known_ingredients[:100])  # Limit list size
        
        return f'''You are a pharmaceutical database expert. Analyze this drug's active ingredients and map them to known ingredients. 

Drug Details:
Name: {drug_entry['tradename']}
Active Ingredients: {cleaned_ingredient}
Original Text: {drug_entry['activeingredient']}
Form: {drug_entry['form']}
Group: {drug_entry['group']}

Known active ingredients (subset): "{ingredients_list}"

Return ONLY a JSON array of mappings following this structure:
{{
    "mappings": [
        {{
            "original": "exact text from drug's active ingredient",
            "matched_ingredient": "exact name from known ingredients list",
            "confidence": number between 0 and 1,
            "notes": "explanation of mapping"
        }}
    ]
}}

Mapping Rules:
1. ONLY use exact matches from the known ingredients list
2. Split compound ingredients at +, /, and common delimiters
3. Remove dosage information (e.g., "500mg")
4. Set confidence scores:
   - 0.95+ for exact matches
   - 0.85-0.94 for clear matches with minor formatting differences
   - Below 0.85 for uncertain matches
5. Never create new ingredients
6. Skip ingredients without clear matches

Example response:
{{
    "mappings": [
        {{
            "original": "paracetamol 500mg",
            "matched_ingredient": "Paracetamol",
            "confidence": 0.98,
            "notes": "Exact match after removing dosage"
        }}
    ]
}}

IMPORTANT:
1. Response must be ONLY the JSON object
2. ALL strings must use double quotes
3. Include only confident matches
4. Never suggest new ingredients'''

    def get_known_ingredients(self, conn) -> List[str]:
        """Get list of all known ingredients from active_ingredients_extended."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ingredient_name 
                FROM active_ingredients_extended 
                WHERE processing_status != 'duplicate'
                ORDER BY ingredient_name;
            """)
            return [row[0] for row in cur.fetchall()]

    def get_unlinked_drugs(self, conn, sample_size: int = None, sample_groups: bool = True) -> List[Dict]:
        """
        Get drugs that haven't been linked yet.
        
        Args:
            conn: Database connection
            sample_size: If provided, limits the number of drugs returned
            sample_groups: If True, tries to get samples from different drug groups
        """
        with conn.cursor() as cur:
            if sample_size and sample_groups:
                # Get sample from different drug groups
                cur.execute("""
                    WITH GroupedDrugs AS (
                        SELECT 
                            drug_id, tradename, activeingredient, form, "group",
                            ROW_NUMBER() OVER (PARTITION BY "group" ORDER BY RANDOM()) as rn
                        FROM drug_database d
                        WHERE NOT EXISTS (
                            SELECT 1 FROM drug_ingredients di 
                            WHERE di.drug_id = d.drug_id
                        )
                        AND activeingredient IS NOT NULL 
                        AND activeingredient != ''
                    )
                    SELECT drug_id, tradename, activeingredient, form, "group"
                    FROM GroupedDrugs
                    WHERE rn <= CEIL(%s::float / (
                        SELECT COUNT(DISTINCT "group") 
                        FROM drug_database
                        WHERE activeingredient IS NOT NULL 
                        AND activeingredient != ''
                    ))
                    LIMIT %s;
                """, (sample_size, sample_size))
            elif sample_size:
                # Simple random sample
                cur.execute("""
                    SELECT drug_id, tradename, activeingredient, form, "group"
                    FROM drug_database d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM drug_ingredients di 
                        WHERE di.drug_id = d.drug_id
                    )
                    AND activeingredient IS NOT NULL 
                    AND activeingredient != ''
                    ORDER BY RANDOM()
                    LIMIT %s;
                """, (sample_size,))
            else:
                # Get all unlinked drugs
                cur.execute("""
                    SELECT drug_id, tradename, activeingredient, form, "group"
                    FROM drug_database d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM drug_ingredients di 
                        WHERE di.drug_id = d.drug_id
                    )
                    AND activeingredient IS NOT NULL 
                    AND activeingredient != ''
                    ORDER BY drug_id;
                """)
            
            return [
                {
                    'drug_id': row[0],
                    'tradename': row[1],
                    'activeingredient': row[2],
                    'form': row[3],
                    'group': row[4]
                }
                for row in cur.fetchall()
            ]

    def process_batch(self, conn, batch: List[Dict], known_ingredients: List[str]) -> Tuple[int, List[Dict], List[Dict]]:
        """Process a batch of drugs and return suggested mappings."""
        successful_mappings = []
        failed_mappings = []
        errors = 0
        
        for drug in batch:
            try:
                # Rate limiting - sleep for 4 seconds between requests (15 RPM limit)
                time.sleep(4)
                
                # Create and send prompt
                prompt = self.create_gemini_prompt(drug, known_ingredients)
                response, _ = gemini_api.generate_content(prompt)
                
                if not response:
                    raise ValueError("Empty response from Gemini API")
                
                # Clean and parse response
                try:
                    # Clean response of any markdown or extra formatting
                    cleaned_response = response.strip()
                    if '```json' in cleaned_response:
                        cleaned_response = cleaned_response.split('```json', 1)[1]
                    elif '```' in cleaned_response:
                        cleaned_response = cleaned_response.split('```', 1)[1]
                    if cleaned_response.endswith('```'):
                        cleaned_response = cleaned_response.rsplit('```', 1)[0]
                    
                    # Remove any YAML-style markers
                    if cleaned_response.startswith('---'):
                        cleaned_response = cleaned_response.split('---', 1)[1]
                    
                    cleaned_response = cleaned_response.strip()
                    
                    # Log the cleaned response for debugging
                    self.logger.debug(f"Cleaned API response for {drug['tradename']}: {cleaned_response}")
                    
                    try:
                        analysis = json.loads(cleaned_response)
                    except json.JSONDecodeError as e:
                        self.logger.error(
                            f"JSON parsing error for drug {drug['tradename']}: {e}\n"
                            f"Raw response: {response}\n"
                            f"Cleaned response: {cleaned_response}"
                        )
                        failed_mappings.append({
                            'drug_id': drug['drug_id'],
                            'tradename': drug['tradename'],
                            'activeingredient': drug['activeingredient'],
                            'error': f"JSON parsing error: {str(e)}"
                        })
                        errors += 1
                        continue
                        
                except Exception as e:
                    self.logger.error(
                        f"Error cleaning response for drug {drug['tradename']}: {e}\n"
                        f"Raw response: {response}"
                    )
                    failed_mappings.append({
                        'drug_id': drug['drug_id'],
                        'tradename': drug['tradename'],
                        'activeingredient': drug['activeingredient'],
                        'error': str(e)
                    })
                    errors += 1
                    continue
                
                if not isinstance(analysis, dict) or 'mappings' not in analysis:
                    self.logger.error(f"Invalid response structure for drug {drug['tradename']}")
                    failed_mappings.append({
                        'drug_id': drug['drug_id'],
                        'tradename': drug['tradename'],
                        'activeingredient': drug['activeingredient'],
                        'error': "Invalid response structure"
                    })
                    errors += 1
                    continue
                
                # Validate mappings
                valid_mappings = []
                for mapping in analysis['mappings']:
                    if not all(key in mapping for key in ['original', 'matched_ingredient', 'confidence', 'notes']):
                        continue
                    
                    if mapping['confidence'] >= 0.85 and mapping['matched_ingredient'] in known_ingredients:
                        mapping['drug_id'] = drug['drug_id']
                        mapping['tradename'] = drug['tradename']
                        valid_mappings.append(mapping)
                
                if not valid_mappings:
                    failed_mappings.append({
                        'drug_id': drug['drug_id'],
                        'tradename': drug['tradename'],
                        'activeingredient': drug['activeingredient'],
                        'error': "No valid ingredient mappings found"
                    })
                
                successful_mappings.extend(valid_mappings)
                
            except Exception as e:
                self.logger.error(f"Error processing drug {drug['tradename']}: {e}")
                failed_mappings.append({
                    'drug_id': drug['drug_id'],
                    'tradename': drug['tradename'],
                    'activeingredient': drug['activeingredient'],
                    'error': str(e)
                })
                errors += 1
                continue
        
        return errors, successful_mappings, failed_mappings

    def create_links(self, conn, mappings: List[Dict]) -> Tuple[int, int]:
        """Create the actual database links based on validated mappings."""
        successful = 0
        errors = 0
        
        try:
            with conn.cursor() as cur:
                # Get ingredient IDs
                ingredient_names = {m['matched_ingredient'] for m in mappings}
                placeholders = ','.join(['%s'] * len(ingredient_names))
                cur.execute(f"""
                    SELECT id, ingredient_name 
                    FROM active_ingredients_extended 
                    WHERE ingredient_name IN ({placeholders});
                """, list(ingredient_names))
                
                ingredient_ids = {row[1]: row[0] for row in cur.fetchall()}
                
                # Create links
                for mapping in mappings:
                    try:
                        ingredient_id = ingredient_ids.get(mapping['matched_ingredient'])
                        if not ingredient_id:
                            continue
                        
                        cur.execute("""
                            INSERT INTO drug_ingredients (drug_id, ingredient_id)
                            VALUES (%s, %s)
                            ON CONFLICT (drug_id, ingredient_id) DO NOTHING
                            RETURNING id;
                        """, (mapping['drug_id'], ingredient_id))
                        
                        if cur.fetchone():
                            successful += 1
                            
                    except Exception as e:
                        self.logger.error(f"Error creating link for {mapping['tradename']}: {e}")
                        errors += 1
                        continue
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error in create_links: {e}")
            conn.rollback()
            errors += 1
            
        return successful, errors

    def generate_mapping_report(self, mappings: List[Dict], failed_mappings: List[Dict] = None) -> str:
        """Generate a detailed report of proposed mappings and failures."""
        report = [
            "Drug-Ingredient Mapping Report",
            f"Generated at: {datetime.now()}",
            f"Total mappings proposed: {len(mappings)}",
            f"Total failed mappings: {len(failed_mappings) if failed_mappings else 0}\n"
        ]
        
        # First list the successful mappings
        if mappings:
            report.append("=== Successful Mappings ===")
            # Group mappings by drug
            drug_mappings = {}
            for mapping in mappings:
                drug_id = mapping['drug_id']
                if drug_id not in drug_mappings:
                    drug_mappings[drug_id] = {
                        'tradename': mapping['tradename'],
                        'mappings': []
                    }
                drug_mappings[drug_id]['mappings'].append(mapping)
            
            # Generate report for each drug
            for drug_id, drug_data in drug_mappings.items():
                report.extend([
                    f"\nDrug: {drug_data['tradename']} (ID: {drug_id})",
                    "Proposed ingredient mappings:"
                ])
                
                for mapping in drug_data['mappings']:
                    report.extend([
                        f"  Original: {mapping['original']}",
                        f"  Matched to: {mapping['matched_ingredient']}",
                        f"  Confidence: {mapping['confidence']}",
                        f"  Notes: {mapping['notes']}",
                        ""
                    ])
        
        # Then list the failures
        if failed_mappings:
            report.extend([
                "\n=== Failed Mappings ===",
                "The following drugs could not be processed:\n"
            ])
            
            for failed in failed_mappings:
                report.extend([
                    f"Drug: {failed['tradename']} (ID: {failed['drug_id']})",
                    f"Active ingredients: {failed['activeingredient']}",
                    f"Error: {failed['error']}",
                    ""
                ])
        
        return '\n'.join(report)

    def process_all_drugs(self):
        """Main processing function."""
        start_time = datetime.now()
        self.logger.info("Starting drug-ingredient linking process")
        
        try:
            # Initialize Gemini API
            gemini_api.initialize_gemini()
            
            # Connect to database
            conn = psycopg2.connect(
                dbname=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST
            )
            
            try:
                # Get known ingredients
                known_ingredients = self.get_known_ingredients(conn)
                self.logger.info(f"Found {len(known_ingredients)} known ingredients")
                
                # Get unlinked drugs
                unlinked_drugs = self.get_unlinked_drugs(conn)
                total_drugs = len(unlinked_drugs)
                self.logger.info(f"Found {total_drugs} unlinked drugs")
                
                if not unlinked_drugs:
                    self.logger.info("No unlinked drugs found")
                    return
                
                # Process in batches
                all_mappings = []
                total_errors = 0
                
                for i in range(0, total_drugs, self.batch_size):
                    batch = unlinked_drugs[i:i + self.batch_size]
                    batch_num = i // self.batch_size + 1
                    total_batches = (total_drugs + self.batch_size - 1) // self.batch_size
                    
                    self.logger.info(f"Processing batch {batch_num}/{total_batches}")
                    
                    errors, mappings = self.process_batch(conn, batch, known_ingredients)
                    total_errors += errors
                    all_mappings.extend(mappings)
                    
                    # Log progress
                    progress = (i + len(batch)) / total_drugs * 100
                    self.logger.info(
                        f"Progress: {progress:.1f}% - "
                        f"Found {len(mappings)} mappings in this batch"
                    )
                
                # Generate and save report
                report = self.generate_mapping_report(all_mappings)
                self.write_report(report)
                
                # Show summary and ask for confirmation
                print(f"\nFound {len(all_mappings)} potential mappings across {total_drugs} drugs")
                print(f"Detailed report written to: {self.report_file}")
                
                if input("\nCreate these ingredient links? (yes/no): ").lower() == 'yes':
                    successful, errors = self.create_links(conn, all_mappings)
                    
                    # Log results
                    self.logger.info(f"""
                    Results:
                    - Successful links created: {successful}
                    - Errors encountered: {errors}
                    - Total processing time: {datetime.now() - start_time}
                    """)
                    
                    print(f"\nCreated {successful} links with {errors} errors")
                else:
                    print("Operation cancelled")
                
            finally:
                conn.close()
                self.logger.info("Database connection closed")
                
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            raise

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Link drugs to active ingredients.')
    parser.add_argument('--sample', type=int, help='Number of drugs to sample for testing')
    parser.add_argument('--batch-size', type=int, default=25, help='Batch size for processing')
    parser.add_argument('--random-sample', action='store_true', 
                       help='Use random sampling instead of group-based sampling')
    args = parser.parse_args()

    linker = DrugIngredientLinker(batch_size=args.batch_size)
    
    try:
        # Initialize Gemini API
        gemini_api.initialize_gemini()
        
        # Connect to database
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        try:
            # Get known ingredients
            known_ingredients = linker.get_known_ingredients(conn)
            linker.logger.info(f"Found {len(known_ingredients)} known ingredients")
            
            # Get unlinked drugs (with optional sampling)
            unlinked_drugs = linker.get_unlinked_drugs(
                conn, 
                sample_size=args.sample,
                sample_groups=not args.random_sample
            )
            total_drugs = len(unlinked_drugs)
            
            if args.sample:
                linker.logger.info(
                    f"Working with sample of {total_drugs} drugs "
                    f"({'random' if args.random_sample else 'group-based'} sampling)"
                )
            else:
                linker.logger.info(f"Found {total_drugs} unlinked drugs")
            
            if not unlinked_drugs:
                linker.logger.info("No unlinked drugs found")
                return
            
            # Process drugs in batches
            all_mappings = []
            all_failed_mappings = []
            total_errors = 0
            
            # First, log all drugs in the sample
            linker.logger.info("\nProcessing the following drugs:")
            for drug in unlinked_drugs:
                linker.logger.info(f"- {drug['tradename']} (ID: {drug['drug_id']})")
                linker.logger.info(f"  Active ingredients: {drug['activeingredient']}")
            
            for i in range(0, total_drugs, args.batch_size):
                batch = unlinked_drugs[i:i + args.batch_size]
                batch_num = i // args.batch_size + 1
                total_batches = (total_drugs + args.batch_size - 1) // args.batch_size
                
                linker.logger.info(f"\nProcessing batch {batch_num}/{total_batches}")
                
                errors, mappings, failed_mappings = linker.process_batch(conn, batch, known_ingredients)
                total_errors += errors
                all_mappings.extend(mappings)
                all_failed_mappings.extend(failed_mappings)
                
                # Log progress
                progress = (i + len(batch)) / total_drugs * 100
                linker.logger.info(
                    f"Progress: {progress:.1f}% - "
                    f"Found {len(mappings)} mappings in this batch"
                )
            
            # Generate and save report
            report = linker.generate_mapping_report(all_mappings, all_failed_mappings)
            linker.write_report(report)
            
            # Show summary and ask for confirmation
            print(f"\nFound {len(all_mappings)} potential mappings across {total_drugs} drugs")
            print(f"Detailed report written to: {linker.report_file}")
            
            if args.sample:
                print("\nThis was a test run. No changes will be made to the database.")
                print("Review the report and if the results look good, run without --sample")
            else:
                if input("\nCreate these ingredient links? (yes/no): ").lower() == 'yes':
                    successful, errors = linker.create_links(conn, all_mappings)
                    print(f"\nCreated {successful} links with {errors} errors")
                else:
                    print("Operation cancelled")
            
        finally:
            conn.close()
            linker.logger.info("Database connection closed")
            
    except Exception as e:
        linker.logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()