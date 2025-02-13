import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
from typing import List, Dict, Set, Tuple
import json
from .. import gemini_api

class AdvancedDuplicateCleaner:
    def __init__(self):
        self.setup_logging()
        
        # Known equivalence groups for validation
        self.equivalence_groups = {
            # Adenosine group
            'adenosine': {
                'adenosine',
                'adenosine triphosphate',
                'adenosine triphosphate disodium'
            },
            # AHA group
            'alpha hydroxy acid': {
                'aha',
                'aha firming extracts',
                'alpha hydroxy acid',
                'alpha hydroxy acids'
            },
            # Lipoic acid group
            'alpha lipoic acid': {
                'ala',
                'alpha-lipoic acid',
                'alpha lipoic acid'
            },
            # Aloe vera group
            'aloe vera': {
                'aloe vera',
                'aloevera',
                'alovera',
                'aloe barbadensis'
            }
        }

    def setup_logging(self):
        """Configure logging with both file and console output."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f'advanced_duplicate_cleanup_{timestamp}.log'
        
        # Create formatters
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        
        # Setup root logger
        self.logger = logging.getLogger('AdvancedDuplicateCleaner')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

    def create_gemini_prompt(self, ingredients: List[Dict]) -> str:
        """Create an advanced prompt for Gemini API to analyze potential duplicates."""
        # Create a formatted list of ingredients with their descriptions
        ingredient_items = []
        for ing in ingredients:
            desc = ing.get('description', 'No description available')
            ingredient_items.append(f"{ing['name']}: {desc}")
        
        ingredients_text = '\n'.join(ingredient_items)
        
        return f'''You are a pharmaceutical database expert. Analyze these ingredient names and their descriptions to identify duplicates and variations. Return ONLY a JSON object.

Important: Your response must be ONLY valid JSON with no markdown formatting, no explanation text.

Input ingredients with descriptions:
{ingredients_text}

Required JSON structure:
{{
    "groups": [
        {{
            "primary_name": "standardized name to use",
            "variations": ["list", "of", "equivalent", "names"],
            "confidence": number between 0 and 1,
            "reason": "explanation based on names and descriptions"
        }}
    ]
}}

Analysis rules:
1. Group chemically equivalent compounds - use descriptions to verify equivalence
2. Consider pharmaceutical salt forms and hydrates
3. Look for spelling variations and formatting differences
4. Consider synonyms and different naming conventions
5. Set high confidence (>0.95) only when descriptions strongly support equivalence
6. Provide detailed reasoning referencing both names and descriptions
7. Be conservative - when in doubt, don't group
8. Pay special attention to possible typos (e.g., "Baicapil" vs "Biacapil")
9. Look for small spelling variations that might indicate data entry errors
10. Use context to determine if two names refer to the same compound

Example response:
{{
    "groups": [
        {{
            "primary_name": "ascorbic acid",
            "variations": ["vitamin c", "l-ascorbic acid"],
            "confidence": 0.98,
            "reason": "Descriptions confirm these are identical compounds - all reference the same vitamin C molecule with identical biological function"
        }}
    ]
}}'''

    def clean_api_response(self, response: str) -> str:
        """Clean and validate the API response."""
        try:
            # Log the raw response for debugging
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

    def analyze_duplicates(self, conn) -> List[Dict]:
        """Analyze the database for potential duplicates using advanced criteria."""
        try:
            with conn.cursor() as cur:
                # Get all active ingredients with descriptions
                cur.execute("""
                    SELECT id, ingredient_name, short_description
                    FROM active_ingredients_extended
                    WHERE processing_status != 'duplicate'
                    ORDER BY LOWER(ingredient_name);
                """)
                
                ingredients = [
                    {
                        'id': row[0], 
                        'name': row[1],
                        'description': row[2] if row[2] else 'No description available'
                    } 
                    for row in cur.fetchall()
                ]
                
                # Process in batches
                batch_size = 100
                all_groups = []
                
                for i in range(0, len(ingredients), batch_size):
                    batch = ingredients[i:i + batch_size]
                    self.logger.info(f"Processing batch {i//batch_size + 1} of {(len(ingredients) + batch_size - 1)//batch_size}")
                    
                    try:
                        # Create and send prompt
                        prompt = self.create_gemini_prompt(batch)
                        response, _ = gemini_api.generate_content(prompt)
                        
                        if not response:
                            self.logger.error("Empty response from Gemini API")
                            continue
                        
                        # Clean and parse response
                        cleaned_response = self.clean_api_response(response)
                        analysis = json.loads(cleaned_response)
                        
                        if not isinstance(analysis, dict) or 'groups' not in analysis:
                            self.logger.error("Invalid response structure - missing 'groups' key")
                            continue
                        
                        # Filter out groups with no variations or empty variations
                        valid_groups = []
                        for group in analysis['groups']:
                            if not all(key in group for key in ['primary_name', 'variations', 'confidence', 'reason']):
                                self.logger.warning(f"Skipping group due to missing required fields: {group}")
                                continue
                                
                            # Skip groups with no variations or empty variations list
                            if not group.get('variations') or len(group['variations']) == 0:
                                self.logger.info(f"Skipping group with no variations: {group['primary_name']}")
                                continue
                                
                            valid_groups.append(group)
                        
                        all_groups.extend(valid_groups)
                        self.logger.info(f"Found {len(valid_groups)} valid duplicate groups in this batch")
                        
                    except Exception as e:
                        self.logger.error(f"Error processing batch: {e}")
                        continue
                
                return all_groups
                
        except Exception as e:
            self.logger.error(f"Error analyzing duplicates: {e}")
            return []

    def apply_duplicate_groups(self, conn, groups: List[Dict]) -> Tuple[int, int]:
      """Apply the identified duplicate groups to the database."""
      updates = 0
      errors = 0
      
      try:
          with conn.cursor() as cur:
              for group in groups:
                  if group['confidence'] < 0.9:
                      self.logger.info(
                          f"Skipping low confidence group: {group['primary_name']} "
                          f"(confidence: {group['confidence']})"
                      )
                      continue
                  
                  try:
                      # Begin transaction for this group
                      cur.execute("BEGIN;")
                      
                      # Get the primary name and variations
                      primary_name = group['primary_name']
                      variations = group.get('variations', [])
                      
                      # Add primary name to variations for complete search
                      all_names = [primary_name] + variations
                      
                      # Debug log
                      self.logger.debug(f"Searching for names: {all_names}")
                      
                      # Modified query to search for exact matches
                      cur.execute("""
                          SELECT id, ingredient_name, short_description 
                          FROM active_ingredients_extended
                          WHERE ingredient_name = ANY(%s);
                      """, (all_names,))
                      
                      matches = cur.fetchall()
                      self.logger.debug(f"Found matches: {[m[1] for m in matches]}")
                      
                      if len(matches) <= 1:
                          self.logger.info(
                              f"Insufficient matches found for {primary_name}. "
                              f"Expected: {all_names}, Found: {[m[1] for m in matches]}"
                          )
                          cur.execute("ROLLBACK;")
                          continue
                      
                      # Find or identify the primary record
                      primary_match = next(
                          (m for m in matches if m[1].lower() == primary_name.lower()),
                          None
                      )
                      
                      if not primary_match:
                          # Use the first match as primary if primary name not found
                          primary_match = matches[0]
                          self.logger.info(
                              f"Primary name {primary_name} not found, "
                              f"using {primary_match[1]} as primary"
                          )
                      
                      primary_id = primary_match[0]
                      
                      # Update references and mark duplicates
                      for match_id, match_name, match_desc in matches:
                          if match_id != primary_id:
                              # Update drug_ingredients references
                              cur.execute("""
                                  UPDATE drug_ingredients 
                                  SET ingredient_id = %s 
                                  WHERE ingredient_id = %s;
                              """, (primary_id, match_id))
                              
                              # Mark as duplicate
                              cur.execute("""
                                  UPDATE active_ingredients_extended 
                                  SET 
                                      processing_status = 'duplicate',
                                      error_message = %s
                                  WHERE id = %s;
                              """, (
                                  f"Merged into {primary_match[1]} "
                                  f"(confidence: {group['confidence']}, "
                                  f"reason: {group['reason']})",
                                  match_id
                              ))
                              
                              self.logger.info(
                                  f"Merged '{match_name}' into '{primary_match[1]}'"
                              )
                              updates += 1
                      
                      cur.execute("COMMIT;")
                      
                  except Exception as e:
                      cur.execute("ROLLBACK;")
                      self.logger.error(f"Error processing group {group['primary_name']}: {e}")
                      errors += 1
                      continue
          
          return updates, errors
          
      except Exception as e:
          self.logger.error(f"Error applying duplicate groups: {e}")
          return 0, 1
     
def main():
    from .. import config
    
    cleaner = AdvancedDuplicateCleaner()
    cleaner.logger.info("Starting advanced duplicate cleanup")
    
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
        
        # Analyze duplicates
        groups = cleaner.analyze_duplicates(conn)
        total_groups = len(groups)
        cleaner.logger.info(f"Found {total_groups} potential duplicate groups")
        
        if groups:
            # Print all identified groups for review
            print(f"\nFound {total_groups} potential duplicate groups:")
            for idx, group in enumerate(groups, 1):
                print(f"\nGroup {idx}/{total_groups}:")
                print(f"Primary: {group['primary_name']}")
                print(f"Variations to merge: {', '.join(group['variations'])}")
                print(f"Confidence: {group['confidence']}")
                print(f"Reason: {group['reason']}")
                
                # For each group, show current database state
                with conn.cursor() as cur:
                    variations = group['variations']
                    placeholders = ','.join(['%s'] * len(variations))
                    cur.execute(f"""
                        SELECT id, ingredient_name, short_description 
                        FROM active_ingredients_extended
                        WHERE LOWER(ingredient_name) IN 
                        (SELECT LOWER(unnest(ARRAY[{placeholders}])));
                    """, variations)
                    
                    print("\nCurrent database entries:")
                    for row in cur.fetchall():
                        print(f"  ID: {row[0]}, Name: {row[1]}")
                        if row[2]:  # If there's a description
                            print(f"  Description: {row[2]}")
                    print("-" * 80)
            
            # Write detailed report to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = f'duplicate_groups_{timestamp}.txt'
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(f"Duplicate Groups Report - {datetime.now()}\n\n")
                for idx, group in enumerate(groups, 1):
                    f.write(f"\nGroup {idx}/{total_groups}:\n")
                    f.write(f"Primary: {group['primary_name']}\n")
                    f.write(f"Variations: {', '.join(group['variations'])}\n")
                    f.write(f"Confidence: {group['confidence']}\n")
                    f.write(f"Reason: {group['reason']}\n")
                    f.write("-" * 80 + "\n")
            
            print(f"\nDetailed report written to: {report_file}")
            
            # Ask for confirmation before applying changes
            if input("\nProceed with merging duplicates? (yes/no): ").lower() == 'yes':
                updates, errors = cleaner.apply_duplicate_groups(conn, groups)
                
                # Show results
                print(f"\nCleanup Results:")
                print(f"- Updates applied: {updates}")
                print(f"- Errors encountered: {errors}")
                
                # Show actual database changes
                print("\nVerifying changes in database...")
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM active_ingredients_extended 
                        WHERE processing_status = 'duplicate';
                    """)
                    duplicate_count = cur.fetchone()[0]
                    
                    print(f"Total ingredients marked as duplicates: {duplicate_count}")
                    
                    # Show some examples of merged ingredients
                    cur.execute("""
                        SELECT ingredient_name, error_message 
                        FROM active_ingredients_extended 
                        WHERE processing_status = 'duplicate'
                        LIMIT 5;
                    """)
                    
                    print("\nExample of merged ingredients:")
                    for row in cur.fetchall():
                        print(f"\nOriginal: {row[0]}")
                        print(f"Merge info: {row[1]}")
                
                cleaner.logger.info(f"""
                Cleanup Results:
                - Updates applied: {updates}
                - Errors encountered: {errors}
                - Total duplicates marked: {duplicate_count}
                """)
            else:
                print("Operation cancelled")
        else:
            print("No duplicate groups identified")
        
    except Exception as e:
        cleaner.logger.error(f"Fatal error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            cleaner.logger.info("Database connection closed")

if __name__ == "__main__":
    main()