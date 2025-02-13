import re
from typing import List, Dict, Tuple
import psycopg2
import logging
from datetime import datetime

class ReportApplier:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.basicConfig(
            filename=f'report_application_{timestamp}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def parse_report(self, report_file: str) -> List[Dict]:
        groups = []
        current_group = None
        
        with open(report_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        for line in lines:
            line = line.strip()
            
            if line.startswith('Group'):
                if current_group:
                    groups.append(current_group)
                current_group = {}
            elif line.startswith('Primary:'):
                current_group['primary_name'] = line.replace('Primary:', '').strip()
            elif line.startswith('Variations:'):
                variations = line.replace('Variations:', '').strip()
                current_group['variations'] = [v.strip() for v in variations.split(',') if v.strip()]
            elif line.startswith('Confidence:'):
                current_group['confidence'] = float(line.replace('Confidence:', '').strip())
            elif line.startswith('Reason:'):
                current_group['reason'] = line.replace('Reason:', '').strip()
                
        if current_group:
            groups.append(current_group)
            
        return groups

    def apply_groups(self, conn, groups: List[Dict]) -> Tuple[int, int]:
        updates = 0
        errors = 0
        
        try:
            with conn.cursor() as cur:
                for group in groups:
                    if group['confidence'] < 0.9:
                        logging.info(
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
                        
                        if not variations:
                            continue
                        
                        # Add primary name to variations for complete search
                        all_names = [primary_name] + variations
                        
                        # Find matching records
                        cur.execute("""
                            SELECT id, ingredient_name, short_description 
                            FROM active_ingredients_extended
                            WHERE ingredient_name = ANY(%s);
                        """, (all_names,))
                        
                        matches = cur.fetchall()
                        if len(matches) <= 1:
                            logging.info(
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
                            primary_match = matches[0]
                            logging.info(
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
                                
                                logging.info(
                                    f"Merged '{match_name}' into '{primary_match[1]}'"
                                )
                                updates += 1
                        
                        cur.execute("COMMIT;")
                        
                    except Exception as e:
                        cur.execute("ROLLBACK;")
                        logging.error(f"Error processing group {group['primary_name']}: {e}")
                        errors += 1
                        continue
            
            return updates, errors
            
        except Exception as e:
            logging.error(f"Error applying groups: {e}")
            return 0, 1

def main():
    from .. import config  # Adjust import path as needed
    
    applier = ReportApplier()
    
    try:
        # Get report file path from user
        report_file = input("Enter the path to the duplicate groups report file: ")
        
        # Parse the report
        groups = applier.parse_report(report_file)
        logging.info(f"Found {len(groups)} groups in report")
        
        # Show groups and ask for confirmation
        print(f"\nFound {len(groups)} groups to process:")
        for idx, group in enumerate(groups, 1):
            print(f"\nGroup {idx}:")
            print(f"Primary: {group['primary_name']}")
            print(f"Variations: {', '.join(group['variations'])}")
            print(f"Confidence: {group['confidence']}")
        
        if input("\nProceed with applying these groups? (yes/no): ").lower() != 'yes':
            print("Operation cancelled")
            return
        
        # Connect to database
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        # Apply the groups
        updates, errors = applier.apply_groups(conn, groups)
        
        # Show results
        print(f"\nResults:")
        print(f"Updates applied: {updates}")
        print(f"Errors encountered: {errors}")
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()