import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Set

class DuplicateCleanup:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging for cleanup operations."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Main operations log
        self.logger = logging.getLogger('DuplicateCleanup')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(f'duplicate_cleanup_{timestamp}.log')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        self.logger.addHandler(console_handler)

    def find_circular_references(self, conn) -> List[Dict]:
        """Find cases where primary records are incorrectly marked as duplicates."""
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        ae.id,
                        ae.ingredient_name,
                        ae.processing_status,
                        ae.error_message,
                        d.primary_id
                    FROM active_ingredients_extended ae
                    JOIN ingredient_duplicates d ON d.primary_id = ae.id
                    WHERE ae.processing_status = 'duplicate';
                """)
                
                return [
                    {
                        'id': row[0],
                        'name': row[1],
                        'status': row[2],
                        'error_message': row[3],
                        'referenced_as_primary': row[4]
                    }
                    for row in cur.fetchall()
                ]
        except Exception as e:
            self.logger.error(f"Error finding circular references: {e}")
            return []

    def fix_circular_references(self, conn) -> int:
        """Fix cases where primary records are incorrectly marked as duplicates."""
        fixed_count = 0
        try:
            circular_refs = self.find_circular_references(conn)
            
            if not circular_refs:
                self.logger.info("No circular references found")
                return 0
                
            self.logger.info(f"Found {len(circular_refs)} circular references to fix")
            
            with conn.cursor() as cur:
                for ref in circular_refs:
                    try:
                        # Begin transaction
                        cur.execute("BEGIN;")
                        
                        # Reset the primary record's status
                        cur.execute("""
                            UPDATE active_ingredients_extended
                            SET 
                                processing_status = 'active',
                                error_message = NULL,
                                last_updated = CURRENT_TIMESTAMP
                            WHERE id = %s
                            RETURNING id;
                        """, (ref['id'],))
                        
                        if cur.fetchone():
                            fixed_count += 1
                            self.logger.info(
                                f"Fixed circular reference for ID {ref['id']} "
                                f"({ref['name']})"
                            )
                        
                        cur.execute("COMMIT;")
                        
                    except Exception as e:
                        cur.execute("ROLLBACK;")
                        self.logger.error(
                            f"Error fixing circular reference for ID {ref['id']}: {e}"
                        )
                        continue
            
            return fixed_count
            
        except Exception as e:
            self.logger.error(f"Error in fix_circular_references: {e}")
            return 0

    def delete_duplicates(self, conn, dry_run: bool = True) -> Dict:
        """Delete duplicate records after ensuring all references are properly handled."""
        stats = {
            'candidates': 0,
            'deleted': 0,
            'skipped': 0,
            'errors': 0
        }
        
        try:
            with conn.cursor() as cur:
                # Get duplicate records
                cur.execute("""
                    SELECT 
                        ae.id,
                        ae.ingredient_name,
                        ae.processing_status,
                        ae.error_message,
                        (
                            SELECT COUNT(*)
                            FROM drug_ingredients di
                            WHERE di.ingredient_id = ae.id
                        ) as reference_count
                    FROM active_ingredients_extended ae
                    WHERE ae.processing_status = 'duplicate'
                    ORDER BY ae.id;
                """)
                
                duplicates = cur.fetchall()
                stats['candidates'] = len(duplicates)
                
                self.logger.info(f"Found {len(duplicates)} duplicate records to process")
                
                if dry_run:
                    self.logger.info("DRY RUN - No changes will be made")
                
                for dup in duplicates:
                    dup_id, name, status, error_msg, ref_count = dup
                    
                    try:
                        # Skip if still referenced
                        if ref_count > 0:
                            self.logger.warning(
                                f"Skipping ID {dup_id} ({name}) - "
                                f"still has {ref_count} references"
                            )
                            stats['skipped'] += 1
                            continue
                        
                        if not dry_run:
                            # Begin transaction
                            cur.execute("BEGIN;")
                            
                            # Delete from ingredient_duplicates first
                            # cur.execute("""
                            #     DELETE FROM ingredient_duplicates
                            #     WHERE duplicate_id = %s;
                            # """, (dup_id,))
                            
                            # Then delete the ingredient
                            cur.execute("""
                                DELETE FROM active_ingredients_extended
                                WHERE id = %s
                                RETURNING id;
                            """, (dup_id,))
                            
                            if cur.fetchone():
                                stats['deleted'] += 1
                                self.logger.info(
                                    f"Deleted duplicate ID {dup_id} ({name})"
                                )
                            
                            cur.execute("COMMIT;")
                        else:
                            self.logger.info(
                                f"Would delete ID {dup_id} ({name})"
                            )
                            stats['deleted'] += 1
                            
                    except Exception as e:
                        if not dry_run:
                            cur.execute("ROLLBACK;")
                        self.logger.error(f"Error processing ID {dup_id}: {e}")
                        stats['errors'] += 1
                        continue
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error in delete_duplicates: {e}")
            return stats

def main():
    from .. import config
    
    cleanup = DuplicateCleanup()
    cleanup.logger.info("Starting duplicate cleanup process")
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        # First, fix any circular references
        fixed_count = cleanup.fix_circular_references(conn)
        cleanup.logger.info(f"Fixed {fixed_count} circular references")
        
        # Then do a dry run of duplicate deletion
        cleanup.logger.info("\nPerforming dry run of duplicate deletion...")
        dry_run_stats = cleanup.delete_duplicates(conn, dry_run=True)
        
        cleanup.logger.info("\nDry Run Results:")
        cleanup.logger.info(f"Total candidates: {dry_run_stats['candidates']}")
        cleanup.logger.info(f"Would delete: {dry_run_stats['deleted']}")
        cleanup.logger.info(f"Would skip: {dry_run_stats['skipped']}")
        cleanup.logger.info(f"Errors: {dry_run_stats['errors']}")
        
        # Ask for confirmation before proceeding
        if input("\nProceed with actual deletion? (yes/no): ").lower() == 'yes':
            cleanup.logger.info("\nProceeding with actual deletion...")
            stats = cleanup.delete_duplicates(conn, dry_run=False)
            
            cleanup.logger.info("\nFinal Results:")
            cleanup.logger.info(f"Total candidates: {stats['candidates']}")
            cleanup.logger.info(f"Actually deleted: {stats['deleted']}")
            cleanup.logger.info(f"Skipped: {stats['skipped']}")
            cleanup.logger.info(f"Errors: {stats['errors']}")
        else:
            cleanup.logger.info("Deletion cancelled")
        
    except Exception as e:
        cleanup.logger.error(f"Fatal error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            cleanup.logger.info("Database connection closed")

if __name__ == "__main__":
    main()