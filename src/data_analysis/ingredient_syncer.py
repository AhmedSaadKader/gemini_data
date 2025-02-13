import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime
from typing import List, Dict, Tuple
import json

class IngredientSyncer:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        """Set up detailed logging for both operations and changes."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Main operations log
        self.logger = logging.getLogger('IngredientSyncer')
        self.logger.setLevel(logging.INFO)
        
        # File handler for operations
        ops_handler = logging.FileHandler(f'ingredient_sync_{timestamp}.log')
        ops_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(ops_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        self.logger.addHandler(console_handler)
        
        # Separate logger for changes
        self.changes_logger = logging.getLogger('IngredientChanges')
        self.changes_logger.setLevel(logging.INFO)
        changes_handler = logging.FileHandler(f'ingredient_changes_{timestamp}.log')
        changes_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.changes_logger.addHandler(changes_handler)

    def log_change(self, change_type: str, details: Dict):
        """Log a change with detailed before/after state."""
        change_msg = f"\n{'='*80}\n"
        change_msg += f"Change Type: {change_type}\n"
        change_msg += f"Timestamp: {datetime.now().isoformat()}\n"
        
        for key, value in details.items():
            change_msg += f"{key}: {value}\n"
            
        change_msg += f"{'='*80}\n"
        self.changes_logger.info(change_msg)

    def verify_tables(self, conn) -> bool:
        """Verify all required tables exist and have the correct structure."""
        try:
            with conn.cursor() as cur:
                # Check active_ingredients_extended
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'active_ingredients_extended';
                """)
                columns = {row[0]: row[1] for row in cur.fetchall()}
                
                required_columns = {
                    'id': 'integer',
                    'ingredient_name': 'character varying',
                    'processing_status': 'character varying',
                    'last_updated': 'timestamp'
                }
                
                missing_columns = set(required_columns.keys()) - set(columns.keys())
                if missing_columns:
                    self.logger.error(f"Missing columns in active_ingredients_extended: {missing_columns}")
                    return False
                
                # Verify ingredient_duplicates table
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'ingredient_duplicates'
                    );
                """)
                if not cur.fetchone()[0]:
                    self.logger.error("ingredient_duplicates table does not exist")
                    return False
                    
                return True
                
        except Exception as e:
            self.logger.error(f"Error verifying tables: {e}")
            return False

    def sync_changes(self, conn) -> Tuple[int, int]:
        """Synchronize changes from ingredient_duplicates to active_ingredients_extended."""
        updates_applied = 0
        errors = 0
        
        try:
            with conn.cursor() as cur:
                # Begin transaction
                cur.execute("BEGIN;")
                
                # Get pending changes from ingredient_duplicates
                cur.execute("""
                    SELECT 
                        d.duplicate_id,
                        d.primary_id,
                        d.confidence,
                        d.notes,
                        a1.ingredient_name as duplicate_name,
                        a2.ingredient_name as primary_name
                    FROM ingredient_duplicates d
                    JOIN active_ingredients_extended a1 ON d.duplicate_id = a1.id
                    JOIN active_ingredients_extended a2 ON d.primary_id = a2.id
                    WHERE a1.processing_status != 'duplicate';
                """)
                
                changes = cur.fetchall()
                self.logger.info(f"Found {len(changes)} pending changes to process")
                
                for change in changes:
                    duplicate_id, primary_id, confidence, notes, duplicate_name, primary_name = change
                    
                    try:
                        # Update drug_ingredients references
                        cur.execute("""
                            UPDATE drug_ingredients 
                            SET ingredient_id = %s 
                            WHERE ingredient_id = %s;
                        """, (primary_id, duplicate_id))
                        
                        # Mark the duplicate ingredient
                        cur.execute("""
                            UPDATE active_ingredients_extended 
                            SET 
                                processing_status = 'duplicate',
                                error_message = %s,
                                last_updated = CURRENT_TIMESTAMP
                            WHERE id = %s
                            RETURNING id;
                        """, (
                            f"Merged into {primary_name} (ID: {primary_id})",
                            duplicate_id
                        ))
                        
                        if cur.fetchone():
                            updates_applied += 1
                            # Log the change
                            self.log_change('DUPLICATE_MERGE', {
                                'Duplicate ID': duplicate_id,
                                'Duplicate Name': duplicate_name,
                                'Primary ID': primary_id,
                                'Primary Name': primary_name,
                                'Confidence': confidence,
                                'Notes': notes
                            })
                        
                    except Exception as e:
                        self.logger.error(f"Error processing change for ID {duplicate_id}: {e}")
                        errors += 1
                        continue
                
                if updates_applied > 0:
                    cur.execute("COMMIT;")
                    self.logger.info(f"Successfully applied {updates_applied} updates")
                else:
                    cur.execute("ROLLBACK;")
                    self.logger.info("No updates to apply")
                
                return updates_applied, errors
                
        except Exception as e:
            self.logger.error(f"Error in sync_changes: {e}")
            if 'cur' in locals():
                cur.execute("ROLLBACK;")
            return 0, 1

    def verify_sync(self, conn) -> Dict:
        """Verify the synchronization status and return statistics."""
        stats = {
            'total_duplicates': 0,
            'synced_duplicates': 0,
            'unsynced_duplicates': 0,
            'orphaned_references': 0
        }
        
        try:
            with conn.cursor() as cur:
                # Count total duplicates
                cur.execute("SELECT COUNT(*) FROM ingredient_duplicates;")
                stats['total_duplicates'] = cur.fetchone()[0]
                
                # Count synced duplicates
                cur.execute("""
                    SELECT COUNT(*)
                    FROM ingredient_duplicates d
                    JOIN active_ingredients_extended a ON d.duplicate_id = a.id
                    WHERE a.processing_status = 'duplicate';
                """)
                stats['synced_duplicates'] = cur.fetchone()[0]
                
                # Calculate unsynced
                stats['unsynced_duplicates'] = stats['total_duplicates'] - stats['synced_duplicates']
                
                # Check for orphaned references
                cur.execute("""
                    SELECT COUNT(*)
                    FROM drug_ingredients di
                    LEFT JOIN active_ingredients_extended ae ON di.ingredient_id = ae.id
                    WHERE ae.id IS NULL;
                """)
                stats['orphaned_references'] = cur.fetchone()[0]
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Error in verify_sync: {e}")
            return stats

def main():
    from .. import config
    
    syncer = IngredientSyncer()
    syncer.logger.info("Starting ingredient synchronization")
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        # Verify table structure
        if not syncer.verify_tables(conn):
            syncer.logger.error("Table verification failed")
            return
        
        # Perform synchronization
        updates, errors = syncer.sync_changes(conn)
        
        # Verify results
        stats = syncer.verify_sync(conn)
        
        # Log results
        syncer.logger.info("\nSynchronization Results:")
        syncer.logger.info(f"Updates applied: {updates}")
        syncer.logger.info(f"Errors encountered: {errors}")
        syncer.logger.info("\nCurrent Status:")
        syncer.logger.info(f"Total duplicate relations: {stats['total_duplicates']}")
        syncer.logger.info(f"Synced duplicates: {stats['synced_duplicates']}")
        syncer.logger.info(f"Unsynced duplicates: {stats['unsynced_duplicates']}")
        syncer.logger.info(f"Orphaned references: {stats['orphaned_references']}")
        
    except Exception as e:
        syncer.logger.error(f"Fatal error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            syncer.logger.info("Database connection closed")

if __name__ == "__main__":
    main()