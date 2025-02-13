import subprocess
import os
import logging
from datetime import datetime
import psycopg2
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_name, db_user, db_password, db_host, backup_dir="backups"):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.backup_dir = backup_dir
        self.setup_logging()

    def setup_logging(self):
        """Configure logging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        
        # Main operations log
        self.logger = logging.getLogger('DatabaseManager')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(f'logs/db_operations_{timestamp}.log')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        self.logger.addHandler(console_handler)

    def create_backup(self) -> str:
        """Create a backup of the database using pg_dump."""
        try:
            # Create backup directory if it doesn't exist
            os.makedirs(self.backup_dir, exist_ok=True)
            
            # Generate backup filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{self.db_name}_backup_{timestamp}.sql"
            backup_path = os.path.join(self.backup_dir, backup_filename)
            
            # Set PGPASSWORD environment variable
            backup_env = os.environ.copy()
            backup_env["PGPASSWORD"] = self.db_password
            
            # Construct pg_dump command
            cmd = [
                "pg_dump",
                "-h", self.db_host,
                "-U", self.db_user,
                "-F", "c",  # Custom format (compressed)
                "-b",  # Include large objects
                "-v",  # Verbose
                "-f", backup_path,
                self.db_name
            ]
            
            self.logger.info(f"Starting database backup to {backup_path}")
            
            # Execute pg_dump
            result = subprocess.run(
                cmd,
                env=backup_env,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                self.logger.info("Backup completed successfully")
                self.logger.info(f"Backup saved to: {backup_path}")
                return backup_path
            else:
                self.logger.error(f"Backup failed: {result.stderr}")
                raise Exception(f"Backup failed: {result.stderr}")
                
        except Exception as e:
            self.logger.error(f"Error creating backup: {e}")
            raise

    def verify_backup(self, backup_path: str) -> bool:
        """Verify the backup file exists and is not empty."""
        try:
            if not os.path.exists(backup_path):
                self.logger.error(f"Backup file not found: {backup_path}")
                return False
                
            size = os.path.getsize(backup_path)
            if size == 0:
                self.logger.error(f"Backup file is empty: {backup_path}")
                return False
                
            self.logger.info(f"Backup verified: {backup_path} (Size: {size/1024/1024:.2f} MB)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error verifying backup: {e}")
            return False

    def clean_duplicates(self, conn) -> dict:
        """Clean up records marked as duplicates after backup."""
        stats = {
            'examined': 0,
            'updated': 0,
            'errors': 0
        }
        
        try:
            with conn.cursor() as cur:
                # Get records marked as duplicates
                cur.execute("""
                    SELECT id, ingredient_name, processing_status
                    FROM active_ingredients_extended
                    WHERE processing_status = 'duplicate';
                """)
                
                duplicates = cur.fetchall()
                stats['examined'] = len(duplicates)
                
                self.logger.info(f"Found {len(duplicates)} records marked as duplicate")
                
                for record in duplicates:
                    try:
                        # Begin transaction
                        cur.execute("BEGIN;")
                        
                        # Reset processing status and error message
                        cur.execute("""
                            UPDATE active_ingredients_extended
                            SET 
                                processing_status = 'active',
                                error_message = NULL,
                                last_updated = CURRENT_TIMESTAMP
                            WHERE id = %s
                            RETURNING id;
                        """, (record[0],))
                        
                        if cur.fetchone():
                            stats['updated'] += 1
                            self.logger.info(
                                f"Reset status for ID {record[0]} ({record[1]})"
                            )
                        
                        cur.execute("COMMIT;")
                        
                    except Exception as e:
                        cur.execute("ROLLBACK;")
                        self.logger.error(
                            f"Error processing ID {record[0]}: {e}"
                        )
                        stats['errors'] += 1
                        continue
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Error in clean_duplicates: {e}")
            return stats

def main():
    from .. import config
    
    # Initialize database manager
    manager = DatabaseManager(
        db_name=config.DB_NAME,
        db_user=config.DB_USER,
        db_password=config.DB_PASSWORD,
        db_host=config.DB_HOST
    )
    
    try:
        # Create backup first
        manager.logger.info("Starting database backup...")
        backup_path = manager.create_backup()
        
        # Verify backup
        if not manager.verify_backup(backup_path):
            manager.logger.error("Backup verification failed. Aborting cleanup.")
            return
            
        # Connect to database
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )
        
        # Ask for confirmation before proceeding
        print("\nBackup completed successfully.")
        if input("Proceed with cleaning duplicate statuses? (yes/no): ").lower() == 'yes':
            # Clean duplicates
            manager.logger.info("\nCleaning duplicate statuses...")
            stats = manager.clean_duplicates(conn)
            
            # Log results
            manager.logger.info("\nCleanup Results:")
            manager.logger.info(f"Records examined: {stats['examined']}")
            manager.logger.info(f"Records updated: {stats['updated']}")
            manager.logger.info(f"Errors encountered: {stats['errors']}")
        else:
            manager.logger.info("Cleanup cancelled")
        
    except Exception as e:
        manager.logger.error(f"Fatal error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            manager.logger.info("Database connection closed")

if __name__ == "__main__":
    main()