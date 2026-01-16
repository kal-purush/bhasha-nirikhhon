cat > postgres-direct-backup.py << 'EOL'
#!/usr/bin/env python3

import os
import sys
import logging
import subprocess
from datetime import datetime, timedelta
import hashlib
import argparse
import json
import re

class PostgresBackupManager:
    def __init__(self, config_path=None):
        """
        Initialize backup manager with configuration
        
        Config can be provided via file or environment variables
        """
        # Load configuration from environment variables or config file
        self.config = self._load_config(config_path)
        
        # Setup logging
        self._setup_logging()
        
        # Ensure backup directory exists
        os.makedirs(self.config['backup']['base_dir'], exist_ok=True)
        
        self.logger.info("PostgresBackupManager initialized")
        self.logger.debug(f"Using database: {self.config['database']['name']} at {self.config['database']['host']}:{self.config['database']['port']}")

    def _load_config(self, config_path):
        """Load configuration from file or environment variables"""
        config = {
            "database": {
                "connection_string": os.environ.get("DB_SOURCE", "postgres://root:secret@localhost:5433/defi?sslmode=disable"),
                "driver": os.environ.get("DB_DRIVER", "postgres")
            },
            "backup": {
                "base_dir": os.environ.get("BACKUP_DIR", "/tmp/defifundr-backups"),
                "retention_days": int(os.environ.get("RETENTION_DAYS", "7")),
                "compression": True
            },
            "logging": {
                "log_file": os.environ.get("LOG_FILE", "/tmp/defifundr_backup.log"),
                "level": os.environ.get("LOG_LEVEL", "INFO")
            }
        }
        
        # Parse connection string
        if "connection_string" in config["database"]:
            conn_str = config["database"]["connection_string"]
            pattern = r"postgres://([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)"
            match = re.match(pattern, conn_str)
            
            if match:
                user, password, host, port, dbname = match.groups()
                config["database"]["user"] = user
                config["database"]["password"] = password
                config["database"]["host"] = host
                config["database"]["port"] = port
                config["database"]["name"] = dbname
                print(f"Parsed connection string for database {dbname} at {host}:{port}")
            else:
                print("Warning: Could not parse connection string. Format should be: postgres://user:password@host:port/dbname")
        
        # Try to load from config file if specified
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as config_file:
                    file_config = json.load(config_file)
                    # Deep merge configs
                    for section in file_config:
                        if section in config:
                            config[section].update(file_config[section])
                        else:
                            config[section] = file_config[section]
            except Exception as e:
                print(f"Error loading configuration from {config_path}: {e}")
        
        return config

    def _setup_logging(self):
        """Configure logging for backup process"""
        log_file = self.config['logging']['log_file']
        log_level_name = self.config['logging'].get('level', 'INFO')
        log_level = getattr(logging, log_level_name.upper(), logging.INFO)
        
        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger('PostgresBackupManager')

    def perform_backup(self, backup_type='daily'):
        """
        Perform database backup
        
        :param backup_type: Type of backup (daily, weekly, migration, test)
        :return: Backup file path or None if backup fails
        """
        try:
            # Generate unique backup filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{backup_type}_backup_{timestamp}.sql"
            
            # Full path for backup
            backup_path = os.path.join(
                self.config['backup']['base_dir'], 
                backup_filename
            )
            
            # Get database details
            db_name = self.config['database']['name']
            db_host = self.config['database']['host']
            db_port = self.config['database']['port']
            db_user = self.config['database']['user']
            db_password = self.config['database']['password']
            
            # Log backup start
            self.logger.info(f"Starting {backup_type} backup for database {db_name}")
            
            # Build pg_dump command
            pg_dump_cmd = [
                'pg_dump',
                '-h', db_host,
                '-p', db_port,
                '-U', db_user,
                '-d', db_name,
                '-f', backup_path
            ]
            
            # Execute backup command 
            env = os.environ.copy()
            env['PGPASSWORD'] = db_password
            
            self.logger.debug(f"Executing: {' '.join(pg_dump_cmd)}")
            result = subprocess.run(
                pg_dump_cmd,
                env=env,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Check backup success
            if result.returncode != 0:
                self.logger.error(f"Backup failed: {result.stderr}")
                return None
            
            # Optional compression
            compressed_path = backup_path
            if self.config['backup'].get('compression', True):
                compressed_path = f"{backup_path}.gz"
                compress_result = subprocess.run(
                    ['gzip', '-f', backup_path],
                    capture_output=True,
                    text=True
                )
                if compress_result.returncode != 0:
                    self.logger.warning(f"Compression failed: {compress_result.stderr}")
                    compressed_path = backup_path
            
            # Generate checksum
            checksum = self._generate_checksum(compressed_path)
            
            # Log successful backup
            file_size = os.path.getsize(compressed_path) / (1024 * 1024)  # Size in MB
            self.logger.info(f"Successful {backup_type} backup: {compressed_path} ({file_size:.2f} MB)")
            
            # Rotate backups
            self._rotate_backups()
            
            return compressed_path
        
        except Exception as e:
            self.logger.error(f"Backup error: {e}")
            return None

    def _generate_checksum(self, filepath):
        """
        Generate SHA-256 checksum for backup file
        
        :param filepath: Path to backup file
        :return: Checksum string
        """
        try:
            with open(filepath, 'rb') as f:
                checksum = hashlib.sha256()
                for chunk in iter(lambda: f.read(4096), b""):
                    checksum.update(chunk)
            
            # Store checksum in a separate file
            checksum_file = f"{filepath}.sha256"
            with open(checksum_file, 'w') as f:
                f.write(checksum.hexdigest())
            
            self.logger.info(f"Checksum generated: {checksum.hexdigest()}")
            return checksum.hexdigest()
        except Exception as e:
            self.logger.error(f"Checksum generation failed: {e}")
            return None

    def _rotate_backups(self):
        """
        Remove old backups based on retention policy
        """
        try:
            retention_days = self.config['backup'].get('retention_days', 7)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            backup_dir = self.config['backup']['base_dir']
            
            self.logger.info(f"Rotating backups older than {retention_days} days")
            
            for filename in os.listdir(backup_dir):
                filepath = os.path.join(backup_dir, filename)
                
                # Skip if not a file
                if not os.path.isfile(filepath):
                    continue
                
                # Get file modification time
                file_modified = datetime.fromtimestamp(os.path.getmtime(filepath))
                
                # Remove backups older than retention period
                if file_modified < cutoff_date:
                    try:
                        os.remove(filepath)
                        self.logger.info(f"Removed old backup: {filename}")
                    except Exception as e:
                        self.logger.error(f"Error removing {filename}: {e}")
        except Exception as e:
            self.logger.error(f"Backup rotation error: {e}")

    def restore_backup(self, backup_file):
        """
        Restore database from a backup file
        
        :param backup_file: Path to backup file to restore
        :return: Boolean indicating success
        """
        try:
            # Check if backup file exists
            if not os.path.exists(backup_file):
                self.logger.error(f"Backup file not found: {backup_file}")
                return False
            
            # Get database details
            db_name = self.config['database']['name']
            db_host = self.config['database']['host']
            db_port = self.config['database']['port']
            db_user = self.config['database']['user']
            db_password = self.config['database']['password']
            
            # Work with compressed or uncompressed file
            temp_file = backup_file
            if backup_file.endswith('.gz'):
                # Create a temporary uncompressed file
                temp_file = backup_file[:-3]
                decompress_command = ['gunzip', '-c', backup_file]
                with open(temp_file, 'wb') as f:
                    decompress_result = subprocess.run(
                        decompress_command, 
                        stdout=f,
                        stderr=subprocess.PIPE
                    )
                if decompress_result.returncode != 0:
                    self.logger.error(f"Decompression failed: {decompress_result.stderr}")
                    return False
            
            self.logger.info(f"Starting restoration of {db_name} from {backup_file}")
            
            # For a clean restore, drop and recreate the database
            # Connect to postgres database to drop/create target database
            drop_db_cmd = [
                'psql',
                '-h', db_host,
                '-p', db_port,
                '-U', db_user,
                '-d', 'postgres',  # Connect to default postgres database
                '-c', f"DROP DATABASE IF EXISTS {db_name};"
            ]
            
            create_db_cmd = [
                'psql',
                '-h', db_host,
                '-p', db_port,
                '-U', db_user,
                '-d', 'postgres',  # Connect to default postgres database
                '-c', f"CREATE DATABASE {db_name};"
            ]
            
            # Environment with password
            env = os.environ.copy()
            env['PGPASSWORD'] = db_password
            
            self.logger.debug("Dropping database if exists")
            drop_result = subprocess.run(drop_db_cmd, capture_output=True, text=True, env=env)
            if drop_result.returncode != 0:
                self.logger.error(f"Failed to drop database: {drop_result.stderr}")
                return False
                
            self.logger.debug("Creating database")
            create_result = subprocess.run(create_db_cmd, capture_output=True, text=True, env=env)
            if create_result.returncode != 0:
                self.logger.error(f"Failed to create database: {create_result.stderr}")
                return False
            
            # Restore from the backup file
            restore_cmd = [
                'psql',
                '-h', db_host,
                '-p', db_port,
                '-U', db_user,
                '-d', db_name,
                '-f', temp_file
            ]
            
            self.logger.debug(f"Restoring from {temp_file}")
            restore_result = subprocess.run(
                restore_cmd,
                capture_output=True,
                text=True,
                env=env
            )
            
            # Clean up temp file if we decompressed
            if backup_file.endswith('.gz') and os.path.exists(temp_file):
                os.remove(temp_file)
                self.logger.debug(f"Removed temporary file {temp_file}")
            
            # Check restore success
            if restore_result.returncode != 0:
                self.logger.error(f"Restore failed: {restore_result.stderr}")
                return False
            
            self.logger.info(f"Successfully restored database {db_name} from {backup_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Restore error: {e}")
            return False

    def list_backups(self):
        """
        List all available backups
        
        :return: List of backup files with details
        """
        backup_dir = self.config['backup']['base_dir']
        backups = []
        
        try:
            for filename in os.listdir(backup_dir):
                filepath = os.path.join(backup_dir, filename)
                
                # Skip if not a backup file
                if not os.path.isfile(filepath) or not (
                    filename.endswith('.sql') or filename.endswith('.sql.gz')
                ):
                    continue
                
                # Skip checksum files
                if filename.endswith('.sha256'):
                    continue
                
                # Get file details
                file_size = os.path.getsize(filepath)
                file_date = datetime.fromtimestamp(os.path.getmtime(filepath))
                
                # Extract backup type from filename
                backup_type = 'unknown'
                if '_backup_' in filename:
                    backup_type = filename.split('_backup_')[0]
                
                backups.append({
                    'filename': filename,
                    'path': filepath,
                    'size': file_size,
                    'date': file_date,
                    'type': backup_type
                })
            
            # Sort by date (newest first)
            backups.sort(key=lambda x: x['date'], reverse=True)
            return backups
            
        except Exception as e:
            self.logger.error(f"Error listing backups: {e}")
            return []

    def test_backup_and_restore(self):
        """
        Test backup and restore functionality using a test database
        
        :return: Boolean indicating test success
        """
        # Save original DB name
        original_db_name = self.config['database']['name']
        
        try:
            # Use a test database name
            test_db_name = f"{original_db_name}_test"
            self.config['database']['name'] = test_db_name
            
            # Get database details
            db_host = self.config['database']['host']
            db_port = self.config['database']['port']
            db_user = self.config['database']['user']
            db_password = self.config['database']['password']
            
            # Environment with password
            env = os.environ.copy()
            env['PGPASSWORD'] = db_password
            
            # Drop test database if it exists
            drop_cmd = [
                'psql',
                '-h', db_host,
                '-p', db_port,
                '-U', db_user,
                '-d', 'postgres',  # Connect to default postgres database
                '-c', f"DROP DATABASE IF EXISTS {test_db_name};"
            ]
            
            self.logger.info(f"Dropping test database {test_db_name} if it exists")
            drop_result = subprocess.run(drop_cmd, capture_output=True, text=True, env=env)
            if drop_result.returncode != 0:
                self.logger.error(f"Failed to drop test database: {drop_result.stderr}")
                return False
            
            # Create test database
            create_cmd = [
                'psql',
                '-h', db_host,
                '-p', db_port,
                '-U', db_user,
                '-d', 'postgres',  # Connect to default postgres database
                '-c', f"CREATE DATABASE {test_db_name};"
            ]
            
            self.logger.info(f"Creating test database {test_db_name}")
            create_result = subprocess.run(create_cmd, capture_output=True, text=True, env=env)
            if create_result.returncode != 0:
                self.logger.error(f"Failed to create test database: {create_result.stderr}")
                return False
            
            # Create a simple test table and data
            self.logger.info("Creating test data")
            test_data_cmd = [
                'psql',
                '-h', db_host,
                '-p', db_port,
                '-U', db_user,
                '-d', test_db_name,
                '-c', "CREATE TABLE test_table (id SERIAL PRIMARY KEY, data TEXT); INSERT INTO test_table (data) VALUES ('test1'), ('test2');"
            ]
            
            data_result = subprocess.run(test_data_cmd, capture_output=True, text=True, env=env)
            if data_result.returncode != 0:
                self.logger.error(f"Failed to create test data: {data_result.stderr}")
                return False
            
            # Perform backup
            self.logger.info("Starting test backup")
            backup_file = self.perform_backup(backup_type='test')
            if not backup_file:
                self.logger.error("Backup test failed: Could not create backup")
                return False
            
            # Drop test database
            self.logger.info("Dropping test database for restore test")
            drop_result = subprocess.run(drop_cmd, capture_output=True, text=True, env=env)
            if drop_result.returncode != 0:
                self.logger.error(f"Failed to drop test database for restore test: {drop_result.stderr}")
                return False
            
            # Perform restore
            self.logger.info("Starting test restore")
            restore_result = self.restore_backup(backup_file)
            
            # Verify test data
            if restore_result:
                self.logger.info("Verifying restored data")
                verify_cmd = [
                    'psql',
                    '-h', db_host,
                    '-p', db_port,
                    '-U', db_user,
                    '-d', test_db_name,
                    '-c', "SELECT COUNT(*) FROM test_table;"
                ]
                
                verify_result = subprocess.run(verify_cmd, capture_output=True, text=True, env=env)
                
                if "2" in verify_result.stdout:
                    self.logger.info("Test backup and restore successful - verified test data")
                    success = True
                else:
                    self.logger.error("Test restore failed - data verification failed")
                    success = False
            else:
                success = False
            
            # Clean up test database
            self.logger.info("Cleaning up test database")
            cleanup_result = subprocess.run(drop_cmd, capture_output=True, text=True, env=env)
            if cleanup_result.returncode != 0:
                self.logger.warning(f"Failed to clean up test database: {cleanup_result.stderr}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Backup and restore test failed: {e}")
            return False
        finally:
            # Restore original database name
            self.config['database']['name'] = original_db_name

def main():
    """
    Main entry point for backup script
    """
    parser = argparse.ArgumentParser(description='DefiFundr PostgreSQL Backup Manager')
    
    # Main commands
    parser.add_argument('command', choices=['backup', 'restore', 'list', 'test'],
                        help='Command to execute')
    
    # Backup options
    parser.add_argument('--type', choices=['daily', 'weekly', 'monthly', 'migration', 'test'],
                        default='daily', help='Type of backup (for backup command)')
    
    # Restore options
    parser.add_argument('--file', help='Backup file to restore (for restore command)')
    
    # Config option
    parser.add_argument('--config', default=None,
                        help='Path to configuration file')
    
    # Verbosity
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set environment variable for logging level
    if args.verbose:
        os.environ['LOG_LEVEL'] = 'DEBUG'
    
    # Create backup manager
    backup_manager = PostgresBackupManager(args.config)
    
    # Execute command
    if args.command == 'backup':
        backup_file = backup_manager.perform_backup(backup_type=args.type)
        if not backup_file:
            sys.exit(1)
        print(f"Backup created: {backup_file}")
    
    elif args.command == 'restore':
        if not args.file:
            backups = backup_manager.list_backups()
            if not backups:
                print("No backups available")
                sys.exit(1)
                
            print("Available backups:")
            for i, backup in enumerate(backups, 1):
                size_mb = backup['size'] / (1024 * 1024)
                date_str = backup['date'].strftime("%Y-%m-%d %H:%M:%S")
                print(f"{i}. {backup['filename']} ({size_mb:.2f} MB) - {date_str} - Type: {backup['type']}")
                
            try:
                choice = int(input("\nEnter backup number to restore (or 0 to cancel): "))
                if choice == 0:
                    print("Restore canceled")
                    sys.exit(0)
                    
                if 1 <= choice <= len(backups):
                    args.file = backups[choice-1]['path']
                else:
                    print("Invalid selection")
                    sys.exit(1)
            except ValueError:
                print("Invalid input")
                sys.exit(1)
        
        success = backup_manager.restore_backup(args.file)
        if not success:
            sys.exit(1)
        print(f"Database restored successfully from {args.file}")
    
    elif args.command == 'list':
        backups = backup_manager.list_backups()
        if not backups:
            print("No backups available")
            sys.exit(0)
            
        print("Available backups:")
        for i, backup in enumerate(backups, 1):
            size_mb = backup['size'] / (1024 * 1024)
            date_str = backup['date'].strftime("%Y-%m-%d %H:%M:%S")
            print(f"{i}. {backup['filename']} ({size_mb:.2f} MB) - {date_str} - Type: {backup['type']}")
    
    elif args.command == 'test':
        print("Starting backup and restore test...")
        success = backup_manager.test_backup_and_restore()
        if not success:
            print("Backup and restore test failed")
            sys.exit(1)
        print("Backup and restore test completed successfully")

if __name__ == "__main__":
    main()