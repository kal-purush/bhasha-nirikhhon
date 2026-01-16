"""
Database management for ApocaCache library maintainer.
Handles caching of meta4 file information in SQLite.
"""

import os
import sqlite3
from datetime import datetime
import structlog
from typing import Dict, Optional, List

log = structlog.get_logger()

class DatabaseManager:
    """Manages SQLite database for meta4 file caching."""
    
    def __init__(self, data_dir: str):
        """Initialize database connection."""
        self.db_path = os.path.join(data_dir, "meta4_cache.db")
        self._init_db()
    
    def _init_db(self):
        """Initialize database schema if not exists."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create meta4 files table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS meta4_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    book_id TEXT UNIQUE,
                    file_name TEXT,
                    file_size INTEGER,
                    md5_hash TEXT,
                    sha1_hash TEXT,
                    sha256_hash TEXT,
                    mirrors TEXT,
                    last_updated TIMESTAMP,
                    meta4_url TEXT
                )
                """)
                
                conn.commit()
                log.info("database.initialized", path=self.db_path)
                
        except Exception as e:
            log.error("database.init_failed", error=str(e))
            raise
    
    async def update_meta4_info(self, book_id: str, meta4_data: Dict):
        """Update or insert meta4 file information."""
        try:
            mirrors = "|".join(meta4_data.get("mirrors", []))
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                INSERT OR REPLACE INTO meta4_files 
                (book_id, file_name, file_size, md5_hash, sha1_hash, sha256_hash, 
                mirrors, last_updated, meta4_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    book_id,
                    meta4_data.get("file_name"),
                    meta4_data.get("file_size", 0),
                    meta4_data.get("md5_hash"),
                    meta4_data.get("sha1_hash"),
                    meta4_data.get("sha256_hash"),
                    mirrors,
                    datetime.now().isoformat(),
                    meta4_data.get("meta4_url")
                ))
                
                conn.commit()
                log.info("database.meta4_updated", 
                        book_id=book_id, 
                        file_name=meta4_data.get("file_name"))
                
        except Exception as e:
            log.error("database.update_failed", 
                     book_id=book_id, 
                     error=str(e))
    
    def get_meta4_info(self, book_id: str) -> Optional[Dict]:
        """Get cached meta4 file information."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                SELECT file_name, file_size, md5_hash, sha1_hash, sha256_hash, 
                       mirrors, last_updated, meta4_url
                FROM meta4_files
                WHERE book_id = ?
                """, (book_id,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        "file_name": row[0],
                        "file_size": row[1],
                        "md5_hash": row[2],
                        "sha1_hash": row[3],
                        "sha256_hash": row[4],
                        "mirrors": row[5].split("|") if row[5] else [],
                        "last_updated": row[6],
                        "meta4_url": row[7]
                    }
                return None
                
        except Exception as e:
            log.error("database.get_failed", 
                     book_id=book_id, 
                     error=str(e))
            return None
    
    def get_all_meta4_info(self) -> List[Dict]:
        """Get all cached meta4 file information."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                SELECT book_id, file_name, file_size, md5_hash, sha1_hash, 
                       sha256_hash, mirrors, last_updated, meta4_url
                FROM meta4_files
                """)
                
                return [{
                    "book_id": row[0],
                    "file_name": row[1],
                    "file_size": row[2],
                    "md5_hash": row[3],
                    "sha1_hash": row[4],
                    "sha256_hash": row[5],
                    "mirrors": row[6].split("|") if row[6] else [],
                    "last_updated": row[7],
                    "meta4_url": row[8]
                } for row in cursor.fetchall()]
                
        except Exception as e:
            log.error("database.get_all_failed", error=str(e))
            return []
    
    def cleanup_old_entries(self, days: int = 30):
        """Remove entries older than specified days."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                DELETE FROM meta4_files
                WHERE last_updated < datetime('now', '-? days')
                """, (days,))
                
                conn.commit()
                log.info("database.cleanup_completed", 
                        deleted_rows=cursor.rowcount)
                
        except Exception as e:
            log.error("database.cleanup_failed", error=str(e)) 