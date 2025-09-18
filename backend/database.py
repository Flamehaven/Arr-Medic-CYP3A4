"""
Simple Database Layer for ARR-MEDIC Opensource

Basic SQLite-based storage for prediction history.
Educational implementation - production systems would use more robust databases.
"""

import sqlite3
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import aiosqlite
import os

logger = logging.getLogger(__name__)

# Database optimization pragmas
INIT_PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA temp_store=memory;",
    "PRAGMA mmap_size=268435456;",  # 256MB
    "PRAGMA busy_timeout=5000;"
]

async def _apply_pragmas(db):
    """Apply performance and reliability pragmas"""
    for pragma in INIT_PRAGMAS:
        try:
            await db.execute(pragma)
            logger.debug(f"Applied pragma: {pragma}")
        except Exception as e:
            logger.warning(f"Failed to apply pragma {pragma}: {e}")

class Database:
    """Simple database interface for ARR-MEDIC opensource"""
    
    def __init__(self, db_path: str = "arr_opensource.db"):
        self.db_path = db_path
        self._initialized = False
    
    async def initialize(self):
        """Initialize database tables"""
        if self._initialized:
            return
            
        try:
            async with aiosqlite.connect(self.db_path, timeout=5.0) as db:
                # Apply performance pragmas
                await _apply_pragmas(db)

                await db.execute('''
                    CREATE TABLE IF NOT EXISTS predictions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        compound_id TEXT,
                        smiles TEXT NOT NULL,
                        prediction TEXT NOT NULL,
                        probability REAL NOT NULL,
                        confidence REAL NOT NULL,
                        risk_level TEXT NOT NULL,
                        molecular_descriptors TEXT,
                        warnings TEXT,
                        processing_time REAL,
                        timestamp TEXT NOT NULL,
                        model_version TEXT DEFAULT 'basic-v1.0'
                    )
                ''')
                
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_timestamp 
                    ON predictions(timestamp DESC)
                ''')
                
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_compound_id 
                    ON predictions(compound_id)
                ''')
                
                await db.commit()
                
            self._initialized = True
            logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def store_prediction(self, request, result: Dict[str, Any]):
        """Store prediction result in database"""
        try:
            await self.initialize()
            
            async with aiosqlite.connect(self.db_path, timeout=5.0) as db:
                await db.execute('''
                    INSERT INTO predictions (
                        compound_id, smiles, prediction, probability, confidence,
                        risk_level, molecular_descriptors, warnings,
                        processing_time, timestamp, model_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    getattr(request, 'compound_id', None),
                    request.smiles,
                    result['prediction'],
                    result['probability'],
                    result['confidence'],
                    result['risk_level'],
                    json.dumps(result.get('descriptors', {})),
                    json.dumps(result.get('warnings', [])),
                    result.get('processing_time', 0.0),
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    'basic-v1.0'
                ))
                
                await db.commit()
                
        except Exception as e:
            logger.error(f"Failed to store prediction: {e}")
            # Don't raise - storage failure shouldn't break prediction
    
    async def get_recent_predictions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent prediction history"""
        try:
            await self.initialize()
            
            async with aiosqlite.connect(self.db_path, timeout=5.0) as db:
                db.row_factory = aiosqlite.Row
                
                async with db.execute('''
                    SELECT * FROM predictions 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                ''', (limit,)) as cursor:
                    
                    rows = await cursor.fetchall()
                    
                    results = []
                    for row in rows:
                        result = {
                            'id': row['id'],
                            'compound_id': row['compound_id'],
                            'smiles': row['smiles'],
                            'prediction': row['prediction'],
                            'probability': row['probability'],
                            'confidence': row['confidence'],
                            'risk_level': row['risk_level'],
                            'timestamp': row['timestamp'],
                            'processing_time': row['processing_time'],
                            'model_version': row['model_version']
                        }
                        
                        # Parse JSON fields
                        try:
                            result['molecular_descriptors'] = json.loads(row['molecular_descriptors'] or '{}')
                            result['warnings'] = json.loads(row['warnings'] or '[]')
                        except json.JSONDecodeError:
                            result['molecular_descriptors'] = {}
                            result['warnings'] = []
                        
                        results.append(result)
                    
                    return results
                    
        except Exception as e:
            logger.error(f"Failed to get prediction history: {e}")
            return []
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get basic database statistics"""
        try:
            await self.initialize()
            
            async with aiosqlite.connect(self.db_path, timeout=5.0) as db:
                # Total predictions
                async with db.execute('SELECT COUNT(*) FROM predictions') as cursor:
                    total = (await cursor.fetchone())[0]
                
                # Predictions by type
                async with db.execute('''
                    SELECT prediction, COUNT(*) as count 
                    FROM predictions 
                    GROUP BY prediction
                ''') as cursor:
                    by_type = {row[0]: row[1] for row in await cursor.fetchall()}
                
                # Recent activity (last 24 hours)
                async with db.execute('''
                    SELECT COUNT(*) FROM predictions
                    WHERE datetime(timestamp) >= datetime('now', '-1 day')
                ''') as cursor:
                    recent = (await cursor.fetchone())[0]
                
                return {
                    'total_predictions': total,
                    'predictions_by_type': by_type,
                    'recent_24h': recent
                }
                
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {
                'total_predictions': 0,
                'predictions_by_type': {},
                'recent_24h': 0
            }

# Global database instance
_db_instance: Optional[Database] = None

async def get_database() -> Optional[Database]:
    """Dependency for getting database instance"""
    global _db_instance
    
    try:
        if _db_instance is None:
            db_path = os.getenv('DB_PATH', 'arr_opensource.db')
            _db_instance = Database(db_path)
            await _db_instance.initialize()
        
        return _db_instance
        
    except Exception as e:
        logger.error(f"Failed to get database: {e}")
        return None

async def close_database():
    """Clean shutdown of database"""
    global _db_instance
    if _db_instance:
        # Perform any cleanup if needed
        logger.info("Database connection closed")
        _db_instance = None
