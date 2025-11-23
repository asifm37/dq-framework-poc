"""
Results Tracker - Stores test results and Iceberg snapshot metadata
"""
import sqlite3
import os
from datetime import datetime
from typing import Optional, Dict
import json

# Detect if running in Docker or Mac
if os.path.exists("/app"):
    DB_PATH = "/app/results/dq_results.db"
else:
    DB_PATH = "/Users/amohiuddeen/Github/dq-framework-poc/results/dq_results.db"


def init_database():
    """Initialize SQLite database with schema"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Main results table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS validation_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp TEXT NOT NULL,
            table_name TEXT NOT NULL,
            test_type TEXT NOT NULL,
            snapshot_id INTEGER,
            row_count INTEGER,
            pass_count INTEGER,
            fail_count INTEGER,
            pass_rate REAL,
            status TEXT NOT NULL,
            details TEXT,
            airflow_run_id TEXT
        )
    """)
    
    # Snapshot tracking table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS snapshot_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            snapshot_id INTEGER NOT NULL,
            last_validated_timestamp TEXT NOT NULL,
            validation_status TEXT NOT NULL,
            UNIQUE(table_name, snapshot_id)
        )
    """)
    
    # Create indexes
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_runs_timestamp 
        ON validation_runs(run_timestamp)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_runs_table 
        ON validation_runs(table_name)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshot_table 
        ON snapshot_tracking(table_name)
    """)
    
    conn.commit()
    conn.close()
    print(f"✅ Database initialized at {DB_PATH}")


def save_validation_result(
    table_name: str,
    test_type: str,
    snapshot_id: Optional[int],
    row_count: int,
    pass_count: int,
    fail_count: int,
    status: str,
    details: str = "",
    airflow_run_id: str = None
):
    """Save validation result to database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    pass_rate = (pass_count / row_count * 100) if row_count > 0 else 100.0
    
    cursor.execute("""
        INSERT INTO validation_runs (
            run_timestamp, table_name, test_type, snapshot_id,
            row_count, pass_count, fail_count, pass_rate,
            status, details, airflow_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now().isoformat(),
        table_name,
        test_type,
        snapshot_id,
        row_count,
        pass_count,
        fail_count,
        pass_rate,
        status,
        details,
        airflow_run_id
    ))
    
    conn.commit()
    conn.close()


def get_last_validated_snapshot(table_name: str) -> Optional[int]:
    """Get the last successfully validated snapshot ID for a table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT snapshot_id 
        FROM snapshot_tracking 
        WHERE table_name = ? AND validation_status = 'success'
        ORDER BY last_validated_timestamp DESC
        LIMIT 1
    """, (table_name,))
    
    result = cursor.fetchone()
    conn.close()
    
    return result[0] if result else None


def update_snapshot_tracking(
    table_name: str,
    snapshot_id: int,
    validation_status: str
):
    """Update snapshot tracking after validation"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT OR REPLACE INTO snapshot_tracking (
            table_name, snapshot_id, last_validated_timestamp, validation_status
        ) VALUES (?, ?, ?, ?)
    """, (
        table_name,
        snapshot_id,
        datetime.now().isoformat(),
        validation_status
    ))
    
    conn.commit()
    conn.close()


def get_recent_results(hours: int = 24) -> list:
    """Get validation results from the last N hours"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM validation_runs
        WHERE datetime(run_timestamp) >= datetime('now', '-' || ? || ' hours')
        ORDER BY run_timestamp DESC
    """, (hours,))
    
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return results


def get_table_summary() -> Dict:
    """Get summary statistics per table"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            table_name,
            test_type,
            COUNT(*) as total_runs,
            AVG(pass_rate) as avg_pass_rate,
            MIN(pass_rate) as min_pass_rate,
            MAX(pass_rate) as max_pass_rate,
            SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as success_count,
            MAX(run_timestamp) as last_run
        FROM validation_runs
        WHERE test_type = 'data_validation'
        GROUP BY table_name, test_type
        ORDER BY table_name
    """)
    
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return results


def get_hourly_trends(table_name: str = None, hours: int = 24) -> list:
    """Get hourly pass rate trends"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = """
        SELECT 
            table_name,
            strftime('%Y-%m-%d %H:00', run_timestamp) as hour_bucket,
            AVG(pass_rate) as avg_pass_rate,
            COUNT(*) as run_count,
            SUM(row_count) as total_rows
        FROM validation_runs
        WHERE test_type = 'data_validation'
        AND datetime(run_timestamp) >= datetime('now', '-' || ? || ' hours')
    """
    
    if table_name:
        query += " AND table_name = ?"
        params = (hours, table_name)
    else:
        params = (hours,)
    
    query += " GROUP BY table_name, hour_bucket ORDER BY hour_bucket DESC"
    
    cursor.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return results


if __name__ == "__main__":
    # Initialize database
    init_database()
    
    # Example usage
    print("\n📊 Database initialized successfully!")
    print(f"Location: {DB_PATH}")

