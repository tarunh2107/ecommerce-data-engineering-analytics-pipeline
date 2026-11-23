"""
Database connection and execution manager for Streamlit analytics dashboard.
Provides pooled database engine, health verification, and query execution.
"""

import os
from typing import Any, Dict, Optional, Tuple
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Load environment variables from .env file at project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
dotenv_path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=dotenv_path)

_ENGINE: Optional[Engine] = None


def get_db_credentials() -> Dict[str, Any]:
    """Retrieve database connection parameters from environment with safe defaults."""
    return {
        "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "olist_dw"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def get_engine() -> Engine:
    """Singleton factory for SQLAlchemy connection engine with pre-ping validation."""
    global _ENGINE
    if _ENGINE is None:
        creds = get_db_credentials()
        # Use 127.0.0.1 to avoid Windows localhost IPv6 ambiguity
        host = creds["host"]
        if host == "localhost":
            host = "127.0.0.1"

        db_url = f"postgresql://{creds['user']}:{creds['password']}@{host}:{creds['port']}/{creds['database']}"
        _ENGINE = create_engine(
            db_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    return _ENGINE


def check_database_connection() -> Tuple[bool, Dict[str, Any]]:
    """
    Validates reachability of PostgreSQL warehouse and returns metadata.
    Returns (is_connected, details_dict).
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Query server version
            version_row = conn.execute(text("SELECT version();")).fetchone()
            db_version = version_row[0] if version_row else "Unknown"

            # Query dwh table count
            count_row = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'dwh';
            """)).fetchone()
            table_count = count_row[0] if count_row else 0

            # Query total warehouse records
            row_count = conn.execute(text("""
                SELECT 
                    (SELECT COUNT(*) FROM dwh.dim_date) +
                    (SELECT COUNT(*) FROM dwh.dim_customer) +
                    (SELECT COUNT(*) FROM dwh.dim_product) +
                    (SELECT COUNT(*) FROM dwh.dim_seller) +
                    (SELECT COUNT(*) FROM dwh.fact_order) +
                    (SELECT COUNT(*) FROM dwh.fact_order_item) AS total_rows;
            """)).fetchone()
            total_rows = row_count[0] if row_count else 0

            return True, {
                "version": db_version,
                "table_count": table_count,
                "total_rows": total_rows,
                "status": "Connected (Healthy)",
            }
    except Exception as exc:
        return False, {
            "error": str(exc),
            "status": "Disconnected",
        }


def run_query(sql_query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Executes an analytical SQL query against the warehouse and returns a Pandas DataFrame.
    """
    engine = get_engine()
    with engine.connect() as conn:
        if params:
            return pd.read_sql_query(text(sql_query), conn, params=params)
        return pd.read_sql_query(text(sql_query), conn)
