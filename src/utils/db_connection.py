"""
Module: db_connection
Description: PostgreSQL database connection and engine provider using SQLAlchemy and psycopg2.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def get_db_url() -> str:
    """Constructs database URL from environment variables."""
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "olist_dw")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"
