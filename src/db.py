"""Database factory - picks backend based on DB_BACKEND setting."""
from src import settings
from src.logging_conf import logger


def Database():
    """Factory function that returns the appropriate database backend."""
    backend = settings.DB_BACKEND

    if backend == "postgres":
        from src.db_postgres import PostgresDatabase
        logger.info("Using PostgreSQL direct connection backend")
        return PostgresDatabase()
    elif backend == "rest":
        from src.db_rest import RestDatabase
        logger.info("Using Supabase REST API backend")
        return RestDatabase()
    else:
        raise ValueError(f"Unknown DB_BACKEND: {backend}. Use 'postgres' or 'rest'")
