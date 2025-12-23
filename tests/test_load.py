import pytest
from sqlalchemy import create_engine, text, exc
from scripts.load import PostgreSQLLoader, MongoDBLoader
#from config import Config
from scripts.config import Config

@pytest.fixture
def pg_loader():
    """Fixture for PostgreSQLLoader"""
    return PostgreSQLLoader()


@pytest.fixture
def mongo_loader():
    """Fixture for MongoDBLoader"""
    return MongoDBLoader()


def test_postgres_connection():
    """Test PostgreSQL connection and ensure the required database exists."""
    
    # 1. Configuration Check (Based on traceback info)
    # These values are used to connect to the default 'postgres' database.
    user = 'airflow'
    password = 'airflow'
    host = 'localhost'
    port = 5432
    target_db = 'meetingbank'
    
    # Create a connection string for the default, known 'postgres' database
    maintenance_conn_str = f"postgresql://{user}:{password}@{host}:{port}/postgres"
    
    # 2. Database Creation Logic
    maintenance_engine = create_engine(maintenance_conn_str)
    try:
        with maintenance_engine.connect() as conn:
            # PostgreSQL requires autocommit for CREATE DATABASE
            # Use conn.execution_options() to temporarily set isolation level
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                text(f"CREATE DATABASE {target_db}")
            )
            # The database is now guaranteed to exist
            print(f"Database '{target_db}' ensured to exist.")
            
    except exc.OperationalError as e:
        # Check for the specific error code indicating the database already exists (42P04)
        # We must check the exception string or code as SQLAlchemy wraps the original psycopg2 exception.
        if 'database "meetingbank" already exists' in str(e) or '42P04' in str(e):
            pass  # Ignore this error, as it means our goal is met.
        else:
            raise # Reraise other critical OperationalErrors
    except Exception as e:
        raise
    finally:
        maintenance_engine.dispose()
    """Test PostgreSQL connection"""
    loader = PostgreSQLLoader()
    try:
        loader.connect()
        assert loader.engine is not None
        
        # Test simple query
        with loader.engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
    finally:
        if loader.engine:
            loader.engine.dispose()


def test_mongodb_connection():
    """Test MongoDB connection"""
    loader = MongoDBLoader()
    try:
        loader.connect()
        assert loader.client is not None
        assert loader.db is not None
        
        # Test ping
        loader.client.admin.command('ping')
    finally:
        if loader.client:
            loader.client.close()


def test_postgres_connection_string():
    """Test connection string generation"""
    conn_str = Config.get_postgres_connection_string()
    assert 'postgresql://' in conn_str
    assert 'meetingbank' in conn_str


def test_mongodb_connection_string():
    """Test MongoDB connection string"""
    conn_str = Config.get_mongodb_connection_string()
    assert 'mongodb://' in conn_str


if __name__ == "__main__":
    pytest.main([__file__, '-v'])