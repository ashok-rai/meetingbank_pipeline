"""
Configuration management for MeetingBank Pipeline
Handles environment variables and application settings
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for pipeline settings"""
    
    # Project paths
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    CLEANED_DATA_DIR = DATA_DIR / "cleaned"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    RESULTS_DIR = DATA_DIR / "results"
    REPORTS_DIR = DATA_DIR / "reports"
    
    # Database configurations
    POSTGRES_CONFIG = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'user': os.getenv('POSTGRES_USER', 'airflow'),
        'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
        'database': os.getenv('POSTGRES_DB', 'meetingbank')
    }
    
    MONGODB_CONFIG = {
        'host': os.getenv('MONGODB_HOST', 'localhost'),
        'port': int(os.getenv('MONGODB_PORT', 27017)),
        'user': os.getenv('MONGODB_USER', 'admin'),
        'password': os.getenv('MONGODB_PASSWORD', 'admin123'),
        'database': os.getenv('MONGODB_DB', 'meetingbank')
    }
    
    # HuggingFace dataset configuration
    HUGGINGFACE_DATASET = "huuuyeah/MeetingBank"
    HUGGINGFACE_TOKEN = os.getenv('HUGGINGFACE_TOKEN', None)
    
    # Pipeline settings
    SUBSET_SIZE = 50  # Number of meetings to process
    TARGET_CITIES = [
        "Seattle", "Boston", "Denver", 
        "King County", "Long Beach", "Alameda"
    ]
    
    # Retry settings
    API_RETRY_COUNT = 3
    API_RETRY_DELAY = 5  # seconds
    API_TIMEOUT = 30  # seconds
    
    @classmethod
    def create_directories(cls):
        """Create all required directories"""
        for dir_path in [
            cls.RAW_DATA_DIR,
            cls.CLEANED_DATA_DIR,
            cls.PROCESSED_DATA_DIR,
            cls.RESULTS_DIR,
            cls.REPORTS_DIR
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_postgres_connection_string(cls):
        """Get PostgreSQL connection string for SQLAlchemy"""
        cfg = cls.POSTGRES_CONFIG
        return f"postgresql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    
    @classmethod
    def get_mongodb_connection_string(cls):
        """Get MongoDB connection string"""
        cfg = cls.MONGODB_CONFIG
        return f"mongodb://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/"
