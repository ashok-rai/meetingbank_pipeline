"""
Database loading module
Handles loading data to PostgreSQL and MongoDB
"""
from scripts.config import Config
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from pymongo import MongoClient, ASCENDING, TEXT
from pymongo.errors import BulkWriteError, DuplicateKeyError

#from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgreSQLLoader:
    """Handles loading data to PostgreSQL"""
    
    def __init__(self):
        self.config = Config()
        self.engine = None
        
    def connect(self):
        """Establish database connection"""
        try:
            conn_str = self.config.get_postgres_connection_string()
            self.engine = create_engine(conn_str)
            logger.info("Connected to PostgreSQL successfully")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
    
    def create_tables(self):
        """Create database tables"""
        logger.info("Creating PostgreSQL tables...")
        
        # FINAL FIX: Use CWD (which is typically /opt/airflow) and navigate to the mounted 'sql' folder
        sql_file = Path.cwd() / 'sql' / 'create_tables.sql'
        print(sql_file)
        logger.info(f"Loading tables schema from: {sql_file}")
        
        if not sql_file.exists():
             error_msg = f"SQL file 'create_tables.sql' not found at expected path: {sql_file}"
             logger.error(error_msg)
             # This should now show the correct /opt/airflow/sql/... path if it fails
             raise FileNotFoundError(error_msg)

        with open(sql_file, 'r') as f:
            sql_commands = f.read()
        
        # Execute SQL commands
        '''with self.engine.connect() as conn:
            # Split by semicolon and execute each command
            for command in sql_commands.split(';'):
                if command.strip():
                    conn.execute(text(command))
                    conn.commit()'''
        # Execute SQL commands using a transaction block
        # The 'begin()' method handles the transaction and commit on exit
        with self.engine.begin() as conn:
            # Split by semicolon and execute each command
            for command in sql_commands.split(';'):
                if command.strip():
                    conn.execute(text(command))
        
        logger.info("Tables created successfully")
            
    def create_indexes(self):
        """Create database indexes"""
        logger.info("Creating database indexes...")
        
        # FINAL FIX: Use CWD (which is typically /opt/airflow) and navigate to the mounted 'sql' folder
        sql_file = Path.cwd() / 'sql' / 'create_indexes.sql'
        
        logger.info(f"Loading indexes schema from: {sql_file}")

        if not sql_file.exists():
             error_msg = f"SQL index file not found at expected path: {sql_file}"
             logger.error(error_msg)
             raise FileNotFoundError(error_msg)

        with open(sql_file, 'r') as f:
            sql_commands = f.read()
        # Execute SQL commands using a transaction block (THE FIX)
        with self.engine.begin() as conn:
            for command in sql_commands.split(';'):
                if command.strip():
                    try:
                        conn.execute(text(command))
                    except Exception as e:
                        # Log warning for indexes, as some might already exist or fail safely
                        logger.warning(f"Index creation warning: {str(e)}")
        
        logger.info("Indexes created successfully")
    
    def load_cities(self, cities_file: Path) -> Dict[str, int]:
        """
        Load cities dimension table
        Returns mapping of city_name to city_id
        """
        logger.info(f"Loading cities from {cities_file}")
        
        cities_df = pd.read_csv(cities_file)
        
        # Load to database
        cities_df[['city_name', 'state']].to_sql(
            'cities',
            self.engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        # Get city_id mapping
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT city_id, city_name FROM cities"))
            city_mapping = {row[1]: row[0] for row in result}
        
        logger.info(f"Loaded {len(cities_df)} cities")
        return city_mapping
    
    def load_meetings(self, meetings_file: Path, city_mapping: Dict[str, int]):
        """Load meetings fact table"""
        logger.info(f"Loading meetings from {meetings_file}")
        
        meetings_df = pd.read_csv(meetings_file)
        
        # Map city names to city_id
        meetings_df['city_id'] = meetings_df['city_name'].map(city_mapping)
        
        # Select columns for meetings table
        columns = [
            'meeting_id', 'city_id', 'meeting_date', 'title',
            'duration_min', 'speaker_count', 
            'transcript_word_count', 'summary_word_count'
        ]
        
        # Load to database
        meetings_df[columns].to_sql(
            'meetings',
            self.engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logger.info(f"Loaded {len(meetings_df)} meetings")
    
    def load_agendas(self, unstructured_file: Path):
        """Load agendas from unstructured data"""
        logger.info(f"Loading agendas from {unstructured_file}")
        
        with open(unstructured_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Processing {len(data)} documents for agenda extraction")
        
        agenda_records = []
        meetings_with_agendas = 0
        meetings_without_agendas = 0
        
        for doc in data:
            meeting_id = doc.get('meeting_id', '')
            agendas = doc.get('agenda', [])
            
            if not agendas or len(agendas) == 0:
                meetings_without_agendas += 1
                logger.debug(f"  Meeting {meeting_id}: No agenda items")
                continue
            
            meetings_with_agendas += 1
            logger.info(f"  Meeting {meeting_id}: {len(agendas)} agenda items")
            
            for idx, topic in enumerate(agendas, 1):
                # Handle different agenda formats
                topic_text = None
                
                if isinstance(topic, dict):
                    # If agenda is a dict, try different keys
                    topic_text = topic.get('topic') or topic.get('title') or topic.get('name') or str(topic)
                elif isinstance(topic, str):
                    # If agenda is already a string
                    topic_text = topic
                else:
                    # Unknown format, convert to string
                    topic_text = str(topic)
                
                topic_text = topic_text.strip()
                
                if topic_text and len(topic_text) > 0:
                    agenda_records.append({
                        'meeting_id': meeting_id,
                        'item_number': idx,
                        'topic': topic_text[:500],  # Limit to 500 chars
                        'description': None
                    })
        
        
        if agenda_records:
            # Show sample for debugging
            logger.info(f"Sample agenda records:")
            for i, record in enumerate(agenda_records[:3]):
                logger.info(f"  {i+1}. Meeting: {record['meeting_id']}, Topic: {record['topic'][:50]}...")
            
            agendas_df = pd.DataFrame(agenda_records)
            
            # Insert to database
            try:
                agendas_df.to_sql(
                    'agendas',
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=100
                )
                logger.info(f"✅ Successfully loaded {len(agenda_records)} agenda items to database")
            except Exception as e:
                logger.error(f"❌ Failed to load agendas: {str(e)}")
                raise
        else:
            logger.warning("⚠️ No agenda items to load - all meetings have empty agendas")
        
    def load_pipeline(self, structured_file: Path, cities_file: Path, 
                     unstructured_file: Path) -> Dict:
        """Complete PostgreSQL loading pipeline"""
        logger.info("="*60)
        logger.info("Starting PostgreSQL Loading Pipeline")
        logger.info("="*60)
        
        try:
            # Connect
            self.connect()
            
            # Create tables
            self.create_tables()
            
            # Load data
            city_mapping = self.load_cities(cities_file)
            self.load_meetings(structured_file, city_mapping)
            self.load_agendas(unstructured_file)
            
            # Create indexes
            self.create_indexes()
            
            # Get counts
            with self.engine.connect() as conn:
                cities_count = conn.execute(text("SELECT COUNT(*) FROM cities")).scalar()
                meetings_count = conn.execute(text("SELECT COUNT(*) FROM meetings")).scalar()
                agendas_count = conn.execute(text("SELECT COUNT(*) FROM agendas")).scalar()
            
            logger.info("="*60)
            logger.info("PostgreSQL Loading Completed")
            logger.info(f"Cities: {cities_count}")
            logger.info(f"Meetings: {meetings_count}")
            logger.info(f"Agendas: {agendas_count}")
            logger.info("="*60)
            
            return {
                'success': True,
                'cities_count': cities_count,
                'meetings_count': meetings_count,
                'agendas_count': agendas_count
            }
            
        except Exception as e:
            logger.error(f"PostgreSQL loading failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            if self.engine:
                self.engine.dispose()


class MongoDBLoader:
    """Handles loading data to MongoDB"""
    
    def __init__(self):
        self.config = Config()
        self.client = None
        self.db = None
        
    def connect(self):
        """Establish MongoDB connection"""
        try:
            conn_str = self.config.get_mongodb_connection_string()
            self.client = MongoClient(conn_str)
            self.db = self.client[self.config.MONGODB_CONFIG['database']]
            logger.info("Connected to MongoDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise
    
    

    # ... (existing create_indexes, load_transcripts, load_summaries methods)

    def load_pipeline(self, unstructured_file: Path) -> Dict:
        """Complete MongoDB loading pipeline"""
        logger.info("="*60)
        logger.info("Starting MongoDB Loading Pipeline")
        logger.info("="*60)
        
        try:
            # Connect
            self.connect()
            
            # FIX: Clear collections to avoid Duplicate Key Errors (Unique Index Violation)
            self.clear_collections()
            
            # Load data
            transcripts_count = self.load_transcripts(unstructured_file)
            summaries_count = self.load_summaries(unstructured_file)
            
            # Create indexes
            self.create_indexes()
            
            logger.info("="*60)
            logger.info("MongoDB Loading Completed")
            logger.info(f"Transcripts: {transcripts_count}")
            logger.info(f"Summaries: {summaries_count}")
            logger.info("="*60)
            
            return {
                'success': True,
                'transcripts_count': transcripts_count,
                'summaries_count': summaries_count
            }
            
        except Exception as e:
            logger.error(f"MongoDB loading failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            if self.client:
                self.client.close()


    def create_indexes(self):
        """Create MongoDB indexes"""
        logger.info("Creating MongoDB indexes...")
        
        # Transcripts collection indexes
        self.db.transcripts.create_index([("meeting_id", ASCENDING)], unique=True)
        self.db.transcripts.create_index([("city_name", ASCENDING), ("meeting_date", ASCENDING)])
        self.db.transcripts.create_index([("transcript.full_text", TEXT)])
        
        # Summaries collection indexes
        self.db.summaries.create_index([("meeting_id", ASCENDING)], unique=True)
        self.db.summaries.create_index([("city_name", ASCENDING), ("meeting_date", ASCENDING)])
        self.db.summaries.create_index([("summary.full", TEXT)])
        
        logger.info("MongoDB indexes created successfully")
    
    def clear_collections(self):
        """Drop existing collections for a clean run"""
        logger.info("Clearing existing MongoDB collections...")
        self.db.transcripts.drop()
        self.db.summaries.drop()
        logger.info("Collections cleared.")

    def load_transcripts(self, unstructured_file: Path) -> int:
        """Load transcripts to MongoDB"""
        logger.info(f"Loading transcripts from {unstructured_file}")
        
        with open(unstructured_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        transcripts = []
        for doc in data:
            transcript_doc = {
                'meeting_id': doc['meeting_id'],
                'city_name': doc['city_name'],
                'meeting_date': doc['meeting_date'],
                'transcript': doc['transcript'],
                'metadata': {
                    'word_count': doc['transcript'].get('word_count', 0)
                },
                'indexed_at': datetime.now()
            }
            transcripts.append(transcript_doc)
        
        # Bulk insert with error handling
        try:
            result = self.db.transcripts.insert_many(transcripts, ordered=False)
            inserted = len(result.inserted_ids)
            logger.info(f"Inserted {inserted} transcript documents")
            return inserted
        except BulkWriteError as e:
            # Some documents might have been inserted
            inserted = e.details.get('nInserted', 0)
            logger.warning(f"Bulk insert partial success: {inserted} inserted, "
                         f"{len(e.details.get('writeErrors', []))} errors")
            return inserted
    
    def load_summaries(self, unstructured_file: Path) -> int:
        """Load summaries to MongoDB"""
        logger.info(f"Loading summaries from {unstructured_file}")
        
        with open(unstructured_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        summaries = []
        for doc in data:
            summary_doc = {
                'meeting_id': doc['meeting_id'],
                'city_name': doc['city_name'],
                'meeting_date': doc['meeting_date'],
                'summary': doc['summary'],
                'agenda': doc.get('agenda', []),
                'metadata': {
                    'word_count': doc['summary'].get('word_count', 0)
                },
                'indexed_at': datetime.now()
            }
            summaries.append(summary_doc)
        
        try:
            result = self.db.summaries.insert_many(summaries, ordered=False)
            inserted = len(result.inserted_ids)
            logger.info(f"Inserted {inserted} summary documents")
            return inserted
        except BulkWriteError as e:
            inserted = e.details.get('nInserted', 0)
            logger.warning(f"Bulk insert partial success: {inserted} inserted")
            return inserted
    
    