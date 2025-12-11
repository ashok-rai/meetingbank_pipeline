# ============================================================================
# HARSHATH VIJAYAKUMAR - Database Loading Module Documentation

## Overview
The database loading module loads transformed data into PostgreSQL (structured) and MongoDB (unstructured) databases.

## Files
- `scripts/load.py` - Database loading functions
- `sql/create_tables.sql` - PostgreSQL schema
- `sql/create_indexes.sql` - Index definitions
- `tests/test_load.py` - Unit tests

## Database Schemas

### PostgreSQL Tables
1. **cities** (dimension)
   - city_id (PK)
   - city_name (unique)
   - state

2. **meetings** (fact)
   - meeting_id (PK)
   - city_id (FK)
   - meeting_date
   - title, duration_min, speaker_count
   - transcript_word_count, summary_word_count

3. **agendas** (detail)
   - agenda_id (PK)
   - meeting_id (FK)
   - item_number, topic, description

### MongoDB Collections
1. **transcripts**
   - meeting_id (unique)
   - city_name, meeting_date
   - transcript.full_text
   - metadata

2. **summaries**
   - meeting_id (unique)
   - summary.full, summary.short
   - agenda items

## Usage

### PostgreSQL Loading
```python
from scripts.load import PostgreSQLLoader

loader = PostgreSQLLoader()
result = loader.load_pipeline(
    structured_file,
    cities_file,
    unstructured_file
)
```

### MongoDB Loading
```python
from scripts.load import MongoDBLoader

loader = MongoDBLoader()
result = loader.load_pipeline(unstructured_file)
```

## Indexes
- PostgreSQL: city, date, composite indexes
- MongoDB: meeting_id, text search, city+date

## Error Handling
- Duplicate key errors: Logged and skipped
- Connection failures: Automatic retry
- Bulk insert errors: Partial success handling

## Testing
```bash
# Test connections
pytest tests/test_load.py::test_postgres_connection -v
pytest tests/test_load.py::test_mongodb_connection -v

# Test loading
pytest tests/test_load.py -v
```

## Database Setup

### PostgreSQL
```bash
# Create database
createdb meetingbank

# Run schema
psql -d meetingbank -f sql/create_tables.sql
```

### MongoDB
```bash
# Start MongoDB
mongod

# Database created automatically on first insert
```

## Dependencies
- sqlalchemy
- psycopg2-binary
- pymongo
