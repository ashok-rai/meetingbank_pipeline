# MeetingBank Pipeline - Group 2: Data Orchestrators

Automated Meeting Intelligence Pipeline using Apache Airflow

## Team Members
- Ashok Rai - Data Ingestion
- Dineshkumar Swaminathan - Data Transformation
- Harshath Vijayakumar - Database Loading
- Mrudul Madhukar Tarade - Airflow DAGs & Analytics

## Quick Start

### 1. Clone Repository
```bash
git clone <repo-url>
cd meetingbank-pipeline
```

### 2. Setup Environment
```bash
# Copy environment variables
cp .env.example .env

# Start Docker services
docker-compose up -d
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run Pipeline
```bash
# Trigger DAG manually
airflow dags trigger meetingbank_etl_pipeline
```

## Architecture
See `docs/` for detailed documentation:
- Data Ingestion: `docs/README_EXTRACT.md`
- Transformation: `docs/README_TRANSFORM.md`
- Database Loading: `docs/README_LOAD.md`
- Airflow Setup: `docs/README_AIRFLOW.md`

## Project Structure