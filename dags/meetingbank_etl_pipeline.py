"""
Main Airflow DAG for MeetingBank ETL Pipeline
Orchestrates the complete data pipeline
"""

from datetime import datetime, timedelta
from pathlib import Path 

# --- Airflow Core Imports ---
from airflow import DAG
from airflow.operators.python import PythonOperator  # <--- MUST be present for your tasks
from airflow.operators.dummy import DummyOperator
# --- Custom Scripts Imports ---
from scripts.extract import MeetingBankExtractor
from scripts.clean import DataCleaner
from scripts.transform import DataTransformer
from scripts.load import PostgreSQLLoader, MongoDBLoader
from scripts.analytics import AnalyticsEngine
from scripts.config import Config


# Default arguments for all tasks
default_args = {
    'owner': 'group2',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email': ['group2@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15)
}

# Create DAG
dag = DAG(
    'meetingbank_etl_pipeline',
    default_args=default_args,
    description='Automated Meeting Intelligence Pipeline with Airflow',
    schedule_interval='@daily',  # Run daily, or None for manual trigger
    catchup=False,
    max_active_runs=1,
    tags=['meetingbank', 'etl', 'group2']
)


# Task functions
def fetch_data_task(**context):
    """Task 1: Fetch data from HuggingFace"""
    extractor = MeetingBankExtractor()
    result = extractor.extract_pipeline()
    
    if not result['success']:
        raise Exception(f"Data extraction failed: {result.get('error')}")
    
    # Push output file path to XCom
    context['ti'].xcom_push(key='raw_data_file', value=result['output_file'])
    return result


def clean_data_task(**context):
    """Task 2: Clean and validate data"""
    # Pull input file from previous task
    ti = context['ti']
    raw_file = ti.xcom_pull(key='raw_data_file', task_ids='fetch_data')
    
    cleaner = DataCleaner()
    result = cleaner.clean_pipeline(Path(raw_file))
    
    if not result['success']:
        raise Exception("Data cleaning failed")
    
    # Push output file to XCom
    ti.xcom_push(key='cleaned_data_file', value=result['output_file'])
    return result


def transform_data_task(**context):
    """Task 3: Transform data"""
    ti = context['ti']
    cleaned_file = ti.xcom_pull(key='cleaned_data_file', task_ids='clean_data')
    
    transformer = DataTransformer()
    result = transformer.transform_pipeline(Path(cleaned_file))
    
    if not result['success']:
        raise Exception("Data transformation failed")
    
    # Push output files to XCom
    ti.xcom_push(key='structured_file', value=result['structured_file'])
    ti.xcom_push(key='cities_file', value=result['cities_file'])
    ti.xcom_push(key='unstructured_file', value=result['unstructured_file'])
    return result


def load_postgres_task(**context):
    """Task 4a: Load data to PostgreSQL"""
    ti = context['ti']
    structured_file = Path(ti.xcom_pull(key='structured_file', task_ids='transform_data'))
    cities_file = Path(ti.xcom_pull(key='cities_file', task_ids='transform_data'))
    unstructured_file = Path(ti.xcom_pull(key='unstructured_file', task_ids='transform_data'))
    
    loader = PostgreSQLLoader()
    result = loader.load_pipeline(structured_file, cities_file, unstructured_file)
    
    if not result['success']:
        raise Exception(f"PostgreSQL loading failed: {result.get('error')}")
    
    return result


def load_mongodb_task(**context):
    """Task 4b: Load data to MongoDB"""
    ti = context['ti']
    unstructured_file = Path(ti.xcom_pull(key='unstructured_file', task_ids='transform_data'))
    
    loader = MongoDBLoader()
    result = loader.load_pipeline(unstructured_file)
    
    if not result['success']:
        raise Exception(f"MongoDB loading failed: {result.get('error')}")
    
    return result


def run_analytics_task(**context):
    """Task 5: Run analytics and generate reports"""
    analytics = AnalyticsEngine()
    result = analytics.analytics_pipeline()
    
    if not result['success']:
        raise Exception("Analytics generation failed")
    
    return result


# Define tasks
start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_task,
    provide_context=True,
    execution_timeout=timedelta(minutes=15),
    dag=dag
)

clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data_task,
    provide_context=True,
    retries=1,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_task,
    provide_context=True,
    retries=1,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

load_postgres = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_postgres_task,
    provide_context=True,
    retries=2,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

load_mongodb = PythonOperator(
    task_id='load_to_mongodb',
    python_callable=load_mongodb_task,
    provide_context=True,
    retries=2,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

run_analytics = PythonOperator(
    task_id='run_analytics',
    python_callable=run_analytics_task,
    provide_context=True,
    retries=1,
    execution_timeout=timedelta(minutes=15),
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

# Define task dependencies
start_task >> fetch_data >> clean_data >> transform_data
transform_data >> [load_postgres, load_mongodb] >> run_analytics >> end_task
