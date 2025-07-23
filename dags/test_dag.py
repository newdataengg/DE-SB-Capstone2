from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'test_databricks_simple',
    default_args=default_args,
    description='Test Databricks connection',
    schedule=None,
    catchup=False,
    tags=['test', 'databricks'],
)

def test_connection():
    try:
        print("Testing Databricks connection...")
        hook = DatabricksHook(databricks_conn_id='databricks_default')
        
        response = hook._do_api_call(
            endpoint_info=('GET', 'api/2.0/clusters/list'),
            json={}
        )
        
        print("Connection successful!")
        print(f"Found {len(response.get('clusters', []))} clusters")
        return "SUCCESS"
        
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        raise

test_task = PythonOperator(
    task_id='test_databricks_connection',
    python_callable=test_connection,
    dag=dag,
)