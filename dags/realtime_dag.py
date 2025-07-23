"""
Stock Sentiment Real-time Processing DAG
Runs every 15 minutes during market hours
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import logging
import pytz
from airflow.operators.empty import EmptyOperator as DummyOperator

# DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 21),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

dag = DAG(
    'stock_sentiment_realtime',
    default_args=default_args,
    description='Real-time stock price and sentiment analysis pipeline',
    schedule='*/15 9-16 * * 1-5',  # Every 15 min, 9AM-4PM, Mon-Fri EST
    catchup=False,
    max_active_runs=1,
    tags=['realtime', 'stock', 'sentiment', 'production']
)

def check_market_hours(**context):
    """Check if US markets are open"""
    et = pytz.timezone('US/Eastern')
    now = datetime.now(et)
    
    # Market hours: 9:30 AM - 4:00 PM ET, Monday-Friday
    if now.weekday() < 5 and 9.5 <= now.hour + now.minute/60 <= 24:
        logging.info(f"✅ Markets are open at {now}")
        return 'start_realtime_pipeline'
    else:
        logging.info(f"🕐 Markets are closed at {now}")
        return 'markets_closed'

# Market hours check
market_check = BranchPythonOperator(
    task_id='check_market_hours',
    python_callable=check_market_hours,
    dag=dag
)

# Pipeline start
start_realtime_pipeline = DummyOperator(
    task_id='start_realtime_pipeline',
    dag=dag
)

# Markets closed - end gracefully
markets_closed = DummyOperator(
    task_id='markets_closed',
    dag=dag
)

# Event Hub Producer - Fetches data and streams to Event Hub
event_hub_producer = DatabricksSubmitRunOperator(
    task_id='event_hub_producer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/01_Event_Hub_Producer',
        'base_parameters': {
            'batch_id': 'realtime_{{ ds_nodash }}_{{ ts_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': 'false',
            'quality_threshold': '0.8',
            'dag_run_id': '{{ dag_run.run_id }}'
        }
    },
    timeout_seconds=1200,  # 20 minutes max
    dag=dag
)

# Stock Consumer - Processes stock data from Event Hub
stock_consumer = DatabricksSubmitRunOperator(
    task_id='stock_consumer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/02_Event_Hub_Stock_Consumer',
        'base_parameters': {
            'batch_id': 'stock_{{ ds_nodash }}_{{ ts_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': 'false',
            'quality_threshold': '0.8',
            'dag_run_id': '{{ dag_run.run_id }}'
        }
    },
    timeout_seconds=900,  # 15 minutes max
    dag=dag
)

# News Consumer - Processes news with FinBERT sentiment analysis
news_consumer = DatabricksSubmitRunOperator(
    task_id='news_consumer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/02_Event_Hub_News_Consumer',
        'base_parameters': {
            'batch_id': 'news_{{ ds_nodash }}_{{ ts_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': 'false',
            'quality_threshold': '0.8',
            'dag_run_id': '{{ dag_run.run_id }}'
        }
    },
    timeout_seconds=1200,  # 20 minutes for FinBERT processing
    dag=dag
)

# Silver Layer - Enhanced analytics and technical indicators
silver_processor = DatabricksSubmitRunOperator(
    task_id='silver_layer_processing',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/02_Silver_Layer_Processor',
        'base_parameters': {
            'batch_id': 'silver_{{ ds_nodash }}_{{ ts_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': 'false',
            'quality_threshold': '0.7',
            'dag_run_id': '{{ dag_run.run_id }}'
        }
    },
    timeout_seconds=1800,  # 30 minutes max
    dag=dag
)

# Gold Layer - Business insights and correlations
gold_processor = DatabricksSubmitRunOperator(
    task_id='gold_layer_processing',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/03_Gold_Layer_Processing',
        'base_parameters': {
            'batch_id': 'gold_{{ ds_nodash }}_{{ ts_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': 'false',
            'quality_threshold': '0.8',
            'dag_run_id': '{{ dag_run.run_id }}'
        }
    },
    timeout_seconds=1200,  # 20 minutes max
    dag=dag
)

def validate_realtime_pipeline(**context):
    """Validate pipeline results and send alerts if needed"""
    
    # Get results from Databricks notebooks
    producer_result = context['task_instance'].xcom_pull(task_ids='event_hub_producer')
    stock_result = context['task_instance'].xcom_pull(task_ids='stock_consumer')
    news_result = context['task_instance'].xcom_pull(task_ids='news_consumer')
    silver_result = context['task_instance'].xcom_pull(task_ids='silver_layer_processing')
    gold_result = context['task_instance'].xcom_pull(task_ids='gold_layer_processing')
    
    # Parse results
    validation_summary = {
        'execution_time': context['ds'],
        'pipeline_type': 'realtime',
        'total_stock_records': 0,
        'total_news_records': 0,
        'failed_tasks': [],
        'warnings': [],
        'success': True
    }
    
    # Check each task result
    tasks_results = {
        'producer': producer_result,
        'stock_consumer': stock_result,
        'news_consumer': news_result,
        'silver_processor': silver_result,
        'gold_processor': gold_result
    }
    
    for task_name, result in tasks_results.items():
        if result and isinstance(result, dict):
            if result.get('status') == 'FAILED':
                validation_summary['failed_tasks'].append(task_name)
                validation_summary['success'] = False
            
            # Count records
            records = result.get('records_processed', 0)
            if 'stock' in task_name:
                validation_summary['total_stock_records'] += records
            elif 'news' in task_name:
                validation_summary['total_news_records'] += records
        else:
            validation_summary['warnings'].append(f"No result from {task_name}")
    
    # Quality checks
    if validation_summary['total_stock_records'] < 5:
        validation_summary['warnings'].append(f"Low stock records: {validation_summary['total_stock_records']}")
    
    if validation_summary['total_news_records'] < 3:
        validation_summary['warnings'].append(f"Low news records: {validation_summary['total_news_records']}")
    
    # Log results
    if validation_summary['success']:
        logging.info(f"✅ Realtime pipeline successful: {validation_summary}")
    else:
        logging.error(f"❌ Realtime pipeline failed: {validation_summary}")
        raise ValueError(f"Pipeline validation failed: {validation_summary['failed_tasks']}")
    
    return validation_summary

# Pipeline validation
pipeline_validation = PythonOperator(
    task_id='validate_realtime_pipeline',
    python_callable=validate_realtime_pipeline,
    dag=dag
)

# End task
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag
)

# Task Dependencies
market_check >> [start_realtime_pipeline, markets_closed]

start_realtime_pipeline >> event_hub_producer
event_hub_producer >> [stock_consumer, news_consumer]
[stock_consumer, news_consumer] >> silver_processor
silver_processor >> gold_processor
gold_processor >> pipeline_validation

[pipeline_validation, markets_closed] >> end_pipeline