"""
Stock Sentiment Backfill DAG - DEBUGGED VERSION
Manual DAG for backfilling historical data with enhanced error handling and debugging
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import Variable
import logging
import json
from airflow.operators.empty import EmptyOperator as DummyOperator


# DAG Configuration for backfilling
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 21),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,  # Reduced for debugging
    'retry_delay': timedelta(minutes=5),  # Shorter retry delay
    'execution_timeout': timedelta(hours=8)
}

dag = DAG(
    'stock_sentiment_backfill_debug',
    default_args=default_args,
    description='DEBUGGED backfill for stock sentiment pipeline with enhanced error handling',
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['backfill', 'manual', 'recovery', 'historical', 'debug']
)

def validate_backfill_parameters(**context):
    """Validate backfill parameters before starting"""
    
    # Get DAG run configuration
    dag_run_conf = context.get('dag_run').conf or {}
    
    # Default parameters
    backfill_params = {
        'start_date': dag_run_conf.get('start_date'),
        'end_date': dag_run_conf.get('end_date'),
        'symbols': dag_run_conf.get('symbols', 'AAPL,GOOGL,MSFT,AMZN,META,TSLA'),
        'force_refresh': dag_run_conf.get('force_refresh', 'true'),
        'include_weekends': dag_run_conf.get('include_weekends', 'false'),
        'batch_size_days': int(dag_run_conf.get('batch_size_days', 7))
    }
    
    # Validation
    if not backfill_params['start_date'] or not backfill_params['end_date']:
        raise ValueError("start_date and end_date are required parameters")
    
    try:
        start_dt = datetime.strptime(backfill_params['start_date'], '%Y-%m-%d')
        end_dt = datetime.strptime(backfill_params['end_date'], '%Y-%m-%d')
    except ValueError:
        raise ValueError("Dates must be in YYYY-MM-DD format")
    
    if start_dt >= end_dt:
        raise ValueError("start_date must be before end_date")
    
    if (end_dt - start_dt).days > 365:
        raise ValueError("Backfill period cannot exceed 365 days")
    
    # Calculate date ranges for batch processing
    date_ranges = []
    current_date = start_dt
    
    while current_date < end_dt:
        batch_end = min(current_date + timedelta(days=backfill_params['batch_size_days']), end_dt)
        date_ranges.append({
            'start': current_date.strftime('%Y-%m-%d'),
            'end': batch_end.strftime('%Y-%m-%d')
        })
        current_date = batch_end
    
    backfill_params['date_ranges'] = date_ranges
    backfill_params['total_batches'] = len(date_ranges)
    
    logging.info(f"📋 Backfill validation passed: {backfill_params}")
    return backfill_params

# Validate parameters
validate_params = PythonOperator(
    task_id='validate_backfill_parameters',
    python_callable=validate_backfill_parameters,
    dag=dag
)

# DEBUGGED Bronze Layer with enhanced error handling
backfill_bronze = DatabricksSubmitRunOperator(
    task_id='backfill_bronze_layer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/01_Historical_Bronze_Layer',
        'base_parameters': {
            # Basic Airflow parameters
            'batch_id': 'backfill_{{ ds_nodash }}_{{ ts_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': '{{ dag_run.conf.get("force_refresh", "true") }}',
            'quality_threshold': '0.85',
            'dag_run_id': '{{ dag_run.run_id }}',
            
            # Dynamic processing parameters that match notebook expectations
            'processing_mode': 'backfill',
            'lookback_days': '{{ dag_run.conf.get("batch_size_days", "7") }}',
            'include_weekends': '{{ dag_run.conf.get("include_weekends", "false") }}',
            'symbol_list': '{{ dag_run.conf.get("symbols", "AAPL,GOOGL,MSFT,AMZN,META,TSLA") }}',
            'news_keywords': 'stock market,earnings,financial,backfill,historical',
            'batch_size': '1000',
            'data_sources': 'standard',
            'expected_stock_records': '42',  # 7 days * 6 symbols
            'expected_news_records': '50',   # Reasonable expectation for backfill
            
            # Backfill-specific parameters
            'start_date': '{{ dag_run.conf.get("start_date") }}',
            'end_date': '{{ dag_run.conf.get("end_date") }}',
            'symbols': '{{ dag_run.conf.get("symbols", "AAPL,GOOGL,MSFT,AMZN,META,TSLA") }}',
            'backfill_mode': 'true'
        }
    },
    timeout_seconds=7200,  # 2 hours for large backfills
    dag=dag
)

# DEBUGGED Silver Layer with continue_on_failure
backfill_silver = DatabricksSubmitRunOperator(
    task_id='backfill_silver_layer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/02_Silver_Layer_Processor',
        'base_parameters': {
            'batch_id': 'backfill_silver_{{ ds_nodash }}_{{ ts_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': '{{ dag_run.conf.get("force_refresh", "true") }}',
            'quality_threshold': '0.8',
            'dag_run_id': '{{ dag_run.run_id }}',
            'processing_mode': 'backfill',
            'enable_finbert': 'true',
            'enable_technical_indicators': 'true',
            'feature_engineering': 'true',
            'start_date': '{{ dag_run.conf.get("start_date") }}',
            'end_date': '{{ dag_run.conf.get("end_date") }}'
        }
    },
    timeout_seconds=10800,  # 3 hours for FinBERT processing
    trigger_rule=TriggerRule.ALL_DONE,  # Continue even if bronze partially fails
    dag=dag
)

# DEBUGGED Gold Layer with continue_on_failure
backfill_gold = DatabricksSubmitRunOperator(
    task_id='backfill_gold_layer',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/03_Gold_Layer_Processing',
        'base_parameters': {
            'batch_id': 'backfill_gold_{{ ds_nodash }}_{{ ts_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': '{{ dag_run.conf.get("force_refresh", "true") }}',
            'quality_threshold': '0.85',
            'dag_run_id': '{{ dag_run.run_id }}',
            'correlation_analysis': 'true',
            'predictive_models': 'false',  # Skip ML training in backfill
            'export_snowflake': 'true',
            'generate_reports': 'true',
            'start_date': '{{ dag_run.conf.get("start_date") }}',
            'end_date': '{{ dag_run.conf.get("end_date") }}'
        }
    },
    timeout_seconds=7200,  # 2 hours for analytics
    trigger_rule=TriggerRule.ALL_DONE,  # Continue even if silver fails
    dag=dag
)

def validate_backfill_results_debug(**context):
    """DEBUGGED validation with enhanced error handling and detailed logging"""
    
    # Get backfill parameters
    backfill_params = context['task_instance'].xcom_pull(task_ids='validate_backfill_parameters')
    
    # Get results from each layer with safe extraction
    def safe_get_result(task_id):
        try:
            result = context['task_instance'].xcom_pull(task_ids=task_id)
            logging.info(f"Result from {task_id}: {result}")
            return result
        except Exception as e:
            logging.warning(f"Could not get result from {task_id}: {e}")
            return None
    
    bronze_result = safe_get_result('backfill_bronze_layer')
    silver_result = safe_get_result('backfill_silver_layer')
    gold_result = safe_get_result('backfill_gold_layer')
    
    # Enhanced validation with detailed debugging
    validation_summary = {
        'backfill_period': f"{backfill_params['start_date']} to {backfill_params['end_date']}",
        'symbols_requested': backfill_params['symbols'].split(','),
        'total_days': (datetime.strptime(backfill_params['end_date'], '%Y-%m-%d') - 
                      datetime.strptime(backfill_params['start_date'], '%Y-%m-%d')).days,
        'layers_attempted': {
            'bronze': bronze_result is not None,
            'silver': silver_result is not None,
            'gold': gold_result is not None
        },
        'layers_processed': {
            'bronze': False,
            'silver': False,
            'gold': False
        },
        'task_states': {},
        'records_created': {
            'bronze_stock': 0,
            'bronze_news': 0,
            'silver_total': 0,
            'gold_total': 0
        },
        'data_quality_scores': {
            'bronze': 0,
            'silver': 0,
            'gold': 0
        },
        'success': False,
        'issues': [],
        'debug_info': {
            'bronze_result_type': type(bronze_result).__name__ if bronze_result else 'None',
            'silver_result_type': type(silver_result).__name__ if silver_result else 'None',
            'gold_result_type': type(gold_result).__name__ if gold_result else 'None'
        }
    }
    
    # Check task states from Airflow context
    try:
        dag_run = context['dag_run']
        for task_instance in dag_run.get_task_instances():
            validation_summary['task_states'][task_instance.task_id] = task_instance.state
        logging.info(f"Task states: {validation_summary['task_states']}")
    except Exception as e:
        logging.warning(f"Could not get task states: {e}")
    
    # Process results with enhanced error handling
    layers_results = {
        'bronze': bronze_result,
        'silver': silver_result,
        'gold': gold_result
    }
    
    successful_layers = 0
    
    for layer, result in layers_results.items():
        try:
            if result and isinstance(result, dict):
                # Check for various success indicators
                status_indicators = [
                    result.get('status') == 'SUCCESS',
                    result.get('status') == 'PARTIAL',
                    result.get('stock_records_processed', 0) > 0,
                    result.get('news_records_processed', 0) > 0,
                    result.get('total_records', 0) > 0
                ]
                
                if any(status_indicators):
                    validation_summary['layers_processed'][layer] = True
                    successful_layers += 1
                    
                    # Extract records with safe defaults
                    if layer == 'bronze':
                        validation_summary['records_created']['bronze_stock'] = result.get('stock_records_processed', 0)
                        validation_summary['records_created']['bronze_news'] = result.get('news_records_processed', 0)
                    elif layer == 'silver':
                        validation_summary['records_created']['silver_total'] = result.get('records_processed', 0)
                    elif layer == 'gold':
                        validation_summary['records_created']['gold_total'] = result.get('records_processed', 0)
                    
                    # Extract quality scores
                    quality_score = result.get('data_quality_score', 0)
                    if quality_score and isinstance(quality_score, (int, float)):
                        validation_summary['data_quality_scores'][layer] = quality_score
                
                # Add debug information
                validation_summary['debug_info'][f'{layer}_keys'] = list(result.keys()) if result else []
                
            elif result:
                # Handle non-dict results
                validation_summary['debug_info'][f'{layer}_result_value'] = str(result)[:200]
                
        except Exception as layer_error:
            logging.error(f"Error processing {layer} result: {layer_error}")
            validation_summary['issues'].append(f"Error processing {layer}: {str(layer_error)}")
    
    # Enhanced success criteria - more lenient for debugging
    total_records = sum(validation_summary['records_created'].values())
    
    if successful_layers >= 1 and total_records > 0:
        validation_summary['success'] = True
        validation_summary['status'] = 'PARTIAL_SUCCESS' if successful_layers < 3 else 'SUCCESS'
    elif successful_layers > 0:
        validation_summary['success'] = True  # Allow partial success
        validation_summary['status'] = 'MINIMAL_SUCCESS'
    else:
        validation_summary['success'] = False
        validation_summary['status'] = 'FAILED'
    
    # Add specific issues for debugging
    if validation_summary['records_created']['bronze_stock'] == 0:
        validation_summary['issues'].append("No stock records created in bronze layer - check API keys and connectivity")
    
    if validation_summary['records_created']['bronze_news'] == 0:
        validation_summary['issues'].append("No news records created in bronze layer - check NewsAPI key and connectivity")
    
    if not any(validation_summary['layers_processed'].values()):
        validation_summary['issues'].append("All layers failed - check Databricks cluster and notebook paths")
    
    # Calculate average quality score for successful layers only
    quality_scores = [score for score in validation_summary['data_quality_scores'].values() if score > 0]
    avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
    
    if avg_quality < 0.8 and quality_scores:
        validation_summary['issues'].append(f"Low average data quality: {avg_quality:.2f}")
    
    # Store results as JSON string
    Variable.set(f"backfill_summary_{context['ds_nodash']}", json.dumps(validation_summary))
    
    # Enhanced logging
    if validation_summary['success']:
        logging.info(f"✅ Backfill validation passed with {successful_layers}/3 layers successful: {validation_summary}")
    else:
        logging.error(f"❌ Backfill validation failed: {validation_summary}")
        # Don't raise exception for debugging - let the pipeline continue
        logging.warning("Continuing pipeline for debugging purposes")
    
    return validation_summary

# DEBUGGED validation task
validate_results = PythonOperator(
    task_id='validate_backfill_results',
    python_callable=validate_backfill_results_debug,
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    dag=dag
)

def generate_debug_report(**context):
    """Generate comprehensive debug report"""
    
    validation_summary = context['task_instance'].xcom_pull(task_ids='validate_backfill_results')
    
    # Create detailed debug report
    debug_report = {
        'timestamp': datetime.now().isoformat(),
        'dag_run_id': context['dag_run'].run_id,
        'execution_date': context['ds'],
        'validation_summary': validation_summary,
        'debug_analysis': {
            'likely_issues': [],
            'recommendations': [],
            'next_steps': []
        }
    }
    
    # Analyze issues and provide recommendations
    if validation_summary:
        issues = validation_summary.get('issues', [])
        
        # API connectivity issues
        if any('API' in issue or 'connectivity' in issue for issue in issues):
            debug_report['debug_analysis']['likely_issues'].append('API connectivity or authentication problems')
            debug_report['debug_analysis']['recommendations'].extend([
                'Check Databricks secrets scope configuration',
                'Verify polygon-api-key and newsapi-key are valid',
                'Test API endpoints manually',
                'Check cluster network connectivity'
            ])
        
        # Layer failures
        if any('layers failed' in issue for issue in issues):
            debug_report['debug_analysis']['likely_issues'].append('Databricks notebook execution problems')
            debug_report['debug_analysis']['recommendations'].extend([
                'Verify notebook paths exist in workspace',
                'Check cluster configuration and availability',
                'Review notebook parameter mapping',
                'Verify Unity Catalog permissions'
            ])
        
        # Data quality issues
        if any('quality' in issue for issue in issues):
            debug_report['debug_analysis']['likely_issues'].append('Data quality or processing issues')
            debug_report['debug_analysis']['recommendations'].extend([
                'Review date range parameters',
                'Check symbol list validity',
                'Verify news keyword effectiveness',
                'Analyze processing mode configuration'
            ])
        
        # Next steps based on success level
        successful_layers = sum(validation_summary.get('layers_processed', {}).values())
        
        if successful_layers == 0:
            debug_report['debug_analysis']['next_steps'] = [
                '1. Check Databricks cluster logs for detailed errors',
                '2. Test bronze layer notebook manually with minimal parameters',
                '3. Verify all secrets and connections are properly configured',
                '4. Check Unity Catalog table permissions and schema'
            ]
        elif successful_layers == 1:
            debug_report['debug_analysis']['next_steps'] = [
                '1. Focus on fixing silver layer processing',
                '2. Check bronze-to-silver data flow',
                '3. Verify silver layer dependencies and requirements',
                '4. Review FinBERT processing configuration'
            ]
        elif successful_layers == 2:
            debug_report['debug_analysis']['next_steps'] = [
                '1. Focus on gold layer analytics',
                '2. Check silver-to-gold data dependencies',
                '3. Verify analytics processing requirements',
                '4. Review export and reporting configurations'
            ]
        else:
            debug_report['debug_analysis']['next_steps'] = [
                '1. Pipeline working - monitor data quality',
                '2. Optimize processing parameters if needed',
                '3. Consider production deployment'
            ]
    
    # Store debug report
    Variable.set(f"debug_report_{context['ds_nodash']}", json.dumps(debug_report))
    
    logging.info(f"📊 Debug report generated: {debug_report}")
    return debug_report

# Debug report generation
generate_report = PythonOperator(
    task_id='generate_debug_report',
    python_callable=generate_debug_report,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# End task
end_backfill = DummyOperator(
    task_id='end_backfill',
    trigger_rule=TriggerRule.ALL_DONE,  # Complete regardless of upstream failures
    dag=dag
)

# Task Dependencies
validate_params >> backfill_bronze
backfill_bronze >> backfill_silver
backfill_silver >> backfill_gold
backfill_gold >> validate_results
validate_results >> generate_report
generate_report >> end_backfill