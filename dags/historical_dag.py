"""
Stock Sentiment Historical Processing DAG - Enhanced Version
Runs daily with intelligent processing scope based on market timing
Handles end-of-day, weekend, and market close processing
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import Variable  # Fixed import
import logging
import pytz
import json  # Added for JSON serialization
from airflow.operators.empty import EmptyOperator as DummyOperator

# Enhanced DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,  # Changed to False to prevent blocking
    'start_date': datetime(2025, 1, 21),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=6)  # Extended for weekend processing
}

dag = DAG(
    'stock_sentiment_historical',
    default_args=default_args,
    description='Enhanced historical stock sentiment analysis with dynamic processing scope',
    schedule='0 18 * * *',  # 6 PM ET daily (including weekends)
    catchup=False,  # Changed to False to prevent excessive backfill
    max_active_runs=1,
    tags=['historical', 'batch', 'stock', 'sentiment', 'dynamic', 'weekend']
)

def determine_processing_scope(**context):
    """
    Intelligent processing strategy based on market schedule and execution context
    Returns dynamic configuration parameters for downstream tasks
    """
    
    execution_date = datetime.strptime(context['ds'], '%Y-%m-%d')
    et = pytz.timezone('US/Eastern')
    current_et = datetime.now(et)
    
    logging.info(f"🎯 Determining processing scope for {execution_date.strftime('%A, %Y-%m-%d')}")
    
    # Initialize comprehensive scope configuration
    scope_config = {
        'processing_mode': 'daily',
        'lookback_days': '1',
        'include_weekends': 'false',
        'data_sources': 'standard',
        'processing_priority': 'normal',
        'quality_threshold': '0.85',
        'symbol_list': 'AAPL,GOOGL,MSFT,AMZN,META,TSLA',
        'news_keywords': 'stock market,earnings,financial',
        'batch_size': '1000',
        'timeout_minutes': '60',
        'cluster_size': 'standard',
        'reasoning': '',
        'expected_stock_records': '6',
        'expected_news_records': '30'
    }
    
    # 📅 MONDAY: Start of week - comprehensive catch-up
    if execution_date.weekday() == 0:  # Monday
        scope_config.update({
            'processing_mode': 'weekly_start',
            'lookback_days': '3',  # Catch Friday + Weekend
            'include_weekends': 'true',
            'data_sources': 'comprehensive',
            'symbol_list': 'AAPL,GOOGL,MSFT,AMZN,META,TSLA,NVDA,JPM,JNJ,PG,XOM,CVX',
            'news_keywords': 'stock market,earnings,financial,weekend analysis,weekly outlook',
            'quality_threshold': '0.9',
            'batch_size': '1500',
            'timeout_minutes': '90',
            'cluster_size': 'large',
            'expected_stock_records': '36',  # 12 symbols * 3 days
            'expected_news_records': '75',
            'reasoning': 'Monday: Weekend catch-up with extended symbol list and comprehensive analysis'
        })
    
    # 📅 TUESDAY-WEDNESDAY: Regular daily processing
    elif execution_date.weekday() in [1, 2]:  # Tue-Wed
        scope_config.update({
            'processing_mode': 'daily_standard',
            'lookback_days': '1',
            'include_weekends': 'false',
            'data_sources': 'standard',
            'timeout_minutes': '60',
            'cluster_size': 'standard',
            'expected_stock_records': '6',
            'expected_news_records': '30',
            'reasoning': f'{execution_date.strftime("%A")}: Standard daily processing'
        })
    
    # 📅 THURSDAY: Mid-week comprehensive check
    elif execution_date.weekday() == 3:  # Thursday
        scope_config.update({
            'processing_mode': 'daily_enhanced',
            'lookback_days': '1',
            'include_weekends': 'false',
            'data_sources': 'enhanced',
            'symbol_list': 'AAPL,GOOGL,MSFT,AMZN,META,TSLA,NVDA,JPM',
            'news_keywords': 'stock market,earnings,financial,weekly trends',
            'quality_threshold': '0.87',
            'batch_size': '1200',
            'timeout_minutes': '75',
            'expected_stock_records': '8',
            'expected_news_records': '40',
            'reasoning': 'Thursday: Enhanced daily processing with additional symbols'
        })
    
    # 📅 FRIDAY: End of week - comprehensive processing
    elif execution_date.weekday() == 4:  # Friday
        scope_config.update({
            'processing_mode': 'weekly_end',
            'lookback_days': '1',
            'include_weekends': 'false',
            'data_sources': 'comprehensive',
            'symbol_list': 'AAPL,GOOGL,MSFT,AMZN,META,TSLA,NVDA,JPM,JNJ,PG,XOM,CVX,ORCL,CRM',
            'news_keywords': 'stock market,earnings,financial,weekly outlook,market analysis,weekend preview',
            'quality_threshold': '0.92',
            'batch_size': '2000',
            'timeout_minutes': '120',
            'cluster_size': 'large',
            'expected_stock_records': '14',
            'expected_news_records': '60',
            'reasoning': 'Friday: End-of-week comprehensive processing with extended analysis'
        })
    
    # 📅 SATURDAY: Weekend processing - focus on Friday data + weekend news
    elif execution_date.weekday() == 5:  # Saturday
        scope_config.update({
            'processing_mode': 'weekend_primary',
            'lookback_days': '2',  # Friday + Saturday news
            'include_weekends': 'true',
            'data_sources': 'news_heavy',
            'symbol_list': 'AAPL,GOOGL,MSFT,AMZN,META,TSLA,NVDA,JPM,JNJ,PG',
            'news_keywords': 'stock market,earnings,financial,weekend analysis,market outlook,monday preview',
            'quality_threshold': '0.88',
            'batch_size': '1800',
            'timeout_minutes': '150',  # Can run longer on weekends
            'cluster_size': 'large',
            'processing_priority': 'low',
            'expected_stock_records': '10',  # Friday data verification
            'expected_news_records': '80',  # More weekend news
            'reasoning': 'Saturday: Weekend news analysis and Friday data verification'
        })
    
    # 📅 SUNDAY: Weekend comprehensive - prepare for Monday
    else:  # Sunday
        scope_config.update({
            'processing_mode': 'weekend_comprehensive',
            'lookback_days': '3',  # Fri, Sat, Sun
            'include_weekends': 'true',
            'data_sources': 'comprehensive',
            'symbol_list': 'AAPL,GOOGL,MSFT,AMZN,META,TSLA,NVDA,JPM,JNJ,PG,XOM,CVX,ORCL,CRM,V,MA',
            'news_keywords': 'stock market,earnings,financial,weekend analysis,monday outlook,market preview,week ahead',
            'quality_threshold': '0.95',
            'batch_size': '2500',
            'timeout_minutes': '180',  # Maximum time for comprehensive processing
            'cluster_size': 'xlarge',
            'processing_priority': 'low',
            'expected_stock_records': '16',  # Weekend verification + Monday prep
            'expected_news_records': '100',  # Comprehensive weekend news
            'reasoning': 'Sunday: Comprehensive weekend analysis + Monday preparation'
        })
    
    # 🎯 SPECIAL CONDITIONS OVERLAY
    
    # Month-end processing enhancement
    if execution_date.day >= 28:
        month_end_overlay = {
            'processing_mode': scope_config['processing_mode'] + '_month_end',
            'quality_threshold': '0.95',
            'symbol_list': scope_config['symbol_list'] + ',VTI,SPY,QQQ,IWM',  # Add ETFs
            'news_keywords': scope_config['news_keywords'] + ',monthly outlook,month end',
            'timeout_minutes': str(int(scope_config['timeout_minutes']) + 30),
            'reasoning': scope_config['reasoning'] + ' + Month-end comprehensive analysis'
        }
        scope_config.update(month_end_overlay)
        logging.info("📅 Month-end overlay applied")
    
    # Quarter-end processing enhancement
    if execution_date.month in [3, 6, 9, 12] and execution_date.day >= 28:
        quarter_end_overlay = {
            'processing_mode': scope_config['processing_mode'] + '_quarter_end',
            'lookback_days': str(max(int(scope_config['lookback_days']), 7)),
            'news_keywords': scope_config['news_keywords'] + ',quarterly results,earnings season,Q' + str((execution_date.month-1)//3 + 1),
            'quality_threshold': '0.98',
            'timeout_minutes': str(int(scope_config['timeout_minutes']) + 60),
            'reasoning': scope_config['reasoning'] + ' + Quarter-end extended analysis'
        }
        scope_config.update(quarter_end_overlay)
        logging.info("📅 Quarter-end overlay applied")
    
    # Holiday processing (day after major holidays)
    us_holidays_2025 = [
        datetime(2025, 1, 1),   # New Year's Day
        datetime(2025, 1, 20),  # MLK Day
        datetime(2025, 2, 17),  # Presidents Day
        datetime(2025, 5, 26),  # Memorial Day
        datetime(2025, 7, 4),   # Independence Day
        datetime(2025, 9, 1),   # Labor Day
        datetime(2025, 11, 27), # Thanksgiving
        datetime(2025, 12, 25)  # Christmas
    ]
    
    yesterday = execution_date - timedelta(days=1)
    if yesterday in us_holidays_2025:
        holiday_overlay = {
            'processing_mode': scope_config['processing_mode'] + '_post_holiday',
            'lookback_days': str(max(int(scope_config['lookback_days']), 2)),
            'include_weekends': 'true',
            'news_keywords': scope_config['news_keywords'] + ',post holiday,market reopening',
            'reasoning': scope_config['reasoning'] + ' + Post-holiday catch-up processing'
        }
        scope_config.update(holiday_overlay)
        logging.info("🎉 Post-holiday overlay applied")
    
    # Log the final decision with full details
    logging.info("="*80)
    logging.info(f"📋 PROCESSING SCOPE DETERMINED for {execution_date.strftime('%A, %Y-%m-%d')}")
    logging.info("="*80)
    logging.info(f"🎯 Mode: {scope_config['processing_mode']}")
    logging.info(f"📅 Lookback Days: {scope_config['lookback_days']}")
    logging.info(f"📊 Include Weekends: {scope_config['include_weekends']}")
    logging.info(f"🏢 Symbols: {scope_config['symbol_list']}")
    logging.info(f"📰 News Keywords: {scope_config['news_keywords']}")
    logging.info(f"⭐ Quality Threshold: {scope_config['quality_threshold']}")
    logging.info(f"⏱️ Timeout: {scope_config['timeout_minutes']} minutes")
    logging.info(f"🖥️ Cluster Size: {scope_config['cluster_size']}")
    logging.info(f"📈 Expected Stock Records: {scope_config['expected_stock_records']}")
    logging.info(f"📰 Expected News Records: {scope_config['expected_news_records']}")
    logging.info(f"💭 Reasoning: {scope_config['reasoning']}")
    logging.info("="*80)
    
    # Store scope for monitoring and reporting - Fixed JSON serialization
    Variable.set(f"processing_scope_{context['ds_nodash']}", json.dumps(scope_config))
    
    return scope_config

def validate_processing_scope(**context):
    """Validate the processing scope before execution"""
    
    scope_config = context['task_instance'].xcom_pull(task_ids='determine_processing_scope')
    
    # Validation checks
    validations = {
        'scope_received': scope_config is not None,
        'required_fields': all(key in scope_config for key in 
                             ['processing_mode', 'lookback_days', 'symbol_list']),
        'valid_lookback': 1 <= int(scope_config.get('lookback_days', 0)) <= 7,
        'valid_symbols': len(scope_config.get('symbol_list', '').split(',')) >= 6,
        'valid_timeout': 30 <= int(scope_config.get('timeout_minutes', 0)) <= 300
    }
    
    if not all(validations.values()):
        failed_validations = [k for k, v in validations.items() if not v]
        raise ValueError(f"❌ Scope validation failed: {failed_validations}")
    
    logging.info("✅ Processing scope validation passed")
    return scope_config

# Start marker
start_historical = DummyOperator(
    task_id='start_historical_processing',
    dag=dag
)

# Dynamic scope determination
determine_scope = PythonOperator(
    task_id='determine_processing_scope',
    python_callable=determine_processing_scope,
    dag=dag
)

# Scope validation
validate_scope = PythonOperator(
    task_id='validate_processing_scope',
    python_callable=validate_processing_scope,
    dag=dag
)

# Enhanced Historical Bronze Layer with dynamic parameters
historical_bronze = DatabricksSubmitRunOperator(
    task_id='historical_bronze_ingestion',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/01_Historical_Bronze_Layer',
        'base_parameters': {
            'batch_id': 'historical_{{ ds_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': 'true',
            'dag_run_id': '{{ dag_run.run_id }}',
            
            # 🎯 DYNAMIC PARAMETERS from processing scope - Fixed templating
            'processing_mode': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["processing_mode"] }}',
            'lookback_days': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["lookback_days"] }}',
            'include_weekends': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["include_weekends"] }}',
            'symbol_list': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["symbol_list"] }}',
            'news_keywords': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["news_keywords"] }}',
            'quality_threshold': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["quality_threshold"] }}',
            'batch_size': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["batch_size"] }}',
            'data_sources': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["data_sources"] }}',
            'expected_stock_records': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["expected_stock_records"] }}',
            'expected_news_records': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["expected_news_records"] }}'
        }
    },
    # Fixed timeout calculation with proper default
    timeout_seconds=3600,  # Default 1 hour, can be overridden
    dag=dag
)

# Enhanced Silver Layer Processing with dynamic configuration
historical_silver = DatabricksSubmitRunOperator(
    task_id='historical_silver_processing',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/02_Silver_Layer_Processor',
        'base_parameters': {
            'batch_id': 'silver_historical_{{ ds_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': 'true',
            'dag_run_id': '{{ dag_run.run_id }}',
            
            # Inherit configuration from bronze processing
            'processing_mode': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["processing_mode"] }}',
            'quality_threshold': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["quality_threshold"] }}',
            'enable_finbert': 'true',
            'enable_technical_indicators': 'true',
            'feature_engineering': 'true'
        }
    },
    # Fixed timeout
    timeout_seconds=5400,  # 1.5 hours
    dag=dag
)

# Advanced Gold Layer Analytics with dynamic scope
historical_gold = DatabricksSubmitRunOperator(
    task_id='historical_gold_analytics',
    databricks_conn_id='databricks_default',
    existing_cluster_id='{{ var.value.databricks_cluster_id }}',
    notebook_task={
        'notebook_path': '/Workspace/stock-sentiment-project/03_Gold_Layer_Processing',
        'base_parameters': {
            'batch_id': 'gold_historical_{{ ds_nodash }}',
            'execution_date': '{{ ds }}',
            'force_refresh': 'true',
            'dag_run_id': '{{ dag_run.run_id }}',
            
            # Enhanced analytics for comprehensive processing
            'processing_mode': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["processing_mode"] }}',
            'quality_threshold': '{{ ti.xcom_pull(task_ids="determine_processing_scope")["quality_threshold"] }}',
            'correlation_analysis': 'true',
            'predictive_models': 'false',  # Simplified conditional logic
            'export_snowflake': 'true',
            'generate_reports': 'true'  # Simplified conditional logic
        }
    },
    # Fixed timeout
    timeout_seconds=4800,  # 80 minutes
    dag=dag
)

def comprehensive_data_validation(**context):
    """Enhanced validation for dynamic historical processing"""
    
    execution_date = context['ds']
    scope_config = context['task_instance'].xcom_pull(task_ids='determine_processing_scope')
    
    # Get results from all processing layers
    bronze_result = context['task_instance'].xcom_pull(task_ids='historical_bronze_ingestion')
    silver_result = context['task_instance'].xcom_pull(task_ids='historical_silver_processing')
    gold_result = context['task_instance'].xcom_pull(task_ids='historical_gold_analytics')
    
    validation_results = {
        'execution_date': execution_date,
        'pipeline_type': 'historical_dynamic',
        'processing_mode': scope_config.get('processing_mode', 'unknown') if scope_config else 'unknown',
        'expected_vs_actual': {},
        'quality_metrics': {},
        'failed_tasks': [],
        'warnings': [],
        'success': True,
        'scope_effectiveness': 0.0
    }
    
    # Handle case where scope_config is None
    if not scope_config:
        validation_results['warnings'].append("No scope configuration found")
        validation_results['success'] = False
        Variable.set(f"dynamic_validation_{context['ds_nodash']}", json.dumps(validation_results))
        return validation_results
    
    # Validate against expected results from scope
    expected_stock = int(scope_config.get('expected_stock_records', 0))
    expected_news = int(scope_config.get('expected_news_records', 0))
    expected_quality = float(scope_config.get('quality_threshold', 0.85))
    
    # Process results from each layer
    layers_results = {
        'bronze': bronze_result,
        'silver': silver_result,
        'gold': gold_result
    }
    
    total_quality_score = 0
    quality_count = 0
    
    for layer, result in layers_results.items():
        if result and isinstance(result, dict):
            if result.get('status') == 'FAILED':
                validation_results['failed_tasks'].append(layer)
                validation_results['success'] = False
            else:
                # Record actual vs expected
                if layer == 'bronze':
                    actual_stock = result.get('stock_records_processed', 0)
                    actual_news = result.get('news_records_processed', 0)
                    
                    validation_results['expected_vs_actual'] = {
                        'stock_records': {'expected': expected_stock, 'actual': actual_stock},
                        'news_records': {'expected': expected_news, 'actual': actual_news}
                    }
                    
                    # Check if we're within reasonable bounds
                    if actual_stock < expected_stock * 0.7:
                        validation_results['warnings'].append(f"Low stock records: {actual_stock} vs expected {expected_stock}")
                    if actual_news < expected_news * 0.5:
                        validation_results['warnings'].append(f"Low news records: {actual_news} vs expected {expected_news}")
                
                # Aggregate quality scores
                quality_score = result.get('data_quality_score', 0)
                if quality_score > 0:
                    total_quality_score += quality_score
                    quality_count += 1
                    validation_results['quality_metrics'][layer] = quality_score
        else:
            validation_results['warnings'].append(f"No result from {layer} layer")
    
    # Calculate overall quality and scope effectiveness
    if quality_count > 0:
        avg_quality = total_quality_score / quality_count
        validation_results['quality_metrics']['overall'] = avg_quality
        
        # Scope effectiveness: how well did our dynamic scope work?
        scope_effectiveness = min(1.0, avg_quality / expected_quality)
        
        # Adjust for actual vs expected records
        if 'expected_vs_actual' in validation_results and validation_results['expected_vs_actual']:
            stock_ratio = min(1.0, validation_results['expected_vs_actual']['stock_records']['actual'] / 
                            max(1, validation_results['expected_vs_actual']['stock_records']['expected']))
            news_ratio = min(1.0, validation_results['expected_vs_actual']['news_records']['actual'] / 
                           max(1, validation_results['expected_vs_actual']['news_records']['expected']))
            scope_effectiveness *= (stock_ratio * 0.4 + news_ratio * 0.6)  # News weighted higher
        
        validation_results['scope_effectiveness'] = scope_effectiveness
    
    # Enhanced validation for different processing modes
    processing_mode = scope_config.get('processing_mode', '')
    
    if 'weekend' in processing_mode:
        # Weekend processing should have more news
        if validation_results.get('expected_vs_actual', {}).get('news_records', {}).get('actual', 0) < 50:
            validation_results['warnings'].append("Weekend processing should yield more news articles")
    
    if 'comprehensive' in processing_mode:
        # Comprehensive processing should have higher quality
        if quality_count > 0 and avg_quality < 0.9:
            validation_results['warnings'].append(f"Comprehensive processing quality below expectations: {avg_quality:.2f}")
    
    # Final validation
    if len(validation_results['failed_tasks']) > 0:
        validation_results['success'] = False
    
    if validation_results['scope_effectiveness'] < 0.7:
        validation_results['warnings'].append(f"Low scope effectiveness: {validation_results['scope_effectiveness']:.2f}")
    
    # Log comprehensive results
    if validation_results['success']:
        logging.info(f"✅ Dynamic historical pipeline validation passed: {validation_results}")
    else:
        logging.error(f"❌ Dynamic historical pipeline validation failed: {validation_results}")
        # Don't raise error to prevent task failure cascade
        logging.warning("Continuing pipeline despite validation issues")
    
    # Store detailed results for monitoring - Fixed JSON serialization
    Variable.set(f"dynamic_validation_{context['ds_nodash']}", json.dumps(validation_results))
    
    return validation_results

# Enhanced data validation
data_validation = PythonOperator(
    task_id='comprehensive_data_validation',
    python_callable=comprehensive_data_validation,
    dag=dag
)

def generate_dynamic_daily_report(**context):
    """Generate enhanced daily report with dynamic scope analysis"""
    
    scope_config = context['task_instance'].xcom_pull(task_ids='determine_processing_scope')
    validation_results = context['task_instance'].xcom_pull(task_ids='comprehensive_data_validation')
    
    # Handle None cases gracefully
    if not scope_config:
        scope_config = {'processing_mode': 'unknown', 'reasoning': 'Configuration not available'}
    if not validation_results:
        validation_results = {'scope_effectiveness': 0, 'warnings': [], 'success': False}
    
    # Create comprehensive report
    daily_report = {
        'date': context['ds'],
        'pipeline_type': 'historical_dynamic',
        'scope_analysis': {
            'determined_mode': scope_config.get('processing_mode'),
            'reasoning': scope_config.get('reasoning'),
            'effectiveness_score': validation_results.get('scope_effectiveness', 0),
            'configuration_used': scope_config
        },
        'execution_summary': validation_results,
        'performance_metrics': {
            'scope_accuracy': validation_results.get('scope_effectiveness', 0),
            'expected_vs_actual_ratio': 'calculated_from_validation',
            'processing_efficiency': 'calculated_from_timing'
        },
        'insights': {
            'processing_mode_insights': f"Used {scope_config.get('processing_mode')} mode",
            'data_coverage': f"Processed {validation_results.get('expected_vs_actual', {}).get('stock_records', {}).get('actual', 0)} stock records",
            'quality_achievement': f"Achieved {validation_results.get('quality_metrics', {}).get('overall', 0):.2%} quality score"
        },
        'recommendations': []
    }
    
    # Generate intelligent recommendations
    scope_effectiveness = validation_results.get('scope_effectiveness', 0)
    
    if scope_effectiveness > 0.9:
        daily_report['recommendations'].append('✅ Dynamic scope working excellently - maintain current logic')
    elif scope_effectiveness > 0.7:
        daily_report['recommendations'].append('🔧 Dynamic scope performing well - minor optimizations possible')
    else:
        daily_report['recommendations'].append('⚠️ Dynamic scope needs optimization - review scope determination logic')
    
    if validation_results.get('warnings'):
        daily_report['recommendations'].append(f'📋 Address {len(validation_results["warnings"])} processing warnings')
    
    # Weekend-specific insights
    if 'weekend' in scope_config.get('processing_mode', ''):
        daily_report['insights']['weekend_processing'] = 'Weekend comprehensive analysis completed'
        daily_report['recommendations'].append('📊 Review weekend insights for Monday market preparation')
    
    # Store enhanced report - Fixed JSON serialization
    Variable.set(f"dynamic_daily_report_{context['ds_nodash']}", json.dumps(daily_report))
    
    logging.info(f"📊 Dynamic daily report generated: {daily_report}")
    return daily_report

# Enhanced daily report generation
daily_report = PythonOperator(
    task_id='generate_dynamic_daily_report',
    python_callable=generate_dynamic_daily_report,
    dag=dag
)

# Data archival with dynamic considerations
def enhanced_monthly_data_archival(**context):
    """Enhanced archival with dynamic scope considerations"""
    
    execution_date = datetime.strptime(context['ds'], '%Y-%m-%d')
    
    if execution_date.day == 1:  # First day of month
        scope_config = context['task_instance'].xcom_pull(task_ids='determine_processing_scope')
        
        retention_days = int(Variable.get('data_retention_days', '365'))  # Default as string
        archive_date = execution_date - timedelta(days=retention_days)
        
        # Enhanced archival for different processing modes
        archival_strategy = {
            'archive_required': True,
            'archive_date': archive_date.isoformat(),
            'retention_days': retention_days,
            'processing_mode_context': scope_config.get('processing_mode', 'standard') if scope_config else 'standard',
            'priority_data_retention': 'weekend_comprehensive,quarter_end' # Keep longer
        }
        
        logging.info(f"📦 Enhanced archival strategy: {archival_strategy}")
        return archival_strategy
    else:
        return {'archive_required': False}

archival_check = PythonOperator(
    task_id='enhanced_monthly_data_archival',
    python_callable=enhanced_monthly_data_archival,
    dag=dag
)

# End task
end_historical = DummyOperator(
    task_id='end_historical_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag
)

# 🎯 ENHANCED TASK DEPENDENCIES with Dynamic Scope
start_historical >> determine_scope
determine_scope >> validate_scope
validate_scope >> historical_bronze

historical_bronze >> historical_silver
historical_silver >> historical_gold
historical_gold >> data_validation

data_validation >> daily_report
daily_report >> archival_check

archival_check >> end_historical