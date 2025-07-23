"""
Stock Sentiment Pipeline Monitoring DAG
Runs every 10 minutes to monitor pipeline health and send alerts
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable, DagRun
from airflow.utils.state import State
import logging
import json

# Slack functionality removed - using log-based alerting only

# Monitoring DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 21),
    'email_on_failure': False,  # We handle our own alerting
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'stock_sentiment_monitoring',
    default_args=default_args,
    description='Monitoring and alerting for stock sentiment pipeline',
    schedule='*/10 * * * *',  # Every 10 minutes
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'alerts', 'sla', 'health']
)

def check_pipeline_health(**context):
    """Check health of both realtime and historical pipelines"""
    
    logging.info("🔍 Checking pipeline health...")
    
    # Initialize health metrics
    health_metrics = {
        'timestamp': datetime.now().isoformat(),
        'realtime_pipeline': {
            'success_rate': 0,
            'avg_duration_minutes': 0,
            'recent_runs': [],
            'failed_runs': []
        },
        'historical_pipeline': {
            'success_rate': 0,
            'avg_duration_minutes': 0,
            'recent_runs': [],
            'failed_runs': []
        },
        'overall_health_score': 0,
        'alerts': []
    }
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check realtime pipeline (last 4 hours)
        realtime_query = """
            SELECT 
                run_id,
                state,
                execution_date,
                start_date,
                end_date,
                EXTRACT(EPOCH FROM (end_date - start_date))/60 as duration_minutes
            FROM dag_run 
            WHERE dag_id = 'stock_sentiment_realtime' 
            AND execution_date >= NOW() - INTERVAL '4 HOURS'
            ORDER BY execution_date DESC
            LIMIT 20
        """
        
        realtime_results = postgres_hook.get_records(realtime_query)
        
        if realtime_results:
            successful_runs = [r for r in realtime_results if r[1] == 'success']
            failed_runs = [r for r in realtime_results if r[1] in ['failed', 'up_for_retry']]
            
            health_metrics['realtime_pipeline'] = {
                'success_rate': len(successful_runs) / len(realtime_results),
                'avg_duration_minutes': sum([r[5] for r in successful_runs if r[5]]) / len(successful_runs) if successful_runs else 0,
                'recent_runs': len(realtime_results),
                'failed_runs': len(failed_runs)
            }
        
        # Check historical pipeline (last 7 days)
        historical_query = """
            SELECT 
                run_id,
                state,
                execution_date,
                start_date,
                end_date,
                EXTRACT(EPOCH FROM (end_date - start_date))/60 as duration_minutes
            FROM dag_run 
            WHERE dag_id = 'stock_sentiment_historical' 
            AND execution_date >= NOW() - INTERVAL '7 DAYS'
            ORDER BY execution_date DESC
            LIMIT 10
        """
        
        historical_results = postgres_hook.get_records(historical_query)
        
        if historical_results:
            successful_runs = [r for r in historical_results if r[1] == 'success']
            failed_runs = [r for r in historical_results if r[1] in ['failed', 'up_for_retry']]
            
            health_metrics['historical_pipeline'] = {
                'success_rate': len(successful_runs) / len(historical_results),
                'avg_duration_minutes': sum([r[5] for r in successful_runs if r[5]]) / len(successful_runs) if successful_runs else 0,
                'recent_runs': len(historical_results),
                'failed_runs': len(failed_runs)
            }
        
        # Calculate overall health score (0-100)
        health_score = 100
        
        # Realtime pipeline health (50% weight)
        rt_success = health_metrics['realtime_pipeline']['success_rate']
        if rt_success < 0.8:
            health_score -= (0.8 - rt_success) * 50
        
        # Historical pipeline health (30% weight)
        hist_success = health_metrics['historical_pipeline']['success_rate']
        if hist_success < 0.9:
            health_score -= (0.9 - hist_success) * 30
        
        # Recent failures (20% weight)
        total_recent_failures = (health_metrics['realtime_pipeline']['failed_runs'] + 
                               health_metrics['historical_pipeline']['failed_runs'])
        if total_recent_failures > 0:
            health_score -= min(total_recent_failures * 10, 20)
        
        health_metrics['overall_health_score'] = max(0, health_score)
        
        # Generate alerts
        if health_metrics['overall_health_score'] < 70:
            health_metrics['alerts'].append('🔴 CRITICAL: Pipeline health below 70%')
        elif health_metrics['overall_health_score'] < 85:
            health_metrics['alerts'].append('🟡 WARNING: Pipeline health degraded')
        
        if health_metrics['realtime_pipeline']['failed_runs'] > 2:
            health_metrics['alerts'].append(f"⚠️ Multiple realtime failures: {health_metrics['realtime_pipeline']['failed_runs']}")
        
        if health_metrics['historical_pipeline']['failed_runs'] > 0:
            health_metrics['alerts'].append(f"⚠️ Historical pipeline failures: {health_metrics['historical_pipeline']['failed_runs']}")
        
    except Exception as e:
        logging.error(f"❌ Error checking pipeline health: {e}")
        health_metrics['alerts'].append(f"❌ Health check error: {str(e)}")
        health_metrics['overall_health_score'] = 0
    
    logging.info(f"📊 Health metrics: {health_metrics}")
    return health_metrics

def check_data_freshness(**context):
    """Check data freshness across all layers"""
    
    logging.info("📅 Checking data freshness...")
    
    freshness_metrics = {
        'timestamp': datetime.now().isoformat(),
        'alerts': [],
        'layers': {
            'bronze': {'hours_since_update': 0, 'status': 'unknown'},
            'silver': {'hours_since_update': 0, 'status': 'unknown'},
            'gold': {'hours_since_update': 0, 'status': 'unknown'}
        }
    }
    
    # Check when each layer was last updated
    # This relies on variables set by the pipeline DAGs
    try:
        # Check last successful runs
        current_time = datetime.now()
        
        # Get last successful realtime run
        last_realtime = Variable.get('last_realtime_success', default_var=None)
        if last_realtime:
            last_time = datetime.fromisoformat(last_realtime)
            hours_diff = (current_time - last_time).total_seconds() / 3600
            freshness_metrics['layers']['bronze']['hours_since_update'] = hours_diff
            freshness_metrics['layers']['silver']['hours_since_update'] = hours_diff
            
            if hours_diff > 2:  # Data older than 2 hours during business hours
                freshness_metrics['alerts'].append(f"⏰ Bronze/Silver data is {hours_diff:.1f} hours old")
        
        # Check last successful historical run
        last_historical = Variable.get('last_historical_success', default_var=None)
        if last_historical:
            last_time = datetime.fromisoformat(last_historical)
            hours_diff = (current_time - last_time).total_seconds() / 3600
            freshness_metrics['layers']['gold']['hours_since_update'] = hours_diff
            
            if hours_diff > 24:  # Historical data older than 24 hours
                freshness_metrics['alerts'].append(f"⏰ Gold layer data is {hours_diff:.1f} hours old")
        
        # Set status based on freshness
        for layer, metrics in freshness_metrics['layers'].items():
            hours = metrics['hours_since_update']
            if hours == 0:
                metrics['status'] = 'unknown'
            elif hours < 1:
                metrics['status'] = 'fresh'
            elif hours < 4:
                metrics['status'] = 'acceptable'
            else:
                metrics['status'] = 'stale'
        
    except Exception as e:
        logging.error(f"❌ Error checking data freshness: {e}")
        freshness_metrics['alerts'].append(f"❌ Freshness check error: {str(e)}")
    
    logging.info(f"📅 Freshness metrics: {freshness_metrics}")
    return freshness_metrics

def check_resource_usage(**context):
    """Monitor Databricks cluster usage and costs"""
    
    logging.info("💰 Checking resource usage...")
    
    resource_metrics = {
        'timestamp': datetime.now().isoformat(),
        'alerts': [],
        'clusters': {
            'realtime_cluster': {'status': 'unknown', 'cost_estimate': 0},
            'analytics_cluster': {'status': 'unknown', 'cost_estimate': 0}
        },
        'daily_cost_estimate': 0
    }
    
    try:
        # This would typically call Databricks API to check cluster status
        # For now, we'll use placeholder logic
        
        # Check if clusters are running when they shouldn't be
        current_hour = datetime.now().hour
        
        # Realtime cluster should only run during market hours
        if current_hour < 9 or current_hour > 16:
            resource_metrics['alerts'].append("💸 Realtime cluster may be running outside market hours")
        
        # Check for long-running historical jobs
        # This would query Databricks jobs API
        
        resource_metrics['daily_cost_estimate'] = 150  # Placeholder
        
        if resource_metrics['daily_cost_estimate'] > 200:
            resource_metrics['alerts'].append(f"💸 High daily cost estimate: ${resource_metrics['daily_cost_estimate']}")
        
    except Exception as e:
        logging.error(f"❌ Error checking resource usage: {e}")
        resource_metrics['alerts'].append(f"❌ Resource check error: {str(e)}")
    
    logging.info(f"💰 Resource metrics: {resource_metrics}")
    return resource_metrics

def generate_monitoring_report(**context):
    """Generate comprehensive monitoring report"""
    
    # Get results from previous tasks
    health_metrics = context['task_instance'].xcom_pull(task_ids='check_pipeline_health')
    freshness_metrics = context['task_instance'].xcom_pull(task_ids='check_data_freshness')
    resource_metrics = context['task_instance'].xcom_pull(task_ids='check_resource_usage')
    
    # Combine all metrics
    monitoring_report = {
        'timestamp': datetime.now().isoformat(),
        'overall_status': 'HEALTHY',
        'health_score': health_metrics.get('overall_health_score', 0),
        'pipeline_health': health_metrics,
        'data_freshness': freshness_metrics,
        'resource_usage': resource_metrics,
        'all_alerts': [],
        'action_required': False
    }
    
    # Collect all alerts
    all_alerts = []
    all_alerts.extend(health_metrics.get('alerts', []))
    all_alerts.extend(freshness_metrics.get('alerts', []))
    all_alerts.extend(resource_metrics.get('alerts', []))
    
    monitoring_report['all_alerts'] = all_alerts
    
    # Determine overall status
    if monitoring_report['health_score'] < 70 or len(all_alerts) > 3:
        monitoring_report['overall_status'] = 'CRITICAL'
        monitoring_report['action_required'] = True
    elif monitoring_report['health_score'] < 85 or len(all_alerts) > 1:
        monitoring_report['overall_status'] = 'WARNING'
        monitoring_report['action_required'] = True
    
    # Store for dashboard and alerting
    Variable.set('monitoring_report_latest', json.dumps(monitoring_report))
    
    logging.info(f"📋 Monitoring report: {monitoring_report}")
    return monitoring_report

def should_send_alert(**context):
    """Determine if we should send an alert"""
    
    monitoring_report = context['task_instance'].xcom_pull(task_ids='generate_monitoring_report')
    
    # Only send alerts for critical issues or significant degradation
    if (monitoring_report['overall_status'] in ['CRITICAL', 'WARNING'] and 
        monitoring_report['action_required']):
        
        return 'log_alert'
    else:
        logging.info("✅ No alerts needed - system healthy")
        return 'skip_alert'

def log_alert_message(**context):
    """Log alert message to Airflow logs"""
    
    monitoring_report = context['task_instance'].xcom_pull(task_ids='generate_monitoring_report')
    
    status = monitoring_report['overall_status']
    health_score = monitoring_report['health_score']
    alerts = monitoring_report['all_alerts']
    
    alert_message = f"""
    🚨 STOCK SENTIMENT PIPELINE ALERT 🚨
    
    Status: {status}
    Health Score: {health_score:.0f}/100
    Active Alerts: {len(alerts)}
    
    Realtime Success Rate: {monitoring_report['pipeline_health']['realtime_pipeline']['success_rate']:.1%}
    Historical Success Rate: {monitoring_report['pipeline_health']['historical_pipeline']['success_rate']:.1%}
    
    Alerts:
    """ + "\n    ".join([f"• {alert}" for alert in alerts[:5]])
    
    logging.error(alert_message)
    print(alert_message)  # Also print to stdout
    
    return alert_message

# Define monitoring tasks
start_monitoring = EmptyOperator(
    task_id='start_monitoring',
    dag=dag
)

health_check = PythonOperator(
    task_id='check_pipeline_health',
    python_callable=check_pipeline_health,
    dag=dag
)

freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

resource_check = PythonOperator(
    task_id='check_resource_usage',
    python_callable=check_resource_usage,
    dag=dag
)

monitoring_report = PythonOperator(
    task_id='generate_monitoring_report',
    python_callable=generate_monitoring_report,
    dag=dag
)

alert_decision = BranchPythonOperator(
    task_id='should_send_alert',
    python_callable=should_send_alert,
    dag=dag
)

# Conditional alerting - log based only
log_alert = PythonOperator(
    task_id='log_alert',
    python_callable=log_alert_message,
    dag=dag
)

skip_alert = EmptyOperator(
    task_id='skip_alert',
    dag=dag
)

# System health collection
collect_system_metrics = BashOperator(
    task_id='collect_system_metrics',
    bash_command="""
    echo "=== System Metrics $(date) ===" > /tmp/airflow_system_metrics.log
    echo "Disk Usage:" >> /tmp/airflow_system_metrics.log
    df -h / >> /tmp/airflow_system_metrics.log
    echo "Memory Usage:" >> /tmp/airflow_system_metrics.log
    free -h >> /tmp/airflow_system_metrics.log
    echo "Airflow Tasks:" >> /tmp/airflow_system_metrics.log
    ps aux | grep airflow | wc -l >> /tmp/airflow_system_metrics.log
    echo "=== End Metrics ===" >> /tmp/airflow_system_metrics.log
    """,
    dag=dag
)

end_monitoring = EmptyOperator(
    task_id='end_monitoring',
    dag=dag
)

# Task Dependencies
start_monitoring >> [health_check, freshness_check, resource_check, collect_system_metrics]

[health_check, freshness_check, resource_check] >> monitoring_report
monitoring_report >> alert_decision >> [log_alert, skip_alert]

[log_alert, skip_alert, collect_system_metrics] >> end_monitoring