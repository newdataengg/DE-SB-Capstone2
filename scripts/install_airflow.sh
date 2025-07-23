cd ~/Documents/Study/Washu-DE/DE-SB-Capstone2
export AIRFLOW_HOME=$(pwd)

# Install
pip install apache-airflow>=2.8.0 apache-airflow-providers-databricks>=6.6.0

# Setup
airflow db init
airflow db migrate
airflow users create --username admin --password admin123 --firstname Monika --lastname Sharma --role Admin --email monikaiitd@gmail.com

# Start (runs web server on your machine)
airflow webserver --port 8080 &
airflow scheduler &

echo "✅ Go to: http://localhost:8080"
echo "Login: admin / admin123"