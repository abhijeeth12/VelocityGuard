from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fraud_dbt_dag',
    default_args=default_args,
    description='A simple DAG to run dbt daily_fraud_summary model',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt', 'fraud'],
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt_models',
        # Airflow container will need access to dbt, but we are running in python venv locally? 
        # Wait, the instruction says "Orchestration: Write an Airflow DAG file named fraud_dbt_dag.py that schedules the dbt run to execute daily."
        # If we use Docker Compose for Airflow, dbt is NOT installed inside the airflow image unless we build a custom one.
        # But wait! I created `requirements.txt` with dbt, pyspark, kafka-python.
        # The user said "create a requirements.txt... install dependencies".
        # Then "Execute docker compose up -d... Airflow instance".
        # This implies either Airflow is run locally via the venv, OR the docker container runs bash operator.
        # If it's a bash operator in the container, it won't have dbt.
        # Let me just run dbt using BashOperator executing via docker exec? Or maybe airflow is meant to be run locally in the venv without docker?
        # The prompt says: "docker-compose.yml that provisions Kafka, Zookeeper, a PostgreSQL database (port 5432), and a local Airflow instance. Execute docker compose up -d"
        # So Airflow IS in docker. Let's make the bash operator just run a command. If dbt is not in the image, we can run it via the local host if we map the venv, or we can just mock it, or install dbt in the airflow container.
        # A simple `pip install dbt-postgres && dbt run --profiles-dir .` in the bash operator works perfectly to self-bootstrap in the container.
        bash_command='cd /opt/airflow/fraud_models && pip install dbt-postgres~=1.9.0 dbt-core~=1.9.0 mashumaro==3.17 && dbt run --profiles-dir .',
    )

    run_dbt
