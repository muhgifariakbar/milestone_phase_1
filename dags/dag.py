import datetime as dt
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-engineering',
    'start_date': dt.datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# Runs once a day, early enough that Gold is ready before business hours.
SCHEDULE_CRON = '0 3 * * *'

with DAG(
    'retail_bronze_silver_gold_etl',
    default_args=default_args,
    schedule_interval=SCHEDULE_CRON,
    catchup=False,
    max_active_runs=1,
) as dag:

    project_root = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    run_id = '{{ run_id }}'

    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command=f'python -m pipelines.bronze.build_bronze --input data/raw --run-id "{run_id}"',
        cwd=project_root,
    )

    build_silver = BashOperator(
        task_id='build_silver',
        bash_command=(
            'python -m pipelines.run_sql_stage --stage build_silver '
            f'--sql pipelines/silver/build_silver.sql --run-id "{run_id}"'
        ),
        cwd=project_root,
    )

    build_gold = BashOperator(
        task_id='build_gold',
        bash_command=(
            'python -m pipelines.run_sql_stage --stage build_gold '
            f'--sql pipelines/gold/build_gold.sql --run-id "{run_id}"'
        ),
        cwd=project_root,
    )

load_bronze >> build_silver >> build_gold
