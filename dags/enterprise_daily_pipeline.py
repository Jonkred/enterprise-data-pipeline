"""Apache Airflow DAG for orchestrating the enterprise data pipeline."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _send_success_notification(**context) -> None:
    """Simulate a success notification for demonstration."""
    dag_run = context["dag_run"]
    print(
        f"Pipeline finished successfully. dag_id={dag_run.dag_id} "
        f"run_id={dag_run.run_id}"
    )


with DAG(
    dag_id="enterprise_daily_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for ERP, CRM, and IoT data sources",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["production", "etl", "enterprise"],
    max_active_runs=1,
) as dag:
    check_erp_files = BashOperator(
        task_id="check_erp_files",
        bash_command="ls /opt/airflow/data/sources/erp/*.parquet >/dev/null 2>&1",
    )

    validate_config = BashOperator(
        task_id="validate_config",
        bash_command="cd /opt/airflow && python -m pipeline.cli validate --config /opt/airflow/configs/pipeline.yaml",
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="cd /opt/airflow && python -m pipeline.cli run --config /opt/airflow/configs/pipeline.yaml",
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_send_success_notification,
    )

    check_erp_files >> validate_config >> run_pipeline >> notify_success
