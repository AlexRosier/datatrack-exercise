from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from pendulum import datetime as dt

datatrack_dag = DAG(
    dag_id="alex-datatrack",
    description="Datatrack dag for Alex",
    default_args={"owner": "Alex Rosier"},
    schedule_interval="0 3 * * *",
    start_date=dt(2023, 11, 24)
)

with datatrack_dag:
    ingestion = BatchOperator(
        task_id="trigger_batch",
        job_name="alex-datatrack-ingestion",
        job_queue="integrated-exercise-job-queue",
        job_definition="alex-datatrack",
        region_name="eu-west-1",
        overrides={"command": ["python3", "datatrack_orchestrator.py", "--date", "{{ ds }}"]},
    )

ingestion