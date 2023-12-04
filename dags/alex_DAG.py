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
        job_name="alex-datatrack-ingest",
        job_queue="integrated-exercise-job-queue",
        job_definition="alex-datatrack-ingest",
        region_name="eu-west-1",
        overrides={"command": ["python3", "ingest.py", "--date", "{{ ds }}"]},
    )
    cleaning = BatchOperator(
        task_id="trigger_cleaning",
        job_name="alex-datatrack-clean",
        job_queue="integrated-exercise-job-queue",
        job_definition="alex-datatrack-clean",
        region_name="eu-west-1",
        overrides={"command": ["python3", "spark_clean.py", "--date", "{{ ds }}", "--bucket_path", "s3a://data-track-integrated-exercise/alex-data"]},
    )
    aggregation = BatchOperator(
        task_id="trigger_aggregation",
        job_name="alex-datatrack-spark_aggregate",
        job_queue="integrated-exercise-job-queue",
        job_definition="alex-datatrack-spark_aggregate",
        region_name="eu-west-1",
        overrides={"command": ["python3", "spark_aggregate.py", "--date", "{{ ds }}", "--bucket_path", "s3a://data-track-integrated-exercise/alex-data"]},
    )


ingestion >> cleaning >> aggregation