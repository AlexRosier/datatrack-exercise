from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
# from airflow.operators.email import EmailOperator
# from airflow.utils.email import send_email
from pendulum import datetime as dt

# def send_status_email(context):
#     task_instance = context['task_instance']
#     task_status = task_instance.current_state()
#
#     subject = f"Airflow Task {task_instance.task_id} {task_status}"
#     body = f"The task {task_instance.task_id} finished with status: {task_status}.\n\n" \
#            f"Task execution date: {context['execution_date']}\n" \
#            f"Log URL: {task_instance.log_url}\n\n"
#
#     to_email = "alex.rosier@axxes.com"
#
#     send_email(to=to_email, subject=subject, html_content=body)

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
    # email_ingestion = EmailOperator(
    #     task_id='ingestion_mail',
    #     to='alex.rosier@axxes.com',
    #     subject='Ingestion failed',
    #     html_content='The datatrack ingestion job failed',
    #     on_failure_callback=send_status_email,
    #     trigger_rule="all_done"
    # )
    cleaning = BatchOperator(
        task_id="trigger_cleaning",
        job_name="alex-datatrack-clean",
        job_queue="integrated-exercise-job-queue",
        job_definition="alex-datatrack-clean",
        region_name="eu-west-1",
        overrides={"command": ["python3", "spark_clean.py", "--date", "{{ ds }}", "--bucket_path", "s3a://data-track-integrated-exercise/alex-data"]},
    )
    # email_cleaning = EmailOperator(
    #     task_id='cleaning_mail',
    #     to='alex.rosier@axxes.com',
    #     subject='Cleaning failed',
    #     html_content='The datatrack cleaning job failed',
    #     on_failure_callback=send_status_email,
    #     trigger_rule="all_done"
    # )
    aggregation = BatchOperator(
        task_id="trigger_aggregation",
        job_name="alex-datatrack-spark_aggregate",
        job_queue="integrated-exercise-job-queue",
        job_definition="alex-datatrack-spark_aggregate",
        region_name="eu-west-1",
        overrides={"command": ["python3", "spark_aggregate.py", "--date", "{{ ds }}", "--bucket_path", "s3a://data-track-integrated-exercise/alex-data"]},
    )
    # email_aggregation = EmailOperator(
    #     task_id='aggregation_mail',
    #     to='alex.rosier@axxes.com',
    #     subject='Aggregation failed',
    #     html_content='The datatrack cleaning job failed',
    #     on_failure_callback=send_status_email,
    #     trigger_rule="all_done"
    # )


ingestion >> cleaning >> aggregation