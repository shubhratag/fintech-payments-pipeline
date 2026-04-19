from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# ──────────────────────────────────────────
# This DAG requires:
# 1. Apache Airflow with databricks provider installed
# 2. A Databricks workspace with Jobs configured
# 3. Airflow connection 'databricks_default' set up
#    with your Databricks host and token
#
# In production this runs on a managed Airflow instance
# (AWS MWAA, Google Cloud Composer, or Astronomer)
# ──────────────────────────────────────────

default_args = {
    'owner'           : 'shubhrata',
    'retries'         : 3,
    'retry_delay'     : timedelta(minutes=5),
    'email_on_failure': True,
    'email'           : ['shubhrata@email.com'],
    'email_on_retry'  : False,
    'sla'             : timedelta(minutes=10),
}

dag = DAG(
    dag_id            = 'fintech_payments_pipeline',
    description       = 'End to end real time payments pipeline Bronze Silver Gold',
    default_args      = default_args,
    start_date        = datetime(2024, 1, 1),
    schedule_interval = '0 * * * *',
    catchup           = False,
    tags              = ['fintech', 'payments', 'delta-lake', 'databricks'],
)

# ──────────────────────────────────────────
# Tasks
# Each DatabricksRunNowOperator:
# 1. Connects to Databricks workspace
# 2. Triggers the specified job by ID
# 3. Waits for completion
# 4. Fails if the job fails — blocking downstream tasks
# ──────────────────────────────────────────

start = EmptyOperator(
    task_id = 'start_pipeline',
    dag     = dag
)

# Triggers Bronze notebook job on Databricks
# Reads from Kafka → writes raw to bronze_payments Delta table
bronze_task = DatabricksRunNowOperator(
    task_id             = 'ingest_to_bronze',
    databricks_conn_id  = 'databricks_default',
    job_id              = 1001,
    dag                 = dag
)

# Triggers Silver notebook job on Databricks
# Reads bronze_payments → deduplicates → writes silver_payments
silver_task = DatabricksRunNowOperator(
    task_id             = 'transform_to_silver',
    databricks_conn_id  = 'databricks_default',
    job_id              = 1002,
    dag                 = dag
)

# Triggers Gold notebook job on Databricks
# Reads silver_payments → aggregates KPIs → writes gold_payments
gold_task = DatabricksRunNowOperator(
    task_id             = 'aggregate_to_gold',
    databricks_conn_id  = 'databricks_default',
    job_id              = 1003,
    dag                 = dag
)

end = EmptyOperator(
    task_id = 'end_pipeline',
    dag     = dag
)

# ──────────────────────────────────────────
# Pipeline order
# Bronze must succeed before Silver starts
# Silver must succeed before Gold starts
# ──────────────────────────────────────────
start >> bronze_task >> silver_task >> gold_task >> end