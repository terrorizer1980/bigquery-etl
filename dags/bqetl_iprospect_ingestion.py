# Generated via https://github.com/mozilla/bigquery-etl/blob/master/bigquery_etl/query_scheduling/generate_airflow_dags.py

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from utils.gcp import bigquery_etl_query, gke_command

docs = """
### bqetl_iprospect_ingestion

Built from bigquery-etl repo, [`dags/bqetl_iprospect_ingestion.py`](https://github.com/mozilla/bigquery-etl/blob/master/dags/bqetl_iprospect_ingestion.py)

#### Description

Load iProspect data into bigquery
#### Owner

bewu@mozilla.com
"""


default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2021, 3, 22, 0, 0),
    "end_date": None,
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

with DAG(
    "bqetl_iprospect_ingestion",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=docs,
) as dag:

    fetch__summary__v1 = gke_command(
        task_id="fetch__summary__v1",
        command=[
            "python",
            "sql/moz-fx-data-marketing-prod/fetch/summary_v1/query.py",
        ]
        + ["--bucket-name moz-fx-data-marketing-prod-iprospect"],
        docker_image="mozilla/bigquery-etl:latest",
        owner="bewu@mozilla.com",
        email=["bewu@mozilla.com"],
    )
