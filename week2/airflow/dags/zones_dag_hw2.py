from datetime import datetime
import os

from airflow.decorators import dag
from airflow.utils.dates import days_ago

from download_file import download_file
from ingest_data import ingest_data

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow") + "/data"

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")


URL_TEMPLATE = (
    "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
)
OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/output_{{ data_interval_start.strftime('%Y-%m') }}_zones.csv"
)
TABLE_NAME_TEMPLATE = "zones"


@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=dict(
    ),
    dagrun_timeout=None
)
def ingest_zones():
    download_file(URL_TEMPLATE, OUTPUT_FILE_TEMPLATE) >> ingest_data(
        PG_USER,
        PG_PASSWORD,
        PG_HOST,
        PG_PORT,
        PG_DATABASE,
        TABLE_NAME_TEMPLATE,
        OUTPUT_FILE_TEMPLATE,
    )


dag_run = ingest_zones()
