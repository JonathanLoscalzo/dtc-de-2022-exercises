from datetime import datetime
import os

from airflow.decorators import dag

from download_file import download_file
from ingest_data import ingest_data

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow") + "/data"

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")


URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data"
URL_TEMPLATE = (
    URL_PREFIX + "/yellow_tripdata_{{ data_interval_start.strftime('%Y-%m') }}.csv"
)
OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/output_{{ data_interval_start.strftime('%Y-%m') }}.csv"
)
TABLE_NAME_TEMPLATE = "yellow_taxi_{{ data_interval_start.strftime('%Y_%m') }}"


@dag(
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    max_active_runs=3,
    catchup=True,
    default_args=dict(
        # user=PG_USER,
        # password=PG_PASSWORD,
        # host=PG_HOST,
        # port=PG_PORT,
        # db=PG_DATABASE,
        # table_name=TABLE_NAME_TEMPLATE,
        # csv_file=OUTPUT_FILE_TEMPLATE,
    ),
    dagrun_timeout=None,
)
def ingest_yellow_taxi_data():
    download_file(URL_TEMPLATE, OUTPUT_FILE_TEMPLATE) >> ingest_data(
        PG_USER,
        PG_PASSWORD,
        PG_HOST,
        PG_PORT,
        PG_DATABASE,
        TABLE_NAME_TEMPLATE,
        OUTPUT_FILE_TEMPLATE,
    )


dag_run = ingest_yellow_taxi_data()
