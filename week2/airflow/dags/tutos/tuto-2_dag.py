import json
from datetime import datetime
from random import randrange 

from airflow.decorators import dag, task
import logging

TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ data_interval_start.strftime(\'%Y_%m\') }}'

@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
)
def tutorial_taskflow_api_etl():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    @task()
    def extract(table_name):
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        logging.info(f'Table Name: {table_name}')
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float, i: int, j:int):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f} : index: {i}/{j}")

    order_data = extract(TABLE_NAME_TEMPLATE)
    order_summary = transform(order_data)
    j = 2 #randrange(2, 2)
    for i in range(j):
        load(order_summary["total_order_value"], i, j)


tutorial_etl_dag = tutorial_taskflow_api_etl()
