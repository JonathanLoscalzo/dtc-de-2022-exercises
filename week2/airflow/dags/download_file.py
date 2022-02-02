import logging
import requests
import os

from airflow.decorators import task
from airflow.exceptions import AirflowFailException

@task()
def download_file(url_template, output_file_template):
    logging.info(f"{url_template}, {output_file_template}")

    if not os.path.exists(output_file_template):
        r = requests.get(url_template)

        if r.status_code == 200:
            open(output_file_template, "wb").write(r.content)
        else:
            raise AirflowFailException(f"Error on that url: {r.reason}")
    else: 
        print(f"The file {output_file_template} exist! ")