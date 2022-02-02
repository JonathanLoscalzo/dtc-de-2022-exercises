

import os
from airflow.decorators import task

@task()
def remove_file(output_file_template):
    if os.path.exists(output_file_template):
        os.remove(output_file_template)
    else:
        print(f"The file {output_file_template} does not exist")