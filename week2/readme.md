

# INFORMATION

Some environment variables are being loaded with docker-compose directly (.env file), but others are being loaded with direnv OUTSIDE THIS REPO due to privacy reasons.

These variables are (.envrc): 
- GOOGLE_APPLICATION_CREDENTIALS
- GCP_PROJECT_ID
- GCP_GCS_BUCKET
- AIRFLOW_HOME

.env file should have: 
- AIRFLOW_UID
- PG_HOST
- PG_PORT
- PG_USER
- PG_PASSWORD
- PG_DATABASE

I am using the taskflow api, because why not (?).


## Enhance (?)

1 - dags are being stateful due to the case when they share files between them. 
It is important to understand that if we save information on a worker, another worker should try to resolve the downstream task and the file downloaded should not be there.

2 - use default_args, variables and others, avoiding use environ directly.
I don't know how to use default_args from a dag, and pass it to a task.

3 - create an environment work machine with terraform on GCP, because the scripts ARE VERY BIG, and internet connection could cause some issues trying to upload files to an storage/bucket.
