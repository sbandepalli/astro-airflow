import json
import requests
import os, pendulum, logging, pandas as pd
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
#from include.files import etl_functions #not being used
from datetime import datetime
from tabulate import tabulate
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# This is a sample getting started Airflow DAG.
# Hit a public API endpoint
# Save the information into a local file
# Push the file from local to AWS S3 bucket
# Push the information from the file in S3 bucket into a snowflake table
# Remember to define your AWS and Snowflake connections

S3_BUCKET = os.environ.get("S3_BUCKET", "astronomer-field-engineering-demo")
S3_KEY = os.environ.get("S3_KEY", "users.csv")

SNOWFLAKE_CONN_ID = 'snow_FE'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_STAGE = 'my_s3_stage'
SNOWFLAKE_WAREHOUSE = 'ROBOTS'
SNOWFLAKE_DATABASE = 'DEMO'
SNOWFLAKE_ROLE = 'FIELDENGINEER'
SNOWFLAKE_SAMPLE_TABLE = 'API_TEST'
S3_FILE_PATH = 'users.csv'

def extract_users(url: str, ti) -> None:
    res = requests.get(url)
    json_data = json.loads(res.content)
    ti.xcom_push(key='extracted_users', value=json_data)

def transform_users(ti) -> None:
    users = ti.xcom_pull(key='extracted_users', task_ids=['extract_users'])[0]
    transformed_users = []
    for user in users:
        transformed_users.append({
            'ID': user['id'],
            'Name': user['name'],
            'Username': user['username'],
            'Email': user['email'],
            'Address': f"{user['address']['street']} {user['address']['suite']} {user['address']['city']}",
            'PhoneNumber': user['phone'],
            'Company': user['company']['name']
        })
    ti.xcom_push(key='transformed_users', value=transformed_users)
    print(tabulate(transformed_users))

def load_users(path: str, ti) -> None:
    users = ti.xcom_pull(key='transformed_users', task_ids=['transform_users'])
    users_df = pd.DataFrame(users[0])
    users_df.to_csv(path, index=None)

def printoutput(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snow_FE")
    engine=dwh_hook.get_sqlalchemy_engine()
    result=pd.read_sql("""select * from DEMO.PUBLIC.API_TEST limit 100; """, engine,)
    print(tabulate(result))

default_args = {
    'start_date': datetime(year=2021, month=5, day=12),
    'snowflake_conn_id': SNOWFLAKE_CONN_ID,
}

CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (ID VARCHAR(25), Name VARCHAR(250), Username varchar(250), email VARCHAR(250), Address varchar(250), PhoneNumber VARCHAR(100), Company varchar(250));"
)

with DAG(
    dag_id='etl_users',
    max_active_runs=1,
    default_args=default_args,
    schedule_interval='@daily',
    description='ETL pipeline for processing users',
    catchup=False,
) as dag:

    # Task 1 - Fetch user data from the API
    task_extract_users = PythonOperator(
        task_id='extract_users',
        python_callable=extract_users,
        op_kwargs={'url': 'https://jsonplaceholder.typicode.com/users'}
    )

    # Task 2 - Transform fetched users
    task_transform_users = PythonOperator(
        task_id='transform_users',
        python_callable=transform_users
    )

    # Task 3 - Save users to CSV
    task_load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_users,
        op_kwargs={'path': '/usr/local/airflow/data/users.csv'}
    )

    #Task 4 - Load local CSV file to S3 bucket
    create_local_to_s3_job = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename="/usr/local/airflow/data/users.csv",
        dest_key=S3_KEY,
        dest_bucket=S3_BUCKET,
        replace=True,
    )

    #Task 5 - Create Snowflake table
    Create_Snowflake_table = SnowflakeOperator(
    task_id='Create_Snowflake_table',
    dag=dag,
    sql=CREATE_TABLE_SQL_STRING,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
    )

    # Task 6 - Load S3 bucket file to snowflake
    Load_S3_file_Snowflake = S3ToSnowflakeOperator(
    task_id='Load_S3_file_Snowflake',
    s3_keys=[S3_FILE_PATH],
    table=SNOWFLAKE_SAMPLE_TABLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    schema=SNOWFLAKE_SCHEMA,
    stage=SNOWFLAKE_STAGE,
    file_format="(type = 'CSV', field_delimiter = ',')",
    )
    
    snowflake_get_data = PythonOperator(
    task_id='get_data',
    python_callable=printoutput,
    )

    task_extract_users >> task_transform_users >> task_load_users >> create_local_to_s3_job
    create_local_to_s3_job >> Create_Snowflake_table >> Load_S3_file_Snowflake >> snowflake_get_data
