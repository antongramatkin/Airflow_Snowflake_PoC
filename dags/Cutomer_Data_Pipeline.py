from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

from datetime import datetime, timedelta
import json
import pandas
import uuid


default_args = {
    'start_date':datetime(2021,3,1),
    'retries':3,
    'retry_delay':3 
}

loadId = uuid.uuid4()

## SQL script for table in the STAGING area
_sql_create_table = '''CREATE OR REPLACE TABLE "AIRFLOW_DBT_POC"."STAGING"."users"(
                                                firstname NVARCHAR(255)
                                                ,lastname NVARCHAR(255)
                                                ,country NVARCHAR(255)
                                                ,username NVARCHAR(255)
                                                ,password NVARCHAR(255)
                                                ,email NVARCHAR(255)
                                                ,loadid NVARCHAR(255)
                                            );'''

## processing users JSON to SQL statements
def _user_processing(ti):
    ##list of tasks with customer JSOM in XCOMs  
    sources = [ 'get-user-from-own-ERP', 
                'get-user-from-own-CRM', 
                'get-user-from-Filiale1-ERP', 
                'get-user-from-Filiale1-CRM',
                'get-user-from-Filiale2-ERP', 
                'get-user-from-Filiale2-CRM']

    insert_users_sql = '''
                        INSERT INTO "AIRFLOW_DBT_POC"."STAGING"."users" (
                        firstname,
                        lastname,
                        country,
                        username,
                        password,
                        email,
                        loadid
                        )
                        VALUES '''

    for src in sources:
        ## getting the JSON from XCOM
        users = ti.xcom_pull(task_ids=[src])

        ## check if len != 0 and first memeber or incoming variable is "results"
        if not len(users) or 'results' not in users[0]:
            raise ValueError('User is empty')

        user = users[0]['results'][0]    
        processed_user = pandas.json_normalize({
            'firstname':user['name']['first'],
            'lastname':user['name']['last'],
            'country':user['location']['country'],
            'username':user['login']['username'],
            'password':user['login']['password'],
            'email' : user['email'],
            'loadid': loadId
        })
        insert_users_sql += '''
                        (
                            '{0}',
                            '{1}',
                            '{2}',
                            '{3}',
                            '{4}',
                            '{5}',
                            '{6}'
                        ),'''.format(processed_user['firstname'][0], 
                                        processed_user['lastname'][0], 
                                        processed_user['country'][0], 
                                        processed_user['username'][0], 
                                        processed_user['password'][0], 
                                        processed_user['email'][0],
                                        processed_user['loadid'][0]
                                        )
    insert_users_sql = insert_users_sql[:-1] + ';'  

    ## put the created SQL statement to the XCOM                                  
    return(insert_users_sql)


 ## upload user data to Snowflake       
def _upload_users_to_snowflake(ti):
    ## get the SQL statement for all Customers from XCOM
    insert_sql = ti.xcom_pull(task_ids=['process-users'])

    snowflake_ins = SnowflakeOperator(
            task_id = 'execute-sql-on-snowflake',
            snowflake_conn_id='db_snowflake',
            sql=insert_sql,
            warehouse='LOAD_WH',
            database='AIRFLOW_DBT_POC',
            schema='STAGING'            
        )
    snowflake_ins.execute(context=ti)

## run dbt model
def _run_dbt_model():
    ssh = SSHHook(ssh_conn_id='dbt_ssh')
    ssh_client = None
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        ssh_client.exec_command('echo "Hello World!"')
    finally:
        if ssh_client:
            ssh_client.close()


with DAG('User_Data_Pipeline',
            schedule_interval=timedelta(minutes=60),
            default_args=default_args,
            catchup=False) as DAG:
    
    ## create a table in the STAGING area
    create_table = SnowflakeOperator(task_id='create-table',
                                        snowflake_conn_id='db_snowflake',
                                        sql=_sql_create_table,
                                        warehouse='LOAD_WH',
                                        database='AIRFLOW_DBT_POC',
                                        schema='STAGING' 
                                        )

    ## get data from the source 1
    get_user_from_own_ERP = SimpleHttpOperator(
        task_id='get-user-from-own-ERP',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
        retries=3,
        retry_delay=3 
    )

    ## get data from the source 2
    get_user_from_own_CRM = SimpleHttpOperator(
        task_id='get-user-from-own-CRM',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True 
    )

    ## get data from the source 3
    get_user_from_Filiale1_ERP = SimpleHttpOperator(
        task_id='get-user-from-Filiale1-ERP',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True 
    )

    ## get data from the source 4
    get_user_from_Filiale1_CRM = SimpleHttpOperator(
        task_id='get-user-from-Filiale1-CRM',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True 
    )

    ## get data from the source 5
    get_user_from_Filiale2_ERP = SimpleHttpOperator(
        task_id='get-user-from-Filiale2-ERP',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True 
    )

    ## get data from the source 6
    get_user_from_Filiale2_CRM = SimpleHttpOperator(
        task_id='get-user-from-Filiale2-CRM',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True 
    )

    ## collect data and create SQL statement for Snowflake
    process_users=PythonOperator(
        task_id='process-users',
        python_callable=_user_processing,
        provide_context=True
    )

    ## run SQL on Snowflake
    upload_users_to_snowflake_stg=PythonOperator(
        task_id='upload-users-to-snowflake',
        python_callable=_upload_users_to_snowflake,
        provide_context=True
    )

    ## get latest dbt Model changes from Git
    fetch_dbt_model = SSHOperator(
        ssh_conn_id='dbt_ssh',
        task_id='fetch-DBT-Model',
        command='cd C:\\DBT\\dbt-snowflake-poc\\models && git pull'
    )

    ## run dbt model DV_Core
    run_dbt_model = SSHOperator(
        ssh_conn_id='dbt_ssh',
        task_id='run-DBT-Model',
        command='cd C:\\DBT\\dbt-snowflake-poc\\models && dbt run -m DV_Core'
    )

    ## run dbt scripts for SATs
    run_dbt_snapshot = SSHOperator(
        ssh_conn_id='dbt_ssh',
        task_id='run-DBT-Snapshot',
        command='cd C:\\DBT\\dbt-snowflake-poc\\models && dbt snapshot --select tag:sattelite'
    )


    create_table >>[get_user_from_own_ERP, get_user_from_own_CRM, get_user_from_Filiale1_ERP, get_user_from_Filiale1_CRM, get_user_from_Filiale2_ERP, get_user_from_Filiale2_CRM] >> process_users 
    process_users >> upload_users_to_snowflake_stg >> fetch_dbt_model >> [run_dbt_model, run_dbt_snapshot]