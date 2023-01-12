import json
import boto3
import sys
import datetime
from datetime import date
from datetime import datetime
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.python_operator import PythonVirtualenvOperator
import os
import glob
import logging
import numpy as np
import pandas as pd
import psycopg2
import configparser
from datetime import timedelta
import yaml
import subprocess
import shlex
import subprocess

# import datahub
# from airflow.operators.pipeline_operator import PipPackageOperator
# from airflow.operators.python_operator import PipPackageOperator

# from datahub.configuration.config_loader import load_config_file
# from datahub.ingestion.run.pipeline import Pipeline

# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator

today = date.today()
timestamp = datetime.now()
RunID = str(timestamp).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')


def read_config_file(config_dir):
    """
    Sourcing Configuration File
    """
    try:
        conf = configparser.RawConfigParser()
        conf.read(config_dir)
    except Exception as ex:
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")
    return conf


def postgre_connect(host, database, user, password):
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(host=host,
                                database=database,
                                user=user,
                                password=password)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn


# with PythonOperator
def print_hello():
    return 'HELLO WORLD!'


def create_dataframe(sql, conn):
    cursor = conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cols = []
    for elt in cursor.description:
        cols.append(elt[0])
    df = pd.DataFrame(data=data, columns=cols)
    cursor.close()
    return df


def up_yml(JSONDict):
    s3 = boto3.resource(
        's3',
        aws_access_key_id='AKIAVOZRE44NWUEQAAVF',
        aws_secret_access_key='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'
    )
    s3_bucket = s3.Bucket('yml-temp')
    for file in s3_bucket.objects.all():
        obj = s3.Object('yml-temp', file.key)
        body = obj.get()['Body'].read().decode('utf-8')
        configfile = yaml.safe_load(body)
        output_file_name = f"output.json"

        output_path = f"./metadata.json"

        source_config = configfile['source']['config']
        # source_config['username'] = JSONDict.get('NAME')
        source_config['username'] = "sayali"
        # source_config['password'] = 'JSONDict.get('password')'
        source_config['password'] = "Atgeir@03"
        # source_config['table_pattern']['allow'] = F".*{JSONDict.get('table')}"
        source_config['table_pattern']['allow'] = F".*MODEL_DETAILS"
        source_config['profile_pattern']['allow'] = F'{JSONDict.get("DATABASE")}.*.*'

        database_pattern = F'^{JSONDict.get("DATABASE")}$'
        source_config['database_pattern']['allow'] = database_pattern
        # source_config['provision_role']['admin_username'] = F'"{JSONDict.get("NAME")}"'
        source_config['provision_role']['admin_username'] = 'sayali'
        # source_config['provision_role']['admin_password'] = JSONDict.get(
        #     'password')
        source_config['provision_role']['admin_password'] = 'Atgeir@03'
        source_config['account_id'] = F'{JSONDict.get("ACCOUNT")}'
#         source_config['check_role_grants'] = 'false'
        source_config['provision_role']['run_ingestion'] = 'true'
        source_config['warehouse'] = F'{JSONDict.get("WAREHOUSE")}'
        source_config['role'] = F'{JSONDict.get("ROLE")}'
        configfile['sink']['config']['filename'] = output_path
        local_file = f'./datahub.yml'

        with open(local_file, 'w+') as f:
            yaml.safe_dump(configfile, f, default_flow_style=False)
            print("file loaded successfully", local_file)
            print(source_config)
        return local_file, output_path, output_file_name


def replace_yml(path):
    try:
        search_text, replace_text = 'allow:', 'allow: \n      - '
        with open(path, 'r') as file:
            data = file.read()
            data = data.replace(search_text, replace_text)

        with open(path, 'w') as file:
            file.write(data)
        print(f"Text replaced for {path}")
        print(data)
    except Exception as ex:
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")


def upload_file(file_path, bucket_name, source, output_file_name):
    try:
        # s3_client = boto3.client('s3')
        s3_client = boto3.client(
            's3',
            aws_access_key_id='AKIAVOZRE44NWUEQAAVF',
            aws_secret_access_key='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'
        )
        s3_client.upload_file(file_path, bucket_name, '%s/%s' %
                              (source, output_file_name))
        print("file uploaded on s3")
    except Exception as ex:
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")


def call_datahub(path):
    try:
#         script_path = "s3://datahub-sh/datahub_call.sh"  # for ec2
        script_path = "./datahub_call.sh"  # for local

        subprocess.call(shlex.split(f"sh {script_path} {path}"))
        print("Metadata script completed...")
    except Exception as ex:
        # logging.error(f"Error code    = {type(ex).__name__}")
        # logging.error(f"Error Message = {ex}")
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")


def metadata_profiling():
    s3 = boto3.resource(
        's3',
        aws_access_key_id='AKIAVOZRE44NWUEQAAVF',
        aws_secret_access_key='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'
    )
    logging.info("login Successful!")
    print("login Successful")
    s3_bucket = s3.Bucket('pgs-config-bucket')
    for file in s3_bucket.objects.all():
        obj = s3.Object('pgs-config-bucket', file.key)
        body = obj.get()['Body'].read().decode('utf-8')
        print(obj, body)
        config = json.loads(body)
        logging.info("Successful!")
        print("Successful")
        source_type = config['source_type']
        host = config['host']
        username = config['username']
        password = config['password']
        database = config['database']

        session = postgre_connect(host, database, username, password)
        sf_conn_sql = f"select properties from data_sources where description = 'ML';"
        df = create_dataframe(sf_conn_sql, session)
        config = df['properties'][0]
        new_dict = json.loads(config)
        JSONDict = dict((k.upper().strip(), v.upper().strip()) for k, v in new_dict.items())
        local_file, output_path, output_file_name = up_yml(JSONDict)
        replace_yml(local_file)
        # config = datahub.configuration.config_loader.load_config_file(local_file)
        # pipeline = datahub.ingestion.run.pipeline.Pipeline.create(config)
        # pipeline.run()
        # pipeline.raise_from_status()
        # subprocess.run(["datahub", "ingest", "-c", "--config", "path/to/config.yml"], check=True)
        call_datahub(local_file)
#         upload_file(output_path, 'datahub-sh', source_type, output_file_name)

        return 'success'


metadata_profiling()

# def datahub_recipe():
#     # Note that this will also resolve environment variables in the recipe.
#     config = load_config_file('./sfs.yml')
#
#     pipeline = Pipeline.create(config)
#     pipeline.run()
#     pipeline.raise_from_status()
# ingestion_task = PythonVirtualenvOperator(
#     task_id="ingestion_task",
#     requirements=[
#         "datahub",
#     ],
#     system_site_packages=False,
#     python_callable=datahub_recipe,
# )
# dag = DAG('DAG2', description='metadata dag',
#           schedule_interval=timedelta(days=1),
#           start_date=datetime(2021, 1, 7),
#           catchup=False)
# # ...
#
# # Install the 'datahub' package
# # install_datahub = PipPackageOperator(
# #     task_id='install_datahub',
# #     package='datahub',
# #     python_callable=None,
# #     op_args=[],
# #     op_kwargs={},
# #     dag=dag,
# # )
# #
# install_dependencies = BashOperator(
#     task_id='install_dependencies',
#     bash_command='pip install -r /usr/local/airflow/dags/requirements.txt',
#     dag=dag
# )
# dummy_operator = DummyOperator(task_id='dummy', retries=3, dag=dag)
# hello_operator = PythonOperator(task_id='hello_world', python_callable=print_hello, dag=dag)
# postgres_operator = PythonOperator(task_id='postgres', python_callable=metadata_profiling, dag=dag)
# # ingest_task = PythonVirtualenvOperator(task_id="ingest_using_recipe",system_site_packages=False, requirements=["datahub"],python_callable=datahub_recipe,dag=dag)
#
# install_dependencies >> dummy_operator >> postgres_operator
