import json
import boto3
import sys
import datetime
from datetime import date
from datetime import datetime
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

config_dir = "./config.properties"  # for ec2

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

today = date.today()
timestamp = datetime.now()
RunID = str(timestamp).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')

try:
    config = read_config_file(config_dir)
    pgs_config_bucket = config.get('AWS', 'pgs_config_bucket')
    yml_bkt = config.get('AWS', 's3_yml_template_bkt')
    output_bkt = config.get('AWS', 's3_output_bkt')
    aws_access_key_id = config.get('AWS', 'aws_access_key_id')
    aws_secret_access_key = config.get('AWS', 'aws_secret_access_key')
#    	metadata-bkt = config.get('AWS', 'metadata_bkt')
except Exception as ex:
    print(f"Error code    = {type(ex).__name__}")
    print(f"Error Message = {ex}")


def stroe_run_id(output_bkt, RunID):
    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    obj = s3.Object(output_bkt, "RunId.json")
    body = obj.get()['Body'].read().decode('utf-8')
    config = json.loads(body)
    config['RunId'] = RunID
    print(config['RunId'])
    s3object = s3.Object(output_bkt, 'RunId.json')
    s3object.put(Body=(bytes(json.dumps(config).encode('UTF-8'))))

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
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3_bucket = s3.Bucket(yml_bkt)
    for file in s3_bucket.objects.all():
        obj = s3.Object(yml_bkt, file.key)
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
        source_config['table_pattern']['allow'] = F".*ITEM"
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
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        s3_client.upload_file(file_path, bucket_name, '%s/%s' %
                              (source, output_file_name))
        print("file uploaded on s3")
    except Exception as ex:
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")


def call_datahub(path):
    try:
        script_path = "./datahub_call.sh"  # for local

        subprocess.call(shlex.split(f"sh {script_path} {path}"))
        print("Metadata script completed...")
    except Exception as ex:
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")


def metadata_profiling():
    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    logging.info("login Successful!")
    print("login Successful")
    s3_bucket = s3.Bucket(pgs_config_bucket)
    for file in s3_bucket.objects.all():
        obj = s3.Object(pgs_config_bucket, file.key)
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
        call_datahub(local_file)
        upload_file(output_path, output_bkt, source_type, f"{RunID}/{output_file_name}")
        stroe_run_id(output_bkt, RunID)
        return 'success'


metadata_profiling()

