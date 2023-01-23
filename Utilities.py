from importlib.resources import contents
import os
import sys
import configparser
import yaml
import boto3
import subprocess
import shlex
from neo4j import GraphDatabase
import snowflake.connector
import psycopg2

sys.path.append("../")

def snow_connect(sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, sf_schema):
    
    connection = snowflake.connector.connect(account=sf_account, user=sf_user, password=sf_password,role=sf_role, 
                                             warehouse=sf_warehouse, database=sf_database, schema=sf_schema,
                                             timezone='UTC')
    print("connected to Snowflake")
    return connection

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


def read_config_file(config_dir):
    """ 
    Sourcing Configuration File
    """
    try:
        conf = configparser.RawConfigParser()
        conf.read(config_dir)
    except Exception as ex:
        # logging.error(f"Error code    = {type(ex).__name__}")
        # logging.error(f"Error Message = {ex}")
        # sys.exit(1)
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")
    return conf


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

def update_yml(source_type, conf, yml_name):
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(
            Bucket=yml_name, Key=f'{source_type}.yml')  # yml templates
        configfile = yaml.safe_load(response["Body"])

        output_file_name = f"{conf.get('table')}.json"

        #output_path = f"Metadata_Ingestion/output-json/{source_type}.json" # for local
        output_path = f'../output-json/{source_type}.json'  # for ec2

        configfile['sink']['config']['filename'] = output_path

        source_config = configfile['source']['config']
        source_config['username'] = conf.get('username')
        source_config['password'] = conf.get('password')
        source_config['table_pattern']['allow'] = F".*{conf.get('table')}"
        source_config['profile_pattern']['allow'] = F"{conf.get('database')}.*.*"

        if source_type == 'snowflake':
            database_pattern = F"^{conf.get('database')}$"
            source_config['database_pattern']['allow'] = database_pattern
            source_config['provision_role']['admin_username'] = conf.get(
                'username')
            source_config['provision_role']['admin_password'] = conf.get(
                'password')
            source_config['account_id'] = conf.get('account')
            source_config['warehouse'] = conf.get('warehouse')
            source_config['role'] = conf.get('role')
        elif source_type == 'mysql':
            database_pattern = F"^{conf.get('database')}$"
            source_config['database_pattern']['allow'] = database_pattern
            source_config['host_port'] = conf.get('host_port')
            source_config['database'] = conf.get('database')
        elif source_type == 'postgres':
            source_config['host_port'] = conf.get('host_port')
            source_config['database'] = conf.get('database')

        local_file = f'../temp/{source_type}.yml'  # for ec2
        #local_file = f'Metadata_Ingestion\\temp\{source_type}.yml' #for local

        with open(local_file, 'w+') as f:
            yaml.safe_dump(configfile, f, default_flow_style=False)
            print("file loaded successfully", local_file)
        return local_file, output_path, output_file_name

    except Exception as ex:
        # logging.error(f"Error code    = {type(ex).__name__}")
        # logging.error(f"Error Message = {ex}")
        print(f"Error code    = {type(ex).__name__}")
        print(f"Error Message = {ex}")
        sys.exit(1)
