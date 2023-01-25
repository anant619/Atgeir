import json
import boto3
import sys
import datetime
from datetime import date
import os
import glob
import numpy as np
import pandas as pd
import configparser
from datetime import timedelta
import yaml
import subprocess
import shlex
from datetime import datetime
import Utilities as utils
from snowflake.connector.pandas_tools import write_pandas
import psycopg2
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import logging
# client_id = os.environ['client_id']
# print(email)
# sys.exit(0)
config_dir = "./config.properties"  
today = date.today()
current_date = today.strftime("%Y-%m-%d")
timestamp = datetime.now()
RunID = str(timestamp).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')

      
def get_RunId():
    s3 = boto3.resource('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    obj = s3.Object(output_bkt, "RunId.json")
    body = obj.get()['Body'].read().decode('utf-8')
    config = json.loads(body)
    RunId = config['RunId']
    return RunId


def create_json(data):
    tableMetadata = {}
    datasetUsage = []
    action = []
    for i in data:      
        if i.get('entityType')== "container":
            if i.get('aspectName') == 'containerProperties':
                data_value = json.loads(i.get('aspect').get('value'))
                Source = data_value.get("customProperties").get("platform")
                tableMetadata['Source'] = Source
                str_timestamp = i.get('systemMetadata').get('lastObserved')
                timestamp = datetime.fromtimestamp(str_timestamp/1000)
                tableMetadata['timestamp'] = str(timestamp).replace('-','').replace(' ','').replace(':','').replace('.','')
                
        elif i.get('entityType')== 'dataset':
            if i.get('aspectName') == 'schemaMetadata':
                data_value = json.loads(i.get('aspect').get('value'))
                tableMetadata["Database_name"],tableMetadata["Schema_name"],tableMetadata["Table_name"]= data_value.get('schemaName').split('.')
                fields = data_value.get('fields')
                temp_list = []
                for field in fields:
                    temp_dict = {}
                    temp_dict['fieldName'] = field.get('fieldPath')
                    temp_dict['dataType'] = field.get('nativeDataType')
                    temp_dict['isNullable'] = field.get('nullable')
                    temp_dict['isPartOfKey'] = field.get('isPartOfKey')
                    temp_dict['recursive'] = field.get('recursive')
                    temp_list.append(temp_dict)
                tableMetadata['fields'] = temp_list
            elif i.get('aspectName') == 'datasetProperties' or i.get('aspectName') == 'DatasetProperties':
                tableMetadata['tags'] = json.loads(i.get('aspect').get('value')).get('tags')

            elif i.get('aspectName') == 'datasetProfile':
                dataset = json.loads(i.get('aspect').get('value'))
                tableMetadata['rowCount'] = dataset.get('rowCount')
                tableMetadata['columnCount'] = dataset.get('columnCount')
                tableMetadata['sizeInBytes'] = dataset.get('sizeInBytes')
                temp_list = []
                for field in dataset.get('fieldProfiles'):
                    temp_dict = {}
                    temp_dict['fieldName'] = field.get('fieldPath') 
                    temp_dict['uniqueCount'] = field.get('uniqueCount')
                    temp_dict['uniqueProportion'] = field.get('uniqueProportion')
                    temp_dict['nullCount'] = field.get('nullCount')
                    temp_dict['nullProportion'] = field.get('nullProportion')
                    temp_dict['min'] = field.get('min')
                    temp_dict['max'] = field.get('max')
                    temp_dict['mean'] = field.get('mean')
                    temp_dict['median'] =field.get('median')
                    temp_dict['distinctValueFrequencies'] = field.get('distinctValueFrequencies')
                    temp_dict['sampleValues'] = field.get('sampleValues')
                    temp_list.append(temp_dict)
                tableMetadata['fieldsData'] = temp_list
            
            elif i.get('aspectName') == 'datasetUsageStatistics':
                datasetUsage.append(json.loads(i.get('aspect').get('value')))
            elif i.get('aspectName') == 'operation':
                dataset = json.loads(i.get('aspect').get('value'))
                temp_dict = {}
                timestamp = datetime.fromtimestamp(dataset.get('lastUpdatedTimestamp')/1000)
                temp_dict['timestamp'] = str(timestamp)
                temp_dict['user'] = dataset.get('actor')[16:]
                temp_dict['operationType'] = dataset.get('operationType')
                action.append(temp_dict)
        elif "proposedSnapshot" in i.keys():
           dataset = i["proposedSnapshot"].get("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot").get('aspects')
           
           if len(dataset) > 2:
            tableMetadata['tags']  = dataset[1].get('com.linkedin.pegasus2avro.dataset.DatasetProperties').get('tags')
            tableMetadata["Database_name"],tableMetadata["Schema_name"],tableMetadata["Table_name"] =  dataset[2].get('com.linkedin.pegasus2avro.schema.SchemaMetadata').get('schemaName').split('.')
     
            fields = dataset[2].get('com.linkedin.pegasus2avro.schema.SchemaMetadata').get("fields")
            temp_list = []
            for field in fields:
                temp_dict = {}
                temp_dict['fieldName'] = field.get('fieldPath')
                temp_dict['dataType'] = field.get('nativeDataType')
                temp_dict['isNullable'] = field.get('nullable')
                temp_dict['isPartOfKey'] = field.get('isPartOfKey')
                temp_dict['recursive'] = field.get('recursive')
                temp_list.append(temp_dict)
            tableMetadata['fields'] = temp_list
                 
    if len(action) > 0:
        tableMetadata['action'] = action
    temp_dict = {}

    if len(datasetUsage) > 0:
        temp_dict['uniqueUserCount'] = datasetUsage[-1].get('uniqueUserCount')
        temp_dict['totalSqlQueriesCount'] = datasetUsage[-1].get('totalSqlQueries')
        temp_dict['topSqlQueries'] = datasetUsage[-1].get('topSqlQueries')
        temp_list = []
        for i in datasetUsage[-1].get('fieldCounts'):
            temp_dct = {}
            temp_dct['fieldName'] = i.get('fieldPath') 
            temp_dct['count'] = i.get('count')
            temp_list.append(temp_dct)
        temp_dict['fieldUsageCounts'] = temp_list

    tableMetadata['datasetUsage'] = temp_dict

    return tableMetadata

def load_df_to_snowflake(snow, csv_df, dbname, schemaname, tablename):
    print("Loading Data Frame")
    status, nchunks, nrows, _ = write_pandas(
        conn=snow, df=csv_df, table_name=tablename, schema=schemaname, quote_identifiers="False")
    print(status, nchunks, nrows)
    return status, nchunks, nrows

try:
    config = utils.read_config_file(config_dir)
    pgs_config_bucket = config.get('AWS', 'pgs_config_bucket')
    yml_bkt = config.get('AWS', 's3_yml_template_bkt')
    output_bkt = config.get('AWS', 's3_output_bkt')
    aws_access_key_id = config.get('AWS', 'aws_access_key_id')
    aws_secret_access_key = config.get('AWS', 'aws_secret_access_key')
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
    pgs_s3_bucket = s3.Bucket(pgs_config_bucket)
    for file in pgs_s3_bucket.objects.all():
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

        session = utils.postgre_connect(host, database, username, password)
#         sql_uuid = f"select uuid from requests where email='{email}';"
#         df_uuid = utils.create_dataframe(sql_uuid, session)
#         a = df_uuid['uuid']
#         print(a[0])
        sql_uuid1 = f"select uuid from requests where parent_uuid ='a7c72ce4-793c-4c5e-bab0-68ddda6b93d1';"
        df_uuid1 = utils.create_dataframe(sql_uuid1, session)
        b = df_uuid1['uuid']
        print(b)
        sys.exit(0)
        for i in b:
#             sys.exit(0)
            sf_conn_sql = f"select id, properties, output_properties from data_sources where data_source_type = 'Hawkeye' and parent_uuid = '{i}';"
            df = utils.create_dataframe(sf_conn_sql, session)
            print(df)
            
#             sys.exit(0)
            id_list = df['id']
            for d in id_list:
                sql = f"select properties from data_sources where id = {d}";
                df1 = utils.create_dataframe(sql, session)
                sql2 = f"select distinct(table_name) from hawkeye_details where data_source_id={d} and end_date is null"; 
                df2 = utils.create_dataframe(sql2, session)
                table_names = df2['table_name']
                s3 = boto3.resource('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
                pgs_s3_bucket = s3.Bucket(pgs_config_bucket)
                for file in pgs_s3_bucket.objects.all():
                    obj = s3.Object(pgs_config_bucket, file.key)
                    body = obj.get()['Body'].read().decode('utf-8')
                    print(obj, body)
                    config = json.loads(body)
                    print("Successful")
                    source_type = config['source_type']
                    host = config['host']
                    username = config['username']
                    password = config['password']
                    database = config['database']

                    session = utils.postgre_connect(host, database, username, password)
                    sf_conn_sql = f"select output_properties from data_sources where id = {d}";
                    df = utils.create_dataframe(sf_conn_sql, session)
                    config = df['output_properties'][0]
                    new_dict = json.loads(config)

                    JSONDict = dict((k.upper().strip(), v) for k, v in new_dict.items())
                    sf_account_url = JSONDict.get('ACCOUNT_URL')
                    sf_account = JSONDict.get('ACCOUNT')
                    sf_role = JSONDict.get('ROLE')
                    sf_user = JSONDict.get('NAME')
                    sf_privatekey = JSONDict.get('PRIVATEKEY')
                    sf_passphrase = JSONDict.get('PASSPHRASE').lower()
                    sf_warehouse = JSONDict.get('WAREHOUSE')
                    sf_database = JSONDict.get('DATABASE')
                    sf_schema = JSONDict.get('SCHEMA')

                    with open("private_key.p8", "wb") as f:
                        f.write(sf_privatekey.encode())

                    try:
                        with open("./private_key.p8", "rb") as key:
                            p_key = serialization.load_pem_private_key(
                                key.read(),
                                password=sf_passphrase.encode(),
                                backend=default_backend()
                            )

                        pkb = p_key.private_bytes(
                            encoding=serialization.Encoding.DER,
                            format=serialization.PrivateFormat.PKCS8,
                            encryption_algorithm=serialization.NoEncryption())

                        connection = snowflake.connector.connect(
                            account=sf_account,
                            user=sf_user,
                            role=sf_role,
                            private_key=pkb,
                            warehouse=sf_warehouse,
                            database=sf_database,
                            schema=sf_schema,
                            timezone='UTC'
                        )

                    except Exception as ex:
                        logging.error(f"Error code    = {type(ex).__name__}")
                        logging.error(f"Error Message = {ex}")
                        sys.exit(1)
                        
                config = df1['properties'][0]
                new_dict = json.loads(config)
                JSONDict = dict((k.upper().strip(), v.upper().strip()) for k, v in new_dict.items())
                s3 = boto3.resource('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
                yml_s3_bucket = s3.Bucket(yml_bkt)
                for j in table_names:
                    for file in yml_s3_bucket.objects.all():
                        print(file,'sssssss')
#                         sys.exit(0)
                        obj = s3.Object(yml_bkt, file.key)
                        body = obj.get()['Body'].read().decode('utf-8')
                        configfile = yaml.safe_load(body)
                        output_file_name = f"output_{j}.json"

                        output_path = f"./metadata_{j}.json"

                        source_config = configfile['source']['config']
                        # source_config['username'] = JSONDict.get('NAME')
                        source_config['username'] = "sayali"
                        # source_config['password'] = 'JSONDict.get('password')'
                        source_config['password'] = "Atgeir@03"
                        # source_config['table_pattern']['allow'] = F".*{JSONDict.get('table')}"
                        source_config['table_pattern']['allow'] = F".*{j}"
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
                        local_file = f'./datahub_{j}.yml'

                        with open(local_file, 'w+') as f:
                            yaml.safe_dump(configfile, f, default_flow_style=False)
                            print("file loaded successfully", local_file)

                        utils.replace_yml(local_file)
                        call_datahub(local_file)
                        upload_file(output_path, output_bkt, source_type, f"{RunID}/{output_file_name}")
#                         stroe_run_id(output_bkt, RunID)       
                        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key )
                        print("login Successful")
                        output_s3_bucket = s3.Bucket(output_bkt)
                        RunId = get_RunId()
                        print(RunID,type(RunID))

                        for file in output_s3_bucket.objects.all():
                            if str(RunId) in file.key:
                              obj = s3.Object(output_bkt, file.key)
                              print(obj,type(obj))
                              body = obj.get()['Body'].read().decode('utf-8')
                              print(body,type(body))
                              try:
                                  data = json.loads(body)
                                  table_data = create_json(data)
                                  source = table_data.get('Source')
                                  Database_name = table_data.get('Database_name')
                                  Schema_name = table_data.get('Schema_name')
                                  Table_name = table_data.get('Table_name')
                                  tags = table_data.get('tags')
                                  topSqlQueries = table_data.get('datasetUsage').get('topSqlQueries')
                                  fielddetails = table_data.get('fields')
                                  uniqueusercount = table_data.get('datasetUsage').get('uniqueUserCount')
                                  totalSqlQueriesCount = table_data.get('datasetUsage').get('totalSqlQueriesCount')
                                  if "action" in table_data.keys():
                                    action = table_data.get('action')
                                    if action[0].get('operationType') == 'CREATE':
                                        timestamp = action[0].get('timestamp')
                                        user = action[0].get('user')
                                    else:
                                        timestamp = 'NULL'
                                        user = 'NULL'
                                  else:
                                    action = 'NULL'
                                    timestamp = 'NULL'
                                    user = 'NULL'      

                                  column = ["SOURCE", "DATABASE_NAME", "SCHEMA_NAME", "TABLE_NAME", "TAGS", "UNIQUEUSERUSAGECOUNT","TOTALQUERIESCOUNT","RUNID","TOPQUERIES","FIELDDETAILS","ACTION","CREATED_BY","CREATIONTIMESTAMP"]
                                  data2 = [[source, Database_name,Schema_name,Table_name,tags,uniqueusercount,totalSqlQueriesCount,RunID,topSqlQueries,fielddetails,action,user,timestamp]]
                                  df = pd.DataFrame(data2,columns=column)

                                  snow = utils.snow_connect(sf_account, sf_user, 'Atg@12345', sf_role, sf_warehouse, sf_database, sf_schema)
                                  load_df_to_snowflake(connection, df, sf_database, sf_schema, 'METADATA_REPORT')

                                  with open('test_data.json', 'w') as f:
                                    json.dump(table_data,f)
                                  output_file_name = f"{Table_name}_output.json"
                                  source_type = "snowflake"
                                  table_data = "./test_data.json"
                                  upload_file(table_data, output_bkt, source_type, f"{RunID}/{output_file_name}")

                              except Exception as e:
                                  print(e)

            
metadata_profiling()
sys.exit(0)
    
s3 = boto3.resource('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
s3_bucket = s3.Bucket(pgs_config_bucket)
for file in s3_bucket.objects.all():
    obj = s3.Object(pgs_config_bucket, file.key)
    body = obj.get()['Body'].read().decode('utf-8')
    print(obj, body)
    config = json.loads(body)
    print("Successful")
    source_type = config['source_type']
    host = config['host']
    username = config['username']
    password = config['password']
    database = config['database']

    session = utils.postgre_connect(host, database, username, password)
    sf_conn_sql = f"select output_properties from data_sources where data_source_type = 'Hawkeye';"
    df = utils.create_dataframe(sf_conn_sql, session)
    config = df['output_properties'][0]
    new_dict = json.loads(config)
   
    JSONDict = dict((k.upper().strip(), v) for k, v in new_dict.items())
    sf_account_url = JSONDict.get('ACCOUNT_URL')
    sf_account = JSONDict.get('ACCOUNT')
    sf_role = JSONDict.get('ROLE')
    sf_user = JSONDict.get('NAME')
    sf_privatekey = JSONDict.get('PRIVATEKEY')
    sf_passphrase = JSONDict.get('PASSPHRASE').lower()
    sf_warehouse = JSONDict.get('WAREHOUSE')
    sf_database = JSONDict.get('DATABASE')
    sf_schema = JSONDict.get('SCHEMA')

    with open("private_key.p8", "wb") as f:
        f.write(sf_privatekey.encode())

    try:
        with open("./private_key.p8", "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=sf_passphrase.encode(),
                backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

        connection = snowflake.connector.connect(
            account=sf_account,
            user=sf_user,
            role=sf_role,
            private_key=pkb,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_schema,
            timezone='UTC'
        )
     
    except Exception as ex:
        logging.error(f"Error code    = {type(ex).__name__}")
        logging.error(f"Error Message = {ex}")
        sys.exit(1)
      
def get_RunId():
    s3 = boto3.resource('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    obj = s3.Object(output_bkt, "RunId.json")
    body = obj.get()['Body'].read().decode('utf-8')
    config = json.loads(body)
    RunId = config['RunId']
    return RunId


def create_json(data):
    tableMetadata = {}
    datasetUsage = []
    action = []
    for i in data:      
        if i.get('entityType')== "container":
            if i.get('aspectName') == 'containerProperties':
                data_value = json.loads(i.get('aspect').get('value'))
                Source = data_value.get("customProperties").get("platform")
                tableMetadata['Source'] = Source
                str_timestamp = i.get('systemMetadata').get('lastObserved')
                timestamp = datetime.fromtimestamp(str_timestamp/1000)
                tableMetadata['timestamp'] = str(timestamp).replace('-','').replace(' ','').replace(':','').replace('.','')
                
        elif i.get('entityType')== 'dataset':
            if i.get('aspectName') == 'schemaMetadata':
                data_value = json.loads(i.get('aspect').get('value'))
                tableMetadata["Database_name"],tableMetadata["Schema_name"],tableMetadata["Table_name"]= data_value.get('schemaName').split('.')
                fields = data_value.get('fields')
                temp_list = []
                for field in fields:
                    temp_dict = {}
                    temp_dict['fieldName'] = field.get('fieldPath')
                    temp_dict['dataType'] = field.get('nativeDataType')
                    temp_dict['isNullable'] = field.get('nullable')
                    temp_dict['isPartOfKey'] = field.get('isPartOfKey')
                    temp_dict['recursive'] = field.get('recursive')
                    temp_list.append(temp_dict)
                tableMetadata['fields'] = temp_list
            elif i.get('aspectName') == 'datasetProperties' or i.get('aspectName') == 'DatasetProperties':
                tableMetadata['tags'] = json.loads(i.get('aspect').get('value')).get('tags')

            elif i.get('aspectName') == 'datasetProfile':
                dataset = json.loads(i.get('aspect').get('value'))
                tableMetadata['rowCount'] = dataset.get('rowCount')
                tableMetadata['columnCount'] = dataset.get('columnCount')
                tableMetadata['sizeInBytes'] = dataset.get('sizeInBytes')
                temp_list = []
                for field in dataset.get('fieldProfiles'):
                    temp_dict = {}
                    temp_dict['fieldName'] = field.get('fieldPath') 
                    temp_dict['uniqueCount'] = field.get('uniqueCount')
                    temp_dict['uniqueProportion'] = field.get('uniqueProportion')
                    temp_dict['nullCount'] = field.get('nullCount')
                    temp_dict['nullProportion'] = field.get('nullProportion')
                    temp_dict['min'] = field.get('min')
                    temp_dict['max'] = field.get('max')
                    temp_dict['mean'] = field.get('mean')
                    temp_dict['median'] =field.get('median')
                    temp_dict['distinctValueFrequencies'] = field.get('distinctValueFrequencies')
                    temp_dict['sampleValues'] = field.get('sampleValues')
                    temp_list.append(temp_dict)
                tableMetadata['fieldsData'] = temp_list
            
            elif i.get('aspectName') == 'datasetUsageStatistics':
                datasetUsage.append(json.loads(i.get('aspect').get('value')))
            elif i.get('aspectName') == 'operation':
                dataset = json.loads(i.get('aspect').get('value'))
                temp_dict = {}
                timestamp = datetime.fromtimestamp(dataset.get('lastUpdatedTimestamp')/1000)
                temp_dict['timestamp'] = str(timestamp)
                temp_dict['user'] = dataset.get('actor')[16:]
                temp_dict['operationType'] = dataset.get('operationType')
                action.append(temp_dict)
        elif "proposedSnapshot" in i.keys():
           dataset = i["proposedSnapshot"].get("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot").get('aspects')
           
           if len(dataset) > 2:
            tableMetadata['tags']  = dataset[1].get('com.linkedin.pegasus2avro.dataset.DatasetProperties').get('tags')
            tableMetadata["Database_name"],tableMetadata["Schema_name"],tableMetadata["Table_name"] =  dataset[2].get('com.linkedin.pegasus2avro.schema.SchemaMetadata').get('schemaName').split('.')
     
            fields = dataset[2].get('com.linkedin.pegasus2avro.schema.SchemaMetadata').get("fields")
            temp_list = []
            for field in fields:
                temp_dict = {}
                temp_dict['fieldName'] = field.get('fieldPath')
                temp_dict['dataType'] = field.get('nativeDataType')
                temp_dict['isNullable'] = field.get('nullable')
                temp_dict['isPartOfKey'] = field.get('isPartOfKey')
                temp_dict['recursive'] = field.get('recursive')
                temp_list.append(temp_dict)
            tableMetadata['fields'] = temp_list
                 
    if len(action) > 0:
        tableMetadata['action'] = action
    temp_dict = {}

    if len(datasetUsage) > 0:
        temp_dict['uniqueUserCount'] = datasetUsage[-1].get('uniqueUserCount')
        temp_dict['totalSqlQueriesCount'] = datasetUsage[-1].get('totalSqlQueries')
        temp_dict['topSqlQueries'] = datasetUsage[-1].get('topSqlQueries')
        temp_list = []
        for i in datasetUsage[-1].get('fieldCounts'):
            temp_dct = {}
            temp_dct['fieldName'] = i.get('fieldPath') 
            temp_dct['count'] = i.get('count')
            temp_list.append(temp_dct)
        temp_dict['fieldUsageCounts'] = temp_list

    tableMetadata['datasetUsage'] = temp_dict

    return tableMetadata

def load_df_to_snowflake(snow, csv_df, dbname, schemaname, tablename):
    print("Loading Data Frame")
    status, nchunks, nrows, _ = write_pandas(
        conn=snow, df=csv_df, table_name=tablename, schema=schemaname, quote_identifiers="False")
    print(status, nchunks, nrows)
    return status, nchunks, nrows

s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
print("login Successful")
s3_bucket = s3.Bucket(output_bkt)
RunId = get_RunId()

for file in s3_bucket.objects.all():
    if str(RunId) in file.key:
      obj = s3.Object(output_bkt, file.key)
      body = obj.get()['Body'].read().decode('utf-8')
      try:
          data = json.loads(body)
          table_data = create_json(data)
          source = table_data.get('Source')
          Database_name = table_data.get('Database_name')
          Schema_name = table_data.get('Schema_name')
          Table_name = table_data.get('Table_name')
          tags = table_data.get('tags')
          topSqlQueries = table_data.get('datasetUsage').get('topSqlQueries')
          fielddetails = table_data.get('fields')
          uniqueusercount = table_data.get('datasetUsage').get('uniqueUserCount')
          totalSqlQueriesCount = table_data.get('datasetUsage').get('totalSqlQueriesCount')
          if "action" in table_data.keys():
            action = table_data.get('action')
            if action[0].get('operationType') == 'CREATE':
                timestamp = action[0].get('timestamp')
                user = action[0].get('user')
            else:
                timestamp = 'NULL'
                user = 'NULL'
          else:
            action = 'NULL'
            timestamp = 'NULL'
            user = 'NULL'      
     
          column = ["SOURCE", "DATABASE_NAME", "SCHEMA_NAME", "TABLE_NAME", "TAGS", "UNIQUEUSERUSAGECOUNT","TOTALQUERIESCOUNT","RUNID","TOPQUERIES","FIELDDETAILS","ACTION","CREATED_BY","CREATIONTIMESTAMP"]
          data2 = [[source, Database_name,Schema_name,Table_name,tags,uniqueusercount,totalSqlQueriesCount,RunID,topSqlQueries,fielddetails,action,user,timestamp]]
          df = pd.DataFrame(data2,columns=column)

          snow = utils.snow_connect(sf_account, sf_user, 'Atg@12345', sf_role, sf_warehouse, sf_database, sf_schema)
          load_df_to_snowflake(connection, df, sf_database, sf_schema, 'METADATA_REPORT')
          
          with open('test_data.json', 'w') as f:
            json.dump(table_data,f)
          output_file_name = f"{Table_name}_output.json"
          source_type = "snowflake"
          table_data = "./test_data.json"
          upload_file(table_data, output_bkt, source_type, f"{RunID}/{output_file_name}")
           
      except Exception as e:
          print(e)



