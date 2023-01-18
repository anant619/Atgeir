import datetime,sys
import json
from datetime import date
from datetime import datetime
import Utilities as utils
import boto3
import pandas as pd
import graph as g
from neo4j import GraphDatabase
from datetime import timedelta
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
import json
config_dir = "./config.properties"  
today = date.today()
current_date = today.strftime("%Y-%m-%d")

today = date.today()
timestamp = datetime.now()
RunID = str(timestamp).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')

try:
    config = utils.read_config_file(config_dir)
    bucket = config.get('AWS', 's3_output_bkt')
    aws_access_key_id = config.get('AWS', 'aws_access_key_id')
    aws_secret_access_key = config.get('AWS', 'aws_secret_access_key')
    pgs_config_bucket = config.get('AWS', 'pgs_config_bucket')
except Exception as ex:
    print(f"Error code    = {type(ex).__name__}")
    print(f"Error Message = {ex}")

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
    
    print(JSONDict)
    
#     account = JSONDict.get('NAME')
#     account = JSONDict.get('NAME')
#     account = JSONDict.get('NAME')
#     account = JSONDict.get('NAME')
#     account = JSONDict.get('NAME')
#     account = JSONDict.get('NAME')
#     account = JSONDict.get('NAME')
#     account = JSONDict.get('NAME')
#     account = JSONDict.get('NAME')
sys.exit(0)
def get_RunId():
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, "RunId.json")
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

# class NpEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, np.integer):
#             return int(obj)
#         if isinstance(obj, np.floating):
#             return float(obj)
#         if isinstance(obj, np.ndarray):
#             return obj.tolist()
#         if isinstance(obj, pd.Series):
#             return obj.tolist()
#         if isinstance(obj, np.bool_):
#             return bool(obj)
#         return json.JSONEncoder.default(self, obj)
    
def load_df_to_snowflake(snow, csv_df, dbname, schemaname, tablename):
    print("Loading Data Frame")
    status, nchunks, nrows, _ = write_pandas(
        conn=snow, df=csv_df, table_name=tablename, schema=schemaname, quote_identifiers="False")
    print(status, nchunks, nrows)
    snow.close()
    return status, nchunks, nrows

s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
print("login Successful")
s3_bucket = s3.Bucket(bucket)
RunId = get_RunId()

for file in s3_bucket.objects.all():
    if str(RunId) in file.key:
      obj = s3.Object(bucket, file.key)
      print(obj)
      body = obj.get()['Body'].read().decode('utf-8')
      print(body)
      try:
          data = json.loads(body)
          table_data = create_json(data)
          source = table_data.get('Source')
          Database_name = table_data.get('Database_name')
          Schema_name = table_data.get('Schema_name')
          Table_name = table_data.get('Table_name')
          tags = table_data.get('tags')
          totalSqlQueriesCount = table_data.get('totalSqlQueriesCount')
          uniqueUserCount = table_data.get('uniqueUserCount')
          fielddetails = table_data.get('fields')
          action = table_data.get('action')
          print(type(action),type(fielddetails))
          
          uniqueusercount = table_data.get('datasetUsage').get('uniqueUserCount')
          totalSqlQueriesCount = table_data.get('datasetUsage').get('totalSqlQueriesCount')
#           load_timestamp = pd.datetime.now()
#           load_timestamp = pd.Timestamp(np.datetime64[ns])
#           print(load_timestamp)
#           timestamp = table_data.get('timestamp')
#           timestamp = datetime.fromtimestamp(int(timestamp))
#           print(type(fielddetails),action)
         
          column = ["SOURCE", "DATABASE_NAME", "SCHEMA_NAME", "TABLE_NAME", "TAGS", "UNIQUEUSERUSAGECOUNT","TOTALQUERIESCOUNT","RUNID","FIELDDETAILS","ACTION"]
          data = [[source, Database_name,Schema_name,Table_name,tags,uniqueUserCount,totalSqlQueriesCount,RunID,fielddetails,action]]
          df = pd.DataFrame(data,columns=column)
#           df['LOAD_TIMESTAMP'].astype('datetime64[ns]')
#           df['LOAD_TIMESTAMP'].astype('str')
          snow = utils.snow_connect('AFA78268', 'sayali', 'Atgeir@03', 'ACCOUNTADMIN', 'HAWKEYE_WH', 'DATAGEIR_HAWKEYE_DEV', 'HAWKEYE_APP')
          load_df_to_snowflake(snow, df, 'DATAGEIR_HAWKEYE_DEV', 'HAWKEYE_APP', 'METADATA_REPORT')

#           insert_sql = f"insert into DATAGEIR_HAWKEYE_DEV.HAWKEYE_APP.METADATA_REPORT (source, Database_name, Schema_name, Table_name, tags, UNIQUEUSERUSAGECOUNT,TOTALQUERIESCOUNT,RUNID,fielddetails,action) VALUES ('{source}', '{Database_name}', '{Schema_name}', '{Table_name}','{tags}','{uniqueusercount}','{totalSqlQueriesCount}','{RunId}',to_variant('{fielddetails}'),to_variant('{action}');"
#           snow = utils.snow_connect('AFA78268', 'sayali', 'Atgeir@03', 'ACCOUNTADMIN', 'HAWKEYE_WH', 'DATAGEIR_HAWKEYE_DEV', 'HAWKEYE_APP')
#           snow.cursor().execute(insert_sql)
          
          with open('test_data.json', 'w') as f:
            json.dump(table_data,f)
          output_file_name = "test_data_final.json"
          source_type = "snowflake"
          table_data = "./test_data.json"
          utils.upload_file(table_data, bucket, source_type, f"{RunID}/{output_file_name}")
           
      except Exception as e:
          print(e)


