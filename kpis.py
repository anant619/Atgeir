import datetime,sys
import json
from datetime import date
from datetime import datetime
import Utilities as utils
import boto3
import graph as g
from neo4j import GraphDatabase
from datetime import timedelta
from snowflake.snowpark import Session
import snowflake.connector
import json
# sys.path.append("../")
config_dir = "./config.properties"  # for ec2
today = date.today()
current_date = today.strftime("%Y-%m-%d")

today = date.today()
timestamp = datetime.now()
RunID = str(timestamp).replace('-', '').replace(' ', '').replace(':', '').replace('.', '')

CONNECTION_PARAMETERS = {

"account": 'AFA78268',
"user": 'sayali',

"password": 'Atgeir@03',
"database": 'DATAGEIR_HAWKEYE_DEV',
"schema": 'HAWKEYE_APP',
"warehouse": 'HAWKEYE_WH',
"role": 'ACCOUNTADMIN'
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()

# try:
#     config = utils.read_config_file(config_dir)
# #     URI = config.get('NEO4J', 'uri')
#     URI = "neo4j://3.231.31.2:7687"
#     username =  config.get('NEO4J', 'username')
#     password = config.get('NEO4J', 'password')
#     database = config.get('NEO4J', 'database')
# except Exception as ex:
#     print(f"Error code    = {type(ex).__name__}")
#     print(f"Error Message = {ex}")
    
# # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
# # URI = "neo4j://44.204.128.255:7687"
# AUTH = (username, password)

# with GraphDatabase.driver(URI, auth=AUTH) as driver:
#     driver.verify_connectivity()
#     print("connected")

# driver = utils.get_gra8ph_driver("neo4j://44.204.128.255:7474","neo4j","sayali@123")

try:
    config = utils.read_config_file(config_dir)
    bucket = config.get('AWS', 's3_output_bkt')
    aws_access_key_id = config.get('AWS', 'aws_access_key_id')
    aws_secret_access_key = config.get('AWS', 'aws_secret_access_key')
      
except Exception as ex:
    print(f"Error code    = {type(ex).__name__}")
    print(f"Error Message = {ex}")

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
#                 for field in dataset.get('fieldProfiles'):
#                     temp_dict = {}
#                     temp_dict['fieldName'] = field.get('fieldPath') 
#                     temp_dict['uniqueCount'] = field.get('uniqueCount')
#                     temp_dict['uniqueProportion'] = field.get('uniqueProportion')
#                     temp_dict['nullCount'] = field.get('nullCount')
#                     temp_dict['nullProportion'] = field.get('nullProportion')
#                     temp_dict['min'] = field.get('min')
#                     temp_dict['max'] = field.get('max')
#                     temp_dict['mean'] = field.get('mean')
#                     temp_dict['median'] =field.get('median')
#                     temp_dict['distinctValueFrequencies'] = field.get('distinctValueFrequencies')
#                     temp_dict['sampleValues'] = field.get('sampleValues')
#                     temp_list.append(temp_dict)
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

# s3 = boto3.resource('s3')
s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
# logging.info("login Successful!")
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
          totalSqlQueriesCount = table_data.get('totalSqlQueriesCount')
          uniqueUserCount = table_data.get('uniqueUserCount')
          fields = table_data.get('fields')
          insert_sql = f"insert into DATAGEIR_HAWKEYE_DEV.HAWKEYE_APP.METADATA_REPORT (SOURCE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,'UNIQUEUSERUSAGECOUNT', 'TOTALQUERIESCOUNT', 'FIELDDETAILS') VALUES (source, Database_name, Schema_name, Table_name, totalSqlQueriesCount, uniqueUserCount, fields);"   
#           snow = utils.snow_connect('AFA78268', 'sayali', 'Atgeir@03', 'ACCOUNTADMIN', 'HAWKEYE_WH', 'DATAGEIR_HAWKEYE_DEV', 'HAWKEYE_APP')
          session.sql(insert_sql).collect()
#           print(table_data)
#           
#           with open('test_data.json', 'w') as f:
#             json.dump(table_data,f)
#           output_file_name = "test_data_final.json"
#           source_type = "snowflake"
#           table_data = "./test_data.json"
#           utils.upload_file(table_data, bucket, source_type, f"{RunID}/{output_file_name}")
           

      except ValueError as e:
          print ("Json is not valid")


