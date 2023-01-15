import Utilities as utils
import pandas as pd
import sys
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import boto3 
import logging

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
    
#sys.path.append("../")
config_dir = "./config.properties"  # for ec2
try:
    config = utils.read_config_file(config_dir)
    bucket = config.get('AWS', 's3_output_bkt')
    aws_access_key_id = config.get('AWS', 'aws_access_key_id')
    aws_secret_access_key = config.get('AWS', 'aws_secret_access_key')
      
except Exception as ex:
    print(f"Error code    = {type(ex).__name__}")
    print(f"Error Message = {ex}")

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
 
    sf_account = JSONDict.get('ACCOUNT')
    sf_role = JSONDict.get('ROLE')
    #sf_user = JSONDict.get('NAME')
    sf_user = 'sayali'
    #sf_password = JSONDict.get('SNOWFLAKE', 'sf_password')
    sf_warehouse = JSONDict.get('WAREHOUSE')
    sf_database = JSONDict.get('DATABASE')
    sf_schema = JSONDict.get('SCHEMA')
    sf_password = 'Atgeir@03'
    #sf_table = JSONDict.get('SNOWFLAKE', 'sf_table')
    sf_table = 'METADATA_REPORT'
    
    #sf_account = 'AFA78268'
    #sf_role = 'ACCOUNTADMIN'
    #sf_user = 'sayali'
    #sf_password = 'Atgeir@03'
    #sf_warehouse = 'HAWKEYE_WH'
    #sf_database = 'DATAGEIR_HAWKEYE_DEV'
    #sf_schema = 'HAWKEYE_APP'
    #sf_table = 'METADATA_REPORT'

def load_df_to_snowflake(snow, csv_df, dbname, schemaname, tablename):
    # try:
    # execute the command
    print("Loading Data Frame")
    status, nchunks, nrows, _ = write_pandas(
        conn=snow, df=csv_df, table_name=tablename, schema=schemaname, quote_identifiers="False")
    print(status, nchunks, nrows)
    snow.close()
    return status, nchunks, nrows

query_string = """
MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:HAS_FIELDS]->(f:FIELD)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:Table_RowCount]->(rc:Rows)
// For ColumnCount
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:Table_ColumnCount]->(tc:ColumnCount)
// For tags
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]-(r:RunId)-[:HAS_TAGS]->(tg:Tag)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:SizeInBytes]->(sib:Size)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:HAS_FIELDS]->(f:FIELD)-[:Columns_Details]->(cd:Details)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:HAS_FIELDS]->(f:FIELD)-[:Field_DistinctValue]->(fdv:DistinctValue)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:operation_performed]->(act:Action) WHERE act.OperationType = 'CREATE'
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:operation_performed]->(action:Action)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:HAS_FIELDS]->(f:FIELD)-[:User_Usage]->(uuc:User_Usage_Count)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:HAS_FIELDS]->(f:FIELD)-[:USAGE_COUNT]->(uc:Count)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:Total_Queries_Executed]->(tqc:Total_Queries_Count)
OPTIONAL MATCH (src:Source)-[:has_Database]->(n:Database)-[:has_schema]->(m:SCHEMA)-[:has_table]->(t:TABLE)-[:RunId]->(r:RunId)-[:Top_Queries]->(tpq:TopQueries)
RETURN src.name as SOURCE, n.name as Database,m.name AS Schema ,t.name AS Table, r.name AS RunId, rc.RowCount AS RowCount, tc.ColumnCount as ColumnCount, tg.tags AS Tags,sib.SizeInBytes AS SizeInBytes, act.User as Created_by,act.Timestamp AS CreationTimestamp , uuc.user_count AS UniqueUserUsageCount,tqc.query_count AS TotalQueriesCount,
collect( DISTINCT { FieldName:f.name, DataType:f.DataType, IsNullable:f.IsNullable, IsPartOfKey:f.IsPartOfKey, Recursive:f.Recursive, UsageCount:uc.count ,Field_DistinctValues: {Value:fdv.Value, Frequency: fdv.Frequency}, FieldValueDetails:{UniqueValueCount: cd.UniqueCount, UniqueValueProportion: cd.UniqueProportion, NullValueCount: cd.nullCount, NullValueProportion:cd.nullProportion, MinValue:cd.Min, MaxValue: cd.Max, MeanValue: cd.Mean, MedianValue: cd.Median, sampleValues: cd.sampleValues } }) as FieldDetails
,collect(DISTINCT {  Operation: action.OperationType, PerformedBy: action.User, Timestamp:action.Timestamp}) as Action, collect ( DISTINCT {TopQueries:tpq.Queries }) AS TopQueries
"""

try:
      config = utils.read_config_file(config_dir)
      #URI = config.get('NEO4J', 'uri')
      URI = "neo4j://3.231.31.2:7687"
      username =  config.get('NEO4J', 'username')
      password = config.get('NEO4J', 'password')
      database = config.get('NEO4J', 'database')
      driver = utils.get_graph_driver(URI, username,password)

except Exception as ex:
    print(f"Error code    = {type(ex).__name__}")
    print(f"Error Message = {ex}")

try:
    session = driver.session()
    result = session.run(query_string).data()
    columns = ['SOURCE','DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'RUNID', 'ROWCOUNT', 'COLUMNCOUNT', 'TAGS', 'SIZEINBYTES',
               'CREATED_BY', 'CREATIONTIMESTAMP', 'UNIQUEUSERUSAGECOUNT', 'TOTALQUERIESCOUNT', 'FIELDDETAILS', 'ACTION', 'TOPQUERIES']
    df = pd.DataFrame(result)
    df.columns = map(lambda x: str(x), columns)
    print("Neo4j query executed successfully ")
except Exception as e:
    print("Query failed:", e)
finally:
    if session is not None:
        session.close()
try:
    snow = utils.snow_connect(sf_account, sf_user, sf_password,
                            sf_role, sf_warehouse, sf_database, sf_schema)
    load_df_to_snowflake(snow, df, sf_database, sf_schema, sf_table)
    print("data loaded to Snowflake successfully")
except Exception as ex:
    print(f"Error code    = {type(ex).__name__}")
    print(f"Error Message = {ex}")
    
    
