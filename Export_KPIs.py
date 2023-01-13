import Utilities as utils
import pandas as pd
import sys
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


sys.path.append("../")
# config_dir = "../configs/config.properties"  # for ec2
config_dir = "Metadata_Ingestion/configs/config.properties"  # for local

try:
#     config = utils.read_config_file(config_dir)
#     sf_account = config.get('SNOWFLAKE', 'sf_account')
#     sf_role = config.get('SNOWFLAKE', 'sf_role')
#     sf_user = config.get('SNOWFLAKE', 'sf_user')
#     sf_password = config.get('SNOWFLAKE', 'sf_password')
#     sf_warehouse = config.get('SNOWFLAKE', 'sf_warehouse')
#     sf_database = config.get('SNOWFLAKE', 'sf_database')
#     sf_schema = config.get('SNOWFLAKE', 'sf_schema')
#     sf_table = config.get('SNOWFLAKE', 'sf_table')


      sf_account = config.get('SNOWFLAKE', 'sf_account')
      sf_role = 'ACCOUNTADMIN'
      sf_user = 'sayali'
      sf_password = 'Atgeir@03'
      sf_warehouse = 'HAWKEYE_WH'
      sf_database = 'DATAGEIR_HAWKEYE_DEV'
      sf_schema = 'HAWKEYE_APP'
      sf_table = 'METADATA_REPORT'
except Exception as ex:
    print(f"Error code    = {type(ex).__name__}")
    print(f"Error Message = {ex}")


def load_df_to_snowflake(snow, csv_df, dbname, schemaname, tablename):
    # try:
    # execute the command
    print("Loading Data Frame")
    status, nchunks, nrows, _ = write_pandas(
        conn=snow, df=csv_df, table_name=tablename, schema=schemaname, quote_identifiers="False")
    print(status, nchunks, nrows)
    snow.close()
    return status, nchunks, nrows

    # except Exception as ex:
    #     print("Snowflake SQL Failed")
    #     print(f"Error code    = {type(ex).__name__}")
    #     print(f"Error Message = {ex}")


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
    driver = utils.get_graph_driver(
        "neo4j://localhost:7687", "neo4j", "Abc@1234")
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
