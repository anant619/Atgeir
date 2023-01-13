from neo4j import GraphDatabase
import Utilities as utils
import json


def create_graph(tx, value):
	try:
     
		tx.run("MERGE (so:Source{name: $value.Source})"
			"MERGE (d:Database{name: $value.Database_name})"
		    "MERGE (so)-[:has_Database]->(d)"
			"MERGE (s:SCHEMA{name: $value.Schema_name})"
			"MERGE (d)-[:has_schema]->(s)"
			"MERGE (t:TABLE{name:$value.Table_name})"
			"MERGE (s)-[:has_table]->(t)"
			"MERGE (ti:RunId{name: $value.timestamp})"
			"MERGE (t)-[:RunId]->(ti)",
			value=value)

		if (value.get('rowCount')):
			tx.run("MERGE (t:RunId{name:$value.timestamp}) "
				"CREATE (tg:Rows{RowCount: $value.rowCount}) "
				"MERGE (t)-[:Table_RowCount]->(tg) ",
				value=value)

		if (value.get('columnCount')):
			tx.run("MERGE (t:RunId{name:$value.timestamp}) "
				"CREATE (tg:ColumnCount{ColumnCount: $value.columnCount}) "
				"MERGE (t)-[:Table_ColumnCount]->(tg) ",
				value=value)

		if (value.get('tags')):
			tx.run("MERGE (t:RunId{name:$value.timestamp}) "
				"CREATE (tg:Tag{tags: $value.tags}) "
				"MERGE (t)-[:HAS_TAGS]->(tg) ",
				value=value)

		if (value.get('sizeInBytes')):
			tx.run("MERGE (t:RunId{name:$value.timestamp}) "
				"CREATE (tg:Size{SizeInBytes: $value.sizeInBytes}) "
				"MERGE (t)-[:SizeInBytes]->(tg) ",
				value=value)

		if (value.get('fields')):
			tx.run("MERGE (t:RunId{name:$value.timestamp}) "
				"with t,$value.fields AS fields "
				"UNWIND fields AS fi "
				"""CREATE (f:FIELD{name:fi.fieldName, DataType:fi.dataType,IsNullable: fi.isNullable,
										IsPartOfKey: fi.isPartOfKey,Recursive: fi.recursive}) """
				"MERGE (t)-[:HAS_FIELDS]->(f) ",
				value=value)

		if (value.get('fieldsData')):
			tx.run("with $value.fieldsData AS fieldData "
				"UNWIND fieldData AS field "
				"MATCH (u:FIELD{name:field.fieldName}), (t:RunId{name:$value.timestamp})  where (t) --> (u) "
				"""CREATE (d:Details{name:field.fieldName,UniqueCount:field.uniqueCount, UniqueProportion:field.uniqueProportion, nullCount:field.nullCount, nullProportion:field.nullProportion,
									Min:field.min, Max:field.max, Mean:field.mean, Median:field.median, sampleValues:field.sampleValues }) """
				"MERGE (u)-[:Columns_Details]->(d) ",
				value=value)

			tx.run("with $value.fieldsData AS fieldData "
				"UNWIND fieldData AS field "
				"MATCH (u:FIELD{name:field.fieldName}), (t:RunId{name:$value.timestamp})  where (t) --> (u) "
				"UNWIND field.distinctValueFrequencies AS distinctValue "
				"CREATE (d:DistinctValue{Value:distinctValue.value, Frequency:distinctValue.frequency}) "
				"MERGE (u)-[:Field_DistinctValue]->(d) ",
				value=value)

		if (value.get('action')):
			tx.run("MERGE (t:RunId{name:$value.timestamp}) "
          			"with t,$value.action AS action "
					"UNWIND action AS ac "
					"""CREATE (a:Action{Timestamp:ac.timestamp, User:ac.user,OperationType: ac.operationType}) """
					"MERGE (t)-[:operation_performed]->(a) ",
					value=value)
			

		if (value.get('datasetUsage')):
			if (value['datasetUsage'].get('uniqueUserCount')):
				tx.run("MERGE (t:RunId{name:$value.timestamp}) "
				"CREATE (tq:User_Usage_Count{user_count: $value.datasetUsage.uniqueUserCount}) "
				"MERGE (t)-[:User_Usage]->(tq) ",
				value=value) 
    
			if (value['datasetUsage'].get('fieldUsageCounts')):
				tx.run("with $value.datasetUsage.fieldUsageCounts AS fields "
					"UNWIND fields AS fi "
					"MATCH (u:FIELD{name:fi.fieldName}), (t:RunId{name:$value.timestamp})  where (t) --> (u) "
					"CREATE (c:Count{count:fi.count}) "
					"MERGE (u)-[:USAGE_COUNT]->(c) ",
					value=value)

			if (value['datasetUsage'].get('totalSqlQueriesCount')):
				tx.run("MERGE (t:RunId{name:$value.timestamp}) "
					"CREATE (tq:Total_Queries_Count{query_count: $value.datasetUsage.totalSqlQueriesCount}) "
					"MERGE (t)-[:Total_Queries_Executed]->(tq) ",
					value=value)
    
			if (value['datasetUsage'].get('topSqlQueries')):
				tx.run("MERGE (t:RunId{name:$value.timestamp}) "
					"CREATE (tq:TopQueries{Queries: $value.datasetUsage.topSqlQueries}) "
					"MERGE (t)-[:Top_Queries]->(tq) ",
					value=value)

	except Exception as ex:
     	# logging.error(f"Error code    = {type(ex).__name__}")
        # logging.error(f"Error Message = {ex}")
		print(f"Error code    = {type(ex).__name__}")
		print(f"Error Message = {ex}") 
