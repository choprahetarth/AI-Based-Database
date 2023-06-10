import requests
import pandas as pd
from modules.read_yaml import YamlBuilder
from modules.connect_to_database import ConnectDb
from modules.approximate_query import ApproxQuery
# from modules.approximate_query import ApproxQuery


#### read the yaml file
y = YamlBuilder('config.yaml')
yaml_parsed  = y.read_yaml()

#### connect the database
d = ConnectDb(yaml_parsed)
conn = d.establish_connection()

query = """SELECT AVG(LENGTH(topic))
        FROM closest_topic
        JOIN sentiment_analysis ON closest_topic.id = sentiment_analysis.id
        WHERE sentiment_analysis.sentiment = '{"sentiment":1}';"""

# instantiate the exact query object
eq = ApproxQuery(query, yaml_parsed, conn)

# get tables 
query_info = eq.extract_query_info()
length_of_tables = eq.execute_queries(query_info)
ml_model_details = eq.get_ml_model_details()

# # if 0, we know that it has to be populated with ML Query 
if length_of_tables==0:
    # we know that the cache table is empty, therefore execute the first ML model to populate the rows
        for idx, x in enumerate(ml_model_details):
                if idx==0:
                        source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
                        api = ml_model_details[x][0]
                        output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
                        print(source_table_and_col, api, output_table_and_col)
                        connection, meta  = eq.connect_to_db_and_reflect()
                        ######## POPULATE THE VALUES WITH FIRST TABLE ML #################
                        for chunk in pd.read_sql_table(source_table_and_col[0], connection, chunksize=1):
                                primary_key = chunk['id'].values[0]
                                payload = chunk[source_table_and_col[1]].values[0]
                                response = requests.request("GET", 
                                                        api,
                                                        params={'text':payload},
                                                        timeout=100)
                                result = response.text
                                result = result.replace('\n','')
                                inserting_query = f"""INSERT INTO {output_table_and_col[0]} (id, {output_table_and_col[1]}) VALUES ({primary_key},'{result}');""" # hack
                                print("Executed the queries")
                                cur = conn.cursor()
                                cur.execute(inserting_query)
                                # close the connection
                        # ######### CHECK THE QUERY FOR WHICH ROWS ARE AFFECTED ################
                        cache_retrivel_query = f"""SELECT id from {output_table_and_col[0]} WHERE {query_info['where_condition'][0]}"""
                        print("Getting the affected rows")
                        cur = conn.cursor()
                        cur.execute(cache_retrivel_query)
                        ######### APPEND THOSE ROWS INTO THE CACHE ########################
                        list_of_rows_affected = [item for tup in cur.fetchall() for item in tup]
                        print(list_of_rows_affected)
                        eq.fill_cache(list_of_rows_affected, output_table_and_col[0])
                        conn.commit()
                        cur.close()
                        ############# PROVIDE THE CACHE QUERY FOR THE SECOND TABLE #############
                        get_cache_for_second_row = f"""SELECT * from {source_table_and_col[0]} WHERE id = ANY((SELECT scope FROM cache WHERE cache.model_name='{output_table_and_col[0]}')::integer[]);"""
                elif idx==1:
                        ######## APPLY THE SECOND ML MODEL ON THE SELECTED COLUMNS #######
                        source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
                        api = ml_model_details[x][0]
                        output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
                        print(source_table_and_col, api, output_table_and_col)
                        connection, meta  = eq.connect_to_db_and_reflect()
                        list_of_rows_affected=[]
                        for chunk in pd.read_sql_query(get_cache_for_second_row, connection, chunksize=1):
                                primary_key = chunk['id'].values[0]
                                api = ml_model_details[x][0]
                                payload = chunk[source_table_and_col[1]].values[0]
                                response = requests.request("GET", 
                                                            api,
                                                            params={'text':payload},
                                                            timeout=100)
                                result = response.text
                                result = result.replace('\n','')
                                inserting_query = f"""INSERT INTO {output_table_and_col[0]} (id, {output_table_and_col[1]}) VALUES ({primary_key},'{result}');""" # hack
                                ######### APPEND THOSE ROWS INTO THE CACHE ########################
                                list_of_rows_affected.append(primary_key) 
                                print("Executed the queries")
                                cur = conn.cursor()
                                cur.execute(inserting_query)
                                conn.commit()
                        print(list_of_rows_affected)
                        eq.fill_cache(list_of_rows_affected, output_table_and_col[0])
                        # close the connection
                        conn.commit()
                        cur.close()
elif length_of_tables!=0:
        for idx, x in enumerate(ml_model_details):
                if idx==0:
                        source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
                        api = ml_model_details[x][0]
                        output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
                        cache_retrivel_query = f"""SELECT id from {output_table_and_col[0]} WHERE {query_info['where_condition'][0]}"""
                        print("Getting the affected rows")
                        cur = conn.cursor()
                        cur.execute(cache_retrivel_query)
                        list_of_rows_affected = [item for tup in cur.fetchall() for item in tup]
                        print(list_of_rows_affected)
                        eq.fill_cache(list_of_rows_affected, output_table_and_col[0])
                        conn.commit()
                        cur.close()


# else:
#     print("DB Already Populated")

# print("Fetching Results of Exact query")

# try:
#     # initiate the cursor
#     print("Executed the queries")
#     cur = conn.cursor()
#     cur.execute(query)
#     list_of_output = cur.fetchall()
#     print((list_of_output))
#     # close the connection
#     cur.close()
# except Exception as e: print(e)


conn.close()