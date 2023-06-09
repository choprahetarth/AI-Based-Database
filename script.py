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
        WHERE sentiment_analysis.sentiment = '{"sentiment":4}';"""

# instantiate the exact query object
eq = ApproxQuery(query, yaml_parsed, conn)

# get tables 
query_info = eq.extract_query_info()
print(query_info)
length_of_tables = eq.execute_queries(query_info)
ml_model_details = eq.get_ml_model_details()



# # if 0, we know that it has to be populated with ML Query 
if length_of_tables==0:
    # we know that the cache table is empty, therefore execute the first ML model to populate the rows
        for (x,y) in ml_model_details.items():
                source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
                api = ml_model_details[x][0]
                output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
                print(source_table_and_col, api, output_table_and_col)
                connection, meta  = eq.connect_to_db_and_reflect()
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
                cache_retrivel_query = f"""SELECT id from {output_table_and_col[0]} WHERE {query_info['where_condition'][0]}"""
                print("Getting the affected rows")
                cur = conn.cursor()
                cur.execute(cache_retrivel_query)
                list_of_rows_affected = [item for tup in cur.fetchall() for item in tup]
                print(list_of_rows_affected)
                eq.fill_cache(list_of_rows_affected, output_table_and_col[0])
                conn.commit()
                cur.close()
                break
#             source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
#             api = ml_model_details[x][0]
#             output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
#             print(source_table_and_col, api, output_table_and_col)
#             # print(output_table_and_col)
#             connection, _  = eq.connect_to_db_and_reflect()
#             for chunk in pd.read_sql_table(source_table_and_col[0], connection, chunksize=1):
#                 primary_key = chunk['id'].values[0]
#                 payload = chunk[source_table_and_col[1]].values[0]
#                 response = requests.request("GET", api, params={'text':payload})
#                 result = response.text
#                 # result = result.replace('\n','')
#                 inserting_query = f"""INSERT INTO {output_table_and_col[0]} (id, {output_table_and_col[1]}) VALUES ({primary_key},'{result}');""" # hack
#                 print(inserting_query)
#                 print("Executed the queries")
#                 cur = conn.cursor()
#                 cur.execute(inserting_query)
#                 # close the connection
#                 conn.commit()
#                 cur.close()
#         print("ML Population done")
#     except Exception as e: print(e)
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