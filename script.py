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
        WHERE sentiment_analysis.sentiment = '{"sentiment":3}';"""

# instantiate the exact query object
eq = ApproxQuery(query, yaml_parsed, conn)

# get tables 
query_info = eq.extract_query_info()
length_of_tables = eq.execute_queries(query_info)
ml_model_details = eq.get_ml_model_details()
print("using approximate query")


# # if 0, we know that it has to be populated with ML Query 
if length_of_tables==0:
        print("No Cache Detected, populating the Sentiment Table")
    # we know that the cache table is empty, therefore execute the first ML model to populate the rows
        for idx, x in enumerate(ml_model_details):
                if idx==0:
                        source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
                        api = ml_model_details[x][0]
                        output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
                        connection, meta  = eq.connect_to_db_and_reflect()
                        ######## POPULATE THE VALUES WITH FIRST TABLE ML #################
                        for chunk in pd.read_sql_table(source_table_and_col[0], connection, chunksize=1):
                                print("Sentiment Table is now being Populated")
                                primary_key = chunk['id'].values[0]
                                payload = chunk[source_table_and_col[1]].values[0]
                                response = requests.request("GET", 
                                                        api,
                                                        params={'text':payload},
                                                        timeout=100)
                                result = response.text
                                result = result.replace('\n','')
                                inserting_query = f"""INSERT INTO {output_table_and_col[0]} (id, {output_table_and_col[1]}) VALUES ({primary_key},'{result}');""" # hack
                                cur = conn.cursor()
                                cur.execute(inserting_query)
                                # close the connection
                        # ######### CHECK THE QUERY FOR WHICH ROWS ARE AFFECTED ################
                        cache_retrivel_query = f"""SELECT id from {output_table_and_col[0]} WHERE {query_info['where_condition'][0]}"""
                        cur = conn.cursor()
                        cur.execute(cache_retrivel_query)
                        ######### APPEND THOSE ROWS INTO THE CACHE ########################
                        list_of_rows_affected = [item for tup in cur.fetchall() for item in tup]
                        print("Adding these rows to the cache of sentiment analysis ", list_of_rows_affected)
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
                        connection, meta  = eq.connect_to_db_and_reflect()
                        list_of_rows_affected=[]
                        print("Read sentiment analysis cache from cache table, executing ML Query on those values only.")
                        for chunk in pd.read_sql_query(get_cache_for_second_row, connection, chunksize=1):
                                print("Executing NER Model on affected values which are stored in cache")
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
                                cur = conn.cursor()
                                cur.execute(inserting_query)
                                conn.commit()
                        print("Adding the rows on which NER Has run for the next cache: ", list_of_rows_affected)
                        eq.fill_cache(list_of_rows_affected, output_table_and_col[0])
                        # close the connection
                        conn.commit()
                        cur.close()
elif length_of_tables!=0:
        print("Cache Detected, Checking which rows will be affected with new condition ")
        for idx, x in enumerate(ml_model_details):
                if idx==0:
                        source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
                        api = ml_model_details[x][0]
                        output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
                        cache_retrivel_query = f"""SELECT id from {output_table_and_col[0]} WHERE {query_info['where_condition'][0]}"""
                        cur = conn.cursor()
                        cur.execute(cache_retrivel_query)
                        list_of_rows_affected = [item for tup in cur.fetchall() for item in tup]
                        eq.update_cache(list_of_rows_affected, output_table_and_col[0])
                        print("These rows will be affected with new condition, adding to cache ", list_of_rows_affected)
                        conn.commit()
                        cur.close()
                if idx==1:
                        source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
                        api = ml_model_details[x][0]
                        output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
                        connection, meta  = eq.connect_to_db_and_reflect()
                        get_cache_for_second_row = f"""SELECT scope FROM cache WHERE cache.model_name='{output_table_and_col[0]}'"""
                        for k in pd.read_sql_query(get_cache_for_second_row, connection, chunksize=1):
                                list_of_rows_affected_second = k['scope'][0]
                        list_of_rows_for_updation = list(set(list_of_rows_affected).union(set(list_of_rows_affected_second)))
                        list_of_rows_for_ml_query = [x for x in list_of_rows_affected if x not in list_of_rows_affected_second]
                        print("Total Rows to be updated for NER ", list_of_rows_for_ml_query)
                        print("Combined Rows for NER Cache ", list_of_rows_for_updation)
                        get_cache_for_second_row = f"""SELECT * from {source_table_and_col[0]} WHERE id = ANY(ARRAY{list_of_rows_for_ml_query});"""
                        if list_of_rows_for_ml_query:
                                for chunk in pd.read_sql_query(get_cache_for_second_row, connection, chunksize=1):
                                        print("Running NER on rows which need updating")
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
#                                         ######### APPEND THOSE ROWS INTO THE CACHE ########################
                                        cur = conn.cursor()
                                        cur.execute(inserting_query)
                                        conn.commit()
                                cur.close()
                                eq.update_cache(list_of_rows_for_updation, output_table_and_col[0])
                        conn.commit()
                        cur.close()


try:
    # initiate the cursor
    print("Running the SQL query ")
    #### connect the database
    conn = d.establish_connection()
    cur = conn.cursor()
    cur.execute(query)
    list_of_output = cur.fetchall()
    print((list_of_output))
    # close the connection
    cur.close()
except Exception as e: print(e)


conn.close()