import requests
import pandas as pd
import os
import streamlit as st
from modules.read_yaml import YamlBuilder
from modules.connect_to_database import ConnectDb
from modules.exact_query import ExactQuery
from modules.yml_parser import CreateSchema


st.header('AIDB-Exact Query')

# so far the implementation is only in PDF Files
# Upload resume
yaml_file = st.file_uploader('Upload your resume')
# Save resume locally
if yaml_file:
    with open(os.path.join('config.yaml'), 'wb') as f:
        f.write(yaml_file.getbuffer())
    st.success('yaml file saved locally')

# Text inputs 
query = st.text_input("PASTE EXACT QUERY")

if st.button('Compute!') and query and yaml_file:
    #### read the yaml file
    y = YamlBuilder('config.yaml')
    yaml_parsed  = y.read_yaml()
    #### connect the database
    d = ConnectDb(yaml_parsed)
    conn = d.establish_connection()

    c = CreateSchema(yaml_parsed)
    queries = c.read_structure_of_tables()
    c.execute_queries(queries,conn)
    c.populate_unstructured(conn)
    st.write('DB is created and populated with Tweets')

    # query = """SELECT AVG(LENGTH(topic))
    #         FROM closest_topic
    #         JOIN sentiment_analysis ON closest_topic.id = sentiment_analysis.id
    #         WHERE sentiment_analysis.sentiment = '{"sentiment":"neutral"}
    # ';"""

    # instantiate the exact query object
    eq = ExactQuery(query, yaml_parsed, conn)

    # get tables 
    query_info = eq.extract_query_info()
    length_of_tables = eq.execute_queries(query_info)
    # print(length_of_tables)
    ml_model_details = eq.get_ml_model_details()
    # print(ml_model_details)
    # if 0, we know that it has to be populated with ML Query 
    if length_of_tables==0:
        try:
            for (x,y) in ml_model_details.items():
                source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
                api = ml_model_details[x][0]
                output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
                # print(output_table_and_col)
                connection, _  = eq.connect_to_db_and_reflect()
                for chunk in pd.read_sql_table(source_table_and_col[0], connection, chunksize=1):
                    primary_key = chunk['id'].values[0]
                    payload = chunk[source_table_and_col[1]].values[0]
                    response = requests.request("GET", api, params={'text':payload})
                    result = response.text
                    inserting_query = f"""INSERT INTO {output_table_and_col[0]} (id, {output_table_and_col[1]}) VALUES ({primary_key},'{result}');""" # hack
                    print(inserting_query)
                    print("Executed the queries")
                    cur = conn.cursor()
                    cur.execute(inserting_query)
                    # close the connection
                    conn.commit()
                    cur.close()
            st.write("ML Population done")
        except Exception as e: print(e)
    else:
        st.write("DB Already Populated")

    st.write("Fetching Results of Exact query")

    try:
        # initiate the cursor
        st.write("Executed the queries")
        cur = conn.cursor()
        cur.execute(query)
        st.write(cur.fetchall())
        # close the connection
        cur.close()
    except Exception as e: print(e)


    conn.close()
