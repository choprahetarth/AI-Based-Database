import requests
import pandas as pd
import os
import streamlit as st
from modules.read_yaml import YamlBuilder
from modules.connect_to_database import ConnectDb
from modules.exact_query import ExactQuery
from modules.yml_parser import CreateSchema


def read_yaml_file(yaml_file):
    y = YamlBuilder(yaml_file)
    return y.read_yaml()


def connect_to_database(yaml_parsed):
    d = ConnectDb(yaml_parsed)
    return d.establish_connection()


def create_and_populate_schema(yaml_parsed, conn):
    c = CreateSchema(yaml_parsed)
    queries = c.read_structure_of_tables()
    c.execute_queries(queries, conn)
    c.populate_unstructured(conn)


def execute_ml_population(yaml_parsed, conn, eq):
    ml_model_details = eq.get_ml_model_details()
    for (x, y) in ml_model_details.items():
        source_table_and_col = ml_model_details[x][1][0]['input'].split('.', 1)
        api = ml_model_details[x][0]
        output_table_and_col = ml_model_details[x][1][0]['output'].split('.', 1)
        connection, _ = eq.connect_to_db_and_reflect()
        for chunk in pd.read_sql_table(source_table_and_col[0], connection, chunksize=1):
            primary_key = chunk['id'].values[0]
            payload = chunk[source_table_and_col[1]].values[0]
            response = requests.request("GET", api, params={'text': payload})
            result = response.text
            inserting_query = f"""INSERT INTO {output_table_and_col[0]} (id, {output_table_and_col[1]}) VALUES ({primary_key},'{result}');"""
            cur = conn.cursor()
            cur.execute(inserting_query)
            conn.commit()
            cur.close()


def execute_exact_query(query, conn):
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    return result


st.header('AIDB-Exact Query')

yaml_file = st.file_uploader('Upload your resume')

if yaml_file:
    with open(os.path.join('config.yaml'), 'wb') as f:
        f.write(yaml_file.getbuffer())
    st.success('yaml file saved locally')

query = st.text_input("PASTE EXACT QUERY")

if yaml_file and query:
    yaml_parsed = read_yaml_file('config.yaml')
    conn = connect_to_database(yaml_parsed)

if st.button('Make Schema') and query and yaml_file:
    create_and_populate_schema(yaml_parsed, conn)
    st.write('DB is created and populated with Tweets')

if st.button('Compute!') and query and yaml_file:
    eq = ExactQuery(query, yaml_parsed, conn)
    query_info = eq.extract_query_info()
    length_of_tables = eq.execute_queries(query_info)

    if length_of_tables == 0:
        try:
            execute_ml_population(yaml_parsed, conn, eq)
            st.write("ML Population done")
        except Exception as e:
            print(e)
    else:
        st.write("DB Already Populated")

    st.write("Fetching Results of Exact query")

    # try:
    result = execute_exact_query(query, conn)
    output_of_the_exact_query = str(float(list_of_output[0][0]))
    print(output_of_the_exact_query)
    st.write(output_of_the_exact_query)
    # except Exception as e:
    #     print(e)

    conn.close()