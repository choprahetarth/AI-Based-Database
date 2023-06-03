import yaml
import psycopg2
import pandas as pd


# query = """SELECT AVG(LENGTH(tweet)) FROM twitter 
# WHERE LENGTH(tweet) >120"""
# columns, rows, tables = identify_query_columns_rows_tables(query,yaml_parsed)
# print(columns,rows, tables)
# from sqlalchemy import create_engine
# from sqlalchemy.engine import URL
# # from sqlalchemy.orm import sessionmaker




# # parse YAML file
# # parse URL 
# url = URL.create(
#     drivername="postgresql",
#     username=yaml_parsed['database']['user'],
#     host=yaml_parsed['database']['host'],
#     database=yaml_parsed['database']['name'],
#     password = yaml_parsed['database']['password']
# )

# engine = create_engine(url)
# connection = engine.connect()
# # table named 'contacts' will be returned as a dataframe.
# df = pd.read_sql_table('twitter', connection)
# print(df)


import sqlparse


def read_yaml(path):
    with open(path, "r") as stream:
        try:
            yaml_parsed = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return yaml_parsed

def extract_query_info(query):
    parsed_query = sqlparse.parse(query)

    columns = []
    tables = []

    for statement in parsed_query:
        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    columns.append(str(identifier))
            elif isinstance(token, sqlparse.sql.Identifier):
                tables.append(str(token))
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                from_seen = True

    return {
        'columns': columns,
        'tables': tables,
    }


def execute_queries(queries, yaml_parsed):
    try:
        conn = psycopg2.connect(
            host=yaml_parsed['database']['host'],
            database=yaml_parsed['database']['name'],
            user=yaml_parsed['database']['user'],
            password=yaml_parsed['database']['password'],
            port=yaml_parsed['database']['port'])
        # initiate the cursor
        print("Executed the queries")
        cur = conn.cursor()
        for i in queries:
            cur.execute(i)
            print(cur.fetchall())
        # close the connection
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e: print(e)

yaml_parsed  = read_yaml("config.yaml")

query = """SELECT AVG(LENGTH(topic))
        FROM closest_topic
        JOIN sentiment_analysis ON closest_topic.id = sentiment_analysis.id
        WHERE sentiment_analysis.sentiment = 'false';"""
query_info = extract_query_info(query)
print("Tables:", query_info['tables'])

# check if table is empty (for the first run)
empty_queries = []
for i in query_info['tables']:
    empty_queries.append(f"""select true from {i} limit 1;""")

execute_queries(empty_queries, yaml_parsed)
