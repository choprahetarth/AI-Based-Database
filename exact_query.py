import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import sqlparse
import requests
from sqlalchemy.schema import MetaData
from modules.read_yaml import YamlBuilder
from modules.connect_to_database import ConnectDb

class ExactQuery():
    def __init__(self, query, yaml_parsed, conn):
        self.query = query
        self.yaml_parsed = yaml_parsed
        self.conn = conn 

    def extract_query_info(self):
        parsed_query = sqlparse.parse(self.query)

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

    def get_ml_model_details(self):
        ml_model_details={}
        for i in self.yaml_parsed['tables']:
            if i['is_aidb']==True:
                model_api = i['model']
                mapping = i['mapping']
                ml_model_details[i['name']] = model_api,mapping
        return ml_model_details


    def execute_queries(self,queries):
        try:
            print("Executing the queries")
            cur = self.conn.cursor()
            length_of_tables=0
            for i in queries:
                cur.execute(i)
                length_of_tables+=len(cur.fetchall())
            # close the connection
            self.conn.commit()
            cur.close()
            return length_of_tables
        except Exception as e: print(e)


    def connect_to_db_and_reflect(self):
        url = URL.create(
            drivername="postgresql",
            username=self.yaml_parsed['database']['user'],
            host=self.yaml_parsed['database']['host'],
            database=self.yaml_parsed['database']['name'],
            password = self.yaml_parsed['database']['password']
        )
        engine = create_engine(url)
        connection = engine.connect()
        meta = MetaData()
        meta.reflect(bind=engine)
        return connection, meta



#### read the yaml file
y = YamlBuilder('config.yaml')
yaml_parsed  = y.read_yaml()

#### connect the database
d = ConnectDb(yaml_parsed)
conn = d.establish_connection()
# yaml_parsed  = read_yaml("config.yaml")

query = """SELECT AVG(LENGTH(topic))
        FROM closest_topic
        JOIN sentiment_analysis ON closest_topic.id = sentiment_analysis.id
        WHERE sentiment_analysis.sentiment = '{"sentiment":"neutral"}
';"""

# instantiate the exact query object
eq = ExactQuery(query, yaml_parsed, conn)

# get query elements
query_info = eq.extract_query_info()
print("Tables:", query_info['tables'])

# check if table is empty (for the first run)
empty_queries = []
for i in query_info['tables']:
    empty_queries.append(f"""select true from {i} limit 1;""")

length_of_tables = eq.execute_queries(empty_queries)
print(length_of_tables)
# if 0, we know that it has to be populated with ML Query 


ml_model_details = eq.get_ml_model_details()

# identify unstructured dataset
print(ml_model_details)
if length_of_tables==0:
    try:
        for (x,y) in ml_model_details.items():
            source_table_and_col = ml_model_details[x][1][0]['input'].split('.',1)
            api = ml_model_details[x][0]
            output_table_and_col = ml_model_details[x][1][0]['output'].split('.',1)
            # print(output_table_and_col)
            connection, _  = connect_to_db_and_reflect(yaml_parsed)
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
        print("ML Population done")
    except Exception as e: print(e)
else:
    print("DB Already Populated")

print("Fetching Results of Exact query")

try:
    # conn = psycopg2.connect(
    #     host=yaml_parsed['database']['host'],
    #     database=yaml_parsed['database']['name'],
    #     user=yaml_parsed['database']['user'],
    #     password=yaml_parsed['database']['password'],
    #     port=yaml_parsed['database']['port'])
    # initiate the cursor
    print("Executed the queries")
    cur = conn.cursor()
    cur.execute(query)
    print(cur.fetchall())
    # close the connection
    cur.close()
except Exception as e: print(e)


conn.close()