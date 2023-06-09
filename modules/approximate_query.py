import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import sqlparse
from sqlalchemy.schema import MetaData


class ApproxQuery:
    def __init__(self, query, yaml_parsed, conn):
        """
        Initialize ExactQuery class with query, yaml_parsed and conn.
        """
        self.query = query
        self.yaml_parsed = yaml_parsed
        self.conn = conn

    def extract_query_info(self):
        """
        Extract columns and tables information from the SQL query.
        """
        parsed_query = sqlparse.parse(self.query)

        columns = []
        tables = []
        conditions = []
        where_condition = []

        for statement in parsed_query:
            for token in statement.tokens:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        columns.append(str(identifier))
                elif isinstance(token, sqlparse.sql.Identifier):
                    tables.append(str(token))
                elif isinstance(token, sqlparse.sql.Comparison):
                    conditions.append(str(token))
                elif isinstance(token, sqlparse.sql.Where):
                    where_condition.append(str(token).replace('WHERE ',''))

        return {
            'columns': columns,
            'tables': tables,
            'conditions': conditions,
            'where_condition': where_condition
        }

    def get_ml_model_details(self):
        """
        Get machine learning model details from the yaml_parsed.
        """
        ml_model_details = {}
        for i in self.yaml_parsed['tables']:
            if i['is_aidb'] == True:
                model_api = i['model']
                mapping = i['mapping']
                ml_model_details[i['name']] = model_api, mapping
        return ml_model_details

    def execute_queries(self, query_info):
        """
        Execute the queries to check if tables are empty.
        """
        empty_cache_query = """select true from cache limit 1;"""
        try:
            print("Executing the queries")
            cur = self.conn.cursor()
            cur.execute(empty_cache_query)
            length_of_tables = len(cur.fetchall())
            # close the connection
            self.conn.commit()
            cur.close()
            return length_of_tables
        except Exception as e:
            print(e)

    def connect_to_db_and_reflect(self):
        """
        Connect to the database and reflect the metadata.
        """
        url = URL.create(
            drivername="postgresql",
            username=self.yaml_parsed['database']['user'],
            host=self.yaml_parsed['database']['host'],
            database=self.yaml_parsed['database']['name'],
            password=self.yaml_parsed['database']['password']
        )
        engine = create_engine(url)
        connection = engine.connect()
        meta = MetaData()
        meta.reflect(bind=engine)
        return connection, meta
    
    def fill_cache(self, rows_affected, column_name):
        query = f"""INSERT INTO cache (model_name, scope) VALUES ({column_name}, ARRAY[{rows_affected}]::integer[]);"""
        try:
            print("Executing the queries")
            cur = self.conn.cursor()
            cur.execute(query)
            # length_of_tables = len(cur.fetchall())
            # close the connection
            self.conn.commit()
            cur.close()
            print("Added a null value")
        except Exception as e:
            print(e)