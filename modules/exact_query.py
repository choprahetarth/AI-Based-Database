import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import sqlparse
from sqlalchemy.schema import MetaData


class ExactQuery:
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
        empty_queries = []
        for i in query_info['tables']:
            empty_queries.append(f"""select true from {i} limit 1;""")
        try:
            print("Executing the queries")
            cur = self.conn.cursor()
            length_of_tables = 0
            for i in empty_queries:
                cur.execute(i)
                length_of_tables += len(cur.fetchall())
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