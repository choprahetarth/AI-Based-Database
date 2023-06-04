import yaml
import psycopg2
from faker import Faker

class CreateSchema():
    def __init__(self,path):
        self.path = path
            
    def read_yaml(self):
        with open(self.path, "rb") as stream:
            try:
                yaml_parsed = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return yaml_parsed

    def connect_to_database(self, yaml_parsed):
        # connect with the postgres instance
        try:
            conn = psycopg2.connect(
                host=yaml_parsed['database']['host'],
                database=yaml_parsed['database']['name'],
                user=yaml_parsed['database']['user'],
                password=yaml_parsed['database']['password'],
                port=yaml_parsed['database']['port'])
            # initiate the cursor
            print("Connected to the Database")
            cur = conn.cursor()
            # close the connection
            cur.close()
            conn.close()
        except Exception as e: print(e)


    def read_structure_of_tables(self, yaml_parsed):
        queries = []
        for table in yaml_parsed["tables"]:
            if table['is_aidb']==True:
                # prefix="ai_"
                prefix=""
            else:
                prefix=""
            # Build the CREATE TABLE query
            query = f"CREATE TABLE IF NOT EXISTS {prefix}{table['name']} ("

            columns = []
            for column in table['columns']:
                col = f"{column['name']} {column['type']}"

                if column.get('not_null', False):
                    col += " NOT NULL"

                if column.get('primary_key', False):
                    col += " PRIMARY KEY"

                if 'foreign_key' in column:
                    reference_table = column['foreign_key']['reference_table']
                    reference_column = column['foreign_key']['reference_column']
                    col += f", FOREIGN KEY ({reference_column}) REFERENCES {reference_table}"

                    if column['foreign_key'].get('on_delete_cascade', False):
                        col += " ON DELETE CASCADE"
                columns.append(col)

            query += ", ".join(columns)
            query += ");"
            queries.append(query)
        return queries

    def execute_queries(self, queries, yaml_parsed):
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
            # close the connection
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e: print(e)

    def populate_unstructured(self, yaml_parsed):
        try:
            conn = psycopg2.connect(
                host=yaml_parsed['database']['host'],
                database=yaml_parsed['database']['name'],
                user=yaml_parsed['database']['user'],
                password=yaml_parsed['database']['password'],
                port=yaml_parsed['database']['port'])
            # initialize the faker object
            fake = Faker()
            num_samples = 5
            print("Started populating the values")
            cur = conn.cursor()
            for _ in range(num_samples):
                for table in yaml_parsed["tables"]:
                    if table['is_aidb']==False:
                        tweet = fake.text()  # Generate a random text for the tweet
                        # Insert into the twitter table
                        query = f"INSERT INTO {table['name']}({table['unstructured_text']}) VALUES ('{tweet}') RETURNING id;"
                        cur.execute(query) 
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e: print(e)

c = CreateSchema('config.yaml')
yaml_parsed  = c.read_yaml()
queries = c.read_structure_of_tables(yaml_parsed)
c.execute_queries(queries,yaml_parsed)
c.populate_unstructured(yaml_parsed)
