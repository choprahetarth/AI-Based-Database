from faker import Faker

class CreateSchema():
    def __init__(self, yaml_parsed):
        print("Initiated Schema Building")
        self.yaml_parsed=yaml_parsed

    def read_structure_of_tables(self):
        queries = []
        for table in self.yaml_parsed["tables"]:
            # Build the CREATE TABLE query
            query = f"CREATE TABLE IF NOT EXISTS {table['name']} ("

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
        # add approximate tables
        approx_query='CREATE TABLE IF NOT EXISTS cache (model_name TEXT, scope INTEGER[]);'
        queries.append(approx_query)
        return queries

    def execute_queries(self, queries, conn):
        try:
            # initiate the cursor
            print("Executed the queries")
            cur = conn.cursor()
            for i in queries:
                cur.execute(i)
            # close the connection
            conn.commit()
            cur.close()
        except Exception as e: print(e)

    def populate_unstructured(self, conn):
        try:
            # initialize the faker object
            fake = Faker()
            num_samples = 5
            print("Started populating the values")
            cur = conn.cursor()
            for _ in range(num_samples):
                for table in self.yaml_parsed["tables"]:
                    if table['is_aidb']==False:
                        tweet = fake.text()  # Generate a random text for the tweet
                        # Insert into the twitter table
                        query = f"INSERT INTO {table['name']}({table['unstructured_text']}) VALUES ('{tweet}') RETURNING id;"
                        cur.execute(query) 
            conn.commit()
            cur.close()
            print("Completed the population")
        except Exception as e: print(e)



# from read_yaml import YamlBuilder
# from connect_to_database import ConnectDb


# y = YamlBuilder('config.yaml')
# yaml_parsed  = y.read_yaml()

# d = ConnectDb(yaml_parsed)
# conn = d.establish_connection()

# c = CreateSchema(yaml_parsed)
# queries = c.read_structure_of_tables()
# c.execute_queries(queries,conn)
# c.populate_unstructured(conn)
# # close the connection
# conn.close()