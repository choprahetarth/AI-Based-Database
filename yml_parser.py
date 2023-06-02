import yaml
import psycopg2

def read_yaml(path):
    with open(path, "r") as stream:
        try:
            yaml_parsed = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return yaml_parsed

def connect_to_database(yaml_parsed):
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
    except:
        print("Unexpected error in database")


def read_structure_of_tables(yaml_parsed):
    queries = []
    for table in yaml_parsed["tables"]:
        if table['is_aidb']==True:
            prefix="ai_"
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
        # close the connection
        conn.commit()
        cur.close()
        conn.close()
    except:
        print("Could not execute the queries")


yaml_parsed  = read_yaml("config.yaml")
queries = read_structure_of_tables(yaml_parsed)
execute_queries(queries,yaml_parsed)

