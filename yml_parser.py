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

def create_schema(yaml_parsed):
    # try:
    conn = psycopg2.connect(
        host=yaml_parsed['database']['host'],
        database=yaml_parsed['database']['name'],
        user=yaml_parsed['database']['user'],
        password=yaml_parsed['database']['password'],
        port=yaml_parsed['database']['port'])
    # initiate the cursor
    print("Connected to the Database")

    # initiate the cursor
    cur = conn.cursor()
    # execute the query
    # iterate over tables
    for i,j in enumerate(yaml_parsed['tables'].values()):
        print(j)
    # close the connection
    cur.close()
    conn.close()
    # except:
    #     print("ahahahhahhah")
    #     cur.execute("""CREATE TABLE IF NOT EXISTS mongodb
    #     (
    #         id SERIAL PRIMARY KEY,
    #         tweet TEXT NOT NULL
    #     )"""
    #                 )

    # cur.execute("""CREATE TABLE IF NOT EXISTS sentiment
    # (
    #     id INTEGER NOT NULL,
    #     sentiment BOOLEAN,
    #     FOREIGN KEY (id) REFERENCES mongodb
    #     (id) ON DELETE CASCADE
    # )
    # """)

    # cur.execute("""  CREATE TABLE closest_topic (
    #         id INTEGER NOT NULL,
    #         topic TEXT,
    #         FOREIGN KEY (id) REFERENCES mongodb(id) ON DELETE CASCADE
    #     );
    # """)

    # # close the connection
    # conn.commit()
    # cur.close()
    # conn.close()

yaml_parsed  = read_yaml("config.yaml")
connect_to_database(yaml_parsed) 
create_schema(yaml_parsed)