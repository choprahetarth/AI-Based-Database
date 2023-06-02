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

yaml_parsed  = read_yaml("config.yaml")
connect_to_database(yaml_parsed) 