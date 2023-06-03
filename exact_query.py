import yaml
import psycopg2

# def read_exact_query():


def read_yaml(path):
    with open(path, "r") as stream:
        try:
            yaml_parsed = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return yaml_parsed


from sqlalchemy import create_engine
yaml_parsed  = read_yaml("config.yaml")
engine = create_engine(f'postgresql+psycopg2://{yaml_parsed['database']['user']}:{yaml_parsed['database']['password']}@{yaml_parsed['database']['host']}/{yaml_parsed['database']['name']}')