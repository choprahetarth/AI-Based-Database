"""This module is used to connect to the database"""
import psycopg2


class ConnectDb():
    '''just a class the entire module can use for
    connecting to the database'''
    def __init__(self, yaml_parsed):
        self.yaml_parsed=yaml_parsed

    def establish_connection(self):
        ''' this function is used to establish
        connection to the postgresql server'''
        conn = psycopg2.connect(
                    host=self.yaml_parsed['database']['host'],
                    database=self.yaml_parsed['database']['name'],
                    user=self.yaml_parsed['database']['user'],
                    password=self.yaml_parsed['database']['password'],
                    port=self.yaml_parsed['database']['port'])
        return conn