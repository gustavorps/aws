import json
import boto3
from os import environ

DB_DRIVER = environ('DB_DRIVER')
try:
    if DB_DRIVER == 'psycopg2':
        import psycopg2 as _driver
    elif DB_DRIVER == 'pymysql':
        import pymysql as _driver
    else:
        raise ValueError("Unsupported DB_DRIVER specified in environment variables.")
except Exception as e:
    raise e


class RDSProxyUtil():
    ENV_AWS_RDS_PROXY_ENDPOINT = environ.get('AWS_RDS_PROXY_ENDPOINT')  # get the rds proxy endpoint
    ENV_DB_PORT = int(environ.get('DB_PORT'))  # get the databse port
    ENV_DB_USERNAME = environ.get('DB_USERNAME')  # get the database username
    ENV_AWS_REGION = environ.get('AWS_REGION') # get the region
    ENV_DB_SCHEMA_NAME = environ.get('DB_SCHEMA_NAME') # Database name

    _kwd_mark = object()     # sentinel for separating args from kwargs
    _connections = {}  # cache for connections

    def __init__(self, rds_client):
        self.rds_client = rds_client

    def generate_db_auth_token(self):
        # generate the authentication token -- temporary password
        token = self.rds_client.generate_db_auth_token(
            DBHostname=self.ENV_AWS_RDS_PROXY_ENDPOINT,
            Port=self.ENV_DB_PORT,
            DBUsername=self.ENV_DB_USERNAME,
            Region=self.ENV_AWS_REGION
        )
        return token

    def create_new_connection(self, token, driver, charset='utf8mb4', ssl_use=True):
        key = tuple(token, driver, charset, ssl_use)
        try: return self._connections[key]
        except KeyError: pass

        try:
            # create a connection object
            connection = pymysql.connect(
                host=self.ENV_AWS_RDS_PROXY_ENDPOINT, # getting the rds proxy endpoint from the environment variables
                port=self.ENV_DB_PORT, # getting the database port from the environment variables
                user=self.ENV_DB_USERNAME, # get the database user from the environment variables
                password=token,
                db=self.ENV_DB_SCHEMA_NAME,
                charset=charset,
                cursorclass=driver.cursors.DictCursor,
                ssl={"use": ssl_use }
            )
        except Exception as e:
            return e

        self._connections[key] = connection
        return connection


def lambda_handler(event, context):
    # TODO implement
    rds_proxy_util = RDSProxyUtil(rds_client)
    conn = rds_proxy_util.create_new_connection(
        token=rds_proxy_util.generate_db_auth_token(),
        driver=_driver,
    )
    cursor = conn.cursor()
    query = "SELECT count(*) FROM `Your_Table` Where `condition`"
    cursor.execute(query)
    results = cursor.fetchmany(4)
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }