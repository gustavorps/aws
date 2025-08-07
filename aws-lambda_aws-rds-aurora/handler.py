import json
from os import environ

import boto3


ENV_AWS_RDS_ARN = environ.get('AWS_RDS_ARN')
ENV_AWS_RDS_SECRET_ARN = environ.get('AWS_RDS_SECRET_ARN') 
ENV_AWS_RDS_DB_NAME = environ.get('AWS_RDS_DB_NAME')
ENV_AWS_RDS_SCHEMA_NAME = environ.get('DB_SCHEMA_NAME') # Database name

rds_data = boto3.client('rds-data')

def lambda_handler(event, context):
    # maximum size of the HTTP request submitted through the Data API is 4 MiB
    sql = """
INSERT INTO field VALUES
  ('c5fa5e9a...', 'cdeff402..', '3659b5e6...'),
  ('c5fa5e9a...', 'cdeff402..', '17bc73ed...'),
  ('c5fa5e9a...', 'cdeff402..', '827a7c92...'),
  ('c5fa5e9a...', '2e03a138..', '49077e57...'),
  ('c5fa5e9a...', '2e03a138..', '7e49db5f...')
"
    # If a call isn’t part of a transaction because it doesn’t include the 
    # transactionID parameter, changes that result from the call are 
    # committed automatically.
    rds_data.batch_execute_statement(
        resourceArn=ENV_AWS_RDS_ARN,
        secretArn=ENV_AWS_RDS_SECRET_ARN,
        database=ENV_AWS_RDS_DB_NAME,
        schema=ENV_AWS_RDS_SCHEMA_NAME,
        sql=sql,
    )

    
    cursor.execute(query)
    results = cursor.fetchmany(4)
    
    return {
        'statusCode': 204
    }
