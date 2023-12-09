import boto3
from botocore.exceptions import ClientError
import json


def __get_secret():
    region_name = "eu-west-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId="snowflake/integrated-exercise/alex-login"
        )
    except ClientError as e:
        raise e

    return json.loads(get_secret_value_response['SecretString'])

snowflake_options = None
def get_snowflake_creds_from_secret_manager() -> dict[str, str]:
    global snowflake_options
    if snowflake_options is None:
        credentials = __get_secret()
        snowflake_options = {
            "sfURL": f"{credentials['URL']}",
            "sfPassword": credentials["PASSWORD"],
            "sfUser": credentials["USER_NAME"],
            "sfDatabase": credentials["DATABASE"],
            "sfWarehouse": credentials["WAREHOUSE"],
            'sfSchema': credentials['SCHEMA'],
            "sfRole": credentials["ROLE"]
        }
    return snowflake_options