import boto3
import json
from botocore.exceptions import ClientError


def get_secret(secret_name: str, region_name: str = None) -> dict:
    """Retrieve a secret from AWS Secrets Manager.

    Returns a dictionary parsed from JSON stored in the secret. The
    function relies on IAM role-based credentials attached to the
    environment (e.g., EC2, EMR, or Glue role).
    """
    client_kwargs = {}
    if region_name:
        client_kwargs["region_name"] = region_name

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", **client_kwargs)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise
    else:
        # Secrets Manager returns the secret as a string
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            # If secret is binary, decode it
            decoded_binary_secret = get_secret_value_response['SecretBinary'].decode('utf-8')
            return json.loads(decoded_binary_secret)
