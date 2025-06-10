from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, helpers
import boto3
import pandas as pd
import json
import requests
region = 'us-east-1'  # replace with your region
service = 'aoss'
credentials = boto3.Session().get_credentials()
awsauth = AWSV4SignerAuthauth = AWSV4SignerAuth(credentials, region, service)

#os_client = OpenSearch(
#     hosts = [{"host": host, "port": 443}],
#     http_auth = awsauth,
#     use_ssl = True,
#     verify_certs = True,
#     connection_class = RequestsHttpConnection,
#     pool_maxsize = 20

#    )
# Collection endpoint - replace with your endpoint
host = 'https://iiu0o713lyeekl244zdg.us-east-1.aoss.amazonaws.com'

# Index name
index_name = 'oscars_index'

# Method 1: Retrieve a specific document by ID
def get_document_by_id(doc_id):
    url = f'{host}/{index_name}/_doc/{doc_id}'
    response = requests.get(url, auth=awsauth)
    return response.json()

print(get_document_by_id('1%3A0%3AHykZTZcBqYt32BX6fupy'))
