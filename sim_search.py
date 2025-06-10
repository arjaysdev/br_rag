from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, helpers
import boto3
import pandas as pd
import json

br_client = boto3.client("bedrock-runtime")

host = "<opensearch serverless connection endpoint"
region = 'us-east-1'
service = "aoss"
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)

os_client = OpenSearch(
    hosts = [{"host": host, "port": 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20

   )

def text_embedding(text):
      print("let's embed")
      body=json.dumps({"inputText": text})
      response = br_client.invoke_model(body=body,modelId="amazon.titan-embed-text-v1")
      #print(response)
      response_body = json.loads(response.get('body').read())
      #print(response_body)
      embedding = response_body.get('embedding')
      #print(embedding)
      return embedding

query = "who won the award for best song in 2023?"
vector = text_embedding(query)

print(f"query vector: {vector[:5]}")

def search_index(vector):
      document = {
            "size":10,
            "_source": {"excludes": ["nom_vector"]},
            "query": {
                  "knn": {
                        "nom_vector": {
                              "vector": vector,
                              "k":10
                        }
                  }
            }
      }
      response = os_client.search(
            body = document,
            index ="oscars_index"
)      
      return response
query = "who won the award for best song?"
vector = text_embedding(query)

response = search_index(vector)
data=response['hits']['hits']
print(data)
context = [data[element]['_source']['nom_text'] for element in range(len(data))]

prompt = f"answer {query} based on the given context: {context}"
config={"maxTokenCount": 1000,
        "stopSequences": [],
        "temperature": 0.1,
        "topP": 1
        }
body=json.dumps({"inputText": prompt,"textGenerationConfig": config })
response = br_client.invoke_model(body=body,modelId="amazon.titan-text-express-v1")
#print(response)
response_body = json.loads(response.get('body').read())
#print(response_body)
output = response_body.get('results')[0].get('outputText')
#print(embedding)
print(output)
