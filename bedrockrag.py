# https://www.youtube.com/watch?v=Lq0JuIOX4jM&t=102s

# https://www.youtube.com/watch?v=BXgaK8PPZAE

# https://www.youtube.com/watch?v=RIw_Ivvrp8g


from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, helpers
import boto3
import pandas as pd
import json

br_client = boto3.client("bedrock-runtime")

host = "iiu0o713lyeekl244zdg.us-east-1.aoss.amazonaws.com"
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
print(type(os_client))

df = pd.read_csv("/Users/rjdaskevich/Downloads/the_oscar_award.csv")
df.info()
df = df.loc[df['year_ceremony'] == 2024]
df = df.dropna(subset=['film'])

df['category'] = df['category'].str.lower()
df.head()


df['text'] = df['name'] + ' got nominated under the category, ' +  df['category'] + ', for the film ' + df['film']
won_text = "and won"
df.loc[df['winner'] == True, 'text'] = df['text'] + ' ' + won_text
pd.set_option('max_colwidth', 100)
df['text'].values[3]

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
subset=df.iloc[:5]
df=df.assign(embedding=(df['text'].apply(lambda x:text_embedding(x))))
print(df['embedding'].values[0][:5])
print(len(df['embedding'].values[0]))

def add_document(vector, text):
       document = {
       "nom_vector":vector,
       "nom_text":text      
       }
       response = os_client.index(
             index= "oscars_index",
             body = document
       )

       print("\naddingdocument:")
       print(response)
import requests  
df.apply(lambda row: add_document(row['embedding'], row['text']), axis=1)
# query = "who won the award for best music?"
# vector = text_embedding(query)

# Collection endpoint - replace with your endpoint

# print(f"query vector: {vector[:5]}")

# def search_index(vector):
#       document = {
#             "size":3,
#             "_source": {"excludes": ["nom_vector"]},
#             "query": {
#                   "knn": {
#                         "nom_vector": {
#                               "vector": vector,
#                               "k":3
#                         }
#                   }
#             }
#       }
#       response = os_client.search(
#             body = document,
#             index ="oscars_index"
# )      
#       return response
# query = "who won the award for best music?"
# vector = text_embedding(query)

# response = search_index(vector)
# data=response['hits']['hits']
# print(data)
# context = [data[element]['_source']['nom_text'] for element in range(len(data))]

# prompt = f"answer {query} based on the given context: {context}"
# config={"maxTokenCount": 1000,
#         "stopSequences": [],
#         "temperature": 0.1,
#         "topP": 1
#         }
# body=json.dumps({"inputText": prompt,"textGenerationConfig": config })
# response = br_client.invoke_model(body=body,modelId="amazon.titan-text-express-v1")
# #print(response)
# response_body = json.loads(response.get('body').read())
# #print(response_body)
# output = response_body.get('results')[0].get('outputText')
# #print(embedding)
# print(output)