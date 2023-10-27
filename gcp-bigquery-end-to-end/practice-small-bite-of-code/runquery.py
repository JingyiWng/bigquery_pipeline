# This script uses Query Method in Python. So you leverage SQL more, rather than pure Python.

from google.cloud import bigquery

SERVICE_ACCOUNT_JSON = '/Users/jennwang/Downloads/strong-bus-402615-a90440e73fbe.json'


client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

query_to_run = "SELECT * FROM strong-bus-402615.myfirstdataset.names LIMIT 5"

#Make an API request using query method within the client class

query_job = client.query(query_to_run)

print(query_job)
print("Success")

for row in query_job:
    print(str(row[0]),",",str(row[1]),",",str(row[2]),",")