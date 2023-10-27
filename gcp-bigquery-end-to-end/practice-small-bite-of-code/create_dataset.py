# This script uses pure Python.

from google.cloud import bigquery
SERVICE_ACCOUNT_JSON = '/Users/jennwang/Downloads/strong-bus-402615-a90440e73fbe.json'

# Construct a BQ Client object using the service key
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON) #Client is a class. It acts as an interface and manages the connections to BigQuery API. It has many useful methods


dataset_id = "strong-bus-402615.dataset_py"

#Construct a full dataset object to send to the API
dataset = bigquery.Dataset(dataset_id) #Dataset() is a class

#Use location attribute to set the location
dataset.location = "US"
dataset.description = 'My dataset from Python'

#To create the dataset, make an API request with an explicit timeout
dataset_ref = client.create_dataset(dataset, timeout=30)
print("Created dataset {}.{}".format(client.project, dataset_ref.dataset_id))
