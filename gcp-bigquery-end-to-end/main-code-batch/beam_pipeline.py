#project-id:dataset_id.table_id
delivered_table_spec = 'strong-bus-402615:dataset_food_orders_airflow.delivered_orders'
other_table_spec = 'strong-bus-402615:dataset_food_orders_airflow.other_status_orders'

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions #These packages provide some config options associated with the pipeline 
# - e.g. pipeline runner - which will execute the pipeline. In our case, will be Dataflow. Then, you can provide any runner-specific config under these 2 packages
import argparse #This Python libraray supports command-line arguments. Need this lib cuz we will provide input file name in the pipeline's run command.  


parser = argparse.ArgumentParser()
#Use add_argument method to describe the arguments which will be passed in CLI
parser.add_argument('--input', #This is the name of arguments. Should use this argument in the run command to parse the input file location
                    dest='input', #In the code, the same arguement can be retrieved with this name
                    required=True, #True means this is a mandatory argument and should be provided in the run command
                    help='Input file to process.')  #help is what description it will show if you ever use help command for it

# Once you pass the arguments in CLI, you have to capture it and use in the code
path_args, pipeline_args = parser.parse_known_args() # pipeline_args will hold the environment-related arguments (runner-type, job-name, temp file location), path_args will hold input file location
print("Jenn! Look at me! I'm pipeline_args ", pipeline_args)
#Jenn! Look at me! I'm pipeline_args  ['--temp_location', 'gs://myfirstbucket_jw']
#Jenn! Look at me! I'm inputs_pattern  gs://daily_food_orders_udemy/food_daily.csv
#Jenn! Look at me! I'm options  PipelineOptions()


#Then, to use the input file location in the code:
inputs_pattern = path_args.input #Use "input" here since we defined it at line above dest='input'
print("Jenn! Look at me! I'm inputs_pattern ", inputs_pattern)

#Create an object of pipeline options class
options = PipelineOptions(pipeline_args)
print("Jenn! Look at me! I'm options ", options)

# So far, all env configs are in 'options' above, which can be then directly parsed in the code where we will create the actual pipeline object (see next line).

#To create an object of pipeline class
p = beam.Pipeline(options=options) #The Pipeline class is important, as it fully controls the lifecycle of it. This Pipeline class is similar to what we have context in Spark.
# p above is the pipeline object. All further transformations will be applied to this object only. 

def remove_last_colon(row):
    cols = row.split(',')
    item = str(cols[4])
    if item.endswith(':'):
        cols[4] = item[:-1]
    return ','.join(cols)

def remove_special_characters(row):
    import re
    cols = row.split(',')
    res = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]','',col)
        res = res + clean_col + ','
    res = res[:-1]
    return res



cleaned_data = (
    p # All transformations on p would be applied using a pipeline operator. Final result will be stored in cleaned_data p collection.
    # p collection is a unified storage entity of Beam, that can store any batch or streaming data. Similar to dataset and RDD in spark, we have collections in Beam.
    | beam.io.ReadFromText(inputs_pattern, skip_header_lines=1) # inputs_pattern is the object containing input file location
    | beam.Map(remove_last_colon)
    | beam.Map(lambda row:row.lower())
    | beam.Map(remove_special_characters)
    | beam.Map(lambda row: row+',1') # Add ',1' to the end of each row. for later purposes e.g. when group by to count etc.

)


# Write delievered items (rows with delivered col = 'delivered') in one table, and the rest in another table. So we need to store those 2 types of data in 2 diff p collections.

delivered_orders = (
    cleaned_data
    | 'delivered filter' >>beam.Filter(lambda row: row.split(',')[8].lower()=='delivered') #'delivered filter' is the label of the transform. provide a unique label to any p transform. Useful for debugging
) 
other_orders = (
    cleaned_data
    | 'undelivered filter' >>beam.Filter(lambda row: row.split(',')[8].lower()!='delivered') 
) 

def print_row(row):
    print(row)

# For controls: do a count for all the 3 p collections above. Put it in logs
(cleaned_data
 | 'count total cleaned_data' >> beam.combiners.Count.Globally()
 | 'total map cleaned_data' >> beam.Map(lambda x: 'Total Count cleaned_data:' + str(x))
 | 'print total cleaned_data' >> beam.Map(print_row)
)
 
(delivered_orders
 | 'count total delivered_orders' >> beam.combiners.Count.Globally()
 | 'total map delivered_orders' >> beam.Map(lambda x: 'Total Count delivered_orders:' + str(x))
 | 'print total delivered_orders' >> beam.Map(print_row)
)

(other_orders
 | 'count total other_orders' >> beam.combiners.Count.Globally()
 | 'total map other_orders' >> beam.Map(lambda x: 'Total Count other_orders:' + str(x))
 | 'print total other_orders' >> beam.Map(print_row)
)

# So far, we have clean and transformed data. Now it's ready to load to BigQuery.
# First, create dataset.
from google.cloud import bigquery
# SERVICE_ACCOUNT_JSON = '/Users/jennwang/Downloads/strong-bus-402615-a90440e73fbe.json'

# No longer need to construct a BQ Client object using the service key - this is cuz we will run the code from cloud shell, not local SDK.
#client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON) #Client is a class. It acts as an interface and manages the connections to BigQuery API. It has many useful methods
client = bigquery.Client()

dataset_id = "{}.dataset_food_orders_airflow".format(client.project)

try:
    client.get_dataset(dataset_id)
except:
    #Construct a full dataset object to send to the API
    dataset = bigquery.Dataset(dataset_id) #Dataset() is a class

    #Use location attribute to set the location
    dataset.location = "US"
    dataset.description = 'My dataset from Python'

    #To create the dataset, make an API request with an explicit timeout
    dataset_ref = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset_ref.dataset_id))

# Second, we create the table. WriteToBigQuery() can create a table in BQ while loading a p collection in it. It takes JSON as input, but our p collection is currently CSV
def to_json(csv_str):
    fields = csv_str.split(',')
    json_str = {
        "customer_id": fields[0],
        "date": fields[1],
        "timestamp": fields[2],
        "order_id": fields[3],
        "items": fields[4],
        "amount": fields[5],
        "mode": fields[6],
        "restaurant": fields[7],        
        "status": fields[8],
        "ratings": fields[9],   
        "feedback": fields[10],   
        "new_col": fields[11]       
    }
    return json_str

#table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant: STRING,status:STRING,ratings:STRING,feedback:STRING,new_col: STRING'
table_schema = {
  "fields": [
  {
    "name" : "customer_id",
    "type" : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "date",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "timestamp",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "order_id",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "items",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "amount",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "mode",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "restaurant",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "status",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "ratings",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "feedback",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  },
  {
    "name" : "new_col",
    "type"  : "STRING",
    "mode" : "NULLABLE"
  }     
 ]
}

(delivered_orders
 | 'transform to json delivered_orders' >> beam.Map(to_json)
 | 'write delivered delivered_orders' >> beam.io.WriteToBigQuery(
     delivered_table_spec, #table name
     schema = table_schema,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
     additional_bq_parameters={'timePartitioning':{'type':'DAY'}} # Ingestion-based daily partition 
    )
)

(other_orders
 | 'transform to json other_orders' >> beam.Map(to_json)
 | 'write other other_orders' >> beam.io.WriteToBigQuery(
     other_table_spec, #table name
     schema = table_schema,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
     additional_bq_parameters={'timePartitioning':{'type':'DAY'}} # Ingestion-based daily partition 
    )
)

# Only doing line below works. But won't let you know if the run is successful or not. In stead, do the block below
#p.run()

from apache_beam.runners.runner import PipelineState
res = p.run()
if res.state == PipelineState.DONE:
    print('Success')
else:
    print('Error running beam pipeline')


#Create a view on the delivered table - to store daily data from current_date only
view_name = "delivered_orders_vw"
dataset_ref = client.dataset('dataset_food_orders_airflow')
view_ref = dataset_ref.table(view_name) # will hold: project_name:dataset_name.table_name
view_to_create = bigquery.Table(view_ref) # Until here, it's a table. Now, create a view using view_query
view_to_create.view_query = 'SELECT * FROM `strong-bus-402615:dataset_food_orders_airflow.delivered_orders`'
view_to_create.view_use_legacy_sql = False

try: # Since this is a one-time only code, use Try & Except
    client.create_table(view_to_create) # This line will create the view
except:
    print('View already exists')

# Final notes: when running on cloud shell: # Recall that on Cloud Shell, I did this: $ python3 beam_pipeline.py --input gs://daily_food_orders_udemy/food_daily.csv --temp_location gs://myfirstbucket_jw