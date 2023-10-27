from airflow import models
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 10, 26),
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
    'dataflow_default_options':{
        'project': 'strong-bus-402615',
        'region': 'northamerica-northeast1', # this is the region of compute engine
        'runner': 'DataflowRunner' # Runner where the pipeline will run. Could be Spark, Flink, or any supported runner of Beam. Here's Dataflowrunner
    }
}

#Instantiate a DAG object
with models.DAG('food_orders_dag',
                default_args = default_args, # will pass all the dictionary args mentioned above to each task instructor that we are going to define
                schedule_interval='@daily',
                catchup = False
                ) as dag:
    
    # Actually define the tasks. What we actually want to run/schedule is specified as task
    t1 = DataFlowPythonOperator(
        task_id = 'beamtask',
        py_file = 'gs://northamerica-northeast1-foo-720eb588-bucket/beam_pipeline.py', # There's a bucket auto-created by the Cloud Composer, to store the DAG files. Need to upload the Python file that u want to schedule to this bucket here. 
        options = {'input':'gs://daily_food_orders_udemy/food_daily.csv'} # These are job-specific options. Unlike Cloud Shell where I need to give a temporary location, DataFlow itself will create a staging location.
        # Recall that on Cloud Shell, I did this: $ python3 beam_pipeline.py --input gs://daily_food_orders_udemy/food_daily.csv --temp_location gs://myfirstbucket_jw
    )

