from google.cloud import pubsub_v1
import time

# Similar to BigQuery, create a client - in this case, it's a publisher client. 
publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project='strong-bus-402615', topic='mimic_data_streaming_v2')


# topic_name = 'projects//topics/mimic_data_streaming_jw' # format: project/projectName/topics/topicName
# topic = publisher.create_topic(request={"name": topic_path})

# Create the topic using createTopic method. Since this is one-time code, use try except.
try:
    publisher.create_topic(request={"name": topic_path})
except:
    print('Topic already exists')


# Now, read the file and publush the messages in a topic
with open('food_daily.csv') as f_in:
    for line in f_in:
        data = line
        future = publisher.publish(topic_path, data.encode("utf-8"))
        print(future.result())
        time.sleep(1) # This is to mimic streaming, otherwise, data would be pubblished at once. 



# This Python script is publishing msgs to a topic, and you can pull these messages from a subscription. 
# Now, we have streaming data available in a pub/sub topic. Now, I can ingest it in my Beam pipeline, transform it, and write to BigQuery