##

import os
import time
import requests
from google.cloud import pubsub_v1

# local modules
from constants import Constants as run_constants

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = run_constants.serviceAccount.value

# setting topic client
publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    run_constants.gcp_project_name.value, 
    run_constants.gcp_user_input_topic.value
)

while True:

    try:
        response = requests.get(run_constants.user_api_url.value)

        data = response.content

        future = publisher.publish(topic_path, data=data)

        print(future.result())
    
    except Exception as e: # TODO: better handle this exception
        print("Error: ", e)
        pass


    time.sleep(20)

