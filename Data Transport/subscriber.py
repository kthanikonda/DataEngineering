import os
import json
from datetime import datetime
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/kthaniko/pub_sub_key.json"

project_id = "parabolic-grid-456118-u8"
subscription_id = "my-sub"

timeout = 40.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

msg_count = 0

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global msg_count
    try:
        data = message.data.decode("utf-8")
        print(f"Received message: {message.data.decode('utf-8')} with attributes {message.attributes}")
        msg_count += 1

        message.ack()

    except Exception as e:
        print(f"Error processing message: {e}")
        message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

start_time = time.time()

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
      
end_time = time.time()

print(f"\nTotal number of messages received: {msg_count}")
print(f"Time taken to consume messages: {end_time - start_time:.2f} seconds\n")
