import os
import json
import time
from google.cloud import pubsub_v1
from concurrent.futures import wait

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/kthaniko/pub_sub_key.json"

project_id = "parabolic-grid-456118-u8"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open("bcsample.json", "r") as file:
    records = json.load(file)

futures = []
msg_count = 0
start_time = time.time()

print(f"\nPublishing {len(records)} breadcrumb records...\n")

for record in records:
    message = json.dumps(record).encode("utf-8")
    future = publisher.publish(topic_path, data=message)
    futures.append(future)
    msg_count += 1

wait(futures)

end_time = time.time()
print(f"\nPublishing Complete:")
print(f"Total breadcrumbs published: {msg_count}")
print(f"Time taken: {end_time - start_time:.2f} seconds\n")
