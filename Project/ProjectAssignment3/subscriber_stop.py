import os
import json
import logging
import time
from datetime import datetime
from typing import List, Dict
from google.cloud import pubsub_v1
import psycopg2
import pandas as pd

# Set up a log file to record events and errors during pipeline execution
log_file = "stop_event_pipeline.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Set up GCP credentials and define the Pub/Sub subscription path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/kthaniko/pub_sub_key.json"
project_id = "parabolic-grid-456118-u8"
subscription_id = "my-sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# PostgreSQL database connection settings
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "DataEngineering"
}

# Global variables
idle_timeout = 60  # seconds
start_time = time.time()
total_received = 0
total_loaded = 0
last_message_time = datetime.now()

# Class to clean and validate stop event records
class StopEventCleaner:
    def __init__(self):
        self.processed_combinations = set()

    def process_and_validate(self, records: List[Dict]) -> pd.DataFrame:
        cleaned_data = []
        unique_entries = set()

        for record in records:
            record_key = (record.get("trip_id"), record.get("vehicle_id"))
            if record_key in unique_entries:
                continue
            unique_entries.add(record_key)

            try:
                if not self.is_valid_record(record):
                    continue
            except Exception as e:
                logging.warning(f"Validation failed for stop event: {record.get('trip_id')}, Reason: {e}")
                continue

            direction = 'Out' if str(record.get("direction")) == '0' else 'Back'
            service_key_map = {'W': 'Weekday', 'S': 'Saturday', 'U': 'Sunday'}
            service_key = service_key_map.get(record.get("service_key", "W"), 'Weekday')

            try:
                cleaned_data.append({
                    "trip_id": int(record["trip_id"]),
                    "route_id": int(record.get("route_number", 0)),
                    "vehicle_id": int(record["vehicle_id"]),
                    "service_key": service_key,
                    "direction": direction
                })
            except Exception as e:
                logging.warning(f"Failed to convert values for record {record.get('trip_id')}: {e}")

        logging.info(f"Validated {len(cleaned_data)} out of {len(records)} received.")
        return pd.DataFrame(cleaned_data)

    def is_valid_record(self, record: Dict) -> bool:
        required_fields = ["trip_id", "route_number", "vehicle_id", "service_key", "direction"]
        for field in required_fields:
            if field not in record or record[field] is None:
                logging.warning(f"Missing or null field '{field}' in record: {record}")
                return False

        try:
            int(record.get("trip_id"))
        except Exception:
            logging.warning(f"Invalid trip_id (non-numeric): {record.get('trip_id')} in {record}")
            return False

        try:
            int(record.get("vehicle_id"))
        except Exception:
            logging.warning(f"Invalid vehicle_id (non-numeric): {record.get('vehicle_id')} in {record}")
            return False

        return True

# Pipeline handler to process and store stop event records
class StopEventPipelineHandler:
    def __init__(self):
        self.cleaner = StopEventCleaner()

    def process_data(self, records: List[Dict]) -> pd.DataFrame:
        return self.cleaner.process_and_validate(records)

    def save_to_postgres(self, trip_metadata_df: pd.DataFrame, db_config: Dict):
        global total_loaded
        connection = None
        try:
            if trip_metadata_df.empty:
                logging.info("No valid records to insert (DataFrame is empty).")
                return

            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()

            inserted = 0
            for _, row in trip_metadata_df.iterrows():
                cursor.execute(
                    "INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (trip_id) DO NOTHING",
                    (row['trip_id'], row['route_id'], row['vehicle_id'], row['service_key'], row['direction'])
                )
                inserted += cursor.rowcount

            connection.commit()
            total_loaded += inserted
            logging.info(f"{inserted} new records inserted into PostgreSQL.")
        except Exception as e:
            logging.error(f"Error saving trip data to PostgreSQL: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()

# Callback for Pub/Sub messages (no batching)
def callback(message: pubsub_v1.subscriber.message.Message):
    global total_received, last_message_time
    try:
        data = json.loads(message.data.decode("utf-8"))
        total_received += 1
        last_message_time = datetime.now()

        trip_metadata_df = handler.process_data([data])
        handler.save_to_postgres(trip_metadata_df, DB_CONFIG)

        message.ack()
    except Exception as e:
        logging.error(f"Callback error: {e}")

# Initialize pipeline handler
handler = StopEventPipelineHandler()

# Utility function to log and print
def log_and_print(message: str):
    print(message)
    logging.info(message)

# Main loop to poll and process messages
if __name__ == "__main__":
    try:
        while True:
            log_and_print(f"[{datetime.now().strftime('%H:%M:%S')}] Subscribing to {subscription_path}...")
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            with subscriber:
                try:
                    while True:
                        if (datetime.now() - last_message_time).total_seconds() > idle_timeout:
                            streaming_pull_future.cancel()
                            break
                        time.sleep(5)
                    break
                except Exception as e:
                    logging.error(f"Subscriber crashed: {e}. Restarting...")
                    streaming_pull_future.cancel()
                    continue
                except KeyboardInterrupt:
                    streaming_pull_future.cancel()
                    break
    finally:
        elapsed = time.time() - start_time
        log_and_print(f"Total messages received: {total_received}")
        log_and_print(f"Total records loaded into DB: {total_loaded}")
        log_and_print(f"Elapsed time: {elapsed:.2f} seconds")
        if os.path.exists(log_file):
            size_kb = os.path.getsize(log_file) / 1024
            log_and_print(f"Log file size: {size_kb:.2f} KB")
