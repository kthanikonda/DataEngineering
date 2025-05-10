import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict
from google.cloud import pubsub_v1
import psycopg2
import pandas as pd
from io import StringIO
from threading import Lock


log_file = "batched_pipeline.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/kthaniko/pub_sub_key.json"
project_id = "parabolic-grid-456118-u8"
subscription_id = ""
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "DataEngineering"
}


first_operation_date = None
buffer_lock = Lock()
message_buffer = []
BATCH_SIZE = 1000
total_received = 0
total_loaded = 0
last_message_time = datetime.now()

timeout = 1000.0
# Stop after 60 seconds of no message
idle_timeout = 60  

start_time = time.time()

# This class handles data validation and speed calculation
class GPSDataProcessor:
    def __init__(self):
        self.processed_combinations = set()

     # Function to process, validate, and calculate speed for GPS records
    def process_and_validate(self, records: List[Dict]) -> pd.DataFrame:
        cleaned_data = []
        unique_entries = set()

        for record in records:
            record_key = (record.get("EVENT_NO_TRIP"), record.get("ACT_TIME"))
            if record_key in unique_entries:
                continue
            unique_entries.add(record_key)

            try:
                if not self.is_valid_record(record):
                    continue
            except AssertionError as e:
                logging.warning(f"Validation failed for record: {record.get('EVENT_NO_TRIP')}, Reason: {e}")
                continue

            trip_id = record["EVENT_NO_TRIP"]
            timestamp = datetime.strptime(record["OPD_DATE"].split(":")[0], "%d%b%Y") + timedelta(seconds=record["ACT_TIME"])

            cleaned_data.append({
                "timestamp": timestamp,
                "latitude": record["GPS_LATITUDE"],
                "longitude": record["GPS_LONGITUDE"],
                "trip_id": trip_id,
                "METERS": record["METERS"],
            })

        df = pd.DataFrame(cleaned_data)
        if df.empty:
            return df

        df.sort_values(by=["trip_id", "timestamp"], inplace=True)
        df['dMETERS'] = df.groupby('trip_id')['METERS'].diff()
        df['dTIMESTAMP'] = df.groupby('trip_id')['timestamp'].diff()

        df['SPEED'] = df.apply(
            lambda r: r['dMETERS'] / r['dTIMESTAMP'].total_seconds()
            if pd.notnull(r['dTIMESTAMP']) and r['dTIMESTAMP'].total_seconds() > 0 else None, axis=1
        )
        df['SPEED'] = df['SPEED'].bfill()

        return df[['timestamp', 'latitude', 'longitude', 'SPEED', 'trip_id']]

    def is_valid_record(self, record: Dict) -> bool:
        global first_operation_date

        # assertion-1: Ensure all required fields are present in the input record
        required_fields = ['EVENT_NO_TRIP', 'OPD_DATE', 'ACT_TIME', 'EVENT_NO_STOP', 'VEHICLE_ID',
                           'GPS_LATITUDE', 'GPS_LONGITUDE', 'METERS']

        for field in required_fields:
            if field not in record:
                raise AssertionError(f"Missing required field '{field}'")

        # assertion-2: Check if ACT_TIME is available or not
        if record.get('ACT_TIME') is None:
            raise AssertionError(f"ACT_TIME is missing for record: {record.get('EVENT_NO_TRIP')}")

        # assertion-3: Validate OPD_DATE is in correct format 
        try:
            datetime.strptime(record['OPD_DATE'].split(":")[0], "%d%b%Y")
        except Exception:
            raise AssertionError(f"Invalid OPD_DATE format: {record.get('OPD_DATE')}")

        # assertion-4: Check latitude is a float and is within expected Portland GPS boundary
        lat = record.get('GPS_LATITUDE')
        if lat is None or not isinstance(lat, float) or not (45.2 <= lat <= 45.7):
            raise AssertionError(f"Latitude {lat} is missing or out of range")

        # assertion-5: Check longitude is a float and is within Portland region
        lon = record.get('GPS_LONGITUDE')
        if lon is None or not isinstance(lon, float) or not (-124.0 <= lon <= -122.0):
            raise AssertionError(f"Longitude {lon} is missing or out of range")

        # assertion-6: Vehicle ID must be a valid positive integer
        if not isinstance(record['VEHICLE_ID'], int) or record['VEHICLE_ID'] <= 0:
            raise AssertionError(f"Invalid VEHICLE_ID: {record['VEHICLE_ID']}")

        # assertion-7: METERS value should not be negative
        if record.get('METERS', 0) < 0:
            raise AssertionError(f"Negative METERS value: {record['METERS']}")

        # assertion-8: If GPS_SATELLITES exists it must be a number between 0 and 12
        if 'GPS_SATELLITES' in record:
            sats = record['GPS_SATELLITES']
            if not isinstance(sats, (int, float)) or not (0 <= sats <= 12):
                raise AssertionError(f"GPS_SATELLITES {sats} out of range (0–12)")

        # assertion-9: Prevent duplicate records by checking unique key combination
        unique_key = (record['VEHICLE_ID'], record['EVENT_NO_TRIP'], record['EVENT_NO_STOP'],
                      record['ACT_TIME'], record['METERS'])
        if unique_key in self.processed_combinations:
            raise AssertionError(f"Duplicate record detected with key: {unique_key}")
        self.processed_combinations.add(unique_key)

        # assertion-10: All records processed in one batch should have same OPD_DATE
        if first_operation_date is None:
            first_operation_date = record['OPD_DATE']
        elif record['OPD_DATE'] != first_operation_date:
            raise AssertionError(f"Inconsistent OPD_DATE: {record['OPD_DATE']} (expected {first_operation_date})")

        return True

class TripMetadataProcessor:
    def __init__(self):
        self.trip_details = {}

    def extract_metadata(self, records: List[Dict]) -> pd.DataFrame:
        for record in records:
            trip_id = record.get("EVENT_NO_TRIP")
            vehicle_id = record.get("VEHICLE_ID")
            route_id = record.get("ROUTE_ID", 0)

            try:
                day_of_week = datetime.strptime(record["OPD_DATE"].split(":")[0], "%d%b%Y").strftime("%A")
                service_key = "Weekday" if day_of_week not in ["Saturday", "Sunday"] else day_of_week
            except Exception:
                service_key = "Weekday"

            if trip_id not in self.trip_details:
                self.trip_details[trip_id] = {
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "vehicle_id": vehicle_id,
                    "service_key": service_key,
                    "direction": "Out"
                }

        return pd.DataFrame(self.trip_details.values())


class DataHandler:
    def __init__(self):
        self.processor = GPSDataProcessor()
        self.metadata_extractor = TripMetadataProcessor()

    def process_data(self, records: List[Dict]) -> pd.DataFrame:
        validated_data = self.processor.process_and_validate(records)
        trip_metadata = self.metadata_extractor.extract_metadata(records)
        return validated_data, trip_metadata

    def save_to_postgres(self, validated_df: pd.DataFrame, trip_metadata_df: pd.DataFrame, db_config: Dict):
        global total_loaded
        try:
            connection = psycopg2.connect(**db_config)
            cursor = connection.cursor()

            validated_df["tstamp"] = validated_df["timestamp"].apply(lambda ts: ts.strftime('%Y-%m-%d %H:%M:%S'))
            validated_df = validated_df.drop(columns=["timestamp"])
            validated_df = validated_df[["tstamp", "latitude", "longitude", "SPEED", "trip_id"]]

            for _, row in trip_metadata_df.iterrows():
                cursor.execute(
                    "INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (trip_id) DO NOTHING",
                    (row['trip_id'], row['route_id'], row['vehicle_id'], row['service_key'], row['direction'])
                )

            if not validated_df.empty:
                batch_size = 10000
                for i in range(0, len(validated_df), batch_size):
                    batch = validated_df.iloc[i:i + batch_size]
                    buffer = StringIO()
                    batch.to_csv(buffer, index=False, header=False, sep='	')
                    buffer.seek(0)
                    cursor.copy_from(buffer, 'breadcrumb', null="", columns=(
                        'tstamp', 'latitude', 'longitude', 'speed', 'trip_id'))

            connection.commit()
            total_loaded += len(validated_df)
            logging.info("Data successfully saved to PostgreSQL.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Data successfully loaded into PostgreSQL.")
        except Exception as e:
            logging.error(f"Error saving data to PostgreSQL: {e}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error saving data to PostgreSQL: {e}")
        finally:
            if connection:
                cursor.close()
                connection.close()


def callback(message: pubsub_v1.subscriber.message.Message):
    global message_buffer, total_received, last_message_time
    try:
        data = json.loads(message.data.decode("utf-8"))
        total_received += 1
        last_message_time = datetime.now()

        

        batch = None
        with buffer_lock:
            message_buffer.append(data)
            if len(message_buffer) >= BATCH_SIZE:
                batch = message_buffer[:]
                message_buffer.clear()

        if batch:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing batch of {len(batch)} messages")
            validated_df, trip_metadata_df = handler.process_data(batch)
            handler.save_to_postgres(validated_df, trip_metadata_df, DB_CONFIG)

        message.ack()
    except Exception as e:
        logging.error(f"Callback error: {e}")


handler = DataHandler()

if __name__ == "__main__":
    try:
        while True:
            if time.time() - start_time > timeout:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Timeout reached. Exiting...")
                break
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Subscribing to {subscription_path}...")
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

            with subscriber:
                try:
                    while True:
                        if (datetime.now() - last_message_time).total_seconds() > idle_timeout:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] No messages for {idle_timeout} seconds. Exiting...")
                            streaming_pull_future.cancel()
                            break
                        time.sleep(5)
                    break  
                except Exception as e:
                    logging.error(f"Subscriber crashed: {e}. Restarting loop...")
                    streaming_pull_future.cancel()
                    continue
                except KeyboardInterrupt:
                    print("Keyboard interrupt received. Exiting...")
                    streaming_pull_future.cancel()
                    break
    finally:
        end_time = time.time()
        elapsed = end_time - start_time

        print(f"Total messages received: {total_received}")
        print(f"Total records loaded into DB: {total_loaded}")
        print(f"Elapsed time: {elapsed:.2f} seconds")
        if os.path.exists(log_file):
            file_size_kb = os.path.getsize(log_file) / 1024
            print(f"Log file: {log_file}")
            print(f"Log file size: {file_size_kb:.2f} KB")
