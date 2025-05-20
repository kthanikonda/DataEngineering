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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# loaded trip_ids to avoid duplicates
loaded_trip_ids = set()

# Logging setup
log_file = "batched_pipeline.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# GCP Credentials and Pub/Sub Config
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/kthaniko/pub_sub_key.json"
project_id = "parabolic-grid-456118-u8"
subscription_id = "my-sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# PostgreSQL Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

first_operation_date = None
buffer_lock = Lock()
message_buffer = []
BATCH_SIZE = 1000
idle_timeout = 60
start_time = time.time()
total_received = 0
total_loaded = 0
last_message_time = datetime.now()

class TriMetRecordCleaner:
    def _init_(self):
        self.processed_combinations = set()

    def process_and_validate(self, records: List[Dict]) -> pd.DataFrame:
        cleaned_data = []
        unique_entries = set()

        for record in records:
            record_key = (record.get("EVENT_NO_TRIP"), record.get("ACT_TIME"))
            if record_key in unique_entries:
                continue
            unique_entries.add(record_key)

            try:
                self.run_all_validations(record)
            except AssertionError as e:
                logging.warning(f"Validation failed: {e} | Record: {record}")
                continue

            timestamp = datetime.strptime(record["OPD_DATE"].split(":")[0], "%d%b%Y") + timedelta(seconds=record["ACT_TIME"])
            cleaned_data.append({
                "timestamp": timestamp,
                "latitude": record["GPS_LATITUDE"],
                "longitude": record["GPS_LONGITUDE"],
                "trip_id": record["EVENT_NO_TRIP"],
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
            if pd.notnull(r['dTIMESTAMP']) and r['dTIMESTAMP'].total_seconds() > 0 else None, axis=1)
        df['SPEED'] = df['SPEED'].bfill()

        return df[['timestamp', 'latitude', 'longitude', 'SPEED', 'trip_id']]

    def run_all_validations(self, record):
        self.validate_required_fields(record)
        self.validate_act_time(record)
        self.validate_opd_date(record)
        self.validate_latitude(record)
        self.validate_longitude(record)
        self.validate_vehicle_id(record)
        self.validate_meters(record)
        self.validate_gps_satellites(record)
        self.validate_uniqueness(record)
        self.validate_opd_consistency(record)

    def validate_required_fields(self, record):
        try:
            required = ['EVENT_NO_TRIP', 'OPD_DATE', 'ACT_TIME', 'EVENT_NO_STOP', 'VEHICLE_ID',
                        'GPS_LATITUDE', 'GPS_LONGITUDE', 'METERS']
            for field in required:
                assert field in record, f"Missing field: {field}"
        except AssertionError as e:
            logging.warning(f"validate_required_fields failed: {e} | Record: {record}")
            raise

    def validate_act_time(self, record):
        try:
            assert record.get('ACT_TIME') is not None, "ACT_TIME is missing"
        except AssertionError as e:
            logging.warning(f"validate_act_time failed: {e} | Record: {record}")
            raise

    def validate_opd_date(self, record):
        try:
            datetime.strptime(record['OPD_DATE'].split(":")[0], "%d%b%Y")
        except Exception as e:
            logging.warning(f"validate_opd_date failed: {e} | Record: {record}")
            raise AssertionError("Invalid OPD_DATE format")

    def validate_latitude(self, record):
        try:
            lat = record.get('GPS_LATITUDE')
            assert lat is not None, "Latitude is None"
            lat = float(lat)
            assert 45.2 <= lat <= 45.7, f"Invalid latitude: {lat}"
        except AssertionError as e:
            logging.warning(f"validate_latitude failed: {e} | Record: {record}")
            raise

    def validate_longitude(self, record):
        try:
            lon = record.get('GPS_LONGITUDE')
            assert lon is not None, "Longitude is None"
            lon = float(lon)
            assert -124.0 <= lon <= -122.0, f"Invalid longitude: {lon}"
        except AssertionError as e:
            logging.warning(f"validate_longitude failed: {e} | Record: {record}")
            raise

    def validate_vehicle_id(self, record):
        try:
            vid = record.get('VEHICLE_ID')
            assert isinstance(vid, int) and vid > 0, f"Invalid VEHICLE_ID: {vid}"
        except AssertionError as e:
            logging.warning(f"validate_vehicle_id failed: {e} | Record: {record}")
            raise

    def validate_meters(self, record):
        try:
            meters = record.get('METERS', 0)
            assert isinstance(meters, (int, float)), "METERS not numeric"
            assert meters >= 0, f"Negative METERS: {meters}"
        except AssertionError as e:
            logging.warning(f"validate_meters failed: {e} | Record: {record}")
            raise

    def validate_gps_satellites(self, record):
        try:
            sats = record.get('GPS_SATELLITES')
            if sats is not None:
                assert isinstance(sats, (int, float)) and 0 <= sats <= 12, f"Invalid GPS_SATELLITES: {sats}"
        except AssertionError as e:
            logging.warning(f"validate_gps_satellites failed: {e} | Record: {record}")
            raise

    def validate_uniqueness(self, record):
        try:
            key = (record['VEHICLE_ID'], record['EVENT_NO_TRIP'], record['EVENT_NO_STOP'],
                   record['ACT_TIME'], record['METERS'])
            assert key not in self.processed_combinations, f"Duplicate record: {key}"
            self.processed_combinations.add(key)
        except AssertionError as e:
            logging.warning(f"validate_uniqueness failed: {e} | Record: {record}")
            raise

    def validate_opd_consistency(self, record):
        try:
            global first_operation_date
            if first_operation_date is None:
                first_operation_date = record['OPD_DATE']
            else:
                assert record['OPD_DATE'] == first_operation_date, f"Mismatched OPD_DATE: {record['OPD_DATE']}"
        except AssertionError as e:
            logging.warning(f"validate_opd_consistency failed: {e} | Record: {record}")
            raise

class TripDetails:
    def _init_(self):
        self.trip_details = {}

    def build_trip_details(self, records: List[Dict]) -> pd.DataFrame:
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

class TriMetPipelineHandler:
    def _init_(self):
        self.cleaner = TriMetRecordCleaner()
        self.metadata_extractor = TripDetails()

    def process_data(self, records: List[Dict]) -> pd.DataFrame:
        validated_data = self.cleaner.process_and_validate(records)
        trip_metadata = self.metadata_extractor.build_trip_details(records)
        return validated_data, trip_metadata

    def save_to_postgres(self, validated_df: pd.DataFrame, trip_metadata_df: pd.DataFrame, db_config: Dict):
        global total_loaded, loaded_trip_ids
        try:
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()

            validated_df["tstamp"] = validated_df["timestamp"].dt.strftime('%Y-%m-%d %H:%M:%S')
            validated_df.drop(columns=["timestamp"], inplace=True)
            validated_df.rename(columns={"SPEED": "speed"}, inplace=True)
            validated_df = validated_df[["tstamp", "latitude", "longitude", "speed", "trip_id"]]

            if not trip_metadata_df.empty:
                trip_metadata_df = trip_metadata_df[~trip_metadata_df["trip_id"].isin(loaded_trip_ids)]

                if not trip_metadata_df.empty:
                    buffer = StringIO()
                    trip_metadata_df = trip_metadata_df[["trip_id", "route_id", "vehicle_id", "service_key", "direction"]]
                    trip_metadata_df.to_csv(buffer, index=False, header=False, sep='\t')
                    buffer.seek(0)
                    cur.copy_from(buffer, 'trip', null="", columns=("trip_id", "route_id", "vehicle_id", "service_key", "direction"))
                    loaded_trip_ids.update(trip_metadata_df["trip_id"].tolist())

            if not validated_df.empty:
                for i in range(0, len(validated_df), 10000):
                    batch = validated_df.iloc[i:i + 10000]
                    buffer = StringIO()
                    batch.to_csv(buffer, index=False, header=False, sep='\t')
                    buffer.seek(0)
                    cur.copy_from(buffer, 'breadcrumb', null="", columns=("tstamp", "latitude", "longitude", "speed", "trip_id"))

            conn.commit()
            total_loaded += len(validated_df)
            logging.info("Data successfully saved to PostgreSQL.")
        except Exception as e:
            logging.error(f"PostgreSQL save error: {e}")
        finally:
            if conn:
                cur.close()
                conn.close()

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
            validated_df, trip_metadata_df = handler.process_data(batch)
            handler.save_to_postgres(validated_df, trip_metadata_df, DB_CONFIG)

        message.ack()
    except Exception as e:
        logging.error(f"Callback error: {e}")

handler = TriMetPipelineHandler()

def log_and_print(msg: str):
    print(msg)
    logging.info(msg)

if _name_ == "_main_":
    try:
        log_and_print(f"[{datetime.now().strftime('%H:%M:%S')}] Subscribing to {subscription_path}...")
        future = subscriber.subscribe(subscription_path, callback=callback)
        while True:
            if (datetime.now() - last_message_time).total_seconds() > idle_timeout:
                future.cancel()
                break
            time.sleep(5)
    except KeyboardInterrupt:
        future.cancel()
    except Exception as e:
        logging.error(f"Subscriber crashed: {e}. Restarting...")
    finally:
        elapsed = time.time() - start_time
        log_and_print(f"Total messages received: {total_received}")
        log_and_print(f"Total records loaded into DB: {total_loaded}")
        log_and_print(f"Elapsed time: {elapsed:.2f} seconds")
        if os.path.exists(log_file):
            size_kb = os.path.getsize(log_file) / 1024
            log_and_print(f"Log file size: {size_kb:.2f} KB")
