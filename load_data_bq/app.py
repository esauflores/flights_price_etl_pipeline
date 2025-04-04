import os
import json
import logging
from dataclasses import dataclass
from typing import Dict
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from flask import Flask, request
from isodate import parse_duration

# # Load environment variables from .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging for visibility in Cloud Run logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use default service account provided by Cloud Run
bq_client = bigquery.Client()
storage_client = storage.Client()

@dataclass
class FlightInfo:
    unique_flight_id: str
    
    departure_airport: str
    departure_time: str
 
    arrival_airport: str
    arrival_time: str
 
    flight_duration: float
    flight_number: str

    cabin_class: str
    grand_total_price: float
    currency: str

    @classmethod
    def from_raw_json(cls, record: Dict) -> "FlightInfo":
        segment = record['itineraries'][0]['segments'][0]
        departure_time = segment['departure']['at']
        flight_number = f"{segment['carrierCode']}{segment['number']}"
        unique_id = f"{flight_number}-{departure_time}"

        segment_travel = record['travelerPricings'][0]['fareDetailsBySegment'][0]

        return cls(
            unique_flight_id=unique_id,
            departure_airport=segment['departure']['iataCode'],
            arrival_airport=segment['arrival']['iataCode'],
            departure_time=departure_time,
            arrival_time=segment['arrival']['at'],
            flight_duration=parse_duration(segment['duration']).total_seconds(),
            flight_number=flight_number,
            cabin_class = segment_travel['cabin'],
            grand_total_price=float(record['price']['grandTotal']),
            currency=record['price']['currency'],
        )

    def to_dict(self) -> Dict:
        return {
            "unique_flight_id": self.unique_flight_id,
            "departure_airport": self.departure_airport,
            "departure_time": self.departure_time,
            "arrival_airport": self.arrival_airport,
            "arrival_time": self.arrival_time,
            "flight_duration": self.flight_duration,
            "flight_number": self.flight_number,
            "cabin_class": self.cabin_class,
            "grand_total_price": self.grand_total_price,
            "currency": self.currency
        }
    
def load_json_to_bq(request):
    """
    Triggered by an HTTP request when a new file is uploaded to Cloud Storage.
    Loads data into BigQuery using a temporary staging table and MERGE logic.
    """
    try:
        request_json = request.get_json()

        # Extract bucket and filename from request
        bucket_name = request_json['bucket']
        file_name = request_json['name']
        logger.info(f"Received file: {file_name} from bucket: {bucket_name}")

        # Get environment configs
        project_id = os.environ.get("GCP_PROJECT")
        dataset_id = os.environ.get("BQ_DATASET")
        table_id = os.environ.get("BQ_TABLE")
        full_table = f"{project_id}.{dataset_id}.{table_id}"
        temp_table = f"{table_id}_temp"

        print(full_table)

        # Read file from GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_string()
        records = json.loads(content)

        records = records["data"]

        # Transform records into FlightInfo objects
        flight_info_list = [FlightInfo.from_raw_json(record) for record in records]
        
        # Convert to list of dictionaries
        records = [flight_info.to_dict() for flight_info in flight_info_list]

        # Define table schema
        schema = [
            bigquery.SchemaField("unique_flight_id", "STRING"),
            bigquery.SchemaField("departure_airport", "STRING"),
            bigquery.SchemaField("departure_time", "TIMESTAMP"),
            bigquery.SchemaField("arrival_airport", "STRING"),
            bigquery.SchemaField("arrival_time", "TIMESTAMP"),
            bigquery.SchemaField("flight_duration", "FLOAT"),
            bigquery.SchemaField("flight_number", "STRING"),
            bigquery.SchemaField("cabin_class", "STRING"),
            bigquery.SchemaField("grand_total_price", "FLOAT"),
            bigquery.SchemaField("currency", "STRING"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
            autodetect=False
        )

        # Load to temp table
        logger.info(f"Loading data into temp table: {temp_table}")
        load_job = bq_client.load_table_from_json(
            records,
            f"{project_id}.{dataset_id}.{temp_table}",
            job_config=job_config
        )
        load_job.result()

        # Merge temp into main table
        merge_query = f"""
            MERGE `{full_table}` T
            USING `{project_id}.{dataset_id}.{temp_table}` S
            ON T.unique_flight_id = S.unique_flight_id
            WHEN MATCHED THEN
              UPDATE SET
                T.departure_airport = S.departure_airport,
                T.departure_time = S.departure_time,
                T.arrival_airport = S.arrival_airport,
                T.arrival_time = S.arrival_time,
                T.flight_duration = S.flight_duration,
                T.flight_number = S.flight_number,
                T.cabin_class = S.cabin_class,
                T.grand_total_price = S.grand_total_price,
                T.currency = S.currency
            WHEN NOT MATCHED THEN
              INSERT (unique_flight_id, departure_airport, departure_time, arrival_airport, arrival_time, flight_duration, flight_number, cabin_class, grand_total_price, currency)
              VALUES (S.unique_flight_id, S.departure_airport, S.departure_time, S.arrival_airport, S.arrival_time, S.flight_duration, S.flight_number, S.cabin_class, S.grand_total_price, S.currency)
        """

        logger.info("Running MERGE into main table")
        query_job = bq_client.query(merge_query)
        query_job.result()
        logger.info(f"Successfully merged into {full_table}")

        # Delete temp table
        try:
            bq_client.delete_table(f"{project_id}.{dataset_id}.{temp_table}")
            logger.info(f"Deleted temporary table {temp_table}")
        except NotFound:
            logger.warning(f"Temporary table {temp_table} not found during cleanup.")

        return f"Processed and merged file: {file_name}", 200

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return f"Error: {str(e)}", 500

# Set up Flask app
app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():
    return load_json_to_bq(request)

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)