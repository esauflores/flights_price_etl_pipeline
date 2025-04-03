import os
import json
from google.cloud import storage

class GCSBucket:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def upload_file(self, source_file_name: str, destination_blob_name: str) -> None:
        """Uploads a file to the bucket."""
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")

    def upload_from_string(self, data: str, destination_blob_name: str) -> None:
        """Uploads a string to the bucket."""
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(data)
        print(f"String uploaded to {destination_blob_name}.")

# Initialize GCS bucket
gcs_bucket = GCSBucket(os.getenv("GCS_BUCKET_NAME"))

def save_json_to_gcs(json_data: dict, destination_blob_name: str) -> None:
    """Saves JSON data to GCS bucket."""
    try:
        # Convert JSON data to string
        json_string = json.dumps(json_data)
        gcs_bucket.upload_from_string(json_string, destination_blob_name)
    except Exception as e:
        raise Exception(f"Failed to save JSON data to GCS")