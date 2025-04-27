import requests
import json
from google.cloud import storage
from py_utils.common.logger import LoggerMixin
from py_workflow.operators.base import BaseOperator


class ApiToGCSOperator(BaseOperator, LoggerMixin):
    def __init__(
        self, api_url, bucket_name, destination_blob_name, gcp_credentials_path
    ):
        """
        Initialize the operator with API URL, GCS bucket details, and GCP credentials path.

        :param api_url: URL of the API endpoint to fetch data from.
        :param bucket_name: Name of the GCS bucket to upload data to.
        :param destination_blob_name: Name of the blob in the GCS bucket.
        :param gcp_credentials_path: Path to the Google Cloud service account credentials file.
        """
        self.api_url = api_url
        self.bucket_name = bucket_name
        self.destination_blob_name = destination_blob_name
        self.gcp_credentials_path = gcp_credentials_path

        # Set the environment variable for Google Cloud authentication

    def fetch_data_from_api(self):
        """Fetch data from the API."""
        response = requests.get(self.api_url)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def upload_to_gcs(self, data):
        """Upload data to the specified GCS bucket."""
        # Initialize a storage client
        storage_client = storage.Client()

        # Get the bucket
        bucket = storage_client.bucket(self.bucket_name)

        # Create a new blob and upload the data
        blob = bucket.blob(self.destination_blob_name)
        blob.upload_from_string(data, content_type="application/json")

        print(
            f"Data uploaded to {self.destination_blob_name} in bucket {self.bucket_name}."
        )

    def execute(self):
        """Execute the operator to fetch data from API and upload it to GCS."""
        # Fetch data from the API
        data = self.fetch_data_from_api()

        # Convert data to JSON string
        data_json = json.dumps(data)

        # Upload data to GCS
        self.upload_to_gcs(data_json)


# Example usage
if __name__ == "__main__":
    api_url = "https://api.example.com/data"
    bucket_name = "your-gcs-bucket-name"
    destination_blob_name = "path/to/your/file.json"
    gcp_credentials_path = "path/to/your/service-account-file.json"

    operator = ApiToGCSOperator(
        api_url, bucket_name, destination_blob_name, gcp_credentials_path
    )
    operator.execute()
