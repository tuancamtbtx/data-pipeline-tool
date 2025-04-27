import os

from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from py_workflow.operators.base import BaseOperator
from py_utils.common.logger import LoggerMixin


class BigqueryToDriveOperator(BaseOperator, LoggerMixin):
    def __init__(
        self, project_id, dataset_id, table_id, destination_folder_id, credentials_path
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.destination_folder_id = destination_folder_id
        self.credentials_path = credentials_path

        # Initialize BigQuery client
        self.bq_client = bigquery.Client.from_service_account_json(credentials_path)

        # Initialize Google Drive service
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=["https://www.googleapis.com/auth/drive"]
        )
        self.drive_service = build("drive", "v3", credentials=credentials)

    def export_table_to_gcs(self, bucket_name, destination_blob_name):
        destination_uri = f"gs://{bucket_name}/{destination_blob_name}.csv"
        dataset_ref = self.bq_client.dataset(self.dataset_id, project=self.project_id)
        table_ref = dataset_ref.table(self.table_id)

        extract_job = self.bq_client.extract_table(
            table_ref,
            destination_uri,
            location="US",  # Location must match that of the source table.
        )
        extract_job.result()  # Wait for the job to complete.

        print(
            f"Exported {self.project_id}:{self.dataset_id}.{self.table_id} to {destination_uri}"
        )
        return destination_uri

    def upload_to_drive(self, file_path, file_name):
        file_metadata = {"name": file_name, "parents": [self.destination_folder_id]}
        media = MediaFileUpload(file_path, mimetype="text/csv")

        file = (
            self.drive_service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        print(f"File ID: {file.get('id')}")
        return file.get("id")

    def execute(self, bucket_name, destination_blob_name, file_name):
        # Export table to Google Cloud Storage
        destination_uri = self.export_table_to_gcs(bucket_name, destination_blob_name)

        # Download the file from GCS to local storage (assuming gsutil is installed)
        os.system(f"gsutil cp {destination_uri} {file_name}")

        # Upload the file to Google Drive
        file_id = self.upload_to_drive(file_name, file_name)

        # Clean up local file
        os.remove(file_name)

        return file_id


# Example usage:
# operator = BigqueryToDriveOperator(
#     project_id='your_project_id',
#     dataset_id='your_dataset_id',
#     table_id='your_table_id',
#     destination_folder_id='your_drive_folder_id',
# )
# operator.execute(bucket_name='your_bucket_name', destination_blob_name='your_blob_name', file_name='your_local_file_name.csv')
