from google.cloud import bigquery
from tsdatalake_workflow.operators.base import BaseOperator
from tsdatalake_utils.common.logger import LoggerMixing

class BigqueryToGCSOperator(BaseOperator, LoggerMixing):
    def __init__(self, project_id, dataset_id, table_id, destination_bucket_name, destination_blob_name, credentials_path):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.destination_bucket_name = destination_bucket_name
        self.destination_blob_name = destination_blob_name
        self.credentials_path = credentials_path

        # Initialize BigQuery client
        self.bq_client = bigquery.Client.from_service_account_json(credentials_path)

    def export_table_to_gcs(self):
        # Construct the destination URI for GCS
        destination_uri = f"gs://{self.destination_bucket_name}/{self.destination_blob_name}.csv"
        
        # Reference to the dataset and table
        dataset_ref = self.bq_client.dataset(self.dataset_id, project=self.project_id)
        table_ref = dataset_ref.table(self.table_id)

        # Create an extract job
        extract_job = self.bq_client.extract_table(
            table_ref,
            destination_uri,
            location="US"  # Location must match that of the source table.
        )

        # Wait for the job to complete
        extract_job.result()

        print(f"Exported {self.project_id}:{self.dataset_id}.{self.table_id} to {destination_uri}")
        return destination_uri

# Example usage:
# operator = BigqueryToGCSOperator(
#     project_id='your_project_id',
#     dataset_id='your_dataset_id',
#     table_id='your_table_id',
#     destination_bucket_name='your_bucket_name',
#     destination_blob_name='your_blob_name',
#     credentials_path='path_to_your_service_account_credentials.json'
# )
# operator.export_table_to_gcs()