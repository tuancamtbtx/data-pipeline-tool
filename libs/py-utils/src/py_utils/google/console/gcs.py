from google.cloud import storage
import os

class GCSUtil:
    def __init__(self, project_id):
        """
        Initialize the GCSUtil with a specific project ID.
        
        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.client = storage.Client(project=project_id)

    def list_buckets(self):
        """
        List all buckets in the project.
        
        Returns:
            list: A list of bucket names.
        """
        buckets = self.client.list_buckets()
        return [bucket.name for bucket in buckets]

    def create_bucket(self, bucket_name, location='US'):
        """
        Create a new bucket in GCS.
        
        Args:
            bucket_name (str): The name of the bucket to create.
            location (str): The location where the bucket will be created. Default is 'US'.
            
        Returns:
            google.cloud.storage.bucket.Bucket: The created bucket object.
        """
        bucket = self.client.bucket(bucket_name)
        bucket.location = location
        bucket = self.client.create_bucket(bucket, exists_ok=True)
        return bucket

    def upload_file(self, bucket_name, source_file_name, destination_blob_name):
        """
        Upload a file to a GCS bucket.
        
        Args:
            bucket_name (str): The name of the bucket.
            source_file_name (str): The path to the file to upload.
            destination_blob_name (str): The name of the blob in GCS.
            
        Returns:
            None
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")

    def download_file(self, bucket_name, source_blob_name, destination_file_name):
        """
        Download a file from a GCS bucket.
        
        Args:
            bucket_name (str): The name of the bucket.
            source_blob_name (str): The name of the blob in GCS.
            destination_file_name (str): The path to the file to download to.
            
        Returns:
            None
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")

    def list_blobs(self, bucket_name):
        """
        List all blobs in a specific bucket.
        
        Args:
            bucket_name (str): The name of the bucket.
            
        Returns:
            list: A list of blob names in the bucket.
        """
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs()
        return [blob.name for blob in blobs]

    def delete_blob(self, bucket_name, blob_name):
        """
        Delete a blob from a GCS bucket.
        
        Args:
            bucket_name (str): The name of the bucket.
            blob_name (str): The name of the blob to delete.
            
        Returns:
            None
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        print(f"Blob {blob_name} deleted.")

