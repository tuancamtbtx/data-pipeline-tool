from google.cloud import storage


class GoogleStorageService:
    """
    Helper for gcs file
    """

    def __init__(self, bucket_name, *args, **kwargs):
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)

    def upload_to_gcs(self, source_file_name, destination_blob_name):
        # Create a blob object from the bucket
        blob = self.bucket.blob(destination_blob_name)
        # Upload the file to GCS
        blob.upload_from_filename(source_file_name)
