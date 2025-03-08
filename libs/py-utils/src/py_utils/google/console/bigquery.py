from google.cloud import bigquery
from tsdatalake_utils.common.logger import LoggerMixing

class BigQueryService(LoggerMixing):
    def __init__(self, project_id):
        """
        Initialize the BigQueryUtil with a specific project ID.
        
        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.client = bigquery.Client(project=project_id)

    def run_query(self, query):
        """
        Run a SQL query against BigQuery and return the results.
        
        Args:
            query (str): The SQL query to execute.
            
        Returns:
            google.cloud.bigquery.table.RowIterator: An iterator over the rows in the results.
        """
        query_job = self.client.query(query)
        return query_job.result()

    def create_dataset(self, dataset_id, location='US'):
        """
        Create a new dataset in BigQuery.
        
        Args:
            dataset_id (str): The ID of the dataset to create.
            location (str): The location where the dataset will be created. Default is 'US'.
            
        Returns:
            google.cloud.bigquery.dataset.Dataset: The created dataset object.
        """
        dataset_ref = self.client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = self.client.create_dataset(dataset, exists_ok=True)
        return dataset

    def list_datasets(self):
        """
        List all datasets in the project.
        
        Returns:
            list: A list of datasets in the project.
        """
        datasets = list(self.client.list_datasets())
        return datasets

    def list_tables(self, dataset_id):
        """
        List all tables in a specific dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            
        Returns:
            list: A list of tables in the dataset.
        """
        dataset_ref = self.client.dataset(dataset_id)
        tables = list(self.client.list_tables(dataset_ref))
        return tables

    def delete_dataset(self, dataset_id, delete_contents=False):
        """
        Delete a dataset in BigQuery.
        
        Args:
            dataset_id (str): The ID of the dataset to delete.
            delete_contents (bool): If True, delete all contents in the dataset. Default is False.
            
        Returns:
            None
        """
        dataset_ref = self.client.dataset(dataset_id)
        self.client.delete_dataset(dataset_ref, delete_contents=delete_contents, not_found_ok=True)
    
    def load_table_from_dataframe(self, df, dataset_id, table_id):
        """
        Load data from a DataFrame into a BigQuery table.
        
        Args:
            df (pandas.DataFrame): The DataFrame to load.
            dataset_id (str): The ID of the dataset.
            table_id (str): The ID of the table.
            
        Returns:
            google.cloud.bigquery.job.LoadJob: The load job object.
        """
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        return job