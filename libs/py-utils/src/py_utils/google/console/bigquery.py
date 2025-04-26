import pandas as pd
from google.cloud import bigquery
from py_utils.common.logger import LoggerMixin


class BigqueryService(LoggerMixin):
    def __init__(self, project_id):
        """
        Initialize the BigQueryUtil with a specific project ID.

        Args:
                        project_id (str): The Google Cloud project ID.
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def scd(
        self,
        destination_dataset: str = None,
        destination_table: str = None,
        df: pd.DataFrame = None,
        unique_keys=None,
        schema=None,
    ):
        self.logger.info(f"Start merge data to table: {destination_table}")
        destination_project_dataset = f"{self.project_id}.{destination_dataset}"
        destination_project_dataset_table = (
            f"{destination_project_dataset}.{destination_table}"
        )
        l = destination_project_dataset_table.split(".")
        staging_project_dataset_table = l[0] + ".staging." + l[2]
        # insert data to staging table
        if schema is not None:
            job_stg_table_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                schema=schema,
            )
        else:
            job_stg_table_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,
            )
        create_stg_table_job = self.client.load_table_from_dataframe(
            df, staging_project_dataset_table, job_config=job_stg_table_config
        )
        create_stg_table_job.result()
        # build dml to merge data from staging table to destination table
        on_clause = self._build_on_clause(unique_keys)
        merge_dml = f"""
			DECLARE cols STRING;
			DECLARE update_clause STRING;

			-- Step 1: Construct the set clause for updating fields, excluding _timestamp initially
			SET cols = (
			SELECT STRING_AGG(
				CONCAT('t.', column_name, ' = s.', column_name),
				', '
			)
			FROM `{destination_project_dataset}.INFORMATION_SCHEMA.COLUMNS`
			WHERE table_name = '{destination_table}'
			AND column_name != '_timestamp'  -- Exclude _timestamp from this list
			);

			-- Step 2: Construct the update clause to include the _timestamp update
			SET update_clause = CONCAT(cols, ', t._timestamp = CURRENT_TIMESTAMP()');

			-- Step 3: Execute the merge statement with the constructed update clause
			EXECUTE IMMEDIATE FORMAT('''
			MERGE `{destination_project_dataset_table}` AS t
			USING `{staging_project_dataset_table}` AS s
			ON {on_clause}
			WHEN NOT MATCHED THEN
				INSERT ROW
			WHEN MATCHED THEN
				UPDATE SET %s
			''', update_clause);	
        """
        self.logger.info(f"Merge DML: {merge_dml}")
        self.client.query(merge_dml).result()
        return merge_dml

    def insert(
        self,
        table_id: str,
        df,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=None,
        clustering_fields=None,
        schema=None,
    ):
        self.logger.info(f"Start insert data to table: {table_id}")
        if schema is not None:
            job_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
                schema=schema,
            )
        else:
            job_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
                autodetect=True,
            )
        if time_partitioning is not None:
            self.logger.info(f"Partition field: {time_partitioning}")
            job_config.time_partitioning = time_partitioning
        if clustering_fields is not None:
            self.logger.info(f"Clustering fields: {clustering_fields}")
            job_config.clustering_fields = clustering_fields
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        self.logger.info(f"Insert data to table: {table_id} - Done")
        return job.state

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

    def create_dataset(self, dataset_id, location="US"):
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
        self.client.delete_dataset(
            dataset_ref, delete_contents=delete_contents, not_found_ok=True
        )

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
        job = self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        return job

    def _build_on_clause(self, unique_keys):
        on_clause = ""
        for key in unique_keys:
            on_clause += f"t.{key} = s.{key} and "
        on_clause = on_clause[:-4]
        return on_clause

    def merge(
        self,
        destination_dataset: str = None,
        destination_table: str = None,
        df: pd.DataFrame = None,
        unique_keys=None,
        schema=None,
    ):
        self.logger.info(f"Start merge data to table: {destination_table}")
        destination_project_dataset = f"{self.project_id}.{destination_dataset}"
        destination_project_dataset_table = (
            f"{destination_project_dataset}.{destination_table}"
        )
        l = destination_project_dataset_table.split(".")
        staging_project_dataset_table = l[0] + ".staging." + l[2]
        # insert data to staging table
        if schema is not None:
            job_stg_table_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                schema=schema,
            )
        else:
            job_stg_table_config = bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,
            )
        create_stg_table_job = self.client.load_table_from_dataframe(
            df, staging_project_dataset_table, job_config=job_stg_table_config
        )
        create_stg_table_job.result()
        # build dml to merge data from staging table to destination table
        on_clause = self._build_on_clause(unique_keys)
        merge_dml = f"""declare cols string;
				set cols = (
				select
					string_agg(concat('t.', column_name, '=s.', column_name), ',')
				from
					`{destination_project_dataset}.INFORMATION_SCHEMA.COLUMNS`
				where
					table_name = '{destination_table}'
				);
				execute immediate format('''
					merge `{destination_project_dataset_table}` as t
					using `{staging_project_dataset_table}` as s
					on {on_clause}
					when not matched then insert row
					when matched then update set %s
					'''
					, cols
				);"""
        self.client.query(merge_dml).result()
        self.logger.info(f"Merge DML: {merge_dml}")
        return merge_dml

    def is_table_exists(self, table_id: str):
        dataset_id = table_id.split(".")[1]
        table_name = table_id.split(".")[2]
        dataset = self.client.dataset(dataset_id)
        table_ref = dataset.table(table_name)
        try:
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False
