import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict
from google.cloud import bigquery
from py_workflow.operators.base import BaseOperator
from py_utils.common.logger import LoggerMixin

from py_utils.google.api.sheet import GoogleSheetService
from py_utils.google.console.bigquery import BigqueryService

from py_utils.utils.string import remove_accents


class WRITEMODE:
    APPEND = "append"
    TRUNCATE = "truncate"
    INCREMENTAL = "incremental"
    SCD = "scd"


class GGSheetToBigQuery(BaseOperator, LoggerMixin):
    def __init__(
        self,
        is_timestamp: bool = False,
        spreadsheet_url: str = None,
        sheet_name: str = None,
        project_id: str = None,
        dataset_id: str = None,
        table_id: str = None,
        write_mode: str = "append",
        unique_keys: list = None,
        clustering_fields: list = None,
        time_partitioning: str = None,
        filter_conditions: list = None,
        columns: list = None,
        schema: list = None,
    ):
        self.spreadsheet_url = spreadsheet_url
        self.sheet_name = sheet_name
        self.unique_keys = unique_keys
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.clustering_fields = clustering_fields
        self.time_partitioning = time_partitioning
        self.filter_conditions = filter_conditions
        self.write_mode = write_mode
        self.is_timestamp = is_timestamp
        self.schema = schema
        self.bigquery_schema = self.convert_to_bigquery_schema(self.schema)
        self.columns = columns
        self.google_sheet_service = GoogleSheetService(url=self.spreadsheet_url)
        self.bigquery_service = BigqueryService(project_id=self.project_id)

    def convert_to_bigquery_schema(self, schema=None):
        if schema is None:
            return None
        schemas = []
        for field in schema:
            field_name = field["field"]
            field_type = field["type"]
            field_mode = field.get("mode", "NULLABLE")
            schemas.append(
                bigquery.SchemaField(field_name, field_type, mode=field_mode)
            )
        if self.is_timestamp:
            schemas.append(
                bigquery.SchemaField("_timestamp", "TIMESTAMP", mode="NULLABLE")
            )
        return schemas

    def normalize_column_names(self, df):
        # Convert field names to lowercase and replace spaces with underscores
        columns = df.columns
        headers = dict()
        for column in columns:
            headers[column] = remove_accents(column)
        df.rename(columns=headers, inplace=True)
        df.columns = df.columns.str.lower().str.replace(" ", "_")
        df = df[self.columns]
        return df

    def get_dataframe_by_conditions(
        self, df: pd.DataFrame, conditions: List[Dict]
    ) -> pd.DataFrame:
        """
        Filter the DataFrame based on a list of conditions, each specified as a dictionary
        containing 'field', 'operator', and 'value'.
        """
        # Create a copy of the DataFrame to avoid modifying the original
        filtered_df = df.copy()

        # Define a mapping of operators to corresponding DataFrame operations
        operator_map = {
            "IN": lambda col, val: col.isin(val),
            "=": lambda col, val: col == val,
            "==": lambda col, val: col == val,
            "!=": lambda col, val: col != val,
            "<>": lambda col, val: col != val,
            ">": lambda col, val: col > val,
            ">=": lambda col, val: col >= val,
            "<": lambda col, val: col < val,
            "<=": lambda col, val: col <= val,
        }

        for condition in conditions:
            field = condition["field"]
            operator = condition["operator"]
            value = condition["value"]

            # Check if the field exists in the DataFrame
            if field not in filtered_df.columns:
                print(
                    f"Warning: Field '{field}' not found in DataFrame. Skipping condition."
                )
                continue

            # Check if the operator is supported
            if operator not in operator_map:
                print(
                    f"Warning: Unsupported operator '{operator}'. Skipping condition."
                )
                continue

            # Apply the filter condition using the operator map
            filtered_df = filtered_df[operator_map[operator](filtered_df[field], value)]

        return filtered_df

    def fetch_data_from_sheets(self):
        # Fetch data from Google Sheets
        df = self.google_sheet_service.read_sheet(self.sheet_name)
        return df

    def load_data_to_bigquery(self, df):
        self.logger.info(
            f"Loading data as {self.write_mode} BigQuery: {self.project_id}.{self.dataset_id}.{self.table_id} - size: {df.shape}"
        )
        if self.write_mode == WRITEMODE.INCREMENTAL:
            self.merge_data_to_bigquery(df)
        elif self.write_mode == WRITEMODE.APPEND:
            self.insert_data_to_bigquery(df)
        elif self.write_mode == WRITEMODE.TRUNCATE:
            self.insert_data_to_bigquery(df)
        elif self.write_mode == WRITEMODE.SCD:
            self.scd_data_to_bigquery(df)
        else:
            raise ValueError(f"Invalid write mode: {self.write_mode}")

    def insert_data_to_bigquery(self, df, write_disposition="WRITE_APPEND"):
        table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        self.bigquery_service.insert(
            table_id=table_id,
            df=df,
            write_disposition=write_disposition,
            time_partitioning=self.time_partitioning,
            clustering_fields=self.clustering_fields,
            schema=self.bigquery_schema,
        )
        self.logger.info(
            f"Inserted {len(df)} rows into {self.dataset_id}:{self.table_id}."
        )

    def merge_data_to_bigquery(self, df):
        table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        if not self.bigquery_service.is_table_exists(table_id=table_id):
            self.insert_data_to_bigquery(df, write_disposition="WRITE_APPEND")
            return
        self.bigquery_service.merge(
            destination_dataset=self.dataset_id,
            destination_table=self.table_id,
            df=df,
            unique_keys=self.unique_keys,
            schema=self.bigquery_schema,
        )
        self.logger.info(
            f"Merged {len(df)} rows into {self.dataset_id}:{self.table_id}."
        )

    def scd_data_to_bigquery(self, df):
        df["_timestamp"] = datetime.now(timezone.utc)
        table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        if not self.bigquery_service.is_table_exists(table_id=table_id):
            self.insert_data_to_bigquery(df, write_disposition="WRITE_APPEND")
            return
        self.bigquery_service.scd(
            destination_dataset=self.dataset_id,
            destination_table=self.table_id,
            df=df,
            unique_keys=self.unique_keys,
            schema=self.bigquery_schema,
        )
        self.logger.info("process slowly changing dimension")

    def apply_schema_to_dataframe(self, df):
        """
        Apply the schema to the DataFrame, converting columns to specified data types.
        """
        # Check if schema is defined
        if self.schema is None:
            self.logger.warning("Schema is None. Skipping parsing.")
            return df

        # Validate the input DataFrame
        if df is None or df.empty:
            self.logger.warning("DataFrame is None or empty. Skipping parsing.")
            return df
        if not isinstance(df, pd.DataFrame):
            self.logger.warning("Input is not a DataFrame. Skipping parsing.")
            return df

        # Validate the schema
        if not isinstance(self.schema, list):
            self.logger.warning("Schema is not a list. Skipping parsing.")
            return df

        # Create a copy of the DataFrame to avoid modifying the original
        df_copy = df.copy()

        # Define a mapping of field types to conversion functions
        type_conversion_map = {
            "TIMESTAMP": lambda col: pd.to_datetime(
                col, format="%Y-%m-%d %H:%M:%S", errors="coerce"
            ),
            "DATE": lambda col: pd.to_datetime(col, format="%d-%m-%Y", errors="coerce"),
            "FLOAT": lambda col: pd.to_numeric(col, errors="coerce"),
            "INTEGER": lambda col: pd.to_numeric(col, errors="coerce"),
            "STRING": lambda col: col.astype(str),
            "BOOLEAN": lambda col: col.astype(bool),
        }

        # Apply schema to DataFrame columns
        for field_schema in self.schema:
            field_name = field_schema["field"]
            field_type = field_schema["type"]

            if field_name not in df_copy.columns:
                self.logger.warning(
                    f"Field '{field_name}' not found in DataFrame. Skipping."
                )
                continue

            if field_type in type_conversion_map:
                df_copy[field_name] = type_conversion_map[field_type](
                    df_copy[field_name]
                )
            else:
                self.logger.warning(
                    f"Unsupported field type '{field_type}' for field '{field_name}'. Skipping."
                )

        return df_copy

    def execute(self):
        # Fetch data from Google Sheets
        self.logger.info(f"Fetching data from Google Sheets: {self.spreadsheet_url}")
        df_sheet = self.fetch_data_from_sheets()
        df_sheet = self.get_dataframe_by_conditions(df_sheet, self.filter_conditions)
        df_sheet = self.normalize_column_names(df_sheet)
        self.logger.info(f"columns: {df_sheet.columns}")
        df_sheet = self.apply_schema_to_dataframe(df_sheet)
        self.logger.info(
            f"Fetched {len(df_sheet)} rows from Google Sheets: {self.spreadsheet_url}"
        )
        self.logger.info(f"columns: {df_sheet.columns}")
        self.load_data_to_bigquery(df_sheet)
