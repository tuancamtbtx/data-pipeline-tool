import pandas as pd
from py_workflow.operators.base import BaseOperator
from py_utils.common.logger import LoggerMixin

from py_utils.google.console.bigquery import BigqueryService
from py_utils.google.api.sheet import GoogleSheetService

from py_utils.utils.dataframe import (
    get_rows_not_in_a_df,
    remove_xy_suffixes,
)


class WRITEMODE:
    APPEND = "a"
    TRUNCATE = "w"


class BigqueryToGGSheetOperator(BaseOperator, LoggerMixin):
    def __init__(
        self,
        project_id: str = None,
        sql: str = None,
        spreadsheet_url: str = None,
        headers: list = None,
        unique_keys: list = None,
        sheet_name: str = None,
        write_mode: str = "w",
    ):
        self.project_id = project_id
        self.sql = sql
        self.spreadsheet_url = spreadsheet_url
        self.sheet_name = sheet_name
        self.write_mode = write_mode
        self.headers = headers
        self.unique_keys = unique_keys

        # Initialize BigQuery client
        self.bq_service = BigqueryService(project_id=self.project_id)
        self.ggsheet_service = GoogleSheetService(url=self.spreadsheet_url)

    def fetch_dataframe_from_bigquery(self):
        self.logger.info(f"Running query:\n {self.sql}")
        results = self.bq_service.run_query(self.sql)
        return results.to_dataframe()

    def fetch_data_from_sheets(self, sheet_name=None):
        # Fetch data from Google Sheets
        df = self.ggsheet_service.read_sheet(sheet_name)
        return df

    def update_google_sheet(self, data):
        self.logger.info(f"Updating Google Sheet\n: {self.spreadsheet_url}")
        self.ggsheet_service.export_to_sheets(
            sheet_idx=self.sheet_name,
            df=pd.DataFrame(data),
            mode=self.write_mode,
        )

    def execute(self):
        # Fetch data from BigQuery
        df_bq = self.fetch_dataframe_from_bigquery()
        df_sheet = self.fetch_data_from_sheets(sheet_name=self.sheet_name)
        if df_bq.empty:
            self.logger.info("No data found.")
            return
        if df_sheet.empty:
            self.logger.info("No data found in Google Sheet.")
            df_sheet = pd.DataFrame(columns=self.headers)
        new_data = get_rows_not_in_a_df(
            df_bq,
            df_sheet,
            headers=self.headers,
            unique_keys=self.unique_keys,
        )
        self.logger.info(f"size data write: {len(new_data)}")
        if new_data.empty:
            self.logger.info("No new data to write.")
            return
        new_data = remove_xy_suffixes(new_data)
        # Update Google Sheet with fetched data
        self.update_google_sheet(new_data)
