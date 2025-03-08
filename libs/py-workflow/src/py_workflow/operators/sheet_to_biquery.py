from tsdatalake_workflow.operators.base import BaseOperator
from tsdatalake_utils.common.logger import LoggerMixing

from tsdatalake_utils.google.api.sheet import GoogleSheetService
from tsdatalake_utils.google.console.bigquery import BigQueryService


class GGSheetToBigQuery(BaseOperator,LoggerMixing):
    def __init__(self, 
                 spreadsheet_url:str=None, 
                 sheet_name:str=None, 
                 project_id:str=None, 
                 dataset_id:str=None, 
                 table_id:str=None
        ):
        self.spreadsheet_url = spreadsheet_url
        self.sheet_name = sheet_name
        self.google_sheet_service = GoogleSheetService(url=self.spreadsheet_url)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.bigquery_service = BigQueryService(project_id=self.project_id)

    def fetch_data_from_sheets(self):
        # Fetch data from Google Sheets
        df = self.google_sheet_service.read_sheet(self.sheet_name)
        return df

    def load_data_to_bigquery(self, df):
        job = self.bigquery_service.load_table_from_dataframe(df, self.dataset_id, self.table_id)
        self.logger.info(f"Loaded {job.output_rows} rows into {self.dataset_id}:{self.table_id}.")

    def execute(self):
        # Fetch data from Google Sheets
        df = self.fetch_data_from_sheets()
        if df is not None:
            # Load data into BigQuery
            self.logger.info(f"Loading data into BigQuery: {self.dataset_id}:{self.table_id}")
            self.load_data_to_bigquery(df)