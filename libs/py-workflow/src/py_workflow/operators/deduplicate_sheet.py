from py_utils.common.logger import LoggerMixin
from py_workflow.operators.base import BaseOperator

from py_utils.google.api.sheet import GoogleSheetService
from py_utils.utils.dataframe import dedup_and_order_df


class DeduplicateSheetOperator(BaseOperator, LoggerMixin):
    """
    Operator to deduplicate a sheet in a Google Spreadsheet.

    Args:
        spreadsheet_id (str): The ID of the Google Spreadsheet.
        sheet_name (str): The name of the sheet to deduplicate.
        column_name (str): The name of the column to use for deduplication.
        **kwargs: Additional arguments for the operator.
    """

    def fetch_data_from_sheets(self, sheet_name=None):
        # Fetch data from Google Sheets
        df = self.ggsheet_service.read_sheet(sheet_name)
        return df

    def __init__(
        self,
        spreadsheet_url: str = None,
        sheet_name: str = None,
        unique_keys: list = None,
        order_by: list = None,
        ascending: list = None,
        **kwargs,
    ):
        self.spreadsheet_url = spreadsheet_url
        self.sheet_name = sheet_name
        self.unique_keys = unique_keys
        self.ascending = ascending
        self.order_by = order_by
        self.ggsheet_service = GoogleSheetService(url=self.spreadsheet_url)

        super().__init__(**kwargs)

    def execute(self, **args):
        df = self.fetch_data_from_sheets(sheet_name=self.sheet_name)
        self.logger.info(f"Dataframe: {df}")
        if df.empty:
            self.logger.info("No data found")
            return
        self.logger.info(f"Deduplicating dataframe: {df}")
        deduped_all = dedup_and_order_df(
            df=df,
            subset=self.unique_keys,
            order_by=self.order_by,
            ascending=self.ascending,
        )
        self.logger.info(f"Deduplicated dataframe: {deduped_all}")
