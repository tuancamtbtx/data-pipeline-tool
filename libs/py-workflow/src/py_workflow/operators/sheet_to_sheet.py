import pandas as pd
from typing import List, Dict
from py_workflow.operators.base import BaseOperator
from py_utils.common.logger import LoggerMixin

from py_utils.google.api.sheet import GoogleSheetService
from py_utils.utils.dataframe import (
    get_diff_rows_left_keep_all,
    remove_xy_suffixes,
)


class SheetToSheetOperator(LoggerMixin, BaseOperator):
    def __init__(
        self,
        src_spreadsheet_url: str = None,
        dest_spreadsheet_url: str = None,
        src_sheet_name: str = None,
        dest_sheet_name: str = None,
        unique_keys: list = None,
        write_mode: str = "w",
        headers: list = None,
        filter_conditions: list = None,
        dropdown_headers: any = None,
        is_remove_sync_data: bool = False,
        **kwargs,
    ):
        self.src_spreadsheet_url = src_spreadsheet_url
        self.src_sheet_name = src_sheet_name
        self.logger.info(f"src_sheet_name: {self.src_sheet_name}")
        self.logger.info(f"src_spreadsheet_url: {self.src_spreadsheet_url}")
        self.src_google_sheet_service = GoogleSheetService(url=self.src_spreadsheet_url)
        self.dest_sheet_name = dest_sheet_name
        self.dest_spreadsheet_url = dest_spreadsheet_url
        self.filter_conditions = filter_conditions
        self.dest_google_sheet_service = GoogleSheetService(
            url=self.dest_spreadsheet_url
        )
        self.unique_keys = unique_keys
        self.write_mode = write_mode
        self.headers = headers
        self.logger.info(f"dropdown_headers: {dropdown_headers}")
        self.dropdown_headers = dropdown_headers
        self.is_remove_sync_data = is_remove_sync_data

    def fetch_data_from_sheets(self, sheet_name=None):
        # Fetch data from Google Sheets
        df = self.src_google_sheet_service.read_sheet(sheet_name)
        return df

    def load_data_to_sheets(self, df):
        self.dest_google_sheet_service.append_row(
            sheet_idx=self.dest_sheet_name,
            row=df.values.tolist(),
        )

    def _validate_headers(self, source_df, dest_df):
        if self.headers:
            for header in self.headers:
                if header not in source_df.columns:
                    raise ValueError(
                        f"Header '{header}' not found in source DataFrame."
                    )
                if header not in dest_df.columns:
                    raise ValueError(
                        f"Header '{header}' not found in destination DataFrame."
                    )

    def _filter_dataframe(
        self, df: pd.DataFrame, conditions: List[Dict]
    ) -> pd.DataFrame:
        """
        Filters a Pandas DataFrame based on a list of filter conditions.

        Args:
                        df: The Pandas DataFrame to filter.
                        conditions: A list of dictionaries, where each dictionary represents a filter condition.
                                        Example:
                                        [
                                                        {'field': 'Status', 'operator': 'IN', 'value': ['TSA follow', 'Đồng ý vay', 'QC_Potential']},
                                                        {'field': 'Amount', 'operator': '>=', 'value': 1000}
                                        ]

        Returns:
                        A new Pandas DataFrame containing only the rows that satisfy all the given conditions.
        """
        filtered_df = (
            df.copy()
        )  # Start with a copy to avoid modifying the original DataFrame

        for condition in conditions:
            field = condition["field"]
            operator = condition["operator"]
            value = condition["value"]

            if field not in filtered_df.columns:
                print(
                    f"Warning: Field '{field}' not found in DataFrame. Skipping condition."
                )
                continue  # Skip this condition if the field doesn't exist

            if operator == "IN":
                filtered_df = filtered_df[filtered_df[field].isin(value)]
            elif operator == "=" or operator == "==":
                filtered_df = filtered_df[filtered_df[field] == value]
            elif operator == "!=" or operator == "<>":
                filtered_df = filtered_df[filtered_df[field] != value]
            elif operator == ">":
                filtered_df = filtered_df[filtered_df[field] > value]
            elif operator == ">=":
                filtered_df = filtered_df[filtered_df[field] >= value]
            elif operator == "<":
                filtered_df = filtered_df[filtered_df[field] < value]
            elif operator == "<=":
                filtered_df = filtered_df[filtered_df[field] <= value]
            else:
                print(
                    f"Warning: Unsupported operator '{operator}'. Skipping condition."
                )
                continue  # Skip unsupported operators

        return filtered_df

    def execute(self, **args):
        source_df = self.fetch_data_from_sheets(self.src_sheet_name)
        if self.filter_conditions:
            source_df = self._filter_dataframe(source_df, self.filter_conditions)
        if source_df.empty:
            self.logger.info("No data found.")
            return
        source_df = source_df[self.headers]
        dest_df = self.fetch_data_from_sheets(self.dest_sheet_name)
        self.logger.info(f"dest_df: {dest_df}")
        self.logger.info(f"source_df: {source_df}")
        if not dest_df.empty:
            self.logger.info("No data found in destination sheet.")
            self._validate_headers(source_df, dest_df)
        new_data = get_diff_rows_left_keep_all(
            source_df,
            dest_df,
            headers=self.headers,
            on=self.unique_keys,
        )
        self.logger.info(f"size data write: {len(new_data)}")
        if new_data.empty:
            self.logger.info("No new data to write.")
            return
        new_data = remove_xy_suffixes(new_data)
        self.logger.info(f"new_data: {new_data}")
        if self.write_mode == "w":
            self.dest_google_sheet_service.clear_sheet(self.dest_sheet_name)
            self.dest_google_sheet_service.append_rows(
                sheet_idx=self.dest_sheet_name,
                row=new_data.values.tolist(),
            )
        elif self.write_mode == "a":
            self.dest_google_sheet_service.export_to_sheets(
                sheet_idx=self.dest_sheet_name,
                df=new_data,
                mode="a",
            )
        if self.is_remove_sync_data:
            key_columns = self.unique_keys
            key_values = new_data[key_columns].values.tolist()
            self.logger.info(f"total data remove: {len(key_values)}")
            self.src_google_sheet_service.remove_rows_by_keys(
                sheet_id=self.src_sheet_name,
                key_columns=key_columns,
                key_values=key_values,
            )
