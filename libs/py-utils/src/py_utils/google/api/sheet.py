from googleapiclient.discovery import build
from oauth2client import client
import gspread
import google.auth
import gspread_dataframe as gd
import pandas as pd
from typing import List
from py_utils.common.logger import LoggerMixin
import time


class GoogleSheetService(LoggerMixin):
    def __init__(self, url: str = None, **kwargs):
        super().__init__()
        self.url = url
        credentials, project_id = google.auth.default(
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
        )
        self.logger.info(f"project_id: {project_id}")
        self.gc = self.authorize(credentials)
        self.spread_sheet = self.gc.open_by_url(self.url)

    def service(self):
        service = build("sheets", "v4", credentials=self.creds)
        return service

    def authorize(self, credentials):
        gc = gspread.authorize(credentials)
        return gc

    def get_creds(self):
        creds = client.GoogleCredentials.get_application_default().create_scoped(
            self._SCOPES
        )
        return creds

    def get_worksheet_by_idx(self, index):
        return self.spread_sheet.get_worksheet(index)

    def get_worksheet_by_name(self, title):
        return self.spread_sheet.worksheet(title)

    def get_worksheet(self, key):
        if isinstance(key, int):
            return self.get_worksheet_by_idx(key)
        else:
            return self.get_worksheet_by_name(key)

    def is_sheet_exists(self, sheet_name):
        try:
            self.spread_sheet.worksheet(sheet_name)
            return True
        except gspread.exceptions.WorksheetNotFound:
            return False

    def write_cell(self, sheet_idx, cell, value):
        current_worksheet = self.spread_sheet.get_worksheet(sheet_idx)
        current_worksheet.update_acell(cell, value)

    def append_rows(self, sheet_idx, row):
        current_worksheet = self.get_worksheet(sheet_idx)
        current_worksheet.append_row(row)

    def deduplicate(self, df, unique_keys):
        """Remove duplicates based on unique keys."""
        if unique_keys:
            df = df.drop_duplicates(subset=unique_keys)
        return df

    def remove_rows_by_keys(
        self,
        sheet_id: str,
        key_columns: List[str],
        key_values: List[
            List
        ],  # List of lists, each sublist is a set of key values for a row
    ):
        """
        Removes rows from a Google Sheet based on matching specific values in key columns.

        Args:
            sheet_id: The ID of the Google Sheet.
            worksheet_name: The name of the worksheet within the sheet.
            key_columns: A list of column names to use as the key for identifying rows to remove.
            key_values: A list of lists. Each sublist contains the values that, when matched
                        in the corresponding key_columns, indicate a row to be removed.
                        The order of values within a sublist corresponds to the order of
                        key_columns.
            credentials_file: Path to the JSON file containing your Google service
                            account credentials.
        """
        worksheet = self.get_worksheet(sheet_id)
        # 2. Get all data from the worksheet
        data = worksheet.get_all_values()
        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)

        # 3. Find rows to delete
        rows_to_delete = []
        for i, row in df.iterrows():
            for value_set in key_values:
                match = True
                for j, key_col in enumerate(key_columns):
                    if key_col not in df.columns:
                        raise ValueError(f"Key column '{key_col}' not found in sheet.")
                    if str(row[key_col]) != str(
                        value_set[j]
                    ):  # Compare as strings for safety
                        match = False
                        break  # No need to check other key columns for this value_set
                if match:
                    rows_to_delete.append(
                        i + 2
                    )  # +2 because gspread uses 1-based indexing and headers are row 1
                    break  # No need to check other value_sets for this row

        # 4. Delete rows (in reverse order to avoid shifting issues)
        rows_to_delete.sort(reverse=True)
        for row_num in rows_to_delete:
            worksheet.delete_rows(row_num)
            self.logger.info(f"Deleted row {row_num} from {sheet_id}")
            time.sleep(0.2)

    def export_to_sheets(self, sheet_idx, df, mode="r"):
        if not self.is_sheet_exists(sheet_idx):
            self.spread_sheet.add_worksheet(title=sheet_idx, rows=1, cols=1)
        current_worksheet = self.get_worksheet(sheet_idx)
        self.logger.info(f"current_worksheet name: {current_worksheet.title}")
        if mode == "w":
            current_worksheet.clear()
            gd.set_with_dataframe(
                worksheet=current_worksheet,
                dataframe=df,
                include_index=False,
                include_column_header=True,
                resize=True,
            )
            return True
        elif mode == "a":
            gd.set_with_dataframe(
                worksheet=current_worksheet,
                dataframe=df,
                include_index=False,
                include_column_header=(
                    False if self._is_headers_exist(sheet_idx) else True
                ),
                row=len(current_worksheet.get_all_values()) + 1,
                resize=False,
            )
            return True
        else:
            raise ValueError("Invalid mode. Use 'w' for write or 'a' for append.")

    def _is_headers_exist(self, sheet_name):
        current_worksheet = self.spread_sheet.worksheet(sheet_name)
        headers = current_worksheet.get_all_values()
        if len(headers) > 0:
            return True
        else:
            return False

    def read_sheet(self, sheet_name, headers=None):
        current_worksheet = self.spread_sheet.worksheet(sheet_name)
        data = current_worksheet.get_all_values()
        if len(data) >= 2:
            df = pd.DataFrame(data[1:], columns=data[0])
            return df
        else:
            return pd.DataFrame(columns=headers)
