from googleapiclient.discovery import build
from oauth2client import client
import gspread
import google.auth
import gspread_dataframe as gd
import pandas as pd

from tsdatalake_utils.common.logger import LoggerMixing


class GoogleSheetService(LoggerMixing):
  def __init__(self,
               url: str = None,
               **kwargs):
    super().__init__()
    self.url = url
    credentials, project_id = google.auth.default(
      scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
      ]
    )
    self.logger.info(f"project_id: {project_id}")
    self.gc = self.authorize(credentials)
    self.spread_sheet = self.gc.open_by_url(self.url)

  def service(self):
    service = build('sheets', 'v4', credentials=self.creds)
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

  def write_cell(self, sheet_idx, cell, value):
    current_worksheet = self.spread_sheet.get_worksheet(sheet_idx)
    current_worksheet.update_acell(cell, value)

  def export_to_sheets(self, sheet_idx, df, mode='r'):
    current_worksheet = self.spread_sheet.get_worksheet(sheet_idx)
    if (mode == 'w'):
      current_worksheet.clear()
      gd.set_with_dataframe(worksheet=current_worksheet, dataframe=df, include_index=False, include_column_header=True,
                            resize=True)
      return True
    elif (mode == 'a'):
      current_worksheet.add_rows(df.shape[0])
      gd.set_with_dataframe(worksheet=current_worksheet, dataframe=df, include_index=False, include_column_header=False,
                            row=current_worksheet.row_count + 1, resize=False)
      return True
    else:
      return gd.get_as_dataframe(worksheet=current_worksheet)
  def read_sheet(self, sheet_name):
      current_worksheet = self.spread_sheet.worksheet(sheet_name)
      expected_headers = current_worksheet.row_values(1)
      self.logger.info(f"expected headers: {expected_headers}")
      data = current_worksheet.get_all_records()
      df = pd.DataFrame(data)
      self.logger.info(f"colums: {df.columns}")
      return df
