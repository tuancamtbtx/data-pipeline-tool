from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
from tsdatalake_workflow.operators.base import BaseOperator
from tsdatalake_utils.common.logger import LoggerMixing

class BigqueryToGGSheetOperator(BaseOperator, LoggerMixing):
    def __init__(self, project_id, dataset_id, table_id, spreadsheet_id, sheet_name, credentials_path):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.credentials_path = credentials_path

        # Initialize BigQuery client
        self.bq_client = bigquery.Client.from_service_account_json(credentials_path)

        # Initialize Google Sheets service
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        self.sheets_service = build('sheets', 'v4', credentials=credentials)

    def fetch_data_from_bigquery(self):
        query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`"
        query_job = self.bq_client.query(query)

        results = query_job.result()
        rows = [list(row.values()) for row in results]
        return rows

    def update_google_sheet(self, data):
        # Prepare the data in the format required by Google Sheets
        body = {
            'values': data
        }
        # Update the specified range in the spreadsheet
        result = self.sheets_service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=self.sheet_name,
            valueInputOption='RAW',
            body=body
        ).execute()

        print(f"{result.get('updatedCells')} cells updated.")
        return result

    def execute(self):
        # Fetch data from BigQuery
        data = self.fetch_data_from_bigquery()

        if not data:
            print("No data found.")
            return

        # Update Google Sheet with fetched data
        self.update_google_sheet(data)

# Example usage:
# operator = BigqueryToGGSheetOperator(
#     project_id='your_project_id',
#     dataset_id='your_dataset_id',
#     table_id='your_table_id',
#     spreadsheet_id='your_spreadsheet_id',
#     sheet_name='your_sheet_name',
#     credentials_path='path_to_your_service_account_credentials.json'
# )
# operator.execute()