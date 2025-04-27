from py_workflow.operators.base import BaseOperator
from py_utils.common.logger import LoggerMixin


class BigqueryToBigqueryOperator(LoggerMixin, BaseOperator):
    def __init__(
        self,
        query: str,
        project_id: str = None,
        write_mode: str = "w",
        dest_table: str = None,
        unique_keys: list = None,
    ):
        super().__init__()
        self.query = query
        self.project_id = project_id
        self.write_mode = write_mode
        self.dest_table = dest_table
        self.unique_keys = unique_keys

    def fetch_dataframe_from_bigquery(self):
        self.logger.info(f"Running query:\n {self.sql}")
        results = self.bq_service.run_query(self.sql)
        return results.to_dataframe()

    def load_dataframe_to_bigquery(self, data):
        pass

    def execute(**args):
        return super().execute()
