from py_utils.common.logger import LoggerMixin
from py_workflow.operators.base import BaseOperator


class BigqueryToSFTPOperator(BaseOperator, LoggerMixin):
    def __init__(self):
        super().__init__()

    def execute(self, **args):
        self.logger.info("BigqueryToSFTPOperator execute")
