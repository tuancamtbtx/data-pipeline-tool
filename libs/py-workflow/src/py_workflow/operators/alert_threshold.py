from google.cloud import bigquery

from py_utils.common.logger import LoggerMixin
from py_workflow.operators.base import BaseOperator
from py_workflow.operators.slack_alert import SlackOperator


class AlertThresholdOperator(BaseOperator, LoggerMixin):
    def __init__(
        self,
        *,
        sql: str,
        threshold_conf: dict = None,
        slack_conf: dict = None,
        **kwargs,
    ):
        self.sql = sql
        self.threshold_conf = threshold_conf
        self.slack_conf = slack_conf
        self.client = bigquery.Client()
        self.slack_operator = SlackOperator(**slack_conf)

    def load_data(self):
        query_job = self.client.query(self.sql)
        return query_job.result().to_dataframe()

    def _check_condition(self, data):
        metric = self.threshold_conf.get("metric")
        operator = self.threshold_conf.get("operator")
        threshold = self.threshold_conf.get("threshold")
        metric_val = data.get(metric)
        self.logger.info(
            f"Metric {metric} has Value: {metric_val} - Threshold: {threshold}"
        )
        if metric_val is None:
            return False
        if operator == "greater":
            return metric_val > threshold
        elif operator == "less":
            return metric_val < threshold
        elif operator == "equal":
            return metric_val == threshold
        elif operator == "greater_equal":
            return metric_val >= threshold
        elif operator == "less_equal":
            return metric_val <= threshold
        elif operator == "not_equal":
            return metric_val != threshold
        return False

    def check_threshold(self):
        df = self.load_data()
        if df.empty:
            self.logger.info("No data found")
            return
        data = df.to_dict(orient="records")[0]
        compares = self._check_condition(data=data)
        self.logger.info(f"Compares: {compares}")
        if compares:
            self.logger.info("Alert Threshold is reached")
            self.slack_operator.execute()
        else:
            self.logger.info("Alert Threshold is not reached")

    def execute(self):
        self.logger.info(f"slack_conf: {self.slack_conf}")
        self.check_threshold()
