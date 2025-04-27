from google.cloud import bigquery

from py_utils.common.logger import LoggerMixin
from py_workflow.operators.base import BaseOperator

# from py_workflow.operators.slack_alert import SlackOperator
from jinja2 import Template

import json


class BigqueryJobAlertOperator(BaseOperator, LoggerMixin):
    def __init__(
        self,
        sql: str,
        slack_conf: dict = None,
        **kwargs,
    ):
        self.sql = sql
        self.slack_conf = slack_conf
        self.client = bigquery.Client()

    def load_data(self):
        query_job = self.client.query(self.sql)
        return query_job.result().to_dataframe()

    def replace_and_render(self, data: list[dict], template_string: str) -> str:
        """
        Replaces "<<>>" with "{{" and "}}" in an array of objects (list of dicts)
        and renders the resulting string using Jinja2.

        Args:
            data: A list of dictionaries where string values might contain "<<>>".
            template_string: The Jinja2 template string.

        Returns:
            The rendered string.
        """
        self.logger.info(f"Template string: {template_string}")
        self.logger.info(f"Data: {data}")
        return

        def replace_in_value(value):
            if isinstance(value, str):
                return value.replace("<<", "{{").replace(">>", "}}")
            return value

        def replace_in_object(obj):
            if isinstance(obj, dict):
                return {k: replace_in_object(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_in_object(elem) for elem in obj]
            else:
                return replace_in_value(obj)

        modified_data = [replace_in_object(item) for item in data]

        # Convert the data to a JSON-like string for safer Jinja2 rendering
        # This helps avoid potential issues with Python object representations in templates
        data_string = json.dumps(modified_data)

        template = Template(template_string)
        return template.render(
            data=json.loads(data_string)
        )  # Load the JSON string back into Python objects

    def execute(self):
        df = self.load_data()
        self.logger.info(f"Dataframe: {df}")
        if df.empty:
            self.logger.info("No data found")
            return
        for idx, row in df.iterrows():
            data = row.to_dict()
            self.logger.info(f"Row {idx}: {data}")
            if self.slack_conf:
                template = self.slack_conf.get("blocks_template")
                obj = row.to_dict()
                rendered = self.replace_and_render(template_string=template, data=obj)
                self.logger.info(f"Template rendered: {rendered}")

                # self.send_slack_alert(data)
        # self.slack_operator = SlackOperator(**seslack_conf)
        # self.slack_operator.send_message(
        #     text=f"Alert: {data['alert']}",
        #     channel=self.slack_conf.get("channel"),
        # )
