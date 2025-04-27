import os

from py_utils.common.logger import LoggerMixin
from py_utils.notify.slack import SlackNotify


class SlackFailureAlert(LoggerMixin):
    def __init__(
        self,
        channel: str,
        message: str,
        dag_name: str = None,
        task_id: str = None,
        owner: str = None,
        error_message: str = None,
    ):
        self.dag_name = dag_name
        self.channel = channel
        self.message = message
        self.owner = owner
        self.task_id = task_id
        self.error_message = error_message
        self.token = os.getenv("SLACK_API_TOKEN")
        self.schannel = os.getenv("SLACK_CHANNEL", channel)
        if not self.token or not self.schannel:
            raise ValueError(
                "SLACK_API_TOKEN or SLACK_CHANNEL environment variable is not set."
            )

    def render_failure_blocks(
        self,
        dag_name: str = None,
        owner: str = None,
        task_id: str = None,
        error_message: str = None,
    ):
        return [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f":red_circle: [Datalake Tool] - It looks like there's a task that failed @{owner} .Please check it quickly! 🙇",
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Pipeline Name:*\n *_{dag_name}_*"},
                    {"type": "mrkdwn", "text": f"*Task Id*:\n *_{task_id}_*"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Error Message*:\n ```{error_message}```",
                    },
                    {"type": "mrkdwn", "text": f"*Created At:*\n@{owner}"},
                ],
            },
        ]

    def send(self):
        """
        Send the alert message to the Slack channel.
        """
        # Implement your Slack API call here
        self.logger.info(f"Sending alert to Slack channel: {self.channel}")
        self.slack_notify = SlackNotify(token=self.token, channel=self.channel)
        self.slack_notify.send_slack_message(
            message=self.message,
            blocks=self.render_failure_blocks(
                dag_name=self.dag_name,
                owner=self.owner,
                task_id=self.task_id,
                error_message=self.error_message,
            ),
        )
