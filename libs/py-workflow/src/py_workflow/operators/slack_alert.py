import ssl
import certifi
import json
import requests
from py_utils.common.logger import LoggerMixin
from py_workflow.operators.base import BaseOperator

ssl_context = ssl.create_default_context(cafile=certifi.where())


class SlackOperator(BaseOperator, LoggerMixin):
    API_URL = "https://slack.com/api/chat.postMessage"

    def __init__(
        self, token: str = None, channel: str = None, message: str = None, blocks=None
    ):
        self.token = token
        self.channel = channel
        self.message = message
        self.blocks = blocks

    def send_slack_message(self, message, blocks=None):
        payload = {
            "channel": self.channel,
            "text": message,
            "color": "#00FF00",
            "blocks": blocks,
        }
        try:
            self.logger.info(f"send_slack_message - {message} to: {self.channel}")
            response = requests.post(
                SlackOperator.API_URL,
                data=json.dumps(payload),
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
            )
            # Check the response
            if response.status_code == 200:
                self.logger.info("Message sent successfully")
            else:
                self.logger.info(
                    f"Failed to send message - Status code: {response.status_code}, Response: {response.text}"
                )
        except Exception as e:
            self.logger.error(e)

    def send_message(self):
        try:
            self.logger.info(f"Sending message to {self.channel}")
            self.send_slack_message(message=self.message, blocks=self.blocks)
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")

    def execute(self):
        # Execute the send_message method
        self.send_message()
