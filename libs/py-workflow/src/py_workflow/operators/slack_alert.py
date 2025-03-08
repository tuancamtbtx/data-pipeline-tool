from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tsdatalake_utils.common.logger import LoggerMixing
class SlackOperator(LoggerMixing):
    
    def __init__(self, token, channel, message):
        self.token = token
        self.channel = channel
        self.message = message

        # Initialize Slack client
        self.client = WebClient(token=self.token)

    def send_message(self):
        try:
            # Send message to the specified Slack channel
            response = self.client.chat_postMessage(
                channel=self.channel,
                text=self.message
            )
            self.logger.info(f"Message sent to {self.channel}, timestamp: {response['ts']}")
        except SlackApiError as e:
            self.logger.error(f"Error sending message: {e.response['error']}")

    def execute(self):
        # Execute the send_message method
        self.send_message()
