import os
import requests
import json

from pytrust.logger.logger_factory import LoggerFactory


def get_file_content(file_name):
  content = ''
  with open(file_name, 'rb') as f:
    content = f.read()
  return content


class SlackConfig:
  SLACK_TOKEN = os.environ.get('SLACK_TOKEN')
  SLACK_CHANNEL_ID = os.environ.get('SLACK_CHANNEL_ID')
  API_URL = os.environ.get("SLACK_API_URL") if os.environ.get(
    "SLACK_API_URL") else "https://slack.com/api/chat.postMessage"


class SlackNotify(LoggerFactory):
  API_URL = "https://slack.com/api/chat.postMessage"
  API_URL_UPLOAD = "https://slack.com/api/files.upload"

  def __init__(self, channel: str = None, proxy=None):
    self.proxy = proxy
    self.slack_token = SlackConfig.SLACK_TOKEN
    self.channel = channel if channel is not None else SlackConfig.SLACK_CHANNEL_ID

  def send_slack_message_upload(self, message, file_name=None, file_type=None):

    payload = {
      "channels": self.channel,
      "filename": file_name,
      "initial_comment": message,
      "filetype": file_type,
      "content": get_file_content(file_name)
    }
    try:
      self.logger.info(f"send_slack_message - {message} to: {self.channel}")
      response = requests.post(SlackNotify.API_URL_UPLOAD, data=payload,
                               headers={"Authorization": f"Bearer {self.slack_token}",
                                        "Content-Type": "application/x-www-form-urlencoded"}, verify=False)
      # Check the response
      if response.status_code == 200:
        self.logger.info("Message sent successfully")
        return True
      else:
        self.logger.info(f"Failed to send message - Status code: {response.status_code}, Response: {response.text}")
        return False
    except Exception as e:
      self.logger.error(e)
      return False

  def send_slack_message(self, message, blocks=None):
    payload = {
      "channel": self.channel,
      "text": message,
      "color": "#3AA3E3",
      "blocks": blocks,
    }
    try:
      self.logger.info(f"send_slack_message - {message} to: {self.channel}")
      response = requests.post(SlackNotify.API_URL, data=json.dumps(payload),
                               headers={"Authorization": f"Bearer {self.slack_token}",
                                        "Content-Type": "application/json"})
      # Check the response
      if response.status_code == 200:
        self.logger.info("Message sent successfully")
        return True
      else:
        self.logger.info(f"Failed to send message - Status code: {response.status_code}, Response: {response.text}")
        return False
    except Exception as e:
      self.logger.error(e)
      return False
