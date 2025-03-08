import json
import requests
import base64
import os


class SendGridConfig:
  SENGRID_FROM_EMAIL = os.environ.get("FROM_EMAIL")
  SENGRID_FROM_EMAIL_NAME = os.environ.get("SENGRID_FROM_EMAIL_NAME")
  SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
  SENDGRID_URL = os.environ.get("SENDGRID_API_URL") if os.environ.get(
    "SENDGRID_URL") else "https://api.sendgrid.com/v3/mail/send"


class SendGridMailNotify:
  def __init__(self, from_email, from_email_name, proxies=None):
    self.api_key = SendGridConfig.SENDGRID_API_KEY
    self.from_email = from_email
    self.from_email_name = from_email_name
    self.proxies = proxies

  def send_email(self, to_email, template_id, dynamic_template_data, attachment_path):
    url = SendGridConfig.SENDGRID_URL
    headers = {"Authorization": "Bearer {}".format(self.api_key), "Content-Type": "application/json"}
    data = {
      "personalizations": [{"to": [{"email": to_email}]}],
      "from": {
        "email": self.from_email,
        "name": self.from_email_name
      }
    }
    if template_id is not None:
      data["template_id"] = template_id
    if attachment_path is not None:
      with open(attachment_path, "rb") as f:
        data["attachments"] = [
          {"content": base64.b64encode(f.read()).decode(), "filename": os.path.basename(attachment_path)}]
    if dynamic_template_data is not None:
      data["personalizations"][0]["dynamic_template_data"] = dynamic_template_data
    response = requests.post(url, headers=headers, data=json.dumps(data), proxies=self.proxies, verify=False)
    return response
