import smtplib

from email.mime.multipart import MIMEMultipart


class SMTP():
  def __init__(self, host, port, username, password, use_tls=False, use_ssl=False):
    self.host = host
    self.port = port
    self.username = username
    self.password = password
    self.use_tls = use_tls
    self.use_ssl = use_ssl

  def send(self, mail_from, mail_to, msg):
    server = smtplib.SMTP_SSL(self.host, self.port)
    server.ehlo()
    server.login(self.username, self.password)
    server.sendmail(mail_from, mail_to, msg.as_string())
    server.close()
    print("mail sent")
