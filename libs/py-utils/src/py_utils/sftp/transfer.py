import os

from pytrust.logger.logger_factory import LoggerFactory
import pysftp


class SFTPTransfer(LoggerFactory):
  def __init__(self, host: str = None, username: str = None, password: str = None, port=22):
    self.host = host
    self.username = username
    self.password = password
    self.port = port

  def upload(self, local_path: str = None, remote_folder: str = None, remote_path: str = None):
    with pysftp.Connection(host=self.host, username=self.username, password=self.password, port=self.port) as sftp:
      if os.path.isfile(local_path):
        self.logger.info("Processing file from {} to {}".format(local_path, remote_path))
        # sftp.put(localpath=local_path, remotepath=remote_path)
        path = sftp.listdir(remote_folder)
        self.logger.info("Current path: {}".format(path))
        self.logger.info("Uploaded file from {} to {}".format(local_path, remote_path))
      else:
        raise IOError('Could not find localFile %s !!' % local_path)

  def download(self, remote_path, local_path):
    with pysftp.Connection(host=self.host, username=self.username, password=self.password, port=self.port) as sftp:
      sftp.get(remote_path, local_path)
      self.logger.info("Downloaded file from {} to {}".format(remote_path, local_path))
