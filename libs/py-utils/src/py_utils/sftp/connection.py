import pysftp


class SFTPConnection(pysftp.Connection):
  def __init__(self, *args, **kwargs):
    self._sftp_live = False
    self._transport = None
    super().__init__(*args, **kwargs)
