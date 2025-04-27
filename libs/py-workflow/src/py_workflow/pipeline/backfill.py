from py_utils.common.logger import LoggerMixin


class BackfillPipeline(LoggerMixin):
    def __init__(self, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def run(self, **kwargs):
        raise NotImplementedError("This method should be overridden in a subclass")
