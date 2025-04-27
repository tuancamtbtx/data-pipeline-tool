import traceback
from dotenv import load_dotenv

from py_utils.utils.path import get_absolute_path
from py_utils.common.logger import LoggerMixin
from py_workflow.pipeline.factory import PipelineFactory

load_dotenv()


class Processor(LoggerMixin):
    def __init__(self, pipeline_file_name: str = None):
        self.pipeline_file_name = pipeline_file_name

    def execute(self):
        try:
            file_config = get_absolute_path(self.pipeline_file_name)
            pipeline_factory = PipelineFactory(file_config=file_config)
            pipeline_factory.run()
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"{e}")
        except InterruptedError as e:
            self.logger.error(f"Exit Process {e}")


def main():
    processor = Processor(pipeline_file_name="pipeline.yaml")
    processor.execute()


if __name__ == "__main__":
    main()
