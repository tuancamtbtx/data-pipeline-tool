from typing import Dict, Any

from py_utils.utils.file import load_yaml
from py_utils.common.logger import LoggerMixin
from py_workflow.pipeline.pipeline import PipelineV1
from py_workflow.pipeline.render import render


class PipelineRunnerV1:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def execute(self):
        pipeline = PipelineV1(config=self.config)
        pipeline.run()


class PipelineFactory(LoggerMixin):
    def __init__(self, file_config: str) -> None:
        self.pipelines = {"v1": PipelineRunnerV1}
        self.config = self.load_config(file_config)

    @render
    def load_config(self, file_config):
        data = load_yaml(file_config)
        return data

    def get_pipeline_buidler(self, version: str):
        if self.config is None:
            self.logger.error("Pipeline Config is None")
            return
        version = self.config.get("version", "v1")
        return self.pipelines[version]

    def run(self):
        version = self.config.get("version", "v1")
        pipeline_builder = self.get_pipeline_buidler(version)
        pipeline_builder(self.config).execute()
