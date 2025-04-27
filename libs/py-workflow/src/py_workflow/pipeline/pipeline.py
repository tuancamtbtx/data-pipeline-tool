import importlib
import pickle
from typing import Dict, Any, List
from abc import ABC, abstractmethod

from py_utils.common.logger import LoggerMixin
from py_workflow.pipeline.config import Task, TaskFields, DagFields
from py_workflow.pipeline.alert import SlackFailureAlert


class PipelineResultStore:
    def __init__(self):
        # Initialize an empty list to store results
        self.results = []

    def add_result(self, stage_name, result_data):
        """
        Add a result to the store.

        :param stage_name: Name of the pipeline stage
        :param result_data: Data to store, typically a dictionary or a Pandas DataFrame
        """
        pickle_data = pickle.dumps(result_data)
        self.results.append({"stage": stage_name, "data": pickle_data})

    def get_results(self):
        """
        Retrieve all stored results.

        :return: List of results
        """
        return self.results

    def get_result(self, stage_name):
        """
        Retrieve a specific result by stage name.

        :param stage_name: Name of the pipeline stage
        :return: Result data
        """
        return next(
            result["data"] for result in self.results if result["stage"] == stage_name
        )


class AbstractPipeline(ABC, LoggerMixin):
    def __init__(self):
        self.result_store = PipelineResultStore()

    @abstractmethod
    def make_task(self, task_dict: Dict[str, Any]):
        """
        Add a task to the pipeline.

        :param operator: Operator instance
        :param task_id: Task identifier
        """
        pass

    def make_tasks(self, configs: Dict[str, Any]):
        """
        Add tasks to the pipeline.

        :param config: Pipeline configuration
        """
        pass

    @abstractmethod
    def _validate_task(self, task: Dict[str, Any]):
        pass

    @abstractmethod
    def _validate_pipeline(self):
        pass

    @abstractmethod
    def run(self):
        """
        Run the pipeline.
        """
        pass


class PipelineV1(AbstractPipeline, LoggerMixin):
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config

    def _validate_task(self, task):
        required_keys = ["operator", "params", "task_id"]
        for key in required_keys:
            if key not in task:
                raise ValueError(f"Task is missing required key: {key}")
        return True

    def _validate_pipeline(self, pipeline):
        required_keys = ["owner_name", "version", "name", "tasks"]
        for key in required_keys:
            if key not in pipeline:
                raise ValueError(f"Task is missing required key: {key}")
        return True

    def make_task(self, task_dict):
        is_valid_task = self._validate_task(task_dict)
        if not is_valid_task:
            raise ValueError(f"Invalid task: {task_dict}")
        return Task(
            task_id=task_dict[TaskFields.task_id],
            operator=task_dict[TaskFields.operator],
            params=task_dict[TaskFields.params],
            dependencies=task_dict[TaskFields.dependencies],
        )

    def make_tasks(self, configs) -> List[Task]:
        tasks = []
        if not configs or len(configs) == 0:
            raise ValueError("No tasks found in the pipeline")
        for config in configs:
            tasks.append(self.make_task(config))
        return tasks

    def run(self):
        try:
            self.logger.info(
                f"Running Datalake-Tool Version {self.config.get(DagFields.version)}"
            )
            self.logger.info(f"Running Job Name {self.config.get(DagFields.name)}")
            is_valid_pipeline = self._validate_pipeline(self.config)
            if not is_valid_pipeline:
                raise ValueError(f"Invalid pipeline: {self.config}")
            tasks = self.make_tasks(self.config.get(DagFields.tasks, []))
            task_executor = TaskExecutor(
                dag_config=self.config,
                tasks=tasks,
            )
            task_executor.run()
        except Exception as e:
            raise Exception(f"{e}")


class TaskExecutor(LoggerMixin):

    def __init__(self, dag_config: any, tasks: List[Task]):
        self.dag_config = dag_config
        self.tasks = tasks
        self.executed_tasks = set()

    def execute_task(self, task):
        try:
            if task.task_id in self.executed_tasks:
                self.logger.info(f"Task {task.task_id} already executed")
                return
            # Execute dependencies first
            for dependency in task.dependencies:
                dependency_task = next(t for t in self.tasks if t.task_id == dependency)
                self.execute_task(dependency_task)

            # Import and instantiate the operator class
            module_name, class_name = task.operator.rsplit(".", 1)
            module = importlib.import_module(module_name)
            operator_class = getattr(module, class_name)
            operator_instance = operator_class(**task.params)

            # Execute the operator
            operator_instance.execute()

            # Mark the task as executed
            self.executed_tasks.add(task.task_id)
        except Exception as e:
            if self.dag_config.get(DagFields.slack_on_failure):
                # Send a Slack notification if configured
                self.logger.info(
                    f"Slack notification sent for failure in pipeline: {self.dag_config.get(DagFields.name)}"
                )
                SlackFailureAlert(
                    dag_name=self.dag_config.get(DagFields.name),
                    channel=self.dag_config.get(DagFields.slack_channel),
                    message=f"Pipeline {self.dag_config.get(DagFields.name)} failed",
                    task_id=f"{task.task_id}",
                    owner=self.dag_config.get(DagFields.owner_name),
                    error_message=str(e),
                ).send()
            raise Exception(f"Error executing task {task.task_id}: {e}")

    def run(self):
        for task in self.tasks:
            self.execute_task(task)
