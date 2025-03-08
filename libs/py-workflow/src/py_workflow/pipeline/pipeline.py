import importlib

from tsdatalake_utils.utils.file import load_json
from tsdatalake_utils.common.logger import LoggerMixing

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
        self.results.append({'stage': stage_name, 'data': result_data})

    def get_results(self):
        """
        Retrieve all stored results.

        :return: List of results
        """
        return self.results


class Pipeline(LoggerMixing):
    def __init__(self, file_config:str=None):
        self.file_config = file_config
    
    def load_config(self):
        json_data = load_json(self.file_config)
        return json_data
    def run(self):
        config = self.load_config()
        task_executor = TaskExecutor(config)
        task_executor.run()

class TaskExecutor(LoggerMixing):
    def __init__(self, config):
        self.tasks = self.parse_config(config)
        self.executed_tasks = set()


    def parse_config(self, config):
        self.logger.info(f"Config: {config['owner_name']}")
        tasks = []
        for task in config['tasks']:
            self.logger.info(f"Task: {task['task_id']}")
            tasks.append(Task(
                task_id=task['task_id'],
                operator=task['operator'],
                params=task['params'],
                dependencies=task['dependencies']
            ))
        return tasks

    def execute_task(self, task):
        if task.task_id in self.executed_tasks:
            return

        # Execute dependencies first
        for dependency in task.dependencies:
            dependency_task = next(t for t in self.tasks if t.task_id == dependency)
            self.execute_task(dependency_task)

        # Import and instantiate the operator class
        module_name, class_name = task.operator.rsplit('.', 1)
        module = importlib.import_module(module_name)
        operator_class = getattr(module, class_name)
        operator_instance = operator_class(**task.params)

        # Execute the operator
        operator_instance.execute()

        # Mark the task as executed
        self.executed_tasks.add(task.task_id)

    def run(self):
        for task in self.tasks:
            
            self.execute_task(task)

class Task:
    def __init__(self, task_id, operator, params, dependencies):
        self.task_id = task_id
        self.operator = operator
        self.params = params
        self.dependencies = dependencies
