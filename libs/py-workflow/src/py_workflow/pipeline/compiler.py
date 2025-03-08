from importlib import import_module
from typing import Dict, Any

from tsdatalake_workflow.pipeline.pipeline import Pipeline

class CompilerV1:
    def __init__(self, conf: Dict[str, Any]):
        self.conf = conf

    def _validate_task(self, task: Dict[str, Any]):
        required_keys = ['operator', 'params', 'task_id']
        for key in required_keys:
            if key not in task:
                raise ValueError(f"Task is missing required key: {key}")
            
    def _render_params(self, params: Dict[str, Any]):
        rendered_params = {}
        for key, value in params.items():
            if isinstance(value, str) and value.startswith('{{') and value.endswith('}}'):
                variable_name = value[2:-2]
                rendered_params[key] = self.conf['variables'][variable_name]
            else:
                rendered_params[key] = value
        return rendered_params
    
    def _load_operator(self, operator: str):
        module_name, class_name = operator.rsplit('.', 1)
        module = import_module(module_name)
        operator_class = getattr(module, class_name)
        return operator_class
    
    def create_pipeline(self):
        pipeline = Pipeline()
        for task in self.conf['tasks']:
            self._validate_task(task)
            operator = self._load_operator(task['operator'])
            pipeline.add_task(operator(**task['params']), task['task_id'])
        return pipeline
    

class CompilerFactory:
    def __init__(self):
        self.compilers = {
            'v1': CompilerV1
        }
    def get_compiler(self, version: str):
        return self.compilers[version]
    
