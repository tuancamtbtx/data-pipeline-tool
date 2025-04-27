import os
import datetime
import yaml
from functools import wraps
from jinja2 import Template
from typing import Any, Dict

from py_utils.utils.path import get_absolute_path
from py_utils.common.logger import LoggerMixin
from py_workflow.pipeline.config import Task
from py_workflow.pipeline.macros import (
    ds_filter,
    ds_nodash_filter,
    ts_filter,
    ts_nodash_filter,
    ts_nodash_with_tz_filter,
    previous_month_filter,
    start_month_filter,
    year_month_filter,
)


def sanitize_data(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: sanitize_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_data(item) for item in data]
    elif isinstance(data, str):
        # Remove escape sequences and special characters
        return data.replace("\n\n", "\n")
    else:
        return data


MACRO_FUNC_DICT = {
    "ds": ds_filter,
    "ds_nodash": ds_nodash_filter,
    "ts": ts_filter,
    "ts_nodash": ts_nodash_filter,
    "ts_nodash_with_tz": ts_nodash_with_tz_filter,
    "previous_month": previous_month_filter,
    "start_month": start_month_filter,
    "year_month": year_month_filter,
}


class TemplateRender(LoggerMixin):

    DEFAULT_TZ = "Asia/Ho_Chi_Minh"
    REF_VAR = "$refs."
    REF_VAR_FILE = "$vars."
    ENV_VAR = "$env."
    LOOKUP_VAR_DIR = "vars"
    # these fields will perform deepmerge instead of override
    SHOULD_MERGE_FIELDS = ["executor_config", "env_vars", "secrets", "conf"]

    def __init__(self):
        self.macro = {}
        self.macro.update(MACRO_FUNC_DICT)

    def load_refs_from_file(self, file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, "r") as file:
                data = yaml.safe_load(file)
                return sanitize_data(data)
        except FileNotFoundError:
            self.logger.error(f"The file at {file_path} was not found.")
            raise FileNotFoundError
        except yaml.YAMLError as exc:
            self.logger.error(f"Error parsing YAML file: {exc}")
            raise yaml.YAMLError

    def resolve_task_ref(self, template: Dict[str, Any], refs) -> str:
        if isinstance(template, Task):
            task_dict = template.to_dict()
            return {
                key: self.resolve_task_ref(value, refs)
                for key, value in task_dict.items()
            }
        if isinstance(template, dict):
            return {
                key: self.resolve_task_ref(value, refs)
                for key, value in template.items()
            }
        elif isinstance(template, list):
            return [self.resolve_task_ref(item, refs) for item in template]
        elif isinstance(template, str):
            return self._render_refs(template, refs)
        return template

    def _render_refs(self, value: dict, refs) -> str:
        if isinstance(value, str) and value.startswith(self.REF_VAR):
            ref_key = value[len(self.REF_VAR) :]
            return refs[ref_key]
        return value

    def _find_macro_values(self, value: str) -> list:
        macro_values = []
        start_pos = 0

        while True:
            start = value.find("{{", start_pos)
            end = value.find("}}", start)

            if start == -1 or end == -1:
                break

            macro_value = value[start + 2 : end].strip()
            macro_values.append(macro_value)

            start_pos = end + 2

        return macro_values

    def _render_macros(self, value: str) -> str:
        macro_values = self._find_macro_values(value)
        context = {
            name: self.macro[name](datetime.datetime.today()) for name in macro_values
        }
        value_template = Template(value)
        return value_template.render(context)

    def resolve_task_macros(self, template: str) -> str:
        if isinstance(template, Task):
            task_dict = template.to_dict()
            return {
                key: self.resolve_task_macros(value) for key, value in task_dict.items()
            }

        if isinstance(template, dict):
            return {
                key: self.resolve_task_macros(value) for key, value in template.items()
            }
        elif isinstance(template, list):
            return [self.resolve_task_macros(item) for item in template]
        elif isinstance(template, str):
            return self._render_macros(template)
        return template

    def resolve_env_vars(self, template: Dict[str, Any]) -> str:
        if isinstance(template, Task):
            task_dict = template.to_dict()
            return {
                key: self.resolve_env_vars(value) for key, value in task_dict.items()
            }

        if isinstance(template, dict):
            return {
                key: self.resolve_env_vars(value) for key, value in template.items()
            }
        elif isinstance(template, list):
            return [self.resolve_env_vars(item) for item in template]
        elif isinstance(template, str):
            return self._render_env_vars(template)
        return template

    def _render_env_vars(self, value: str) -> str:
        if isinstance(value, str) and value.startswith(self.ENV_VAR):
            env_key = value[len(self.ENV_VAR) :]
            return os.environ.get(env_key)
        return value

    def list_refs_path_vars_folder(self, vars_folder):
        list_files = []
        for root, dirs, files in os.walk(vars_folder):
            for file in files:
                if file.endswith(".yaml"):
                    list_files.append(os.path.join(root, file))
        return list_files

    def load_refs_from_vars_folder(self, list_refs_files):
        refs = {}
        for file in list_refs_files:
            with open(file, "r") as f:
                data = yaml.safe_load(f)
                refs.update(data)
        return refs


def get_vars_path(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return f"{get_absolute_path(TemplateRender.LOOKUP_VAR_DIR)}/{func(*args, **kwargs)}"

    return wrapper


def render(func):
    cls = TemplateRender()

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Call the original function to get the template
        template = func(*args, **kwargs)
        # Load refs from file
        vars_folder = f'{get_absolute_path(f"{TemplateRender.LOOKUP_VAR_DIR}")}'
        list_refs_files = cls.list_refs_path_vars_folder(vars_folder)
        template_refs = cls.resolve_task_ref(
            template, cls.load_refs_from_vars_folder(list_refs_files)
        )
        # Render the template
        template_macros = cls.resolve_task_macros(template_refs)
        template_env_vars = cls.resolve_env_vars(template_macros)
        return template_env_vars

    return wrapper
