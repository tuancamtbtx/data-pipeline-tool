from dataclasses import dataclass

from typing import Any, Dict, List


@dataclass
class Task:
    task_id: str
    operator: str
    params: Dict[str, Any]
    dependencies: List[str]

    def to_dict(self):
        return self.__dict__


@dataclass
class Dag:
    owner_name: str
    version: str
    name: str
    slack_on_failure: bool
    tasks: List[Task]

    def to_dict(self):
        return self.__dict__


class TaskFields:
    task_id = "task_id"
    operator = "operator"
    params = "params"
    dependencies = "dependencies"


class DagFields:
    owner_name = "owner_name"
    tasks = "tasks"
    version = "version"
    name = "name"
    slack_on_failure = "slack_on_failure"
    slack_channel = "slack_channel"
