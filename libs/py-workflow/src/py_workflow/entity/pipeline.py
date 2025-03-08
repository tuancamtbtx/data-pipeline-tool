from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class TaskConfig:
    task_id: str
    operator: str
    params: Dict[str, str]
    dependencies: List[str] = field(default_factory=list)

@dataclass
class PipelineConfig:
    owner_name: str
    tasks: List[TaskConfig]

# Example usage
json_config = {
    "owner_name": "owner_name",
    "tasks": [
        {
            "task_id": "bigquery_to_drive",
            "operator": "BigqueryToDriveOperator",
            "params": {
                "project_id": "your_project_id",
                "dataset_id": "your_dataset_id",
                "table_id": "your_table_id",
                "destination_folder_id": "your_drive_folder_id",
                "file_name": "exported_data.csv",
                "credentials_path": "path_to_your_service_account_credentials.json"
            },
            "dependencies": []
        },
        {
            "task_id": "slack_alert",
            "operator": "SlackOperator",
            "params": {
                "token": "your_slack_api_token",
                "channel": "#your_channel_name",
                "message": "BigQuery data has been successfully exported to Google Drive!"
            },
            "dependencies": ["bigquery_to_drive"]
        }
    ]
}

# Function to convert JSON config to dataclass
def parse_pipeline_config(config: dict) -> PipelineConfig:
    tasks = [TaskConfig(**task) for task in config['tasks']]
    return PipelineConfig(owner_name=config['owner_name'], tasks=tasks)

# Parse the example JSON config
pipeline_config = parse_pipeline_config(json_config)

# Accessing data from the parsed config
print(pipeline_config.owner_name)
for task in pipeline_config.tasks:
    print(f"Task ID: {task.task_id}, Operator: {task.operator}, Dependencies: {task.dependencies}")