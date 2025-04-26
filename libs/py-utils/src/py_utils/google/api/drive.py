from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import google.auth

from py_utils.common.logger import LoggerMixin


class GoogleDriveService(LoggerMixin):
    def __init__(self, **kwargs):
        super().__init__()
        self._SCOPES = ["https://www.googleapis.com/auth/drive"]
        self.drive_service = self.service()

    def service(self):
        credentials, project_id = google.auth.default(
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
        )
        self.logger.info(f"project_id: {project_id}")
        service = build("drive", "v3", credentials=credentials)
        return service

    def create_folder(self, folder_name, parent_folder_id=None):
        """Create a folder in Google Drive and return its ID."""
        folder_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id] if parent_folder_id else [],
        }

        created_folder = (
            self.drive_service.files()
            .create(body=folder_metadata, fields="id", supportsAllDrives=True)
            .execute()
        )

        self.logger.info(f'Created Folder ID: {created_folder["id"]}')
        return created_folder["id"]

    def upload_file_to_drive(
        self, file_path: Path, parent_folder_id: str = None, file_name: str = None
    ) -> str:
        self.logger.info(
            f"Upload file to drive with file_path: {file_path} and parent_folder_id: {parent_folder_id}"
        )
        file_metadata = {
            "name": file_name if file_name else file_path.name,
            "parents": [parent_folder_id] if parent_folder_id else None,
        }
        media = MediaFileUpload(
            file_path.resolve(),
            chunksize=5 * 1024 * 1024,
            mimetype="application/octet-stream",
            resumable=True,
        )
        request = self.drive_service.files().create(
            body=file_metadata, media_body=media, fields="id", supportsAllDrives=True
        )
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                self.logger.info("Uploaded %d%%." % int(status.progress() * 100))
        return file_path

    def upload_folder_to_drive(
        self, folder_path: Path, parent_folder_id: str = None
    ) -> str:
        folder_metadata = {
            "name": folder_path.name,
            "parents": [parent_folder_id] if parent_folder_id else None,
            "mimeType": "application/vnd.google-apps.folder",
        }
        folder = (
            self.drive_service.files()
            .create(
                body=folder_metadata,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )

        folder_id = folder.get("id")
        if folder_id:
            for file_path in folder_path.iterdir():
                if file_path.is_dir():
                    self.upload_folder_to_drive(
                        folder_path=file_path, parent_folder_id=folder_id
                    )
                else:
                    self.upload_file_to_drive(
                        file_path=file_path, parent_folder_id=folder_id
                    )

        return folder_id

    def get_folders(self, folder_name: str, parent_folder_id: str = None):
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
        response = (
            self.drive_service.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

        return [file.get("id") for file in response.get("files", [])]

    def list_folder_names(self, parent_folder_id: str):
        return [
            obj["name"]
            for obj in self._list_objects_in_folder(parent_id=parent_folder_id)
            if obj["mimeType"] == "application/vnd.google-apps.folder"
        ]

    def _list_objects_in_folder(self, parent_id: str = None, object_name: str = None):
        query = f"'{parent_id}' in parents and trashed=false" if parent_id else ""
        query += f" and name='{object_name}'" if object_name else ""
        response = (
            self.drive_service.files()
            .list(
                q=query,
                fields="nextPageToken, files",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        return response.get("files", [])

    def copy_file(self, file_id: str, new_name: str, parent_folder_id: str = None):
        file_metadata = {
            "name": new_name,
            "parents": [parent_folder_id] if parent_folder_id else None,
        }
        return (
            self.drive_service.files()
            .copy(fileId=file_id, body=file_metadata, supportsAllDrives=True)
            .execute()
        )
